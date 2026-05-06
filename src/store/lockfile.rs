//! `fcm.lock` — deterministic per-project lockfile.
//!
//! Records every resolved `(name, version)` together with the registry
//! URL it came from and an SRI integrity hash, so subsequent
//! `fcm install --frozen-lockfile` runs are bit-for-bit reproducible
//! across machines and registries. Format is TOML for diff-ability and
//! for consistency with `fcm.toml`.
//!
//! # Determinism rules
//!
//! 1. `[[package]]` arrays sorted by `(name, version)`.
//! 2. Within each entry the field order is fixed by the `serde::Serialize`
//!    derive (TOML respects struct field order). Order:
//!    `name, version, resolved, integrity, fhir_version, dependencies`.
//! 3. Each `dependencies` array is sorted lexicographically.
//! 4. No timestamps or machine-specific paths anywhere.
//! 5. UTF-8 + LF only. We round-trip via `toml::to_string_pretty` which
//!    produces stable output.
//!
//! # Integrity
//!
//! `integrity` is a [Subresource Integrity](https://www.w3.org/TR/SRI/)
//! string of the form `sha512-<base64>`. Computed locally from the
//! tarball bytes during install and pinned. Future installs from the
//! same lockfile re-verify against this value; a mismatch hard-fails.
//!
//! Some registries (e.g. `fs.get-ig.org`'s S3 stubs) don't publish
//! per-package hashes — the SRI is still computed from the bytes we
//! receive on first install ("trust on first use"). The lockfile then
//! makes subsequent installs strictly verifiable.

use base64::Engine;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha512};
use std::path::Path;

use crate::error::{FcmError, Result, StorageError};

/// Top-level `fcm.lock` schema. Versioned for forward-compat: the
/// `version = N` line is the first thing we read; older code refuses
/// newer files rather than silently accepting them.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Lockfile {
    pub version: u32,
    #[serde(default)]
    pub generated_by: String,
    #[serde(default)]
    pub meta: LockfileMeta,
    /// Resolved packages, sorted by `(name, version)` on every write.
    #[serde(default, rename = "package")]
    pub packages: Vec<LockedPackage>,
}

/// Lockfile-wide settings.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct LockfileMeta {
    /// FHIR versions referenced by any locked package.
    #[serde(default)]
    pub fhir_versions: Vec<String>,
    /// When `true`, lockfile may contain `current` / `dev` floating
    /// versions. Default `false` — IG Publisher's tolerance of these is
    /// a known reproducibility footgun.
    #[serde(default)]
    pub allow_floating_versions: bool,
}

/// One entry per resolved `(name, version)` tuple.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct LockedPackage {
    pub name: String,
    pub version: String,
    /// Exact registry URL the tarball was downloaded from on lock
    /// creation. Re-installs hit this URL first; falling through the
    /// configured fallback chain only on failure.
    pub resolved: String,
    /// SRI string `sha512-<base64>` over the tarball bytes.
    pub integrity: String,
    #[serde(default)]
    pub fhir_version: Option<String>,
    /// Transitive dependencies as `name@version` strings, sorted
    /// lexicographically on every write.
    #[serde(default)]
    pub dependencies: Vec<String>,
}

impl Lockfile {
    /// Current schema version emitted by writes. Bump on incompatible
    /// shape changes.
    pub const CURRENT_VERSION: u32 = 1;

    /// File name convention.
    pub const FILE_NAME: &'static str = "fcm.lock";

    /// Construct an empty lockfile stamped with the current version and
    /// `generated_by` line. Caller adds `[[package]]` entries via
    /// [`Self::upsert`] then [`Self::save`].
    pub fn empty() -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            generated_by: format!("{} {}", env!("CARGO_PKG_NAME"), env!("CARGO_PKG_VERSION")),
            meta: LockfileMeta::default(),
            packages: Vec::new(),
        }
    }

    /// Load from disk. Missing file returns an empty lockfile rather
    /// than `Err` — first-run install creates one. Schema versions
    /// newer than [`Self::CURRENT_VERSION`] are rejected with an
    /// explicit error so we don't silently downgrade.
    pub async fn load_or_empty(project_root: &Path) -> Result<Self> {
        let path = project_root.join(Self::FILE_NAME);
        match tokio::fs::read_to_string(&path).await {
            Ok(text) => {
                let lock: Self = toml::from_str(&text).map_err(|e| FcmError::TomlDe(e))?;
                if lock.version > Self::CURRENT_VERSION {
                    return Err(FcmError::Storage(StorageError::IoError {
                        message: format!(
                            "fcm.lock version {} is newer than supported version {}; \
                             upgrade octofhir-canonical-manager",
                            lock.version,
                            Self::CURRENT_VERSION
                        ),
                    }));
                }
                Ok(lock)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Self::empty()),
            Err(e) => Err(FcmError::Io(e)),
        }
    }

    /// Insert or replace a package entry. Maintains the determinism
    /// invariants — call [`Self::canonicalise`] once before saving.
    pub fn upsert(&mut self, pkg: LockedPackage) {
        if let Some(existing) = self
            .packages
            .iter_mut()
            .find(|p| p.name == pkg.name && p.version == pkg.version)
        {
            *existing = pkg;
        } else {
            self.packages.push(pkg);
        }
    }

    /// Apply determinism rules: sort `[[package]]` entries by
    /// `(name, version)`; sort each entry's `dependencies` lex.
    pub fn canonicalise(&mut self) {
        self.packages
            .sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.version.cmp(&b.version)));
        for p in &mut self.packages {
            p.dependencies.sort();
            p.dependencies.dedup();
        }
        self.meta.fhir_versions.sort();
        self.meta.fhir_versions.dedup();
    }

    /// Atomic save to `<project_root>/fcm.lock`. Canonicalises before
    /// serialising; writes via tmp + rename so the on-disk file is
    /// either fully old or fully new.
    pub async fn save(&mut self, project_root: &Path) -> Result<()> {
        self.canonicalise();
        let body = toml::to_string_pretty(self).map_err(FcmError::TomlSer)?;

        let dest = project_root.join(Self::FILE_NAME);
        let tmp = project_root.join(format!(".{}.tmp", Self::FILE_NAME));
        tokio::fs::write(&tmp, body.as_bytes()).await?;
        tokio::fs::rename(&tmp, &dest).await?;
        Ok(())
    }

    /// Compute the SRI integrity string for tarball bytes.
    /// Format: `sha512-<base64-of-raw-digest>`, matching pnpm/Bun.
    pub fn compute_integrity(bytes: &[u8]) -> String {
        let mut hasher = Sha512::new();
        hasher.update(bytes);
        let digest = hasher.finalize();
        let b64 = base64::engine::general_purpose::STANDARD.encode(digest);
        format!("sha512-{b64}")
    }

    /// Verify a candidate byte stream against an SRI string from a
    /// locked entry. Tolerates only `sha512-` (the format we emit).
    pub fn verify_integrity(bytes: &[u8], expected: &str) -> Result<()> {
        let Some((algo, b64)) = expected.split_once('-') else {
            return Err(FcmError::Storage(StorageError::IoError {
                message: format!("Malformed SRI string '{expected}': missing '-' separator"),
            }));
        };
        if algo != "sha512" {
            return Err(FcmError::Storage(StorageError::IoError {
                message: format!(
                    "Unsupported SRI algorithm '{algo}' in lockfile (only sha512 emitted)"
                ),
            }));
        }
        let want = base64::engine::general_purpose::STANDARD
            .decode(b64.as_bytes())
            .map_err(|e| {
                FcmError::Storage(StorageError::IoError {
                    message: format!("Invalid base64 in SRI '{expected}': {e}"),
                })
            })?;
        let got = Sha512::digest(bytes).to_vec();
        if want == got {
            Ok(())
        } else {
            Err(FcmError::Storage(StorageError::IoError {
                message: format!("Integrity mismatch: tarball does not match locked '{expected}'"),
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn pkg(name: &str, version: &str) -> LockedPackage {
        LockedPackage {
            name: name.into(),
            version: version.into(),
            resolved: format!("https://example/{name}/{version}"),
            integrity: "sha512-AAAA".into(),
            fhir_version: Some("4.0.1".into()),
            dependencies: vec![],
        }
    }

    #[test]
    fn integrity_roundtrip() {
        let bytes = b"abcdef";
        let sri = Lockfile::compute_integrity(bytes);
        assert!(sri.starts_with("sha512-"));
        Lockfile::verify_integrity(bytes, &sri).unwrap();
    }

    #[test]
    fn integrity_mismatch_errors() {
        let sri = Lockfile::compute_integrity(b"a");
        assert!(Lockfile::verify_integrity(b"b", &sri).is_err());
    }

    #[test]
    fn integrity_rejects_unknown_algo() {
        assert!(Lockfile::verify_integrity(b"a", "md5-xx").is_err());
        assert!(Lockfile::verify_integrity(b"a", "no-prefix").is_err());
    }

    #[test]
    fn canonicalise_sorts_packages_and_deps() {
        let mut lock = Lockfile::empty();
        let mut a = pkg("hl7.fhir.us.core", "7.0.0");
        a.dependencies = vec!["c@1".into(), "a@1".into(), "b@1".into(), "a@1".into()];
        let b = pkg("hl7.fhir.r4.core", "4.0.1");
        lock.upsert(a);
        lock.upsert(b);
        lock.canonicalise();
        assert_eq!(lock.packages[0].name, "hl7.fhir.r4.core");
        assert_eq!(lock.packages[1].name, "hl7.fhir.us.core");
        assert_eq!(lock.packages[1].dependencies, vec!["a@1", "b@1", "c@1"]);
    }

    #[tokio::test]
    async fn save_and_load_round_trip() {
        let dir = TempDir::new().unwrap();
        let mut lock = Lockfile::empty();
        lock.upsert(pkg("hl7.fhir.r4.core", "4.0.1"));
        lock.upsert(pkg("hl7.fhir.us.core", "7.0.0"));
        lock.save(dir.path()).await.unwrap();

        let loaded = Lockfile::load_or_empty(dir.path()).await.unwrap();
        assert_eq!(loaded.version, Lockfile::CURRENT_VERSION);
        assert_eq!(loaded.packages.len(), 2);
        assert_eq!(loaded.packages[0].name, "hl7.fhir.r4.core");
    }

    #[tokio::test]
    async fn missing_lockfile_yields_empty() {
        let dir = TempDir::new().unwrap();
        let lock = Lockfile::load_or_empty(dir.path()).await.unwrap();
        assert!(lock.packages.is_empty());
        assert_eq!(lock.version, Lockfile::CURRENT_VERSION);
    }

    #[tokio::test]
    async fn upsert_replaces_existing() {
        let mut lock = Lockfile::empty();
        let mut p = pkg("x", "1.0");
        p.integrity = "sha512-AAAA".into();
        lock.upsert(p);
        let mut p2 = pkg("x", "1.0");
        p2.integrity = "sha512-BBBB".into();
        lock.upsert(p2);
        assert_eq!(lock.packages.len(), 1);
        assert_eq!(lock.packages[0].integrity, "sha512-BBBB");
    }

    #[tokio::test]
    async fn future_version_rejected() {
        let dir = TempDir::new().unwrap();
        let body = format!(
            "version = {}\ngenerated_by = \"future\"\n",
            Lockfile::CURRENT_VERSION + 1
        );
        tokio::fs::write(dir.path().join("fcm.lock"), body)
            .await
            .unwrap();
        let r = Lockfile::load_or_empty(dir.path()).await;
        assert!(r.is_err(), "expected error on future version");
    }

    #[test]
    fn deterministic_serialisation_is_stable() {
        // Two equal lockfiles must serialise to byte-identical strings.
        let mut a = Lockfile::empty();
        let mut b = Lockfile::empty();
        // Insert in reversed orders to flush out any sort dependence.
        a.upsert(pkg("z", "1"));
        a.upsert(pkg("a", "1"));
        b.upsert(pkg("a", "1"));
        b.upsert(pkg("z", "1"));
        a.canonicalise();
        b.canonicalise();
        let sa = toml::to_string_pretty(&a).unwrap();
        let sb = toml::to_string_pretty(&b).unwrap();
        assert_eq!(sa, sb);
    }
}
