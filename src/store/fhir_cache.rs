//! `~/.fhir/packages/` compatibility shim.
//!
//! Mirrors a successful install into the cross-tool layout SUSHI / IG
//! Publisher / Firely tooling expect:
//!
//! ```text
//! ~/.fhir/packages/<name>-<version>/
//! └── package/...
//! ```
//!
//! The bytes are linked from the file-level CAS via the existing
//! [`crate::store::Linker`] ladder so the shim doesn't duplicate disk
//! usage when the destination filesystem supports reflink/hardlink.
//!
//! Tests pass an explicit `target_root` rather than calling
//! `dirs::home_dir()` so they can route the shim into a temp dir.
//! Production callers default to `dirs::home_dir().join(".fhir/packages")`
//! via [`default_fhir_cache_root`].

use std::path::{Path, PathBuf};

use crate::error::{FcmError, Result, StorageError};
use crate::store::linker::Linker;
use crate::store::materialize::{MaterialiseReport, materialise_dir};

/// Default `~/.fhir/packages/` root. Returns `None` when `HOME` is
/// unset (CI sandboxes) so callers can decide whether to fall back to
/// a config-controlled path or skip the shim entirely.
pub fn default_fhir_cache_root() -> Option<PathBuf> {
    dirs::home_dir().map(|h| h.join(".fhir").join("packages"))
}

/// Mirror an installed package into `<target_root>/<name>-<version>/`.
///
/// `source_package_root` is the directory containing the extracted
/// `package/` subdir, matching the convention used by the install
/// pipeline. When the source already has the inner `package/` layer it
/// is preserved; otherwise the source's contents are placed under
/// `package/` so SUSHI / IG Publisher see the path they expect.
pub async fn populate_fhir_cache(
    target_root: &Path,
    name: &str,
    version: &str,
    source_package_root: &Path,
    linker: &Linker,
) -> Result<MaterialiseReport> {
    let dest = target_root.join(format!("{name}-{version}"));
    let inner_dest = dest.join("package");
    tokio::fs::create_dir_all(&inner_dest).await?;

    let inner_source = source_package_root.join("package");
    let actual_source = if tokio::fs::try_exists(&inner_source).await.unwrap_or(false) {
        inner_source
    } else if tokio::fs::try_exists(source_package_root)
        .await
        .unwrap_or(false)
    {
        source_package_root.to_path_buf()
    } else {
        return Err(FcmError::Storage(StorageError::IoError {
            message: format!(
                "Cannot mirror {name}@{version} to FHIR cache: source missing at {}",
                source_package_root.display()
            ),
        }));
    };

    materialise_dir(&actual_source, &inner_dest, linker).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::linker::{LinkCapability, LinkerMode};
    use tempfile::TempDir;

    async fn write(p: &Path, body: &[u8]) {
        if let Some(parent) = p.parent() {
            tokio::fs::create_dir_all(parent).await.unwrap();
        }
        tokio::fs::write(p, body).await.unwrap();
    }

    #[tokio::test]
    async fn populate_writes_package_subdir_layout() {
        let tmp = TempDir::new().unwrap();
        // FHIR-standard layout: source has top-level `package/`.
        let pkg_root = tmp.path().join("hl7.fhir.r4.core-4.0.1");
        write(&pkg_root.join("package/.index.json"), b"{}").await;
        write(
            &pkg_root.join("package/StructureDefinition-Patient.json"),
            b"{}",
        )
        .await;

        let target_root = tmp.path().join("fhir");
        let linker = Linker::with_caps(LinkerMode::Copy, LinkCapability::COPY_ONLY);
        let report = populate_fhir_cache(
            &target_root,
            "hl7.fhir.r4.core",
            "4.0.1",
            &pkg_root,
            &linker,
        )
        .await
        .unwrap();
        assert_eq!(report.copied, 2);

        let mirrored = target_root.join("hl7.fhir.r4.core-4.0.1/package");
        assert!(mirrored.join(".index.json").exists());
        assert!(mirrored.join("StructureDefinition-Patient.json").exists());
    }

    #[tokio::test]
    async fn populate_handles_flat_source_layout() {
        let tmp = TempDir::new().unwrap();
        // Source without the `package/` subdir: callers that already
        // unpacked the inner directory should still produce the
        // canonical `<target>/<name>-<version>/package/...` shape.
        let pkg_root = tmp.path().join("flat-pkg");
        write(&pkg_root.join("package.json"), b"{}").await;
        write(&pkg_root.join("CodeSystem-foo.json"), b"{}").await;

        let target_root = tmp.path().join("fhir");
        let linker = Linker::with_caps(LinkerMode::Copy, LinkCapability::COPY_ONLY);
        let report = populate_fhir_cache(&target_root, "flat", "0.0.1", &pkg_root, &linker)
            .await
            .unwrap();
        assert_eq!(report.copied, 2);
        assert!(target_root.join("flat-0.0.1/package/package.json").exists());
    }

    #[tokio::test]
    async fn populate_errors_when_source_missing() {
        let tmp = TempDir::new().unwrap();
        let target_root = tmp.path().join("fhir");
        let bogus = tmp.path().join("does-not-exist");
        let linker = Linker::with_caps(LinkerMode::Copy, LinkCapability::COPY_ONLY);
        let r = populate_fhir_cache(&target_root, "x", "1", &bogus, &linker).await;
        assert!(r.is_err());
    }
}
