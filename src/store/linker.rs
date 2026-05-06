//! Reflink → hardlink → copy ladder for materialising blobs into a
//! per-project layout.
//!
//! The store always owns the canonical content (under `<store>/blobs/…`).
//! Projects materialise files by *linking* them into
//! `fhir-modules/.fcm/<pkg>@<ver>/package/<filename>`. Three strategies
//! are available, ordered by cost:
//!
//! 1. **Reflink (CoW)** via [`reflink-copy`](https://crates.io/crates/reflink-copy):
//!    APFS clonefile, Linux FICLONE on btrfs/xfs/bcachefs/ReFS Dev Drives.
//!    Zero disk space, edits stay independent (CoW).
//! 2. **Hardlink** via `std::fs::hard_link`: zero disk space, but edits
//!    propagate across all references — fine because FHIR resources are
//!    treated as read-only.
//! 3. **Copy** via `std::fs::copy`: full duplication. Falls back when the
//!    destination filesystem differs from the store FS (hardlinks cross
//!    nothing) or both reflink and hardlink fail.
//!
//! The chosen mode mirrors pnpm's `package-import-method` config knob.
//! Capability is probed once per [`Linker`] and cached.

use std::io;
use std::path::Path;

/// Strategy preferences. `Auto` follows the recommended ladder; the
/// other variants force a single approach (useful for benchmarking).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LinkerMode {
    /// Try reflink → hardlink → copy.
    #[default]
    Auto,
    /// Reflink only; error if unsupported.
    Reflink,
    /// Reflink → copy; skip hardlink (preserves CoW edit-safety).
    CloneOrCopy,
    /// Hardlink only; error if cross-FS.
    Hardlink,
    /// Always full copy.
    Copy,
}

bitflags::bitflags! {
    /// Detected runtime capabilities. Each `Linker` probes once and caches.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct LinkCapability: u8 {
        const REFLINK = 0b001;
        const HARDLINK = 0b010;
        const COPY = 0b100;
    }
}

impl LinkCapability {
    /// Always-available baseline. Copy can never fail for capability
    /// reasons — only for permission/space reasons (which surface as
    /// regular `io::Error`).
    pub const COPY_ONLY: Self = Self::COPY;
}

/// Materialises CAS blobs into project-local paths.
///
/// Construct with [`Linker::probe`] to detect capabilities for a given
/// `(store_root, project_root)` pair. Probing performs one tiny I/O to
/// test each strategy; cache the result for the lifetime of an install.
#[derive(Debug, Clone)]
pub struct Linker {
    mode: LinkerMode,
    caps: LinkCapability,
}

impl Linker {
    /// Static constructor without capability probing. Useful for tests
    /// or when the caller has already determined the right mode.
    pub fn with_caps(mode: LinkerMode, caps: LinkCapability) -> Self {
        Self { mode, caps }
    }

    /// Probe capabilities by attempting a no-op reflink and hardlink
    /// against a tiny scratch file inside `store_root`. Falls back
    /// silently — every probe failure is fine, we just record what works.
    ///
    /// `project_root` is the directory we'll later link *into*; it must
    /// exist. If `store_root` and `project_root` are on different
    /// filesystems, hardlinks are unavailable regardless of the per-FS
    /// reflink support.
    pub async fn probe(
        store_root: &Path,
        project_root: &Path,
        mode: LinkerMode,
    ) -> io::Result<Self> {
        let mut caps = LinkCapability::COPY;

        // Make sure both roots exist; the probe writes a temporary file
        // under each to test cross-FS link behaviour.
        tokio::fs::create_dir_all(store_root).await?;
        tokio::fs::create_dir_all(project_root).await?;

        let probe_id = uuid::Uuid::new_v4();
        let src = store_root.join(format!(".fcm-probe-src-{probe_id}"));
        let dst_reflink = project_root.join(format!(".fcm-probe-reflink-{probe_id}"));
        let dst_hardlink = project_root.join(format!(".fcm-probe-hardlink-{probe_id}"));
        tokio::fs::write(&src, b"probe").await?;

        // Reflink: best-effort, ignore any error.
        let src_for_reflink = src.clone();
        let dst_for_reflink = dst_reflink.clone();
        let reflink_ok = tokio::task::spawn_blocking(move || {
            reflink_copy::reflink(&src_for_reflink, &dst_for_reflink).is_ok()
        })
        .await
        .unwrap_or(false);
        if reflink_ok {
            caps |= LinkCapability::REFLINK;
        }
        let _ = tokio::fs::remove_file(&dst_reflink).await;

        // Hardlink: best-effort.
        let src_for_hardlink = src.clone();
        let dst_for_hardlink = dst_hardlink.clone();
        let hardlink_ok = tokio::task::spawn_blocking(move || {
            std::fs::hard_link(&src_for_hardlink, &dst_for_hardlink).is_ok()
        })
        .await
        .unwrap_or(false);
        if hardlink_ok {
            caps |= LinkCapability::HARDLINK;
        }
        let _ = tokio::fs::remove_file(&dst_hardlink).await;
        let _ = tokio::fs::remove_file(&src).await;

        Ok(Self { mode, caps })
    }

    pub fn capabilities(&self) -> LinkCapability {
        self.caps
    }
    pub fn mode(&self) -> LinkerMode {
        self.mode
    }

    /// Materialise `src` (a CAS blob) at `dst`. Creates parent dirs as
    /// needed. Strategy is determined by [`Self::mode`] + capabilities;
    /// each step falls through silently on failure.
    pub async fn materialise(&self, src: &Path, dst: &Path) -> io::Result<MaterialiseOutcome> {
        if let Some(parent) = dst.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        // If the destination already exists, skip. The store is the
        // source of truth; over-writing would risk dropping in-progress
        // user edits if they ever materialise *into* the same path
        // (shouldn't happen; FHIR consumers treat fhir-modules/ as
        // read-only).
        if tokio::fs::metadata(dst).await.is_ok() {
            return Ok(MaterialiseOutcome::AlreadyPresent);
        }

        let try_reflink = self.mode == LinkerMode::Auto
            || self.mode == LinkerMode::Reflink
            || self.mode == LinkerMode::CloneOrCopy;
        let try_hardlink = self.mode == LinkerMode::Auto || self.mode == LinkerMode::Hardlink;
        let must_reflink = self.mode == LinkerMode::Reflink;
        let must_hardlink = self.mode == LinkerMode::Hardlink;
        let allow_copy = matches!(
            self.mode,
            LinkerMode::Auto | LinkerMode::CloneOrCopy | LinkerMode::Copy
        );

        if try_reflink && self.caps.contains(LinkCapability::REFLINK) {
            let src_owned = src.to_path_buf();
            let dst_owned = dst.to_path_buf();
            let r =
                tokio::task::spawn_blocking(move || reflink_copy::reflink(&src_owned, &dst_owned))
                    .await
                    .map_err(|e| io::Error::other(format!("Join error: {e}")))?;
            if r.is_ok() {
                return Ok(MaterialiseOutcome::Reflink);
            }
            if must_reflink {
                return Err(r.unwrap_err());
            }
        } else if must_reflink {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Reflink unsupported on this filesystem",
            ));
        }

        if try_hardlink && self.caps.contains(LinkCapability::HARDLINK) {
            let src_owned = src.to_path_buf();
            let dst_owned = dst.to_path_buf();
            let r = tokio::task::spawn_blocking(move || std::fs::hard_link(&src_owned, &dst_owned))
                .await
                .map_err(|e| io::Error::other(format!("Join error: {e}")))?;
            if r.is_ok() {
                return Ok(MaterialiseOutcome::Hardlink);
            }
            if must_hardlink {
                return Err(r.unwrap_err());
            }
        } else if must_hardlink {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "Hardlink unsupported (probably cross-filesystem)",
            ));
        }

        if allow_copy {
            tokio::fs::copy(src, dst).await?;
            Ok(MaterialiseOutcome::Copy)
        } else {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "All eligible link strategies failed and copy is disabled",
            ))
        }
    }
}

/// What [`Linker::materialise`] actually did. Useful for telemetry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaterialiseOutcome {
    Reflink,
    Hardlink,
    Copy,
    AlreadyPresent,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn probe_works_within_temp() {
        let store = TempDir::new().unwrap();
        let project = TempDir::new().unwrap();
        let linker = Linker::probe(store.path(), project.path(), LinkerMode::Auto)
            .await
            .unwrap();
        // COPY is always available; the others depend on host FS.
        assert!(linker.capabilities().contains(LinkCapability::COPY));
    }

    #[tokio::test]
    async fn materialise_copies_when_forced() {
        let store = TempDir::new().unwrap();
        let project = TempDir::new().unwrap();
        let src = store.path().join("source");
        tokio::fs::write(&src, b"hello").await.unwrap();
        let dst = project.path().join("dst");

        let linker = Linker::with_caps(LinkerMode::Copy, LinkCapability::COPY_ONLY);
        let outcome = linker.materialise(&src, &dst).await.unwrap();
        assert_eq!(outcome, MaterialiseOutcome::Copy);
        assert_eq!(tokio::fs::read(&dst).await.unwrap(), b"hello");
    }

    #[tokio::test]
    async fn materialise_skips_when_dst_exists() {
        let store = TempDir::new().unwrap();
        let project = TempDir::new().unwrap();
        let src = store.path().join("source");
        tokio::fs::write(&src, b"new").await.unwrap();
        let dst = project.path().join("dst");
        tokio::fs::write(&dst, b"existing").await.unwrap();

        let linker = Linker::with_caps(LinkerMode::Copy, LinkCapability::COPY_ONLY);
        let outcome = linker.materialise(&src, &dst).await.unwrap();
        assert_eq!(outcome, MaterialiseOutcome::AlreadyPresent);
        assert_eq!(tokio::fs::read(&dst).await.unwrap(), b"existing");
    }

    #[tokio::test]
    async fn forced_hardlink_errors_when_capability_absent() {
        let store = TempDir::new().unwrap();
        let project = TempDir::new().unwrap();
        let src = store.path().join("source");
        tokio::fs::write(&src, b"x").await.unwrap();
        let dst = project.path().join("dst");
        let linker = Linker::with_caps(LinkerMode::Hardlink, LinkCapability::COPY_ONLY);
        let r = linker.materialise(&src, &dst).await;
        assert!(r.is_err());
    }
}
