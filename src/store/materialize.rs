//! Materialise installed packages into a per-project virtual layout.
//!
//! Produces:
//!
//! ```text
//! <project>/.fcm/modules/
//! ├── <name> -> <name>@<version>/package        (symlink)
//! └── <name>@<version>/package/...              (linked files)
//! ```
//!
//! Lives under the existing project-local `.fcm/` convention (matches
//! `cache_dir = ".fcm/cache"` and `packages_dir = ".fcm/packages"`).
//! Top-level `<name>` symlinks let downstream tools resolve a package by
//! name; pinned versions live at `<name>@<version>/package`. Inspired
//! by pnpm's `node_modules/.pnpm/<pkg>@<ver>/node_modules/<pkg>` but
//! flattened — FHIR has no peer-dep instantiation so we skip the extra
//! hop.
//!
//! File contents are *linked* (reflink → hardlink → copy fallback) from
//! `<config.storage.packages_dir>/<name>-<version>/package/`, so
//! multiple projects can share one cache without duplicating bytes.

use std::path::{Path, PathBuf};

use crate::error::{FcmError, Result, StorageError};
use crate::store::linker::{Linker, MaterialiseOutcome};

/// Per-call summary returned by [`materialise_package`]. Useful for
/// telemetry and CLI status output.
#[derive(Debug, Default, Clone)]
pub struct MaterialiseReport {
    /// Files materialised via reflink (CoW).
    pub reflinked: usize,
    /// Files materialised via hardlink.
    pub hardlinked: usize,
    /// Files materialised via copy.
    pub copied: usize,
    /// Files skipped because destination already existed with right content.
    pub skipped: usize,
    /// Total files attempted (sum of above).
    pub total: usize,
}

impl MaterialiseReport {
    pub fn merge(&mut self, other: &Self) {
        self.reflinked += other.reflinked;
        self.hardlinked += other.hardlinked;
        self.copied += other.copied;
        self.skipped += other.skipped;
        self.total += other.total;
    }
}

/// Recursively walk `src` and link every regular file into the
/// corresponding `dst` path. Directories are created as needed; symlinks
/// in `src` are followed (FHIR packages don't ship symlinks; if one is
/// encountered, treat as a regular file via the underlying tar entry
/// type filter).
///
/// `dst` is the final on-disk path; caller is responsible for ensuring
/// it sits inside the project tree.
pub async fn materialise_dir(src: &Path, dst: &Path, linker: &Linker) -> Result<MaterialiseReport> {
    if !tokio::fs::try_exists(src).await.unwrap_or(false) {
        return Err(FcmError::Storage(StorageError::IoError {
            message: format!("Source directory does not exist: {}", src.display()),
        }));
    }
    tokio::fs::create_dir_all(dst).await?;

    let mut report = MaterialiseReport::default();
    let mut stack: Vec<(PathBuf, PathBuf)> = vec![(src.to_path_buf(), dst.to_path_buf())];
    while let Some((cur_src, cur_dst)) = stack.pop() {
        let mut entries = tokio::fs::read_dir(&cur_src).await?;
        while let Some(entry) = entries.next_entry().await? {
            let ty = entry.file_type().await?;
            let name = entry.file_name();
            let next_src = cur_src.join(&name);
            let next_dst = cur_dst.join(&name);
            if ty.is_dir() {
                tokio::fs::create_dir_all(&next_dst).await?;
                stack.push((next_src, next_dst));
            } else if ty.is_file() {
                let outcome = linker
                    .materialise(&next_src, &next_dst)
                    .await
                    .map_err(|e| {
                        FcmError::Storage(StorageError::IoError {
                            message: format!(
                                "Failed to materialise {} → {}: {e}",
                                next_src.display(),
                                next_dst.display()
                            ),
                        })
                    })?;
                report.total += 1;
                match outcome {
                    MaterialiseOutcome::Reflink => report.reflinked += 1,
                    MaterialiseOutcome::Hardlink => report.hardlinked += 1,
                    MaterialiseOutcome::Copy => report.copied += 1,
                    MaterialiseOutcome::AlreadyPresent => report.skipped += 1,
                }
            }
            // Symlinks and special entries are skipped silently — same
            // policy as the tar-extract entry filter (FHIR packages
            // shouldn't ship them).
        }
    }
    Ok(report)
}

/// Build the project-local `.fcm/modules/` directory entry for a single
/// installed package.
///
/// `source_package_root` is the directory containing the extracted
/// `package/` subdir (i.e. `<packages_dir>/<name>-<version>` per the
/// install pipeline's convention). `project_root` is where `fcm.toml`
/// lives.
///
/// On success the project tree gains:
///
/// ```text
/// <project_root>/.fcm/modules/
/// ├── <name> -> <name>@<version>/package        (symlink)
/// └── <name>@<version>/package/...              (linked files)
/// ```
///
/// The top-level `<name>` symlink is recreated each call so it always
/// points at the latest materialised version. Existing `<name>@<version>/`
/// is overlaid (idempotent at the file level: identical files are
/// skipped, new files added).
pub async fn materialise_package(
    project_root: &Path,
    name: &str,
    version: &str,
    source_package_root: &Path,
    linker: &Linker,
) -> Result<MaterialiseReport> {
    let modules_dir = project_root.join(".fcm/modules");
    let internal_dir = modules_dir
        .join(format!("{name}@{version}"))
        .join("package");
    let source_inner = source_package_root.join("package");

    // FHIR packages always have a top-level `package/` subdir. If the
    // installer dropped files at the source root instead (older path
    // conventions), fall back to the source root itself.
    let actual_source = if tokio::fs::try_exists(&source_inner).await.unwrap_or(false) {
        source_inner
    } else {
        source_package_root.to_path_buf()
    };

    let report = materialise_dir(&actual_source, &internal_dir, linker).await?;

    // Refresh the top-level symlink: <project>/.fcm/modules/<name>.
    let symlink_path = modules_dir.join(name);
    // Drop any prior link/dir at the symlink path; ignore NotFound.
    let _ = tokio::fs::remove_file(&symlink_path).await;
    let _ = tokio::fs::remove_dir_all(&symlink_path).await;
    create_dir_symlink(&internal_dir, &symlink_path).await?;
    Ok(report)
}

/// Create a directory-style symlink. Wraps the platform-specific calls.
async fn create_dir_symlink(target: &Path, link: &Path) -> Result<()> {
    let target = target.to_path_buf();
    let link = link.to_path_buf();
    tokio::task::spawn_blocking(move || {
        #[cfg(unix)]
        {
            std::os::unix::fs::symlink(&target, &link)
        }
        #[cfg(windows)]
        {
            std::os::windows::fs::symlink_dir(&target, &link)
        }
    })
    .await
    .map_err(|e| {
        FcmError::Storage(StorageError::IoError {
            message: format!("Symlink task join failed: {e}"),
        })
    })?
    .map_err(|e| {
        FcmError::Storage(StorageError::IoError {
            message: format!("Failed to create symlink: {e}"),
        })
    })
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
    async fn materialise_dir_recurses_and_counts() {
        let tmp = TempDir::new().unwrap();
        let src = tmp.path().join("src");
        let dst = tmp.path().join("dst");
        write(&src.join("a.json"), b"a").await;
        write(&src.join("nested/b.json"), b"b").await;
        write(&src.join("nested/deep/c.json"), b"c").await;

        let linker = Linker::with_caps(LinkerMode::Copy, LinkCapability::COPY_ONLY);
        let report = materialise_dir(&src, &dst, &linker).await.unwrap();
        assert_eq!(report.total, 3);
        assert_eq!(report.copied, 3);
        assert_eq!(report.skipped, 0);

        // Re-run is a no-op (everything skipped).
        let report = materialise_dir(&src, &dst, &linker).await.unwrap();
        assert_eq!(report.skipped, 3);
        assert_eq!(report.copied, 0);

        // Files match.
        assert_eq!(tokio::fs::read(dst.join("a.json")).await.unwrap(), b"a");
        assert_eq!(
            tokio::fs::read(dst.join("nested/deep/c.json"))
                .await
                .unwrap(),
            b"c"
        );
    }

    #[tokio::test]
    async fn materialise_package_creates_symlink_and_layout() {
        let tmp = TempDir::new().unwrap();
        // Source has FHIR-standard `package/` layout.
        let pkg_root = tmp.path().join("hl7.fhir.r4.core-4.0.1");
        write(&pkg_root.join("package/package.json"), b"{}").await;
        write(
            &pkg_root.join("package/StructureDefinition-Patient.json"),
            b"{}",
        )
        .await;

        let project = TempDir::new().unwrap();
        let linker = Linker::with_caps(LinkerMode::Copy, LinkCapability::COPY_ONLY);

        let report = materialise_package(
            project.path(),
            "hl7.fhir.r4.core",
            "4.0.1",
            &pkg_root,
            &linker,
        )
        .await
        .unwrap();
        assert_eq!(report.total, 2);
        assert_eq!(report.copied, 2);

        // Internal layout exists.
        let internal = project
            .path()
            .join(".fcm/modules/hl7.fhir.r4.core@4.0.1/package");
        assert!(internal.is_dir());
        assert!(internal.join("package.json").exists());
        assert!(internal.join("StructureDefinition-Patient.json").exists());

        // Symlink exists and resolves into .fcm/<pkg>@<ver>/package.
        let link = project.path().join(".fcm/modules/hl7.fhir.r4.core");
        let resolved = tokio::fs::read_link(&link).await.unwrap();
        assert!(resolved.ends_with("hl7.fhir.r4.core@4.0.1/package"));
    }

    #[tokio::test]
    async fn materialise_package_handles_no_inner_package_dir() {
        // Older callers that pass a directory whose contents are already
        // package files (no `package/` subdir).
        let tmp = TempDir::new().unwrap();
        let pkg_root = tmp.path().join("legacy-pkg-root");
        write(&pkg_root.join("package.json"), b"{}").await;
        write(&pkg_root.join("CodeSystem-foo.json"), b"{}").await;

        let project = TempDir::new().unwrap();
        let linker = Linker::with_caps(LinkerMode::Copy, LinkCapability::COPY_ONLY);
        let report = materialise_package(project.path(), "legacy", "0.0.1", &pkg_root, &linker)
            .await
            .unwrap();
        assert_eq!(report.copied, 2);

        let internal = project.path().join(".fcm/modules/legacy@0.0.1/package");
        assert!(internal.join("package.json").exists());
    }
}
