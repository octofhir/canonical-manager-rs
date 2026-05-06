//! Store path conventions.
//!
//! Mirrors pnpm's "per-drive store" pattern: the canonical location is
//! `~/.fcm/store/`, but if a project lives on a different filesystem
//! (hardlinks can't cross FS boundaries) callers can override via
//! [`StorePaths::with_root`] to put the store at
//! `<filesystem-root>/.fcm-store/`.
//!
//! All paths are absolute. Construction creates the directory tree
//! lazily on first I/O — `StorePaths` itself only computes paths.

use std::path::{Path, PathBuf};

/// Resolved layout for the global content-addressable store.
#[derive(Debug, Clone)]
pub struct StorePaths {
    root: PathBuf,
}

impl StorePaths {
    /// Default store root: `$HOME/.fcm/store/`.
    ///
    /// Falls back to `./.fcm-store/` when `HOME` is unset (CI sandboxes,
    /// containers). Emits a tracing warning in that case so the user can
    /// configure an explicit cache dir before disk usage piles up next to
    /// the working directory.
    pub fn default_user() -> Self {
        let root = match dirs::home_dir() {
            Some(h) => h.join(".fcm").join("store"),
            None => {
                tracing::warn!(
                    "HOME is not set; using ./.fcm-store/ as the store root. \
                     Set FCM_CACHE_DIR or configure storage.cache_dir to control this."
                );
                PathBuf::from(".fcm-store")
            }
        };
        Self { root }
    }

    /// Construct from an explicit root. Useful for tests and per-project
    /// overrides (e.g. when the project is on a different filesystem from
    /// `$HOME`).
    pub fn with_root(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Absolute path to the store root.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// File-level CAS root: `<store>/blobs/`.
    pub fn blobs(&self) -> PathBuf {
        self.root.join("blobs")
    }

    /// Tarball CAS root: `<store>/tarballs/`.
    pub fn tarballs(&self) -> PathBuf {
        self.root.join("tarballs")
    }

    /// Staging area for atomic writes: `<store>/tmp/`.
    pub fn tmp(&self) -> PathBuf {
        self.root.join("tmp")
    }

    /// Cross-process locks: `<store>/locks/`.
    pub fn locks(&self) -> PathBuf {
        self.root.join("locks")
    }

    /// Store metadata file: `<store>/store.json`. Records schema version
    /// and detected linker capabilities so we don't re-probe per install.
    pub fn meta_file(&self) -> PathBuf {
        self.root.join("store.json")
    }

    /// Build the full path for a hex-keyed blob with two-char fanout.
    /// `<store>/blobs/<aa>/<bbcc...>`. Used by [`crate::store::FileCas`].
    pub fn blob_path(&self, hex: &str) -> PathBuf {
        debug_assert!(hex.len() >= 2, "blob hex digest must be at least 2 chars");
        let (head, tail) = hex.split_at(2);
        self.blobs().join(head).join(tail)
    }

    /// Build the full path for a tarball CAS entry with two-char fanout.
    pub fn tarball_path(&self, hex: &str, ext: &str) -> PathBuf {
        debug_assert!(
            hex.len() >= 2,
            "tarball hex digest must be at least 2 chars"
        );
        let (head, tail) = hex.split_at(2);
        self.tarballs().join(head).join(format!("{tail}.{ext}"))
    }

    /// Materialise the directory tree on disk. Idempotent.
    pub async fn ensure_dirs(&self) -> std::io::Result<()> {
        for p in [self.blobs(), self.tarballs(), self.tmp(), self.locks()] {
            tokio::fs::create_dir_all(&p).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fanout_layout() {
        let s = StorePaths::with_root("/tmp/teststore");
        assert_eq!(s.root(), Path::new("/tmp/teststore"));
        assert_eq!(s.blobs(), Path::new("/tmp/teststore/blobs"));
        assert_eq!(
            s.blob_path("abcdef0123"),
            Path::new("/tmp/teststore/blobs/ab/cdef0123")
        );
        assert_eq!(
            s.tarball_path("0123456789", "tgz"),
            Path::new("/tmp/teststore/tarballs/01/23456789.tgz")
        );
    }

    #[tokio::test]
    async fn ensure_dirs_creates_layout() {
        let temp = tempfile::TempDir::new().unwrap();
        let s = StorePaths::with_root(temp.path());
        s.ensure_dirs().await.unwrap();
        for sub in &["blobs", "tarballs", "tmp", "locks"] {
            assert!(
                temp.path().join(sub).is_dir(),
                "{sub} should exist after ensure_dirs"
            );
        }
    }
}
