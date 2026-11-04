//! Per-package cross-process install locks.
//!
//! Two `fcm install` invocations against the same store must not race on
//! the same `(name, version)`: extracting twice into the same blob/tarball
//! pair is wasteful but safe (CAS makes it idempotent), but the
//! `package_files` index insertion needs serialising at the SQLite-writer
//! level. To avoid even the wasteful work, hold an exclusive file lock
//! per package while we work.
//!
//! Locks live at `<store>/locks/<name>@<version>.lock`. They're advisory
//! (POSIX `flock` / Win32 `LockFileEx`) — backed by the maintained `fs4`
//! crate, which replaced `fs2` after the latter was abandoned in 2020.

use std::path::PathBuf;

use fs4::AsyncFileExt;
use tokio::fs::{File, OpenOptions};

use crate::error::{FcmError, Result, StorageError};
use crate::store::paths::StorePaths;

/// RAII handle around an exclusive package install lock. Drop releases
/// the lock automatically (the file stays on disk for re-use; the store
/// `gc` command can prune stale lockfiles).
#[derive(Debug)]
pub struct PackageInstallLock {
    file: File,
    path: PathBuf,
}

impl PackageInstallLock {
    /// Acquire an exclusive lock for `(name, version)`. Blocks
    /// asynchronously until the lock is available (no spinning).
    pub async fn acquire(paths: &StorePaths, name: &str, version: &str) -> Result<Self> {
        let locks_dir = paths.locks();
        tokio::fs::create_dir_all(&locks_dir).await?;
        // Sanitise filename: `@` and `/` are illegal on Windows; `name`
        // can contain dots which are fine. We URL-encode `/` (theoretical
        // — FHIR package ids don't actually contain slashes) but leave
        // other chars alone for legibility.
        let safe = format!("{name}@{version}.lock").replace('/', "%2F");
        let path = locks_dir.join(safe);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .await
            .map_err(|e| {
                FcmError::Storage(StorageError::IoError {
                    message: format!("Failed to open install lock {}: {e}", path.display()),
                })
            })?;
        // fs4 1.x exposes the lock operation as a sync method on the
        // AsyncFileExt trait (it doesn't actually need an executor — it's
        // a thin wrapper around `flock`/`LockFileEx`). The blocking call
        // is microseconds in the uncontended case; under contention it
        // can hold the worker thread, so we punt to spawn_blocking when
        // we can't be sure the wait is short.
        let lock_file = file.try_clone().await.map_err(|e| {
            FcmError::Storage(StorageError::IoError {
                message: format!("Failed to clone fd for lock {}: {e}", path.display()),
            })
        })?;
        tokio::task::spawn_blocking(move || lock_file.lock())
            .await
            .map_err(|e| {
                FcmError::Storage(StorageError::IoError {
                    message: format!("Lock task join failed for {}: {e}", path.display()),
                })
            })?
            .map_err(|e| {
                FcmError::Storage(StorageError::IoError {
                    message: format!("Failed to acquire install lock {}: {e}", path.display()),
                })
            })?;
        Ok(Self { file, path })
    }

    /// Path on disk. Useful for diagnostics and for the `fcm store gc`
    /// command which prunes stale lockfiles.
    pub fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for PackageInstallLock {
    fn drop(&mut self) {
        // `fs4`'s lock is released automatically when the file handle is
        // closed, which happens when `self.file` drops. Nothing to do.
        let _ = &self.file;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn acquire_then_release() {
        let temp = TempDir::new().unwrap();
        let paths = StorePaths::with_root(temp.path());
        paths.ensure_dirs().await.unwrap();
        let lock = PackageInstallLock::acquire(&paths, "hl7.fhir.r4.core", "4.0.1")
            .await
            .unwrap();
        assert!(lock.path().exists());
        drop(lock);
        // The file is left on disk for re-use; only the lock is released.
        assert!(temp.path().join("locks").exists());
    }

    #[tokio::test]
    async fn second_acquire_within_same_task_blocks_then_succeeds_after_drop() {
        let temp = TempDir::new().unwrap();
        let paths = StorePaths::with_root(temp.path());
        paths.ensure_dirs().await.unwrap();
        let l1 = PackageInstallLock::acquire(&paths, "p", "1").await.unwrap();
        // Spawn a contender that should block until l1 drops.
        let p2 = paths.clone();
        let handle =
            tokio::spawn(async move { PackageInstallLock::acquire(&p2, "p", "1").await.unwrap() });
        // Give it a moment to start.
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert!(!handle.is_finished(), "second acquire must block");
        drop(l1);
        // Now the contender should complete.
        let l2 = handle.await.unwrap();
        drop(l2);
    }
}
