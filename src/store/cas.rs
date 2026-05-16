//! File-level content-addressable storage.
//!
//! Each blob is keyed by its **SHA-256** hex digest and stored under
//! `<store>/blobs/<aa>/<bbcc...>` with a 2-char fanout dir. Identical
//! files across packages share a single on-disk copy; updating one file
//! in a multi-version IG dedupes naturally because only the changed
//! blob's hex differs.
//!
//! Why SHA-256 and not Blake3? The legacy [`crate::cas_storage::CasStorage`]
//! used Blake3 for tarball-level CAS — that stays unchanged. The new
//! file-level CAS uses SHA-256 hex because:
//!
//! - Pnpm 11, Cargo, Nix, and IPFS all use SHA-256 hex paths — long-term
//!   tooling compatibility is easier when the hash family matches the
//!   ecosystem.
//! - `fcm.lock` integrity (a different concern) uses SHA-512 SRI per
//!   pnpm/Bun lockfile convention. Two algos at different layers; each
//!   runs once per artefact.
//!
//! Atomic writes go through `<store>/tmp/<uuid>.part` then `rename`.

use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;

use crate::error::{FcmError, Result, StorageError};
use crate::store::paths::StorePaths;

/// SHA-256 hex digest. Wrapped to keep the hash family explicit at type
/// boundaries (we mix Blake3 elsewhere — see [`crate::content_hash`]).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlobHash(String);

impl BlobHash {
    /// Compute hash of in-memory bytes.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        Self(hex::encode(hasher.finalize()))
    }

    /// Construct from a known-good hex digest. Caller is responsible for
    /// length/charset (32-byte digest = 64 hex chars).
    pub fn from_hex(hex: impl Into<String>) -> Self {
        Self(hex.into())
    }

    /// Borrow as `&str` for path construction etc.
    pub fn as_hex(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for BlobHash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// File-level CAS facade.
///
/// Operations are async because `tokio::fs` calls into the blocking pool
/// for disk I/O. The CAS is intentionally **not** transactional: blobs
/// are content-addressed so concurrent writes of the same hash are safe
/// (the rename either succeeds or finds the destination already there).
#[derive(Debug, Clone)]
pub struct FileCas {
    paths: StorePaths,
    /// When `true` (default), every blob insert calls `sync_all` before
    /// the atomic rename so the blob bytes survive a kernel crash. When
    /// `false`, skip the fsync and rely on rename atomicity alone — a
    /// torn blob from a crash is recoverable by re-downloading its
    /// containing package. Drops ~3 ms per blob on APFS.
    durable: bool,
}

impl FileCas {
    pub fn new(paths: StorePaths) -> Self {
        Self::new_with_durability(paths, true)
    }

    pub fn new_with_durability(paths: StorePaths, durable: bool) -> Self {
        Self { paths, durable }
    }

    /// Insert raw bytes into CAS. Idempotent: returns the hash even when
    /// the blob already exists.
    pub async fn insert_bytes(&self, bytes: &[u8]) -> Result<BlobHash> {
        let hash = BlobHash::from_bytes(bytes);
        let dest = self.paths.blob_path(hash.as_hex());

        if tokio::fs::metadata(&dest).await.is_ok() {
            return Ok(hash);
        }

        // Ensure both the fanout dir and the staging tmp dir exist.
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let tmp_dir = self.paths.tmp();
        tokio::fs::create_dir_all(&tmp_dir).await?;

        // Stage write under tmp/, sync, then atomic rename. If the rename
        // races with another process inserting the same hash, the second
        // rename returns AlreadyExists on Windows and overwrites silently
        // on POSIX — content is identical either way.
        let tmp_path = tmp_dir.join(format!("blob-{}.part", uuid::Uuid::new_v4()));
        let mut file = tokio::fs::File::create(&tmp_path).await?;
        file.write_all(bytes).await?;
        if self.durable {
            file.sync_all().await?;
        }
        drop(file);

        match tokio::fs::rename(&tmp_path, &dest).await {
            Ok(()) => Ok(hash),
            Err(_) if tokio::fs::metadata(&dest).await.is_ok() => {
                let _ = tokio::fs::remove_file(&tmp_path).await;
                Ok(hash)
            }
            Err(e) => {
                let _ = tokio::fs::remove_file(&tmp_path).await;
                Err(FcmError::Storage(StorageError::IoError {
                    message: format!("Failed to commit blob {} to {}: {e}", hash, dest.display()),
                }))
            }
        }
    }

    /// Insert from an existing on-disk file. Hashes the bytes, then
    /// inserts into CAS. Returns the hash; the source file is NOT
    /// removed — caller decides whether to clean up.
    pub async fn insert_file(&self, src: &Path) -> Result<BlobHash> {
        let bytes = tokio::fs::read(src).await?;
        self.insert_bytes(&bytes).await
    }

    /// Insert from an existing on-disk file, preferring a hardlink (or
    /// reflink on COW filesystems) over a byte copy. The source file is
    /// hashed via a streaming reader, then linked in-place into the CAS
    /// fanout. Falls back to a full copy on cross-device or
    /// hardlink-unsupported filesystems.
    ///
    /// Use this when the source file is already authoritative on disk
    /// (e.g. a freshly-downloaded tarball cached at a known path) — it
    /// avoids the read-into-RAM + write-to-CAS round trip that
    /// `insert_file` pays.
    pub async fn insert_file_linked(&self, src: &Path) -> Result<BlobHash> {
        use tokio::io::AsyncReadExt;
        // Stream-hash the source so multi-hundred-MB tarballs don't
        // sit in RAM just to compute their digest. SHA-256 to match the
        // rest of FileCas's addressing scheme.
        let mut file = tokio::fs::File::open(src).await?;
        let mut hasher = Sha256::new();
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = file.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            hasher.update(&buf[..n]);
        }
        drop(file);
        let hash = BlobHash::from_hex(hex::encode(hasher.finalize()));
        let dest = self.paths.blob_path(hash.as_hex());

        if tokio::fs::metadata(&dest).await.is_ok() {
            return Ok(hash);
        }
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let src_owned = src.to_path_buf();
        let dest_owned = dest.clone();
        // Linking ladder (reflink → hardlink → copy). All three are
        // sync syscalls so they run in `spawn_blocking`. Order matches
        // the rest of the codebase (`linker.link_*`).
        let link_result = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
            if reflink_copy::reflink(&src_owned, &dest_owned).is_ok() {
                return Ok(());
            }
            if std::fs::hard_link(&src_owned, &dest_owned).is_ok() {
                return Ok(());
            }
            std::fs::copy(&src_owned, &dest_owned).map(|_| ())
        })
        .await
        .map_err(|e| {
            FcmError::Storage(StorageError::IoError {
                message: format!("CAS link worker panicked: {e}"),
            })
        })?;

        match link_result {
            Ok(()) => Ok(hash),
            Err(_) if tokio::fs::metadata(&dest).await.is_ok() => Ok(hash),
            Err(e) => Err(FcmError::Storage(StorageError::IoError {
                message: format!("Failed to link blob {} to {}: {e}", hash, dest.display()),
            })),
        }
    }

    /// Resolve a hex digest to its on-disk path. Does not check existence.
    pub fn path_for(&self, hash: &BlobHash) -> PathBuf {
        self.paths.blob_path(hash.as_hex())
    }

    /// True if the blob exists in CAS.
    pub async fn contains(&self, hash: &BlobHash) -> bool {
        tokio::fs::metadata(self.path_for(hash)).await.is_ok()
    }

    /// Read the blob's full contents.
    pub async fn read(&self, hash: &BlobHash) -> Result<Vec<u8>> {
        let path = self.path_for(hash);
        tokio::fs::read(&path).await.map_err(|e| {
            FcmError::Storage(StorageError::IoError {
                message: format!("Failed to read blob {hash} at {}: {e}", path.display()),
            })
        })
    }

    /// Borrow the underlying paths (for callers that need to inspect the
    /// store layout, e.g. for the `fcm store path` CLI subcommand).
    pub fn paths(&self) -> &StorePaths {
        &self.paths
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn hash_is_stable() {
        let h1 = BlobHash::from_bytes(b"hello world");
        let h2 = BlobHash::from_bytes(b"hello world");
        assert_eq!(h1, h2);
        assert_eq!(h1.as_hex().len(), 64); // 32 bytes hex
    }

    #[test]
    fn hash_differs_on_content() {
        assert_ne!(BlobHash::from_bytes(b"a"), BlobHash::from_bytes(b"b"));
    }

    #[tokio::test]
    async fn insert_then_read_round_trip() {
        let tmp = TempDir::new().unwrap();
        let cas = FileCas::new(StorePaths::with_root(tmp.path()));
        let bytes = b"a fhir resource pretending to be JSON".to_vec();
        let hash = cas.insert_bytes(&bytes).await.unwrap();
        assert!(cas.contains(&hash).await);
        assert_eq!(cas.read(&hash).await.unwrap(), bytes);
    }

    #[tokio::test]
    async fn insert_is_idempotent() {
        let tmp = TempDir::new().unwrap();
        let cas = FileCas::new(StorePaths::with_root(tmp.path()));
        let bytes = b"same content";
        let h1 = cas.insert_bytes(bytes).await.unwrap();
        let h2 = cas.insert_bytes(bytes).await.unwrap();
        assert_eq!(h1, h2);
    }

    #[tokio::test]
    async fn insert_file_dedup_two_paths_same_content() {
        let tmp = TempDir::new().unwrap();
        let cas = FileCas::new(StorePaths::with_root(tmp.path()));
        let src1 = tmp.path().join("a.json");
        let src2 = tmp.path().join("b.json");
        let body = b"{\"resourceType\":\"Patient\"}";
        tokio::fs::write(&src1, body).await.unwrap();
        tokio::fs::write(&src2, body).await.unwrap();

        let h1 = cas.insert_file(&src1).await.unwrap();
        let h2 = cas.insert_file(&src2).await.unwrap();
        assert_eq!(h1, h2);
        // Only one underlying blob exists.
        let dest = cas.path_for(&h1);
        assert!(dest.exists());
    }
}
