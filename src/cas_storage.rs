// Content-Addressable Storage (CAS) for FHIR resources
//
// Implements a filesystem-based content-addressable storage system where files
// are stored by their Blake3 content hash. This provides:
//
// - Automatic deduplication (same content = same hash = stored once)
// - Immutable storage (content never changes after write)
// - Parallel-safe operations (atomic writes via temp files)
// - Efficient lookups (O(1) by hash)
//
// Storage layout:
//   ~/.maki/cache/
//     ├── packages/{hash}.tgz    # Compressed package tarballs
//     └── resources/{hash}.json  # Individual FHIR resources

use crate::content_hash::ContentHash;
use std::path::{Path, PathBuf};
use tokio::fs;
use tokio::io::AsyncWriteExt;

/// Content-addressable storage manager
///
/// This struct manages a directory-based CAS where content is stored by hash.
/// It ensures atomic writes and provides efficient concurrent access.
#[derive(Debug, Clone)]
pub struct CasStorage {
    base_path: PathBuf,
}

impl CasStorage {
    /// Create a new CAS storage at the given base path
    ///
    /// The base path will contain subdirectories for different content types:
    /// - `packages/` for package tarballs (.tgz)
    /// - `resources/` for individual FHIR resources (.json)
    ///
    /// # Example
    /// ```no_run
    /// use octofhir_canonical_manager::cas_storage::CasStorage;
    /// use std::path::PathBuf;
    ///
    /// # tokio_test::block_on(async {
    /// let cas = CasStorage::new(PathBuf::from("~/.maki/cache")).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    pub async fn new(base_path: PathBuf) -> std::io::Result<Self> {
        // Ensure base directories exist
        fs::create_dir_all(&base_path).await?;
        fs::create_dir_all(base_path.join("packages")).await?;
        fs::create_dir_all(base_path.join("resources")).await?;

        Ok(Self { base_path })
    }

    /// Store content in CAS and return the path
    ///
    /// This performs an atomic write using a temporary file and rename.
    /// If content with the same hash already exists, the existing file is used.
    ///
    /// # Arguments
    /// * `content` - The bytes to store
    /// * `hash` - The Blake3 hash of the content
    /// * `content_type` - Either "packages" or "resources" subdirectory
    ///
    /// # Returns
    /// The path where the content was stored
    ///
    /// # Atomic Guarantees
    /// - Writes to temporary file first (.tmp suffix)
    /// - Renames to final name only after successful write
    /// - If destination exists, assumes it's identical (same hash) and skips
    ///
    /// # Example
    /// ```no_run
    /// use octofhir_canonical_manager::cas_storage::{CasStorage, ContentType};
    /// use octofhir_canonical_manager::content_hash::ContentHash;
    /// use std::path::PathBuf;
    ///
    /// # tokio_test::block_on(async {
    /// let cas = CasStorage::new(PathBuf::from("~/.maki/cache")).await?;
    /// let content = b"{\"resourceType\": \"Patient\"}";
    /// let hash = ContentHash::from_bytes(content);
    ///
    /// let path = cas.store(content, &hash, ContentType::Resource).await?;
    /// println!("Stored at: {}", path.display());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    pub async fn store(
        &self,
        content: &[u8],
        hash: &ContentHash,
        content_type: ContentType,
    ) -> std::io::Result<PathBuf> {
        let subdir = match content_type {
            ContentType::Package => "packages",
            ContentType::Resource => "resources",
        };

        let extension = match content_type {
            ContentType::Package => "tgz",
            ContentType::Resource => "json",
        };

        let filename = format!("{}.{}", hash.to_hex(), extension);
        let final_path = self.base_path.join(subdir).join(&filename);

        // If file already exists, assume it's identical (same hash) and return
        if final_path.exists() {
            return Ok(final_path);
        }

        // Write to temporary file first (atomic operation)
        let temp_path = final_path.with_extension(format!("{}.tmp", extension));

        let mut file = fs::File::create(&temp_path).await?;
        file.write_all(content).await?;
        file.sync_all().await?; // Ensure data is flushed to disk
        drop(file); // Close file before rename

        // Atomic rename to final destination
        match fs::rename(&temp_path, &final_path).await {
            Ok(()) => Ok(final_path),
            Err(e) => {
                let _ = fs::remove_file(&temp_path).await;
                if final_path.exists() || e.kind() == std::io::ErrorKind::AlreadyExists {
                    Ok(final_path)
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Retrieve content from CAS by hash
    ///
    /// # Errors
    /// Returns `std::io::ErrorKind::NotFound` if content doesn't exist
    ///
    /// # Example
    /// ```no_run
    /// # use octofhir_canonical_manager::cas_storage::{CasStorage, ContentType};
    /// # use octofhir_canonical_manager::content_hash::ContentHash;
    /// # use std::path::PathBuf;
    /// # tokio_test::block_on(async {
    /// # let cas = CasStorage::new(PathBuf::from("~/.maki/cache")).await?;
    /// # let hash = ContentHash::from_bytes(b"test");
    /// let content = cas.retrieve(&hash, ContentType::Resource).await?;
    /// println!("Retrieved {} bytes", content.len());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    pub async fn retrieve(
        &self,
        hash: &ContentHash,
        content_type: ContentType,
    ) -> std::io::Result<Vec<u8>> {
        let path = self.path_for(hash, content_type);
        fs::read(&path).await
    }

    /// Check if content exists in CAS
    ///
    /// This is more efficient than trying to retrieve and checking for error.
    ///
    /// # Example
    /// ```no_run
    /// # use octofhir_canonical_manager::cas_storage::{CasStorage, ContentType};
    /// # use octofhir_canonical_manager::content_hash::ContentHash;
    /// # use std::path::PathBuf;
    /// # tokio_test::block_on(async {
    /// # let cas = CasStorage::new(PathBuf::from("~/.maki/cache")).await?;
    /// # let hash = ContentHash::from_bytes(b"test");
    /// if cas.exists(&hash, ContentType::Resource).await {
    ///     println!("Content already cached!");
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    pub async fn exists(&self, hash: &ContentHash, content_type: ContentType) -> bool {
        let path = self.path_for(hash, content_type);
        tokio::fs::try_exists(&path).await.unwrap_or(false)
    }

    /// Get the filesystem path for a given hash (without checking existence)
    ///
    /// This is useful for passing paths to external tools or for batch operations.
    pub fn path_for(&self, hash: &ContentHash, content_type: ContentType) -> PathBuf {
        let (subdir, extension) = match content_type {
            ContentType::Package => ("packages", "tgz"),
            ContentType::Resource => ("resources", "json"),
        };

        let filename = format!("{}.{}", hash.to_hex(), extension);
        self.base_path.join(subdir).join(filename)
    }

    /// Delete content from CAS
    ///
    /// **Warning:** This permanently removes content. Use with caution.
    /// Typically only used for garbage collection of unreferenced content.
    ///
    /// # Errors
    /// Returns error if file doesn't exist or can't be deleted
    pub async fn delete(
        &self,
        hash: &ContentHash,
        content_type: ContentType,
    ) -> std::io::Result<()> {
        let path = self.path_for(hash, content_type);
        fs::remove_file(&path).await
    }

    /// Get storage statistics
    ///
    /// Returns information about storage usage, content counts, etc.
    pub async fn stats(&self) -> std::io::Result<CasStats> {
        let mut stats = CasStats::default();

        // Count packages
        if let Ok(mut entries) = fs::read_dir(self.base_path.join("packages")).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                if let Ok(metadata) = entry.metadata().await {
                    stats.package_count += 1;
                    stats.total_size += metadata.len();
                }
            }
        }

        // Count resources
        if let Ok(mut entries) = fs::read_dir(self.base_path.join("resources")).await {
            while let Ok(Some(entry)) = entries.next_entry().await {
                if let Ok(metadata) = entry.metadata().await {
                    stats.resource_count += 1;
                    stats.total_size += metadata.len();
                }
            }
        }

        Ok(stats)
    }

    /// Get the base path of this CAS
    pub fn base_path(&self) -> &Path {
        &self.base_path
    }
}

/// Type of content stored in CAS
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentType {
    /// Package tarball (.tgz)
    Package,
    /// Individual FHIR resource (.json)
    Resource,
}

/// Storage statistics
#[derive(Debug, Default, Clone)]
pub struct CasStats {
    /// Number of package tarballs
    pub package_count: usize,
    /// Number of individual resources
    pub resource_count: usize,
    /// Total storage size in bytes
    pub total_size: u64,
}

impl CasStats {
    /// Format total size as human-readable string
    pub fn human_readable_size(&self) -> String {
        let size = self.total_size;
        const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
        let mut value = size as f64;
        let mut unit_idx = 0;

        while value >= 1024.0 && unit_idx < UNITS.len() - 1 {
            value /= 1024.0;
            unit_idx += 1;
        }

        format!("{:.2} {}", value, UNITS[unit_idx])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_store_and_retrieve() {
        let temp_dir = TempDir::new().unwrap();
        let cas = CasStorage::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();

        let content = b"{\"resourceType\": \"Patient\"}";
        let hash = ContentHash::from_bytes(content);

        // Store
        let path = cas
            .store(content, &hash, ContentType::Resource)
            .await
            .unwrap();
        assert!(path.exists());

        // Retrieve
        let retrieved = cas.retrieve(&hash, ContentType::Resource).await.unwrap();
        assert_eq!(content, &retrieved[..]);
    }

    #[tokio::test]
    async fn test_exists() {
        let temp_dir = TempDir::new().unwrap();
        let cas = CasStorage::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();

        let content = b"test content";
        let hash = ContentHash::from_bytes(content);

        // Should not exist initially
        assert!(!cas.exists(&hash, ContentType::Resource).await);

        // Store content
        cas.store(content, &hash, ContentType::Resource)
            .await
            .unwrap();

        // Should exist now
        assert!(cas.exists(&hash, ContentType::Resource).await);
    }

    #[tokio::test]
    async fn test_idempotent_store() {
        let temp_dir = TempDir::new().unwrap();
        let cas = CasStorage::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();

        let content = b"test content";
        let hash = ContentHash::from_bytes(content);

        // Store twice
        let path1 = cas
            .store(content, &hash, ContentType::Resource)
            .await
            .unwrap();
        let path2 = cas
            .store(content, &hash, ContentType::Resource)
            .await
            .unwrap();

        // Should return same path
        assert_eq!(path1, path2);

        // Should only have one file
        let retrieved = cas.retrieve(&hash, ContentType::Resource).await.unwrap();
        assert_eq!(content, &retrieved[..]);
    }

    #[tokio::test]
    async fn test_delete() {
        let temp_dir = TempDir::new().unwrap();
        let cas = CasStorage::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();

        let content = b"test content";
        let hash = ContentHash::from_bytes(content);

        // Store and verify
        cas.store(content, &hash, ContentType::Resource)
            .await
            .unwrap();
        assert!(cas.exists(&hash, ContentType::Resource).await);

        // Delete
        cas.delete(&hash, ContentType::Resource).await.unwrap();
        assert!(!cas.exists(&hash, ContentType::Resource).await);
    }

    #[tokio::test]
    async fn test_stats() {
        let temp_dir = TempDir::new().unwrap();
        let cas = CasStorage::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();

        // Initially empty
        let stats = cas.stats().await.unwrap();
        assert_eq!(stats.package_count, 0);
        assert_eq!(stats.resource_count, 0);

        // Store some content
        let content1 = b"resource 1";
        let hash1 = ContentHash::from_bytes(content1);
        cas.store(content1, &hash1, ContentType::Resource)
            .await
            .unwrap();

        let content2 = b"package data";
        let hash2 = ContentHash::from_bytes(content2);
        cas.store(content2, &hash2, ContentType::Package)
            .await
            .unwrap();

        // Check stats
        let stats = cas.stats().await.unwrap();
        assert_eq!(stats.resource_count, 1);
        assert_eq!(stats.package_count, 1);
        assert!(stats.total_size > 0);
    }

    #[tokio::test]
    async fn test_content_type_separation() {
        let temp_dir = TempDir::new().unwrap();
        let cas = CasStorage::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();

        let content = b"same content, different types";
        let hash = ContentHash::from_bytes(content);

        // Store as both package and resource
        cas.store(content, &hash, ContentType::Package)
            .await
            .unwrap();
        cas.store(content, &hash, ContentType::Resource)
            .await
            .unwrap();

        // Both should exist
        assert!(cas.exists(&hash, ContentType::Package).await);
        assert!(cas.exists(&hash, ContentType::Resource).await);

        // Paths should be different
        let package_path = cas.path_for(&hash, ContentType::Package);
        let resource_path = cas.path_for(&hash, ContentType::Resource);
        assert_ne!(package_path, resource_path);
    }

    #[tokio::test]
    async fn test_human_readable_size() {
        let stats = CasStats {
            package_count: 10,
            resource_count: 100,
            total_size: 1_500_000, // ~1.43 MB
        };

        let size_str = stats.human_readable_size();
        assert!(size_str.contains("MB"));
    }

    #[tokio::test]
    async fn test_concurrent_stores() {
        let temp_dir = TempDir::new().unwrap();
        let cas = CasStorage::new(temp_dir.path().to_path_buf())
            .await
            .unwrap();

        let content = b"concurrent test";
        let hash = ContentHash::from_bytes(content);

        // Store concurrently (should be safe due to atomic writes)
        let cas1 = cas.clone();
        let cas2 = cas.clone();
        let hash1 = hash;
        let hash2 = hash;

        let handle1 =
            tokio::spawn(async move { cas1.store(content, &hash1, ContentType::Resource).await });

        let handle2 =
            tokio::spawn(async move { cas2.store(content, &hash2, ContentType::Resource).await });

        let (result1, result2) = tokio::join!(handle1, handle2);
        assert!(result1.unwrap().is_ok());
        assert!(result2.unwrap().is_ok());

        // Should only have one copy
        assert!(cas.exists(&hash, ContentType::Resource).await);
    }
}
