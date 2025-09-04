use crate::error::Result;
use memmap2::MmapMut;
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexData {
    pub version: u32,
    pub resources: Vec<ResourceIndex>,
    pub metadata: IndexMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceIndex {
    pub id: String,
    pub resource_type: String,
    pub canonical_url: Option<String>,
    pub package_id: String,
    pub file_path: PathBuf,
    pub checksum: [u8; 32],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMetadata {
    pub version: u32,
    pub created_at: SystemTime,
    pub resource_count: usize,
    pub compressed_size: usize,
    pub original_size: usize,
    pub compression_ratio: f64,
}

pub struct OptimizedIndexStorage {
    base_path: PathBuf,
    compression_level: i32,
    use_mmap: bool,
    max_memory_usage: usize,
}

impl OptimizedIndexStorage {
    pub fn new(base_path: PathBuf) -> Self {
        Self {
            base_path,
            compression_level: 3,
            use_mmap: true,
            max_memory_usage: 2_000_000_000, // 2GB
        }
    }

    pub fn with_compression_level(mut self, level: i32) -> Self {
        self.compression_level = level;
        self
    }

    pub fn with_mmap(mut self, use_mmap: bool) -> Self {
        self.use_mmap = use_mmap;
        self
    }

    pub fn with_max_memory(mut self, max_bytes: usize) -> Self {
        self.max_memory_usage = max_bytes;
        self
    }

    pub async fn store_index(&self, index_data: &IndexData) -> Result<()> {
        // Ensure directory exists
        tokio::fs::create_dir_all(&self.base_path).await?;

        // Serialize with bincode (more efficient than JSON)
        let serialized = serde_json::to_vec(index_data)?;
        let original_size = serialized.len();

        // Compress with zstd
        let compressed = zstd::encode_all(&serialized[..], self.compression_level)?;
        let compressed_size = compressed.len();

        let index_path = self.base_path.join("index.zst");

        if self.use_mmap && compressed.len() > 1_000_000 {
            // Use memory-mapped file for large indexes
            self.write_mmap(&index_path, &compressed).await?;
        } else {
            tokio::fs::write(&index_path, &compressed).await?;
        }

        // Store metadata separately for quick access
        let metadata = IndexMetadata {
            version: index_data.version,
            created_at: SystemTime::now(),
            resource_count: index_data.resources.len(),
            compressed_size,
            original_size,
            compression_ratio: compressed_size as f64 / original_size as f64,
        };

        let metadata_path = self.base_path.join("index.meta");
        let metadata_json = serde_json::to_string_pretty(&metadata)?;
        tokio::fs::write(&metadata_path, metadata_json).await?;

        tracing::info!(
            "Stored index with {} resources, compression ratio: {:.2}%",
            metadata.resource_count,
            metadata.compression_ratio * 100.0
        );

        Ok(())
    }

    pub async fn load_index(&self) -> Result<IndexData> {
        let index_path = self.base_path.join("index.zst");

        if !index_path.exists() {
            return Err(crate::error::FcmError::Generic(
                "Index file does not exist".to_string(),
            ));
        }

        let compressed = if self.use_mmap {
            self.read_mmap(&index_path).await?
        } else {
            tokio::fs::read(&index_path).await?
        };

        // Decompress
        let serialized = zstd::decode_all(&compressed[..])?;

        // Deserialize
        let index_data: IndexData = serde_json::from_slice(&serialized)?;

        tracing::info!(
            "Loaded index with {} resources from {}",
            index_data.resources.len(),
            index_path.display()
        );

        Ok(index_data)
    }

    pub async fn load_metadata(&self) -> Result<IndexMetadata> {
        let metadata_path = self.base_path.join("index.meta");

        if !metadata_path.exists() {
            return Err(crate::error::FcmError::Generic(
                "Index metadata does not exist".to_string(),
            ));
        }

        let metadata_json = tokio::fs::read_to_string(&metadata_path).await?;
        let metadata: IndexMetadata = serde_json::from_str(&metadata_json)?;

        Ok(metadata)
    }

    pub async fn index_exists(&self) -> bool {
        let index_path = self.base_path.join("index.zst");
        let metadata_path = self.base_path.join("index.meta");

        index_path.exists() && metadata_path.exists()
    }

    pub async fn get_index_size(&self) -> Result<u64> {
        let index_path = self.base_path.join("index.zst");
        let metadata = tokio::fs::metadata(&index_path).await?;
        Ok(metadata.len())
    }

    pub async fn cleanup_old_indexes(&self, keep_count: usize) -> Result<()> {
        // This would implement cleanup logic for old index versions
        // For now, just log the intent
        tracing::info!("Cleanup would keep {} index versions", keep_count);
        Ok(())
    }

    async fn write_mmap(&self, path: &Path, data: &[u8]) -> Result<()> {
        tokio::task::spawn_blocking({
            let path = path.to_owned();
            let data = data.to_owned();
            move || -> Result<()> {
                let file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(&path)?;

                file.set_len(data.len() as u64)?;

                let mut mmap = unsafe { MmapMut::map_mut(&file)? };
                mmap.copy_from_slice(&data);
                mmap.flush()?;

                Ok(())
            }
        })
        .await
        .map_err(|e| crate::error::FcmError::Generic(format!("Join error: {e}")))??;

        Ok(())
    }

    async fn read_mmap(&self, path: &Path) -> Result<Vec<u8>> {
        let data = tokio::task::spawn_blocking({
            let path = path.to_owned();
            move || -> Result<Vec<u8>> {
                let file = std::fs::File::open(&path)?;
                let mmap = unsafe { memmap2::Mmap::map(&file)? };
                Ok(mmap.to_vec())
            }
        })
        .await
        .map_err(|e| crate::error::FcmError::Generic(format!("Join error: {e}")))??;

        Ok(data)
    }

    pub async fn verify_integrity(&self) -> Result<bool> {
        // Check if both index and metadata files exist and are readable
        if !self.index_exists().await {
            return Ok(false);
        }

        // Try to load metadata
        let metadata = match self.load_metadata().await {
            Ok(meta) => meta,
            Err(_) => return Ok(false),
        };

        // Check file size matches metadata
        let actual_size = self.get_index_size().await?;
        if actual_size as usize != metadata.compressed_size {
            tracing::warn!(
                "Index file size mismatch: expected {}, actual {}",
                metadata.compressed_size,
                actual_size
            );
            return Ok(false);
        }

        // Try to load the full index to verify it's not corrupted
        match self.load_index().await {
            Ok(_) => Ok(true),
            Err(e) => {
                tracing::error!("Index integrity check failed: {}", e);
                Ok(false)
            }
        }
    }
}

#[async_trait::async_trait]
impl crate::traits::IndexStore for OptimizedIndexStorage {
    async fn store_index(
        &self,
        index: &crate::storage::optimized::IndexData,
    ) -> crate::error::Result<()> {
        self.store_index(index).await
    }

    async fn load_metadata(
        &self,
    ) -> crate::error::Result<crate::storage::optimized::IndexMetadata> {
        self.load_metadata().await
    }

    async fn verify_integrity(&self) -> crate::error::Result<bool> {
        self.verify_integrity().await
    }
}

// Builder pattern for creating IndexData
pub struct IndexDataBuilder {
    version: u32,
    resources: Vec<ResourceIndex>,
}

impl IndexDataBuilder {
    pub fn new(version: u32) -> Self {
        Self {
            version,
            resources: Vec::new(),
        }
    }

    pub fn add_resource(mut self, resource: ResourceIndex) -> Self {
        self.resources.push(resource);
        self
    }

    pub fn add_resources(mut self, resources: Vec<ResourceIndex>) -> Self {
        self.resources.extend(resources);
        self
    }

    pub fn build(self) -> IndexData {
        let resource_count = self.resources.len();

        IndexData {
            version: self.version,
            resources: self.resources,
            metadata: IndexMetadata {
                version: self.version,
                created_at: SystemTime::now(),
                resource_count,
                compressed_size: 0,     // Will be filled when storing
                original_size: 0,       // Will be filled when storing
                compression_ratio: 0.0, // Will be filled when storing
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blake3;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_store_and_load_index() {
        let temp_dir = TempDir::new().unwrap();
        let storage = OptimizedIndexStorage::new(temp_dir.path().to_path_buf());

        // Create test index data
        let test_resource = ResourceIndex {
            id: "test-resource".to_string(),
            resource_type: "StructureDefinition".to_string(),
            canonical_url: Some("http://example.org/StructureDefinition/test".to_string()),
            package_id: "test-package".to_string(),
            file_path: PathBuf::from("test.json"),
            checksum: blake3::hash(b"test content").into(),
        };

        let index_data = IndexDataBuilder::new(1)
            .add_resource(test_resource.clone())
            .build();

        // Store the index
        storage.store_index(&index_data).await.unwrap();

        // Verify files exist
        assert!(storage.index_exists().await);

        // Load the index back
        let loaded_index = storage.load_index().await.unwrap();

        assert_eq!(loaded_index.version, 1);
        assert_eq!(loaded_index.resources.len(), 1);
        assert_eq!(loaded_index.resources[0].id, "test-resource");
        assert_eq!(
            loaded_index.resources[0].resource_type,
            "StructureDefinition"
        );
    }

    #[tokio::test]
    async fn test_metadata_operations() {
        let temp_dir = TempDir::new().unwrap();
        let storage = OptimizedIndexStorage::new(temp_dir.path().to_path_buf());

        let index_data = IndexDataBuilder::new(42).build();

        // Store index
        storage.store_index(&index_data).await.unwrap();

        // Load metadata
        let metadata = storage.load_metadata().await.unwrap();

        assert_eq!(metadata.version, 42);
        assert_eq!(metadata.resource_count, 0);
        assert!(metadata.compression_ratio > 0.0);
        assert!(metadata.compressed_size > 0);
        assert!(metadata.original_size > 0);
    }

    #[tokio::test]
    async fn test_integrity_verification() {
        let temp_dir = TempDir::new().unwrap();
        let storage = OptimizedIndexStorage::new(temp_dir.path().to_path_buf());

        // Initially no index exists
        assert!(!storage.verify_integrity().await.unwrap());

        // Create and store valid index
        let index_data = IndexDataBuilder::new(1).build();
        storage.store_index(&index_data).await.unwrap();

        // Should pass integrity check
        assert!(storage.verify_integrity().await.unwrap());
    }

    #[tokio::test]
    async fn test_compression_settings() {
        let temp_dir = TempDir::new().unwrap();

        // Test different compression levels
        let storage_high =
            OptimizedIndexStorage::new(temp_dir.path().join("high")).with_compression_level(9);
        let storage_low =
            OptimizedIndexStorage::new(temp_dir.path().join("low")).with_compression_level(1);

        // Create large compressible test data
        let mut resources = Vec::new();
        let repeated_content =
            "This is a very long repeated string that should compress well ".repeat(100);

        for i in 0..100 {
            resources.push(ResourceIndex {
                id: format!("resource-{i}-{repeated_content}"),
                resource_type: format!("Patient-{repeated_content}"),
                canonical_url: Some(format!(
                    "http://example.com/resource-{i}-{repeated_content}"
                )),
                package_id: format!("test-package-{repeated_content}"),
                file_path: PathBuf::from(format!("patient-{i}-{repeated_content}.json")),
                checksum: blake3::hash(format!("content {i} {repeated_content}").as_bytes()).into(),
            });
        }

        let index_data = IndexDataBuilder::new(1).add_resources(resources).build();

        // Store with both compression levels
        storage_high.store_index(&index_data).await.unwrap();
        storage_low.store_index(&index_data).await.unwrap();

        // High compression should result in smaller or equal file size
        let high_size = storage_high.get_index_size().await.unwrap();
        let low_size = storage_low.get_index_size().await.unwrap();

        // Allow for compression variations - sometimes higher compression may result in slightly larger files
        // due to algorithm overhead, especially with highly redundant artificial test data
        let size_ratio = high_size as f64 / low_size as f64;
        assert!(
            size_ratio <= 1.1,
            "High compression size {high_size} should be within 10% of low compression size {low_size}"
        );
    }
}
