//! Unified storage system combining package management and optimized indexing
//!
//! This module provides a unified interface that combines the functionality of
//! BinaryStorage (for package management) and OptimizedIndexStorage (for indexing)
//! to provide a seamless high-performance storage solution.

use crate::binary_storage::{BinaryStorage, PackageInfo, ResourceIndex, StorageMetadata};
use crate::config::StorageConfig;
use crate::error::Result;
use crate::package::ExtractedPackage;
use crate::storage::optimized::ResourceIndex as OptimizedResourceIndex;
use crate::storage::optimized::{IndexData, IndexMetadata, OptimizedIndexStorage};
use std::sync::Arc;
use tracing::{debug, info, warn};

/// Unified storage system providing both package management and optimized indexing
pub struct UnifiedStorage {
    /// Package storage for managing FHIR packages
    package_storage: Arc<BinaryStorage>,
    /// Optimized index storage for high-performance search indexes
    index_storage: OptimizedIndexStorage,
    /// Configuration
    config: StorageConfig,
}

impl UnifiedStorage {
    /// Get access to the package storage
    pub fn package_storage(&self) -> &Arc<BinaryStorage> {
        &self.package_storage
    }

    /// Create a new unified storage system
    pub async fn new(
        config: StorageConfig,
        use_mmap: bool,
        compression_level: i32,
    ) -> Result<Self> {
        // Initialize package storage
        let package_storage = Arc::new(BinaryStorage::new(config.clone()).await?);

        // Initialize optimized index storage
        let index_dir = config.index_dir.join("optimized");
        let index_storage = OptimizedIndexStorage::new(index_dir)
            .with_mmap(use_mmap)
            .with_compression_level(compression_level);

        Ok(Self {
            package_storage,
            index_storage,
            config,
        })
    }

    /// Add a package to storage and update indexes
    pub async fn add_package(&self, package: &ExtractedPackage) -> Result<()> {
        let start_time = std::time::Instant::now();

        // Add to package storage
        self.package_storage.add_package(package).await?;

        // Build index data for this package
        let index_data = self.build_index_data_for_package(package).await?;

        // Store in optimized index storage
        self.index_storage.store_index(&index_data).await?;

        let duration = start_time.elapsed();
        info!(
            "Package {} added to unified storage in {:?}",
            package.name, duration
        );

        Ok(())
    }

    /// Remove a package from storage and indexes
    pub async fn remove_package(&self, name: &str, version: &str) -> Result<bool> {
        let start_time = std::time::Instant::now();

        // Remove from package storage
        let removed = self.package_storage.remove_package(name, version).await?;

        if removed {
            // TODO: Remove from optimized index storage
            // This would require index rebuilding or maintaining package->index mappings
            warn!("Package removed from storage, but index cleanup not yet implemented");
        }

        let duration = start_time.elapsed();
        debug!("Package removal completed in {:?}", duration);

        Ok(removed)
    }

    /// List all packages
    pub async fn list_packages(&self) -> Result<Vec<PackageInfo>> {
        self.package_storage.list_packages().await
    }

    /// Find a resource by canonical URL
    pub async fn find_resource(&self, canonical_url: &str) -> Result<Option<ResourceIndex>> {
        self.package_storage.find_resource(canonical_url).await
    }

    /// Get storage statistics combining both systems
    pub async fn get_unified_stats(&self) -> Result<UnifiedStorageStats> {
        let package_stats = self.package_storage.get_stats().await?;
        let index_metadata = self.get_index_metadata().await?;

        Ok(UnifiedStorageStats {
            package_stats,
            index_metadata,
            total_size_bytes: self.calculate_total_size().await?,
        })
    }

    /// Get optimized index metadata
    pub async fn get_index_metadata(&self) -> Result<Option<IndexMetadata>> {
        if self.index_storage.index_exists().await {
            Ok(Some(self.index_storage.load_metadata().await?))
        } else {
            Ok(None)
        }
    }

    /// Force a complete index rebuild
    pub async fn rebuild_index(&self) -> Result<()> {
        info!("Starting complete index rebuild in unified storage");
        let start_time = std::time::Instant::now();

        // Get all packages
        let packages = self.package_storage.list_packages().await?;

        // Build comprehensive index data
        let mut index_builder = IndexDataBuilder::new();

        for package_info in packages {
            let package_key = format!("{}:{}", package_info.name, package_info.version);
            debug!("Adding package to index: {}", package_key);

            // Get all resources for this package
            let resources = self.package_storage.get_cache_entries();
            for (_canonical_url, resource_index) in resources {
                if resource_index.package_name == package_info.name
                    && resource_index.package_version == package_info.version
                {
                    // Convert to optimized resource index
                    let optimized_resource = OptimizedResourceIndex {
                        id: resource_index.metadata.id.clone(),
                        resource_type: resource_index.resource_type,
                        canonical_url: Some(resource_index.canonical_url),
                        package_id: format!(
                            "{}:{}",
                            resource_index.package_name, resource_index.package_version
                        ),
                        file_path: resource_index.file_path,
                        checksum: [0u8; 32], // TODO: Compute actual checksum
                    };
                    index_builder.add_resource(optimized_resource);
                }
            }
        }

        let index_data = index_builder.build();
        self.index_storage.store_index(&index_data).await?;

        let duration = start_time.elapsed();
        info!(
            "Index rebuild completed in {:?} with {} resources",
            duration,
            index_data.resources.len()
        );

        Ok(())
    }

    /// Perform integrity check on both storage systems
    pub async fn integrity_check(&self) -> Result<UnifiedIntegrityReport> {
        let package_integrity = self.package_storage.integrity_check().await?;
        let index_integrity = self.index_storage.verify_integrity().await?;

        let recommendations =
            self.generate_integrity_recommendations(&package_integrity, index_integrity);

        Ok(UnifiedIntegrityReport {
            package_integrity,
            index_integrity_ok: index_integrity,
            recommendations,
        })
    }

    /// Compact both storage systems
    pub async fn compact(&self) -> Result<()> {
        info!("Starting unified storage compaction");

        // Compact package storage
        self.package_storage.compact().await?;

        // For index storage, we rebuild the index which effectively compacts it
        self.rebuild_index().await?;

        info!("Unified storage compaction completed");
        Ok(())
    }

    // Private helper methods

    async fn build_index_data_for_package(&self, package: &ExtractedPackage) -> Result<IndexData> {
        let mut builder = IndexDataBuilder::new();

        for resource in &package.resources {
            let optimized_resource_index = OptimizedResourceIndex {
                id: resource.id.clone(),
                resource_type: resource.resource_type.clone(),
                canonical_url: resource.url.clone(),
                package_id: format!("{}:{}", package.name, package.version),
                file_path: resource.file_path.clone(),
                checksum: [0u8; 32], // TODO: Compute actual checksum
            };

            builder.add_resource(optimized_resource_index);
        }

        Ok(builder.build())
    }

    async fn calculate_total_size(&self) -> Result<u64> {
        let mut total_size = 0u64;

        // Add package storage size
        if let Ok(metadata) = tokio::fs::metadata(&self.config.packages_dir).await {
            total_size += metadata.len();
        }

        // Add index storage size
        if let Ok(index_size) = self.index_storage.get_index_size().await {
            total_size += index_size;
        }

        Ok(total_size)
    }

    fn generate_integrity_recommendations(
        &self,
        package_integrity: &crate::binary_storage::IntegrityReport,
        index_integrity: bool,
    ) -> Vec<String> {
        let mut recommendations = Vec::new();

        if !package_integrity.is_healthy() {
            recommendations.push(
                "Package storage has integrity issues - consider running compact()".to_string(),
            );
        }

        if !index_integrity {
            recommendations.push(
                "Index storage integrity check failed - consider rebuilding index".to_string(),
            );
        }

        if package_integrity.issue_count() > 0 || !index_integrity {
            recommendations
                .push("Run integrity_check() regularly to monitor storage health".to_string());
        }

        recommendations
    }
}

#[async_trait::async_trait]
impl crate::traits::PackageStore for UnifiedStorage {
    async fn add_package(&self, package: &ExtractedPackage) -> crate::error::Result<()> {
        self.add_package(package).await
    }

    async fn remove_package(&self, name: &str, version: &str) -> crate::error::Result<bool> {
        self.remove_package(name, version).await
    }

    async fn find_resource(
        &self,
        canonical_url: &str,
    ) -> crate::error::Result<Option<ResourceIndex>> {
        self.find_resource(canonical_url).await
    }

    async fn list_packages(&self) -> crate::error::Result<Vec<PackageInfo>> {
        self.list_packages().await
    }
}

#[async_trait::async_trait]
impl crate::traits::IndexStore for UnifiedStorage {
    async fn store_index(
        &self,
        index: &crate::storage::optimized::IndexData,
    ) -> crate::error::Result<()> {
        self.index_storage.store_index(index).await
    }

    async fn load_metadata(
        &self,
    ) -> crate::error::Result<crate::storage::optimized::IndexMetadata> {
        self.get_index_metadata()
            .await?
            .ok_or_else(|| crate::error::FcmError::Generic("Index metadata not found".to_string()))
    }

    async fn verify_integrity(&self) -> crate::error::Result<bool> {
        self.index_storage.verify_integrity().await
    }
}

/// Helper for building index data
struct IndexDataBuilder {
    resources: Vec<OptimizedResourceIndex>,
}

impl IndexDataBuilder {
    fn new() -> Self {
        Self {
            resources: Vec::new(),
        }
    }

    fn add_resource(&mut self, resource: OptimizedResourceIndex) {
        self.resources.push(resource);
    }

    fn build(self) -> IndexData {
        let metadata = IndexMetadata {
            version: 1,
            created_at: std::time::SystemTime::now(),
            resource_count: self.resources.len(),
            compressed_size: 0,     // Will be updated when stored
            original_size: 0,       // Will be updated when stored
            compression_ratio: 0.0, // Will be updated when stored
        };

        IndexData {
            version: 1,
            resources: self.resources,
            metadata,
        }
    }
}

/// Combined storage statistics
#[derive(Debug, Clone)]
pub struct UnifiedStorageStats {
    pub package_stats: StorageMetadata,
    pub index_metadata: Option<IndexMetadata>,
    pub total_size_bytes: u64,
}

/// Combined integrity report
#[derive(Debug, Clone)]
pub struct UnifiedIntegrityReport {
    pub package_integrity: crate::binary_storage::IntegrityReport,
    pub index_integrity_ok: bool,
    pub recommendations: Vec<String>,
}

impl UnifiedIntegrityReport {
    pub fn is_healthy(&self) -> bool {
        self.package_integrity.is_healthy() && self.index_integrity_ok
    }

    pub fn total_issues(&self) -> usize {
        let mut issues = self.package_integrity.issue_count();
        if !self.index_integrity_ok {
            issues += 1;
        }
        issues
    }
}
