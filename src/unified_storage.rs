//! Unified storage system using SQLite for package management
//!
//! This module provides a simplified interface that wraps SqliteStorage.
//! SQLite's built-in B-tree indexes eliminate the need for a separate index storage.

use crate::config::StorageConfig;
use crate::error::Result;
use crate::package::ExtractedPackage;
use crate::sqlite_storage::{
    IntegrityReport, PackageInfo, ResourceIndex, SqliteStorage, StorageStats,
};
use std::sync::Arc;
use tracing::info;

/// Unified storage system using SQLite for all storage needs
pub struct UnifiedStorage {
    /// Package storage for managing FHIR packages and resources
    package_storage: Arc<SqliteStorage>,
}

impl UnifiedStorage {
    /// Get access to the package storage
    pub fn package_storage(&self) -> &Arc<SqliteStorage> {
        &self.package_storage
    }

    /// Create a new unified storage system
    pub async fn new(config: StorageConfig) -> Result<Self> {
        // Initialize SQLite storage (handles indexing, compression, and memory mapping automatically)
        let package_storage = Arc::new(SqliteStorage::new(config.clone()).await?);

        Ok(Self { package_storage })
    }

    /// Add a package to storage
    pub async fn add_package(&self, package: &ExtractedPackage) -> Result<()> {
        let package_name = package.name.clone();

        // Add to SQLite storage (indexes are automatically maintained)
        // Clone the package to pass ownership
        self.package_storage.add_package(package.clone()).await?;

        info!("Package {} added to storage", package_name);

        Ok(())
    }

    /// Add multiple packages to storage in a single batch operation.
    ///
    /// This is significantly more efficient than calling `add_package()` multiple times
    /// as it uses a single database transaction for all packages.
    ///
    /// # Arguments
    ///
    /// * `packages` - Vector of extracted packages to add
    ///
    /// # Performance
    ///
    /// Expected speedup: ~3.6x for 10-20 packages compared to individual inserts
    pub async fn add_packages_batch(&self, packages: Vec<ExtractedPackage>) -> Result<()> {
        let package_count = packages.len();

        // Delegate to SQLite storage batch operation
        self.package_storage.add_packages_batch(packages).await?;

        info!("Batch added {} packages to storage", package_count);

        Ok(())
    }

    /// Remove a package from storage
    pub async fn remove_package(&self, name: &str, version: &str) -> Result<bool> {
        // Remove from SQLite storage (indexes are automatically maintained)
        self.package_storage.remove_package(name, version).await
    }

    /// List all packages
    pub async fn list_packages(&self) -> Result<Vec<PackageInfo>> {
        self.package_storage.list_packages().await
    }

    /// Find a resource by canonical URL
    pub async fn find_resource(&self, canonical_url: &str) -> Result<Option<ResourceIndex>> {
        self.package_storage.find_resource(canonical_url).await
    }

    /// Get storage statistics
    pub async fn get_unified_stats(&self) -> Result<UnifiedStorageStats> {
        let package_stats = self.package_storage.get_stats().await?;

        Ok(UnifiedStorageStats {
            package_stats,
            total_size_bytes: self.calculate_total_size().await?,
        })
    }

    /// Rebuild index (no-op with SQLite - indexes are automatically maintained)
    pub async fn rebuild_index(&self) -> Result<()> {
        info!("Index rebuild requested (no-op with SQLite - indexes auto-maintained)");
        Ok(())
    }

    /// Perform integrity check
    pub async fn integrity_check(&self) -> Result<UnifiedIntegrityReport> {
        let package_integrity = self.package_storage.integrity_check().await?;

        let recommendations = self.generate_integrity_recommendations(&package_integrity);

        Ok(UnifiedIntegrityReport {
            package_integrity,
            recommendations,
        })
    }

    /// Compact storage
    pub async fn compact(&self) -> Result<()> {
        info!("Starting storage compaction");
        self.package_storage.compact().await?;
        info!("Storage compaction completed");
        Ok(())
    }

    // Private helper methods

    async fn calculate_total_size(&self) -> Result<u64> {
        let stats = self.package_storage.get_stats().await?;
        Ok(stats.storage_size_bytes)
    }

    fn generate_integrity_recommendations(
        &self,
        package_integrity: &IntegrityReport,
    ) -> Vec<String> {
        let mut recommendations = Vec::new();

        if !package_integrity.is_valid {
            recommendations
                .push("Storage has integrity issues - consider running compact()".to_string());
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

/// Storage statistics
#[derive(Debug, Clone)]
pub struct UnifiedStorageStats {
    pub package_stats: StorageStats,
    pub total_size_bytes: u64,
}

/// Integrity report
#[derive(Debug, Clone)]
pub struct UnifiedIntegrityReport {
    pub package_integrity: IntegrityReport,
    pub recommendations: Vec<String>,
}

impl UnifiedIntegrityReport {
    pub fn is_healthy(&self) -> bool {
        self.package_integrity.is_valid
    }

    pub fn total_issues(&self) -> usize {
        self.package_integrity.errors.len() + self.package_integrity.warnings.len()
    }
}
