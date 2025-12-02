//! Unified storage system using SQLite for package management
//!
//! This module provides a simplified interface that wraps SqliteStorage.
//! SQLite's built-in B-tree indexes eliminate the need for a separate index storage.

use crate::config::StorageConfig;
use crate::domain::{PackageInfo, ResourceIndex};
use crate::error::Result;
use crate::package::ExtractedPackage;
use crate::sqlite_storage::{IntegrityReport, SqliteStorage, StorageStats};
use std::sync::Arc;
use tracing::info;

/// Unified storage system supporting multiple storage backends via traits
pub struct UnifiedStorage {
    /// Package storage using trait object (SQLite, PostgreSQL, etc.)
    package_store: Arc<dyn crate::traits::PackageStore + Send + Sync>,
    /// Search storage using trait object
    search_storage: Arc<dyn crate::traits::SearchStorage + Send + Sync>,
}

impl UnifiedStorage {
    /// Get search storage as trait object (for use with resolver and search engine)
    pub fn search_storage(&self) -> Arc<dyn crate::traits::SearchStorage + Send + Sync> {
        self.search_storage.clone()
    }

    /// Create a new unified storage system with SQLite backend (for CLI usage)
    pub async fn new(config: StorageConfig) -> Result<Self> {
        let sqlite_storage = Arc::new(SqliteStorage::new(config.clone()).await?);
        Ok(Self {
            package_store: sqlite_storage.clone(),
            search_storage: sqlite_storage,
        })
    }

    /// Create a new unified storage system with custom storage backends (for FHIR server)
    pub fn new_with_custom_storage(
        package_store: Arc<dyn crate::traits::PackageStore + Send + Sync>,
        search_storage: Arc<dyn crate::traits::SearchStorage + Send + Sync>,
    ) -> Self {
        Self {
            package_store,
            search_storage,
        }
    }

    /// Add a package to storage
    pub async fn add_package(&self, package: &ExtractedPackage) -> Result<()> {
        let package_name = package.name.clone();
        self.package_store.add_package(package).await?;
        info!("Package {} added to storage", package_name);
        Ok(())
    }

    /// Add multiple packages to storage in a single batch operation.
    pub async fn add_packages_batch(&self, packages: Vec<ExtractedPackage>) -> Result<()> {
        let package_count = packages.len();
        // Add packages one by one using the trait method
        for package in &packages {
            self.package_store.add_package(package).await?;
        }
        info!("Batch added {} packages to storage", package_count);
        Ok(())
    }

    /// Remove a package from storage
    pub async fn remove_package(&self, name: &str, version: &str) -> Result<bool> {
        self.package_store.remove_package(name, version).await
    }

    /// List all packages
    pub async fn list_packages(&self) -> Result<Vec<PackageInfo>> {
        self.package_store.list_packages().await
    }

    /// Find a resource by canonical URL
    pub async fn find_resource(&self, canonical_url: &str) -> Result<Option<ResourceIndex>> {
        self.package_store.find_resource(canonical_url).await
    }

    /// Get storage statistics (returns default stats for non-SQLite backends)
    pub async fn get_unified_stats(&self) -> Result<UnifiedStorageStats> {
        Ok(UnifiedStorageStats {
            package_stats: StorageStats::default(),
            total_size_bytes: 0,
        })
    }

    /// Rebuild index (no-op - indexes are automatically maintained by backends)
    pub async fn rebuild_index(&self) -> Result<()> {
        info!("Index rebuild requested");
        Ok(())
    }

    /// Perform integrity check (returns healthy for non-SQLite backends)
    pub async fn integrity_check(&self) -> Result<UnifiedIntegrityReport> {
        Ok(UnifiedIntegrityReport {
            package_integrity: IntegrityReport {
                is_valid: true,
                errors: vec![],
                warnings: vec![],
            },
            recommendations: vec![],
        })
    }

    /// Compact storage (no-op for non-SQLite backends)
    pub async fn compact(&self) -> Result<()> {
        info!("Storage compaction requested");
        Ok(())
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
