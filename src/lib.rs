//! # FHIR Canonical Manager
//!
//! A library-first solution for managing FHIR Implementation Guide packages,
//! providing fast canonical URL resolution and resource search capabilities.
//!
//! ## Quick Start
//!
//! ```rust,no_run
//! use octofhir_canonical_manager::{CanonicalManager, FcmConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = FcmConfig::load()?;
//!     let manager = CanonicalManager::new(config).await?;
//!     
//!     // Install a package
//!     manager.install_package("hl7.fhir.us.core", "6.1.0").await?;
//!     
//!     // Resolve a canonical URL
//!     let resource = manager.resolve("http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient").await?;
//!     
//!     Ok(())
//! }
//! ```

pub mod config;
pub mod error;
pub mod output;
pub mod package;
pub mod registry;
pub mod resolver;
pub mod search;
pub mod storage;

#[cfg(feature = "cli")]
pub mod cli;

#[cfg(feature = "cli")]
pub mod cli_error;

// Re-export main types
pub use config::{FcmConfig, PackageSpec, RegistryConfig, StorageConfig};
pub use error::{FcmError, Result};
pub use resolver::CanonicalResolver;
pub use search::SearchEngine;

use std::collections::HashSet;
use std::sync::Arc;
use tokio::fs;
use tracing::info;

#[cfg(feature = "cli")]
use crate::output::Progress;

#[cfg(feature = "cli")]
use crate::cli::ProgressContext;

/// Main FHIR Canonical Manager
///
/// Provides high-level interface for managing FHIR packages and resolving canonical URLs.
pub struct CanonicalManager {
    config: FcmConfig,
    storage: Arc<storage::IndexedStorage>,
    registry_client: registry::RegistryClient,
    resolver: CanonicalResolver,
    search_engine: SearchEngine,
}

impl CanonicalManager {
    /// Create a new CanonicalManager with the given configuration
    pub async fn new(config: FcmConfig) -> Result<Self> {
        info!("Initializing FHIR Canonical Manager");

        // Initialize storage
        let storage = Arc::new(storage::IndexedStorage::new(config.storage.clone()).await?);

        // Initialize registry client
        let expanded_storage = config.get_expanded_storage_config();
        let registry_client =
            registry::RegistryClient::new(&config.registry, expanded_storage.cache_dir.clone())
                .await?;

        // Initialize resolver
        let resolver = CanonicalResolver::new(Arc::clone(&storage));

        // Initialize search engine
        let search_engine = SearchEngine::new(Arc::clone(&storage));

        Ok(Self {
            config,
            storage,
            registry_client,
            resolver,
            search_engine,
        })
    }

    /// Install a FHIR package by name and version
    pub async fn install_package(&self, name: &str, version: &str) -> Result<()> {
        let mut installed = HashSet::new();
        self.install_package_with_dependencies(name, version, &mut installed)
            .await?;

        // Rebuild index to include newly installed packages
        info!("Rebuilding index to include newly installed packages...");
        self.rebuild_index().await?;

        Ok(())
    }

    /// Count total packages that will be installed (including dependencies)
    #[cfg(feature = "cli")]
    pub async fn count_packages_to_install(&self, name: &str, version: &str) -> Result<usize> {
        let mut visited = HashSet::new();
        self.count_packages_recursive(name, version, &mut visited)
            .await
    }

    /// Recursively count packages
    #[cfg(feature = "cli")]
    fn count_packages_recursive<'a>(
        &'a self,
        name: &'a str,
        version: &'a str,
        visited: &'a mut HashSet<String>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<usize>> + 'a>> {
        Box::pin(async move {
            let package_key = format!("{name}@{version}");

            if visited.contains(&package_key) {
                return Ok(0);
            }

            visited.insert(package_key);
            let mut total = 1; // Count this package

            // Get dependencies
            let metadata = self
                .registry_client
                .get_package_metadata(name, version)
                .await?;

            // Recursively count dependencies
            for (dep_name, dep_version) in metadata.dependencies {
                total += self
                    .count_packages_recursive(&dep_name, &dep_version, visited)
                    .await?;
            }

            Ok(total)
        })
    }

    /// Install a package with progress tracking (CLI feature)
    #[cfg(feature = "cli")]
    pub async fn install_package_with_progress(
        &self,
        name: &str,
        version: &str,
        progress: &mut ProgressContext,
    ) -> Result<()> {
        let mut installed = HashSet::new();
        self.install_package_with_dependencies_and_progress(
            name,
            version,
            &mut installed,
            progress,
        )
        .await?;

        // Rebuild index to include newly installed packages
        info!("Rebuilding index to include newly installed packages...");
        self.rebuild_index().await?;

        Ok(())
    }

    /// Install a package with recursive dependency resolution
    fn install_package_with_dependencies<'a>(
        &'a self,
        name: &'a str,
        version: &'a str,
        installed: &'a mut HashSet<String>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + 'a>> {
        Box::pin(async move {
            let package_key = format!("{name}@{version}");

            // Check if already installed to avoid cycles and duplicates
            if installed.contains(&package_key) {
                info!("Package already processed: {}", package_key);
                return Ok(());
            }

            info!("Installing package: {}@{}", name, version);

            let spec = PackageSpec {
                name: name.to_string(),
                version: version.to_string(),
                priority: 1,
            };

            // Download package and get metadata
            let download = self.registry_client.download_package(&spec).await?;
            let dependencies = download.metadata.dependencies.clone();

            // Extract package
            let extractor = package::PackageExtractor::new(self.config.storage.cache_dir.clone());
            let extracted = extractor.extract_package(download).await?;

            // Move extracted package to packages directory
            let package_dir = self
                .config
                .storage
                .packages_dir
                .join(format!("{name}-{version}"));
            if package_dir.exists() {
                fs::remove_dir_all(&package_dir).await?;
            }
            fs::rename(&extracted.extraction_path, &package_dir).await?;

            // Mark as installed
            installed.insert(package_key.clone());

            // Install dependencies recursively
            for (dep_name, dep_version) in dependencies {
                info!("Installing dependency: {}@{}", dep_name, dep_version);
                self.install_package_with_dependencies(&dep_name, &dep_version, installed)
                    .await?;
            }

            // Note: Package indexing happens during rebuild_index or explicit index updates
            // Individual packages are indexed when the full index is rebuilt

            info!("Package installed successfully: {}", package_key);
            Ok(())
        })
    }

    /// Install a package with recursive dependency resolution and progress tracking
    #[cfg(feature = "cli")]
    fn install_package_with_dependencies_and_progress<'a>(
        &'a self,
        name: &'a str,
        version: &'a str,
        installed: &'a mut HashSet<String>,
        progress: &'a mut ProgressContext,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + 'a>> {
        Box::pin(async move {
            let package_key = format!("{name}@{version}");

            // Check if already installed to avoid cycles and duplicates
            if installed.contains(&package_key) {
                info!("Package already processed: {}", package_key);
                return Ok(());
            }

            // Create stream progress for this package
            let stream_progress = progress.create_stream_progress();

            info!("Installing package: {}@{}", name, version);
            #[cfg(feature = "cli")]
            Progress::step(&format!("Downloading {package_key}"));

            let spec = PackageSpec {
                name: name.to_string(),
                version: version.to_string(),
                priority: 1,
            };

            // Download package with streaming progress
            let download = self
                .registry_client
                .download_package_with_progress(&spec, Some(&stream_progress))
                .await?;
            let dependencies = download.metadata.dependencies.clone();

            // Extract package
            #[cfg(feature = "cli")]
            Progress::step(&format!("Extracting {package_key}"));
            let extractor = package::PackageExtractor::new(self.config.storage.cache_dir.clone());
            let extracted = extractor.extract_package(download).await?;

            // Move extracted package to packages directory
            let package_dir = self
                .config
                .storage
                .packages_dir
                .join(format!("{name}-{version}"));
            if package_dir.exists() {
                fs::remove_dir_all(&package_dir).await?;
            }
            fs::rename(&extracted.extraction_path, &package_dir).await?;

            // Mark as installed
            installed.insert(package_key.clone());

            // Update progress
            progress.increment(&format!("Completed {package_key}"));

            // Install dependencies recursively
            for (dep_name, dep_version) in dependencies {
                info!("Installing dependency: {}@{}", dep_name, dep_version);
                self.install_package_with_dependencies_and_progress(
                    &dep_name,
                    &dep_version,
                    installed,
                    progress,
                )
                .await?;
            }

            // Note: Package indexing happens during rebuild_index or explicit index updates
            // Individual packages are indexed when the full index is rebuilt

            info!("Package installed successfully: {}", package_key);
            Ok(())
        })
    }

    /// Resolve a canonical URL to a FHIR resource
    pub async fn resolve(&self, canonical_url: &str) -> Result<resolver::ResolvedResource> {
        self.resolver.resolve(canonical_url).await
    }

    /// Search for FHIR resources
    pub async fn search(&self) -> search::SearchQueryBuilder {
        search::SearchQueryBuilder::new(Arc::clone(&self.storage))
    }

    /// Get direct access to the search engine
    pub fn search_engine(&self) -> &search::SearchEngine {
        &self.search_engine
    }

    /// List installed packages
    pub async fn list_packages(&self) -> Result<Vec<String>> {
        let packages = self.storage.list_packages().await?;
        Ok(packages
            .into_iter()
            .map(|p| format!("{}@{}", p.name, p.version))
            .collect())
    }

    /// Rebuild the search index from existing packages
    pub async fn rebuild_index(&self) -> Result<()> {
        // We'll need to use interior mutability for this
        // For now, let's create a new storage instance to rebuild from
        let config = self.config.clone();
        let mut temp_storage = storage::IndexedStorage::new(config.storage).await?;
        temp_storage.rebuild_index().await
    }

    /// Remove a FHIR package by name and version
    pub async fn remove_package(&self, name: &str, version: &str) -> Result<()> {
        info!("Removing package: {}@{}", name, version);

        // Create a temporary storage instance to perform the removal
        let config = self.config.clone();
        let mut temp_storage = storage::IndexedStorage::new(config.storage).await?;
        temp_storage.remove_package(name, version).await?;

        info!("Package removed successfully: {}@{}", name, version);
        Ok(())
    }

    /// Batch resolve multiple canonical URLs
    pub async fn batch_resolve(&self, urls: &[String]) -> Result<Vec<resolver::ResolvedResource>> {
        info!("Batch resolving {} URLs", urls.len());

        let mut results = Vec::new();
        for url in urls {
            match self.resolve(url).await {
                Ok(resolved) => results.push(resolved),
                Err(e) => {
                    // For batch operations, we continue processing even if one fails
                    tracing::warn!("Failed to resolve {}: {}", url, e);
                }
            }
        }

        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_canonical_manager_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = FcmConfig::test_config(temp_dir.path());

        let manager = CanonicalManager::new(config).await;
        assert!(manager.is_ok());
    }
}
