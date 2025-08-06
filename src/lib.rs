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

pub mod binary_storage;
pub mod change_detection;
pub mod config;
pub mod config_validator;
pub mod error;
pub mod incremental_indexer;
pub mod output;
pub mod package;
pub mod parallel_processor;
pub mod performance;
pub mod rebuild_strategy;
pub mod registry;
pub mod resolver;
pub mod search;
pub mod storage;
pub mod unified_storage;

#[cfg(feature = "cli")]
pub mod cli;

#[cfg(feature = "cli")]
pub mod cli_error;

// Re-export main types
pub use unified_storage::{UnifiedIntegrityReport, UnifiedStorage, UnifiedStorageStats};
// Re-export selected types from binary_storage for compatibility
pub use binary_storage::{IntegrityReport, PackageInfo, ResourceIndex, ResourceMetadata};
pub use change_detection::{ChangeSet, PackageChangeDetector, PackageChecksum};
pub use config::{FcmConfig, OptimizationConfig, PackageSpec, RegistryConfig, StorageConfig};
pub use config_validator::{ConfigValidator, PerformanceImpact, ValidationResult};
pub use error::{FcmError, Result};
pub use incremental_indexer::{FhirResource, IncrementalIndexer, IndexStats};
pub use parallel_processor::{ParallelPackageProcessor, ProcessingReport};
pub use performance::{
    IndexOperationType, PackageOperationType, PerformanceAnalysis, PerformanceConfig,
    PerformanceMetrics, PerformanceMonitor,
};
pub use rebuild_strategy::{RebuildStrategy, SmartRebuildStrategy};
pub use resolver::CanonicalResolver;
pub use search::SearchEngine;
pub use storage::{IndexData, IndexDataBuilder, OptimizedIndexStorage};

use std::collections::HashSet;
use std::sync::Arc;
use tantivy::{Index, schema::*};
use tokio::fs;
use tracing::{debug, info, warn};

#[cfg(feature = "cli")]
use crate::output::Progress;

#[cfg(feature = "cli")]
use crate::cli::ProgressContext;

/// Performance metrics for optimization components
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OptimizationMetrics {
    pub change_detector_cache_size: usize,
    pub incremental_updates_count: usize,
    pub parallel_processing_efficiency: f64,
    pub average_rebuild_time: std::time::Duration,
    pub storage_compression_ratio: f64,
}

/// Status of package changes
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PackageChangeStatus {
    pub package_id: String,
    pub has_changes: bool,
    pub added_files: usize,
    pub modified_files: usize,
    pub removed_files: usize,
}

/// Information about a FHIR SearchParameter
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SearchParameterInfo {
    pub name: String,
    pub code: String,
    pub base: Vec<String>,
    pub type_field: String,
    pub description: Option<String>,
    pub expression: Option<String>,
    pub xpath: Option<String>,
    pub url: Option<String>,
    pub status: Option<String>,
}

impl SearchParameterInfo {
    /// Create from a FHIR resource
    pub fn from_resource(resource: &package::FhirResource) -> Result<Self> {
        let content = &resource.content;

        Ok(Self {
            name: content
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string(),
            code: content
                .get("code")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string(),
            base: content
                .get("base")
                .and_then(|v| v.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
                .or_else(|| {
                    content
                        .get("base")
                        .and_then(|v| v.as_str())
                        .map(|s| vec![s.to_string()])
                })
                .unwrap_or_default(),
            type_field: content
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("string")
                .to_string(),
            description: content
                .get("description")
                .and_then(|v| v.as_str())
                .map(String::from),
            expression: content
                .get("expression")
                .and_then(|v| v.as_str())
                .map(String::from),
            xpath: content
                .get("xpath")
                .and_then(|v| v.as_str())
                .map(String::from),
            url: content
                .get("url")
                .and_then(|v| v.as_str())
                .map(String::from),
            status: content
                .get("status")
                .and_then(|v| v.as_str())
                .map(String::from),
        })
    }
}

/// Main FHIR Canonical Manager
///
/// Provides high-level interface for managing FHIR packages and resolving canonical URLs.
pub struct CanonicalManager {
    config: FcmConfig,
    storage: Arc<UnifiedStorage>, // Optimized unified storage system
    registry_client: registry::RegistryClient,
    resolver: CanonicalResolver,
    search_engine: SearchEngine,
    // Optimization components
    change_detector: Arc<PackageChangeDetector>,
    rebuild_strategy: SmartRebuildStrategy,
    incremental_indexer: Arc<IncrementalIndexer>,
    parallel_processor: Arc<ParallelPackageProcessor>,
    // Performance tracking
    performance_monitor: Arc<PerformanceMonitor>,
    tantivy_index: Arc<Index>,
}

impl CanonicalManager {
    /// Create a new CanonicalManager with the given configuration
    pub async fn new(mut config: FcmConfig) -> Result<Self> {
        info!("Initializing FHIR Canonical Manager with optimization components");

        // Validate and optimize configuration before initialization
        let validation = crate::config_validator::ConfigValidator::optimize_config(&mut config);

        if !validation.is_valid {
            for error in &validation.errors {
                tracing::error!("Configuration error: {}", error);
            }
            return Err(crate::error::FcmError::Config(
                crate::error::ConfigError::ValidationFailed {
                    message: "Invalid optimization configuration".to_string(),
                },
            ));
        }

        // Log configuration warnings and recommendations
        for warning in &validation.warnings {
            warn!("Configuration warning: {}", warning);
        }

        for recommendation in &validation.recommendations {
            info!("Configuration recommendation: {}", recommendation);
        }

        // Log performance impact estimates
        info!("Expected performance improvements:");
        info!(
            "  - Incremental speedup: {:.1}x",
            validation.performance_impact.incremental_speedup_estimate
        );
        info!(
            "  - Parallel efficiency: {:.1}%",
            validation.performance_impact.parallel_efficiency_estimate * 100.0
        );
        info!(
            "  - Memory usage: {}",
            validation.performance_impact.memory_usage_estimate
        );
        info!(
            "  - Disk usage: {}",
            validation.performance_impact.disk_usage_estimate
        );

        // Initialize unified storage system
        let storage = Arc::new(
            UnifiedStorage::new(
                config.storage.clone(),
                config.optimization.use_mmap,
                config.optimization.compression_level,
            )
            .await?,
        );

        // Initialize registry client
        let expanded_storage = config.get_expanded_storage_config();
        let registry_client =
            registry::RegistryClient::new(&config.registry, expanded_storage.cache_dir.clone())
                .await?;

        // Initialize resolver with unified storage
        let resolver = CanonicalResolver::new(Arc::clone(storage.package_storage()));

        // Initialize search engine with unified storage
        let search_engine = SearchEngine::new(Arc::clone(storage.package_storage()));

        // Initialize optimization components based on config
        let change_detector = Arc::new(PackageChangeDetector::new());

        // Initialize smart rebuild strategy
        let rebuild_strategy = SmartRebuildStrategy::new(Arc::clone(&change_detector))
            .with_full_rebuild_threshold(config.optimization.full_rebuild_threshold)
            .with_batch_size(config.optimization.incremental_batch_size);

        // Create Tantivy index for full-text search
        let tantivy_index = Arc::new(Self::create_search_index()?);

        // Initialize incremental indexer
        let incremental_indexer = Arc::new(IncrementalIndexer::new(
            Arc::clone(&tantivy_index),
            Arc::clone(&change_detector),
            config.optimization.batch_size,
        ));

        // Initialize parallel processor
        let parallel_processor = Arc::new(ParallelPackageProcessor::new(
            config.optimization.parallel_workers,
            Arc::clone(&incremental_indexer),
        ));

        // Initialize performance monitor
        let perf_config = PerformanceConfig {
            enable_metrics: config.optimization.enable_metrics,
            metrics_interval: std::time::Duration::from_secs(30),
            max_samples: 1000,
            enable_detailed_logging: config.optimization.enable_metrics,
        };
        let performance_monitor = Arc::new(PerformanceMonitor::new(perf_config));

        info!("FHIR Canonical Manager initialized successfully with optimized configuration");

        Ok(Self {
            config,
            storage,
            registry_client,
            resolver,
            search_engine,
            change_detector,
            rebuild_strategy,
            incremental_indexer,
            parallel_processor,
            performance_monitor,
            tantivy_index,
        })
    }

    fn create_search_index() -> Result<Index> {
        let mut schema_builder = Schema::builder();

        schema_builder.add_text_field("resource_type", TEXT | STORED);
        schema_builder.add_text_field("id", TEXT | STORED);
        schema_builder.add_text_field("canonical_url", TEXT | STORED);
        schema_builder.add_text_field("package_id", TEXT | STORED);
        schema_builder.add_text_field("name", TEXT | STORED);
        schema_builder.add_text_field("title", TEXT | STORED);
        schema_builder.add_text_field("description", TEXT);
        schema_builder.add_text_field("content", TEXT);
        schema_builder.add_text_field("search_text", TEXT);

        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);

        Ok(index)
    }

    /// Install a FHIR package by name and version
    /// Enhanced with optimization components for faster installation
    pub async fn install_package(&self, name: &str, version: &str) -> Result<()> {
        let tracker = self.performance_monitor.start_operation("install_package");
        let mut installed = HashSet::new();

        info!(
            "Installing package {}@{} with optimization features enabled",
            name, version
        );

        self.install_package_with_dependencies(name, version, &mut installed)
            .await?;

        // Use smart rebuild strategy to determine if full or incremental rebuild is needed
        info!("Analyzing rebuild strategy for newly installed packages...");
        self.smart_rebuild_index().await?;

        let total_duration = tracker.finish();

        // Record package operation metrics
        self.performance_monitor
            .record_package_operation(PackageOperationType::Install, total_duration)
            .await;

        info!("Package installation completed in {:?}", total_duration);

        // Track performance metrics
        if self.config.optimization.enable_metrics {
            info!(
                "Performance: Installed {} packages in {:?}",
                installed.len(),
                total_duration
            );
        }

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

        // Use smart rebuild strategy to determine if full or incremental rebuild is needed
        info!("Analyzing rebuild strategy for newly installed packages...");
        self.smart_rebuild_index().await?;

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

            // Check if package already exists in storage
            let packages = self.storage.list_packages().await?;
            if packages
                .iter()
                .any(|p| p.name == name && p.version == version)
            {
                info!("Package already installed in storage: {}", package_key);
                installed.insert(package_key);
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

            // Update file paths in extracted package to point to new location
            let mut updated_extracted = extracted;
            for resource in &mut updated_extracted.resources {
                // Replace the old extraction path with the new package directory path
                if let Ok(relative_path) = resource
                    .file_path
                    .strip_prefix(&updated_extracted.extraction_path)
                {
                    resource.file_path = package_dir.join(relative_path);
                }
            }
            updated_extracted.extraction_path = package_dir.clone();

            // Add package to unified storage
            if let Err(e) = self.storage.add_package(&updated_extracted).await {
                match &e {
                    crate::error::FcmError::Storage(
                        crate::error::StorageError::PackageAlreadyExists { .. },
                    ) => {
                        info!("Package already exists in storage: {}", package_key);
                    }
                    _ => return Err(e),
                }
            }

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

            // Check if package already exists in storage
            let packages = self.storage.list_packages().await?;
            if packages
                .iter()
                .any(|p| p.name == name && p.version == version)
            {
                info!("Package already installed in storage: {}", package_key);
                #[cfg(feature = "cli")]
                Progress::step(&format!("✓ Package already installed: {package_key}"));
                installed.insert(package_key);
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

            // Update file paths in extracted package to point to new location
            let mut updated_extracted = extracted;
            for resource in &mut updated_extracted.resources {
                // Replace the old extraction path with the new package directory path
                if let Ok(relative_path) = resource
                    .file_path
                    .strip_prefix(&updated_extracted.extraction_path)
                {
                    resource.file_path = package_dir.join(relative_path);
                }
            }
            updated_extracted.extraction_path = package_dir.clone();

            // Add package to unified storage
            if let Err(e) = self.storage.add_package(&updated_extracted).await {
                match &e {
                    crate::error::FcmError::Storage(
                        crate::error::StorageError::PackageAlreadyExists { .. },
                    ) => {
                        info!("Package already exists in storage: {}", package_key);
                    }
                    _ => return Err(e),
                }
            }

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
    /// Enhanced with optimization components for better performance
    pub async fn resolve(&self, canonical_url: &str) -> Result<resolver::ResolvedResource> {
        let tracker = self.performance_monitor.start_operation("resolve");
        let result = self.resolver.resolve(canonical_url).await;
        let duration = tracker.finish();

        // Record search operation metrics (resolution is a type of search)
        self.performance_monitor
            .record_search_operation(duration, false)
            .await; // TODO: Track cache hits

        // Log performance metrics if enabled
        if self.config.optimization.enable_metrics {
            debug!("URL resolution took {:?} for: {}", duration, canonical_url);
        }

        result
    }

    /// Search for FHIR resources
    pub async fn search(&self) -> search::SearchQueryBuilder {
        search::SearchQueryBuilder::new(Arc::clone(self.storage.package_storage()))
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

    /// Rebuild the search index from existing packages (legacy method)
    pub async fn rebuild_index(&self) -> Result<()> {
        // Delegate to smart rebuild for backward compatibility
        self.smart_rebuild_index().await
    }

    /// Smart rebuild using the new optimization system
    pub async fn smart_rebuild_index(&self) -> Result<()> {
        info!("Starting optimized index rebuild...");

        let start_time = std::time::Instant::now();

        // Get all packages and determine rebuild strategy
        let packages = self.list_package_infos().await?;
        let strategy = self.rebuild_strategy.determine_strategy(&packages).await?;

        let result = match strategy {
            RebuildStrategy::None => {
                info!("No changes detected, skipping index rebuild");
                Ok(())
            }
            RebuildStrategy::Incremental {
                ref packages,
                batch_size,
            } => {
                info!(
                    "Performing incremental index update for {} packages",
                    packages.len()
                );
                self.incremental_rebuild(packages, batch_size).await?;

                let duration = start_time.elapsed();
                self.rebuild_strategy
                    .record_performance(&strategy, duration, packages.len())
                    .await;

                // Record performance metrics
                self.performance_monitor
                    .record_index_operation(IndexOperationType::IncrementalUpdate, duration)
                    .await;

                Ok(())
            }
            RebuildStrategy::Full => {
                info!(
                    "Performing full index rebuild for {} packages",
                    packages.len()
                );
                let report = self
                    .parallel_processor
                    .process_packages(packages.clone())
                    .await?;

                info!(
                    "Full rebuild completed: success_rate={:.1}%, throughput={:.1} pkg/s",
                    report.success_rate() * 100.0,
                    report.throughput_packages_per_second()
                );

                self.rebuild_strategy
                    .record_performance(&strategy, report.total_time, report.packages_processed)
                    .await;

                // Record performance metrics
                self.performance_monitor
                    .record_index_operation(IndexOperationType::FullRebuild, report.total_time)
                    .await;

                Ok(())
            }
        };

        info!("Index rebuild completed in {:?}", start_time.elapsed());
        result
    }

    /// Perform incremental rebuild for specific packages
    async fn incremental_rebuild(
        &self,
        packages: &[parallel_processor::PackageInfo],
        _batch_size: usize,
    ) -> Result<()> {
        for package in packages {
            let stats = self.incremental_indexer.update_index(&package.path).await?;
            info!(
                "Updated index for {}: indexed={}, removed={}, duration={:?}",
                package.id, stats.indexed, stats.removed, stats.duration
            );
        }
        Ok(())
    }

    /// Get package information for rebuild strategy analysis
    async fn list_package_infos(&self) -> Result<Vec<parallel_processor::PackageInfo>> {
        let packages = self.storage.list_packages().await?;
        let mut package_infos = Vec::new();

        for package in packages {
            let package_dir = self
                .config
                .storage
                .packages_dir
                .join(format!("{}-{}", package.name, package.version));

            if package_dir.exists() {
                let info = parallel_processor::PackageInfo::new(
                    format!("{}@{}", package.name, package.version),
                    package.name.clone(),
                    package.version.clone(),
                    package_dir,
                );
                package_infos.push(info);
            }
        }

        Ok(package_infos)
    }

    /// Remove a FHIR package by name and version
    pub async fn remove_package(&self, name: &str, version: &str) -> Result<()> {
        info!("Removing package: {}@{}", name, version);

        let removed = self.storage.remove_package(name, version).await?;

        if removed {
            info!("Package removed successfully: {}@{}", name, version);
            // Trigger incremental index update to remove resources
            self.smart_rebuild_index().await?;
        } else {
            info!("Package not found: {}@{}", name, version);
        }
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

    /// Get optimization performance metrics
    pub async fn get_optimization_metrics(&self) -> Result<OptimizationMetrics> {
        let perf_metrics = self.performance_monitor.get_metrics().await;

        Ok(OptimizationMetrics {
            change_detector_cache_size: 0, // TODO: Implement actual metrics from change_detector
            incremental_updates_count: perf_metrics.index_operations.incremental_updates_count
                as usize,
            parallel_processing_efficiency: perf_metrics.index_operations.parallel_efficiency,
            average_rebuild_time: perf_metrics.index_operations.average_incremental_time,
            storage_compression_ratio: perf_metrics.system_metrics.compression_ratio,
        })
    }

    /// Get detailed performance metrics
    pub async fn get_performance_metrics(&self) -> Result<PerformanceMetrics> {
        Ok(self.performance_monitor.get_metrics().await)
    }

    /// Get performance analysis with recommendations
    pub async fn analyze_performance(&self) -> Result<PerformanceAnalysis> {
        Ok(self.performance_monitor.analyze_performance().await)
    }

    /// Get search parameters for a specific resource type
    pub async fn get_search_parameters(
        &self,
        resource_type: &str,
    ) -> Result<Vec<SearchParameterInfo>> {
        info!(
            "Getting search parameters for resource type: {}",
            resource_type
        );

        let query = search::SearchQuery {
            text: None,
            resource_types: vec!["SearchParameter".to_string()],
            packages: vec![],
            canonical_pattern: None,
            version_constraints: vec![],
            limit: Some(1000),
            offset: Some(0),
        };

        let results = self.search_engine.search(&query).await?;
        let mut search_params = Vec::new();

        for resource_match in results.resources {
            let resource = &resource_match.resource;
            if let Some(base) = resource.content.get("base") {
                if let Some(base_array) = base.as_array() {
                    for base_type in base_array {
                        if base_type.as_str() == Some(resource_type) {
                            search_params.push(SearchParameterInfo::from_resource(resource)?);
                            break;
                        }
                    }
                } else if base.as_str() == Some(resource_type) {
                    search_params.push(SearchParameterInfo::from_resource(resource)?);
                }
            }
        }

        Ok(search_params)
    }

    /// Force a full index rebuild (bypassing smart strategy)
    pub async fn force_full_rebuild(&self) -> Result<()> {
        info!("Forcing full index rebuild...");
        let packages = self.list_package_infos().await?;
        let report = self.parallel_processor.process_packages(packages).await?;

        info!(
            "Forced rebuild completed: success_rate={:.1}%, throughput={:.1} pkg/s",
            report.success_rate() * 100.0,
            report.throughput_packages_per_second()
        );

        Ok(())
    }

    /// Get change detection status for packages
    pub async fn get_package_change_status(&self) -> Result<Vec<PackageChangeStatus>> {
        let packages = self.list_package_infos().await?;
        let mut statuses = Vec::new();

        for package in packages {
            let changes = match self.change_detector.detect_changes(&package.path).await {
                Ok(changes) => changes,
                Err(e) => {
                    tracing::warn!("Failed to detect changes for {}: {}", package.id, e);
                    continue;
                }
            };

            statuses.push(PackageChangeStatus {
                package_id: package.id,
                has_changes: !changes.is_empty(),
                added_files: changes.added_files.len(),
                modified_files: changes.modified_files.len(),
                removed_files: changes.removed_files.len(),
            });
        }

        Ok(statuses)
    }

    /// Validate the current configuration and get recommendations
    pub fn validate_configuration(&self) -> ValidationResult {
        crate::config_validator::ConfigValidator::validate_config(&self.config)
    }

    /// Get access to the Tantivy search index
    pub fn tantivy_index(&self) -> &Arc<Index> {
        &self.tantivy_index
    }

    /// Get unified storage statistics
    pub async fn get_unified_storage_stats(&self) -> Result<UnifiedStorageStats> {
        self.storage.get_unified_stats().await
    }

    /// Perform integrity check on unified storage system
    pub async fn integrity_check_unified(&self) -> Result<UnifiedIntegrityReport> {
        self.storage.integrity_check().await
    }

    /// Compact unified storage system for optimal performance
    pub async fn compact_storage(&self) -> Result<()> {
        info!("Compacting unified storage system...");
        self.storage.compact().await?;
        info!("Storage compaction completed successfully");
        Ok(())
    }

    /// Get a reference to the unified storage system
    pub fn storage(&self) -> &Arc<UnifiedStorage> {
        &self.storage
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
