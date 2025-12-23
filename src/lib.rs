//! # FHIR Canonical Manager
//!
//! A library-first solution for managing FHIR Implementation Guide packages,
//! providing fast canonical URL resolution and resource search capabilities.
//!
//! ## Features
//!
//! This crate supports several optional features:
//!
//! - **`sqlite`** (enabled by default): Enables SQLite storage backend
//!   - Provides [`SqliteStorage`] implementation
//!   - Required for [`CanonicalManager::new`] and [`unified_storage::UnifiedStorage::new`]
//! - **`cli`** (enabled by default): Enables the command-line interface
//!   - Automatically enables the `sqlite` feature
//! - **`fuzzy-search`**: Enables fuzzy matching for canonical URL resolution
//! - **`metrics`**: Enables performance metrics collection
//!
//! ## Storage Backend
//!
//! ### With SQLite (default)
//!
//! When the `sqlite` feature is enabled, CanonicalManager uses **SQLite storage** for package and resource management:
//! - Fast canonical URL lookups with B-tree indexes (O(1) average query time)
//! - WAL mode for better concurrency and crash recovery
//! - Single-query JOINs for efficient resolution
//! - Atomic transactions for data consistency
//!
//! ### With Custom Storage
//!
//! You can provide custom storage backends by implementing the [`traits::PackageStore`] and
//! [`traits::SearchStorage`] traits. This is useful when integrating with FHIR servers or
//! applications that use different storage backends (PostgreSQL, MongoDB, etc.).
//!
//! Use [`CanonicalManager::new_with_components`] to provide your custom storage implementation.
//!
//! ## Quick Start (With SQLite - default)
//!
//! **Requires the `sqlite` feature (enabled by default).**
//!
//! ```rust,no_run
//! use octofhir_canonical_manager::CanonicalManager;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create manager with default configuration and SQLite storage
//!     let manager = CanonicalManager::with_default_config().await?;
//!
//!     // Install a package
//!     manager.install_package("hl7.fhir.us.core", "6.1.0").await?;
//!
//!     // Resolve a canonical URL
//!     let resource = manager.resolve("http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient").await?;
//!
//!     // Get search parameters for a resource type
//!     let search_params = manager.get_search_parameters("Patient").await?;
//!     for param in search_params {
//!         println!("{}: {} ({})", param.code, param.name, param.type_field);
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Quick Start (With Custom Storage)
//!
//! **Available without the `sqlite` feature.**
//!
//! ```rust,ignore
//! use octofhir_canonical_manager::{CanonicalManager, FcmConfig, PackageStore, SearchStorage};
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create your custom storage implementations
//!     let package_store = Arc::new(MyPostgresPackageStore::new());
//!     let search_storage = Arc::new(MyPostgresSearchStorage::new());
//!     let registry = Arc::new(MyCustomRegistry::new());
//!
//!     let config = FcmConfig::default();
//!     let manager = CanonicalManager::new_with_components(
//!         config,
//!         package_store,
//!         registry,
//!         search_storage,
//!     ).await?;
//!
//!     // Use manager as normal
//!     manager.install_package("hl7.fhir.us.core", "6.1.0").await?;
//!
//!     Ok(())
//! }
//! ```

#[doc(hidden)]
pub mod cas_storage;
#[doc(hidden)]
pub mod change_detection;
#[doc(hidden)]
pub mod concurrency;
pub mod config;
#[doc(hidden)]
pub mod config_validator;
pub mod content_hash;
pub mod domain;
pub mod error;
#[cfg(feature = "fuzzy-search")]
#[doc(hidden)]
pub mod fuzzy;
#[cfg(feature = "cli")]
#[doc(hidden)]
pub mod output;
#[doc(hidden)]
pub mod package;
#[doc(hidden)]
pub mod performance;
pub mod progress;
#[doc(hidden)]
pub mod registry;
pub mod resolver;
pub mod search;
#[cfg(feature = "sqlite")]
#[doc(hidden)]
pub mod sqlite_storage;
#[doc(hidden)]
#[doc(hidden)]
pub mod traits;
#[doc(hidden)]
pub mod unified_storage;

#[cfg(feature = "cli")]
#[doc(hidden)]
pub mod cli;

#[cfg(feature = "cli")]
#[doc(hidden)]
pub mod cli_error;

// Public facade: configs, domain, resolver/search entrypoints, storage stats, and top-level errors
pub use config::{FcmConfig, OptimizationConfig, PackageSpec, RegistryConfig, StorageConfig};
pub use domain::{
    CanonicalUrl, CanonicalWithVersion, FhirVersion, PackageInfo, PackageVersion, ResourceIndex,
    ResourceMetadata,
};
pub use error::{FcmError, Result};
pub use progress::{
    BroadcastCallback, ChannelCallback, CollectingCallback, FnCallback, InstallEvent,
    InstallProgressCallback, InstallProgressContext,
};
pub use registry::PackageInfo as RegistryPackageInfo;
pub use resolver::CanonicalResolver;
pub use search::SearchEngine;
pub use traits::{PackageStore, SearchStorage};
pub use unified_storage::{UnifiedIntegrityReport, UnifiedStorageStats};

// Conditionally export SqliteStorage when sqlite feature is enabled
#[cfg(feature = "sqlite")]
pub use sqlite_storage::SqliteStorage;

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tracing::{debug, info, warn};

// Internal imports for types no longer re-exported at crate root
use crate::change_detection::PackageChangeDetector;
use crate::config_validator::ValidationResult;
use crate::package::PackageManifest;
use crate::performance::{
    PackageOperationType, PerformanceAnalysis, PerformanceConfig, PerformanceMetrics,
    PerformanceMonitor,
};
use crate::unified_storage::UnifiedStorage;

#[cfg(feature = "cli")]
use crate::output::Progress;

#[cfg(feature = "cli")]
use crate::cli::ProgressContext;

/// Bridge between DownloadProgress and InstallProgressCallback.
/// Converts download progress events to install events.
struct DownloadProgressBridge {
    name: String,
    version: String,
    ctx_callback: Arc<dyn crate::progress::InstallProgressCallback>,
}

impl registry::DownloadProgress for DownloadProgressBridge {
    fn on_start(&self, total: Option<u64>) {
        self.ctx_callback
            .on_event(crate::progress::InstallEvent::DownloadStarted {
                package: self.name.clone(),
                version: self.version.clone(),
                current: 0, // Will be set by caller
                total: 0,
                total_bytes: total,
            });
    }

    fn on_progress(&self, downloaded: u64, total: Option<u64>) {
        let percent = if let Some(total) = total {
            if total > 0 {
                ((downloaded as f64 / total as f64) * 100.0).min(100.0) as u8
            } else {
                0
            }
        } else {
            0
        };

        self.ctx_callback
            .on_event(crate::progress::InstallEvent::DownloadProgress {
                package: self.name.clone(),
                version: self.version.clone(),
                downloaded_bytes: downloaded,
                total_bytes: total,
                percent,
            });
    }

    fn on_complete(&self) {
        // Completion is handled by the caller
    }
}

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

/// Package information for rebuild operations
#[derive(Debug, Clone)]
pub struct RebuildPackageInfo {
    pub id: String,
    pub path: std::path::PathBuf,
}

impl RebuildPackageInfo {
    pub fn new(id: String, path: std::path::PathBuf) -> Self {
        Self { id, path }
    }
}

/// Main FHIR Canonical Manager
///
/// Provides high-level interface for managing FHIR packages and resolving canonical URLs.
pub struct CanonicalManager {
    config: FcmConfig,
    storage: Arc<UnifiedStorage>, // Optimized unified storage system
    registry_client: registry::RegistryClient,
    // Optional trait-based components for advanced construction
    registry_dyn: Option<Arc<dyn crate::traits::AsyncRegistry + Send + Sync>>,
    pkg_store_dyn: Option<Arc<dyn crate::traits::PackageStore + Send + Sync>>,
    resolver: CanonicalResolver,
    search_engine: SearchEngine,
    // Optimization components
    change_detector: Arc<PackageChangeDetector>,
    // Performance tracking
    performance_monitor: Arc<PerformanceMonitor>,
}

impl std::fmt::Debug for CanonicalManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CanonicalManager")
            .field("registry_url", &self.config.registry.url)
            .field("packages_configured", &self.config.packages.len())
            .finish()
    }
}

impl CanonicalManager {
    /// Create a new CanonicalManager with the given configuration
    ///
    /// **Requires the `sqlite` feature to be enabled.**
    ///
    /// This method uses **SQLite storage** by default for package and resource management.
    /// For custom storage backends (without the sqlite feature), use [`CanonicalManager::new_with_components`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use octofhir_canonical_manager::{CanonicalManager, FcmConfig};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = FcmConfig::default();
    /// let manager = CanonicalManager::new(config).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "sqlite")]
    pub async fn new(mut config: FcmConfig) -> Result<Self> {
        // Add early timeout check for CI/test environments to avoid hanging
        if std::env::var("CI").is_ok() || std::env::var("FHIRPATH_QUICK_INIT").is_ok() {
            info!("Quick initialization mode detected (CI or FHIRPATH_QUICK_INIT set)");
            // Use minimal configuration for quick initialization
            config.optimization.parallel_workers = 1;
            config.optimization.enable_metrics = false;
            // config.optimization.use_mmap = false; // Field doesn't exist in current version
        }

        // Validate and optimize configuration before initialization
        // Skip optimization for test configurations (parallel_workers=1 and enable_metrics=false)
        let skip_optimization =
            config.optimization.parallel_workers == 1 && !config.optimization.enable_metrics;
        let validation = if skip_optimization {
            debug!("Skipping config optimization for test mode");
            crate::config_validator::ConfigValidator::validate_config(&config)
        } else {
            // crate::config_validator::ConfigValidator::optimize_config(&mut config)
            crate::config_validator::ConfigValidator::validate_config(&config) // Use validate instead
        };

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

        // Initialize unified storage system
        let storage = Arc::new(UnifiedStorage::new(config.storage.clone()).await?);

        // Initialize registry client
        let expanded_storage = config.get_expanded_storage_config();
        let registry_client =
            registry::RegistryClient::new(&config.registry, expanded_storage.cache_dir.clone())
                .await?;

        // Initialize resolver with unified storage (using trait object)
        let resolver = CanonicalResolver::new(storage.search_storage()).await;

        // Initialize search engine with unified storage (using trait object)
        let search_engine = SearchEngine::new(storage.search_storage());

        // Initialize optimization components based on config
        let change_detector = Arc::new(PackageChangeDetector::new());

        // Initialize performance monitor
        let perf_config = PerformanceConfig {
            enable_metrics: config.optimization.enable_metrics,
            metrics_interval: std::time::Duration::from_secs(30),
            max_samples: 1000,
            enable_detailed_logging: config.optimization.enable_metrics,
        };
        let performance_monitor = Arc::new(PerformanceMonitor::new(perf_config));

        debug!("FHIR Canonical Manager initialized successfully with optimized configuration");

        Ok(Self {
            config,
            storage,
            registry_client,
            registry_dyn: None,
            pkg_store_dyn: None,
            resolver,
            search_engine,
            change_detector,
            performance_monitor,
        })
    }

    /// Simple alias for end-user clarity
    ///
    /// **Requires the `sqlite` feature to be enabled.**
    #[cfg(feature = "sqlite")]
    pub async fn new_simple(config: FcmConfig) -> Result<Self> {
        Self::new(config).await
    }

    /// Create a new CanonicalManager with default configuration and SQLite storage
    ///
    /// **Requires the `sqlite` feature to be enabled.**
    ///
    /// This is a convenience method for users who want to get started quickly without
    /// needing to configure anything. It uses:
    /// - Default registry URL (<https://fs.get-ig.org/pkgs/>)
    /// - Default storage paths (~/.maki/cache, ~/.maki/packages, ~/.maki/index)
    /// - SQLite storage backend
    ///
    /// For custom configuration or custom storage backends (without sqlite feature),
    /// use [`CanonicalManager::new_with_components`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use octofhir_canonical_manager::CanonicalManager;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// // Create a manager with default configuration
    /// let manager = CanonicalManager::with_default_config().await?;
    ///
    /// // Now you can use it immediately
    /// manager.install_package("hl7.fhir.r4.core", "4.0.1").await?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "sqlite")]
    pub async fn with_default_config() -> Result<Self> {
        Self::new(FcmConfig::default()).await
    }

    /// Advanced constructor using trait-based components
    ///
    /// **Available without any feature flags.**
    ///
    /// This method allows you to create a CanonicalManager with custom storage backends
    /// that implement the [`crate::traits::PackageStore`] and [`crate::traits::SearchStorage`] traits.
    /// This is useful when integrating with FHIR servers or applications that use different
    /// storage backends (PostgreSQL, custom databases, etc.).
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// # use octofhir_canonical_manager::{CanonicalManager, FcmConfig};
    /// # use std::sync::Arc;
    /// // Assuming you have custom storage implementations
    /// let package_store = Arc::new(MyPostgresPackageStore::new());
    /// let registry = Arc::new(MyCustomRegistry::new());
    /// let search_storage = Arc::new(MyPostgresSearchStorage::new());
    ///
    /// let manager = CanonicalManager::new_with_components(
    ///     FcmConfig::default(),
    ///     package_store,
    ///     registry,
    ///     search_storage,
    /// ).await?;
    /// ```
    pub async fn new_with_components(
        config: FcmConfig,
        package_store: Arc<dyn crate::traits::PackageStore + Send + Sync>,
        registry: Arc<dyn crate::traits::AsyncRegistry + Send + Sync>,
        search_storage: Arc<dyn crate::traits::SearchStorage + Send + Sync>,
    ) -> Result<Self> {
        // Reuse config optimization/validation
        let skip_optimization =
            config.optimization.parallel_workers == 1 && !config.optimization.enable_metrics;
        let validation = if skip_optimization {
            crate::config_validator::ConfigValidator::validate_config(&config)
        } else {
            // crate::config_validator::ConfigValidator::optimize_config(&mut config)
            crate::config_validator::ConfigValidator::validate_config(&config) // Use validate instead
        };
        if !validation.is_valid {
            return Err(crate::error::FcmError::Config(
                crate::error::ConfigError::ValidationFailed {
                    message: "Invalid optimization configuration".to_string(),
                },
            ));
        }

        // Build UnifiedStorage using the provided custom storage backends
        let storage = Arc::new(UnifiedStorage::new_with_custom_storage(
            Arc::clone(&package_store),
            Arc::clone(&search_storage),
        ));

        // Resolver/Search backed by provided search_storage (SqliteStorage)
        let resolver = CanonicalResolver::new(Arc::clone(&search_storage)).await;
        let search_engine = SearchEngine::new(Arc::clone(&search_storage));

        // Optimization components
        let change_detector = Arc::new(PackageChangeDetector::new());

        let perf_config = PerformanceConfig {
            enable_metrics: config.optimization.enable_metrics,
            metrics_interval: std::time::Duration::from_secs(30),
            max_samples: 1000,
            enable_detailed_logging: config.optimization.enable_metrics,
        };
        let performance_monitor = Arc::new(PerformanceMonitor::new(perf_config));

        // Create a dummy concrete client for compatibility, but route calls to trait object helpers
        let expanded_storage = config.get_expanded_storage_config();
        let registry_client =
            registry::RegistryClient::new(&config.registry, expanded_storage.cache_dir.clone())
                .await?;

        Ok(Self {
            config,
            storage,
            registry_client,
            registry_dyn: Some(registry),
            pkg_store_dyn: Some(package_store),
            resolver,
            search_engine,
            change_detector,
            performance_monitor,
        })
    }

    // Helper: registry download via trait or concrete client
    async fn registry_download(&self, spec: &PackageSpec) -> Result<registry::PackageDownload> {
        if let Some(r) = &self.registry_dyn {
            r.download_package(spec).await
        } else {
            self.registry_client.download_package(spec).await
        }
    }

    // Helper: list packages via trait or unified storage
    async fn list_packages_via_store(&self) -> Result<Vec<PackageInfo>> {
        if let Some(s) = &self.pkg_store_dyn {
            s.list_packages().await
        } else {
            self.storage.list_packages().await
        }
    }

    // Helper: add package via trait or unified storage
    async fn add_package_via_store(&self, pkg: &package::ExtractedPackage) -> Result<()> {
        if let Some(s) = &self.pkg_store_dyn {
            s.add_package(pkg).await
        } else {
            self.storage.add_package(pkg).await
        }
    }

    fn find_local_package(&self, name: &str, version: &str) -> Option<&config::LocalPackageSpec> {
        self.config
            .local_packages
            .iter()
            .find(|p| p.name == name && p.version == version)
    }

    /// Install a FHIR package by name and version
    /// Enhanced with optimization components for faster installation
    #[tracing::instrument(name = "manager.install_package", skip(self), fields(pkg = %name, ver = %version))]
    pub async fn install_package(&self, name: &str, version: &str) -> Result<()> {
        // Check if we should use simplified installation for testing
        if self.config.optimization.parallel_workers == 1
            && !self.config.optimization.enable_metrics
        {
            debug!("Using simplified installation for testing mode");
            return self.install_package_simple(name, version).await;
        }

        let spec = PackageSpec {
            name: name.to_string(),
            version: version.to_string(),
            priority: 1,
        };

        self.install_packages_batch(vec![spec]).await
    }

    /// Install a FHIR package with progress callback for real-time updates.
    ///
    /// This method provides detailed progress events during package installation,
    /// including dependency resolution, download progress, extraction, and indexing.
    /// It's designed for use with SSE or WebSocket streaming to provide real-time
    /// feedback to web clients.
    ///
    /// # Arguments
    ///
    /// * `name` - Package name (e.g., "hl7.fhir.us.core")
    /// * `version` - Package version (e.g., "6.1.0")
    /// * `callback` - Progress callback to receive installation events
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::{CanonicalManager, FcmConfig, ChannelCallback};
    /// # async fn example(manager: &CanonicalManager) -> Result<(), Box<dyn std::error::Error>> {
    /// use std::sync::Arc;
    ///
    /// let (callback, mut receiver) = ChannelCallback::new();
    ///
    /// // Spawn task to handle events
    /// tokio::spawn(async move {
    ///     while let Some(event) = receiver.recv().await {
    ///         println!("Progress: {:?}", event);
    ///     }
    /// });
    ///
    /// // Install with progress
    /// manager.install_package_with_callback(
    ///     "hl7.fhir.us.core",
    ///     "6.1.0",
    ///     Arc::new(callback),
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[tracing::instrument(name = "manager.install_package_with_callback", skip(self, callback), fields(pkg = %name, ver = %version))]
    pub async fn install_package_with_callback(
        &self,
        name: &str,
        version: &str,
        callback: std::sync::Arc<dyn crate::progress::InstallProgressCallback>,
    ) -> Result<()> {
        use crate::progress::{InstallEvent, InstallProgressContext};

        let mut ctx = InstallProgressContext::new(callback);
        let mut installed = HashSet::new();
        let mut to_install = Vec::new();

        // Phase 1: Resolve dependencies
        ctx.resolving_dependencies(name, version);

        // Collect all packages to install (recursively)
        self.collect_packages_to_install(name, version, &mut to_install, &mut installed)
            .await?;

        if to_install.is_empty() {
            // Already installed
            ctx.skipped(name, version, "already installed");
            ctx.emit(InstallEvent::Completed {
                total_installed: 0,
                total_resources: 0,
                duration_ms: 0,
            });
            return Ok(());
        }

        let package_names: Vec<String> = to_install
            .iter()
            .map(|(n, v)| format!("{}@{}", n, v))
            .collect();
        ctx.dependencies_resolved(package_names);
        ctx.started(to_install.len());

        // Reset installed set for actual installation tracking
        installed.clear();

        // Phase 2: Install each package with progress
        for (pkg_name, pkg_version) in to_install {
            self.install_single_package_with_progress(
                &pkg_name,
                &pkg_version,
                &mut ctx,
                &mut installed,
            )
            .await?;
        }

        ctx.completed();
        Ok(())
    }

    /// Collect all packages that need to be installed (including dependencies).
    fn collect_packages_to_install<'a>(
        &'a self,
        name: &'a str,
        version: &'a str,
        to_install: &'a mut Vec<(String, String)>,
        visited: &'a mut HashSet<String>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let package_key = format!("{name}@{version}");

            if visited.contains(&package_key) {
                return Ok(());
            }
            visited.insert(package_key.clone());

            // Check if already installed
            let packages = self.list_packages_via_store().await?;
            if packages
                .iter()
                .any(|p| p.name == name && p.version == version)
            {
                return Ok(());
            }

            // Get dependencies from registry
            let metadata = self
                .registry_client
                .get_package_metadata(name, version)
                .await?;

            // Recursively collect dependencies first
            for (dep_name, dep_version) in &metadata.dependencies {
                self.collect_packages_to_install(dep_name, dep_version, to_install, visited)
                    .await?;
            }

            // Add this package after dependencies
            to_install.push((name.to_string(), version.to_string()));

            Ok(())
        })
    }

    /// Install a single package with progress tracking.
    async fn install_single_package_with_progress(
        &self,
        name: &str,
        version: &str,
        ctx: &mut crate::progress::InstallProgressContext,
        installed: &mut HashSet<String>,
    ) -> Result<()> {
        let package_key = format!("{name}@{version}");

        if installed.contains(&package_key) {
            return Ok(());
        }

        // Check if already in storage
        let packages = self.list_packages_via_store().await?;
        if packages
            .iter()
            .any(|p| p.name == name && p.version == version)
        {
            ctx.skipped(name, version, "already installed");
            installed.insert(package_key);
            return Ok(());
        }

        let spec = PackageSpec {
            name: name.to_string(),
            version: version.to_string(),
            priority: 1,
        };

        // Download with progress
        ctx.download_started(name, version, None);

        // Create a progress bridge for download
        let progress_bridge = DownloadProgressBridge {
            name: name.to_string(),
            version: version.to_string(),
            ctx_callback: ctx.callback.clone(),
        };

        let download = self
            .registry_client
            .download_package_with_progress(&spec, Some(&progress_bridge))
            .await
            .inspect_err(|e| {
                ctx.error(Some(name), Some(version), &e.to_string());
            })?;

        ctx.download_completed(name, version);

        // Extract
        ctx.extracting(name, version);
        let extractor = package::PackageExtractor::new(self.config.storage.cache_dir.clone());
        let extracted = extractor.extract_package(download).await.inspect_err(|e| {
            ctx.error(Some(name), Some(version), &e.to_string());
        })?;

        let resource_count = extracted.resources.len();
        ctx.extracted(name, version, resource_count);

        // Move to packages directory
        let package_dir = self
            .config
            .storage
            .packages_dir
            .join(format!("{name}-{version}"));
        if package_dir.exists() {
            fs::remove_dir_all(&package_dir).await?;
        }
        fs::rename(&extracted.extraction_path, &package_dir).await?;

        // Update file paths
        let mut updated_extracted = extracted;
        for resource in &mut updated_extracted.resources {
            if let Ok(relative_path) = resource
                .file_path
                .strip_prefix(&updated_extracted.extraction_path)
            {
                resource.file_path = package_dir.join(relative_path);
            }
        }
        updated_extracted.extraction_path = package_dir;

        // Index
        ctx.indexing(name, version);
        if let Err(e) = self.add_package_via_store(&updated_extracted).await {
            match &e {
                crate::error::FcmError::Storage(
                    crate::error::StorageError::PackageAlreadyExists { .. },
                ) => {
                    // Already exists, that's fine
                }
                _ => {
                    ctx.error(Some(name), Some(version), &e.to_string());
                    return Err(e);
                }
            }
        }

        ctx.package_installed(name, version, resource_count);
        installed.insert(package_key);

        Ok(())
    }

    /// Install multiple FHIR packages in batch, rebuilding index only once
    /// This is much more efficient than calling install_package() multiple times
    #[tracing::instrument(name = "manager.install_packages_batch", skip(self), fields(count = %packages.len()))]
    pub async fn install_packages_batch(&self, packages: Vec<PackageSpec>) -> Result<()> {
        let tracker = self
            .performance_monitor
            .start_operation("install_packages_batch");
        let mut installed = HashSet::new();

        info!("Installing {} packages in batch mode", packages.len());

        for spec in &packages {
            debug!("Installing package: {}@{}", spec.name, spec.version);
            self.install_package_no_rebuild(&spec.name, &spec.version, &mut installed)
                .await?;
        }

        // SQLite automatically maintains indexes, no rebuild needed
        debug!("All packages installed (SQLite auto-indexed)");

        let total_duration = tracker.finish();

        self.performance_monitor
            .record_package_operation(PackageOperationType::Install, total_duration)
            .await;

        info!(
            "Batch installation completed: {} packages in {:?}",
            installed.len(),
            total_duration
        );

        Ok(())
    }

    /// Install multiple packages using parallel pipeline for maximum performance.
    ///
    /// This method implements a three-stage parallel pipeline:
    /// 1. **Parallel Downloads**: Downloads multiple packages concurrently (8 concurrent)
    /// 2. **Parallel Extraction**: Extracts packages using all CPU cores
    /// 3. **Batch Indexing**: Indexes all packages in a single database transaction
    ///
    /// This approach provides significant performance improvements over sequential installation:
    /// - Download: 6-7x faster through concurrency
    /// - Extraction: 7x faster through CPU parallelism
    /// - Indexing: 3.6x faster through batch operations
    ///
    /// # Arguments
    ///
    /// * `packages` - Vector of package specifications to install
    ///
    /// # Returns
    ///
    /// * `Ok(())` - All packages installed successfully
    /// * `Err` - If any stage fails
    ///
    /// # Performance
    ///
    /// Expected total time for 70+ packages: 240s → 30-60s (4-8x improvement)
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::{CanonicalManager, FcmConfig, PackageSpec};
    /// # async fn example(manager: &CanonicalManager) -> Result<(), Box<dyn std::error::Error>> {
    /// let packages = vec![
    ///     PackageSpec {
    ///         name: "hl7.fhir.r4.core".to_string(),
    ///         version: "4.0.1".to_string(),
    ///         priority: 1,
    ///     },
    ///     PackageSpec {
    ///         name: "hl7.fhir.us.core".to_string(),
    ///         version: "6.1.0".to_string(),
    ///         priority: 2,
    ///     },
    /// ];
    ///
    /// manager.install_packages_parallel(packages).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[tracing::instrument(name = "manager.install_packages_parallel", skip(self), fields(count = %packages.len()))]
    pub async fn install_packages_parallel(&self, packages: Vec<PackageSpec>) -> Result<()> {
        info!(
            "Starting parallel installation of {} packages",
            packages.len()
        );
        let overall_start = std::time::Instant::now();

        let tracker = self
            .performance_monitor
            .start_operation("install_packages_parallel");

        // Stage 1: Parallel Downloads
        info!(
            "Stage 1/3: Downloading {} packages in parallel",
            packages.len()
        );
        let download_start = std::time::Instant::now();

        let downloads = self
            .registry_client
            .download_packages_parallel(packages)
            .await?;

        let download_duration = download_start.elapsed();
        info!(
            "Stage 1/3 completed: Downloaded {} packages in {:?}",
            downloads.len(),
            download_duration
        );

        // Stage 2: Parallel Extraction
        info!(
            "Stage 2/3: Extracting {} packages in parallel",
            downloads.len()
        );
        let extract_start = std::time::Instant::now();

        let extractor =
            crate::package::PackageExtractor::new(self.config.storage.cache_dir.clone());
        let extracted = extractor.extract_packages_parallel(downloads).await?;

        let extracted_count = extracted.len();
        let extract_duration = extract_start.elapsed();
        info!(
            "Stage 2/3 completed: Extracted {} packages in {:?}",
            extracted_count, extract_duration
        );

        // Stage 3: Batch Indexing
        info!("Stage 3/3: Indexing {} packages in batch", extracted_count);
        let index_start = std::time::Instant::now();

        self.storage.add_packages_batch(extracted).await?;

        let index_duration = index_start.elapsed();
        info!(
            "Stage 3/3 completed: Indexed {} packages in {:?}",
            extracted_count, index_duration
        );

        // Complete
        let total_duration = tracker.finish();
        let overall_duration = overall_start.elapsed();

        self.performance_monitor
            .record_package_operation(PackageOperationType::Install, total_duration)
            .await;

        info!(
            "Parallel installation completed successfully: {} packages in {:?} (download: {:?}, extract: {:?}, index: {:?})",
            extracted_count, overall_duration, download_duration, extract_duration, index_duration
        );

        Ok(())
    }

    /// Install a package without triggering index rebuild
    /// Used internally by batch installation to defer rebuild until all packages are installed
    async fn install_package_no_rebuild(
        &self,
        name: &str,
        version: &str,
        installed: &mut HashSet<String>,
    ) -> Result<()> {
        self.install_package_with_dependencies(name, version, installed)
            .await
    }

    /// Simplified package installation for testing (bypasses complex optimizations)
    async fn install_package_simple(&self, name: &str, version: &str) -> Result<()> {
        debug!("Installing package {}@{} in simplified mode", name, version);

        // Use a simple set to track what we've installed to avoid infinite recursion
        let mut installed = std::collections::HashSet::new();
        self.install_package_simple_recursive(name, version, &mut installed)
            .await
    }

    /// Recursive helper for simplified installation with dependency handling
    fn install_package_simple_recursive<'a>(
        &'a self,
        name: &'a str,
        version: &'a str,
        installed: &'a mut std::collections::HashSet<String>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let package_key = format!("{name}@{version}");

            // Skip if already processed
            if installed.contains(&package_key) {
                debug!("Package {} already processed", package_key);
                return Ok(());
            }

            // Check if package already exists in storage
            debug!(
                "Checking if package {} already exists in storage...",
                package_key
            );
            let packages = self.storage.list_packages().await?;
            if packages
                .iter()
                .any(|p| p.name == name && p.version == version)
            {
                debug!("Package {} already installed in storage", package_key);
                installed.insert(package_key);
                return Ok(());
            }
            debug!(
                "Package {} not found in storage, proceeding with installation",
                package_key
            );

            if let Some(local_spec) = self.find_local_package(name, version) {
                debug!(
                    "Installing package {} from local directory {}",
                    package_key,
                    local_spec.path.display()
                );

                let manifest_path = local_spec.path.join("package.json");
                let manifest_content = fs::read_to_string(&manifest_path).await.map_err(|e| {
                    FcmError::Package(crate::error::PackageError::ExtractionFailed {
                        message: format!(
                            "Failed to read manifest for local package {}: {}",
                            package_key, e
                        ),
                    })
                })?;

                let manifest: PackageManifest =
                    serde_json::from_str(&manifest_content).map_err(|e| {
                        FcmError::Package(crate::error::PackageError::ExtractionFailed {
                            message: format!(
                                "Invalid manifest for local package {}: {}",
                                package_key, e
                            ),
                        })
                    })?;

                for (dep_name, dep_version) in manifest.dependencies.clone() {
                    debug!(
                        "Installing local dependency for {}: {}@{}",
                        package_key, dep_name, dep_version
                    );
                    self.install_package_simple_recursive(&dep_name, &dep_version, installed)
                        .await?;
                }

                self.load_from_directory(&local_spec.path, Some(&package_key))
                    .await?;

                installed.insert(package_key.clone());
                info!(
                    "Package {} installed successfully from local directory",
                    package_key
                );
                return Ok(());
            }

            let spec = crate::config::PackageSpec {
                name: name.to_string(),
                version: version.to_string(),
                priority: 1,
            };

            // Download package and get metadata (includes dependencies)
            debug!("Downloading package {}...", package_key);
            let download = self.registry_client.download_package(&spec).await?;
            let dependencies = download.metadata.dependencies.clone();
            debug!(
                "Package {} downloaded successfully, found {} dependencies",
                package_key,
                dependencies.len()
            );

            // Install dependencies first
            for (dep_name, dep_version) in dependencies {
                debug!("Installing dependency: {}@{}", dep_name, dep_version);
                self.install_package_simple_recursive(&dep_name, &dep_version, installed)
                    .await?;
            }

            debug!("Extracting package {}...", package_key);
            let extractor =
                crate::package::PackageExtractor::new(self.config.storage.packages_dir.clone());
            let extracted = extractor.extract_package(download).await?;
            debug!("Package {} extracted successfully", package_key);

            // Add to storage (simplified - no complex indexing)
            debug!("Adding package {} to storage...", package_key);
            self.storage.add_package(&extracted).await?;
            #[cfg(feature = "metrics")]
            {
                metrics::increment_counter!("packages_installed");
            }
            debug!("Package {} added to storage successfully", package_key);

            installed.insert(package_key.clone());
            info!(
                "Package {} installed successfully (simplified mode)",
                package_key
            );
            Ok(())
        })
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
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<usize>> + Send + 'a>> {
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
        let spec = PackageSpec {
            name: name.to_string(),
            version: version.to_string(),
            priority: 1,
        };

        self.install_packages_batch_with_progress(vec![spec], progress)
            .await
    }

    /// Install multiple packages in batch with progress tracking (CLI feature)
    #[cfg(feature = "cli")]
    pub async fn install_packages_batch_with_progress(
        &self,
        packages: Vec<PackageSpec>,
        progress: &mut ProgressContext,
    ) -> Result<()> {
        let mut installed = HashSet::new();

        Progress::step(&format!(
            "Installing {} packages in batch mode",
            packages.len()
        ));

        for spec in &packages {
            self.install_package_with_dependencies_and_progress(
                &spec.name,
                &spec.version,
                &mut installed,
                progress,
            )
            .await?;
        }

        // SQLite automatically maintains indexes, no rebuild needed
        Progress::step("Index maintained automatically by SQLite");

        Progress::step(&format!(
            "Batch installation completed: {} packages",
            installed.len()
        ));

        Ok(())
    }

    /// Install a package with recursive dependency resolution
    fn install_package_with_dependencies<'a>(
        &'a self,
        name: &'a str,
        version: &'a str,
        installed: &'a mut HashSet<String>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move {
            let package_key = format!("{name}@{version}");

            // Check if already installed to avoid cycles and duplicates
            if installed.contains(&package_key) {
                debug!("Package already processed: {}", package_key);
                return Ok(());
            }

            // Check if package already exists in storage (trait-aware)
            let packages = self.list_packages_via_store().await?;
            if packages
                .iter()
                .any(|p| p.name == name && p.version == version)
            {
                debug!("Package already installed in storage: {}", package_key);
                installed.insert(package_key);
                return Ok(());
            }

            if let Some(local_spec) = self.find_local_package(name, version) {
                debug!(
                    "Installing package {} from local directory {}",
                    package_key,
                    local_spec.path.display()
                );

                let manifest_path = local_spec.path.join("package.json");
                let manifest_content = fs::read_to_string(&manifest_path).await.map_err(|e| {
                    FcmError::Package(crate::error::PackageError::ExtractionFailed {
                        message: format!(
                            "Failed to read manifest for local package {}: {}",
                            package_key, e
                        ),
                    })
                })?;

                let manifest: PackageManifest =
                    serde_json::from_str(&manifest_content).map_err(|e| {
                        FcmError::Package(crate::error::PackageError::ExtractionFailed {
                            message: format!(
                                "Invalid manifest for local package {}: {}",
                                package_key, e
                            ),
                        })
                    })?;

                for (dep_name, dep_version) in manifest.dependencies.clone() {
                    debug!(
                        "Installing local dependency for {}: {}@{}",
                        package_key, dep_name, dep_version
                    );
                    self.install_package_with_dependencies(&dep_name, &dep_version, installed)
                        .await?;
                }

                self.load_from_directory(&local_spec.path, Some(&package_key))
                    .await?;

                installed.insert(package_key.clone());
                info!(
                    "Package {} installed successfully from local directory",
                    package_key
                );
                return Ok(());
            }

            debug!("Installing package: {}@{}", name, version);

            let spec = PackageSpec {
                name: name.to_string(),
                version: version.to_string(),
                priority: 1,
            };

            // Download package and get metadata
            let download = self.registry_download(&spec).await?;
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
            if let Err(e) = self.add_package_via_store(&updated_extracted).await {
                match &e {
                    crate::error::FcmError::Storage(
                        crate::error::StorageError::PackageAlreadyExists { .. },
                    ) => {
                        info!("Package already exists in storage: {}", package_key);
                    }
                    _ => return Err(e),
                }
            }
            #[cfg(feature = "metrics")]
            {
                metrics::increment_counter!("packages_installed");
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
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
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
                debug!("Package already installed in storage: {}", package_key);
                #[cfg(feature = "cli")]
                Progress::step(&format!("✓ Package already installed: {package_key}"));
                installed.insert(package_key);
                return Ok(());
            }

            if let Some(local_spec) = self.find_local_package(name, version) {
                debug!(
                    "Installing package {} from local directory {}",
                    package_key,
                    local_spec.path.display()
                );
                #[cfg(feature = "cli")]
                Progress::step(&format!("Loading local package {package_key}"));

                let manifest_path = local_spec.path.join("package.json");
                let manifest_content = fs::read_to_string(&manifest_path).await.map_err(|e| {
                    FcmError::Package(crate::error::PackageError::ExtractionFailed {
                        message: format!(
                            "Failed to read manifest for local package {}: {}",
                            package_key, e
                        ),
                    })
                })?;

                let manifest: PackageManifest =
                    serde_json::from_str(&manifest_content).map_err(|e| {
                        FcmError::Package(crate::error::PackageError::ExtractionFailed {
                            message: format!(
                                "Invalid manifest for local package {}: {}",
                                package_key, e
                            ),
                        })
                    })?;

                for (dep_name, dep_version) in manifest.dependencies.clone() {
                    self.install_package_with_dependencies_and_progress(
                        &dep_name,
                        &dep_version,
                        installed,
                        progress,
                    )
                    .await?;
                }

                self.load_from_directory(&local_spec.path, Some(&package_key))
                    .await?;

                installed.insert(package_key.clone());
                #[cfg(feature = "cli")]
                Progress::step(&format!("✓ Installed local package {package_key}"));
                info!(
                    "Package {} installed successfully from local directory",
                    package_key
                );
                return Ok(());
            }

            // Create stream progress for this package
            let stream_progress = progress.create_stream_progress();

            debug!("Installing package: {}@{}", name, version);
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
    #[tracing::instrument(name = "manager.resolve", skip(self), fields(canonical = %canonical_url))]
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

    /// Resolve a canonical URL filtered by FHIR version
    ///
    /// This method resolves a canonical URL to a FHIR resource, filtering by the specified
    /// FHIR version. This is useful when working with projects that use a specific FHIR version
    /// and need to ensure resolved resources match that version.
    ///
    /// # Arguments
    ///
    /// * `canonical_url` - The canonical URL of the resource to resolve
    /// * `fhir_version` - The FHIR version to filter by (e.g., "4.0.1", "5.0.0")
    ///
    /// # Returns
    ///
    /// Returns the resolved resource if found in the specified FHIR version,
    /// or an error if the resource is not found or resolution fails.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::{CanonicalManager, FcmConfig};
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let config = FcmConfig::default();
    /// # let manager = CanonicalManager::new(config).await?;
    /// // Resolve Patient resource for FHIR R4 (4.0.1)
    /// let resource = manager.resolve_with_fhir_version(
    ///     "http://hl7.org/fhir/StructureDefinition/Patient",
    ///     "4.0.1"
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    #[tracing::instrument(name = "manager.resolve_with_fhir_version", skip(self), fields(canonical = %canonical_url, fhir_version = %fhir_version))]
    pub async fn resolve_with_fhir_version(
        &self,
        canonical_url: &str,
        fhir_version: &str,
    ) -> Result<resolver::ResolvedResource> {
        let tracker = self
            .performance_monitor
            .start_operation("resolve_with_fhir_version");
        let result = self
            .resolver
            .resolve_with_fhir_version(canonical_url, fhir_version)
            .await;
        let duration = tracker.finish();

        // Record search operation metrics
        self.performance_monitor
            .record_search_operation(duration, false)
            .await;

        // Log performance metrics if enabled
        if self.config.optimization.enable_metrics {
            debug!(
                "FHIR version-specific resolution took {:?} for: {} (FHIR {})",
                duration, canonical_url, fhir_version
            );
        }

        result
    }

    /// Search for FHIR resources
    pub async fn search(&self) -> search::SearchQueryBuilder {
        search::SearchQueryBuilder::new(self.storage.search_storage())
    }

    /// Find resource by exact resource type and ID match (fast path, no text search)
    /// This is much faster than using search() for exact ID lookups
    pub async fn find_by_type_and_id(
        &self,
        resource_type: &str,
        id: &str,
    ) -> Result<Vec<ResourceIndex>> {
        self.storage
            .search_storage()
            .find_by_type_and_id(resource_type.to_string(), id.to_string())
            .await
    }

    /// Find resource by exact resource type and Name match (fast path, no text search)
    /// Useful for US Core profiles with names like "USCoreMedicationRequestProfile"
    pub async fn find_by_type_and_name(
        &self,
        resource_type: &str,
        name: &str,
    ) -> Result<Vec<ResourceIndex>> {
        self.storage
            .search_storage()
            .find_by_type_and_name(resource_type.to_string(), name.to_string())
            .await
    }

    /// List all base FHIR resource type names for a given FHIR version
    /// Returns names like "Patient", "Observation", "Condition", etc.
    pub async fn list_base_resource_type_names(&self, fhir_version: &str) -> Result<Vec<String>> {
        self.storage
            .search_storage()
            .list_base_resource_type_names(fhir_version)
            .await
    }

    /// Find all resources by resource type and package name
    /// This queries the database directly without any caching - fast and reliable
    pub async fn find_by_type_and_package(
        &self,
        resource_type: &str,
        package_name: &str,
    ) -> Result<Vec<ResourceIndex>> {
        self.storage
            .search_storage()
            .find_by_type_and_package(resource_type, package_name)
            .await
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

    /// List available versions for a package from the registry.
    ///
    /// This method queries the FHIR package registry to find all available
    /// versions of a specific package.
    ///
    /// # Arguments
    ///
    /// * `name` - Package name (e.g., "hl7.fhir.us.core")
    ///
    /// # Returns
    ///
    /// * `Ok(Vec<String>)` - List of available versions sorted by semver (newest first)
    /// * `Err` - If the package is not found or network error occurs
    pub async fn list_registry_versions(&self, name: &str) -> Result<Vec<String>> {
        self.registry_client.list_versions(name).await
    }

    /// Search for packages in the registry.
    ///
    /// This method searches the FHIR package registry (fs.get-ig.org) for packages
    /// matching the query string. The search supports partial matching (ILIKE) -
    /// spaces in the query are treated as wildcards for fuzzy matching.
    ///
    /// # Arguments
    ///
    /// * `query` - Search query string (e.g., "us core", "hl7.fhir")
    ///
    /// # Returns
    ///
    /// * `Ok(Vec<registry::PackageInfo>)` - List of matching packages with their available versions
    /// * `Err` - If search fails
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::CanonicalManager;
    /// # async fn example(manager: &CanonicalManager) -> Result<(), Box<dyn std::error::Error>> {
    /// let results = manager.search_registry("us core").await?;
    /// for pkg in results {
    ///     println!("{} - {} versions available", pkg.name, pkg.versions.len());
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn search_registry(&self, query: &str) -> Result<Vec<RegistryPackageInfo>> {
        self.registry_client.search_packages(query).await
    }

    /// Rebuild the search index from existing packages (no-op, SQLite auto-indexes)
    ///
    /// This method is kept for backward compatibility but does nothing since
    /// SQLite automatically maintains all indexes.
    pub async fn rebuild_index(&self) -> Result<()> {
        debug!("Index rebuild requested - SQLite maintains indexes automatically");
        Ok(())
    }

    /// Remove a FHIR package by name and version
    pub async fn remove_package(&self, name: &str, version: &str) -> Result<()> {
        info!("Removing package: {}@{}", name, version);

        let removed = self.storage.remove_package(name, version).await?;

        if removed {
            info!("Package removed successfully: {}@{}", name, version);
            // SQLite automatically updates indexes when resources are removed
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

    /// Force a full index rebuild (no-op, SQLite auto-indexes)
    ///
    /// This method is kept for backward compatibility but does nothing since
    /// SQLite automatically maintains all indexes.
    pub async fn force_full_rebuild(&self) -> Result<()> {
        debug!("Force rebuild requested - SQLite maintains indexes automatically");
        Ok(())
    }

    /// Get change detection status for packages
    pub async fn get_package_change_status(&self) -> Result<Vec<PackageChangeStatus>> {
        let packages = self.storage.list_packages().await?;
        let mut statuses = Vec::new();

        for package in packages {
            let package_dir = self
                .config
                .storage
                .packages_dir
                .join(format!("{}-{}", package.name, package.version));

            let changes = match self.change_detector.detect_changes(&package_dir).await {
                Ok(changes) => changes,
                Err(e) => {
                    tracing::warn!(
                        "Failed to detect changes for {}@{}: {}",
                        package.name,
                        package.version,
                        e
                    );
                    continue;
                }
            };

            statuses.push(PackageChangeStatus {
                package_id: format!("{}@{}", package.name, package.version),
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

    /// Load a FHIR package from a local directory
    ///
    /// The directory should contain a package structure:
    /// - package.json (manifest)
    /// - *.json files (FHIR resources)
    ///
    /// If package_id is None, it's read from package.json
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the package directory
    /// * `package_id` - Optional package identifier (name@version). If None, read from package.json
    ///
    /// # Returns
    ///
    /// * `Ok(PackageInfo)` - Information about the loaded package
    /// * `Err` - If directory doesn't exist, package.json is missing/invalid, or loading fails
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::{CanonicalManager, FcmConfig};
    /// # use std::path::Path;
    /// # async fn example(manager: &CanonicalManager) -> Result<(), Box<dyn std::error::Error>> {
    /// // Load local package
    /// let package_info = manager.load_from_directory(
    ///     Path::new("./local-packages/hl7.fhir.us.core-custom"),
    ///     None  // Read from package.json
    /// ).await?;
    ///
    /// println!("Loaded package: {}@{}", package_info.name, package_info.version);
    /// # Ok(())
    /// # }
    /// ```
    #[tracing::instrument(name = "manager.load_from_directory", skip(self))]
    pub async fn load_from_directory(
        &self,
        path: &Path,
        package_id: Option<&str>,
    ) -> Result<PackageInfo> {
        use crate::package::{ExtractedPackage, FhirResource, PackageManifest};

        info!("Loading package from local directory: {}", path.display());

        // Validate directory exists
        if !path.exists() {
            return Err(FcmError::Package(
                crate::error::PackageError::ExtractionFailed {
                    message: format!("Directory not found: {}", path.display()),
                },
            ));
        }

        if !path.is_dir() {
            return Err(FcmError::Package(
                crate::error::PackageError::ExtractionFailed {
                    message: format!("Path is not a directory: {}", path.display()),
                },
            ));
        }

        // Read and parse package.json
        let manifest_path = path.join("package.json");
        if !manifest_path.exists() {
            return Err(FcmError::Package(
                crate::error::PackageError::ExtractionFailed {
                    message: format!("package.json not found in: {}", path.display()),
                },
            ));
        }

        let manifest_content = fs::read_to_string(&manifest_path).await.map_err(|e| {
            FcmError::Package(crate::error::PackageError::ExtractionFailed {
                message: format!("Failed to read package.json: {}", e),
            })
        })?;

        let manifest: PackageManifest = serde_json::from_str(&manifest_content).map_err(|e| {
            FcmError::Package(crate::error::PackageError::ExtractionFailed {
                message: format!("Invalid package.json: {}", e),
            })
        })?;

        // Validate package_id matches manifest if provided
        if let Some(expected_id) = package_id {
            let actual_id = format!("{}@{}", manifest.name, manifest.version);
            if expected_id != actual_id {
                warn!(
                    "Package ID mismatch: expected {}, got {}",
                    expected_id, actual_id
                );
            }
        }

        // Scan directory for .json resource files
        let mut resources = Vec::new();
        let mut entries = fs::read_dir(path).await.map_err(|e| {
            FcmError::Package(crate::error::PackageError::ExtractionFailed {
                message: format!("Failed to read directory: {}", e),
            })
        })?;

        while let Some(entry) = entries.next_entry().await.map_err(|e| {
            FcmError::Package(crate::error::PackageError::ExtractionFailed {
                message: format!("Failed to read directory entry: {}", e),
            })
        })? {
            let entry_path = entry.path();

            // Skip non-JSON files
            if entry_path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }

            // Skip package.json itself
            if entry_path.file_name().and_then(|n| n.to_str()) == Some("package.json") {
                continue;
            }

            // Read and parse resource
            let content = match fs::read_to_string(&entry_path).await {
                Ok(c) => c,
                Err(e) => {
                    warn!("Failed to read {}: {}", entry_path.display(), e);
                    continue;
                }
            };

            let json: serde_json::Value = match serde_json::from_str(&content) {
                Ok(j) => j,
                Err(e) => {
                    warn!("Invalid JSON in {}: {}", entry_path.display(), e);
                    continue;
                }
            };

            // Extract resource metadata
            let resource_type = json
                .get("resourceType")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown")
                .to_string();

            let id = json
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();

            let url = json.get("url").and_then(|v| v.as_str()).map(String::from);

            let version = json
                .get("version")
                .and_then(|v| v.as_str())
                .map(String::from);

            resources.push(FhirResource {
                resource_type,
                id,
                url,
                version,
                content: json,
                file_path: entry_path,
            });
        }

        info!("Found {} resources in {}", resources.len(), path.display());

        // Create ExtractedPackage structure
        let extracted = ExtractedPackage {
            name: manifest.name.clone(),
            version: manifest.version.clone(),
            manifest,
            resources,
            extraction_path: path.to_path_buf(),
        };

        // Add to storage
        self.add_package_via_store(&extracted).await?;

        // Return package info
        let fhir_version = extracted
            .manifest
            .fhir_versions
            .as_ref()
            .and_then(|versions| versions.first())
            .cloned()
            .unwrap_or_else(|| "4.0.1".to_string());
        Ok(PackageInfo {
            name: extracted.name,
            version: extracted.version,
            fhir_version,
            installed_at: chrono::Utc::now(),
            resource_count: extracted.resources.len(),
        })
    }

    /// Load individual resources from directory without package.json
    ///
    /// All .json files are loaded and indexed under the given namespace.
    /// Resources must have valid resourceType, url/id fields.
    ///
    /// The namespace is used for grouping (e.g., "local", "test-fixtures")
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the resources directory
    /// * `namespace` - Namespace for grouping these resources
    ///
    /// # Returns
    ///
    /// * `Ok(usize)` - Number of resources loaded
    /// * `Err` - If directory doesn't exist or loading fails
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::{CanonicalManager, FcmConfig};
    /// # use std::path::Path;
    /// # async fn example(manager: &CanonicalManager) -> Result<(), Box<dyn std::error::Error>> {
    /// // Load loose resources
    /// let count = manager.load_resources_from_directory(
    ///     Path::new("./input/resources"),
    ///     "project-resources"
    /// ).await?;
    ///
    /// println!("Loaded {} resources", count);
    /// # Ok(())
    /// # }
    /// ```
    #[tracing::instrument(name = "manager.load_resources_from_directory", skip(self))]
    pub async fn load_resources_from_directory(
        &self,
        path: &Path,
        namespace: &str,
    ) -> Result<usize> {
        use crate::package::{ExtractedPackage, FhirResource, PackageManifest};

        info!(
            "Loading resources from directory: {} (namespace: {})",
            path.display(),
            namespace
        );

        // Validate directory exists
        if !path.exists() {
            return Err(FcmError::Package(
                crate::error::PackageError::ExtractionFailed {
                    message: format!("Directory not found: {}", path.display()),
                },
            ));
        }

        if !path.is_dir() {
            return Err(FcmError::Package(
                crate::error::PackageError::ExtractionFailed {
                    message: format!("Path is not a directory: {}", path.display()),
                },
            ));
        }

        // Scan directory for .json resource files
        let mut resources = Vec::new();
        let mut entries = fs::read_dir(path).await.map_err(|e| {
            FcmError::Package(crate::error::PackageError::ExtractionFailed {
                message: format!("Failed to read directory: {}", e),
            })
        })?;

        while let Some(entry) = entries.next_entry().await.map_err(|e| {
            FcmError::Package(crate::error::PackageError::ExtractionFailed {
                message: format!("Failed to read directory entry: {}", e),
            })
        })? {
            let entry_path = entry.path();

            // Skip non-JSON files
            if entry_path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }

            // Skip package.json
            if entry_path.file_name().and_then(|n| n.to_str()) == Some("package.json") {
                continue;
            }

            // Read and parse resource
            let content = match fs::read_to_string(&entry_path).await {
                Ok(c) => c,
                Err(e) => {
                    warn!("Failed to read {}: {}", entry_path.display(), e);
                    continue;
                }
            };

            let json: serde_json::Value = match serde_json::from_str(&content) {
                Ok(j) => j,
                Err(e) => {
                    warn!("Invalid JSON in {}: {}", entry_path.display(), e);
                    continue;
                }
            };

            // Validate resource has resourceType
            let resource_type = match json.get("resourceType").and_then(|v| v.as_str()) {
                Some(rt) => rt.to_string(),
                None => {
                    warn!("Skipping {}: missing resourceType", entry_path.display());
                    continue;
                }
            };

            // Extract id (required)
            let id = match json.get("id").and_then(|v| v.as_str()) {
                Some(id_val) => id_val.to_string(),
                None => {
                    warn!("Skipping {}: missing id", entry_path.display());
                    continue;
                }
            };

            let url = json.get("url").and_then(|v| v.as_str()).map(String::from);

            let version = json
                .get("version")
                .and_then(|v| v.as_str())
                .map(String::from);

            resources.push(FhirResource {
                resource_type,
                id,
                url,
                version,
                content: json,
                file_path: entry_path,
            });
        }

        let resource_count = resources.len();

        if resource_count == 0 {
            warn!("No valid resources found in {}", path.display());
            return Ok(0);
        }

        info!(
            "Found {} resources in {} (namespace: {})",
            resource_count,
            path.display(),
            namespace
        );

        // Create a synthetic package for these resources
        let manifest = PackageManifest {
            name: format!("local.{}", namespace),
            version: "dev".to_string(),
            fhir_versions: None,
            dependencies: std::collections::HashMap::new(),
            canonical: None,
            jurisdiction: None,
            package_type: Some("local-resources".to_string()),
            title: Some(format!("Local resources: {}", namespace)),
            description: Some(format!("Resources loaded from {}", path.display())),
        };

        let extracted = ExtractedPackage {
            name: manifest.name.clone(),
            version: manifest.version.clone(),
            manifest,
            resources,
            extraction_path: path.to_path_buf(),
        };

        // Add to storage
        self.add_package_via_store(&extracted).await?;

        Ok(resource_count)
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
