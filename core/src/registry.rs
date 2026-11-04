//! Registry data contracts, with optional HTTP clients.
use crate::config::PackageSpec;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

#[cfg(feature = "http")]
mod http;
#[cfg(feature = "http")]
pub use http::{RegistryClient, build_default_http_client};

/// Trait for receiving progress updates during package downloads.
///
/// Implement this trait to provide user feedback during long-running download operations.
/// All methods are called from async contexts and should be non-blocking.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_core::registry::DownloadProgress;
///
/// struct ConsoleProgress;
///
/// impl DownloadProgress for ConsoleProgress {
///     fn on_progress(&self, downloaded: u64, total: Option<u64>) {
///         if let Some(total) = total {
///             let percent = (downloaded * 100) / total;
///             println!("Progress: {}% ({}/{})", percent, downloaded, total);
///         } else {
///             println!("Downloaded: {} bytes", downloaded);
///         }
///     }
///
///     fn on_start(&self, total: Option<u64>) {
///         println!("Starting download...");
///     }
///
///     fn on_complete(&self) {
///         println!("Download complete!");
///     }
/// }
/// ```
pub trait DownloadProgress: Send + Sync {
    fn on_progress(&self, downloaded: u64, total: Option<u64>);
    fn on_start(&self, total: Option<u64>);
    fn on_complete(&self);
}

/// Information about a package available in a registry.
///
/// Contains metadata about a package including all available versions
/// and descriptive information.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_core::registry::PackageInfo;
///
/// let info = PackageInfo {
///     name: "hl7.fhir.us.core".to_string(),
///     versions: vec!["6.1.0".to_string(), "5.0.1".to_string()],
///     description: Some("US Core Implementation Guide".to_string()),
///     latest_version: "6.1.0".to_string(),
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageInfo {
    pub name: String,
    pub versions: Vec<String>,
    pub description: Option<String>,
    pub latest_version: String,
}

/// Result of a successful package download operation.
///
/// Contains the downloaded file path along with package metadata
/// retrieved from the registry.
///
/// # Example
///
/// ```rust,no_run
/// # use octofhir_canonical_core::registry::PackageDownload;
/// # fn example(download: PackageDownload) {
/// println!("Downloaded: {}@{}", download.spec.name, download.spec.version);
/// println!("File: {}", download.file_path.display());
/// println!("Description: {:?}", download.metadata.description);
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct PackageDownload {
    pub spec: PackageSpec,
    pub file_path: PathBuf,
    pub metadata: PackageMetadata,
}

/// Metadata about a package retrieved from a registry.
///
/// Contains detailed information about a specific version of a package,
/// including its dependencies and FHIR version compatibility.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_core::registry::PackageMetadata;
/// use std::collections::HashMap;
///
/// let metadata = PackageMetadata {
///     name: "hl7.fhir.us.core".to_string(),
///     version: "6.1.0".to_string(),
///     description: Some("US Core Implementation Guide".to_string()),
///     fhir_version: "4.0.1".to_string(),
///     dependencies: HashMap::new(),
///     canonical_base: Some("http://hl7.org/fhir/us/core".to_string()),
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageMetadata {
    pub name: String,
    pub version: String,
    pub description: Option<String>,
    pub fhir_version: String,
    pub dependencies: HashMap<String, String>,
    pub canonical_base: Option<String>,
}

/// Information about a package dependency.
///
/// Represents a dependency relationship between packages,
/// specifying the required package name and version.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_core::registry::PackageDependency;
///
/// let dependency = PackageDependency {
///     name: "hl7.fhir.r4.core".to_string(),
///     version: "4.0.1".to_string(),
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageDependency {
    pub name: String,
    pub version: String,
}

/// Registry adapter that rejects remote operations, for local-only managers.
#[derive(Debug, Default)]
pub struct OfflineRegistry;

#[async_trait::async_trait]
impl crate::traits::AsyncRegistry for OfflineRegistry {
    async fn get_package_metadata(
        &self,
        _name: &str,
        _version: &str,
    ) -> crate::error::Result<PackageMetadata> {
        Err(crate::CanonicalManager::registry_unavailable())
    }
    async fn download_package(&self, _spec: &PackageSpec) -> crate::error::Result<PackageDownload> {
        Err(crate::CanonicalManager::registry_unavailable())
    }
}
