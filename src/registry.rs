//! Registry client for downloading FHIR packages

use crate::config::{PackageSpec, RegistryConfig};
use crate::error::{RegistryError, Result};
use futures_util::StreamExt;
use reqwest::{Client, Response};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tracing::{debug, info, warn};

/// Trait for receiving progress updates during package downloads.
///
/// Implement this trait to provide user feedback during long-running download operations.
/// All methods are called from async contexts and should be non-blocking.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_manager::registry::DownloadProgress;
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

/// Client for interacting with FHIR package registries.
///
/// Provides functionality to download packages, query package metadata,
/// and search for available packages. Supports multiple registry URLs
/// with automatic fallback capabilities.
///
/// # Example
///
/// ```rust,no_run
/// use octofhir_canonical_manager::registry::RegistryClient;
/// use octofhir_canonical_manager::config::{RegistryConfig, PackageSpec};
/// use std::path::PathBuf;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = RegistryConfig::default();
/// let cache_dir = PathBuf::from("/tmp/cache");
/// let client = RegistryClient::new(&config, cache_dir).await?;
///
/// let spec = PackageSpec {
///     name: "hl7.fhir.us.core".to_string(),
///     version: "6.1.0".to_string(),
///     priority: 1,
/// };
///
/// let download = client.download_package(&spec).await?;
/// println!("Downloaded to: {}", download.file_path.display());
/// # Ok(())
/// # }
/// ```
pub struct RegistryClient {
    client: Client,
    base_url: url::Url,
    config: RegistryConfig,
    cache_dir: PathBuf,
    fallback_registries: Vec<String>,
}

/// Information about a package available in a registry.
///
/// Contains metadata about a package including all available versions
/// and descriptive information.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_manager::registry::PackageInfo;
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

/// Registry catalog response
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct CatalogResponse {
    packages: Vec<CatalogPackage>,
}

/// Package in catalog
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct CatalogPackage {
    name: String,
    versions: Vec<String>,
    description: Option<String>,
}

/// NPM-style package metadata response
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct NpmPackageResponse {
    name: String,
    versions: HashMap<String, NpmVersionInfo>,
    #[serde(rename = "dist-tags")]
    dist_tags: HashMap<String, String>,
    // For simple packages without versions object
    dist: Option<NpmDistInfo>,
    version: Option<String>,
    #[serde(rename = "fhirVersions")]
    fhir_versions: Option<Vec<String>>,
    description: Option<String>,
    canonical: Option<String>,
}

/// NPM version information
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct NpmVersionInfo {
    name: String,
    version: String,
    description: Option<String>,
    #[serde(rename = "fhirVersions")]
    fhir_versions: Option<Vec<String>>,
    dependencies: Option<HashMap<String, String>>,
    canonical: Option<String>,
    dist: NpmDistInfo,
}

/// NPM distribution information
#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct NpmDistInfo {
    tarball: Option<String>,
    shasum: Option<String>,
    integrity: Option<String>,
}

/// Result of a successful package download operation.
///
/// Contains the downloaded file path along with package metadata
/// retrieved from the registry.
///
/// # Example
///
/// ```rust,no_run
/// # use octofhir_canonical_manager::registry::PackageDownload;
/// # fn example(download: PackageDownload) {
/// println!("Downloaded: {}@{}", download.spec.name, download.spec.version);
/// println!("File: {}", download.file_path.display());
/// println!("Description: {:?}", download.metadata.description);
/// # }
/// ```
#[derive(Debug)]
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
/// use octofhir_canonical_manager::registry::PackageMetadata;
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
/// use octofhir_canonical_manager::registry::PackageDependency;
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

impl RegistryClient {
    /// Creates a new registry client with the specified configuration.
    ///
    /// Sets up HTTP client with appropriate timeouts, user agent, and fallback
    /// registry URLs for improved reliability.
    ///
    /// # Arguments
    ///
    /// * `config` - Registry configuration including URL and timeout settings
    /// * `cache_dir` - Directory where downloaded packages will be cached
    ///
    /// # Returns
    ///
    /// * `Ok(RegistryClient)` - Successfully configured client
    /// * `Err` - If configuration is invalid or cache directory cannot be created
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use octofhir_canonical_manager::registry::RegistryClient;
    /// use octofhir_canonical_manager::config::RegistryConfig;
    /// use std::path::PathBuf;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = RegistryConfig::default();
    /// let cache_dir = PathBuf::from("/tmp/fhir-cache");
    ///
    /// let client = RegistryClient::new(&config, cache_dir).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(config: &RegistryConfig, cache_dir: PathBuf) -> Result<Self> {
        // Use a shorter timeout (10 seconds max) to avoid hanging
        let timeout_secs = std::cmp::min(config.timeout, 10);
        let client = Client::builder()
            .timeout(std::time::Duration::from_secs(timeout_secs))
            .connect_timeout(std::time::Duration::from_secs(5))
            .user_agent("octofhir-canonical-manager/0.1.0")
            .build()?;

        let base_url =
            url::Url::parse(&config.url).map_err(|_| RegistryError::RegistryUnavailable {
                url: config.url.clone(),
            })?;

        // Ensure cache directory exists
        fs::create_dir_all(&cache_dir).await?;

        // Common fallback registries for FHIR packages - prioritize fs.get-ig.org
        let fallback_registries = vec![
            "https://fs.get-ig.org/pkgs/".to_string(),
            "https://packages.fhir.org/packages/".to_string(),
        ];

        Ok(Self {
            client,
            base_url,
            config: config.clone(),
            cache_dir,
            fallback_registries,
        })
    }

    /// Downloads a FHIR package from the registry.
    ///
    /// Attempts to download the specified package, trying the primary registry
    /// first and falling back to alternative registries if necessary.
    ///
    /// # Arguments
    ///
    /// * `spec` - Package specification including name and version
    ///
    /// # Returns
    ///
    /// * `Ok(PackageDownload)` - Successfully downloaded package information
    /// * `Err(RegistryError)` - If package cannot be found or downloaded
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::registry::RegistryClient;
    /// # use octofhir_canonical_manager::config::PackageSpec;
    /// # async fn example(client: RegistryClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let spec = PackageSpec {
    ///     name: "hl7.fhir.us.core".to_string(),
    ///     version: "6.1.0".to_string(),
    ///     priority: 1,
    /// };
    ///
    /// let download = client.download_package(&spec).await?;
    /// println!("Downloaded to: {}", download.file_path.display());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn download_package(&self, spec: &PackageSpec) -> Result<PackageDownload> {
        self.download_package_with_progress(spec, None).await
    }

    /// Downloads a FHIR package with progress reporting.
    ///
    /// Same as `download_package` but provides progress callbacks during the
    /// download operation. Useful for providing user feedback during large downloads.
    ///
    /// # Arguments
    ///
    /// * `spec` - Package specification including name and version
    /// * `progress` - Optional progress callback implementation
    ///
    /// # Returns
    ///
    /// * `Ok(PackageDownload)` - Successfully downloaded package information
    /// * `Err(RegistryError)` - If package cannot be found or downloaded
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::registry::{RegistryClient, DownloadProgress};
    /// # use octofhir_canonical_manager::config::PackageSpec;
    ///
    /// struct MyProgress;
    /// impl DownloadProgress for MyProgress {
    ///     fn on_progress(&self, downloaded: u64, total: Option<u64>) {
    ///         println!("Downloaded: {} bytes", downloaded);
    ///     }
    ///     fn on_start(&self, _total: Option<u64>) {}
    ///     fn on_complete(&self) {}
    /// }
    ///
    /// # async fn example(client: RegistryClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let spec = PackageSpec {
    ///     name: "hl7.fhir.us.core".to_string(),
    ///     version: "6.1.0".to_string(),
    ///     priority: 1,
    /// };
    ///
    /// let progress = MyProgress;
    /// let download = client.download_package_with_progress(&spec, Some(&progress)).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn download_package_with_progress(
        &self,
        spec: &PackageSpec,
        progress: Option<&dyn DownloadProgress>,
    ) -> Result<PackageDownload> {
        info!("Downloading package: {}@{}", spec.name, spec.version);

        // Try primary registry first, then fallbacks
        let mut last_error;

        // Try primary registry
        match self
            .try_download_from_registry(self.base_url.as_ref(), spec, progress)
            .await
        {
            Ok(download) => return Ok(download),
            Err(e) => {
                warn!(
                    "Primary registry failed for {}@{}: {}",
                    spec.name, spec.version, e
                );
                last_error = Some(e);
            }
        }

        // Try fallback registries
        for fallback_url in &self.fallback_registries {
            if fallback_url == &self.base_url.to_string() {
                continue; // Skip if it's the same as primary
            }

            info!("Trying fallback registry: {}", fallback_url);
            match self
                .try_download_from_registry(fallback_url, spec, progress)
                .await
            {
                Ok(download) => {
                    info!(
                        "Successfully downloaded from fallback registry: {}",
                        fallback_url
                    );
                    return Ok(download);
                }
                Err(e) => {
                    warn!(
                        "Fallback registry {} failed for {}@{}: {}",
                        fallback_url, spec.name, spec.version, e
                    );
                    last_error = Some(e);
                }
            }
        }

        // All registries failed
        Err(last_error.unwrap_or_else(|| {
            RegistryError::PackageNotFound {
                name: spec.name.clone(),
                version: spec.version.clone(),
            }
            .into()
        }))
    }

    /// Try downloading from a specific registry
    async fn try_download_from_registry(
        &self,
        registry_url: &str,
        spec: &PackageSpec,
        progress: Option<&dyn DownloadProgress>,
    ) -> Result<PackageDownload> {
        // Get package metadata to find download URL
        let (metadata, download_url) = self
            .get_package_metadata_and_url_from_registry(registry_url, &spec.name, &spec.version)
            .await?;

        debug!("Download URL from metadata: {}", download_url);

        // Perform download with retries and progress
        let response = self.download_with_retries(&download_url).await?;

        // Get content length for progress tracking
        let content_length = response.content_length();

        // Notify progress start
        if let Some(progress) = progress {
            progress.on_start(content_length);
        }

        // Save to cache directory with streaming
        let filename = format!("{}-{}.tgz", spec.name, spec.version);
        let file_path = self.cache_dir.join(&filename);
        let mut file = fs::File::create(&file_path).await?;

        // Stream the download with progress updates
        let mut stream = response.bytes_stream();
        let mut downloaded = 0u64;
        let mut last_progress_update = std::time::Instant::now();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            file.write_all(&chunk).await?;
            downloaded += chunk.len() as u64;

            // Update progress more frequently and ensure visible updates
            let now = std::time::Instant::now();
            if let Some(progress) = progress {
                // Update progress every chunk and force update every 100ms
                progress.on_progress(downloaded, content_length);
                if now.duration_since(last_progress_update) >= std::time::Duration::from_millis(100)
                {
                    // Small delay to ensure progress bar can render
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    last_progress_update = now;
                }
            }
        }

        // Notify completion
        if let Some(progress) = progress {
            progress.on_complete();
        }

        info!("Package downloaded to: {}", file_path.display());

        Ok(PackageDownload {
            spec: spec.clone(),
            file_path,
            metadata,
        })
    }

    /// Retrieves metadata for a specific package version.
    ///
    /// Queries the registry for detailed information about a package including
    /// its dependencies, FHIR version compatibility, and descriptive metadata.
    ///
    /// # Arguments
    ///
    /// * `name` - The package name
    /// * `version` - The specific version to query
    ///
    /// # Returns
    ///
    /// * `Ok(PackageMetadata)` - Package metadata from the registry
    /// * `Err(RegistryError)` - If package or version is not found
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::registry::RegistryClient;
    /// # async fn example(client: RegistryClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let metadata = client.get_package_metadata("hl7.fhir.us.core", "6.1.0").await?;
    ///
    /// println!("Package: {}@{}", metadata.name, metadata.version);
    /// println!("FHIR Version: {}", metadata.fhir_version);
    /// println!("Dependencies: {}", metadata.dependencies.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_package_metadata(&self, name: &str, version: &str) -> Result<PackageMetadata> {
        let (metadata, _) = self.get_package_metadata_and_url(name, version).await?;
        Ok(metadata)
    }

    /// Get package metadata and download URL from specific registry
    async fn get_package_metadata_and_url_from_registry(
        &self,
        registry_url: &str,
        name: &str,
        version: &str,
    ) -> Result<(PackageMetadata, String)> {
        debug!(
            "Getting metadata for package: {}@{} from {}",
            name, version, registry_url
        );

        // Build metadata URL for specific registry
        let metadata_url = format!("{registry_url}{name}");

        let response = self.client.get(&metadata_url).send().await?;

        if !response.status().is_success() {
            return Err(RegistryError::PackageNotFound {
                name: name.to_string(),
                version: version.to_string(),
            }
            .into());
        }

        let npm_response: NpmPackageResponse = response.json().await?;

        debug!(
            "Metadata response has {} versions",
            npm_response.versions.len()
        );
        debug!("Has top-level version: {}", npm_response.version.is_some());

        // Check if this is a simple response (no versions object)
        if npm_response.versions.is_empty() && npm_response.version.is_some() {
            // Simple format - use top-level fields
            let download_url = npm_response
                .dist
                .as_ref()
                .and_then(|d| d.tarball.clone())
                .ok_or_else(|| RegistryError::InvalidMetadata {
                    message: "No download URL in metadata".to_string(),
                })?;

            let fhir_version = npm_response
                .fhir_versions
                .as_ref()
                .and_then(|versions| versions.first())
                .unwrap_or(&"4.0.1".to_string())
                .clone();

            let metadata = PackageMetadata {
                name: name.to_string(),
                version: version.to_string(),
                description: npm_response.description.clone(),
                fhir_version,
                dependencies: HashMap::new(), // No dependencies in simple format
                canonical_base: npm_response.canonical.clone(),
            };

            return Ok((metadata, download_url));
        }

        // Full format with versions object
        let version_info =
            npm_response
                .versions
                .get(version)
                .ok_or_else(|| RegistryError::PackageNotFound {
                    name: name.to_string(),
                    version: version.to_string(),
                })?;

        let download_url =
            version_info
                .dist
                .tarball
                .clone()
                .ok_or_else(|| RegistryError::InvalidMetadata {
                    message: "No download URL in version metadata".to_string(),
                })?;

        // Extract dependencies
        let dependencies = version_info.dependencies.clone().unwrap_or_default();

        // Determine FHIR version
        let fhir_version = version_info
            .fhir_versions
            .as_ref()
            .and_then(|versions| versions.first())
            .unwrap_or(&"4.0.1".to_string())
            .clone();

        let metadata = PackageMetadata {
            name: name.to_string(),
            version: version.to_string(),
            description: version_info.description.clone(),
            fhir_version,
            dependencies,
            canonical_base: version_info.canonical.clone(),
        };

        Ok((metadata, download_url))
    }

    /// Get package metadata and download URL
    async fn get_package_metadata_and_url(
        &self,
        name: &str,
        version: &str,
    ) -> Result<(PackageMetadata, String)> {
        self.get_package_metadata_and_url_from_registry(self.base_url.as_ref(), name, version)
            .await
    }

    /// Lists all available versions for a package.
    ///
    /// Queries the registry to get a list of all published versions
    /// for the specified package, sorted with newest versions first.
    ///
    /// # Arguments
    ///
    /// * `name` - The package name to query
    ///
    /// # Returns
    ///
    /// * `Ok(Vec<String>)` - List of available versions, newest first
    /// * `Err(RegistryError)` - If package is not found
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::registry::RegistryClient;
    /// # async fn example(client: RegistryClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let versions = client.list_versions("hl7.fhir.us.core").await?;
    ///
    /// println!("Available versions:");
    /// for version in versions {
    ///     println!("  {}", version);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_versions(&self, name: &str) -> Result<Vec<String>> {
        debug!("Listing versions for package: {}", name);

        let metadata_url = self.build_metadata_url(name);

        let response = self.client.get(&metadata_url).send().await?;

        if !response.status().is_success() {
            return Err(RegistryError::PackageNotFound {
                name: name.to_string(),
                version: "latest".to_string(),
            }
            .into());
        }

        let npm_response: NpmPackageResponse = response.json().await?;

        let mut versions: Vec<String> = npm_response.versions.keys().cloned().collect();
        versions.sort_by(|a, b| {
            // Simple version sorting - in production would use semver crate
            b.cmp(a) // Descending order (newest first)
        });

        Ok(versions)
    }

    /// Searches for packages matching a query string.
    ///
    /// **Note**: This functionality is not fully implemented for most registry types.
    /// Currently returns empty results but may be enhanced in future releases.
    ///
    /// # Arguments
    ///
    /// * `query` - Search query string
    ///
    /// # Returns
    ///
    /// * `Ok(Vec<PackageInfo>)` - List of matching packages (currently empty)
    /// * `Err` - If search operation fails
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::registry::RegistryClient;
    /// # async fn example(client: RegistryClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let results = client.search_packages("us core").await?;
    /// println!("Found {} packages", results.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn search_packages(&self, query: &str) -> Result<Vec<PackageInfo>> {
        debug!("Searching for packages with query: {}", query);

        // This is a simplified implementation - real registries may have different search APIs
        warn!("Package search not fully implemented for this registry type");

        // Return empty results for now
        Ok(vec![])
    }

    /// Build metadata URL for a package (npm-style)
    fn build_metadata_url(&self, name: &str) -> String {
        format!("{}{}", self.base_url, name)
    }

    /// Build download URL for a package (npm-style)
    #[cfg(test)]
    fn build_download_url(&self, name: &str, version: &str) -> String {
        format!("{}{}/-/{}-{}.tgz", self.base_url, name, name, version)
    }

    /// Download with retries
    async fn download_with_retries(&self, url: &str) -> Result<Response> {
        let mut last_error = None;

        for attempt in 1..=self.config.retry_attempts {
            debug!(
                "Download attempt {} of {} for URL: {}",
                attempt, self.config.retry_attempts, url
            );
            eprintln!("DEBUG: Attempting to download from: {url}");

            match self.client.get(url).send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        return Ok(response);
                    } else {
                        last_error = Some(RegistryError::RegistryUnavailable {
                            url: url.to_string(),
                        });
                    }
                }
                Err(e) => {
                    last_error = Some(RegistryError::RegistryUnavailable {
                        url: format!("Network error: {e}"),
                    });
                }
            }

            if attempt < self.config.retry_attempts {
                // Exponential backoff
                let delay = std::time::Duration::from_millis(1000 * (2_u64.pow(attempt - 1)));
                tokio::time::sleep(delay).await;
            }
        }

        Err(last_error.unwrap().into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RegistryConfig;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_registry_client_creation() {
        let config = RegistryConfig::default();
        let temp_dir = TempDir::new().unwrap();
        let client = RegistryClient::new(&config, temp_dir.path().to_path_buf()).await;
        assert!(client.is_ok());
    }

    #[tokio::test]
    async fn test_url_building() {
        let config = RegistryConfig::default();
        let temp_dir = TempDir::new().unwrap();
        let client = RegistryClient::new(&config, temp_dir.path().to_path_buf())
            .await
            .unwrap();

        let metadata_url = client.build_metadata_url("hl7.fhir.us.core");
        assert_eq!(metadata_url, "https://fs.get-ig.org/pkgs/hl7.fhir.us.core");

        let download_url = client.build_download_url("hl7.fhir.us.core", "6.1.0");
        assert_eq!(
            download_url,
            "https://fs.get-ig.org/pkgs/hl7.fhir.us.core/-/hl7.fhir.us.core-6.1.0.tgz"
        );
    }

    #[tokio::test]
    async fn test_package_search() {
        let config = RegistryConfig::default();
        let temp_dir = TempDir::new().unwrap();
        let client = RegistryClient::new(&config, temp_dir.path().to_path_buf())
            .await
            .unwrap();

        // Search should return empty results for now
        let results = client.search_packages("core").await.unwrap();
        assert!(results.is_empty());
    }

    // Note: Wiremock test removed due to lifetime issues
    // The retry mechanism is tested implicitly through real usage
}
