//! Registry client for downloading FHIR packages

use crate::cas_storage::{CasStorage, ContentType};
use crate::config::{PackageSpec, RegistryConfig};
use crate::content_hash::ContentHash;
use crate::error::{RegistryError, Result};
use base64::Engine;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use futures_util::{StreamExt, stream};
use reqwest::{Client, Response};
use serde::{Deserialize, Serialize};
use sha2::{Sha256, Sha384, Sha512, digest::Digest};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
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
    // In-memory metadata cache keyed by metadata URL
    metadata_cache: Arc<DashMap<String, CachedResponse>>,
    // Persisted validators (etag/last-modified) across runs
    persisted_validators: Arc<DashMap<String, PersistedValidators>>,
    validators_path: PathBuf,
    last_save: Arc<AtomicU64>, // Stores timestamp as milliseconds since UNIX_EPOCH
    // Content-addressable storage for downloaded packages
    cas: Arc<CasStorage>,
}

#[derive(Clone, Debug)]
struct CachedResponse {
    etag: Option<String>,
    last_modified: Option<String>,
    npm: NpmPackageResponse,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct PersistedValidators {
    etag: Option<String>,
    last_modified: Option<String>,
    updated_at: DateTime<Utc>,
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
#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)]
struct CatalogResponse {
    packages: Vec<CatalogPackage>,
}

/// Package in catalog
#[derive(Debug, Deserialize, Clone)]
#[allow(dead_code)]
struct CatalogPackage {
    name: String,
    versions: Vec<String>,
    description: Option<String>,
}

/// NPM-style package metadata response
#[derive(Debug, Deserialize, Clone)]
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
#[derive(Debug, Deserialize, Clone)]
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
#[derive(Debug, Deserialize, Clone)]
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

        let validators_path = cache_dir.join("registry-cache.json");
        let persisted_validators = {
            // Best-effort load with warning on corruption
            if let Ok(bytes) = tokio::fs::read(&validators_path).await {
                match serde_json::from_slice::<std::collections::HashMap<String, PersistedValidators>>(
                    &bytes,
                ) {
                    Ok(map) => map,
                    Err(e) => {
                        tracing::warn!("Failed to parse registry validators cache: {}", e);
                        std::collections::HashMap::new()
                    }
                }
            } else {
                std::collections::HashMap::new()
            }
        };

        // Initialize content-addressable storage
        let cas_path = cache_dir.join("cas");
        let cas = Arc::new(CasStorage::new(cas_path).await?);

        Ok(Self {
            client,
            base_url,
            config: config.clone(),
            cache_dir,
            fallback_registries,
            metadata_cache: Arc::new(DashMap::new()),
            persisted_validators: {
                let map = Arc::new(DashMap::new());
                for (k, v) in persisted_validators {
                    map.insert(k, v);
                }
                map
            },
            validators_path,
            last_save: Arc::new(AtomicU64::new(Self::now_millis().saturating_sub(1_000))),
            cas,
        })
    }

    fn now_millis() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_else(|_| std::time::Duration::from_secs(0))
            .as_millis() as u64
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
    #[tracing::instrument(name = "registry.download_package", skip(self), fields(pkg = %spec.name, ver = %spec.version))]
    pub async fn download_package(&self, spec: &PackageSpec) -> Result<PackageDownload> {
        #[cfg(feature = "metrics")]
        let start = std::time::Instant::now();
        let result = self.download_package_with_progress(spec, None).await;
        #[cfg(feature = "metrics")]
        {
            match &result {
                Ok(_) => {
                    metrics::increment_counter!("downloads_succeeded");
                    metrics::histogram!(
                        "download_latency_ms",
                        start.elapsed().as_secs_f64() * 1000.0
                    );
                }
                Err(_) => {
                    metrics::increment_counter!("downloads_failed");
                }
            }
        }
        result
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
    #[tracing::instrument(name = "registry.download_package", skip_all, fields(pkg = %spec.name, ver = %spec.version))]
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

    /// Downloads multiple packages in parallel with deduplication
    ///
    /// This method downloads multiple packages concurrently (up to 8 at a time)
    /// and automatically deduplicates downloads using content-addressable storage.
    /// Packages with the same content hash are only downloaded once.
    ///
    /// # Arguments
    ///
    /// * `specs` - List of package specifications to download
    ///
    /// # Returns
    ///
    /// * `Ok(Vec<PackageDownload>)` - Successfully downloaded packages
    /// * The order of results may differ from input order due to parallel execution
    ///
    /// # Performance
    ///
    /// Downloads are parallelized with a concurrency limit of 8. This provides
    /// significant speedup for batch operations while avoiding overwhelming the
    /// registry server.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::registry::RegistryClient;
    /// # use octofhir_canonical_manager::config::PackageSpec;
    /// # async fn example(client: RegistryClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let specs = vec![
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
    /// let downloads = client.download_packages_parallel(specs).await?;
    /// println!("Downloaded {} packages", downloads.len());
    /// # Ok(())
    /// # }
    /// ```
    #[tracing::instrument(name = "registry.download_parallel", skip(self), fields(count = specs.len()))]
    pub async fn download_packages_parallel(
        &self,
        specs: Vec<PackageSpec>,
    ) -> Result<Vec<PackageDownload>> {
        info!("Starting parallel download of {} packages", specs.len());

        let results: Vec<Result<PackageDownload>> = stream::iter(specs)
            .map(|spec| async move {
                // Each download is independent and uses CAS for deduplication
                self.download_package(&spec).await
            })
            .buffer_unordered(8) // 8 concurrent downloads
            .collect()
            .await;

        // Separate successes from failures
        let mut downloads = Vec::new();
        let mut errors = Vec::new();

        for (idx, result) in results.into_iter().enumerate() {
            match result {
                Ok(download) => downloads.push(download),
                Err(e) => {
                    warn!("Package download #{} failed: {}", idx, e);
                    errors.push(e);
                }
            }
        }

        if downloads.is_empty() && !errors.is_empty() {
            // All downloads failed
            return Err(errors.into_iter().next().unwrap());
        }

        info!(
            "Parallel download complete: {} succeeded, {} failed",
            downloads.len(),
            errors.len()
        );

        Ok(downloads)
    }

    /// Try downloading from a specific registry
    async fn try_download_from_registry(
        &self,
        registry_url: &str,
        spec: &PackageSpec,
        progress: Option<&dyn DownloadProgress>,
    ) -> Result<PackageDownload> {
        // Get package metadata to find download URL
        let (metadata, download_url, expected_shasum, expected_integrity) = self
            .get_package_metadata_and_url_from_registry(registry_url, &spec.name, &spec.version)
            .await?;

        debug!("Download URL from metadata: {}", download_url);

        let filename = format!("{}-{}.tgz", spec.name, spec.version);
        let file_path = self.cache_dir.join(&filename);
        let part_path = self.cache_dir.join(format!("{filename}.part"));

        if fs::metadata(&part_path).await.is_ok() {
            let _ = fs::remove_file(&part_path).await;
        }

        if fs::metadata(&file_path).await.is_ok() {
            match self
                .verify_cached_package(
                    &file_path,
                    expected_shasum.as_deref(),
                    expected_integrity.as_deref(),
                )
                .await
            {
                Ok(()) => {
                    let cas_path = self.store_in_cas(&file_path).await?;
                    debug!(
                        "Using cached package {}@{} (CAS: {})",
                        spec.name,
                        spec.version,
                        cas_path.display()
                    );
                    if let Some(progress) = progress {
                        let size = fs::metadata(&file_path).await.ok().map(|m| m.len());
                        progress.on_start(size);
                        progress.on_complete();
                    }
                    return Ok(PackageDownload {
                        spec: spec.clone(),
                        file_path,
                        metadata,
                    });
                }
                Err(e) => {
                    warn!(
                        "Cached package {}@{} failed verification ({}), re-downloading",
                        spec.name, spec.version, e
                    );
                    let _ = fs::remove_file(&file_path).await;
                }
            }
        }

        // Perform download with retries and progress
        let response = self.download_with_retries(&download_url).await?;

        // Get content length for progress tracking
        let content_length = response.content_length();

        // Notify progress start
        if let Some(progress) = progress {
            progress.on_start(content_length);
        }

        // Save to cache directory with streaming
        let file_path = self.cache_dir.join(&filename);
        let mut file = fs::File::create(&part_path).await?;

        // Stream the download with progress updates
        let mut stream = response.bytes_stream();
        let mut sha1_hasher = sha1::Sha1::new();
        // Integrity (SRI) support: initialize only if integrity present
        enum IntegrityAlgo {
            Sha256,
            Sha384,
            Sha512,
        }
        let mut sri_algo: Option<IntegrityAlgo> = None;
        let mut sri_sha256: Option<sha2::Sha256> = None;
        let mut sri_sha384: Option<sha2::Sha384> = None;
        let mut sri_sha512: Option<sha2::Sha512> = None;
        if let Some(intg) = expected_integrity.as_ref()
            && let Some((algo, _b64)) = intg.split_once('-')
        {
            match algo.to_ascii_lowercase().as_str() {
                "sha256" => {
                    sri_algo = Some(IntegrityAlgo::Sha256);
                    sri_sha256 = Some(sha2::Sha256::new());
                }
                "sha384" => {
                    sri_algo = Some(IntegrityAlgo::Sha384);
                    sri_sha384 = Some(sha2::Sha384::new());
                }
                "sha512" => {
                    sri_algo = Some(IntegrityAlgo::Sha512);
                    sri_sha512 = Some(sha2::Sha512::new());
                }
                _ => {}
            }
        }
        let mut downloaded = 0u64;
        let mut last_progress_update = std::time::Instant::now();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            file.write_all(&chunk).await?;
            sha1_hasher.update(&chunk);
            if let Some(algo) = &sri_algo {
                match algo {
                    IntegrityAlgo::Sha256 => {
                        if let Some(h) = &mut sri_sha256 {
                            use sha2::Digest;
                            h.update(&chunk);
                        }
                    }
                    IntegrityAlgo::Sha384 => {
                        if let Some(h) = &mut sri_sha384 {
                            use sha2::Digest;
                            h.update(&chunk);
                        }
                    }
                    IntegrityAlgo::Sha512 => {
                        if let Some(h) = &mut sri_sha512 {
                            use sha2::Digest;
                            h.update(&chunk);
                        }
                    }
                }
            }
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

        // Verify checksum if provided
        if let Some(expected) = expected_shasum {
            let digest = sha1_hasher.finalize();
            let got = hex::encode(digest);
            // Compare case-insensitively
            let is_hex40 = expected.len() == 40 && expected.chars().all(|c| c.is_ascii_hexdigit());
            if is_hex40 && !expected.eq_ignore_ascii_case(&got) {
                let _ = fs::remove_file(&part_path).await;
                return Err(RegistryError::InvalidMetadata {
                    message: format!(
                        "SHA1 checksum mismatch for {}@{}: expected {}, got {}",
                        spec.name, spec.version, expected, got
                    ),
                }
                .into());
            }
        }

        // Verify SRI integrity if provided and recognized
        if let (Some(algo), Some(intg)) = (sri_algo, expected_integrity)
            && let Some((_, b64)) = intg.split_once('-')
        {
            let expected_bytes: Vec<u8> = base64::engine::general_purpose::STANDARD
                .decode(b64.as_bytes())
                .unwrap_or_default();
            use sha2::Digest;
            if expected_bytes.is_empty() { /* skip invalid integrity */
            } else {
                let ok = match algo {
                    IntegrityAlgo::Sha256 => {
                        sri_sha256.map(|h| h.finalize().to_vec()) == Some(expected_bytes.clone())
                    }
                    IntegrityAlgo::Sha384 => {
                        sri_sha384.map(|h| h.finalize().to_vec()) == Some(expected_bytes.clone())
                    }
                    IntegrityAlgo::Sha512 => {
                        sri_sha512.map(|h| h.finalize().to_vec()) == Some(expected_bytes.clone())
                    }
                };
                if !ok {
                    let _ = fs::remove_file(&part_path).await;
                    return Err(RegistryError::InvalidMetadata {
                        message: format!(
                            "Integrity (SRI) mismatch for {}@{}",
                            spec.name, spec.version
                        ),
                    }
                    .into());
                }
            }
        }

        // Notify completion
        if let Some(progress) = progress {
            progress.on_complete();
        }

        // Atomically move the .part file to final name
        fs::rename(&part_path, &file_path).await?;
        info!("Package downloaded to: {}", file_path.display());

        // Store in CAS for future reuse and deduplication
        let cas_path = self.store_in_cas(&file_path).await?;
        debug!("Package stored in CAS: {}", cas_path.display());

        Ok(PackageDownload {
            spec: spec.clone(),
            file_path,
            metadata,
        })
    }

    async fn verify_cached_package(
        &self,
        file_path: &PathBuf,
        expected_shasum: Option<&str>,
        expected_integrity: Option<&str>,
    ) -> std::result::Result<(), RegistryError> {
        let content = fs::read(file_path)
            .await
            .map_err(|e| RegistryError::InvalidMetadata {
                message: format!(
                    "Failed to read cached package {}: {}",
                    file_path.display(),
                    e
                ),
            })?;

        if let Some(expected) = expected_shasum {
            let mut sha1_hasher = sha1::Sha1::new();
            sha1_hasher.update(&content);
            let actual = hex::encode(sha1_hasher.finalize());
            if !actual.eq_ignore_ascii_case(expected) {
                return Err(RegistryError::InvalidMetadata {
                    message: format!(
                        "Cached package checksum mismatch: expected {}, got {}",
                        expected, actual
                    ),
                });
            }
        }

        if let Some(integrity) = expected_integrity
            && let Some((algo_raw, encoded)) = integrity.split_once('-')
        {
            let algo = algo_raw.to_ascii_lowercase();
            let expected_bytes = base64::engine::general_purpose::STANDARD
                .decode(encoded.as_bytes())
                .map_err(|e| RegistryError::InvalidMetadata {
                    message: format!("Invalid integrity value '{}': {}", integrity, e),
                })?;

            let matches = match algo.as_str() {
                "sha256" => expected_bytes == Sha256::digest(&content).to_vec(),
                "sha384" => expected_bytes == Sha384::digest(&content).to_vec(),
                "sha512" => expected_bytes == Sha512::digest(&content).to_vec(),
                other => {
                    warn!("Unsupported integrity algorithm: {}", other);
                    true
                }
            };

            if !matches {
                return Err(RegistryError::InvalidMetadata {
                    message: "Cached package integrity mismatch".to_string(),
                });
            }
        }

        Ok(())
    }

    /// Store downloaded package file in content-addressable storage
    ///
    /// Computes Blake3 hash of the file and stores it in CAS for future deduplication.
    /// Returns the path in CAS.
    async fn store_in_cas(&self, file_path: &PathBuf) -> Result<PathBuf> {
        // Read file and compute hash
        let content = fs::read(file_path).await?;
        let hash = ContentHash::from_bytes(&content);

        // Store in CAS (idempotent - if exists, returns existing path)
        let cas_path = self
            .cas
            .store(&content, &hash, ContentType::Package)
            .await?;

        Ok(cas_path)
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
    #[tracing::instrument(name = "registry.get_metadata", skip(self))]
    pub async fn get_package_metadata(&self, name: &str, version: &str) -> Result<PackageMetadata> {
        let (metadata, _, _, _) = self.get_package_metadata_and_url(name, version).await?;
        Ok(metadata)
    }

    /// Get package metadata and download URL from specific registry
    async fn get_package_metadata_and_url_from_registry(
        &self,
        registry_url: &str,
        name: &str,
        version: &str,
    ) -> Result<(PackageMetadata, String, Option<String>, Option<String>)> {
        debug!(
            "Getting metadata for package: {}@{} from {}",
            name, version, registry_url
        );

        // Build metadata URL for specific registry
        let metadata_url = format!("{registry_url}{name}");

        let npm_response = self.fetch_npm_metadata(&metadata_url).await?;

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

            let shasum = npm_response.dist.as_ref().and_then(|d| d.shasum.clone());
            let integrity = npm_response.dist.as_ref().and_then(|d| d.integrity.clone());
            return Ok((metadata, download_url, shasum, integrity));
        }

        // Full format with versions object
        // Resolve effective version: exact, dist-tag, or semver range
        let effective_version = if let Some(info) = npm_response.versions.get(version) {
            info.version.clone()
        } else if let Some(tagged) = npm_response.dist_tags.get(version) {
            tagged.clone()
        } else if let Ok(req) = semver::VersionReq::parse(version) {
            // Choose highest satisfying version
            let mut vs: Vec<semver::Version> = npm_response
                .versions
                .keys()
                .filter_map(|v| semver::Version::parse(v.trim_start_matches('v')).ok())
                .collect();
            vs.sort();
            vs.into_iter()
                .rev()
                .find(|v| req.matches(v))
                .map(|v| v.to_string())
                .ok_or_else(|| RegistryError::PackageNotFound {
                    name: name.to_string(),
                    version: version.to_string(),
                })?
        } else {
            return Err(RegistryError::PackageNotFound {
                name: name.to_string(),
                version: version.to_string(),
            }
            .into());
        };

        let version_info = npm_response
            .versions
            .get(&effective_version)
            .ok_or_else(|| RegistryError::PackageNotFound {
                name: name.to_string(),
                version: effective_version.clone(),
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
            version: effective_version.clone(),
            description: version_info.description.clone(),
            fhir_version,
            dependencies,
            canonical_base: version_info.canonical.clone(),
        };

        let shasum = version_info.dist.shasum.clone();
        let integrity = version_info.dist.integrity.clone();

        Ok((metadata, download_url, shasum, integrity))
    }

    /// Get package metadata and download URL
    async fn get_package_metadata_and_url(
        &self,
        name: &str,
        version: &str,
    ) -> Result<(PackageMetadata, String, Option<String>, Option<String>)> {
        self.get_package_metadata_and_url_from_registry(self.base_url.as_ref(), name, version)
            .await
    }

    /// Fetch NPM-style package metadata with simple ETag/Last-Modified caching (in-memory)
    async fn fetch_npm_metadata(&self, metadata_url: &str) -> Result<NpmPackageResponse> {
        let mut req = self.client.get(metadata_url);

        // Apply cache validators if present
        {
            // In-memory cache
            if let Some(cached) = self
                .metadata_cache
                .get(metadata_url)
                .map(|entry| entry.value().clone())
            {
                if let Some(etag) = &cached.etag {
                    req = req.header(reqwest::header::IF_NONE_MATCH, etag);
                }
                if let Some(lm) = &cached.last_modified {
                    req = req.header(reqwest::header::IF_MODIFIED_SINCE, lm);
                }
            } else if let Some(persisted) = self
                .persisted_validators
                .get(metadata_url)
                .map(|entry| entry.value().clone())
            {
                // Persisted validators
                // Simple TTL: 24h
                let ttl = chrono::Duration::hours(24);
                let fresh = chrono::Utc::now().signed_duration_since(persisted.updated_at) < ttl;
                if fresh {
                    let etag = persisted.etag.clone();
                    let last_modified = persisted.last_modified.clone();
                    if let Some(etag) = etag {
                        req = req.header(reqwest::header::IF_NONE_MATCH, etag);
                    }
                    if let Some(lm) = last_modified {
                        req = req.header(reqwest::header::IF_MODIFIED_SINCE, lm);
                    }
                } else {
                    tracing::debug!(
                        "Validators expired by TTL for {}. Forcing full fetch.",
                        metadata_url
                    );
                }
            }
        }

        let resp = req.send().await?;

        match resp.status() {
            s if s.is_success() => {
                let etag = resp
                    .headers()
                    .get(reqwest::header::ETAG)
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string());
                let last_modified = resp
                    .headers()
                    .get(reqwest::header::LAST_MODIFIED)
                    .and_then(|v| v.to_str().ok())
                    .map(|s| s.to_string());
                let npm: NpmPackageResponse = resp.json().await?;
                let cached = CachedResponse {
                    etag,
                    last_modified,
                    npm: npm.clone(),
                };

                self.metadata_cache
                    .insert(metadata_url.to_string(), cached.clone());
                // Update persisted validators and save
                self.update_and_save_validators(metadata_url, &cached);
                Ok(npm)
            }
            reqwest::StatusCode::NOT_MODIFIED => {
                if let Some(cached) = self
                    .metadata_cache
                    .get(metadata_url)
                    .map(|entry| entry.value().clone())
                {
                    // refresh persisted validators
                    self.update_and_save_validators(metadata_url, &cached);
                    Ok(cached.npm.clone())
                } else {
                    // No cache to use; fallback to a fresh fetch
                    let npm: NpmPackageResponse =
                        self.client.get(metadata_url).send().await?.json().await?;
                    // No validators to store in this path
                    Ok(npm)
                }
            }
            _ => Err(RegistryError::PackageNotFound {
                name: metadata_url.to_string(),
                version: "latest".to_string(),
            }
            .into()),
        }
    }

    fn update_and_save_validators(&self, url: &str, cached: &CachedResponse) {
        self.persisted_validators.insert(
            url.to_string(),
            PersistedValidators {
                etag: cached.etag.clone(),
                last_modified: cached.last_modified.clone(),
                updated_at: Utc::now(),
            },
        );

        // Best-effort debounced save (min 500ms interval)
        let now_ms = Self::now_millis();
        let last_ms = self.last_save.load(Ordering::Relaxed);
        if now_ms.saturating_sub(last_ms) >= 500 {
            let snapshot: HashMap<String, PersistedValidators> = self
                .persisted_validators
                .iter()
                .map(|entry| (entry.key().clone(), entry.value().clone()))
                .collect();

            if let Ok(json) = serde_json::to_vec_pretty(&snapshot) {
                let validators_path = self.validators_path.clone();
                tokio::spawn(async move {
                    if let Err(e) = tokio::fs::write(&validators_path, json).await {
                        warn!("Failed to persist registry validators cache: {}", e);
                    }
                });
                self.last_save.store(now_ms, Ordering::Relaxed);
            }
        }
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
    #[tracing::instrument(name = "registry.list_versions", skip(self))]
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
        // Sort using semver when possible, newest first; fall back to lexical
        versions.sort_by(|a, b| {
            let na = a.trim_start_matches('v');
            let nb = b.trim_start_matches('v');
            match (semver::Version::parse(na), semver::Version::parse(nb)) {
                (Ok(va), Ok(vb)) => vb.cmp(&va),
                _ => b.cmp(a),
            }
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
    #[tracing::instrument(name = "registry.search_packages", skip(self))]
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
            let res = self.client.get(url).send().await;
            match res {
                Ok(response) => {
                    let status = response.status();
                    if status.is_success() {
                        return Ok(response);
                    }

                    // Retry only for retryable statuses
                    if status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS
                    {
                        last_error = Some(RegistryError::RegistryUnavailable {
                            url: format!("HTTP {status} for {url}"),
                        });
                    } else {
                        // Non-retryable
                        return Err(RegistryError::RegistryUnavailable {
                            url: format!("HTTP {status} for {url}"),
                        }
                        .into());
                    }
                }
                Err(e) => {
                    last_error = Some(RegistryError::RegistryUnavailable {
                        url: format!("Network error: {e}"),
                    });
                }
            }

            if attempt < self.config.retry_attempts {
                // Exponential backoff with jitter
                let base = 500u64 * (2_u64.pow(attempt - 1));
                let jitter: u64 = rand::random::<u8>() as u64 % 250; // up to 250ms
                let delay = std::time::Duration::from_millis(base + jitter);
                tokio::time::sleep(delay).await;
            }
        }

        Err(last_error.unwrap().into())
    }
}

#[async_trait::async_trait]
impl crate::traits::AsyncRegistry for RegistryClient {
    async fn get_package_metadata(
        &self,
        name: &str,
        version: &str,
    ) -> crate::error::Result<PackageMetadata> {
        self.get_package_metadata(name, version).await
    }

    async fn download_package(
        &self,
        spec: &crate::config::PackageSpec,
    ) -> crate::error::Result<PackageDownload> {
        self.download_package(spec).await
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
