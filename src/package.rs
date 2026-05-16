//! Package extraction and validation

use crate::error::{PackageError, Result};
use crate::registry::PackageDownload;
use flate2::read::GzDecoder;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use tar::Archive;
use tokio::fs;
// use tokio::io::AsyncReadExt;
use tracing::{debug, info, warn};

/// Recommended fan-out for per-file CPU-bound work inside one package
/// (JSON decode, hashing). Capped at 16 because the tokio blocking pool
/// and the OS file-descriptor table are the real bottlenecks above that.
fn num_cpus_hint() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get().min(16))
        .unwrap_or(4)
}

/// Service for extracting and processing FHIR Implementation Guide packages.
///
/// The `PackageExtractor` handles downloading, extracting, and parsing FHIR packages
/// in the standard .tgz format. It validates package structure, parses manifests,
/// and extracts individual FHIR resources.
///
/// # Example
///
/// ```rust,no_run
/// use octofhir_canonical_manager::package::PackageExtractor;
/// use octofhir_canonical_manager::registry::PackageDownload;
/// use std::path::PathBuf;
///
/// # async fn example(download: PackageDownload) -> Result<(), Box<dyn std::error::Error>> {
/// let extractor = PackageExtractor::new(PathBuf::from("/tmp/cache"));
///
/// // Extract a downloaded package
/// let extracted = extractor.extract_package(download).await?;
/// println!("Extracted {} resources", extracted.resources.len());
/// # Ok(())
/// # }
/// ```
pub struct PackageExtractor {
    cache_dir: PathBuf,
    temp_dir: PathBuf,
    /// When `true`, skip writing FHIR resource JSON files
    /// (`package/*.json`, excluding `package.json` and `.index.json`)
    /// to disk during extraction. The bytes are still captured in
    /// `ExtractedPackage.raw_bytes` so storage backends that don't
    /// need an on-disk layout (Postgres, in-memory, etc.) can ingest
    /// the package without the per-resource write syscall.
    ///
    /// Default `false` (write everything) preserves backwards
    /// compatibility with the `populate_fhir_cache` shim and the
    /// sequential install path which still reads files back from disk.
    skip_resource_disk_write: bool,
}

/// Index of files contained in a FHIR package.
///
/// Parsed from the `.index.json` file that accompanies FHIR packages,
/// providing metadata about each resource file in the package.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_manager::package::PackageIndex;
/// use std::collections::HashMap;
///
/// // Typically loaded from .index.json
/// let index = PackageIndex {
///     files: HashMap::new(),
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageIndex {
    pub files: HashMap<String, IndexEntry>,
}

/// Metadata for a single file in a FHIR package index.
///
/// Contains FHIR-specific metadata about a resource file, typically
/// extracted from the resource content during package creation.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_manager::package::IndexEntry;
///
/// let entry = IndexEntry {
///     resource_type: Some("Patient".to_string()),
///     id: Some("example".to_string()),
///     url: Some("http://example.com/Patient/example".to_string()),
///     version: Some("1.0.0".to_string()),
///     kind: Some("resource".to_string()),
///     type_field: None,
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexEntry {
    #[serde(rename = "resourceType")]
    pub resource_type: Option<String>,
    pub id: Option<String>,
    pub url: Option<String>,
    pub version: Option<String>,
    pub kind: Option<String>,
    pub type_field: Option<String>,
}

/// Complete information about an extracted FHIR package.
///
/// Contains the package manifest, all extracted FHIR resources,
/// and metadata about the extraction process.
///
/// # Example
///
/// ```rust,no_run
/// # use octofhir_canonical_manager::package::ExtractedPackage;
/// # fn example(package: ExtractedPackage) {
/// println!("Package: {}@{}", package.name, package.version);
/// println!("Contains {} resources", package.resources.len());
/// println!("Extracted to: {}", package.extraction_path.display());
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct ExtractedPackage {
    pub name: String,
    pub version: String,
    pub manifest: PackageManifest,
    pub resources: Vec<FhirResource>,
    pub extraction_path: PathBuf,
    /// Optional sidecar: original JSON bytes captured during the
    /// streaming tar walk, keyed by absolute resource file path.
    /// Downstream storage paths can hash these directly instead of
    /// re-reading the just-written file. Empty when an `ExtractedPackage`
    /// is constructed from disk-only inputs.
    pub raw_bytes: HashMap<PathBuf, Vec<u8>>,
}

/// FHIR package manifest (package.json) structure.
///
/// Contains metadata about a FHIR package including its name, version,
/// supported FHIR versions, dependencies, and other publication information.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_manager::package::PackageManifest;
/// use std::collections::HashMap;
///
/// let manifest = PackageManifest {
///     name: "hl7.fhir.us.core".to_string(),
///     version: "6.1.0".to_string(),
///     fhir_versions: Some(vec!["4.0.1".to_string()]),
///     dependencies: HashMap::new(),
///     canonical: Some("http://hl7.org/fhir/us/core".to_string()),
///     jurisdiction: Some("US".to_string()),
///     package_type: Some("fhir.ig".to_string()),
///     title: Some("US Core Implementation Guide".to_string()),
///     description: Some("The US Core Implementation Guide".to_string()),
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageManifest {
    pub name: String,
    pub version: String,
    #[serde(rename = "fhirVersions")]
    pub fhir_versions: Option<Vec<String>>,
    #[serde(default)]
    pub dependencies: HashMap<String, String>,
    pub canonical: Option<String>,
    pub jurisdiction: Option<String>,
    #[serde(rename = "type")]
    pub package_type: Option<String>,
    pub title: Option<String>,
    pub description: Option<String>,
}

/// Representation of a FHIR resource extracted from a package.
///
/// Contains the parsed FHIR resource content along with metadata
/// and file path information.
///
/// # Example
///
/// ```rust,no_run
/// # use octofhir_canonical_manager::package::FhirResource;
/// # use std::path::PathBuf;
/// # fn example(resource: FhirResource) {
/// println!("Resource: {} (ID: {})", resource.resource_type, resource.id);
/// if let Some(url) = &resource.url {
///     println!("Canonical URL: {}", url);
/// }
/// println!("File: {}", resource.file_path.display());
/// # }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FhirResource {
    pub resource_type: String,
    pub id: String,
    pub url: Option<String>,
    pub version: Option<String>,
    pub content: serde_json::Value,
    pub file_path: PathBuf,
}

/// Dependency specification for a FHIR package.
///
/// Represents a dependency on another FHIR package with a specific version.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_manager::package::PackageDependency;
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

impl PackageExtractor {
    /// Creates a new package extractor with the specified cache directory.
    ///
    /// # Arguments
    ///
    /// * `cache_dir` - Directory where extracted packages will be stored
    ///
    /// # Example
    ///
    /// ```rust
    /// use octofhir_canonical_manager::package::PackageExtractor;
    /// use std::path::PathBuf;
    ///
    /// let extractor = PackageExtractor::new(PathBuf::from("/tmp/fhir-cache"));
    /// ```
    pub fn new(cache_dir: PathBuf) -> Self {
        let temp_dir = cache_dir.join("temp");
        Self {
            cache_dir,
            temp_dir,
            skip_resource_disk_write: false,
        }
    }

    /// Construct an extractor that skips writing FHIR resource JSON
    /// files to disk. Resource bytes are still captured in
    /// `ExtractedPackage.raw_bytes`. Use this with storage backends
    /// (e.g. Postgres) that ingest from in-memory bytes directly and
    /// never read the on-disk extraction tree.
    ///
    /// `package.json` and `.index.json` are still written so
    /// `validate_package` and `parse_manifest` keep working.
    pub fn new_in_memory(cache_dir: PathBuf) -> Self {
        let temp_dir = cache_dir.join("temp");
        Self {
            cache_dir,
            temp_dir,
            skip_resource_disk_write: true,
        }
    }

    /// Returns the cache directory path.
    pub fn cache_dir(&self) -> &PathBuf {
        &self.cache_dir
    }

    /// Extracts a downloaded FHIR package archive.
    ///
    /// Extracts the .tgz file, validates the package structure, parses the manifest,
    /// and extracts all FHIR resources contained in the package.
    ///
    /// # Arguments
    ///
    /// * `download` - Information about the downloaded package file
    ///
    /// # Returns
    ///
    /// * `Ok(ExtractedPackage)` - Successfully extracted package with all resources
    /// * `Err` - If extraction, validation, or parsing fails
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - The archive cannot be extracted
    /// - The package structure is invalid
    /// - The manifest cannot be parsed
    /// - Resource files are corrupted
    ///
    /// # Example
    ///
    /// ```text
    /// # use octofhir_canonical_manager::package::PackageExtractor;
    /// # use octofhir_canonical_manager::registry::PackageDownload;
    /// # use std::path::PathBuf;
    /// # async fn example(download: PackageDownload) -> Result<(), Box<dyn std::error::Error>> {
    /// let extractor = PackageExtractor::new(PathBuf::from("/tmp/cache"));
    /// let extracted = extractor.extract_package(download).await?;
    ///
    /// println!("Extracted package: {}@{}", extracted.name, extracted.version);
    /// println!("Found {} FHIR resources", extracted.resources.len());
    /// # Ok(())
    /// # }
    /// ```
    #[tracing::instrument(name = "package.extract", skip_all, fields(pkg = %download.spec.name, ver = %download.spec.version))]
    pub async fn extract_package(&self, download: PackageDownload) -> Result<ExtractedPackage> {
        info!(
            "Extracting package: {}@{}",
            download.spec.name, download.spec.version
        );
        #[cfg(feature = "metrics")]
        let start = std::time::Instant::now();

        // Prepare atomic extraction paths
        let final_extraction_path = self
            .temp_dir
            .join(format!("{}-{}", download.spec.name, download.spec.version));
        let tmp_extraction_path =
            final_extraction_path.with_extension(format!("tmp-{}", uuid::Uuid::new_v4()));

        // Ensure tmp dir exists
        fs::create_dir_all(&tmp_extraction_path).await?;

        // Extract the .tgz file into tmp dir. Captures JSON bytes from
        // the tar stream so downstream code can hash + parse without
        // re-reading the just-written files.
        let raw_bytes_tmp = match self
            .extract_tgz_file(&download.file_path, &tmp_extraction_path)
            .await
        {
            Ok(m) => m,
            Err(e) => {
                let _ = fs::remove_dir_all(&tmp_extraction_path).await;
                return Err(e);
            }
        };

        // Move into final location atomically (replace if exists). On
        // rename failure (e.g. cross-device), drop the tmp dir so it does
        // not leak into `temp_dir/`.
        if final_extraction_path.exists() {
            let _ = fs::remove_dir_all(&final_extraction_path).await;
        }
        if let Err(e) = fs::rename(&tmp_extraction_path, &final_extraction_path).await {
            let _ = fs::remove_dir_all(&tmp_extraction_path).await;
            return Err(e.into());
        }

        // Rewrite the raw-bytes map keys from `tmp_extraction_path` →
        // `final_extraction_path` since we renamed the parent dir.
        let raw_bytes: HashMap<PathBuf, Vec<u8>> = raw_bytes_tmp
            .into_iter()
            .filter_map(|(p, b)| {
                let rel = p.strip_prefix(&tmp_extraction_path).ok()?;
                Some((final_extraction_path.join(rel), b))
            })
            .collect();

        // Validate package structure
        self.validate_package(&final_extraction_path).await?;

        // Parse manifest
        let manifest = self.parse_manifest(&final_extraction_path).await?;

        // Extract resources. The raw-bytes sidecar lets
        // `extract_resources` skip the disk re-read for JSON files we
        // already captured during the tar walk.
        let resources = self
            .extract_resources_with_raw(&final_extraction_path, &raw_bytes)
            .await?;

        info!(
            "Successfully extracted package: {}@{} with {} resources",
            download.spec.name,
            download.spec.version,
            resources.len()
        );

        let result = ExtractedPackage {
            name: download.spec.name,
            version: download.spec.version,
            manifest,
            resources,
            extraction_path: final_extraction_path,
            raw_bytes,
        };

        #[cfg(feature = "metrics")]
        {
            metrics::histogram!("extract_latency_ms", start.elapsed().as_secs_f64() * 1000.0);
        }

        Ok(result)
    }

    /// Extract multiple packages in parallel using rayon for CPU parallelism.
    ///
    /// This method processes multiple package downloads concurrently, leveraging
    /// multiple CPU cores for faster extraction. It uses rayon's parallel iterators
    /// for CPU-bound extraction work while coordinating with tokio's async runtime.
    ///
    /// # Arguments
    ///
    /// * `downloads` - Vector of package downloads to extract
    ///
    /// # Returns
    ///
    /// * `Ok(Vec<ExtractedPackage>)` - All successfully extracted packages
    /// * `Err` - If any extraction fails (returns first error)
    ///
    /// # Performance
    ///
    /// Expected speedup: ~7x for 10-20 packages on multi-core systems
    ///
    /// # Example
    ///
    /// ```text
    /// # use octofhir_canonical_manager::package::PackageExtractor;
    /// # use octofhir_canonical_manager::registry::PackageDownload;
    /// # use std::path::PathBuf;
    /// # async fn example(downloads: Vec<PackageDownload>) -> Result<(), Box<dyn std::error::Error>> {
    /// let extractor = PackageExtractor::new(PathBuf::from("/tmp/cache"));
    /// let extracted = extractor.extract_packages_parallel(downloads).await?;
    ///
    /// println!("Extracted {} packages in parallel", extracted.len());
    /// # Ok(())
    /// # }
    /// ```
    #[tracing::instrument(name = "package.extract_parallel", skip_all, fields(count = downloads.len()))]
    pub async fn extract_packages_parallel(
        &self,
        downloads: Vec<PackageDownload>,
    ) -> Result<Vec<ExtractedPackage>> {
        use futures_util::stream::{FuturesUnordered, StreamExt};

        info!(
            "Starting parallel extraction of {} packages",
            downloads.len()
        );
        let start = std::time::Instant::now();

        // Use tokio's FuturesUnordered for concurrent extraction
        // This provides true async concurrency without rayon/tokio mixing issues
        let mut tasks = FuturesUnordered::new();

        for download in downloads {
            let extractor = PackageExtractor {
                cache_dir: self.cache_dir.clone(),
                temp_dir: self.temp_dir.clone(),
                skip_resource_disk_write: self.skip_resource_disk_write,
            };
            tasks.push(async move { extractor.extract_package(download).await });
        }

        // Collect results
        let mut results = Vec::new();
        while let Some(result) = tasks.next().await {
            results.push(result);
        }

        // Separate successes from failures
        let mut extracted = Vec::new();
        let mut errors = Vec::new();

        for result in results {
            match result {
                Ok(pkg) => extracted.push(pkg),
                Err(e) => errors.push(e),
            }
        }

        // If any failed, return first error
        if !errors.is_empty() {
            warn!(
                "Parallel extraction completed with {} failures out of {} packages",
                errors.len(),
                extracted.len() + errors.len()
            );
            if let Some(first) = errors.into_iter().next() {
                return Err(first);
            }
        }

        let duration = start.elapsed();
        info!(
            "Successfully extracted {} packages in parallel in {:?}",
            extracted.len(),
            duration
        );

        #[cfg(feature = "metrics")]
        {
            metrics::histogram!(
                "extract_parallel_latency_ms",
                duration.as_secs_f64() * 1000.0
            );
            metrics::counter!("extract_parallel_packages", extracted.len() as u64);
        }

        Ok(extracted)
    }

    /// Validates the structure of an extracted FHIR package.
    ///
    /// Checks that the package contains the required directories and files,
    /// including the `package/` directory and `package.json` manifest.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the extracted package directory
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Package structure is valid
    /// * `Err(PackageError)` - Package structure is invalid
    ///
    /// # Example
    ///
    /// ```text
    /// # use octofhir_canonical_manager::package::PackageExtractor;
    /// # use std::path::{Path, PathBuf};
    /// let extractor = PackageExtractor::new(PathBuf::from("/tmp"));
    ///
    /// match extractor.validate_package(Path::new("/path/to/package")).await {
    ///     Ok(()) => println!("Package structure is valid"),
    ///     Err(e) => println!("Invalid package: {}", e),
    /// }
    /// ```
    pub async fn validate_package(&self, path: &Path) -> Result<()> {
        debug!("Validating package structure");

        // Check that package directory exists
        let package_dir = path.join("package");
        if !package_dir.exists() {
            return Err(PackageError::ValidationFailed {
                message: "Missing package/ directory".to_string(),
            }
            .into());
        }

        // Check that package.json exists
        let package_json = package_dir.join("package.json");
        if !package_json.exists() {
            return Err(PackageError::MissingManifest.into());
        }

        // Validate package.json is readable (async)
        if let Err(e) = fs::read_to_string(&package_json).await {
            return Err(PackageError::ValidationFailed {
                message: format!("Cannot read package.json: {e}"),
            }
            .into());
        }

        // Check for common FHIR package files
        let expected_files = vec![".index.json"];
        for file in expected_files {
            let file_path = package_dir.join(file);
            if !file_path.exists() {
                warn!("Optional file missing: {}", file);
            }
        }

        debug!("Package structure validation passed");
        Ok(())
    }

    /// Parses the package manifest (package.json) from an extracted package.
    ///
    /// Reads and parses the `package/package.json` file to extract package
    /// metadata including name, version, dependencies, and FHIR version information.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the extracted package directory
    ///
    /// # Returns
    ///
    /// * `Ok(PackageManifest)` - Successfully parsed manifest
    /// * `Err(PackageError)` - If manifest is missing or invalid
    ///
    /// # Example
    ///
    /// ```text
    /// # use octofhir_canonical_manager::package::PackageExtractor;
    /// # use std::path::{Path, PathBuf};
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let extractor = PackageExtractor::new(PathBuf::from("/tmp"));
    ///
    /// let manifest = extractor.parse_manifest(Path::new("/path/to/package")).await?;
    /// println!("Package: {}@{}", manifest.name, manifest.version);
    /// println!("FHIR versions: {:?}", manifest.fhir_versions);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn parse_manifest(&self, path: &Path) -> Result<PackageManifest> {
        debug!("Parsing package manifest");

        // Look for package.json in package/ subdirectory
        let package_json_path = path.join("package").join("package.json");

        if !package_json_path.exists() {
            return Err(PackageError::MissingManifest.into());
        }

        let content = fs::read_to_string(&package_json_path).await?;
        let manifest: PackageManifest =
            serde_json::from_str(&content).map_err(|e| PackageError::ValidationFailed {
                message: format!("Invalid package manifest: {e}"),
            })?;

        Ok(manifest)
    }

    /// Extract a `.tgz` archive into `output_dir`. Streams via the
    /// blocking `tar` + `flate2` crates inside `spawn_blocking`. The
    /// previous fully-async path (`async-tar` + `async-compression`)
    /// was 1.5–2 × slower across all tested IG sizes
    /// (`benches/tar_extract.rs`); blocking-on-thread-pool wins because
    /// `flate2` is a tight CPU loop and the per-entry context-switch
    /// cost in `async-tar` dwarfs any I/O parallelism win for the
    /// disk-local extraction we're doing here.
    ///
    /// Returns a sidecar map of `(out_path -> raw_bytes)` for every
    /// `package/*.json` regular file captured during the walk. Callers
    /// that need the original JSON payload (CAS hashing, in-memory
    /// parse) can avoid the post-extract disk re-read.
    async fn extract_tgz_file(
        &self,
        tgz_path: &Path,
        output_dir: &Path,
    ) -> Result<HashMap<PathBuf, Vec<u8>>> {
        debug!(
            "Extracting .tgz file: {} to {}",
            tgz_path.display(),
            output_dir.display()
        );

        let tgz_path = tgz_path.to_path_buf();
        let output_dir = output_dir.to_path_buf();
        let skip_disk = self.skip_resource_disk_write;
        let (_, raw_bytes) =
            tokio::task::spawn_blocking(move || -> Result<((), HashMap<PathBuf, Vec<u8>>)> {
                use std::io::Read as _;
                let file =
                    std::fs::File::open(&tgz_path).map_err(|e| PackageError::ExtractionFailed {
                        message: format!("Failed to open archive: {e}"),
                    })?;
                let reader = std::io::BufReader::new(file);
                let tar = GzDecoder::new(reader);
                let mut archive = Archive::new(tar);
                let mut raw: HashMap<PathBuf, Vec<u8>> = HashMap::new();

                for entry in archive
                    .entries()
                    .map_err(|e| PackageError::ExtractionFailed {
                        message: format!("Failed to read archive entries: {e}"),
                    })?
                {
                    let mut entry = entry.map_err(|e| PackageError::ExtractionFailed {
                        message: format!("Invalid entry: {e}"),
                    })?;
                    let entry_path = entry.path().map_err(|e| PackageError::ExtractionFailed {
                        message: format!("Invalid entry path: {e}"),
                    })?;
                    let entry_path = entry_path.into_owned();

                    let entry_type = entry.header().entry_type();
                    if !(entry_type.is_file()
                        || entry_type.is_dir()
                        || entry_type.is_gnu_longname()
                        || entry_type.is_gnu_longlink()
                        || entry_type.is_pax_global_extensions()
                        || entry_type.is_pax_local_extensions())
                    {
                        warn!(
                            "Skipping unsafe tar entry type {:?} for path {}",
                            entry_type,
                            entry_path.display()
                        );
                        continue;
                    }

                    let out_path = Self::sanitize_extract_path(&output_dir, &entry_path)?;

                    // For regular `.json` files under `package/` (the
                    // FHIR resource tree), read the entry body once,
                    // optionally write to disk, and keep the bytes for
                    // the caller. Everything else falls through to the
                    // generic `entry.unpack` which handles dirs and
                    // metadata-only tar entries correctly.
                    let is_capturable_json = entry_type.is_file()
                        && entry_path.starts_with("package")
                        && entry_path
                            .extension()
                            .and_then(|s| s.to_str())
                            .is_some_and(|e| e.eq_ignore_ascii_case("json"));

                    // `package.json` and `.index.json` are always
                    // materialised: `parse_manifest`/`validate_package`
                    // still read them from disk regardless of the
                    // in-memory mode.
                    let file_name = entry_path
                        .file_name()
                        .and_then(|s| s.to_str())
                        .unwrap_or("");
                    let is_essential_metadata =
                        file_name == "package.json" || file_name == ".index.json";

                    let need_disk_write = if is_capturable_json {
                        is_essential_metadata || !skip_disk
                    } else {
                        true
                    };

                    if need_disk_write && let Some(parent) = out_path.parent() {
                        std::fs::create_dir_all(parent).map_err(|e| {
                            PackageError::ExtractionFailed {
                                message: format!("Failed to create directories: {e}"),
                            }
                        })?;
                    }

                    if is_capturable_json {
                        let size = entry.header().size().unwrap_or(0) as usize;
                        let mut buf = Vec::with_capacity(size);
                        entry.read_to_end(&mut buf).map_err(|e| {
                            PackageError::ExtractionFailed {
                                message: format!("Failed to read entry bytes: {e}"),
                            }
                        })?;
                        if need_disk_write {
                            std::fs::write(&out_path, &buf).map_err(|e| {
                                PackageError::ExtractionFailed {
                                    message: format!("Failed to write entry: {e}"),
                                }
                            })?;
                        }
                        raw.insert(out_path, buf);
                    } else {
                        entry
                            .unpack(&out_path)
                            .map_err(|e| PackageError::ExtractionFailed {
                                message: format!("Failed to unpack entry: {e}"),
                            })?;
                    }
                }

                Ok(((), raw))
            })
            .await
            .map_err(|e| PackageError::ExtractionFailed {
                message: format!("Task join error: {e}"),
            })??;

        Ok(raw_bytes)
    }

    fn sanitize_extract_path(base: &Path, path: &std::path::Path) -> Result<PathBuf> {
        use std::path::Component;
        let mut out_path = base.to_path_buf();
        for comp in path.components() {
            match comp {
                Component::Prefix(_) | Component::RootDir | Component::CurDir => continue,
                Component::ParentDir => {
                    return Err(PackageError::ExtractionFailed {
                        message: "Archive contains path traversal".into(),
                    }
                    .into());
                }
                Component::Normal(p) => out_path.push(p),
            }
        }
        Ok(out_path)
    }

    /// Like `extract_resources` but uses pre-captured tar bytes from
    /// `raw` instead of re-reading each JSON file from disk. Falls
    /// through to the disk read when a candidate isn't in the map.
    ///
    /// In in-memory mode (`skip_resource_disk_write = true`) the
    /// resource JSON files are not on disk, so candidates come from
    /// the `raw` map's keyset. `package.json` and `.index.json` stay
    /// on disk regardless.
    async fn extract_resources_with_raw(
        &self,
        path: &Path,
        raw: &HashMap<PathBuf, Vec<u8>>,
    ) -> Result<Vec<FhirResource>> {
        debug!("Extracting FHIR resources (in-memory fast path)");

        let package_dir = path.join("package");
        if !package_dir.exists() {
            return Err(PackageError::ValidationFailed {
                message: "Package directory not found".to_string(),
            }
            .into());
        }

        // .index.json may have come from disk OR from the raw map.
        let index_path = package_dir.join(".index.json");
        let index = if let Some(bytes) = raw.get(&index_path) {
            serde_json::from_slice::<PackageIndex>(bytes).ok()
        } else if index_path.exists() {
            let content = fs::read_to_string(&index_path).await?;
            serde_json::from_str::<PackageIndex>(&content).ok()
        } else {
            None
        };

        let mut candidates: Vec<std::path::PathBuf> = Vec::new();
        if self.skip_resource_disk_write {
            // No on-disk resource files: walk the captured-bytes map
            // for everything under `package/` that isn't manifest /
            // index metadata.
            for p in raw.keys() {
                let Some(name) = p.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                if p.extension().and_then(|s| s.to_str()) == Some("json")
                    && !name.starts_with('.')
                    && name != "package.json"
                {
                    candidates.push(p.clone());
                }
            }
        } else {
            let mut dir_entries = fs::read_dir(&package_dir).await?;
            while let Some(entry) = dir_entries.next_entry().await? {
                let p = entry.path();
                let Some(name) = p.file_name().and_then(|n| n.to_str()) else {
                    debug!(
                        "Skipping non-UTF-8 filename under {}",
                        package_dir.display()
                    );
                    continue;
                };
                if p.extension().and_then(|s| s.to_str()) == Some("json")
                    && !name.starts_with('.')
                    && name != "package.json"
                {
                    candidates.push(p);
                }
            }
        }

        use futures_util::stream::{self, StreamExt};
        let concurrency = num_cpus_hint();
        let index_ref = &index;
        let resources: Vec<FhirResource> = stream::iter(candidates.into_iter())
            .map(|p| async move {
                let bytes_owned: Option<Vec<u8>> = raw.get(&p).cloned();
                self.parse_fhir_resource_with_bytes(&p, index_ref, bytes_owned)
                    .await
            })
            .buffer_unordered(concurrency)
            .filter_map(|r| async move {
                match r {
                    Ok(Some(res)) => Some(Ok(res)),
                    Ok(None) => None,
                    Err(e) => Some(Err(e)),
                }
            })
            .collect::<Vec<Result<FhirResource>>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        Ok(resources)
    }

    /// Like `parse_fhir_resource`, but if `bytes` is `Some` skip the
    /// `fs::read` and parse from the in-memory buffer.
    async fn parse_fhir_resource_with_bytes(
        &self,
        file_path: &Path,
        index: &Option<PackageIndex>,
        bytes: Option<Vec<u8>>,
    ) -> Result<Option<FhirResource>> {
        let bytes = match bytes {
            Some(b) => b,
            None => fs::read(file_path).await?,
        };
        let json_value: serde_json::Value =
            tokio::task::spawn_blocking(move || serde_json::from_slice(&bytes))
                .await
                .map_err(|e| PackageError::ExtractionFailed {
                    message: format!("JSON parse worker panicked: {e}"),
                })??;

        let resource_type = json_value["resourceType"]
            .as_str()
            .unwrap_or("Unknown")
            .to_string();
        let id = json_value["id"].as_str().unwrap_or("").to_string();
        let url = json_value["url"].as_str().map(|s| s.to_string());
        let version = json_value["version"].as_str().map(|s| s.to_string());

        if resource_type == "Unknown" || resource_type.is_empty() {
            return Ok(None);
        }

        let filename = match file_path.file_name().and_then(|n| n.to_str()) {
            Some(s) => s,
            None => return Ok(None),
        };
        if let Some(index) = index
            && let Some(index_entry) = index.files.get(filename)
        {
            return Ok(Some(FhirResource {
                resource_type: index_entry.resource_type.clone().unwrap_or(resource_type),
                id: index_entry.id.clone().unwrap_or(id),
                url: index_entry.url.clone().or(url),
                version: index_entry.version.clone().or(version),
                content: json_value,
                file_path: file_path.to_path_buf(),
            }));
        }

        Ok(Some(FhirResource {
            resource_type,
            id,
            url,
            version,
            content: json_value,
            file_path: file_path.to_path_buf(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_package_extractor_creation() {
        let temp_dir = TempDir::new().unwrap();
        let extractor = PackageExtractor::new(temp_dir.path().to_path_buf());
        assert_eq!(extractor.cache_dir(), temp_dir.path());
        assert_eq!(extractor.temp_dir, temp_dir.path().join("temp"));
    }

    #[tokio::test]
    async fn test_package_structure_validation() {
        let temp_dir = TempDir::new().unwrap();
        let extractor = PackageExtractor::new(temp_dir.path().to_path_buf());

        // Test invalid package (no package directory)
        let invalid_path = temp_dir.path().join("invalid");
        fs::create_dir_all(&invalid_path).unwrap();
        assert!(extractor.validate_package(&invalid_path).await.is_err());

        // Test valid package structure
        let valid_path = temp_dir.path().join("valid");
        let package_dir = valid_path.join("package");
        fs::create_dir_all(&package_dir).unwrap();

        // Create package.json
        let package_json = package_dir.join("package.json");
        fs::write(&package_json, r#"{"name": "test", "version": "1.0.0"}"#).unwrap();

        assert!(extractor.validate_package(&valid_path).await.is_ok());
    }

    #[tokio::test]
    async fn test_manifest_parsing() {
        let temp_dir = TempDir::new().unwrap();
        let extractor = PackageExtractor::new(temp_dir.path().to_path_buf());

        // Create test package structure
        let package_path = temp_dir.path().join("test-package");
        let package_dir = package_path.join("package");
        fs::create_dir_all(&package_dir).unwrap();

        // Create package.json with FHIR package structure
        let manifest_content = serde_json::json!({
            "name": "test.package",
            "version": "1.0.0",
            "fhirVersions": ["4.0.1"],
            "dependencies": {
                "hl7.fhir.r4.core": "4.0.1"
            },
            "canonical": "http://example.com/test",
            "jurisdiction": "US"
        });

        let package_json = package_dir.join("package.json");
        fs::write(&package_json, manifest_content.to_string()).unwrap();

        let manifest = extractor.parse_manifest(&package_path).await.unwrap();
        assert_eq!(manifest.name, "test.package");
        assert_eq!(manifest.version, "1.0.0");
        assert_eq!(manifest.fhir_versions, Some(vec!["4.0.1".to_string()]));
        assert_eq!(manifest.dependencies.len(), 1);
        assert_eq!(
            manifest.dependencies.get("hl7.fhir.r4.core"),
            Some(&"4.0.1".to_string())
        );
        assert_eq!(
            manifest.canonical,
            Some("http://example.com/test".to_string())
        );
    }

    #[tokio::test]
    async fn test_fhir_resource_parsing() {
        let temp_dir = TempDir::new().unwrap();
        let extractor = PackageExtractor::new(temp_dir.path().to_path_buf());

        // Create test FHIR resource file
        let resource_path = temp_dir.path().join("Patient.json");
        let resource_content = serde_json::json!({
            "resourceType": "Patient",
            "id": "example",
            "url": "http://example.com/Patient/example",
            "version": "1.0.0",
            "name": [{"family": "Doe", "given": ["John"]}]
        });

        fs::write(&resource_path, resource_content.to_string()).unwrap();

        let resource = extractor
            .parse_fhir_resource_with_bytes(&resource_path, &None, None)
            .await
            .unwrap();
        assert!(resource.is_some());

        let resource = resource.unwrap();
        assert_eq!(resource.resource_type, "Patient");
        assert_eq!(resource.id, "example");
        assert_eq!(
            resource.url,
            Some("http://example.com/Patient/example".to_string())
        );
        assert_eq!(resource.version, Some("1.0.0".to_string()));
    }

    #[tokio::test]
    async fn test_invalid_resource_parsing() {
        let temp_dir = TempDir::new().unwrap();
        let extractor = PackageExtractor::new(temp_dir.path().to_path_buf());

        // Create invalid resource file (no resourceType)
        let resource_path = temp_dir.path().join("invalid.json");
        let resource_content = serde_json::json!({
            "id": "example",
            "name": "test"
        });

        fs::write(&resource_path, resource_content.to_string()).unwrap();

        let resource = extractor
            .parse_fhir_resource_with_bytes(&resource_path, &None, None)
            .await
            .unwrap();
        assert!(resource.is_none()); // Should be filtered out as invalid
    }
}
