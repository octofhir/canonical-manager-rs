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
#[derive(Debug)]
pub struct ExtractedPackage {
    pub name: String,
    pub version: String,
    pub manifest: PackageManifest,
    pub resources: Vec<FhirResource>,
    pub extraction_path: PathBuf,
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

        // Extract the .tgz file into tmp dir
        if let Err(e) = self
            .extract_tgz_file(&download.file_path, &tmp_extraction_path)
            .await
        {
            let _ = fs::remove_dir_all(&tmp_extraction_path).await;
            return Err(e);
        }

        // Move into final location atomically (replace if exists)
        if final_extraction_path.exists() {
            let _ = fs::remove_dir_all(&final_extraction_path).await;
        }
        fs::rename(&tmp_extraction_path, &final_extraction_path).await?;

        // Validate package structure
        self.validate_package(&final_extraction_path).await?;

        // Parse manifest
        let manifest = self.parse_manifest(&final_extraction_path).await?;

        // Extract resources
        let resources = self.extract_resources(&final_extraction_path).await?;

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
        };

        #[cfg(feature = "metrics")]
        {
            metrics::histogram!("extract_latency_ms", start.elapsed().as_secs_f64() * 1000.0);
        }

        Ok(result)
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

    /// Extract .tgz file to directory
    async fn extract_tgz_file(&self, tgz_path: &Path, output_dir: &Path) -> Result<()> {
        debug!(
            "Extracting .tgz file: {} to {}",
            tgz_path.display(),
            output_dir.display()
        );

        // Prefer fully-async extraction; fall back to blocking if async path fails
        if let Err(e) = self.extract_tgz_file_async(tgz_path, output_dir).await {
            warn!(
                "Async extraction failed ({}), falling back to blocking path",
                e
            );

            // Fallback: blocking streaming extraction without loading whole file
            let tgz_path = tgz_path.to_path_buf();
            let output_dir = output_dir.to_path_buf();
            tokio::task::spawn_blocking(move || {
                let file =
                    std::fs::File::open(&tgz_path).map_err(|e| PackageError::ExtractionFailed {
                        message: format!("Failed to open archive: {e}"),
                    })?;
                let reader = std::io::BufReader::new(file);
                let tar = GzDecoder::new(reader);
                let mut archive = Archive::new(tar);

                for entry in archive
                    .entries()
                    .map_err(|e| PackageError::ExtractionFailed {
                        message: format!("Failed to read archive entries: {e}"),
                    })?
                {
                    let mut entry = entry.map_err(|e| PackageError::ExtractionFailed {
                        message: format!("Invalid entry: {e}"),
                    })?;
                    let path = entry.path().map_err(|e| PackageError::ExtractionFailed {
                        message: format!("Invalid entry path: {e}"),
                    })?;

                    let out_path = Self::sanitize_extract_path(&output_dir, &path)?;

                    if let Some(parent) = out_path.parent() {
                        std::fs::create_dir_all(parent).map_err(|e| {
                            PackageError::ExtractionFailed {
                                message: format!("Failed to create directories: {e}"),
                            }
                        })?;
                    }

                    entry
                        .unpack(&out_path)
                        .map_err(|e| PackageError::ExtractionFailed {
                            message: format!("Failed to unpack entry: {e}"),
                        })?;
                }

                Ok::<(), crate::error::FcmError>(())
            })
            .await
            .map_err(|e| PackageError::ExtractionFailed {
                message: format!("Task join error: {e}"),
            })??;
        }

        Ok(())
    }

    async fn extract_tgz_file_async(&self, tgz_path: &Path, output_dir: &Path) -> Result<()> {
        use async_compression::tokio::bufread::GzipDecoder as AsyncGzipDecoder;
        use futures_util::StreamExt;
        use tokio_util::compat::TokioAsyncReadCompatExt;

        let file = fs::File::open(tgz_path).await?;
        let reader = tokio::io::BufReader::new(file);
        let gz = AsyncGzipDecoder::new(reader);
        let archive = async_tar::Archive::new(gz.compat());
        let mut entries = archive
            .entries()
            .map_err(|e| PackageError::ExtractionFailed {
                message: format!("Failed to read archive entries: {e}"),
            })?;

        while let Some(next) = entries.next().await {
            let mut entry = next.map_err(|e| PackageError::ExtractionFailed {
                message: format!("Invalid archive entry: {e}"),
            })?;

            let path = entry.path().map_err(|e| PackageError::ExtractionFailed {
                message: format!("Invalid entry path: {e}"),
            })?;
            let std_path = std::path::Path::new(path.as_os_str());
            let out_path = Self::sanitize_extract_path(output_dir, std_path)?;

            if let Some(parent) = out_path.parent() {
                fs::create_dir_all(parent).await?;
            }

            // Use built-in unpack if available; otherwise stream copy
            entry
                .unpack(&out_path)
                .await
                .map_err(|e| PackageError::ExtractionFailed {
                    message: format!("Failed to unpack entry: {e}"),
                })?;
        }

        Ok(())
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

    /// Extract FHIR resources from package
    async fn extract_resources(&self, path: &Path) -> Result<Vec<FhirResource>> {
        debug!("Extracting FHIR resources");

        let package_dir = path.join("package");
        if !package_dir.exists() {
            return Err(PackageError::ValidationFailed {
                message: "Package directory not found".to_string(),
            }
            .into());
        }

        // Parse .index.json if it exists
        let index_path = package_dir.join(".index.json");
        let index = if index_path.exists() {
            let content = fs::read_to_string(&index_path).await?;
            serde_json::from_str::<PackageIndex>(&content).ok()
        } else {
            None
        };

        let mut resources = Vec::new();

        // Read all .json files in package directory
        let mut dir_entries = fs::read_dir(&package_dir).await?;
        while let Some(entry) = dir_entries.next_entry().await? {
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("json")
                && !path.file_name().unwrap().to_str().unwrap().starts_with('.')
                && path.file_name().unwrap().to_str().unwrap() != "package.json"
            {
                let resource = self.parse_fhir_resource(&path, &index).await?;
                if let Some(resource) = resource {
                    resources.push(resource);
                }
            }
        }

        Ok(resources)
    }

    /// Parse a single FHIR resource file
    async fn parse_fhir_resource(
        &self,
        file_path: &Path,
        index: &Option<PackageIndex>,
    ) -> Result<Option<FhirResource>> {
        let content = fs::read_to_string(file_path).await?;
        let json_value: serde_json::Value = serde_json::from_str(&content)?;

        // Extract basic FHIR resource information
        let resource_type = json_value["resourceType"]
            .as_str()
            .unwrap_or("Unknown")
            .to_string();
        let id = json_value["id"].as_str().unwrap_or("").to_string();
        let url = json_value["url"].as_str().map(|s| s.to_string());
        let version = json_value["version"].as_str().map(|s| s.to_string());

        // Skip if not a valid FHIR resource
        if resource_type == "Unknown" || resource_type.is_empty() {
            return Ok(None);
        }

        // Try to get additional info from index
        let filename = file_path.file_name().unwrap().to_str().unwrap();
        if let Some(index) = index
            && let Some(index_entry) = index.files.get(filename)
        {
            // Use index information if available
            let resource = FhirResource {
                resource_type: index_entry.resource_type.clone().unwrap_or(resource_type),
                id: index_entry.id.clone().unwrap_or(id),
                url: index_entry.url.clone().or(url),
                version: index_entry.version.clone().or(version),
                content: json_value,
                file_path: file_path.to_path_buf(),
            };
            return Ok(Some(resource));
        }

        // Fallback to parsed information
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
            .parse_fhir_resource(&resource_path, &None)
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
            .parse_fhir_resource(&resource_path, &None)
            .await
            .unwrap();
        assert!(resource.is_none()); // Should be filtered out as invalid
    }
}
