//! Indexed storage system for FHIR resources

use crate::config::StorageConfig;
use crate::error::{Result, StorageError};
use crate::package::{ExtractedPackage, FhirResource};
use chrono::{DateTime, Utc};
use rusqlite::{Connection, params};
use serde::{Deserialize, Serialize};
use serde_json;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tokio::fs;
use tracing::{debug, info};

/// Indexed storage system for FHIR resources with SQLite backend.
///
/// Provides persistent storage and fast retrieval of FHIR resources with
/// full-text search capabilities. Maintains both a SQLite database for
/// persistence and an in-memory cache for fast access.
///
/// # Example
///
/// ```rust,no_run
/// use octofhir_canonical_manager::storage::IndexedStorage;
/// use octofhir_canonical_manager::config::StorageConfig;
/// use std::path::PathBuf;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = StorageConfig {
///     cache_dir: PathBuf::from("/tmp/cache"),
///     index_dir: PathBuf::from("/tmp/index"),
///     packages_dir: PathBuf::from("/tmp/packages"),
///     max_cache_size: "1GB".to_string(),
/// };
///
/// let mut storage = IndexedStorage::new(config).await?;
/// let packages = storage.list_packages().await?;
/// println!("Found {} installed packages", packages.len());
/// # Ok(())
/// # }
/// ```
pub struct IndexedStorage {
    config: StorageConfig,
    db_path: PathBuf,
    in_memory_cache: Arc<RwLock<HashMap<String, ResourceIndex>>>,
}

/// Index entry for a FHIR resource in storage.
///
/// Contains metadata about a stored FHIR resource including its location,
/// package information, and searchable metadata. Used for fast resource
/// lookup and search operations.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_manager::storage::{ResourceIndex, ResourceMetadata};
/// use std::path::PathBuf;
///
/// let index = ResourceIndex {
///     canonical_url: "http://hl7.org/fhir/Patient".to_string(),
///     resource_type: "Patient".to_string(),
///     package_name: "hl7.fhir.r4.core".to_string(),
///     package_version: "4.0.1".to_string(),
///     file_path: PathBuf::from("/path/to/Patient.json"),
///     metadata: ResourceMetadata {
///         id: "Patient".to_string(),
///         version: Some("4.0.1".to_string()),
///         status: Some("active".to_string()),
///         date: None,
///         publisher: Some("HL7".to_string()),
///     },
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceIndex {
    pub canonical_url: String,
    pub resource_type: String,
    pub package_name: String,
    pub package_version: String,
    pub file_path: PathBuf,
    pub metadata: ResourceMetadata,
}

/// Metadata extracted from FHIR resources for indexing and search.
///
/// Contains commonly used FHIR resource fields that are extracted
/// and stored separately for efficient querying and filtering.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_manager::storage::ResourceMetadata;
///
/// let metadata = ResourceMetadata {
///     id: "patient-example".to_string(),
///     version: Some("1.0.0".to_string()),
///     status: Some("active".to_string()),
///     date: Some("2023-01-01".to_string()),
///     publisher: Some("Example Organization".to_string()),
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceMetadata {
    pub id: String,
    pub version: Option<String>,
    pub status: Option<String>,
    pub date: Option<String>,
    pub publisher: Option<String>,
}

/// Information about an installed FHIR package.
///
/// Contains metadata about packages that have been installed in storage,
/// including installation timestamp and resource counts.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_manager::storage::PackageInfo;
/// use chrono::Utc;
///
/// let info = PackageInfo {
///     name: "hl7.fhir.us.core".to_string(),
///     version: "6.1.0".to_string(),
///     installed_at: Utc::now(),
///     resource_count: 150,
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageInfo {
    pub name: String,
    pub version: String,
    pub installed_at: chrono::DateTime<chrono::Utc>,
    pub resource_count: usize,
}

impl IndexedStorage {
    /// Creates a new indexed storage system with the given configuration.
    ///
    /// Initializes the SQLite database, creates necessary directories,
    /// and loads the in-memory cache from existing data.
    ///
    /// # Arguments
    ///
    /// * `config` - Storage configuration specifying directories and cache settings
    ///
    /// # Returns
    ///
    /// * `Ok(IndexedStorage)` - Successfully initialized storage system
    /// * `Err` - If database initialization or directory creation fails
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use octofhir_canonical_manager::storage::IndexedStorage;
    /// use octofhir_canonical_manager::config::StorageConfig;
    /// use std::path::PathBuf;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = StorageConfig {
    ///     cache_dir: PathBuf::from("/tmp/cache"),
    ///     index_dir: PathBuf::from("/tmp/index"),
    ///     packages_dir: PathBuf::from("/tmp/packages"),
    ///     max_cache_size: "1GB".to_string(),
    /// };
    ///
    /// let storage = IndexedStorage::new(config).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn new(config: StorageConfig) -> Result<Self> {
        // Create storage directories
        tokio::fs::create_dir_all(&config.cache_dir).await?;
        tokio::fs::create_dir_all(&config.index_dir).await?;
        tokio::fs::create_dir_all(&config.packages_dir).await?;

        let db_path = config.index_dir.join("resources.db");

        let storage = Self {
            config,
            db_path,
            in_memory_cache: Arc::new(RwLock::new(HashMap::new())),
        };

        // Initialize database
        storage.init_database().await?;

        // Load cache from database
        storage.load_cache().await?;

        Ok(storage)
    }

    /// Adds an extracted FHIR package to storage.
    ///
    /// Stores all package resources in the configured packages directory,
    /// updates the database with resource indices, and refreshes the in-memory cache.
    ///
    /// # Arguments
    ///
    /// * `package` - The extracted package to add to storage
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Package successfully added to storage
    /// * `Err` - If storage operation fails
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::storage::IndexedStorage;
    /// # use octofhir_canonical_manager::package::ExtractedPackage;
    /// # async fn example(mut storage: IndexedStorage, package: ExtractedPackage) -> Result<(), Box<dyn std::error::Error>> {
    /// storage.add_package(package).await?;
    /// println!("Package added to storage successfully");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn add_package(&mut self, package: ExtractedPackage) -> Result<()> {
        info!(
            "Adding package to storage: {}@{}",
            package.name, package.version
        );

        // Store package files
        let package_dir = self
            .config
            .packages_dir
            .join(format!("{}-{}", package.name, package.version));
        fs::create_dir_all(&package_dir).await?;

        // Copy resources to package directory
        for resource in &package.resources {
            let dest_path = package_dir.join(resource.file_path.file_name().unwrap());
            fs::copy(&resource.file_path, &dest_path).await?;
        }

        // Create database entries
        let package_info = PackageInfo {
            name: package.name.clone(),
            version: package.version.clone(),
            installed_at: Utc::now(),
            resource_count: package.resources.len(),
        };

        self.store_package_info(&package_info).await?;

        // Index all resources
        for resource in &package.resources {
            if let Some(canonical_url) = &resource.url {
                let resource_index = ResourceIndex {
                    canonical_url: canonical_url.clone(),
                    resource_type: resource.resource_type.clone(),
                    package_name: package.name.clone(),
                    package_version: package.version.clone(),
                    file_path: package_dir.join(resource.file_path.file_name().unwrap()),
                    metadata: ResourceMetadata {
                        id: resource.id.clone(),
                        version: resource.version.clone(),
                        status: extract_resource_status(&resource.content),
                        date: extract_resource_date(&resource.content),
                        publisher: extract_resource_publisher(&resource.content),
                    },
                };

                self.store_resource_index(&resource_index).await?;

                // Update in-memory cache
                let mut cache = self.in_memory_cache.write().unwrap();
                cache.insert(canonical_url.clone(), resource_index);
            }
        }

        info!(
            "Successfully added package: {}@{} with {} resources",
            package.name,
            package.version,
            package.resources.len()
        );

        Ok(())
    }

    /// Removes a package and all its resources from storage.
    ///
    /// Deletes the package directory, removes all database entries,
    /// and updates the in-memory cache.
    ///
    /// # Arguments
    ///
    /// * `name` - The package name to remove
    /// * `version` - The package version to remove
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Package successfully removed from storage
    /// * `Err` - If removal operation fails
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::storage::IndexedStorage;
    /// # async fn example(mut storage: IndexedStorage) -> Result<(), Box<dyn std::error::Error>> {
    /// storage.remove_package("hl7.fhir.us.core", "6.1.0").await?;
    /// println!("Package removed from storage");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn remove_package(&mut self, name: &str, version: &str) -> Result<()> {
        info!("Removing package from storage: {}@{}", name, version);

        // Remove from database
        self.remove_package_from_db(name, version).await?;

        // Remove package directory
        let package_dir = self.config.packages_dir.join(format!("{name}-{version}"));
        if package_dir.exists() {
            fs::remove_dir_all(&package_dir).await?;
        }

        // Update in-memory cache - remove all resources from this package
        let mut cache = self.in_memory_cache.write().unwrap();
        cache.retain(|_, index| !(index.package_name == name && index.package_version == version));

        info!("Successfully removed package: {}@{}", name, version);
        Ok(())
    }

    /// Finds a resource by its canonical URL.
    ///
    /// Performs a fast lookup in the in-memory cache to find a resource
    /// with the exact canonical URL match.
    ///
    /// # Arguments
    ///
    /// * `url` - The canonical URL to search for
    ///
    /// # Returns
    ///
    /// * `Some(ResourceIndex)` - If a resource with that URL is found
    /// * `None` - If no resource with that URL exists
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::storage::IndexedStorage;
    /// # fn example(storage: IndexedStorage) {
    /// if let Some(resource_index) = storage.find_by_canonical("http://hl7.org/fhir/Patient") {
    ///     println!("Found resource: {} from package {}@{}",
    ///              resource_index.resource_type,
    ///              resource_index.package_name,
    ///              resource_index.package_version);
    /// }
    /// # }
    /// ```
    pub fn find_by_canonical(&self, url: &str) -> Option<ResourceIndex> {
        let cache = self.in_memory_cache.read().unwrap();
        cache.get(url).cloned()
    }

    /// Retrieves the full content of a FHIR resource.
    ///
    /// Loads the complete FHIR resource from disk based on the provided index.
    /// The resource is parsed from its JSON file and returned as a structured object.
    ///
    /// # Arguments
    ///
    /// * `index` - Resource index pointing to the resource to load
    ///
    /// # Returns
    ///
    /// * `Ok(FhirResource)` - The fully loaded FHIR resource
    /// * `Err(StorageError)` - If the resource file cannot be read or parsed
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::storage::IndexedStorage;
    /// # async fn example(storage: IndexedStorage) -> Result<(), Box<dyn std::error::Error>> {
    /// if let Some(index) = storage.find_by_canonical("http://hl7.org/fhir/Patient") {
    ///     let resource = storage.get_resource(&index).await?;
    ///     println!("Loaded resource: {} (ID: {})", resource.resource_type, resource.id);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_resource(&self, index: &ResourceIndex) -> Result<FhirResource> {
        debug!(
            "Loading resource content from: {}",
            index.file_path.display()
        );

        if !index.file_path.exists() {
            return Err(StorageError::ResourceNotFound {
                canonical_url: index.canonical_url.clone(),
            }
            .into());
        }

        let content_str = fs::read_to_string(&index.file_path).await?;
        let content: serde_json::Value = serde_json::from_str(&content_str)?;

        Ok(FhirResource {
            resource_type: index.resource_type.clone(),
            id: index.metadata.id.clone(),
            url: Some(index.canonical_url.clone()),
            version: index.metadata.version.clone(),
            content,
            file_path: index.file_path.clone(),
        })
    }

    /// Lists all packages currently installed in storage.
    ///
    /// Retrieves information about all packages that have been added to storage,
    /// including their installation timestamps and resource counts.
    ///
    /// # Returns
    ///
    /// * `Ok(Vec<PackageInfo>)` - List of installed packages
    /// * `Err` - If database query fails
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::storage::IndexedStorage;
    /// # async fn example(storage: IndexedStorage) -> Result<(), Box<dyn std::error::Error>> {
    /// let packages = storage.list_packages().await?;
    ///
    /// println!("Installed packages:");
    /// for package in packages {
    ///     println!("  {}@{} ({} resources)",
    ///              package.name,
    ///              package.version,
    ///              package.resource_count);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_packages(&self) -> Result<Vec<PackageInfo>> {
        self.get_all_packages().await
    }

    /// Searches for resources by FHIR resource type.
    ///
    /// Returns all resources in storage that match the specified resource type
    /// (e.g., "Patient", "Observation", "CodeSystem").
    ///
    /// # Arguments
    ///
    /// * `resource_type` - The FHIR resource type to search for
    ///
    /// # Returns
    ///
    /// Vector of resource indices matching the specified type.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::storage::IndexedStorage;
    /// # fn example(storage: IndexedStorage) {
    /// let patients = storage.search_by_type("Patient");
    /// println!("Found {} Patient resources", patients.len());
    ///
    /// for patient in patients {
    ///     println!("  {}: {}", patient.metadata.id, patient.canonical_url);
    /// }
    /// # }
    /// ```
    pub fn search_by_type(&self, resource_type: &str) -> Vec<ResourceIndex> {
        let cache = self.in_memory_cache.read().unwrap();
        cache
            .values()
            .filter(|index| index.resource_type == resource_type)
            .cloned()
            .collect()
    }

    /// Searches for resources from a specific package.
    ///
    /// Returns all resources that belong to the specified package.
    /// Optionally filters by package version if provided.
    ///
    /// # Arguments
    ///
    /// * `package_name` - The package name to search for
    /// * `package_version` - Optional specific version to match
    ///
    /// # Returns
    ///
    /// Vector of resource indices from the specified package.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::storage::IndexedStorage;
    /// # fn example(storage: IndexedStorage) {
    /// // Search all versions of a package
    /// let resources = storage.search_by_package("hl7.fhir.us.core", None);
    /// println!("Found {} resources from US Core package", resources.len());
    ///
    /// // Search specific version
    /// let v6_resources = storage.search_by_package("hl7.fhir.us.core", Some("6.1.0"));
    /// println!("Found {} resources from US Core v6.1.0", v6_resources.len());
    /// # }
    /// ```
    pub fn search_by_package(
        &self,
        package_name: &str,
        package_version: Option<&str>,
    ) -> Vec<ResourceIndex> {
        let cache = self.in_memory_cache.read().unwrap();
        cache
            .values()
            .filter(|index| {
                index.package_name == package_name
                    && (package_version.is_none()
                        || Some(index.package_version.as_str()) == package_version)
            })
            .cloned()
            .collect()
    }

    /// Returns all canonical URLs currently indexed in storage.
    ///
    /// Provides a complete list of all canonical URLs for resources
    /// that are available in storage. Useful for inventory and debugging.
    ///
    /// # Returns
    ///
    /// Vector of all canonical URLs in storage.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::storage::IndexedStorage;
    /// # fn example(storage: IndexedStorage) {
    /// let urls = storage.get_all_canonical_urls();
    /// println!("Storage contains {} resources:", urls.len());
    ///
    /// for url in urls.iter().take(5) {
    ///     println!("  {}", url);
    /// }
    /// # }
    /// ```
    pub fn get_all_canonical_urls(&self) -> Vec<String> {
        let cache = self.in_memory_cache.read().unwrap();
        cache.keys().cloned().collect()
    }

    /// Returns all cache entries as (URL, ResourceIndex) pairs.
    ///
    /// Provides access to the complete in-memory cache for operations
    /// that need to iterate over all resources, such as resolution and search.
    ///
    /// # Returns
    ///
    /// Vector of (canonical_url, resource_index) tuples.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::storage::IndexedStorage;
    /// # fn example(storage: IndexedStorage) {
    /// let entries = storage.get_cache_entries();
    ///
    /// for (url, index) in entries.into_iter().take(3) {
    ///     println!("{}: {} from {}@{}",
    ///              url,
    ///              index.resource_type,
    ///              index.package_name,
    ///              index.package_version);
    /// }
    /// # }
    /// ```
    pub fn get_cache_entries(&self) -> Vec<(String, ResourceIndex)> {
        let cache = self.in_memory_cache.read().unwrap();
        cache.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    /// Rebuilds the entire search index from existing packages.
    ///
    /// Scans the packages directory for installed packages and recreates
    /// the database index from scratch. Useful for recovering from index
    /// corruption or after manual file system changes.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Index successfully rebuilt
    /// * `Err` - If rebuild operation fails
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::storage::IndexedStorage;
    /// # async fn example(mut storage: IndexedStorage) -> Result<(), Box<dyn std::error::Error>> {
    /// storage.rebuild_index().await?;
    /// println!("Search index rebuilt successfully");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn rebuild_index(&mut self) -> Result<()> {
        info!("Rebuilding search index...");

        // Clear current index
        self.clear_index().await?;

        // Scan packages directory for installed packages
        let packages_dir = &self.config.packages_dir;

        if !packages_dir.exists() {
            info!("No packages directory found, index rebuild complete");
            return Ok(());
        }

        let mut entries = fs::read_dir(packages_dir).await?;
        let mut rebuilt_count = 0;

        while let Some(entry) = entries.next_entry().await? {
            if entry.file_type().await?.is_dir() {
                let dir_name = entry.file_name().to_string_lossy().to_string();

                // Parse package name and version from directory name (format: name-version)
                if let Some(last_dash) = dir_name.rfind('-') {
                    let name = &dir_name[..last_dash];
                    let version = &dir_name[last_dash + 1..];

                    info!("Re-indexing package: {}@{}", name, version);

                    // Create ExtractedPackage from directory contents
                    match self
                        .create_extracted_package_from_dir(entry.path(), name, version)
                        .await
                    {
                        Ok(package) => {
                            self.add_package(package).await?;
                            rebuilt_count += 1;
                        }
                        Err(e) => {
                            tracing::warn!("Failed to re-index package {}: {}", dir_name, e);
                        }
                    }
                }
            }
        }

        info!(
            "Index rebuild complete: {} packages re-indexed",
            rebuilt_count
        );
        Ok(())
    }

    /// Clears the entire search index.
    ///
    /// Removes all entries from the database and clears the in-memory cache.
    /// This does not delete the actual package files, only the index.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Index successfully cleared
    /// * `Err` - If clear operation fails
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::storage::IndexedStorage;
    /// # async fn example(mut storage: IndexedStorage) -> Result<(), Box<dyn std::error::Error>> {
    /// storage.clear_index().await?;
    /// println!("Search index cleared");
    /// # Ok(())
    /// # }
    /// ```
    pub async fn clear_index(&mut self) -> Result<()> {
        info!("Clearing search index...");

        // Clear database
        let db_path = self.db_path.clone();
        tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&db_path)?;
            conn.execute("DELETE FROM resources", [])?;
            conn.execute("DELETE FROM packages", [])?;
            Ok::<(), rusqlite::Error>(())
        })
        .await
        .map_err(|e| StorageError::DatabaseError {
            message: format!("Failed to clear index: {e}"),
        })?
        .map_err(|e| StorageError::DatabaseError {
            message: format!("SQLite error: {e}"),
        })?;

        // Clear in-memory cache
        let mut cache = self.in_memory_cache.write().unwrap();
        cache.clear();

        info!("Search index cleared");
        Ok(())
    }

    /// Create ExtractedPackage from existing package directory
    async fn create_extracted_package_from_dir(
        &self,
        package_dir: std::path::PathBuf,
        name: &str,
        version: &str,
    ) -> Result<ExtractedPackage> {
        let mut resources = Vec::new();

        // Look for package.json to get metadata
        let package_json_path = package_dir.join("package.json");
        let mut dependencies = HashMap::new();

        if package_json_path.exists() {
            let package_json_content = fs::read_to_string(&package_json_path).await?;
            if let Ok(package_json) =
                serde_json::from_str::<serde_json::Value>(&package_json_content)
            {
                if let Some(deps) = package_json.get("dependencies").and_then(|d| d.as_object()) {
                    for (dep_name, dep_version) in deps {
                        if let Some(version_str) = dep_version.as_str() {
                            dependencies.insert(dep_name.clone(), version_str.to_string());
                        }
                    }
                }
            }
        }

        // Scan for FHIR resource files (JSON files)
        // Check if there's a nested 'package' directory (common FHIR package structure)
        let resource_dir = if package_dir.join("package").exists() {
            package_dir.join("package")
        } else {
            package_dir.clone()
        };

        let mut entries = fs::read_dir(&resource_dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();

            if path.extension().and_then(|ext| ext.to_str()) == Some("json")
                && path.file_name().and_then(|name| name.to_str()) != Some("package.json")
            {
                // Try to parse as FHIR resource
                match self.parse_fhir_resource(&path).await {
                    Ok(resource) => {
                        resources.push(resource);
                    }
                    Err(e) => {
                        debug!("Skipping non-FHIR file {}: {}", path.display(), e);
                    }
                }
            }
        }

        // Create a basic manifest with the extracted dependencies
        let manifest = crate::package::PackageManifest {
            name: name.to_string(),
            version: version.to_string(),
            fhir_versions: None,
            dependencies,
            canonical: None,
            jurisdiction: None,
            package_type: None,
            title: None,
            description: None,
        };

        Ok(ExtractedPackage {
            name: name.to_string(),
            version: version.to_string(),
            manifest,
            resources,
            extraction_path: package_dir,
        })
    }

    /// Parse a FHIR resource from a JSON file
    async fn parse_fhir_resource(&self, file_path: &std::path::Path) -> Result<FhirResource> {
        let content_str = fs::read_to_string(file_path).await?;
        let content: serde_json::Value = serde_json::from_str(&content_str)?;

        // Extract basic FHIR resource info
        let resource_type = content
            .get("resourceType")
            .and_then(|rt| rt.as_str())
            .ok_or_else(|| StorageError::InvalidResource {
                message: "Missing resourceType field".to_string(),
            })?
            .to_string();

        let id = content
            .get("id")
            .and_then(|id| id.as_str())
            .unwrap_or("unknown")
            .to_string();

        let url = content
            .get("url")
            .and_then(|url| url.as_str())
            .map(|s| s.to_string());

        let version = content
            .get("version")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        Ok(FhirResource {
            resource_type,
            id,
            url,
            version,
            content,
            file_path: file_path.to_path_buf(),
        })
    }

    /// Initialize SQLite database
    async fn init_database(&self) -> Result<()> {
        let db_path = self.db_path.clone();
        tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&db_path)?;

            // Create packages table
            conn.execute(
                "CREATE TABLE IF NOT EXISTS packages (
                    name TEXT NOT NULL,
                    version TEXT NOT NULL,
                    installed_at TEXT NOT NULL,
                    resource_count INTEGER NOT NULL,
                    PRIMARY KEY (name, version)
                )",
                [],
            )?;

            // Create resources table
            conn.execute(
                "CREATE TABLE IF NOT EXISTS resources (
                    canonical_url TEXT PRIMARY KEY,
                    resource_type TEXT NOT NULL,
                    package_name TEXT NOT NULL,
                    package_version TEXT NOT NULL,
                    file_path TEXT NOT NULL,
                    resource_id TEXT NOT NULL,
                    resource_version TEXT,
                    status TEXT,
                    date TEXT,
                    publisher TEXT,
                    FOREIGN KEY (package_name, package_version) REFERENCES packages(name, version)
                )",
                [],
            )?;

            // Create indexes for fast lookups
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_resources_type ON resources(resource_type)",
                [],
            )?;

            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_resources_package ON resources(package_name, package_version)",
                [],
            )?;

            Ok::<(), rusqlite::Error>(())
        }).await
        .map_err(|e| StorageError::DatabaseError {
            message: format!("Failed to initialize database: {e}")
        })?
        .map_err(|e| StorageError::DatabaseError {
            message: format!("SQLite error: {e}")
        })?;

        Ok(())
    }

    /// Load cache from database
    async fn load_cache(&self) -> Result<()> {
        let db_path = self.db_path.clone();
        let resources = tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&db_path)?;
            let mut stmt = conn.prepare(
                "SELECT canonical_url, resource_type, package_name, package_version,
                        file_path, resource_id, resource_version, status, date, publisher
                 FROM resources",
            )?;

            let resource_iter = stmt.query_map([], |row| {
                Ok(ResourceIndex {
                    canonical_url: row.get(0)?,
                    resource_type: row.get(1)?,
                    package_name: row.get(2)?,
                    package_version: row.get(3)?,
                    file_path: PathBuf::from(row.get::<_, String>(4)?),
                    metadata: ResourceMetadata {
                        id: row.get(5)?,
                        version: row.get(6)?,
                        status: row.get(7)?,
                        date: row.get(8)?,
                        publisher: row.get(9)?,
                    },
                })
            })?;

            let mut resources = Vec::new();
            for resource in resource_iter {
                resources.push(resource?);
            }

            Ok::<Vec<ResourceIndex>, rusqlite::Error>(resources)
        })
        .await
        .map_err(|e| StorageError::DatabaseError {
            message: format!("Failed to load cache: {e}"),
        })?
        .map_err(|e| StorageError::DatabaseError {
            message: format!("SQLite error: {e}"),
        })?;

        // Update in-memory cache
        let mut cache = self.in_memory_cache.write().unwrap();
        for resource in resources {
            cache.insert(resource.canonical_url.clone(), resource);
        }

        debug!("Loaded {} resources into cache", cache.len());
        Ok(())
    }

    /// Store package info in database
    async fn store_package_info(&self, package_info: &PackageInfo) -> Result<()> {
        let db_path = self.db_path.clone();
        let package_info = package_info.clone();

        tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&db_path)?;
            conn.execute(
                "INSERT OR REPLACE INTO packages (name, version, installed_at, resource_count)
                 VALUES (?1, ?2, ?3, ?4)",
                params![
                    package_info.name,
                    package_info.version,
                    package_info.installed_at.to_rfc3339(),
                    package_info.resource_count
                ],
            )?;
            Ok::<(), rusqlite::Error>(())
        })
        .await
        .map_err(|e| StorageError::DatabaseError {
            message: format!("Failed to store package info: {e}"),
        })?
        .map_err(|e| StorageError::DatabaseError {
            message: format!("SQLite error: {e}"),
        })?;

        Ok(())
    }

    /// Store resource index in database
    async fn store_resource_index(&self, resource_index: &ResourceIndex) -> Result<()> {
        let db_path = self.db_path.clone();
        let resource_index = resource_index.clone();

        tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&db_path)?;
            conn.execute(
                "INSERT OR REPLACE INTO resources
                 (canonical_url, resource_type, package_name, package_version, file_path,
                  resource_id, resource_version, status, date, publisher)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                params![
                    resource_index.canonical_url,
                    resource_index.resource_type,
                    resource_index.package_name,
                    resource_index.package_version,
                    resource_index.file_path.to_string_lossy(),
                    resource_index.metadata.id,
                    resource_index.metadata.version,
                    resource_index.metadata.status,
                    resource_index.metadata.date,
                    resource_index.metadata.publisher
                ],
            )?;
            Ok::<(), rusqlite::Error>(())
        })
        .await
        .map_err(|e| StorageError::DatabaseError {
            message: format!("Failed to store resource index: {e}"),
        })?
        .map_err(|e| StorageError::DatabaseError {
            message: format!("SQLite error: {e}"),
        })?;

        Ok(())
    }

    /// Remove package from database
    async fn remove_package_from_db(&self, name: &str, version: &str) -> Result<()> {
        let db_path = self.db_path.clone();
        let name = name.to_string();
        let version = version.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&db_path)?;

            // Remove resources first (foreign key constraint)
            conn.execute(
                "DELETE FROM resources WHERE package_name = ?1 AND package_version = ?2",
                params![name, version],
            )?;

            // Remove package
            conn.execute(
                "DELETE FROM packages WHERE name = ?1 AND version = ?2",
                params![name, version],
            )?;

            Ok::<(), rusqlite::Error>(())
        })
        .await
        .map_err(|e| StorageError::DatabaseError {
            message: format!("Failed to remove package from database: {e}"),
        })?
        .map_err(|e| StorageError::DatabaseError {
            message: format!("SQLite error: {e}"),
        })?;

        Ok(())
    }

    /// Get all packages from database
    async fn get_all_packages(&self) -> Result<Vec<PackageInfo>> {
        let db_path = self.db_path.clone();

        let packages = tokio::task::spawn_blocking(move || {
            let conn = Connection::open(&db_path)?;
            let mut stmt =
                conn.prepare("SELECT name, version, installed_at, resource_count FROM packages")?;

            let package_iter = stmt.query_map([], |row| {
                let installed_at_str: String = row.get(2)?;
                let installed_at =
                    DateTime::parse_from_rfc3339(&installed_at_str).map_err(|_| {
                        rusqlite::Error::InvalidColumnType(
                            2,
                            "installed_at".to_string(),
                            rusqlite::types::Type::Text,
                        )
                    })?;

                Ok(PackageInfo {
                    name: row.get(0)?,
                    version: row.get(1)?,
                    installed_at: installed_at.with_timezone(&Utc),
                    resource_count: row.get(3)?,
                })
            })?;

            let mut packages = Vec::new();
            for package in package_iter {
                packages.push(package?);
            }

            Ok::<Vec<PackageInfo>, rusqlite::Error>(packages)
        })
        .await
        .map_err(|e| StorageError::DatabaseError {
            message: format!("Failed to get packages: {e}"),
        })?
        .map_err(|e| StorageError::DatabaseError {
            message: format!("SQLite error: {e}"),
        })?;

        Ok(packages)
    }
}

/// Extract resource status from FHIR content
fn extract_resource_status(content: &serde_json::Value) -> Option<String> {
    content
        .get("status")
        .and_then(|s| s.as_str())
        .map(|s| s.to_string())
}

/// Extract resource date from FHIR content
fn extract_resource_date(content: &serde_json::Value) -> Option<String> {
    content
        .get("date")
        .and_then(|d| d.as_str())
        .map(|s| s.to_string())
        .or_else(|| {
            // Try other date fields commonly used in FHIR
            content
                .get("lastModified")
                .and_then(|d| d.as_str())
                .map(|s| s.to_string())
        })
        .or_else(|| {
            content
                .get("effectiveDateTime")
                .and_then(|d| d.as_str())
                .map(|s| s.to_string())
        })
}

/// Extract resource publisher from FHIR content
fn extract_resource_publisher(content: &serde_json::Value) -> Option<String> {
    content
        .get("publisher")
        .and_then(|p| p.as_str())
        .map(|s| s.to_string())
        .or_else(|| {
            // Try to extract from contact info
            content
                .get("contact")
                .and_then(|contacts| contacts.as_array())
                .and_then(|contacts| contacts.first())
                .and_then(|contact| contact.get("name"))
                .and_then(|name| name.as_str())
                .map(|s| s.to_string())
        })
        .or_else(|| {
            // Try organization reference
            content
                .get("publisherReference")
                .and_then(|org| org.get("display"))
                .and_then(|display| display.as_str())
                .map(|s| s.to_string())
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_indexed_storage_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            cache_dir: temp_dir.path().join("cache"),
            index_dir: temp_dir.path().join("index"),
            packages_dir: temp_dir.path().join("packages"),
            max_cache_size: "100MB".to_string(),
        };

        let storage = IndexedStorage::new(config).await;
        assert!(storage.is_ok());

        let storage = storage.unwrap();
        let packages = storage.list_packages().await.unwrap();
        assert_eq!(packages.len(), 0);
    }

    #[tokio::test]
    async fn test_canonical_url_lookup() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            cache_dir: temp_dir.path().join("cache"),
            index_dir: temp_dir.path().join("index"),
            packages_dir: temp_dir.path().join("packages"),
            max_cache_size: "100MB".to_string(),
        };

        let storage = IndexedStorage::new(config).await.unwrap();

        // Test empty lookup
        let result = storage.find_by_canonical("http://example.com/missing");
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_search_functions() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            cache_dir: temp_dir.path().join("cache"),
            index_dir: temp_dir.path().join("index"),
            packages_dir: temp_dir.path().join("packages"),
            max_cache_size: "100MB".to_string(),
        };

        let storage = IndexedStorage::new(config).await.unwrap();

        // Test search by type
        let results = storage.search_by_type("Patient");
        assert_eq!(results.len(), 0);

        // Test search by package
        let results = storage.search_by_package("test.package", None);
        assert_eq!(results.len(), 0);
    }
}
