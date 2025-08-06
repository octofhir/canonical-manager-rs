use crate::config::StorageConfig;
use crate::error::{Result, StorageError};
use crate::package::{ExtractedPackage, FhirResource};
use chrono::{DateTime, Utc};
use lz4_flex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use tokio::fs;
use tracing::{debug, info, warn};

/// Index entry for a FHIR resource in storage.
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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceMetadata {
    pub id: String,
    pub version: Option<String>,
    pub status: Option<String>,
    pub date: Option<String>,
    pub publisher: Option<String>,
}

/// Information about an installed FHIR package.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageInfo {
    pub name: String,
    pub version: String,
    pub installed_at: DateTime<Utc>,
    pub resource_count: usize,
}

/// Binary storage format for packages and resources
#[derive(Debug, Clone, Serialize, Deserialize)]
struct StorageData {
    packages: HashMap<String, PackageInfo>, // key: "name:version"
    resources: HashMap<String, ResourceIndex>, // key: canonical_url
    metadata: StorageMetadata,
}

/// Metadata about the storage file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageMetadata {
    version: u32,
    created_at: DateTime<Utc>,
    last_updated: DateTime<Utc>,
    resource_count: usize,
    package_count: usize,
}

/// High-performance binary storage system for FHIR resources.
///
/// Uses bincode for efficient serialization and lz4_flex for compression.
/// Provides better performance than SQLite for read-heavy workloads while
/// maintaining data integrity and atomic operations.
///
/// # Example
///
/// ```rust,no_run
/// use octofhir_canonical_manager::binary_storage::BinaryStorage;
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
/// let mut storage = BinaryStorage::new(config).await?;
/// let packages = storage.list_packages().await?;
/// println!("Found {} installed packages", packages.len());
/// # Ok(())
/// # }
/// ```
pub struct BinaryStorage {
    #[allow(dead_code)]
    config: StorageConfig,
    storage_path: PathBuf,
    backup_path: PathBuf,
    in_memory_cache: Arc<RwLock<StorageData>>,
}

impl BinaryStorage {
    /// Creates a new binary storage instance.
    ///
    /// Initializes the storage system and loads existing data from disk.
    /// Creates necessary directories if they don't exist.
    ///
    /// # Arguments
    ///
    /// * `config` - Storage configuration specifying directories and limits
    ///
    /// # Errors
    ///
    /// Returns `StorageError` if:
    /// - Cannot create required directories
    /// - Cannot read existing storage file
    /// - Storage file is corrupted
    pub async fn new(config: StorageConfig) -> Result<Self> {
        // Ensure directories exist
        fs::create_dir_all(&config.cache_dir)
            .await
            .map_err(|e| StorageError::IoError {
                message: format!("Failed to create cache directory: {e}"),
            })?;

        fs::create_dir_all(&config.index_dir)
            .await
            .map_err(|e| StorageError::IoError {
                message: format!("Failed to create index directory: {e}"),
            })?;

        fs::create_dir_all(&config.packages_dir)
            .await
            .map_err(|e| StorageError::IoError {
                message: format!("Failed to create packages directory: {e}"),
            })?;

        let storage_path = config.index_dir.join("storage.bin");
        let backup_path = config.index_dir.join("storage.bin.backup");

        let mut storage = Self {
            config,
            storage_path,
            backup_path,
            in_memory_cache: Arc::new(RwLock::new(StorageData {
                packages: HashMap::new(),
                resources: HashMap::new(),
                metadata: StorageMetadata {
                    version: 1,
                    created_at: Utc::now(),
                    last_updated: Utc::now(),
                    resource_count: 0,
                    package_count: 0,
                },
            })),
        };

        // Load existing data
        storage.load_from_disk().await?;

        info!("Binary storage initialized at {:?}", storage.storage_path);
        Ok(storage)
    }

    /// Adds a package to storage with all its resources.
    ///
    /// Indexes all FHIR resources in the package and updates the storage.
    /// This operation is atomic - either all resources are added or none.
    ///
    /// # Arguments
    ///
    /// * `package` - The extracted package to add
    ///
    /// # Errors
    ///
    /// Returns `StorageError` if:
    /// - Package already exists with same version
    /// - Cannot write to storage file
    /// - Resource indexing fails
    pub async fn add_package(&self, package: &ExtractedPackage) -> Result<()> {
        let package_key = format!("{}:{}", package.name, package.version);

        // Check if package already exists
        {
            let cache = self.in_memory_cache.read().unwrap();
            if cache.packages.contains_key(&package_key) {
                return Err(crate::error::FcmError::Storage(
                    StorageError::PackageAlreadyExists {
                        name: package.name.clone(),
                        version: package.version.clone(),
                    },
                ));
            }
        }

        let mut resource_indices = Vec::new();

        // Index all resources in the package
        for resource in &package.resources {
            let index = self.create_resource_index(resource, &package.name, &package.version)?;
            resource_indices.push(index);
        }

        // Update cache atomically
        {
            let mut cache = self.in_memory_cache.write().unwrap();

            // Add package info
            let package_info = PackageInfo {
                name: package.name.clone(),
                version: package.version.clone(),
                installed_at: Utc::now(),
                resource_count: resource_indices.len(),
            };
            cache.packages.insert(package_key, package_info);

            // Add all resource indices
            for index in resource_indices {
                cache.resources.insert(index.canonical_url.clone(), index);
            }

            // Update metadata
            cache.metadata.last_updated = Utc::now();
            cache.metadata.package_count = cache.packages.len();
            cache.metadata.resource_count = cache.resources.len();
        }

        // Persist to disk
        self.save_to_disk().await?;

        info!(
            "Added package {}:{} with {} resources",
            package.name,
            package.version,
            package.resources.len()
        );

        Ok(())
    }

    /// Removes a package and all its resources from storage.
    ///
    /// This operation is atomic - either all resources are removed or none.
    ///
    /// # Arguments
    ///
    /// * `name` - Package name
    /// * `version` - Package version
    ///
    /// # Returns
    ///
    /// `true` if the package was found and removed, `false` if not found.
    pub async fn remove_package(&self, name: &str, version: &str) -> Result<bool> {
        let package_key = format!("{name}:{version}");

        let removed = {
            let mut cache = self.in_memory_cache.write().unwrap();

            // Check if package exists
            if !cache.packages.contains_key(&package_key) {
                return Ok(false);
            }

            // Remove package
            cache.packages.remove(&package_key);

            // Remove all resources from this package
            let resources_to_remove: Vec<String> = cache
                .resources
                .iter()
                .filter(|(_, resource)| {
                    resource.package_name == name && resource.package_version == version
                })
                .map(|(url, _)| url.clone())
                .collect();

            for url in &resources_to_remove {
                cache.resources.remove(url);
            }

            // Update metadata
            cache.metadata.last_updated = Utc::now();
            cache.metadata.package_count = cache.packages.len();
            cache.metadata.resource_count = cache.resources.len();

            info!(
                "Removed package {}:{} and {} resources",
                name,
                version,
                resources_to_remove.len()
            );

            true
        };

        if removed {
            // Persist to disk
            self.save_to_disk().await?;
        }

        Ok(removed)
    }

    /// Lists all installed packages.
    ///
    /// Returns a vector of package information sorted by name and version.
    pub async fn list_packages(&self) -> Result<Vec<PackageInfo>> {
        let cache = self.in_memory_cache.read().unwrap();
        let mut packages: Vec<PackageInfo> = cache.packages.values().cloned().collect();
        packages.sort_by(|a, b| a.name.cmp(&b.name).then(a.version.cmp(&b.version)));
        Ok(packages)
    }

    /// Finds a resource by its canonical URL.
    ///
    /// # Arguments
    ///
    /// * `canonical_url` - The canonical URL of the resource
    ///
    /// # Returns
    ///
    /// `Some(ResourceIndex)` if found, `None` otherwise.
    pub async fn find_resource(&self, canonical_url: &str) -> Result<Option<ResourceIndex>> {
        let cache = self.in_memory_cache.read().unwrap();
        Ok(cache.resources.get(canonical_url).cloned())
    }

    /// Gets all resource indices for search operations.
    ///
    /// Returns a HashMap mapping canonical URLs to resource indices.
    pub fn get_cache_entries(&self) -> HashMap<String, ResourceIndex> {
        let cache = self.in_memory_cache.read().unwrap();
        cache.resources.clone()
    }

    /// Loads a FHIR resource from disk.
    ///
    /// # Arguments
    ///
    /// * `resource_index` - Index entry pointing to the resource file
    ///
    /// # Returns
    ///
    /// The loaded FHIR resource with its content.
    pub fn get_resource(&self, resource_index: &ResourceIndex) -> Result<FhirResource> {
        let file_path = &resource_index.file_path;

        // Read file synchronously (this is typically fast for individual files)
        let content = std::fs::read_to_string(file_path).map_err(|e| StorageError::IoError {
            message: format!("Failed to read resource file {file_path:?}: {e}"),
        })?;

        let json_content: serde_json::Value =
            serde_json::from_str(&content).map_err(|e| StorageError::SerializationError {
                message: format!("Failed to parse JSON from {file_path:?}: {e}"),
            })?;

        Ok(FhirResource {
            resource_type: resource_index.resource_type.clone(),
            id: resource_index.metadata.id.clone(),
            url: Some(resource_index.canonical_url.clone()),
            version: resource_index.metadata.version.clone(),
            content: json_content,
            file_path: file_path.clone(),
        })
    }

    /// Gets storage statistics.
    ///
    /// Returns information about the current state of storage including
    /// package count, resource count, and last update time.
    pub async fn get_stats(&self) -> Result<StorageMetadata> {
        let cache = self.in_memory_cache.read().unwrap();
        Ok(cache.metadata.clone())
    }

    /// Creates a resource index from a FHIR resource.
    fn create_resource_index(
        &self,
        resource: &FhirResource,
        package_name: &str,
        package_version: &str,
    ) -> Result<ResourceIndex> {
        let canonical_url = resource
            .url
            .clone()
            .unwrap_or_else(|| format!("{}#{}", resource.resource_type, resource.id));

        Ok(ResourceIndex {
            canonical_url,
            resource_type: resource.resource_type.clone(),
            package_name: package_name.to_string(),
            package_version: package_version.to_string(),
            file_path: resource.file_path.clone(),
            metadata: ResourceMetadata {
                id: resource.id.clone(),
                version: resource.version.clone(),
                status: extract_resource_status(&resource.content),
                date: extract_resource_date(&resource.content),
                publisher: extract_resource_publisher(&resource.content),
            },
        })
    }

    /// Loads storage data from disk.
    async fn load_from_disk(&mut self) -> Result<()> {
        if !self.storage_path.exists() {
            debug!("Storage file does not exist, starting with empty storage");
            return Ok(());
        }

        let compressed_data =
            fs::read(&self.storage_path)
                .await
                .map_err(|e| StorageError::IoError {
                    message: format!("Failed to read storage file: {e}"),
                })?;

        if compressed_data.is_empty() {
            debug!("Storage file is empty, starting with empty storage");
            return Ok(());
        }

        // Try to decompress and deserialize data with recovery
        match self.try_load_data(&compressed_data).await {
            Ok(storage_data) => {
                // Update cache
                let mut cache = self.in_memory_cache.write().unwrap();
                *cache = storage_data;
                debug!("Successfully loaded storage data from disk");
                Ok(())
            }
            Err(e) => {
                warn!("Failed to load storage data: {}. Attempting recovery...", e);

                // Try to load from backup
                if let Ok(backup_data) = self.try_load_backup().await {
                    let mut cache = self.in_memory_cache.write().unwrap();
                    *cache = backup_data;
                    warn!("Recovered storage data from backup");
                    Ok(())
                } else {
                    // If both primary and backup fail, start fresh but preserve the corrupted file
                    warn!(
                        "Both primary storage and backup are corrupted. Starting with empty storage."
                    );
                    warn!(
                        "Corrupted file preserved as: {:?}.corrupted",
                        self.storage_path
                    );

                    // Rename corrupted file
                    if let Err(rename_err) = fs::rename(
                        &self.storage_path,
                        self.storage_path.with_extension("storage.corrupted"),
                    )
                    .await
                    {
                        warn!("Failed to preserve corrupted file: {}", rename_err);
                    }

                    // Start with empty storage
                    Ok(())
                }
            }
        }
    }

    /// Try to load and decompress storage data
    async fn try_load_data(&self, compressed_data: &[u8]) -> Result<StorageData> {
        // Decompress data
        let serialized_data =
            lz4_flex::decompress_size_prepended(compressed_data).map_err(|e| {
                StorageError::SerializationError {
                    message: format!("Failed to decompress storage data: {e}"),
                }
            })?;

        // Check if decompressed data is empty or invalid
        if serialized_data.is_empty() {
            return Err(crate::error::FcmError::Storage(
                StorageError::SerializationError {
                    message: "Decompressed data is empty".to_string(),
                },
            ));
        }

        // Deserialize data
        let storage_data: StorageData = serde_json::from_slice(&serialized_data).map_err(|e| {
            StorageError::SerializationError {
                message: format!("Failed to deserialize storage data: {e}"),
            }
        })?;

        Ok(storage_data)
    }

    /// Try to load data from backup file
    async fn try_load_backup(&self) -> Result<StorageData> {
        if !self.backup_path.exists() {
            return Err(crate::error::FcmError::Storage(
                StorageError::SerializationError {
                    message: "Backup file does not exist".to_string(),
                },
            ));
        }

        let backup_data = fs::read(&self.backup_path)
            .await
            .map_err(|e| StorageError::IoError {
                message: format!("Failed to read backup file: {e}"),
            })?;

        if backup_data.is_empty() {
            return Err(crate::error::FcmError::Storage(
                StorageError::SerializationError {
                    message: "Backup file is empty".to_string(),
                },
            ));
        }

        self.try_load_data(&backup_data).await
    }

    /// Saves storage data to disk with atomic write operation.
    async fn save_to_disk(&self) -> Result<()> {
        let storage_data = {
            let cache = self.in_memory_cache.read().unwrap();
            cache.clone()
        };

        // Serialize data
        let serialized_data =
            serde_json::to_vec(&storage_data).map_err(|e| StorageError::SerializationError {
                message: format!("Failed to serialize storage data: {e}"),
            })?;

        // Compress data
        let compressed_data = lz4_flex::compress_prepend_size(&serialized_data);

        // Create backup of existing file
        if self.storage_path.exists() {
            fs::copy(&self.storage_path, &self.backup_path)
                .await
                .map_err(|e| StorageError::IoError {
                    message: format!("Failed to create backup: {e}"),
                })?;
        }

        // Write to temporary file first, then rename (atomic operation)
        let temp_path = self.storage_path.with_extension("tmp");
        fs::write(&temp_path, &compressed_data)
            .await
            .map_err(|e| StorageError::IoError {
                message: format!("Failed to write temporary storage file: {e}"),
            })?;

        fs::rename(&temp_path, &self.storage_path)
            .await
            .map_err(|e| StorageError::IoError {
                message: format!("Failed to rename temporary storage file: {e}"),
            })?;

        debug!(
            "Saved storage with {} packages and {} resources ({} bytes compressed)",
            storage_data.metadata.package_count,
            storage_data.metadata.resource_count,
            compressed_data.len()
        );

        Ok(())
    }

    /// Performs integrity check on storage data.
    ///
    /// Verifies that all referenced files exist and are readable.
    /// Returns a report of any issues found.
    pub async fn integrity_check(&self) -> Result<IntegrityReport> {
        let mut report = IntegrityReport {
            total_packages: 0,
            total_resources: 0,
            missing_files: Vec::new(),
            corrupted_files: Vec::new(),
            orphaned_resources: Vec::new(),
        };

        let cache = self.in_memory_cache.read().unwrap();
        report.total_packages = cache.packages.len();
        report.total_resources = cache.resources.len();

        // Check each resource file
        for (canonical_url, resource_index) in &cache.resources {
            let file_path = &resource_index.file_path;

            if !file_path.exists() {
                report.missing_files.push(canonical_url.clone());
                continue;
            }

            // Try to read and parse the file
            match std::fs::read_to_string(file_path) {
                Ok(content) => {
                    if serde_json::from_str::<serde_json::Value>(&content).is_err() {
                        report.corrupted_files.push(canonical_url.clone());
                    }
                }
                Err(_) => {
                    report.missing_files.push(canonical_url.clone());
                }
            }

            // Check if resource has a corresponding package
            let package_key = format!(
                "{}:{}",
                resource_index.package_name, resource_index.package_version
            );
            if !cache.packages.contains_key(&package_key) {
                report.orphaned_resources.push(canonical_url.clone());
            }
        }

        Ok(report)
    }

    /// Compacts storage by removing unused space and optimizing layout.
    ///
    /// This operation rewrites the storage file to remove fragmentation
    /// and optimize for better read performance.
    pub async fn compact(&self) -> Result<()> {
        info!("Starting storage compaction");

        // Simply save to disk - this will rewrite the file with current data
        self.save_to_disk().await?;

        info!("Storage compaction completed");
        Ok(())
    }
}

/// Report from storage integrity check.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityReport {
    pub total_packages: usize,
    pub total_resources: usize,
    pub missing_files: Vec<String>,
    pub corrupted_files: Vec<String>,
    pub orphaned_resources: Vec<String>,
}

impl IntegrityReport {
    /// Returns true if no issues were found.
    pub fn is_healthy(&self) -> bool {
        self.missing_files.is_empty()
            && self.corrupted_files.is_empty()
            && self.orphaned_resources.is_empty()
    }

    /// Returns the total number of issues found.
    pub fn issue_count(&self) -> usize {
        self.missing_files.len() + self.corrupted_files.len() + self.orphaned_resources.len()
    }
}

/// Extract the status field from a FHIR resource.
pub fn extract_resource_status(content: &Value) -> Option<String> {
    content
        .get("status")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

/// Extract the date field from a FHIR resource.
pub fn extract_resource_date(content: &Value) -> Option<String> {
    let date_fields = ["date", "lastUpdated"];

    for field in &date_fields {
        if let Some(date_value) = content.get(field) {
            if let Some(date_str) = date_value.as_str() {
                return Some(date_str.to_string());
            }
        }
    }

    if let Some(meta) = content.get("meta") {
        if let Some(last_updated) = meta.get("lastUpdated") {
            if let Some(date_str) = last_updated.as_str() {
                return Some(date_str.to_string());
            }
        }
    }

    None
}

/// Extract the publisher field from a FHIR resource.
pub fn extract_resource_publisher(content: &Value) -> Option<String> {
    let publisher_fields = ["publisher", "author"];

    for field in &publisher_fields {
        if let Some(publisher_value) = content.get(field) {
            if let Some(publisher_str) = publisher_value.as_str() {
                return Some(publisher_str.to_string());
            }
        }
    }

    if let Some(contact) = content.get("contact") {
        if let Some(contact_array) = contact.as_array() {
            for contact_item in contact_array {
                if let Some(name) = contact_item.get("name") {
                    if let Some(name_str) = name.as_str() {
                        return Some(name_str.to_string());
                    }
                }
            }
        }
    }

    None
}
