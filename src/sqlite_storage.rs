//! SQLite-based storage for FHIR packages and resources
//!
//! Uses a deadpool-backed SQLite connection pool to provide async access
//! without blocking the Tokio runtime. All public APIs remain unchanged.

use crate::cas_storage::{CasStorage, ContentType};
use crate::config::StorageConfig;
use crate::content_hash::ContentHash;
use crate::domain::{PackageInfo, ResourceIndex, SD_FLAVORS};
use crate::error::{FcmError, Result, StorageError};
use crate::package::{ExtractedPackage, FhirResource};
use chrono::{DateTime, Utc};
use deadpool_sqlite::rusqlite::{self, OptionalExtension};
use deadpool_sqlite::{Config as DeadpoolConfig, Pool, Runtime};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, info, warn};

// Reset to version 1 - following fhir-package-loader schema pattern
const SCHEMA_VERSION: i32 = 1;

pub struct SqliteStorage {
    pool: Pool,
    db_path: PathBuf,
    cas: Arc<CasStorage>,
}

/// Internal struct for building resource rows during insertion
#[derive(Debug, Clone)]
struct ResourceRow {
    resource_type: String,
    id: Option<String>,
    url: Option<String>,
    name: Option<String>,
    version: Option<String>,
    sd_kind: Option<String>,
    sd_derivation: Option<String>,
    sd_type: Option<String>,
    sd_base_definition: Option<String>,
    sd_abstract: Option<bool>,
    sd_impose_profiles: Option<Vec<String>>,
    sd_characteristics: Option<Vec<String>>,
    sd_flavor: Option<String>,
    content_hash: String,
    content_path: String,
    id_lower: Option<String>,
    name_lower: Option<String>,
}

impl SqliteStorage {
    fn configure_connection(conn: &mut rusqlite::Connection) -> rusqlite::Result<()> {
        conn.execute_batch(
            r#"
            PRAGMA foreign_keys = ON;
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA temp_store = MEMORY;
            "#,
        )?;
        Ok(())
    }

    async fn with_connection<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&mut rusqlite::Connection) -> rusqlite::Result<R> + Send + 'static,
        R: Send + 'static,
    {
        let conn = self.pool.get().await.map_err(|e| {
            FcmError::Storage(StorageError::DatabaseError {
                message: format!("Failed to acquire SQLite connection: {e}"),
            })
        })?;

        let result = conn
            .interact(move |conn| {
                Self::configure_connection(conn)?;
                f(conn)
            })
            .await
            .map_err(|e| {
                FcmError::Storage(StorageError::DatabaseError {
                    message: format!("SQLite connection worker failed: {e}"),
                })
            })?;

        result.map_err(|e| {
            FcmError::Storage(StorageError::DatabaseError {
                message: e.to_string(),
            })
        })
    }

    pub async fn new(config: StorageConfig) -> Result<Self> {
        tokio::fs::create_dir_all(&config.packages_dir).await?;
        let db_path = config.packages_dir.join("storage.db");

        let pool = DeadpoolConfig::new(db_path.clone())
            .builder(Runtime::Tokio1)
            .map_err(|e| {
                FcmError::Storage(StorageError::InitializationFailed {
                    message: format!("Failed to create SQLite pool builder: {e}"),
                })
            })?
            .max_size(config.connection_pool_size)
            .wait_timeout(Some(std::time::Duration::from_secs(30)))
            .create_timeout(Some(std::time::Duration::from_secs(30)))
            .recycle_timeout(Some(std::time::Duration::from_secs(30)))
            .build()
            .map_err(|e| {
                FcmError::Storage(StorageError::InitializationFailed {
                    message: format!("Failed to create SQLite pool: {e}"),
                })
            })?;

        let cas_path = config
            .packages_dir
            .parent()
            .unwrap_or(&config.packages_dir)
            .join("cache");
        let cas = Arc::new(CasStorage::new(cas_path).await.map_err(|e| {
            FcmError::Storage(StorageError::IoError {
                message: format!("Failed to initialize CAS storage: {e}"),
            })
        })?);

        let storage = Self {
            pool,
            db_path: db_path.clone(),
            cas,
        };

        storage.init_schema().await?;
        info!(
            "SQLite storage initialized with connection pool at {:?}",
            db_path
        );
        Ok(storage)
    }

    async fn init_schema(&self) -> Result<()> {
        let current_version = self
            .with_connection(move |conn| {
                conn.execute_batch(
                    r#"
                    CREATE TABLE IF NOT EXISTS metadata (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL
                    );
                    "#,
                )?;

                let version = conn
                    .query_row(
                        "SELECT value FROM metadata WHERE key = 'schema_version'",
                        [],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?;

                Ok(version)
            })
            .await?
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);

        if current_version == 0 {
            self.create_schema().await?;
        } else if current_version < SCHEMA_VERSION {
            return Err(FcmError::Storage(StorageError::UnsupportedSchemaVersion {
                current: current_version,
                required: SCHEMA_VERSION,
                message: "Database schema changed. Please delete ~/.maki/index/fhir.db and run 'maki config init'".to_string(),
            }));
        }

        self.with_connection(move |conn| {
            conn.execute(
                "INSERT OR REPLACE INTO metadata (key, value) VALUES (?1, ?2)",
                rusqlite::params!["schema_version", SCHEMA_VERSION.to_string()],
            )?;
            Ok(())
        })
        .await?;

        debug!("SQLite schema initialized (version {})", SCHEMA_VERSION);
        Ok(())
    }

    async fn create_schema(&self) -> Result<()> {
        self.with_connection(move |conn| {
            conn.execute_batch(
                r#"
                -- Packages table (following fhir-package-loader pattern)
                CREATE TABLE IF NOT EXISTS packages (
                    rowid INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    version TEXT NOT NULL,
                    package_path TEXT,
                    fhir_version TEXT,
                    manifest_hash TEXT NOT NULL,
                    installed_at TEXT NOT NULL,
                    resource_count INTEGER NOT NULL,
                    priority INTEGER DEFAULT 0,
                    UNIQUE(name, version)
                );

                -- Resources table (following fhir-package-loader pattern with extensions)
                CREATE TABLE IF NOT EXISTS resources (
                    rowid INTEGER PRIMARY KEY,
                    -- Core fields
                    resource_type TEXT NOT NULL,
                    id TEXT,
                    url TEXT,
                    name TEXT,
                    version TEXT,
                    -- StructureDefinition specific fields (like fhir-package-loader)
                    sd_kind TEXT,
                    sd_derivation TEXT,
                    sd_type TEXT,
                    sd_base_definition TEXT,
                    sd_abstract INTEGER,
                    sd_impose_profiles TEXT,   -- JSON array as string
                    sd_characteristics TEXT,   -- JSON array as string
                    sd_flavor TEXT,            -- 'Extension', 'Profile', 'Type', 'Resource', 'Logical'
                    -- Package reference
                    package_name TEXT NOT NULL,
                    package_version TEXT NOT NULL,
                    fhir_version TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    content_path TEXT NOT NULL,
                    -- Lowercase variants for case-insensitive search (our extension)
                    id_lower TEXT,
                    name_lower TEXT,
                    FOREIGN KEY(package_name, package_version) REFERENCES packages(name, version) ON DELETE CASCADE
                );

                -- Indexes (following fhir-package-loader pattern)
                CREATE INDEX IF NOT EXISTS idx_package_name_version ON packages(name, version);
                CREATE INDEX IF NOT EXISTS idx_package_priority ON packages(priority);

                CREATE INDEX IF NOT EXISTS idx_resource_id_type_flavor ON resources(id_lower, resource_type, sd_flavor);
                CREATE INDEX IF NOT EXISTS idx_resource_name_type_flavor ON resources(name_lower, resource_type, sd_flavor);
                CREATE INDEX IF NOT EXISTS idx_resource_url_type_flavor ON resources(url, resource_type, sd_flavor);
                CREATE INDEX IF NOT EXISTS idx_resource_sd_flavor ON resources(sd_flavor) WHERE sd_flavor IS NOT NULL;
                CREATE INDEX IF NOT EXISTS idx_resource_package ON resources(package_name, package_version);
                CREATE INDEX IF NOT EXISTS idx_resource_fhir_version ON resources(fhir_version);
                CREATE INDEX IF NOT EXISTS idx_content_hash ON resources(content_hash);
                "#,
            )?;
            Ok(())
        })
        .await?;

        debug!("Created database schema version {}", SCHEMA_VERSION);
        Ok(())
    }

    #[allow(dead_code)]
    async fn migrate_schema(&self, from_version: i32) -> Result<()> {
        info!(
            "Migrating schema from version {} to {}",
            from_version, SCHEMA_VERSION
        );
        // Migrations are intentionally unsupported for now.
        Ok(())
    }

    /// Set package priority (higher = more preferred for resolution)
    pub async fn set_package_priority(
        &self,
        package_name: &str,
        package_version: &str,
        priority: i32,
    ) -> Result<()> {
        let name = package_name.to_string();
        let version = package_version.to_string();
        self.with_connection(move |conn| {
            conn.execute(
                "UPDATE packages SET priority = ?1 WHERE name = ?2 AND version = ?3",
                rusqlite::params![priority, name, version],
            )?;
            Ok(())
        })
        .await
    }

    pub async fn add_package(&self, package: ExtractedPackage) -> Result<()> {
        let package_name = package.name.clone();
        let package_version = package.version.clone();

        // Check if package already exists
        let name_check = package_name.clone();
        let version_check = package_version.clone();
        let exists: bool = self
            .with_connection(move |conn| {
                conn.query_row(
                    "SELECT 1 FROM packages WHERE name = ?1 AND version = ?2",
                    rusqlite::params![name_check, version_check],
                    |_| Ok(true),
                )
                .optional()
            })
            .await?
            .unwrap_or(false);

        if exists {
            info!(
                "Package {}@{} already exists, skipping",
                package_name, package_version
            );
            return Ok(());
        }

        let fhir_version = package
            .manifest
            .fhir_versions
            .as_ref()
            .and_then(|versions| versions.first())
            .cloned()
            .unwrap_or_else(|| "4.0.1".to_string());

        // Prepare resource data with new schema fields
        let mut resource_data: Vec<ResourceRow> = Vec::new();
        for resource in &package.resources {
            let content_json = serde_json::to_string(&resource.content).map_err(|e| {
                FcmError::Storage(StorageError::IoError {
                    message: format!("Failed to serialize resource: {e}"),
                })
            })?;

            let content_bytes = content_json.as_bytes();
            let content_hash = ContentHash::from_bytes(content_bytes);

            let content_path = self
                .cas
                .store(content_bytes, &content_hash, ContentType::Resource)
                .await
                .map_err(|e| {
                    FcmError::Storage(StorageError::IoError {
                        message: format!("Failed to store resource in CAS: {e}"),
                    })
                })?
                .to_string_lossy()
                .into_owned();

            let canonical_url = resource
                .url
                .clone()
                .unwrap_or_else(|| format!("{}/{}", resource.resource_type, resource.id));

            // Extract name from content
            let name = resource
                .content
                .get("name")
                .and_then(|v| v.as_str())
                .map(String::from);

            // Extract SD-specific fields (following fhir-package-loader)
            let sd_fields = extract_sd_fields(&resource.content);

            resource_data.push(ResourceRow {
                resource_type: resource.resource_type.clone(),
                id: Some(resource.id.clone()),
                url: Some(canonical_url),
                name: name.clone(),
                version: resource.version.clone(),
                sd_kind: sd_fields.sd_kind,
                sd_derivation: sd_fields.sd_derivation,
                sd_type: sd_fields.sd_type,
                sd_base_definition: sd_fields.sd_base_definition,
                sd_abstract: sd_fields.sd_abstract,
                sd_impose_profiles: sd_fields.sd_impose_profiles,
                sd_characteristics: sd_fields.sd_characteristics,
                sd_flavor: sd_fields.sd_flavor,
                content_hash: content_hash.to_hex(),
                content_path,
                // Case-insensitive search columns
                id_lower: Some(resource.id.to_lowercase()),
                name_lower: name.map(|n| n.to_lowercase()),
            });
        }

        let manifest_json = serde_json::to_string(&package.manifest).map_err(|e| {
            FcmError::Storage(StorageError::IoError {
                message: format!("Failed to serialize manifest: {e}"),
            })
        })?;
        let manifest_hash = ContentHash::from_bytes(manifest_json.as_bytes());
        let installed_at = Utc::now().to_rfc3339();
        let resource_count = resource_data.len();

        let name_tx = package_name.clone();
        let version_tx = package_version.clone();
        let fhir_version_tx = fhir_version.clone();
        let manifest_hash_hex = manifest_hash.to_hex();

        self.with_connection(move |conn| {
            let tx = conn.unchecked_transaction()?;

            tx.execute(
                "INSERT INTO packages (name, version, fhir_version, manifest_hash, installed_at, resource_count, priority)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![
                    &name_tx,
                    &version_tx,
                    &fhir_version_tx,
                    &manifest_hash_hex,
                    &installed_at,
                    resource_count,
                    0,  // default priority
                ],
            )?;

            for r in resource_data {
                let sd_impose_profiles_json = r.sd_impose_profiles
                    .as_ref()
                    .map(|v| serde_json::to_string(v).unwrap_or_default());
                let sd_characteristics_json = r.sd_characteristics
                    .as_ref()
                    .map(|v| serde_json::to_string(v).unwrap_or_default());

                tx.execute(
                    r#"INSERT OR REPLACE INTO resources (
                        resource_type, id, url, name, version,
                        sd_kind, sd_derivation, sd_type, sd_base_definition, sd_abstract,
                        sd_impose_profiles, sd_characteristics, sd_flavor,
                        package_name, package_version, fhir_version,
                        content_hash, content_path, id_lower, name_lower
                    ) VALUES (
                        ?1, ?2, ?3, ?4, ?5,
                        ?6, ?7, ?8, ?9, ?10,
                        ?11, ?12, ?13,
                        ?14, ?15, ?16,
                        ?17, ?18, ?19, ?20
                    )"#,
                    rusqlite::params![
                        &r.resource_type,
                        &r.id,
                        &r.url,
                        &r.name,
                        &r.version,
                        &r.sd_kind,
                        &r.sd_derivation,
                        &r.sd_type,
                        &r.sd_base_definition,
                        r.sd_abstract.map(|b| if b { 1 } else { 0 }),
                        &sd_impose_profiles_json,
                        &sd_characteristics_json,
                        &r.sd_flavor,
                        &name_tx,
                        &version_tx,
                        &fhir_version_tx,
                        &r.content_hash,
                        &r.content_path,
                        &r.id_lower,
                        &r.name_lower,
                    ],
                )?;
            }

            tx.commit()?;
            Ok(())
        })
        .await?;

        info!(
            "Package {}@{} added to SQLite ({} resources)",
            package_name, package_version, resource_count
        );
        Ok(())
    }

    #[tracing::instrument(name = "sqlite.add_packages_batch", skip_all, fields(count = packages.len()))]
    pub async fn add_packages_batch(&self, packages: Vec<ExtractedPackage>) -> Result<()> {
        info!("Adding {} packages in batch mode", packages.len());
        let start = std::time::Instant::now();

        // Build batch data with new schema
        struct BatchPackage {
            name: String,
            version: String,
            fhir_version: String,
            manifest_hash: String,
            resource_count: usize,
            resources: Vec<ResourceRow>,
        }

        let mut batch_data: Vec<BatchPackage> = Vec::new();

        for package in packages {
            let fhir_version = package
                .manifest
                .fhir_versions
                .as_ref()
                .and_then(|versions| versions.first())
                .cloned()
                .unwrap_or_else(|| "4.0.1".to_string());

            let manifest_json = serde_json::to_string(&package.manifest).map_err(|e| {
                FcmError::Storage(StorageError::IoError {
                    message: format!("Failed to serialize manifest: {e}"),
                })
            })?;
            let manifest_hash = ContentHash::from_bytes(manifest_json.as_bytes());

            let mut resource_data: Vec<ResourceRow> = Vec::new();
            for resource in &package.resources {
                let content_json = serde_json::to_string(&resource.content).map_err(|e| {
                    FcmError::Storage(StorageError::IoError {
                        message: format!("Failed to serialize resource: {e}"),
                    })
                })?;

                let content_bytes = content_json.as_bytes();
                let content_hash = ContentHash::from_bytes(content_bytes);

                let content_path = self
                    .cas
                    .store(content_bytes, &content_hash, ContentType::Resource)
                    .await
                    .map_err(|e| {
                        FcmError::Storage(StorageError::IoError {
                            message: format!("Failed to store resource in CAS: {e}"),
                        })
                    })?
                    .to_string_lossy()
                    .into_owned();

                let canonical_url = resource
                    .url
                    .clone()
                    .unwrap_or_else(|| format!("{}/{}", resource.resource_type, resource.id));

                // Extract name from content
                let name = resource
                    .content
                    .get("name")
                    .and_then(|v| v.as_str())
                    .map(String::from);

                // Extract SD-specific fields (following fhir-package-loader)
                let sd_fields = extract_sd_fields(&resource.content);

                resource_data.push(ResourceRow {
                    resource_type: resource.resource_type.clone(),
                    id: Some(resource.id.clone()),
                    url: Some(canonical_url),
                    name: name.clone(),
                    version: resource.version.clone(),
                    sd_kind: sd_fields.sd_kind,
                    sd_derivation: sd_fields.sd_derivation,
                    sd_type: sd_fields.sd_type,
                    sd_base_definition: sd_fields.sd_base_definition,
                    sd_abstract: sd_fields.sd_abstract,
                    sd_impose_profiles: sd_fields.sd_impose_profiles,
                    sd_characteristics: sd_fields.sd_characteristics,
                    sd_flavor: sd_fields.sd_flavor,
                    content_hash: content_hash.to_hex(),
                    content_path,
                    id_lower: Some(resource.id.to_lowercase()),
                    name_lower: name.map(|n| n.to_lowercase()),
                });
            }

            batch_data.push(BatchPackage {
                name: package.name,
                version: package.version,
                fhir_version,
                manifest_hash: manifest_hash.to_hex(),
                resource_count: resource_data.len(),
                resources: resource_data,
            });
        }

        let package_count = batch_data.len();
        self.with_connection(move |conn| {
            let tx = conn.unchecked_transaction()?;

            let mut package_stmt = tx.prepare(
                "INSERT OR REPLACE INTO packages (name, version, fhir_version, manifest_hash, installed_at, resource_count, priority)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )?;

            let mut resource_stmt = tx.prepare(
                r#"INSERT OR REPLACE INTO resources (
                    resource_type, id, url, name, version,
                    sd_kind, sd_derivation, sd_type, sd_base_definition, sd_abstract,
                    sd_impose_profiles, sd_characteristics, sd_flavor,
                    package_name, package_version, fhir_version,
                    content_hash, content_path, id_lower, name_lower
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5,
                    ?6, ?7, ?8, ?9, ?10,
                    ?11, ?12, ?13,
                    ?14, ?15, ?16,
                    ?17, ?18, ?19, ?20
                )"#,
            )?;

            let now = Utc::now().to_rfc3339();

            for pkg in batch_data {
                package_stmt.execute(rusqlite::params![
                    &pkg.name,
                    &pkg.version,
                    &pkg.fhir_version,
                    &pkg.manifest_hash,
                    &now,
                    pkg.resource_count,
                    0,  // default priority
                ])?;

                for r in pkg.resources {
                    let sd_impose_profiles_json = r.sd_impose_profiles
                        .as_ref()
                        .map(|v| serde_json::to_string(v).unwrap_or_default());
                    let sd_characteristics_json = r.sd_characteristics
                        .as_ref()
                        .map(|v| serde_json::to_string(v).unwrap_or_default());

                    resource_stmt.execute(rusqlite::params![
                        &r.resource_type,
                        &r.id,
                        &r.url,
                        &r.name,
                        &r.version,
                        &r.sd_kind,
                        &r.sd_derivation,
                        &r.sd_type,
                        &r.sd_base_definition,
                        r.sd_abstract.map(|b| if b { 1 } else { 0 }),
                        &sd_impose_profiles_json,
                        &sd_characteristics_json,
                        &r.sd_flavor,
                        &pkg.name,
                        &pkg.version,
                        &pkg.fhir_version,
                        &r.content_hash,
                        &r.content_path,
                        &r.id_lower,
                        &r.name_lower,
                    ])?;
                }
            }

            drop(resource_stmt);
            drop(package_stmt);
            tx.commit()?;
            Ok(())
        })
        .await?;

        let duration = start.elapsed();
        info!(
            "Batch indexing completed: {} packages in {:?}",
            package_count, duration
        );

        #[cfg(feature = "metrics")]
        {
            metrics::histogram!("batch_indexing_latency_ms", duration.as_secs_f64() * 1000.0);
            metrics::counter!("batch_indexed_packages", package_count as u64);
        }

        Ok(())
    }

    /// Standard SELECT clause for resource queries (new schema)
    const RESOURCE_SELECT: &'static str = r#"
        SELECT r.url, r.resource_type, r.package_name, r.package_version, r.content_path, r.fhir_version,
               r.id, r.name, r.version,
               r.sd_kind, r.sd_derivation, r.sd_type, r.sd_base_definition, r.sd_abstract,
               r.sd_impose_profiles, r.sd_characteristics, r.sd_flavor
        FROM resources r
        JOIN packages p ON r.package_name = p.name AND r.package_version = p.version
    "#;

    #[tracing::instrument(name = "sqlite.find_resource", skip(self), fields(key = %canonical_url))]
    pub async fn find_resource(&self, canonical_url: &str) -> Result<Option<ResourceIndex>> {
        let url = canonical_url.to_string();
        self.with_connection(move |conn| {
            let query = format!("{} WHERE r.url = ?1 LIMIT 1", Self::RESOURCE_SELECT);
            conn.query_row(&query, rusqlite::params![url], extract_resource_index)
                .optional()
        })
        .await
        .map_err(|err| match err {
            FcmError::Storage(StorageError::DatabaseError { message }) => {
                FcmError::Storage(StorageError::IoError {
                    message: format!("Failed to query resource: {message}"),
                })
            }
            other => other,
        })
    }

    pub async fn find_resource_with_fhir_version(
        &self,
        canonical_url: &str,
        fhir_version: &str,
    ) -> Result<Option<ResourceIndex>> {
        let url = canonical_url.to_string();
        let version = fhir_version.to_string();
        self.with_connection(move |conn| {
            let query = format!(
                "{} WHERE r.url = ?1 AND r.fhir_version = ?2 LIMIT 1",
                Self::RESOURCE_SELECT
            );
            conn.query_row(
                &query,
                rusqlite::params![url, version],
                extract_resource_index,
            )
            .optional()
        })
        .await
        .map_err(|err| match err {
            FcmError::Storage(StorageError::DatabaseError { message }) => {
                FcmError::Storage(StorageError::IoError {
                    message: format!("Failed to query resource: {message}"),
                })
            }
            other => other,
        })
    }

    pub async fn find_by_base_url(&self, base_url: &str) -> Result<Vec<ResourceIndex>> {
        let pattern = format!("{}%", base_url);
        self.with_connection(move |conn| {
            let query = format!("{} WHERE r.url LIKE ?1", Self::RESOURCE_SELECT);
            let mut stmt = conn.prepare(&query)?;
            let results = stmt
                .query_map(rusqlite::params![pattern], extract_resource_index)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(results)
        })
        .await
        .map_err(|err| match err {
            FcmError::Storage(StorageError::DatabaseError { message }) => {
                FcmError::Storage(StorageError::IoError {
                    message: format!("Failed to query resources: {message}"),
                })
            }
            other => other,
        })
    }

    /// Find a resource by its name field (case-insensitive).
    /// This enables SUSHI-compatible resolution where Parent: USCoreVitalSignsProfile
    /// can find a resource with name="USCoreVitalSignsProfile".
    pub async fn find_resource_by_name(&self, name: &str) -> Result<Option<ResourceIndex>> {
        let name_lower = name.to_lowercase();
        self.with_connection(move |conn| {
            let query = format!("{} WHERE r.name_lower = ?1 LIMIT 1", Self::RESOURCE_SELECT);
            conn.query_row(
                &query,
                rusqlite::params![name_lower],
                extract_resource_index,
            )
            .optional()
        })
        .await
        .map_err(|err| match err {
            FcmError::Storage(StorageError::DatabaseError { message }) => {
                FcmError::Storage(StorageError::IoError {
                    message: format!("Failed to query resource by name: {message}"),
                })
            }
            other => other,
        })
    }

    pub async fn find_latest_by_base_url(&self, base_url: &str) -> Result<Option<ResourceIndex>> {
        let resources = self.find_by_base_url(base_url).await?;
        Ok(resources.into_iter().next())
    }

    pub async fn get_resource(&self, resource_index: &ResourceIndex) -> Result<FhirResource> {
        let canonical_url = resource_index.canonical_url.clone();
        let resource_type = resource_index.resource_type.clone();
        let resource_id = resource_index.id.clone().unwrap_or_default();
        let resource_version = resource_index.version.clone();
        let file_path = resource_index.file_path.clone();
        let package_name = resource_index.package_name.clone();
        let package_version = resource_index.package_version.clone();
        let fhir_version = resource_index.fhir_version.clone();

        let content_path: String = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            self.with_connection(move |conn| {
                conn.query_row(
                    "SELECT content_path FROM resources WHERE url = ?1 AND package_name = ?2 AND package_version = ?3 AND fhir_version = ?4",
                    rusqlite::params![canonical_url, package_name, package_version, fhir_version],
                    |row| row.get(0),
                )
            }),
        )
        .await
        .map_err(|_| {
            FcmError::Storage(StorageError::IoError {
                message: format!(
                    "Timeout getting content_path for {}",
                    resource_index.canonical_url
                ),
            })
        })?
        .map_err(|err| match err {
            FcmError::Storage(StorageError::DatabaseError { message }) => {
                FcmError::Storage(StorageError::IoError {
                    message: format!("Failed to get resource content_path: {message}"),
                })
            }
            other => other,
        })?;

        let content = tokio::fs::read_to_string(&content_path)
            .await
            .map_err(|e| {
                FcmError::Storage(StorageError::IoError {
                    message: format!(
                        "Failed to read resource from CAS at {}: {}",
                        content_path, e
                    ),
                })
            })?;

        let json_content: serde_json::Value = serde_json::from_str(&content).map_err(|e| {
            FcmError::Storage(StorageError::IoError {
                message: format!("Failed to parse resource JSON from CAS: {e}"),
            })
        })?;

        Ok(FhirResource {
            resource_type,
            id: resource_id,
            url: Some(resource_index.canonical_url.clone()),
            version: resource_version,
            content: json_content,
            file_path,
        })
    }

    /// Find resource by exact resource type and ID match (case-insensitive)
    /// This is much faster than text search for exact lookups
    pub async fn find_by_type_and_id(
        &self,
        resource_type: String,
        id: String,
    ) -> Result<Vec<ResourceIndex>> {
        let id_lower = id.to_lowercase();
        self.with_connection(move |conn| {
            let query = format!(
                "{} WHERE r.resource_type = ?1 AND r.id_lower = ?2",
                Self::RESOURCE_SELECT
            );
            let mut stmt = conn.prepare(&query)?;
            let resources = stmt
                .query_map(
                    rusqlite::params![resource_type, id_lower],
                    extract_resource_index,
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(resources)
        })
        .await
    }

    /// Find resource by exact resource type and name match (case-insensitive)
    /// Useful for US Core profiles that have names like "USCoreMedicationRequestProfile"
    pub async fn find_by_type_and_name(
        &self,
        resource_type: String,
        name: String,
    ) -> Result<Vec<ResourceIndex>> {
        let name_lower = name.to_lowercase();
        self.with_connection(move |conn| {
            let query = format!(
                "{} WHERE r.resource_type = ?1 AND r.name_lower = ?2",
                Self::RESOURCE_SELECT
            );
            let mut stmt = conn.prepare(&query)?;
            let resources = stmt
                .query_map(
                    rusqlite::params![resource_type, name_lower],
                    extract_resource_index,
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(resources)
        })
        .await
    }

    /// Find all resources by resource type and package name
    /// This queries the database directly without any caching
    pub async fn find_by_type_and_package(
        &self,
        resource_type: &str,
        package_name: &str,
    ) -> Result<Vec<ResourceIndex>> {
        let rt = resource_type.to_string();
        let pkg = package_name.to_string();
        self.with_connection(move |conn| {
            let query = format!(
                "{} WHERE r.resource_type = ?1 AND r.package_name = ?2",
                Self::RESOURCE_SELECT
            );
            let mut stmt = conn.prepare(&query)?;
            let resources = stmt
                .query_map(rusqlite::params![rt, pkg], extract_resource_index)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(resources)
        })
        .await
    }

    /// List all base FHIR resource type names (e.g., "Patient", "Observation")
    /// These are StructureDefinitions with sd_flavor = 'Resource'
    pub async fn list_base_resource_type_names(&self, fhir_version: &str) -> Result<Vec<String>> {
        let version = fhir_version.to_string();
        self.with_connection(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT DISTINCT name FROM resources
                 WHERE resource_type = 'StructureDefinition'
                   AND sd_flavor = 'Resource'
                   AND fhir_version = ?1
                   AND name IS NOT NULL",
            )?;
            let names = stmt
                .query_map(rusqlite::params![version], |row| row.get(0))?
                .collect::<rusqlite::Result<Vec<String>>>()?;
            Ok(names)
        })
        .await
    }

    pub async fn list_packages(&self) -> Result<Vec<PackageInfo>> {
        self.with_connection(move |conn| {
            let mut stmt = conn.prepare(
                "SELECT name, version, installed_at, resource_count FROM packages ORDER BY name, version",
            )?;

            let packages = stmt
                .query_map([], |row| {
                    Ok(PackageInfo {
                        name: row.get(0)?,
                        version: row.get(1)?,
                        installed_at: DateTime::parse_from_rfc3339(&row.get::<_, String>(2)?)
                            .map(|dt| dt.with_timezone(&Utc))
                            .unwrap_or_else(|_| Utc::now()),
                        resource_count: row.get(3)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;

            Ok(packages)
        })
        .await
    }

    pub async fn remove_package(&self, name: &str, version: &str) -> Result<bool> {
        let pkg_name = name.to_string();
        let pkg_version = version.to_string();
        self.with_connection(move |conn| {
            let tx = conn.unchecked_transaction()?;

            tx.execute(
                "DELETE FROM resources WHERE package_name = ?1 AND package_version = ?2",
                rusqlite::params![pkg_name, pkg_version],
            )?;

            let deleted = tx.execute(
                "DELETE FROM packages WHERE name = ?1 AND version = ?2",
                rusqlite::params![pkg_name, pkg_version],
            )?;

            tx.commit()?;
            Ok(deleted > 0)
        })
        .await
    }

    pub async fn get_cache_entries(&self) -> Vec<ResourceIndex> {
        match self
            .with_connection(move |conn| {
                let query = Self::RESOURCE_SELECT;
                let mut stmt = conn.prepare(query)?;
                let rows = stmt
                    .query_map([], extract_resource_index)?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows)
            })
            .await
        {
            Ok(rows) => rows,
            Err(e) => {
                warn!("Failed to acquire snapshot of cache entries: {}", e);
                Vec::new()
            }
        }
    }

    /// Find resource by key (matches id, name, or url - case-insensitive for id/name)
    /// Following fhir-package-loader pattern for type filtering and priority ordering.
    ///
    /// # Arguments
    /// * `key` - The identifier to search for (matches id, name, or url)
    /// * `types` - Optional list of SD flavors or resource types to filter by
    /// * `exclude_extensions` - If true, excludes resources with sd_flavor = 'Extension'
    /// * `sort_by_priority` - If true, orders by package priority DESC then rowid DESC
    pub async fn find_resource_info(
        &self,
        key: &str,
        types: Option<&[&str]>,
        exclude_extensions: bool,
        sort_by_priority: bool,
    ) -> Result<Option<ResourceIndex>> {
        let key_lower = key.to_lowercase();
        let key_url = key.to_string();

        // Build type conditions
        let types_owned: Option<Vec<String>> =
            types.map(|t| t.iter().map(|s| s.to_string()).collect());

        self.with_connection(move |conn| {
            let mut conditions =
                vec!["(r.id_lower = ?1 OR r.name_lower = ?1 OR r.url = ?2)".to_string()];

            let mut params: Vec<Box<dyn rusqlite::ToSql>> =
                vec![Box::new(key_lower.clone()), Box::new(key_url.clone())];

            if let Some(ref types) = types_owned {
                let sd_flavors: Vec<&str> = types
                    .iter()
                    .filter(|t| SD_FLAVORS.contains(&t.as_str()))
                    .map(|s| s.as_str())
                    .collect();
                let resource_types: Vec<&str> = types
                    .iter()
                    .filter(|t| !SD_FLAVORS.contains(&t.as_str()))
                    .map(|s| s.as_str())
                    .collect();

                let mut type_conditions = Vec::new();

                if !sd_flavors.is_empty() {
                    let placeholders: Vec<String> = sd_flavors
                        .iter()
                        .enumerate()
                        .map(|(i, _)| format!("?{}", params.len() + i + 1))
                        .collect();
                    type_conditions.push(format!(
                        "(r.resource_type = 'StructureDefinition' AND r.sd_flavor IN ({}))",
                        placeholders.join(", ")
                    ));
                    for flavor in sd_flavors {
                        params.push(Box::new(flavor.to_string()));
                    }
                }

                if !resource_types.is_empty() {
                    let placeholders: Vec<String> = resource_types
                        .iter()
                        .enumerate()
                        .map(|(i, _)| format!("?{}", params.len() + i + 1))
                        .collect();
                    type_conditions
                        .push(format!("r.resource_type IN ({})", placeholders.join(", ")));
                    for rt in resource_types {
                        params.push(Box::new(rt.to_string()));
                    }
                }

                if !type_conditions.is_empty() {
                    conditions.push(format!("({})", type_conditions.join(" OR ")));
                }
            }

            if exclude_extensions {
                conditions.push("(r.sd_flavor IS NULL OR r.sd_flavor != 'Extension')".to_string());
            }

            let order = if sort_by_priority {
                "ORDER BY p.priority DESC, r.rowid DESC"
            } else {
                "ORDER BY r.rowid ASC"
            };

            let query = format!(
                "{} WHERE {} {} LIMIT 1",
                Self::RESOURCE_SELECT,
                conditions.join(" AND "),
                order
            );

            let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p.as_ref()).collect();

            conn.query_row(&query, param_refs.as_slice(), extract_resource_index)
                .optional()
        })
        .await
        .map_err(|err| match err {
            FcmError::Storage(StorageError::DatabaseError { message }) => {
                FcmError::Storage(StorageError::IoError {
                    message: format!("Failed to find resource info: {message}"),
                })
            }
            other => other,
        })
    }

    /// Find all matching resources by key with type filtering and priority ordering.
    /// Returns all matches instead of just the first one.
    pub async fn find_resource_infos(
        &self,
        key: &str,
        types: Option<&[&str]>,
        limit: Option<usize>,
    ) -> Result<Vec<ResourceIndex>> {
        let key_lower = key.to_lowercase();
        let key_url = key.to_string();
        let types_owned: Option<Vec<String>> =
            types.map(|t| t.iter().map(|s| s.to_string()).collect());

        self.with_connection(move |conn| {
            let mut conditions =
                vec!["(r.id_lower = ?1 OR r.name_lower = ?1 OR r.url = ?2)".to_string()];

            let mut params: Vec<Box<dyn rusqlite::ToSql>> =
                vec![Box::new(key_lower.clone()), Box::new(key_url.clone())];

            if let Some(ref types) = types_owned {
                let sd_flavors: Vec<&str> = types
                    .iter()
                    .filter(|t| SD_FLAVORS.contains(&t.as_str()))
                    .map(|s| s.as_str())
                    .collect();
                let resource_types: Vec<&str> = types
                    .iter()
                    .filter(|t| !SD_FLAVORS.contains(&t.as_str()))
                    .map(|s| s.as_str())
                    .collect();

                let mut type_conditions = Vec::new();

                if !sd_flavors.is_empty() {
                    let placeholders: Vec<String> = sd_flavors
                        .iter()
                        .enumerate()
                        .map(|(i, _)| format!("?{}", params.len() + i + 1))
                        .collect();
                    type_conditions.push(format!(
                        "(r.resource_type = 'StructureDefinition' AND r.sd_flavor IN ({}))",
                        placeholders.join(", ")
                    ));
                    for flavor in sd_flavors {
                        params.push(Box::new(flavor.to_string()));
                    }
                }

                if !resource_types.is_empty() {
                    let placeholders: Vec<String> = resource_types
                        .iter()
                        .enumerate()
                        .map(|(i, _)| format!("?{}", params.len() + i + 1))
                        .collect();
                    type_conditions
                        .push(format!("r.resource_type IN ({})", placeholders.join(", ")));
                    for rt in resource_types {
                        params.push(Box::new(rt.to_string()));
                    }
                }

                if !type_conditions.is_empty() {
                    conditions.push(format!("({})", type_conditions.join(" OR ")));
                }
            }

            let mut query = format!(
                "{} WHERE {} ORDER BY r.rowid ASC",
                Self::RESOURCE_SELECT,
                conditions.join(" AND ")
            );

            if let Some(lim) = limit {
                query.push_str(&format!(" LIMIT {}", lim));
            }

            let param_refs: Vec<&dyn rusqlite::ToSql> = params.iter().map(|p| p.as_ref()).collect();

            let mut stmt = conn.prepare(&query)?;
            let resources = stmt
                .query_map(param_refs.as_slice(), extract_resource_index)?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(resources)
        })
        .await
        .map_err(|err| match err {
            FcmError::Storage(StorageError::DatabaseError { message }) => {
                FcmError::Storage(StorageError::IoError {
                    message: format!("Failed to find resource infos: {message}"),
                })
            }
            other => other,
        })
    }

    pub async fn compact(&self) -> Result<()> {
        self.with_connection(move |conn| {
            conn.execute_batch("VACUUM; ANALYZE;")?;
            Ok(())
        })
        .await?;
        info!("SQLite storage compacted");
        Ok(())
    }

    pub async fn get_stats(&self) -> Result<StorageStats> {
        let (package_count, resource_count) = self
            .with_connection(move |conn| {
                let package_count: i64 =
                    conn.query_row("SELECT COUNT(*) FROM packages", [], |row| row.get(0))?;

                let resource_count: i64 =
                    conn.query_row("SELECT COUNT(*) FROM resources", [], |row| row.get(0))?;

                Ok((package_count, resource_count))
            })
            .await?;

        let file_size = tokio::fs::metadata(&self.db_path)
            .await
            .map(|m| m.len())
            .unwrap_or(0);

        Ok(StorageStats {
            package_count: package_count as usize,
            resource_count: resource_count as usize,
            storage_size_bytes: file_size,
        })
    }

    pub async fn integrity_check(&self) -> Result<IntegrityReport> {
        let integrity = self
            .with_connection(move |conn| {
                conn.query_row("PRAGMA integrity_check", [], |row| row.get(0))
            })
            .await?;

        let is_ok = integrity == "ok";

        Ok(IntegrityReport {
            is_valid: is_ok,
            errors: if is_ok { vec![] } else { vec![integrity] },
            warnings: vec![],
        })
    }
}

/// Extracted StructureDefinition-specific fields (following fhir-package-loader)
#[derive(Debug, Clone, Default)]
pub struct SdFields {
    pub sd_kind: Option<String>,
    pub sd_derivation: Option<String>,
    pub sd_type: Option<String>,
    pub sd_base_definition: Option<String>,
    pub sd_abstract: Option<bool>,
    pub sd_impose_profiles: Option<Vec<String>>,
    pub sd_characteristics: Option<Vec<String>>,
    pub sd_flavor: Option<String>,
}

/// Compute SD flavor from FHIR resource JSON (same logic as fhir-package-loader)
pub fn compute_sd_flavor(content: &serde_json::Value) -> Option<&'static str> {
    if content.get("resourceType")?.as_str()? != "StructureDefinition" {
        return None;
    }

    let type_val = content.get("type").and_then(|v| v.as_str());
    let kind = content.get("kind").and_then(|v| v.as_str());
    let derivation = content.get("derivation").and_then(|v| v.as_str());
    let base_def = content.get("baseDefinition").and_then(|v| v.as_str());

    // Extension: type='Extension' and not base Element (same as fhir-package-loader)
    if type_val == Some("Extension")
        && base_def != Some("http://hl7.org/fhir/StructureDefinition/Element")
    {
        return Some("Extension");
    }

    // Profile: derivation='constraint'
    if derivation == Some("constraint") {
        return Some("Profile");
    }

    // Type: kind contains 'type' (primitive-type, complex-type, datatype)
    if let Some(k) = kind
        && k.contains("type")
    {
        return Some("Type");
    }

    // Resource: kind='resource'
    if kind == Some("resource") {
        return Some("Resource");
    }

    // Logical: kind='logical'
    if kind == Some("logical") {
        return Some("Logical");
    }

    None
}

/// Extract all SD-specific fields from FHIR resource JSON
pub fn extract_sd_fields(content: &serde_json::Value) -> SdFields {
    if content.get("resourceType").and_then(|v| v.as_str()) != Some("StructureDefinition") {
        return SdFields::default();
    }

    let sd_kind = content
        .get("kind")
        .and_then(|v| v.as_str())
        .map(String::from);
    let sd_derivation = content
        .get("derivation")
        .and_then(|v| v.as_str())
        .map(String::from);
    let sd_type = content
        .get("type")
        .and_then(|v| v.as_str())
        .map(String::from);
    let sd_base_definition = content
        .get("baseDefinition")
        .and_then(|v| v.as_str())
        .map(String::from);
    let sd_abstract = content.get("abstract").and_then(|v| v.as_bool());

    // Extract imposeProfile extension (like fhir-package-loader)
    let sd_impose_profiles = content
        .get("extension")
        .and_then(|v| v.as_array())
        .map(|exts| {
            exts.iter()
                .filter(|ext| {
                    ext.get("url").and_then(|u| u.as_str())
                        == Some("http://hl7.org/fhir/StructureDefinition/structuredefinition-imposeProfile")
                })
                .filter_map(|ext| {
                    ext.get("valueCanonical")
                        .and_then(|v| v.as_str())
                        .map(String::from)
                })
                .collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty());

    // Extract type-characteristics extension (like fhir-package-loader)
    let sd_characteristics = content
        .get("extension")
        .and_then(|v| v.as_array())
        .map(|exts| {
            exts.iter()
                .filter(|ext| {
                    ext.get("url").and_then(|u| u.as_str())
                        == Some("http://hl7.org/fhir/StructureDefinition/structuredefinition-type-characteristics")
                })
                .filter_map(|ext| {
                    ext.get("valueCode")
                        .and_then(|v| v.as_str())
                        .map(String::from)
                })
                .collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty());

    let sd_flavor = compute_sd_flavor(content).map(String::from);

    SdFields {
        sd_kind,
        sd_derivation,
        sd_type,
        sd_base_definition,
        sd_abstract,
        sd_impose_profiles,
        sd_characteristics,
        sd_flavor,
    }
}

fn extract_resource_index(row: &rusqlite::Row<'_>) -> rusqlite::Result<ResourceIndex> {
    // New schema: columns are in order of the SELECT statement
    // url, resource_type, package_name, package_version, content_path, fhir_version,
    // id, name, version, sd_kind, sd_derivation, sd_type, sd_base_definition, sd_abstract,
    // sd_impose_profiles, sd_characteristics, sd_flavor
    let sd_impose_profiles_json: Option<String> = row.get(14)?;
    let sd_characteristics_json: Option<String> = row.get(15)?;

    Ok(ResourceIndex {
        canonical_url: row.get(0)?,
        resource_type: row.get(1)?,
        package_name: row.get(2)?,
        package_version: row.get(3)?,
        file_path: PathBuf::from(row.get::<_, String>(4)?),
        fhir_version: row.get(5)?,
        id: row.get(6)?,
        name: row.get(7)?,
        version: row.get(8)?,
        sd_kind: row.get(9)?,
        sd_derivation: row.get(10)?,
        sd_type: row.get(11)?,
        sd_base_definition: row.get(12)?,
        sd_abstract: row.get::<_, Option<i32>>(13)?.map(|v| v != 0),
        sd_impose_profiles: sd_impose_profiles_json.and_then(|s| serde_json::from_str(&s).ok()),
        sd_characteristics: sd_characteristics_json.and_then(|s| serde_json::from_str(&s).ok()),
        sd_flavor: row.get(16)?,
    })
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct StorageStats {
    pub package_count: usize,
    pub resource_count: usize,
    pub storage_size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityReport {
    pub is_valid: bool,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

// Implement SearchStorage trait for SqliteStorage
#[async_trait::async_trait]
impl crate::traits::SearchStorage for SqliteStorage {
    async fn find_resource(&self, canonical_url: &str) -> Result<Option<ResourceIndex>> {
        SqliteStorage::find_resource(self, canonical_url).await
    }

    async fn find_resource_with_fhir_version(
        &self,
        canonical_url: &str,
        fhir_version: &str,
    ) -> Result<Option<ResourceIndex>> {
        SqliteStorage::find_resource_with_fhir_version(self, canonical_url, fhir_version).await
    }

    async fn find_by_base_url(&self, base_url: &str) -> Result<Vec<ResourceIndex>> {
        SqliteStorage::find_by_base_url(self, base_url).await
    }

    async fn find_latest_by_base_url(&self, base_url: &str) -> Result<Option<ResourceIndex>> {
        SqliteStorage::find_latest_by_base_url(self, base_url).await
    }

    async fn find_resource_by_name(&self, name: &str) -> Result<Option<ResourceIndex>> {
        SqliteStorage::find_resource_by_name(self, name).await
    }

    async fn find_by_type_and_id(
        &self,
        resource_type: String,
        id: String,
    ) -> Result<Vec<ResourceIndex>> {
        SqliteStorage::find_by_type_and_id(self, resource_type, id).await
    }

    async fn find_by_type_and_name(
        &self,
        resource_type: String,
        name: String,
    ) -> Result<Vec<ResourceIndex>> {
        SqliteStorage::find_by_type_and_name(self, resource_type, name).await
    }

    async fn find_resource_info(
        &self,
        key: &str,
        types: Option<&[&str]>,
        exclude_extensions: bool,
        sort_by_priority: bool,
    ) -> Result<Option<ResourceIndex>> {
        SqliteStorage::find_resource_info(self, key, types, exclude_extensions, sort_by_priority)
            .await
    }

    async fn find_resource_infos(
        &self,
        key: &str,
        types: Option<&[&str]>,
        limit: Option<usize>,
    ) -> Result<Vec<ResourceIndex>> {
        SqliteStorage::find_resource_infos(self, key, types, limit).await
    }

    async fn list_base_resource_type_names(&self, fhir_version: &str) -> Result<Vec<String>> {
        SqliteStorage::list_base_resource_type_names(self, fhir_version).await
    }

    async fn get_resource(&self, resource_index: &ResourceIndex) -> Result<FhirResource> {
        SqliteStorage::get_resource(self, resource_index).await
    }

    async fn get_cache_entries(&self) -> Vec<ResourceIndex> {
        SqliteStorage::get_cache_entries(self).await
    }

    async fn find_by_type_and_package(
        &self,
        resource_type: &str,
        package_name: &str,
    ) -> Result<Vec<ResourceIndex>> {
        SqliteStorage::find_by_type_and_package(self, resource_type, package_name).await
    }

    async fn list_packages(&self) -> Result<Vec<PackageInfo>> {
        SqliteStorage::list_packages(self).await
    }

    async fn set_package_priority(
        &self,
        package_name: &str,
        package_version: &str,
        priority: i32,
    ) -> Result<()> {
        SqliteStorage::set_package_priority(self, package_name, package_version, priority).await
    }
}

// Implement PackageStore trait for SqliteStorage
#[async_trait::async_trait]
impl crate::traits::PackageStore for SqliteStorage {
    async fn add_package(&self, package: &crate::package::ExtractedPackage) -> Result<()> {
        SqliteStorage::add_package(self, package.clone()).await
    }

    async fn remove_package(&self, name: &str, version: &str) -> Result<bool> {
        SqliteStorage::remove_package(self, name, version).await
    }

    async fn find_resource(&self, canonical_url: &str) -> Result<Option<ResourceIndex>> {
        SqliteStorage::find_resource(self, canonical_url).await
    }

    async fn list_packages(&self) -> Result<Vec<PackageInfo>> {
        SqliteStorage::list_packages(self).await
    }
}
