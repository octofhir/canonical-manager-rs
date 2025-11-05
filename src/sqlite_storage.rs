//! SQLite-based storage for FHIR packages and resources
//!
//! Uses a deadpool-backed SQLite connection pool to provide async access
//! without blocking the Tokio runtime. All public APIs remain unchanged.

use crate::cas_storage::{CasStorage, ContentType};
use crate::config::StorageConfig;
use crate::content_hash::ContentHash;
use crate::error::{FcmError, Result, StorageError};
use crate::package::{ExtractedPackage, FhirResource};
use chrono::{DateTime, Utc};
use deadpool_sqlite::rusqlite::{self, OptionalExtension, types::Type};
use deadpool_sqlite::{Config as DeadpoolConfig, Pool, Runtime};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, info, warn};

const SCHEMA_VERSION: i32 = 3;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceIndex {
    pub canonical_url: String,
    pub resource_type: String,
    pub package_name: String,
    pub package_version: String,
    pub fhir_version: String,
    pub file_path: PathBuf,
    pub metadata: ResourceMetadata,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceMetadata {
    pub id: String,
    pub name: Option<String>,
    pub version: Option<String>,
    pub status: Option<String>,
    pub date: Option<String>,
    pub publisher: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageInfo {
    pub name: String,
    pub version: String,
    pub installed_at: DateTime<Utc>,
    pub resource_count: usize,
}

pub struct SqliteStorage {
    pool: Pool,
    db_path: PathBuf,
    cas: Arc<CasStorage>,
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
                -- Packages table: metadata only, no large content
                CREATE TABLE IF NOT EXISTS packages (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    version TEXT NOT NULL,
                    manifest_hash TEXT NOT NULL,
                    installed_at TEXT NOT NULL,
                    resource_count INTEGER NOT NULL,
                    UNIQUE(name, version)
                );

                -- Resources table: content-addressable, no JSON BLOBs
                CREATE TABLE IF NOT EXISTS resources (
                    canonical_url TEXT NOT NULL,
                    package_id TEXT NOT NULL,
                    resource_type TEXT NOT NULL,
                    fhir_version TEXT NOT NULL,
                    content_hash TEXT NOT NULL,
                    content_path TEXT NOT NULL,
                    metadata JSON NOT NULL,
                    PRIMARY KEY (canonical_url, package_id),
                    FOREIGN KEY(package_id) REFERENCES packages(id) ON DELETE CASCADE
                );

                -- Indexes for efficient queries
                CREATE INDEX IF NOT EXISTS idx_resource_type ON resources(resource_type);
                CREATE INDEX IF NOT EXISTS idx_package_id ON resources(package_id);
                CREATE INDEX IF NOT EXISTS idx_canonical_fhir_version ON resources(canonical_url, fhir_version);
                CREATE INDEX IF NOT EXISTS idx_package_name_version ON packages(name, version);
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

    pub async fn add_package(&self, package: ExtractedPackage) -> Result<()> {
        let package_id = format!("{}:{}", package.name, package.version);
        let package_name = package.name.clone();
        let package_version = package.version.clone();

        let package_id_lookup = package_id.clone();
        let exists: bool = self
            .with_connection(move |conn| {
                conn.query_row(
                    "SELECT 1 FROM packages WHERE id = ?1",
                    rusqlite::params![package_id_lookup],
                    |_| Ok(true),
                )
                .optional()
            })
            .await?
            .unwrap_or(false);

        if exists {
            return Err(FcmError::Storage(StorageError::PackageAlreadyExists {
                name: package_name,
                version: package_version,
            }));
        }

        let fhir_version = package
            .manifest
            .fhir_versions
            .as_ref()
            .and_then(|versions| versions.first())
            .cloned()
            .unwrap_or_else(|| "4.0.1".to_string());

        let mut resource_data = Vec::new();
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

            let metadata = ResourceMetadata {
                id: resource.id.clone(),
                name: resource
                    .content
                    .get("name")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                version: resource.version.clone(),
                status: resource
                    .content
                    .get("status")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                date: resource
                    .content
                    .get("date")
                    .and_then(|v| v.as_str())
                    .map(String::from),
                publisher: resource
                    .content
                    .get("publisher")
                    .and_then(|v| v.as_str())
                    .map(String::from),
            };

            let metadata_json = serde_json::to_string(&metadata).map_err(|e| {
                FcmError::Storage(StorageError::IoError {
                    message: format!("Failed to serialize metadata: {e}"),
                })
            })?;

            resource_data.push((
                canonical_url,
                resource.resource_type.clone(),
                content_hash.to_hex(),
                content_path,
                metadata_json,
            ));
        }

        let manifest_json = serde_json::to_string(&package.manifest).map_err(|e| {
            FcmError::Storage(StorageError::IoError {
                message: format!("Failed to serialize manifest: {e}"),
            })
        })?;
        let manifest_hash = ContentHash::from_bytes(manifest_json.as_bytes());
        let installed_at = Utc::now().to_rfc3339();
        let resource_count = resource_data.len();

        let package_id_tx = package_id.clone();
        let fhir_version_tx = fhir_version.clone();
        let manifest_hash_hex = manifest_hash.to_hex();

        let package_for_tx = package;
        self.with_connection(move |conn| {
            let tx = conn.unchecked_transaction()?;

            tx.execute(
                "INSERT INTO packages (id, name, version, manifest_hash, installed_at, resource_count)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    &package_id_tx,
                    &package_for_tx.name,
                    &package_for_tx.version,
                    &manifest_hash_hex,
                    &installed_at,
                    resource_count,
                ],
            )?;

            for (canonical_url, resource_type, content_hash, content_path, metadata_json) in
                resource_data
            {
                tx.execute(
                    "INSERT OR REPLACE INTO resources (canonical_url, package_id, resource_type, fhir_version, content_hash, content_path, metadata)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    rusqlite::params![
                        &canonical_url,
                        &package_id_tx,
                        &resource_type,
                        &fhir_version_tx,
                        &content_hash,
                        &content_path,
                        &metadata_json,
                    ],
                )?;
            }

            tx.commit()?;
            Ok(())
        })
        .await?;

        info!(
            "Package {} added to SQLite ({} resources)",
            package_id, resource_count
        );
        Ok(())
    }

    #[tracing::instrument(name = "sqlite.add_packages_batch", skip_all, fields(count = packages.len()))]
    pub async fn add_packages_batch(&self, packages: Vec<ExtractedPackage>) -> Result<()> {
        info!("Adding {} packages in batch mode", packages.len());
        let start = std::time::Instant::now();

        let mut batch_data = Vec::new();
        for package in packages {
            let package_id = format!("{}:{}", package.name, package.version);

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

            let mut resource_data = Vec::new();
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

                let metadata = ResourceMetadata {
                    id: resource.id.clone(),
                    name: resource
                        .content
                        .get("name")
                        .and_then(|v| v.as_str())
                        .map(String::from),
                    version: resource.version.clone(),
                    status: resource
                        .content
                        .get("status")
                        .and_then(|v| v.as_str())
                        .map(String::from),
                    date: resource
                        .content
                        .get("date")
                        .and_then(|v| v.as_str())
                        .map(String::from),
                    publisher: resource
                        .content
                        .get("publisher")
                        .and_then(|v| v.as_str())
                        .map(String::from),
                };

                let metadata_json = serde_json::to_string(&metadata).map_err(|e| {
                    FcmError::Storage(StorageError::IoError {
                        message: format!("Failed to serialize metadata: {e}"),
                    })
                })?;

                resource_data.push((
                    canonical_url,
                    resource.resource_type.clone(),
                    content_hash.to_hex(),
                    content_path,
                    metadata_json,
                ));
            }

            batch_data.push((
                package_id,
                package.name,
                package.version,
                manifest_hash.to_hex(),
                package.resources.len(),
                fhir_version,
                resource_data,
            ));
        }

        let package_count = batch_data.len();
        self.with_connection(move |conn| {
            let tx = conn.unchecked_transaction()?;

            let mut package_stmt = tx.prepare(
                "INSERT OR REPLACE INTO packages (id, name, version, manifest_hash, installed_at, resource_count)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )?;

            let mut resource_stmt = tx.prepare(
                "INSERT OR REPLACE INTO resources (canonical_url, package_id, resource_type, fhir_version, content_hash, content_path, metadata)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )?;

            let now = Utc::now().to_rfc3339();

            for (package_id, name, version, manifest_hash, resource_count, fhir_version, resources) in
                batch_data
            {
                package_stmt.execute(rusqlite::params![
                    &package_id,
                    &name,
                    &version,
                    &manifest_hash,
                    &now,
                    resource_count,
                ])?;

                for (canonical_url, resource_type, content_hash, content_path, metadata_json) in resources {
                    resource_stmt.execute(rusqlite::params![
                        &canonical_url,
                        &package_id,
                        &resource_type,
                        &fhir_version,
                        &content_hash,
                        &content_path,
                        &metadata_json,
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

    #[tracing::instrument(name = "sqlite.find_resource", skip(self), fields(key = %canonical_url))]
    pub async fn find_resource(&self, canonical_url: &str) -> Result<Option<ResourceIndex>> {
        let url = canonical_url.to_string();
        self.with_connection(move |conn| {
            conn.query_row(
                r#"
            SELECT r.canonical_url, r.resource_type, p.name, p.version, r.content_path, r.metadata, r.fhir_version
            FROM resources r
            JOIN packages p ON r.package_id = p.id
            WHERE r.canonical_url = ?1
            LIMIT 1
            "#,
                rusqlite::params![url],
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

    pub async fn find_resource_with_fhir_version(
        &self,
        canonical_url: &str,
        fhir_version: &str,
    ) -> Result<Option<ResourceIndex>> {
        let url = canonical_url.to_string();
        let version = fhir_version.to_string();
        self.with_connection(move |conn| {
            conn.query_row(
                r#"
            SELECT r.canonical_url, r.resource_type, p.name, p.version, r.content_path, r.metadata, r.fhir_version
            FROM resources r
            JOIN packages p ON r.package_id = p.id
            WHERE r.canonical_url = ?1 AND r.fhir_version = ?2
            LIMIT 1
            "#,
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
            let mut stmt = conn.prepare(
                r#"
                SELECT r.canonical_url, r.resource_type, p.name, p.version, r.content_path, r.metadata, r.fhir_version
                FROM resources r
                JOIN packages p ON r.package_id = p.id
                WHERE r.canonical_url LIKE ?1
                "#,
            )?;

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

    /// Find a resource by its name field (from metadata JSON).
    /// This enables SUSHI-compatible resolution where Parent: USCoreVitalSignsProfile
    /// can find a resource with name="USCoreVitalSignsProfile".
    pub async fn find_resource_by_name(&self, name: &str) -> Result<Option<ResourceIndex>> {
        let name_query = name.to_string();
        self.with_connection(move |conn| {
            conn.query_row(
                r#"
                SELECT r.canonical_url, r.resource_type, p.name, p.version, r.content_path, r.metadata, r.fhir_version
                FROM resources r
                JOIN packages p ON r.package_id = p.id
                WHERE json_extract(r.metadata, '$.name') = ?1
                LIMIT 1
                "#,
                rusqlite::params![name_query],
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
        let metadata_id = resource_index.metadata.id.clone();
        let metadata_version = resource_index.metadata.version.clone();
        let file_path = resource_index.file_path.clone();

        let content_path: String = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            self.with_connection(move |conn| {
                conn.query_row(
                    "SELECT content_path FROM resources WHERE canonical_url = ?1",
                    rusqlite::params![canonical_url],
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
            id: metadata_id,
            url: Some(resource_index.canonical_url.clone()),
            version: metadata_version,
            content: json_content,
            file_path,
        })
    }

    /// Find resource by exact resource type and metadata ID match
    /// This is much faster than text search for exact lookups
    pub async fn find_by_type_and_id(
        &self,
        resource_type: String,
        id: String,
    ) -> Result<Vec<ResourceIndex>> {
        self.with_connection(move |conn| {
            let mut stmt = conn.prepare(
                r#"
                SELECT r.canonical_url, r.resource_type, p.name, p.version, r.content_path, r.metadata, r.fhir_version
                FROM resources r
                JOIN packages p ON r.package_id = p.id
                WHERE r.resource_type = ?1
                  AND json_extract(r.metadata, '$.id') = ?2
                "#,
            )?;

            let resources = stmt
                .query_map(rusqlite::params![resource_type, id], |row| {
                    extract_resource_index(row)
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;

            Ok(resources)
        })
        .await
    }

    /// Find resource by exact resource type and metadata name match
    /// Useful for US Core profiles that have names like "USCoreMedicationRequestProfile"
    pub async fn find_by_type_and_name(
        &self,
        resource_type: String,
        name: String,
    ) -> Result<Vec<ResourceIndex>> {
        self.with_connection(move |conn| {
            let mut stmt = conn.prepare(
                r#"
                SELECT r.canonical_url, r.resource_type, p.name, p.version, r.content_path, r.metadata, r.fhir_version
                FROM resources r
                JOIN packages p ON r.package_id = p.id
                WHERE r.resource_type = ?1
                  AND json_extract(r.metadata, '$.name') = ?2
                "#,
            )?;

            let resources = stmt
                .query_map(rusqlite::params![resource_type, name], |row| {
                    extract_resource_index(row)
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;

            Ok(resources)
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
        let package_id = format!("{}:{}", name, version);
        self.with_connection(move |conn| {
            let tx = conn.unchecked_transaction()?;

            tx.execute(
                "DELETE FROM resources WHERE package_id = ?1",
                rusqlite::params![package_id],
            )?;

            let deleted = tx.execute(
                "DELETE FROM packages WHERE id = ?1",
                rusqlite::params![package_id],
            )?;

            tx.commit()?;
            Ok(deleted > 0)
        })
        .await
    }

    pub async fn get_cache_entries(&self) -> HashMap<String, ResourceIndex> {
        match self
            .with_connection(move |conn| {
                let mut stmt = conn.prepare(
                    r#"
                    SELECT r.canonical_url, r.resource_type, p.name, p.version, r.content_path, r.metadata, r.fhir_version
                    FROM resources r
                    JOIN packages p ON r.package_id = p.id
                    "#,
                )?;

                let rows = stmt
                    .query_map([], |row| {
                        let key: String = row.get(0)?;
                        let value = extract_resource_index(row)?;
                        Ok((key, value))
                    })?
                    .collect::<rusqlite::Result<Vec<_>>>()?;

                Ok(rows)
            })
            .await
        {
            Ok(rows) => rows.into_iter().collect(),
            Err(e) => {
                warn!("Failed to acquire snapshot of cache entries: {}", e);
                HashMap::new()
            }
        }
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

fn extract_resource_index(row: &rusqlite::Row<'_>) -> rusqlite::Result<ResourceIndex> {
    let metadata_json: String = row.get(5)?;
    let metadata: ResourceMetadata = serde_json::from_str(&metadata_json)
        .map_err(|e| rusqlite::Error::FromSqlConversionFailure(5, Type::Text, Box::new(e)))?;

    Ok(ResourceIndex {
        canonical_url: row.get(0)?,
        resource_type: row.get(1)?,
        package_name: row.get(2)?,
        package_version: row.get(3)?,
        fhir_version: row.get(6)?,
        file_path: PathBuf::from(row.get::<_, String>(4)?),
        metadata,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
