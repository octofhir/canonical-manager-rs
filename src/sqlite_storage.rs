//! SQLite-based storage for FHIR packages and resources
//! Replaces BinaryStorage with industry-standard SQL database
//! Uses tokio-rusqlite for async-first SQLite operations to prevent thread pool starvation

use crate::config::StorageConfig;
use crate::error::{FcmError, Result, StorageError};
use crate::package::{ExtractedPackage, FhirResource};
use chrono::{DateTime, Utc};
use rusqlite::{OptionalExtension, types::Type};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use tokio_rusqlite::{Connection, Result as TokioResult, params};
use tracing::{debug, info};

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
    conn: Arc<Connection>,
    #[allow(dead_code)]
    config: StorageConfig,
    db_path: PathBuf,
}

impl SqliteStorage {
    pub async fn new(config: StorageConfig) -> Result<Self> {
        tokio::fs::create_dir_all(&config.packages_dir).await?;
        let db_path = config.packages_dir.join("storage.db");

        let conn = Connection::open(&db_path).await.map_err(|e| {
            FcmError::Storage(StorageError::IoError {
                message: format!("Failed to open SQLite database: {}", e),
            })
        })?;

        let storage = Self {
            conn: Arc::new(conn),
            config,
            db_path: db_path.clone(),
        };

        storage.init_schema().await?;
        info!("SQLite storage initialized at {:?}", db_path);
        Ok(storage)
    }

    async fn init_schema(&self) -> Result<()> {
        let conn = Arc::clone(&self.conn);

        // Set up basic database configuration
        conn.call(|conn: &mut rusqlite::Connection| -> TokioResult<()> {
            conn.execute_batch(
                r#"
                PRAGMA journal_mode = WAL;
                PRAGMA synchronous = NORMAL;
                PRAGMA cache_size = -64000;
                PRAGMA temp_store = MEMORY;
                PRAGMA foreign_keys = ON;
                "#,
            )?;
            Ok(())
        })
        .await?;

        // Create metadata table first to track schema version
        conn.call(|conn: &mut rusqlite::Connection| -> TokioResult<()> {
            conn.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS metadata (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                "#,
            )?;
            Ok(())
        })
        .await?;

        // Check current schema version
        let current_version: i32 = conn
            .call(
                |conn: &mut rusqlite::Connection| -> TokioResult<Option<String>> {
                    Ok(conn
                        .query_row(
                            "SELECT value FROM metadata WHERE key = 'schema_version'",
                            [],
                            |row| row.get::<_, String>(0),
                        )
                        .optional()?)
                },
            )
            .await?
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);

        if current_version == 0 {
            // New database - create current schema
            self.create_schema().await?;
        } else if current_version < SCHEMA_VERSION {
            // Breaking change: No migration path between major schema versions
            // User must delete old database and recreate
            return Err(FcmError::Storage(StorageError::UnsupportedSchemaVersion {
                current: current_version,
                required: SCHEMA_VERSION,
                message: format!(
                    "Database schema changed. Please delete ~/.maki/index/fhir.db and run 'maki config init'"
                ),
            }));
        }

        // Update schema version
        conn.call(move |conn| -> TokioResult<usize> {
            Ok(conn.execute(
                "INSERT OR REPLACE INTO metadata (key, value) VALUES (?1, ?2)",
                params!["schema_version", SCHEMA_VERSION.to_string()],
            )?)
        })
        .await?;

        debug!("SQLite schema initialized (version {})", SCHEMA_VERSION);
        Ok(())
    }

    async fn create_schema(&self) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        conn.call(|conn: &mut rusqlite::Connection| -> TokioResult<()> {
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

    // Migration removed: No migrations until approach is stabilized
    // Users with old schema must recreate database
    #[allow(dead_code)]
    async fn migrate_schema(&self, from_version: i32) -> Result<()> {
        info!(
            "Migrating schema from version {} to {}",
            from_version, SCHEMA_VERSION
        );

        if from_version == 1 {
            // Migration from v1 to v2: Add fhir_version column and change primary key
            let conn = Arc::clone(&self.conn);

            // Create new resources table with updated schema
            conn.call(|conn: &mut rusqlite::Connection| -> TokioResult<()> {
                conn.execute_batch(
                r#"
                -- Create new resources table with v2 schema
                CREATE TABLE resources_v2 (
                    canonical_url TEXT NOT NULL,
                    package_id TEXT NOT NULL,
                    resource_type TEXT NOT NULL,
                    fhir_version TEXT NOT NULL DEFAULT '4.0.1',
                    file_path TEXT NOT NULL,
                    content JSON NOT NULL,
                    metadata JSON NOT NULL,
                    PRIMARY KEY (canonical_url, package_id),
                    FOREIGN KEY(package_id) REFERENCES packages(id) ON DELETE CASCADE
                );

                -- Copy data from old table, extracting FHIR version from package manifest
                INSERT INTO resources_v2
                    (canonical_url, package_id, resource_type, fhir_version, file_path, content, metadata)
                SELECT
                    r.canonical_url,
                    r.package_id,
                    r.resource_type,
                    COALESCE(
                        json_extract(p.manifest, '$.fhirVersions[0]'),
                        '4.0.1'
                    ) as fhir_version,
                    r.file_path,
                    r.content,
                    r.metadata
                FROM resources r
                JOIN packages p ON r.package_id = p.id;

                -- Drop old table and rename new one
                DROP TABLE resources;
                ALTER TABLE resources_v2 RENAME TO resources;

                -- Recreate indexes
                CREATE INDEX idx_resource_type ON resources(resource_type);
                CREATE INDEX idx_package_id ON resources(package_id);
                CREATE INDEX idx_canonical_fhir_version ON resources(canonical_url, fhir_version);
                "#,
                )?;
                Ok(())
            })
            .await?;

            info!("Successfully migrated from schema v1 to v2");
        }

        Ok(())
    }

    pub async fn add_package(&self, package: ExtractedPackage) -> Result<()> {
        // Take ownership to ensure 'static lifetime
        let package_owned = package;

        let pkg_name = package_owned.name.clone();
        let pkg_version = package_owned.version.clone();

        let package_id = format!("{}:{}", package_owned.name, package_owned.version);
        let package_name = package_owned.name.clone();
        let package_version = package_owned.version.clone();
        let conn = Arc::clone(&self.conn);

        // Check if package already exists
        let package_id_check = package_id.clone();
        let exists: bool = conn
            .call(move |conn| -> TokioResult<Option<bool>> {
                Ok(conn
                    .query_row(
                        "SELECT 1 FROM packages WHERE id = ?1",
                        params![package_id_check],
                        |_| Ok(true),
                    )
                    .optional()?)
            })
            .await?
            .unwrap_or(false);

        if exists {
            return Err(FcmError::Storage(StorageError::PackageAlreadyExists {
                name: package_name,
                version: package_version,
            }));
        }

        // Prepare for transaction
        let conn = Arc::clone(&self.conn);

        // Perform the entire transaction in async context
        conn.call(move |conn| -> TokioResult<()> {
            let tx = conn.unchecked_transaction()?;

            let manifest_json = serde_json::to_string(&package_owned.manifest)
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

        // Extract FHIR version from package manifest
        let fhir_version = package_owned
            .manifest
            .fhir_versions
            .as_ref()
            .and_then(|versions| versions.first())
            .cloned()
            .unwrap_or_else(|| "4.0.1".to_string());

            tx.execute(
                "INSERT INTO packages (id, name, version, manifest, installed_at, resource_count)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    package_id,
                    package_owned.name,
                    package_owned.version,
                    manifest_json,
                    Utc::now().to_rfc3339(),
                    package_owned.resources.len(),
                ],
            )?;

            for resource in &package_owned.resources {
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

                let content_json = serde_json::to_string(&resource.content)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
                let metadata_json = serde_json::to_string(&metadata)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;

                // Use INSERT OR REPLACE to handle duplicate resources gracefully
                // This can happen if a previous installation was interrupted
                tx.execute(
                    "INSERT OR REPLACE INTO resources (canonical_url, package_id, resource_type, fhir_version, file_path, content, metadata)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    params![
                        canonical_url,
                        package_id,
                        resource.resource_type,
                        &fhir_version,
                        resource.file_path.to_string_lossy().to_string(),
                        content_json,
                        metadata_json,
                    ],
                )?;
            }

            tx.commit()?;

            info!(
                "Package {} added to SQLite ({} resources)",
                package_id,
                package_owned.resources.len()
            );
            Ok(())
        })
        .await?;
        Ok(())
    }

    #[tracing::instrument(name = "sqlite.find_resource", skip(self), fields(key = %canonical_url))]
    pub async fn find_resource(&self, canonical_url: &str) -> Result<Option<ResourceIndex>> {
        let conn = Arc::clone(&self.conn);
        let canonical_url = canonical_url.to_string();

        conn.call(move |conn| -> TokioResult<Option<ResourceIndex>> {
            Ok(conn.query_row(
                r#"
            SELECT r.canonical_url, r.resource_type, p.name, p.version, r.file_path, r.metadata, r.fhir_version
            FROM resources r
            JOIN packages p ON r.package_id = p.id
            WHERE r.canonical_url = ?1
            LIMIT 1
            "#,
                params![canonical_url],
                |row| {
                    let metadata_json: String = row.get(5)?;
                    let metadata: ResourceMetadata =
                        serde_json::from_str(&metadata_json).map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                5,
                                Type::Text,
                                Box::new(e),
                            )
                        })?;

                    Ok(ResourceIndex {
                        canonical_url: row.get(0)?,
                        resource_type: row.get(1)?,
                        package_name: row.get(2)?,
                        package_version: row.get(3)?,
                        fhir_version: row.get(6)?,
                        file_path: PathBuf::from(row.get::<_, String>(4)?),
                        metadata,
                    })
                },
            )
                .optional()?)
        })
        .await
        .map_err(|e| {
            FcmError::Storage(StorageError::IoError {
                message: format!("Failed to query resource: {}", e),
            })
        })
    }

    pub async fn find_resource_with_fhir_version(
        &self,
        canonical_url: &str,
        fhir_version: &str,
    ) -> Result<Option<ResourceIndex>> {
        let conn = Arc::clone(&self.conn);
        let canonical_url = canonical_url.to_string();
        let fhir_version = fhir_version.to_string();

        conn.call(move |conn| -> TokioResult<Option<ResourceIndex>> {
            Ok(conn.query_row(
                r#"
            SELECT r.canonical_url, r.resource_type, p.name, p.version, r.file_path, r.metadata, r.fhir_version
            FROM resources r
            JOIN packages p ON r.package_id = p.id
            WHERE r.canonical_url = ?1 AND r.fhir_version = ?2
            LIMIT 1
            "#,
                params![canonical_url, fhir_version],
                |row| {
                    let metadata_json: String = row.get(5)?;
                    let metadata: ResourceMetadata =
                        serde_json::from_str(&metadata_json).map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                5,
                                Type::Text,
                                Box::new(e),
                            )
                        })?;

                    Ok(ResourceIndex {
                        canonical_url: row.get(0)?,
                        resource_type: row.get(1)?,
                        package_name: row.get(2)?,
                        package_version: row.get(3)?,
                        fhir_version: row.get(6)?,
                        file_path: PathBuf::from(row.get::<_, String>(4)?),
                        metadata,
                    })
                },
                )
                .optional()?)
        })
        .await
        .map_err(|e| {
            FcmError::Storage(StorageError::IoError {
                message: format!("Failed to query resource: {}", e),
            })
        })
    }

    pub async fn find_by_base_url(&self, base_url: &str) -> Result<Vec<ResourceIndex>> {
        let conn = Arc::clone(&self.conn);
        let pattern = format!("{}%", base_url);

        conn.call(move |conn| {
            let mut stmt = conn
                .prepare(
                    r#"
                SELECT r.canonical_url, r.resource_type, p.name, p.version, r.file_path, r.metadata, r.fhir_version
                FROM resources r
                JOIN packages p ON r.package_id = p.id
                WHERE r.canonical_url LIKE ?1
                "#,
                )?;

            let results = stmt
                .query_map(params![pattern], |row| {
                let metadata_json: String = row.get(5)?;
                let metadata: ResourceMetadata =
                    serde_json::from_str(&metadata_json).map_err(|e| {
                        rusqlite::Error::FromSqlConversionFailure(
                            5,
                            Type::Text,
                            Box::new(e),
                        )
                    })?;

                Ok(ResourceIndex {
                    canonical_url: row.get(0)?,
                    resource_type: row.get(1)?,
                    package_name: row.get(2)?,
                    package_version: row.get(3)?,
                    fhir_version: row.get(6)?,
                    file_path: PathBuf::from(row.get::<_, String>(4)?),
                        metadata,
                    })
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;

            Ok(results)
        })
        .await
        .map_err(|e| {
            FcmError::Storage(StorageError::IoError {
                message: format!("Failed to query resources: {}", e),
            })
        })
    }

    pub async fn find_latest_by_base_url(&self, base_url: &str) -> Result<Option<ResourceIndex>> {
        let resources = self.find_by_base_url(base_url).await?;

        if resources.is_empty() {
            return Ok(None);
        }

        let latest = resources.into_iter().next().unwrap();
        Ok(Some(latest))
    }

    pub async fn get_resource(&self, resource_index: &ResourceIndex) -> Result<FhirResource> {
        let conn = Arc::clone(&self.conn);
        let canonical_url = resource_index.canonical_url.clone();
        let resource_type = resource_index.resource_type.clone();
        let metadata_id = resource_index.metadata.id.clone();
        let metadata_version = resource_index.metadata.version.clone();
        let file_path = resource_index.file_path.clone();

        let content: String = conn
            .call(move |conn| -> TokioResult<String> {
                Ok(conn.query_row(
                    "SELECT content FROM resources WHERE canonical_url = ?1",
                    params![canonical_url],
                    |row| row.get(0),
                )?)
            })
            .await
            .map_err(|e| {
                FcmError::Storage(StorageError::IoError {
                    message: format!("Failed to get resource content: {}", e),
                })
            })?;

        let json_content: serde_json::Value = serde_json::from_str(&content).map_err(|e| {
            FcmError::Storage(StorageError::IoError {
                message: format!("Failed to parse resource JSON: {}", e),
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

    pub async fn list_packages(&self) -> Result<Vec<PackageInfo>> {
        let conn = Arc::clone(&self.conn);

        conn.call(|conn: &mut rusqlite::Connection| -> TokioResult<Vec<PackageInfo>> {
            let mut stmt = conn.prepare(
                "SELECT name, version, installed_at, resource_count FROM packages ORDER BY name, version"
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
                .collect::<std::result::Result<Vec<_>, _>>()?;

            Ok(packages)
        })
        .await
        .map_err(Into::into)
    }

    pub async fn remove_package(&self, name: &str, version: &str) -> Result<bool> {
        let package_id = format!("{}:{}", name, version);
        let conn = Arc::clone(&self.conn);

        let result = conn
            .call(move |conn| -> TokioResult<bool> {
                let tx = conn.unchecked_transaction()?;

                tx.execute(
                    "DELETE FROM resources WHERE package_id = ?1",
                    params![package_id],
                )?;

                let deleted =
                    tx.execute("DELETE FROM packages WHERE id = ?1", params![package_id])?;

                tx.commit()?;

                Ok(deleted > 0)
            })
            .await?;

        Ok(result)
    }

    pub async fn get_cache_entries(&self) -> HashMap<String, ResourceIndex> {
        let conn = Arc::clone(&self.conn);

        conn.call(|conn: &mut rusqlite::Connection| {
            let mut stmt = conn.prepare(
                r#"
                SELECT r.canonical_url, r.resource_type, p.name, p.version, r.file_path, r.metadata, r.fhir_version
                FROM resources r
                JOIN packages p ON r.package_id = p.id
                "#,
            )?;

            let rows: Vec<(String, ResourceIndex)> = stmt
                .query_map([], |row| {
                    let metadata_json: String = row.get(5)?;
                    let metadata: ResourceMetadata =
                        serde_json::from_str(&metadata_json).map_err(|e| {
                            rusqlite::Error::FromSqlConversionFailure(
                                5,
                                Type::Text,
                                Box::new(e),
                            )
                        })?;

                    let canonical_url: String = row.get(0)?;
                    let index = ResourceIndex {
                        canonical_url: canonical_url.clone(),
                        resource_type: row.get(1)?,
                        package_name: row.get(2)?,
                        package_version: row.get(3)?,
                        fhir_version: row.get(6)?,
                        file_path: PathBuf::from(row.get::<_, String>(4)?),
                        metadata,
                    };

                    Ok((canonical_url, index))
                })?
                .collect::<std::result::Result<Vec<_>, _>>()?;

            let mut map = HashMap::new();
            for (url, index) in rows {
                map.insert(url, index);
            }

            Ok(map)
        })
        .await
        .unwrap_or_default()
    }

    pub async fn compact(&self) -> Result<()> {
        let conn = Arc::clone(&self.conn);
        conn.call(|conn: &mut rusqlite::Connection| -> TokioResult<()> {
            conn.execute_batch("VACUUM; ANALYZE;")?;
            Ok(())
        })
        .await?;
        info!("SQLite storage compacted");
        Ok(())
    }

    pub async fn get_stats(&self) -> Result<StorageStats> {
        let conn = Arc::clone(&self.conn);
        let db_path = self.db_path.clone();

        let (package_count, resource_count) = conn
            .call(|conn: &mut rusqlite::Connection| {
                let package_count: i64 =
                    conn.query_row("SELECT COUNT(*) FROM packages", [], |row| row.get(0))?;

                let resource_count: i64 =
                    conn.query_row("SELECT COUNT(*) FROM resources", [], |row| row.get(0))?;

                Ok((package_count, resource_count))
            })
            .await?;

        let file_size = tokio::fs::metadata(&db_path)
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
        let conn = Arc::clone(&self.conn);

        let integrity: String = conn
            .call(|conn: &mut rusqlite::Connection| -> TokioResult<String> {
                Ok(conn.query_row("PRAGMA integrity_check", [], |row| row.get(0))?)
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_config() -> (TempDir, StorageConfig) {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            cache_dir: temp_dir.path().join("cache"),
            packages_dir: temp_dir.path().join("packages"),
            max_cache_size: "1GB".to_string(),
        };
        (temp_dir, config)
    }

    #[tokio::test]
    async fn test_sqlite_storage_creation() {
        let (_dir, config) = test_config();
        let storage = SqliteStorage::new(config).await.unwrap();
        let packages = storage.list_packages().await.unwrap();
        assert_eq!(packages.len(), 0);
    }

    #[tokio::test]
    async fn test_integrity_check() {
        let (_dir, config) = test_config();
        let storage = SqliteStorage::new(config).await.unwrap();
        let report = storage.integrity_check().await.unwrap();
        assert!(report.is_valid);
        assert_eq!(report.errors.len(), 0);
    }

    #[tokio::test]
    async fn test_get_stats() {
        let (_dir, config) = test_config();
        let storage = SqliteStorage::new(config).await.unwrap();
        let stats = storage.get_stats().await.unwrap();
        assert_eq!(stats.package_count, 0);
        assert_eq!(stats.resource_count, 0);
        assert!(stats.storage_size_bytes > 0);
    }
}
