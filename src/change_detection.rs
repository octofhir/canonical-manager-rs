use crate::error::Result;
use blake3;
use dashmap::DashMap;
use lru::LruCache;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::SystemTime;
use walkdir::WalkDir;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageChecksum {
    pub package_id: String,
    pub version: String,
    pub content_hash: [u8; 32],
    pub structure_hash: [u8; 32],
    pub file_checksums: HashMap<String, [u8; 32]>,
    pub last_modified: SystemTime,
}

#[derive(Debug, Clone)]
pub struct ChangeSet {
    pub added_files: Vec<PathBuf>,
    pub modified_files: Vec<PathBuf>,
    pub removed_files: Vec<PathBuf>,
    pub structure_changed: bool,
    pub timestamp: SystemTime,
}

impl ChangeSet {
    pub fn new_package(package_path: &Path) -> Result<Self> {
        let mut added_files = Vec::new();

        for entry in WalkDir::new(package_path) {
            let entry = entry
                .map_err(|e| crate::error::FcmError::Generic(format!("Walkdir error: {e}")))?;
            if entry.path().extension() == Some(std::ffi::OsStr::new("json")) {
                added_files.push(entry.path().to_path_buf());
            }
        }

        Ok(Self {
            added_files,
            modified_files: Vec::new(),
            removed_files: Vec::new(),
            structure_changed: true,
            timestamp: SystemTime::now(),
        })
    }

    pub fn is_empty(&self) -> bool {
        self.added_files.is_empty()
            && self.modified_files.is_empty()
            && self.removed_files.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct PackageDiff {
    pub changeset: ChangeSet,
    pub similarity_score: f64,
}

pub struct PackageChangeDetector {
    checksum_store: Arc<DashMap<String, PackageChecksum>>,
    diff_cache: Arc<tokio::sync::Mutex<LruCache<(String, String), PackageDiff>>>,
}

impl Default for PackageChangeDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl PackageChangeDetector {
    pub fn new() -> Self {
        Self {
            checksum_store: Arc::new(DashMap::new()),
            diff_cache: Arc::new(tokio::sync::Mutex::new(LruCache::new(
                std::num::NonZeroUsize::new(1000).unwrap(),
            ))),
        }
    }

    pub async fn detect_changes(&self, package_path: &Path) -> Result<ChangeSet> {
        let package_id = extract_package_id(package_path).await?;
        let new_checksum = self.compute_package_checksum(package_path).await?;

        if let Some(old_checksum) = self.checksum_store.get(&package_id) {
            let changes = self.compute_diff(&old_checksum, &new_checksum).await?;

            // Cache the diff for future operations
            let mut cache = self.diff_cache.lock().await;
            cache.put(
                (old_checksum.version.clone(), new_checksum.version.clone()),
                PackageDiff {
                    changeset: changes.clone(),
                    similarity_score: self.compute_similarity(&old_checksum, &new_checksum),
                },
            );

            return Ok(changes);
        }

        // New package - all files are additions
        ChangeSet::new_package(package_path)
    }

    pub async fn compute_package_checksum(&self, package_path: &Path) -> Result<PackageChecksum> {
        let mut file_checksums = HashMap::new();
        let mut hasher = blake3::Hasher::new();

        // Process all JSON files in parallel
        let entries: Vec<_> = WalkDir::new(package_path)
            .into_iter()
            .filter_map(|entry| entry.ok())
            .filter(|e| e.path().extension() == Some(std::ffi::OsStr::new("json")))
            .collect();

        // Process files sequentially to avoid blocking the async runtime with parallel blocking ops
        let mut checksums = Vec::new();
        for entry in entries {
            let path = entry.path().to_owned();
            let (path_clone, hash) = tokio::task::spawn_blocking(move || {
                let content = std::fs::read(&path)?;
                let file_hash = blake3::hash(&content);
                Ok::<_, crate::error::FcmError>((path, file_hash))
            })
            .await??;
            checksums.push((path_clone, hash));
        }

        for (path, hash) in checksums {
            let relative_path = path
                .strip_prefix(package_path)
                .map_err(|e| crate::error::FcmError::Generic(format!("Strip prefix error: {e}")))?;
            file_checksums.insert(relative_path.to_string_lossy().into_owned(), hash.into());
            hasher.update(hash.as_bytes());
        }

        Ok(PackageChecksum {
            package_id: extract_package_id(package_path).await?,
            version: extract_version(package_path).await?,
            content_hash: hasher.finalize().into(),
            structure_hash: self.compute_structure_hash(&file_checksums),
            file_checksums,
            last_modified: SystemTime::now(),
        })
    }

    pub async fn store_checksum(&self, checksum: PackageChecksum) -> Result<()> {
        self.checksum_store
            .insert(checksum.package_id.clone(), checksum);
        Ok(())
    }

    async fn compute_diff(
        &self,
        old: &PackageChecksum,
        new: &PackageChecksum,
    ) -> Result<ChangeSet> {
        let mut added_files = Vec::new();
        let mut modified_files = Vec::new();
        let mut removed_files = Vec::new();

        // Find added and modified files
        for (path, new_hash) in &new.file_checksums {
            match old.file_checksums.get(path) {
                Some(old_hash) if old_hash != new_hash => {
                    modified_files.push(PathBuf::from(path));
                }
                None => {
                    added_files.push(PathBuf::from(path));
                }
                _ => {} // File unchanged
            }
        }

        // Find removed files
        for path in old.file_checksums.keys() {
            if !new.file_checksums.contains_key(path) {
                removed_files.push(PathBuf::from(path));
            }
        }

        let structure_changed = old.structure_hash != new.structure_hash;

        Ok(ChangeSet {
            added_files,
            modified_files,
            removed_files,
            structure_changed,
            timestamp: SystemTime::now(),
        })
    }

    fn compute_structure_hash(&self, file_checksums: &HashMap<String, [u8; 32]>) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        let mut sorted_paths: Vec<_> = file_checksums.keys().collect();
        sorted_paths.sort();

        for path in sorted_paths {
            hasher.update(path.as_bytes());
        }

        hasher.finalize().into()
    }

    fn compute_similarity(&self, old: &PackageChecksum, new: &PackageChecksum) -> f64 {
        let old_files: std::collections::HashSet<_> = old.file_checksums.keys().collect();
        let new_files: std::collections::HashSet<_> = new.file_checksums.keys().collect();

        let intersection = old_files.intersection(&new_files).count();
        let union = old_files.union(&new_files).count();

        if union == 0 {
            1.0
        } else {
            intersection as f64 / union as f64
        }
    }
}

async fn extract_package_id(package_path: &Path) -> Result<String> {
    // Try to read package.json for package ID
    let package_json_path = package_path.join("package.json");
    if package_json_path.exists() {
        let package_json_path_clone = package_json_path.clone();
        let content =
            tokio::task::spawn_blocking(move || std::fs::read_to_string(&package_json_path_clone))
                .await??;
        let json: serde_json::Value = serde_json::from_str(&content)?;
        if let Some(name) = json.get("name").and_then(|n| n.as_str()) {
            return Ok(name.to_string());
        }
    }

    // Fallback to directory name
    package_path
        .file_name()
        .and_then(|n| n.to_str())
        .map(|s| s.to_string())
        .ok_or_else(|| crate::error::FcmError::Generic("Unable to extract package ID".to_string()))
}

async fn extract_version(package_path: &Path) -> Result<String> {
    // Try to read package.json for version
    let package_json_path = package_path.join("package.json");
    if package_json_path.exists() {
        let package_json_path_clone = package_json_path.clone();
        let content =
            tokio::task::spawn_blocking(move || std::fs::read_to_string(&package_json_path_clone))
                .await??;
        let json: serde_json::Value = serde_json::from_str(&content)?;
        if let Some(version) = json.get("version").and_then(|v| v.as_str()) {
            return Ok(version.to_string());
        }
    }

    // Fallback to "unknown"
    Ok("unknown".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_package_checksum_computation() {
        let temp_dir = TempDir::new().unwrap();
        let package_path = temp_dir.path().join("test-package");
        fs::create_dir_all(&package_path).unwrap();

        // Create test files
        fs::write(
            package_path.join("package.json"),
            r#"{"name": "test-package", "version": "1.0.0"}"#,
        )
        .unwrap();

        fs::write(
            package_path.join("resource1.json"),
            r#"{"resourceType": "Patient", "id": "123"}"#,
        )
        .unwrap();

        let detector = PackageChangeDetector::new();
        let checksum = detector
            .compute_package_checksum(&package_path)
            .await
            .unwrap();

        assert_eq!(checksum.package_id, "test-package");
        assert_eq!(checksum.version, "1.0.0");
        assert_eq!(checksum.file_checksums.len(), 2);
    }

    #[tokio::test]
    async fn test_change_detection() {
        let temp_dir = TempDir::new().unwrap();
        let package_path = temp_dir.path().join("test-package");
        fs::create_dir_all(&package_path).unwrap();

        // Create initial package
        fs::write(
            package_path.join("package.json"),
            r#"{"name": "test-package", "version": "1.0.0"}"#,
        )
        .unwrap();

        fs::write(
            package_path.join("resource1.json"),
            r#"{"resourceType": "Patient", "id": "123"}"#,
        )
        .unwrap();

        let detector = PackageChangeDetector::new();

        // First detection - should be treated as new package
        let changes1 = detector.detect_changes(&package_path).await.unwrap();
        assert_eq!(changes1.added_files.len(), 2);
        assert!(changes1.structure_changed);

        // Store the checksum
        let checksum = detector
            .compute_package_checksum(&package_path)
            .await
            .unwrap();
        detector.store_checksum(checksum).await.unwrap();

        // Second detection without changes - should be empty
        let changes2 = detector.detect_changes(&package_path).await.unwrap();
        assert!(changes2.is_empty());

        // Modify a file
        fs::write(
            package_path.join("resource1.json"),
            r#"{"resourceType": "Patient", "id": "456"}"#,
        )
        .unwrap();

        // Third detection - should detect modification
        let changes3 = detector.detect_changes(&package_path).await.unwrap();
        assert_eq!(changes3.modified_files.len(), 1);
        assert!(!changes3.structure_changed);
    }
}
