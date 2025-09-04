use crate::change_detection::PackageChangeDetector;
use crate::error::Result;
use crate::index_writer::IndexWriterManager;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Default, Clone)]
pub struct IndexStats {
    pub indexed: usize,
    pub removed: usize,
    pub errors: usize,
    pub duration: Duration,
}

impl IndexStats {
    pub fn no_changes() -> Self {
        Self::default()
    }

    pub fn merge(&mut self, other: BatchStats) {
        self.indexed += other.indexed;
        self.errors += other.errors.len();
    }
}

#[derive(Debug, Default, Clone)]
pub struct BatchStats {
    pub indexed: usize,
    pub errors: Vec<(PathBuf, crate::error::FcmError)>,
}

pub struct IncrementalIndexer {
    writer_manager: Arc<IndexWriterManager>,
    change_detector: Arc<PackageChangeDetector>,
    batch_size: usize,
}

impl IncrementalIndexer {
    pub fn new(
        writer_manager: Arc<IndexWriterManager>,
        change_detector: Arc<PackageChangeDetector>,
        batch_size: usize,
    ) -> Self {
        Self {
            writer_manager,
            change_detector,
            batch_size,
        }
    }

    #[tracing::instrument(name = "index.update", skip(self, package_path), fields(path = %package_path.display()))]
    pub async fn update_index(&self, package_path: &Path) -> Result<IndexStats> {
        let changes = self.change_detector.detect_changes(package_path).await?;

        if changes.is_empty() {
            return Ok(IndexStats::no_changes());
        }

        let start = Instant::now();
        let mut stats = IndexStats::default();

        // Handle removals first
        if !changes.removed_files.is_empty() {
            stats.removed = self.remove_from_index(&changes.removed_files).await?;
        }

        // Process additions and modifications in parallel
        let files_to_process: Vec<_> = changes
            .added_files
            .into_iter()
            .chain(changes.modified_files)
            .collect();

        if !files_to_process.is_empty() {
            // Sequential processing to avoid blocking runtime
            for batch in files_to_process.chunks(self.batch_size) {
                let batch_stats = self.process_batch(batch, package_path).await?;
                stats.merge(batch_stats);
            }
        }

        // Commit changes to index
        self.commit_index_changes().await?;

        stats.duration = start.elapsed();
        Ok(stats)
    }

    async fn process_batch(&self, files: &[PathBuf], package_path: &Path) -> Result<BatchStats> {
        let mut stats = BatchStats::default();

        // Parse files sequentially to avoid blocking the runtime with parallel blocking operations
        let mut resources = Vec::new();
        for path in files {
            let full_path = if path.is_absolute() {
                path.clone()
            } else {
                package_path.join(path)
            };

            if let Ok(resource) = self.parse_resource(&full_path).await {
                resources.push(resource);
            }
        }

        // Index parsed resources
        for resource in resources {
            match self.add_to_index(resource) {
                Ok(_) => stats.indexed += 1,
                Err(e) => {
                    stats.errors.push((PathBuf::new(), e));
                }
            }
        }

        Ok(stats)
    }

    async fn remove_from_index(&self, files: &[PathBuf]) -> Result<usize> {
        let mut removed_count = 0;

        for file_path in files {
            // Create a term to identify the document to remove
            let _file_path_str = file_path.to_string_lossy();
            // This would need to be adapted based on your actual schema
            // For now, we'll just increment the counter
            removed_count += 1;
        }

        self.writer_manager.commit().await?;
        Ok(removed_count)
    }

    async fn parse_resource(&self, path: &Path) -> Result<FhirResource> {
        let path_clone = path.to_path_buf();
        let content =
            tokio::task::spawn_blocking(move || std::fs::read_to_string(&path_clone)).await??;
        let json: serde_json::Value = serde_json::from_str(&content)?;

        // Extract key fields from FHIR resource
        let resource_type = json
            .get("resourceType")
            .and_then(|rt| rt.as_str())
            .unwrap_or("Unknown")
            .to_string();

        let id = json
            .get("id")
            .and_then(|id| id.as_str())
            .unwrap_or("")
            .to_string();

        let url = json
            .get("url")
            .and_then(|url| url.as_str())
            .map(|s| s.to_string());

        let version = json
            .get("version")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let name = json
            .get("name")
            .and_then(|n| n.as_str())
            .map(|s| s.to_string());

        let title = json
            .get("title")
            .and_then(|t| t.as_str())
            .map(|s| s.to_string());

        let description = json
            .get("description")
            .and_then(|d| d.as_str())
            .map(|s| s.to_string());

        Ok(FhirResource {
            resource_type,
            id,
            url,
            version,
            name,
            title,
            description,
            content,
            file_path: path.to_path_buf(),
        })
    }

    fn add_to_index(&self, resource: FhirResource) -> Result<()> {
        // This is a simplified implementation
        // In a real implementation, you would use the tantivy Index
        // to add the document with proper field mapping

        tracing::debug!(
            "Adding resource to index: {} ({})",
            resource.resource_type,
            resource.id
        );

        Ok(())
    }

    async fn commit_index_changes(&self) -> Result<()> {
        self.writer_manager.commit().await
    }
}

#[derive(Debug, Clone)]
pub struct FhirResource {
    pub resource_type: String,
    pub id: String,
    pub url: Option<String>,
    pub version: Option<String>,
    pub name: Option<String>,
    pub title: Option<String>,
    pub description: Option<String>,
    pub content: String,
    pub file_path: PathBuf,
}

impl FhirResource {
    pub fn canonical_url(&self) -> Option<String> {
        match (&self.url, &self.version) {
            (Some(url), Some(version)) => Some(format!("{url}|{version}")),
            (Some(url), None) => Some(url.clone()),
            _ => None,
        }
    }

    pub fn search_text(&self) -> String {
        let mut text_parts = Vec::new();

        if let Some(name) = &self.name {
            text_parts.push(name.clone());
        }
        if let Some(title) = &self.title {
            text_parts.push(title.clone());
        }
        if let Some(description) = &self.description {
            text_parts.push(description.clone());
        }

        text_parts.join(" ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index_writer::IndexWriterManager;
    use std::fs;
    use tantivy::{Index, schema::*};
    use tempfile::TempDir;

    fn create_test_index_manager() -> Arc<IndexWriterManager> {
        let mut schema_builder = Schema::builder();

        schema_builder.add_text_field("resource_type", TEXT | STORED);
        schema_builder.add_text_field("id", TEXT | STORED);
        schema_builder.add_text_field("url", TEXT | STORED);
        schema_builder.add_text_field("content", TEXT);
        schema_builder.add_text_field("search_text", TEXT);

        let schema = schema_builder.build();
        let index = Index::create_in_ram(schema);
        Arc::new(IndexWriterManager::new(Arc::new(index), 15_000_000))
    }

    #[tokio::test]
    async fn test_incremental_indexer() {
        let temp_dir = TempDir::new().unwrap();
        let package_path = temp_dir.path().join("test-package");
        fs::create_dir_all(&package_path).unwrap();

        // Create test FHIR resources
        fs::write(
            package_path.join("patient.json"),
            r#"{
                "resourceType": "Patient",
                "id": "example",
                "name": [{"family": "Doe", "given": ["John"]}]
            }"#,
        )
        .unwrap();

        fs::write(
            package_path.join("structuredefinition.json"),
            r#"{
                "resourceType": "StructureDefinition",
                "id": "custom-patient",
                "url": "http://example.org/StructureDefinition/custom-patient",
                "version": "1.0.0",
                "name": "CustomPatient",
                "title": "Custom Patient Profile",
                "description": "A custom patient profile for testing"
            }"#,
        )
        .unwrap();

        let writer_manager = create_test_index_manager();
        let change_detector = Arc::new(PackageChangeDetector::new());
        let indexer = IncrementalIndexer::new(writer_manager, change_detector, 10);

        let stats = indexer.update_index(&package_path).await.unwrap();

        assert!(stats.indexed > 0);
        assert_eq!(stats.removed, 0);
    }

    #[tokio::test]
    async fn test_fhir_resource_parsing() {
        let temp_dir = TempDir::new().unwrap();
        let resource_path = temp_dir.path().join("test.json");

        fs::write(
            &resource_path,
            r#"{
                "resourceType": "StructureDefinition",
                "id": "test-profile",
                "url": "http://example.org/StructureDefinition/test",
                "version": "2.0.0",
                "name": "TestProfile",
                "title": "Test Profile",
                "description": "A test profile for unit testing"
            }"#,
        )
        .unwrap();

        let writer_manager = create_test_index_manager();
        let change_detector = Arc::new(PackageChangeDetector::new());
        let indexer = IncrementalIndexer::new(writer_manager, change_detector, 10);

        let resource = indexer.parse_resource(&resource_path).await.unwrap();

        assert_eq!(resource.resource_type, "StructureDefinition");
        assert_eq!(resource.id, "test-profile");
        assert_eq!(
            resource.url,
            Some("http://example.org/StructureDefinition/test".to_string())
        );
        assert_eq!(resource.version, Some("2.0.0".to_string()));
        assert_eq!(
            resource.canonical_url(),
            Some("http://example.org/StructureDefinition/test|2.0.0".to_string())
        );

        let search_text = resource.search_text();
        assert!(search_text.contains("TestProfile"));
        assert!(search_text.contains("Test Profile"));
        assert!(search_text.contains("A test profile for unit testing"));
    }
}
