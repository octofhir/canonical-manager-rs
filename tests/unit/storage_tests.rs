//! Unit tests for storage module

use octofhir_canonical_manager::package::{ExtractedPackage, FhirResource, PackageManifest};
use octofhir_canonical_manager::storage::IndexedStorage;
use octofhir_canonical_manager::{StorageConfig, error::Validate};
use serde_json::json;
use std::collections::HashMap;
use std::path::PathBuf;
use tempfile::TempDir;

use crate::common::setup_test_env;

/// Test indexed storage creation
#[tokio::test]
async fn test_indexed_storage_creation() {
    let temp_dir = setup_test_env();
    let config = StorageConfig {
        cache_dir: temp_dir.path().join("cache"),
        index_dir: temp_dir.path().join("index"),
        packages_dir: temp_dir.path().join("packages"),
        max_cache_size: "100MB".to_string(),
    };

    let storage = IndexedStorage::new(config).await.unwrap();

    // Verify empty storage
    let packages = storage.list_packages().await.unwrap();
    assert_eq!(packages.len(), 0);

    let urls = storage.get_all_canonical_urls();
    assert_eq!(urls.len(), 0);
}

/// Test storage configuration validation
#[test]
fn test_storage_config_validation() {
    let temp_dir = TempDir::new().unwrap();
    let config = StorageConfig {
        cache_dir: temp_dir.path().join("cache"),
        index_dir: temp_dir.path().join("index"),
        packages_dir: temp_dir.path().join("packages"),
        max_cache_size: "100MB".to_string(),
    };

    // Valid config should pass
    assert!(config.validate().is_ok());

    // Test invalid cache size
    let mut invalid_config = config.clone();
    invalid_config.max_cache_size = "invalid-size".to_string();
    assert!(invalid_config.validate().is_err());

    // Test empty path
    let mut empty_path_config = config.clone();
    empty_path_config.cache_dir = PathBuf::new();
    assert!(empty_path_config.validate().is_err());
}

/// Test package addition to storage
#[tokio::test]
async fn test_add_package_to_storage() {
    let temp_dir = setup_test_env();
    let config = StorageConfig {
        cache_dir: temp_dir.path().join("cache"),
        index_dir: temp_dir.path().join("index"),
        packages_dir: temp_dir.path().join("packages"),
        max_cache_size: "100MB".to_string(),
    };

    let mut storage = IndexedStorage::new(config).await.unwrap();

    // Create test package
    let extracted_package = create_test_extracted_package(&temp_dir);

    // Add package
    let result = storage.add_package(extracted_package).await;
    assert!(result.is_ok(), "Package addition should succeed");

    // Verify package is stored
    let packages = storage.list_packages().await.unwrap();
    assert_eq!(packages.len(), 1);
    assert_eq!(packages[0].name, "test.package");
    assert_eq!(packages[0].version, "1.0.0");
    assert_eq!(packages[0].resource_count, 2);

    // Verify resources are indexed
    let urls = storage.get_all_canonical_urls();
    assert_eq!(urls.len(), 2);
    assert!(urls.contains(&"http://example.com/StructureDefinition/test-patient".to_string()));
    assert!(urls.contains(&"http://example.com/ValueSet/test-codes".to_string()));
}

/// Test canonical URL lookup
#[tokio::test]
async fn test_canonical_url_lookup() {
    let temp_dir = setup_test_env();
    let config = StorageConfig {
        cache_dir: temp_dir.path().join("cache"),
        index_dir: temp_dir.path().join("index"),
        packages_dir: temp_dir.path().join("packages"),
        max_cache_size: "100MB".to_string(),
    };

    let mut storage = IndexedStorage::new(config).await.unwrap();

    // Test lookup on empty storage
    let result = storage.find_by_canonical("http://example.com/missing");
    assert!(result.is_none());

    // Add package
    let extracted_package = create_test_extracted_package(&temp_dir);
    storage.add_package(extracted_package).await.unwrap();

    // Test successful lookup
    let result = storage.find_by_canonical("http://example.com/StructureDefinition/test-patient");
    assert!(result.is_some());

    let resource_index = result.unwrap();
    assert_eq!(
        resource_index.canonical_url,
        "http://example.com/StructureDefinition/test-patient"
    );
    assert_eq!(resource_index.resource_type, "StructureDefinition");
    assert_eq!(resource_index.package_name, "test.package");
    assert_eq!(resource_index.package_version, "1.0.0");
}

/// Test resource content retrieval
#[tokio::test]
async fn test_get_resource_content() {
    let temp_dir = setup_test_env();
    let config = StorageConfig {
        cache_dir: temp_dir.path().join("cache"),
        index_dir: temp_dir.path().join("index"),
        packages_dir: temp_dir.path().join("packages"),
        max_cache_size: "100MB".to_string(),
    };

    let mut storage = IndexedStorage::new(config).await.unwrap();
    let extracted_package = create_test_extracted_package(&temp_dir);
    storage.add_package(extracted_package).await.unwrap();

    // Get resource index
    let resource_index = storage
        .find_by_canonical("http://example.com/StructureDefinition/test-patient")
        .unwrap();

    // Get resource content
    let resource = storage.get_resource(&resource_index).await.unwrap();

    assert_eq!(resource.resource_type, "StructureDefinition");
    assert_eq!(resource.id, "test-patient");
    assert_eq!(
        resource.url,
        Some("http://example.com/StructureDefinition/test-patient".to_string())
    );
    assert_eq!(resource.content["resourceType"], "StructureDefinition");
    assert_eq!(resource.content["status"], "active");
}

/// Test search by resource type
#[tokio::test]
async fn test_search_by_type() {
    let temp_dir = setup_test_env();
    let config = StorageConfig {
        cache_dir: temp_dir.path().join("cache"),
        index_dir: temp_dir.path().join("index"),
        packages_dir: temp_dir.path().join("packages"),
        max_cache_size: "100MB".to_string(),
    };

    let mut storage = IndexedStorage::new(config).await.unwrap();
    let extracted_package = create_test_extracted_package(&temp_dir);
    storage.add_package(extracted_package).await.unwrap();

    // Search for StructureDefinition resources
    let structure_defs = storage.search_by_type("StructureDefinition");
    assert_eq!(structure_defs.len(), 1);
    assert_eq!(structure_defs[0].resource_type, "StructureDefinition");
    assert_eq!(
        structure_defs[0].canonical_url,
        "http://example.com/StructureDefinition/test-patient"
    );

    // Search for ValueSet resources
    let value_sets = storage.search_by_type("ValueSet");
    assert_eq!(value_sets.len(), 1);
    assert_eq!(value_sets[0].resource_type, "ValueSet");
    assert_eq!(
        value_sets[0].canonical_url,
        "http://example.com/ValueSet/test-codes"
    );

    // Search for non-existent type
    let missing_type = storage.search_by_type("CodeSystem");
    assert_eq!(missing_type.len(), 0);
}

/// Helper function to create a test extracted package
fn create_test_extracted_package(temp_dir: &TempDir) -> ExtractedPackage {
    // Create test resources
    let structure_def = json!({
        "resourceType": "StructureDefinition",
        "id": "test-patient",
        "url": "http://example.com/StructureDefinition/test-patient",
        "name": "TestPatient",
        "status": "active",
        "kind": "resource",
        "abstract": false,
        "type": "Patient",
        "baseDefinition": "http://hl7.org/fhir/StructureDefinition/Patient"
    });

    let value_set = json!({
        "resourceType": "ValueSet",
        "id": "test-codes",
        "url": "http://example.com/ValueSet/test-codes",
        "name": "TestCodes",
        "status": "active"
    });

    // Create resource files
    let struct_file = temp_dir
        .path()
        .join("StructureDefinition-test-patient.json");
    let valueset_file = temp_dir.path().join("ValueSet-test-codes.json");

    std::fs::write(
        &struct_file,
        serde_json::to_string_pretty(&structure_def).unwrap(),
    )
    .unwrap();
    std::fs::write(
        &valueset_file,
        serde_json::to_string_pretty(&value_set).unwrap(),
    )
    .unwrap();

    let resources = vec![
        FhirResource {
            resource_type: "StructureDefinition".to_string(),
            id: "test-patient".to_string(),
            url: Some("http://example.com/StructureDefinition/test-patient".to_string()),
            version: Some("1.0.0".to_string()),
            content: structure_def,
            file_path: struct_file,
        },
        FhirResource {
            resource_type: "ValueSet".to_string(),
            id: "test-codes".to_string(),
            url: Some("http://example.com/ValueSet/test-codes".to_string()),
            version: Some("1.0.0".to_string()),
            content: value_set,
            file_path: valueset_file,
        },
    ];

    let manifest = PackageManifest {
        name: "test.package".to_string(),
        version: "1.0.0".to_string(),
        description: Some("Test package".to_string()),
        fhir_versions: Some(vec!["4.0.1".to_string()]),
        dependencies: HashMap::new(),
        canonical: Some("http://example.com/test-package".to_string()),
        jurisdiction: None,
        package_type: None,
        title: None,
    };

    ExtractedPackage {
        name: "test.package".to_string(),
        version: "1.0.0".to_string(),
        manifest,
        resources,
        extraction_path: temp_dir.path().to_path_buf(),
    }
}
