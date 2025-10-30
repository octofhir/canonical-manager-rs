//! Tests for local package and resource directory loading functionality

use octofhir_canonical_manager::{CanonicalManager, FcmConfig};
use std::path::PathBuf;
use tempfile::TempDir;
use tokio::fs;

/// Helper to create a test package directory with package.json and resources
async fn create_test_package_dir(base_dir: &std::path::Path, name: &str, version: &str) -> PathBuf {
    let package_dir = base_dir.join(format!("{}-{}", name, version));
    fs::create_dir_all(&package_dir).await.unwrap();

    // Create package.json
    let manifest = serde_json::json!({
        "name": name,
        "version": version,
        "fhirVersions": ["4.0.1"],
        "dependencies": {},
        "canonical": format!("http://example.org/{}", name),
        "type": "fhir.ig",
        "title": format!("Test Package {}", name),
        "description": "Test package for local loading"
    });

    fs::write(
        package_dir.join("package.json"),
        serde_json::to_string_pretty(&manifest).unwrap(),
    )
    .await
    .unwrap();

    // Create sample Patient resource
    let patient = serde_json::json!({
        "resourceType": "Patient",
        "id": "example-patient",
        "url": format!("http://example.org/{}/Patient/example", name),
        "name": [{
            "family": "Doe",
            "given": ["John"]
        }]
    });

    fs::write(
        package_dir.join("Patient-example.json"),
        serde_json::to_string_pretty(&patient).unwrap(),
    )
    .await
    .unwrap();

    // Create sample StructureDefinition resource
    let structure_def = serde_json::json!({
        "resourceType": "StructureDefinition",
        "id": "custom-patient",
        "url": format!("http://example.org/{}/StructureDefinition/custom-patient", name),
        "name": "CustomPatient",
        "status": "draft",
        "kind": "resource",
        "abstract": false,
        "type": "Patient"
    });

    fs::write(
        package_dir.join("StructureDefinition-custom-patient.json"),
        serde_json::to_string_pretty(&structure_def).unwrap(),
    )
    .await
    .unwrap();

    package_dir
}

/// Helper to create a resource-only directory without package.json
async fn create_resource_dir(base_dir: &std::path::Path, namespace: &str) -> PathBuf {
    let resource_dir = base_dir.join(namespace);
    fs::create_dir_all(&resource_dir).await.unwrap();

    // Create sample Observation resource
    let observation = serde_json::json!({
        "resourceType": "Observation",
        "id": "vital-signs",
        "status": "final",
        "code": {
            "coding": [{
                "system": "http://loinc.org",
                "code": "85354-9"
            }]
        }
    });

    fs::write(
        resource_dir.join("Observation-vital-signs.json"),
        serde_json::to_string_pretty(&observation).unwrap(),
    )
    .await
    .unwrap();

    // Create sample Condition resource
    let condition = serde_json::json!({
        "resourceType": "Condition",
        "id": "diabetes",
        "code": {
            "coding": [{
                "system": "http://snomed.info/sct",
                "code": "44054006"
            }]
        }
    });

    fs::write(
        resource_dir.join("Condition-diabetes.json"),
        serde_json::to_string_pretty(&condition).unwrap(),
    )
    .await
    .unwrap();

    resource_dir
}

#[tokio::test]
async fn test_load_from_directory_basic() {
    let temp_dir = TempDir::new().unwrap();
    let config = FcmConfig::test_config(temp_dir.path());

    let manager = CanonicalManager::new(config).await.unwrap();

    // Create test package directory
    let package_dir = create_test_package_dir(temp_dir.path(), "test.package", "1.0.0").await;

    // Load package from directory
    let package_info = manager
        .load_from_directory(&package_dir, None)
        .await
        .unwrap();

    assert_eq!(package_info.name, "test.package");
    assert_eq!(package_info.version, "1.0.0");
    assert_eq!(package_info.resource_count, 2); // Patient + StructureDefinition
}

#[tokio::test]
async fn test_load_from_directory_with_package_id() {
    let temp_dir = TempDir::new().unwrap();
    let config = FcmConfig::test_config(temp_dir.path());

    let manager = CanonicalManager::new(config).await.unwrap();

    // Create test package directory
    let package_dir = create_test_package_dir(temp_dir.path(), "test.package", "2.0.0").await;

    // Load package with explicit package_id
    let package_info = manager
        .load_from_directory(&package_dir, Some("test.package@2.0.0"))
        .await
        .unwrap();

    assert_eq!(package_info.name, "test.package");
    assert_eq!(package_info.version, "2.0.0");
}

#[tokio::test]
async fn test_load_from_directory_missing_manifest() {
    let temp_dir = TempDir::new().unwrap();
    let config = FcmConfig::test_config(temp_dir.path());

    let manager = CanonicalManager::new(config).await.unwrap();

    // Create directory without package.json
    let empty_dir = temp_dir.path().join("empty");
    fs::create_dir_all(&empty_dir).await.unwrap();

    // Should fail with package.json not found error
    let result = manager.load_from_directory(&empty_dir, None).await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("package.json not found")
    );
}

#[tokio::test]
async fn test_load_from_directory_nonexistent() {
    let temp_dir = TempDir::new().unwrap();
    let config = FcmConfig::test_config(temp_dir.path());

    let manager = CanonicalManager::new(config).await.unwrap();

    let nonexistent = temp_dir.path().join("does-not-exist");

    // Should fail with directory not found error
    let result = manager.load_from_directory(&nonexistent, None).await;
    assert!(result.is_err());
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("Directory not found")
    );
}

#[tokio::test]
async fn test_load_resources_from_directory_basic() {
    let temp_dir = TempDir::new().unwrap();
    let config = FcmConfig::test_config(temp_dir.path());

    let manager = CanonicalManager::new(config).await.unwrap();

    // Create resource directory
    let resource_dir = create_resource_dir(temp_dir.path(), "test-resources").await;

    // Load resources
    let count = manager
        .load_resources_from_directory(&resource_dir, "test")
        .await
        .unwrap();

    assert_eq!(count, 2); // Observation + Condition
}

#[tokio::test]
async fn test_load_resources_from_directory_empty() {
    let temp_dir = TempDir::new().unwrap();
    let config = FcmConfig::test_config(temp_dir.path());

    let manager = CanonicalManager::new(config).await.unwrap();

    // Create empty directory
    let empty_dir = temp_dir.path().join("empty-resources");
    fs::create_dir_all(&empty_dir).await.unwrap();

    // Should succeed but return 0
    let count = manager
        .load_resources_from_directory(&empty_dir, "empty")
        .await
        .unwrap();

    assert_eq!(count, 0);
}

#[tokio::test]
async fn test_load_resources_from_directory_invalid_json() {
    let temp_dir = TempDir::new().unwrap();
    let config = FcmConfig::test_config(temp_dir.path());

    let manager = CanonicalManager::new(config).await.unwrap();

    // Create directory with invalid JSON
    let invalid_dir = temp_dir.path().join("invalid-resources");
    fs::create_dir_all(&invalid_dir).await.unwrap();

    // Write invalid JSON
    fs::write(
        invalid_dir.join("invalid.json"),
        "{ this is not valid json }",
    )
    .await
    .unwrap();

    // Write valid resource
    let observation = serde_json::json!({
        "resourceType": "Observation",
        "id": "valid",
        "status": "final"
    });
    fs::write(
        invalid_dir.join("valid.json"),
        serde_json::to_string_pretty(&observation).unwrap(),
    )
    .await
    .unwrap();

    // Should skip invalid and load only valid
    let count = manager
        .load_resources_from_directory(&invalid_dir, "mixed")
        .await
        .unwrap();

    assert_eq!(count, 1); // Only the valid resource
}

#[tokio::test]
async fn test_load_resources_from_directory_missing_required_fields() {
    let temp_dir = TempDir::new().unwrap();
    let config = FcmConfig::test_config(temp_dir.path());

    let manager = CanonicalManager::new(config).await.unwrap();

    // Create directory with resources missing required fields
    let resource_dir = temp_dir.path().join("incomplete-resources");
    fs::create_dir_all(&resource_dir).await.unwrap();

    // Resource missing resourceType
    let no_type = serde_json::json!({
        "id": "no-type",
        "status": "final"
    });
    fs::write(
        resource_dir.join("no-type.json"),
        serde_json::to_string_pretty(&no_type).unwrap(),
    )
    .await
    .unwrap();

    // Resource missing id
    let no_id = serde_json::json!({
        "resourceType": "Observation",
        "status": "final"
    });
    fs::write(
        resource_dir.join("no-id.json"),
        serde_json::to_string_pretty(&no_id).unwrap(),
    )
    .await
    .unwrap();

    // Valid resource
    let valid = serde_json::json!({
        "resourceType": "Observation",
        "id": "complete",
        "status": "final"
    });
    fs::write(
        resource_dir.join("valid.json"),
        serde_json::to_string_pretty(&valid).unwrap(),
    )
    .await
    .unwrap();

    // Should skip incomplete and load only valid
    let count = manager
        .load_resources_from_directory(&resource_dir, "incomplete")
        .await
        .unwrap();

    assert_eq!(count, 1); // Only the complete resource
}

#[tokio::test]
async fn test_load_multiple_packages_from_directories() {
    let temp_dir = TempDir::new().unwrap();
    let config = FcmConfig::test_config(temp_dir.path());

    let manager = CanonicalManager::new(config).await.unwrap();

    // Create multiple package directories
    let package1 = create_test_package_dir(temp_dir.path(), "package.one", "1.0.0").await;
    let package2 = create_test_package_dir(temp_dir.path(), "package.two", "2.0.0").await;

    // Load both packages
    let info1 = manager.load_from_directory(&package1, None).await.unwrap();
    let info2 = manager.load_from_directory(&package2, None).await.unwrap();

    assert_eq!(info1.name, "package.one");
    assert_eq!(info2.name, "package.two");

    // Verify both are in the manager's package list
    let packages = manager.list_packages().await.unwrap();
    assert!(packages.iter().any(|p| p.contains("package.one@1.0.0")));
    assert!(packages.iter().any(|p| p.contains("package.two@2.0.0")));
}
