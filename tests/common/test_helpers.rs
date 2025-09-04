//! Test helper functions and utilities

use crate::common::{MockPackageData, MockRegistry};
use octofhir_canonical_manager::{FcmConfig, OptimizationConfig, RegistryConfig, StorageConfig};
use std::path::Path;
use tempfile::TempDir;

/// Create a test configuration for testing
pub fn create_test_config(temp_dir: &Path) -> FcmConfig {
    FcmConfig {
        registry: RegistryConfig {
            url: "http://localhost:8080/".to_string(),
            timeout: 30,
            retry_attempts: 3,
        },
        packages: vec![],
        storage: StorageConfig {
            cache_dir: temp_dir.join("cache"),
            index_dir: temp_dir.join("index"),
            packages_dir: temp_dir.join("packages"),
            max_cache_size: "100MB".to_string(),
        },
        optimization: OptimizationConfig::default(),
    }
}

/// Setup test directories and return temporary directory
pub fn setup_test_env() -> TempDir {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");

    // Create required subdirectories
    std::fs::create_dir_all(temp_dir.path().join("cache")).unwrap();
    std::fs::create_dir_all(temp_dir.path().join("index")).unwrap();
    std::fs::create_dir_all(temp_dir.path().join("packages")).unwrap();

    temp_dir
}

/// Returns true if network-bound tests should be skipped.
///
/// Behavior:
/// - Opt-in to run with `FCM_RUN_NET_TESTS` set to a truthy value ("1", "true", "yes").
/// - Explicitly skip if `FCM_SKIP_NET_TESTS` is set to a truthy value.
/// - In CI (when `CI` is set), skip unless explicitly opted-in.
/// - Otherwise, attempt a localhost bind to detect sandbox restrictions; skip on failure.
pub fn should_skip_net() -> bool {
    let is_truthy = |v: String| -> bool {
        matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on")
    };

    // Explicit opt-in to run network tests
    if std::env::var("FCM_RUN_NET_TESTS")
        .map(is_truthy)
        .unwrap_or(false)
    {
        return false;
    }

    // Explicit skip takes precedence if truthy
    if std::env::var("FCM_SKIP_NET_TESTS")
        .map(is_truthy)
        .unwrap_or(false)
    {
        return true;
    }

    // In CI environments, default to skipping unless explicitly opted-in
    if std::env::var("CI").is_ok() {
        return true;
    }

    // Try a harmless local bind to detect sandbox restrictions
    match std::net::TcpListener::bind("127.0.0.1:0") {
        Ok(_sock) => false,
        Err(_e) => true,
    }
}

/// Assert that a path exists
pub fn assert_path_exists(path: &Path) {
    assert!(path.exists(), "Path should exist: {}", path.display());
}

/// Create a minimal FHIR package structure for testing
pub fn create_test_package_structure(
    package_dir: &Path,
    package_name: &str,
    version: &str,
) -> std::io::Result<()> {
    let package_path = package_dir.join(format!("{package_name}-{version}"));
    std::fs::create_dir_all(&package_path)?;

    // Create package.json
    let package_json = serde_json::json!({
        "name": package_name,
        "version": version,
        "description": "Test package",
        "fhirVersions": ["4.0.1"],
        "dependencies": {},
        "canonical": format!("http://example.com/{}", package_name)
    });

    std::fs::write(
        package_path.join("package.json"),
        serde_json::to_string_pretty(&package_json)?,
    )?;

    // Create package directory structure
    std::fs::create_dir_all(package_path.join("package"))?;

    // Create a test StructureDefinition resource
    let test_resource = serde_json::json!({
        "resourceType": "StructureDefinition",
        "id": "test-structure",
        "url": format!("http://example.com/{}/StructureDefinition/test-structure", package_name),
        "name": "TestStructure",
        "status": "active",
        "kind": "resource",
        "abstract": false,
        "type": "Patient",
        "baseDefinition": "http://hl7.org/fhir/StructureDefinition/Patient"
    });

    std::fs::write(
        package_path
            .join("package")
            .join("StructureDefinition-test-structure.json"),
        serde_json::to_string_pretty(&test_resource)?,
    )?;

    Ok(())
}

/// Create a test registry with common packages for testing
pub async fn create_test_registry_with_packages() -> MockRegistry {
    let mut registry = MockRegistry::new().await;

    // Add standard test packages
    let test_package =
        MockPackageData::new("test.package", "1.0.0").with_description("Test FHIR package");
    registry.add_package(test_package);

    let r4_core =
        MockPackageData::new("hl7.fhir.r4.core", "4.0.1").with_description("FHIR R4 Core Package");
    registry.add_package(r4_core);

    let us_core = MockPackageData::new("hl7.fhir.us.core", "6.1.0")
        .with_description("US Core Implementation Guide")
        .with_dependencies({
            let mut deps = std::collections::HashMap::new();
            deps.insert("hl7.fhir.r4.core".to_string(), "4.0.1".to_string());
            deps
        });
    registry.add_package(us_core);

    registry.setup_mocks().await;
    registry
}

/// Wait for async operations to complete (for test compatibility)
#[allow(dead_code)]
pub async fn wait_for_async() {
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_test_config() {
        let temp_dir = setup_test_env();
        let config = create_test_config(temp_dir.path());

        assert_eq!(config.registry.url, "http://localhost:8080/");
        assert_eq!(config.registry.timeout, 30);
        assert_eq!(config.registry.retry_attempts, 3);
        assert!(config.packages.is_empty());
        assert_eq!(config.storage.max_cache_size, "100MB");
    }

    #[test]
    fn test_setup_test_env() {
        let temp_dir = setup_test_env();

        assert_path_exists(&temp_dir.path().join("cache"));
        assert_path_exists(&temp_dir.path().join("index"));
        assert_path_exists(&temp_dir.path().join("packages"));
    }

    #[test]
    fn test_create_test_package_structure() {
        let temp_dir = setup_test_env();
        let result = create_test_package_structure(temp_dir.path(), "test.package", "1.0.0");

        assert!(result.is_ok());
        assert_path_exists(&temp_dir.path().join("test.package-1.0.0"));
        assert_path_exists(&temp_dir.path().join("test.package-1.0.0/package.json"));
        assert_path_exists(&temp_dir.path().join("test.package-1.0.0/package"));
    }
}
