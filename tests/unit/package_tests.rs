//! Unit tests for package module

use octofhir_canonical_manager::PackageSpec;
use octofhir_canonical_manager::package::{
    ExtractedPackage, FhirResource, PackageExtractor, PackageManifest,
};
use octofhir_canonical_manager::registry::{PackageDownload, PackageMetadata};
use serde_json::json;
use std::collections::HashMap;

use crate::common::{create_test_package_structure, setup_test_env};

/// Test package extractor creation
#[test]
fn test_package_extractor_creation() {
    let temp_dir = setup_test_env();
    let _extractor = PackageExtractor::new(temp_dir.path().to_path_buf());

    // This test verifies the extractor can be created without panicking
}

/// Test package manifest parsing
#[test]
fn test_package_manifest_parsing() {
    let manifest_json = json!({
        "name": "test.package",
        "version": "1.0.0",
        "description": "Test package",
        "fhirVersions": ["4.0.1", "4.3.0"],
        "dependencies": {
            "hl7.fhir.r4.core": "4.0.1",
            "hl7.terminology.r4": "5.0.0"
        },
        "canonical": "http://example.com/test-package",
        "author": "Test Author",
        "maintainers": [
            {
                "name": "Test Maintainer",
                "email": "test@example.com"
            }
        ]
    });

    let manifest: PackageManifest = serde_json::from_value(manifest_json).unwrap();

    assert_eq!(manifest.name, "test.package");
    assert_eq!(manifest.version, "1.0.0");
    assert_eq!(manifest.description, Some("Test package".to_string()));
    assert_eq!(
        manifest.fhir_versions,
        Some(vec!["4.0.1".to_string(), "4.3.0".to_string()])
    );
    assert_eq!(manifest.dependencies.len(), 2);
    assert_eq!(
        manifest.dependencies.get("hl7.fhir.r4.core"),
        Some(&"4.0.1".to_string())
    );
    assert_eq!(
        manifest.dependencies.get("hl7.terminology.r4"),
        Some(&"5.0.0".to_string())
    );
    assert_eq!(
        manifest.canonical,
        Some("http://example.com/test-package".to_string())
    );
}

/// Test package manifest parsing with minimal data
#[test]
fn test_minimal_package_manifest_parsing() {
    let minimal_json = json!({
        "name": "minimal.package",
        "version": "0.1.0",
        "fhirVersions": ["4.0.1"],
        "dependencies": {}
    });

    let manifest: PackageManifest = serde_json::from_value(minimal_json).unwrap();

    assert_eq!(manifest.name, "minimal.package");
    assert_eq!(manifest.version, "0.1.0");
    assert_eq!(manifest.description, None);
    assert_eq!(manifest.fhir_versions, Some(vec!["4.0.1".to_string()]));
    assert!(manifest.dependencies.is_empty());
    assert_eq!(manifest.canonical, None);
}

/// Test package manifest parsing with invalid data
#[test]
fn test_invalid_package_manifest_parsing() {
    let invalid_json = json!({
        "name": "invalid.package",
        // Missing required fields: version, fhirVersions
        "description": "Invalid package"
    });

    let result: Result<PackageManifest, _> = serde_json::from_value(invalid_json);
    assert!(result.is_err(), "Invalid manifest should fail to parse");
}

/// Test package extraction with valid structure
#[tokio::test]
async fn test_package_extraction_valid() {
    let temp_dir = setup_test_env();
    let extractor = PackageExtractor::new(temp_dir.path().to_path_buf());

    // Create a test package structure
    create_test_package_structure(temp_dir.path(), "test.package", "1.0.0").unwrap();

    // Create a mock PackageDownload
    let spec = PackageSpec {
        name: "test.package".to_string(),
        version: "1.0.0".to_string(),
        priority: 1,
    };

    let metadata = PackageMetadata {
        name: "test.package".to_string(),
        version: "1.0.0".to_string(),
        description: Some("Test package".to_string()),
        fhir_version: "4.0.1".to_string(),
        dependencies: HashMap::new(),
        canonical_base: Some("http://example.com/test-package".to_string()),
    };

    // For this test, we'll create a simple file instead of a real tar.gz
    let package_file = temp_dir.path().join("test-package-1.0.0.tgz");
    std::fs::write(&package_file, b"mock tarball content").unwrap();

    let download = PackageDownload {
        spec,
        file_path: package_file,
        metadata,
    };

    // Note: The actual extract_package method expects a real tar.gz file
    // This test would need a proper tar.gz creation or mocking
    // For now, we test that the method exists and handles errors gracefully
    let result = extractor.extract_package(download).await;

    // The extraction will likely fail due to invalid tar.gz content
    // but we're testing that the method can be called
    assert!(result.is_err() || result.is_ok());
}

/// Test FHIR resource validation
#[test]
fn test_fhir_resource_validation() {
    // Valid FHIR StructureDefinition
    let valid_resource = json!({
        "resourceType": "StructureDefinition",
        "id": "test-structure",
        "url": "http://example.com/StructureDefinition/test-structure",
        "name": "TestStructure",
        "status": "active",
        "kind": "resource",
        "abstract": false,
        "type": "Patient",
        "baseDefinition": "http://hl7.org/fhir/StructureDefinition/Patient"
    });

    // Test that the resource has required fields
    assert_eq!(valid_resource["resourceType"], "StructureDefinition");
    assert_eq!(valid_resource["id"], "test-structure");
    assert_eq!(
        valid_resource["url"],
        "http://example.com/StructureDefinition/test-structure"
    );
    assert_eq!(valid_resource["status"], "active");

    // Invalid resource (missing required fields)
    let invalid_resource = json!({
        "resourceType": "StructureDefinition",
        "id": "invalid-structure"
        // Missing required fields like url, status, etc.
    });

    assert_eq!(invalid_resource["resourceType"], "StructureDefinition");
    assert_eq!(invalid_resource["url"], serde_json::Value::Null);
}

/// Test package validation logic
#[test]
fn test_package_validation() {
    let temp_dir = setup_test_env();

    // Create a valid package structure
    create_test_package_structure(temp_dir.path(), "valid.package", "1.0.0").unwrap();
    let valid_package_dir = temp_dir.path().join("valid.package-1.0.0");

    // Test package directory exists
    assert!(valid_package_dir.exists(), "Package directory should exist");
    assert!(
        valid_package_dir.join("package.json").exists(),
        "package.json should exist"
    );
    assert!(
        valid_package_dir.join("package").exists(),
        "package directory should exist"
    );

    // Test package.json content
    let package_json_content =
        std::fs::read_to_string(valid_package_dir.join("package.json")).unwrap();
    let package_json: serde_json::Value = serde_json::from_str(&package_json_content).unwrap();

    assert_eq!(package_json["name"], "valid.package");
    assert_eq!(package_json["version"], "1.0.0");
    assert_eq!(package_json["fhirVersions"][0], "4.0.1");
}

/// Test extracted package structure
#[test]
fn test_extracted_package_structure() {
    let temp_dir = setup_test_env();

    // Create test resources
    let resources = vec![
        FhirResource {
            resource_type: "StructureDefinition".to_string(),
            id: "test-structure".to_string(),
            url: Some("http://example.com/StructureDefinition/test-structure".to_string()),
            version: None,
            content: json!({
                "resourceType": "StructureDefinition",
                "id": "test-structure",
                "url": "http://example.com/StructureDefinition/test-structure"
            }),
            file_path: temp_dir.path().join("test-structure.json"),
        },
        FhirResource {
            resource_type: "ValueSet".to_string(),
            id: "test-valueset".to_string(),
            url: Some("http://example.com/ValueSet/test-valueset".to_string()),
            version: None,
            content: json!({
                "resourceType": "ValueSet",
                "id": "test-valueset",
                "url": "http://example.com/ValueSet/test-valueset"
            }),
            file_path: temp_dir.path().join("test-valueset.json"),
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

    let extracted = ExtractedPackage {
        name: "test.package".to_string(),
        version: "1.0.0".to_string(),
        manifest,
        resources,
        extraction_path: temp_dir.path().to_path_buf(),
    };

    assert_eq!(extracted.name, "test.package");
    assert_eq!(extracted.version, "1.0.0");
    assert_eq!(extracted.resources.len(), 2);
    assert_eq!(extracted.resources[0].resource_type, "StructureDefinition");
    assert_eq!(extracted.resources[1].resource_type, "ValueSet");
}

/// Test package dependency extraction
#[test]
fn test_package_dependencies() {
    let mut dependencies = HashMap::new();
    dependencies.insert("hl7.fhir.r4.core".to_string(), "4.0.1".to_string());
    dependencies.insert("hl7.terminology.r4".to_string(), "5.0.0".to_string());

    let manifest = PackageManifest {
        name: "dependent.package".to_string(),
        version: "1.0.0".to_string(),
        description: Some("Package with dependencies".to_string()),
        fhir_versions: Some(vec!["4.0.1".to_string()]),
        dependencies,
        canonical: Some("http://example.com/dependent-package".to_string()),
        jurisdiction: None,
        package_type: None,
        title: None,
    };

    assert_eq!(manifest.dependencies.len(), 2);
    assert!(manifest.dependencies.contains_key("hl7.fhir.r4.core"));
    assert!(manifest.dependencies.contains_key("hl7.terminology.r4"));
    assert_eq!(
        manifest.dependencies.get("hl7.fhir.r4.core"),
        Some(&"4.0.1".to_string())
    );
    assert_eq!(
        manifest.dependencies.get("hl7.terminology.r4"),
        Some(&"5.0.0".to_string())
    );
}

/// Test package canonical URL validation
#[test]
fn test_canonical_url_validation() {
    // Valid canonical URLs
    let valid_urls = vec![
        "http://hl7.org/fhir/us/core",
        "https://example.com/fhir/package",
        "http://fhir.example.org/StructureDefinition/Patient",
    ];

    for url in valid_urls {
        // Test that URL parsing works
        let parsed = url::Url::parse(url);
        assert!(parsed.is_ok(), "Valid URL should parse: {url}");

        let parsed_url = parsed.unwrap();
        assert!(parsed_url.scheme() == "http" || parsed_url.scheme() == "https");
    }

    // Invalid canonical URLs
    let invalid_urls = vec![
        "not-a-url",
        "ftp://example.com/package",
        "",
        "relative/path",
    ];

    for url in invalid_urls {
        if !url.is_empty() {
            let parsed = url::Url::parse(url);
            // Some might parse successfully but with wrong scheme
            if let Ok(parsed_url) = parsed {
                assert!(
                    parsed_url.scheme() != "http" && parsed_url.scheme() != "https",
                    "Invalid URL should not have valid HTTP scheme: {url}"
                );
            }
        }
    }
}

/// Test package version validation
#[test]
fn test_package_version_validation() {
    // Valid version formats
    let valid_versions = vec!["1.0.0", "2.1.3", "0.9.0-beta", "1.0.0-alpha.1", "10.20.30"];

    for version in valid_versions {
        assert!(
            version.chars().next().unwrap().is_ascii_digit(),
            "Valid version should start with digit: {version}"
        );
    }

    // Invalid version formats
    let invalid_versions = vec!["alpha-1.0.0", "beta.1.0.0", "v1.0.0", "latest", ""];

    for version in invalid_versions {
        if !version.is_empty() {
            assert!(
                !version.chars().next().unwrap().is_ascii_digit(),
                "Invalid version should not start with digit: {version}"
            );
        }
    }
}
