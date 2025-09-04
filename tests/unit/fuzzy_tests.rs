#![cfg(feature = "fuzzy-search")]

use octofhir_canonical_manager::binary_storage::BinaryStorage;
use octofhir_canonical_manager::package::{ExtractedPackage, FhirResource, PackageManifest};
use octofhir_canonical_manager::resolver::{CanonicalResolver, ResolutionConfig, VersionPreference, ResolutionPath};
use octofhir_canonical_manager::config::StorageConfig;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

fn make_package(temp: &TempDir) -> ExtractedPackage {
    let structure_def = json!({
        "resourceType": "StructureDefinition",
        "id": "test-patient",
        "url": "http://example.com/StructureDefinition/test-patient",
        "status": "active",
        "type": "Patient"
    });
    let struct_file = temp.path().join("StructureDefinition-test-patient.json");
    std::fs::write(&struct_file, serde_json::to_string_pretty(&structure_def).unwrap()).unwrap();

    let resources = vec![FhirResource {
        resource_type: "StructureDefinition".to_string(),
        id: "test-patient".to_string(),
        url: Some("http://example.com/StructureDefinition/test-patient".to_string()),
        version: Some("1.0.0".to_string()),
        content: structure_def,
        file_path: struct_file,
    }];

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
        extraction_path: temp.path().to_path_buf(),
    }
}

#[tokio::test]
async fn test_fuzzy_index_resolves_similar_url() {
    let temp = TempDir::new().unwrap();
    let config = StorageConfig {
        cache_dir: temp.path().join("cache"),
        index_dir: temp.path().join("index"),
        packages_dir: temp.path().join("packages"),
        max_cache_size: "100MB".to_string(),
    };

    let storage = BinaryStorage::new(config).await.unwrap();
    let pkg = make_package(&temp);
    storage.add_package(&pkg).await.unwrap();
    let storage = Arc::new(storage);

    let cfg = ResolutionConfig {
        package_priorities: vec![],
        version_preference: VersionPreference::Latest,
        fuzzy_matching_threshold: 0.6,
        enable_fuzzy_matching: true,
        #[cfg(feature = "fuzzy-search")]
        fuzzy_max_candidates: 10,
    };
    let resolver = CanonicalResolver::with_config(storage, cfg);

    // Missing last char 't'
    let res = resolver
        .resolve("http://example.com/StructureDefinition/test-patien")
        .await
        .expect("should resolve via fuzzy");

    match res.resolution_path {
        ResolutionPath::FuzzyMatch { similarity } => {
            assert!(similarity > 0.6);
        }
        other => panic!("expected fuzzy match, got: {:?}", other),
    }
}

#[tokio::test]
async fn test_fuzzy_respects_candidate_cap() {
    let temp = TempDir::new().unwrap();
    let config = StorageConfig {
        cache_dir: temp.path().join("cache"),
        index_dir: temp.path().join("index"),
        packages_dir: temp.path().join("packages"),
        max_cache_size: "100MB".to_string(),
    };
    let storage = BinaryStorage::new(config).await.unwrap();

    // Add several packages/urls with close names
    for i in 0..50 {
        let structure_def = json!({
            "resourceType": "StructureDefinition",
            "id": format!("test-patient-{}", i),
            "url": format!("http://example.com/StructureDefinition/test-patient-{}", i),
            "status": "active",
            "type": "Patient"
        });
        let file = temp.path().join(format!("StructureDefinition-test-patient-{}.json", i));
        std::fs::write(&file, serde_json::to_string_pretty(&structure_def).unwrap()).unwrap();
        let res = FhirResource {
            resource_type: "StructureDefinition".to_string(),
            id: format!("test-patient-{}", i),
            url: Some(format!("http://example.com/StructureDefinition/test-patient-{}", i)),
            version: Some("1.0.0".to_string()),
            content: structure_def,
            file_path: file,
        };
        let manifest = PackageManifest {
            name: format!("test.package.{}", i),
            version: "1.0.0".to_string(),
            description: None,
            fhir_versions: Some(vec!["4.0.1".to_string()]),
            dependencies: HashMap::new(),
            canonical: None,
            jurisdiction: None,
            package_type: None,
            title: None,
        };
        let pkg = ExtractedPackage { name: manifest.name.clone(), version: manifest.version.clone(), manifest, resources: vec![res], extraction_path: temp.path().to_path_buf() };
        storage.add_package(&pkg).await.unwrap();
    }
    let storage = Arc::new(storage);
    let cfg = ResolutionConfig {
        package_priorities: vec![],
        version_preference: VersionPreference::Latest,
        fuzzy_matching_threshold: 0.5,
        enable_fuzzy_matching: true,
        #[cfg(feature = "fuzzy-search")]
        fuzzy_max_candidates: 5,
    };
    let resolver = CanonicalResolver::with_config(storage, cfg);
    let res = resolver
        .resolve("http://example.com/StructureDefinition/test-patient-2x")
        .await
        .expect("should resolve with limited candidates");
    match res.resolution_path {
        ResolutionPath::FuzzyMatch { similarity } => assert!(similarity > 0.5),
        other => panic!("expected fuzzy match, got: {:?}", other),
    }
}

