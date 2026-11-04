#![cfg(feature = "sqlite")]
use octofhir_canonical_core::{CanonicalManager, FcmConfig};

#[tokio::test]
async fn local_packages_resolve_without_an_http_adapter() {
    let temp = tempfile::tempdir().unwrap();
    let input = temp.path().join("input");
    std::fs::create_dir_all(&input).unwrap();
    let resource = serde_json::json!({
        "resourceType": "StructureDefinition", "id": "local-patient", "name": "LocalPatient",
        "url": "https://example.org/StructureDefinition/local-patient", "version": "1.0.0", "status": "draft"
    });
    std::fs::write(input.join("structure.json"), resource.to_string()).unwrap();
    let mut config = FcmConfig::test_config(temp.path());
    config.registry.url = "http://127.0.0.1:9".into();
    let storage = std::sync::Arc::new(
        octofhir_canonical_core::SqliteStorage::new(config.storage.clone())
            .await
            .unwrap(),
    );
    // A custom/local manager must not parse or initialize a remote registry.
    config.registry.url = "not-an-http-registry".into();
    let manager = CanonicalManager::new_with_storage(config, storage.clone(), storage)
        .await
        .unwrap();
    assert_eq!(
        manager
            .load_resources_from_directory(&input, "test")
            .await
            .unwrap(),
        1
    );
    let result = manager
        .resolve("https://example.org/StructureDefinition/local-patient")
        .await
        .unwrap();
    assert_eq!(result.resource.content, resource);
    let search = manager
        .search()
        .await
        .resource_type("StructureDefinition")
        .execute()
        .await
        .unwrap();
    assert_eq!(search.total_count, 1);
    let error = manager
        .list_registry_versions("hl7.fhir.r4.core")
        .await
        .unwrap_err();
    assert!(error.to_string().contains("No registry adapter"));
    assert!(
        manager
            .install_package("missing.remote.package", "1.0.0")
            .await
            .is_err()
    );
}
