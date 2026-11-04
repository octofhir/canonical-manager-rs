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

/// A registry adapter handed to the manager answers version lists and searches too, not only
/// metadata and downloads (a server supplying its own HTTP registry client lost both in 0.2.11).
#[tokio::test]
async fn a_supplied_registry_lists_versions_and_searches() {
    use octofhir_canonical_core::config::PackageSpec;
    use octofhir_canonical_core::registry::{PackageDownload, PackageInfo, PackageMetadata};
    use octofhir_canonical_core::traits::AsyncRegistry;

    struct Listing;

    #[async_trait::async_trait]
    impl AsyncRegistry for Listing {
        async fn get_package_metadata(
            &self,
            _name: &str,
            _version: &str,
        ) -> octofhir_canonical_core::Result<PackageMetadata> {
            unreachable!("not asked for metadata")
        }
        async fn download_package(
            &self,
            _spec: &PackageSpec,
        ) -> octofhir_canonical_core::Result<PackageDownload> {
            unreachable!("not asked to download")
        }
        async fn list_versions(&self, name: &str) -> octofhir_canonical_core::Result<Vec<String>> {
            assert_eq!(name, "hl7.fhir.us.core");
            Ok(vec!["9.0.0".into(), "6.1.0".into()])
        }
        async fn search_packages(
            &self,
            query: &str,
        ) -> octofhir_canonical_core::Result<Vec<PackageInfo>> {
            Ok(vec![PackageInfo {
                name: format!("{query}.found"),
                versions: vec!["1.0.0".into()],
                description: None,
                latest_version: "1.0.0".into(),
            }])
        }
    }

    let temp = tempfile::tempdir().unwrap();
    let config = FcmConfig::test_config(temp.path());
    let storage = std::sync::Arc::new(
        octofhir_canonical_core::SqliteStorage::new(config.storage.clone())
            .await
            .unwrap(),
    );
    let manager = CanonicalManager::new_with_components(
        config,
        storage.clone(),
        std::sync::Arc::new(Listing),
        storage,
    )
    .await
    .unwrap();
    assert_eq!(
        manager
            .list_registry_versions("hl7.fhir.us.core")
            .await
            .unwrap(),
        ["9.0.0", "6.1.0"]
    );
    let found = manager.search_registry("us.core").await.unwrap();
    assert_eq!(found[0].name, "us.core.found");
}
