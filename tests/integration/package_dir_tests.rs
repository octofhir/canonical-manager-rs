//! The folder of an installed package (`<packages_dir>/<name>-<version>/package/…`) is laid out
//! from the content store, whichever install path stored the package, and the FHIR cache mirror
//! and `remove_package` work from it.

use octofhir_canonical_manager::{
    CanonicalManager, FcmConfig, OptimizationConfig, RegistryConfig, StorageConfig,
};

use crate::common::{create_test_registry_with_packages, setup_test_env};

async fn manager(
    temp: &std::path::Path,
    registry_url: String,
    parallel_workers: usize,
) -> CanonicalManager {
    let config = FcmConfig {
        registry: RegistryConfig {
            url: registry_url,
            timeout: 30,
            retry_attempts: 1,
            ..RegistryConfig::default()
        },
        packages: vec![],
        local_packages: vec![],
        resource_directories: vec![],
        storage: StorageConfig {
            cache_dir: temp.join("cache"),
            packages_dir: temp.join("packages"),
            max_cache_size: "50MB".to_string(),
            connection_pool_size: 4,
            fhir_cache_compat: false,
        },
        optimization: OptimizationConfig {
            parallel_workers,
            batch_size: 100,
            enable_checksums: false,
            checksum_algorithm: "blake3".to_string(),
            checksum_cache_size: 1000,
            enable_metrics: false,
            metrics_interval: "30s".to_string(),
            durable_writes: false,
        },
    };
    CanonicalManager::new(config)
        .await
        .expect("manager creation")
}

#[tokio::test]
async fn installed_packages_have_a_folder() {
    if crate::common::test_helpers::should_skip_net() {
        eprintln!("skipping net-bound test");
        return;
    }
    // 1 worker takes the sequential install, more the parallel pipeline, which keeps no
    // folder of its own.
    for workers in [1, 4] {
        let temp_dir = setup_test_env();
        let registry = create_test_registry_with_packages().await;
        let manager = manager(temp_dir.path(), registry.url(), workers).await;
        manager
            .install_package("test.package", "1.0.0")
            .await
            .expect("install succeeds");

        let folder = manager
            .package_dir("test.package", "1.0.0")
            .await
            .expect("package folder");
        assert_eq!(folder, temp_dir.path().join("packages/test.package-1.0.0"));
        let manifest: serde_json::Value = serde_json::from_slice(
            &std::fs::read(folder.join("package/package.json")).expect("package.json"),
        )
        .expect("manifest is JSON");
        assert_eq!(manifest["name"], "test.package", "workers = {workers}");
        let resources: Vec<_> = std::fs::read_dir(folder.join("package"))
            .expect("package folder")
            .flatten()
            .filter(|e| e.file_name() != "package.json")
            .filter(|e| e.path().extension().is_some_and(|x| x == "json"))
            .collect();
        assert!(
            !resources.is_empty(),
            "resources laid out (workers = {workers})"
        );

        // The FHIR cache mirror finds it.
        let fhir_cache = temp_dir.path().join("fhir-packages");
        let report = manager
            .populate_fhir_cache_all(Some(&fhir_cache))
            .await
            .expect("populate FHIR cache");
        assert!(report.total > 0, "mirrored files (workers = {workers})");
        assert!(
            fhir_cache
                .join("test.package-1.0.0/package/package.json")
                .exists()
        );

        // Removing the package removes its folder.
        manager
            .remove_package("test.package", "1.0.0")
            .await
            .expect("remove");
        assert!(!folder.exists(), "folder removed (workers = {workers})");
        assert!(manager.package_dir("test.package", "1.0.0").await.is_err());
    }
}
