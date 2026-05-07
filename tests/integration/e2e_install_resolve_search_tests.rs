//! End-to-end install → resolve → search test against the wiremock-based
//! `MockRegistry`. Hits no real network: the registry serves a canned
//! StructureDefinition tarball, the manager installs it, and we verify
//! that resolution by canonical URL plus full-text search both observe
//! the resource without any extra setup.

use octofhir_canonical_manager::{
    CanonicalManager, FcmConfig, OptimizationConfig, RegistryConfig, StorageConfig,
};

use crate::common::{create_test_registry_with_packages, setup_test_env};

#[tokio::test]
async fn install_then_resolve_then_search_roundtrips() {
    if crate::common::test_helpers::should_skip_net() {
        eprintln!("skipping net-bound test");
        return;
    }

    let temp_dir = setup_test_env();
    let registry = create_test_registry_with_packages().await;

    let config = FcmConfig {
        registry: RegistryConfig {
            url: registry.url(),
            timeout: 30,
            retry_attempts: 1,
            ..RegistryConfig::default()
        },
        packages: vec![],
        local_packages: vec![],
        resource_directories: vec![],
        storage: StorageConfig {
            cache_dir: temp_dir.path().join("cache"),
            packages_dir: temp_dir.path().join("packages"),
            max_cache_size: "50MB".to_string(),
            connection_pool_size: 4,
            fhir_cache_compat: false,
        },
        optimization: OptimizationConfig {
            parallel_workers: 1,
            batch_size: 100,
            enable_checksums: false,
            checksum_algorithm: "blake3".to_string(),
            checksum_cache_size: 1000,
            enable_metrics: false,
            metrics_interval: "30s".to_string(),
            durable_writes: false,
        },
    };

    let manager = CanonicalManager::new(config)
        .await
        .expect("manager creation");

    // Install — `test.package@1.0.0` is registered by
    // `create_test_registry_with_packages` and ships a single
    // StructureDefinition with canonical URL
    // `http://example.com/test.package/StructureDefinition/test`.
    manager
        .install_package("test.package", "1.0.0")
        .await
        .expect("install succeeds");

    let packages = manager.list_packages().await.expect("list packages");
    assert!(
        packages.iter().any(|p| p == "test.package@1.0.0"),
        "installed list should contain test.package@1.0.0; got {packages:?}"
    );

    // Resolve the canonical URL the mock tarball publishes.
    let canonical = "http://example.com/test.package/StructureDefinition/test";
    let resolved = manager
        .resolve(canonical)
        .await
        .expect("resolve installed canonical URL");
    assert_eq!(
        resolved.canonical_url, canonical,
        "resolved canonical URL should match request"
    );
    assert_eq!(
        resolved.resource.resource_type, "StructureDefinition",
        "resource type should round-trip from tarball"
    );
    assert_eq!(
        resolved.package_info.name, "test.package",
        "resolved package name should match installed package"
    );

    // Search — text index should pick up the resource type / canonical URL.
    let hits = manager
        .search()
        .await
        .resource_type("StructureDefinition")
        .execute()
        .await
        .expect("search executes");
    assert!(
        hits.resources
            .iter()
            .any(|r| r.index.canonical_url == canonical),
        "search should surface installed StructureDefinition; got {} hit(s)",
        hits.resources.len()
    );
}
