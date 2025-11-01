//! Simple perf baseline: install + resolve timing

use octofhir_canonical_manager::{CanonicalManager, FcmConfig, OptimizationConfig, RegistryConfig, StorageConfig};
use crate::common::create_test_registry_with_packages;

#[tokio::test]
async fn install_and_resolve_timing_baseline() {
    if crate::common::test_helpers::should_skip_net() { eprintln!("skipping net-bound test"); return; }
    let tmp = crate::common::setup_test_env();
    let registry = create_test_registry_with_packages().await;

    let cfg = FcmConfig {
        registry: RegistryConfig { url: registry.url(), timeout: 5, retry_attempts: 1 },
        packages: vec![],
        storage: StorageConfig {
            cache_dir: tmp.path().join("cache"),
            index_dir: tmp.path().join("index"),
            packages_dir: tmp.path().join("packages"),
            max_cache_size: "10MB".into(),
        },
        optimization: OptimizationConfig {
            parallel_workers: 1,
            batch_size: 100,
            enable_checksums: false,
            checksum_algorithm: "blake3".to_string(),
            checksum_cache_size: 1000,
            enable_metrics: false,
            metrics_interval: "30s".to_string(),
        },
    };

    let manager = CanonicalManager::new(cfg).await.unwrap();

    let start_install = std::time::Instant::now();
    manager.install_package("test.package", "1.0.0").await.unwrap();
    let install_ms = start_install.elapsed().as_millis();

    let start_resolve = std::time::Instant::now();
    let _ = manager.resolve("http://example.com/test.package/StructureDefinition/test").await;
    let resolve_ms = start_resolve.elapsed().as_millis();

    println!("perf_baseline install_ms={} resolve_ms={}", install_ms, resolve_ms);
}
