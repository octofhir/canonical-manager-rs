//! Verify that the per-`(name, version)` cross-process install lock
//! serialises concurrent `install_package` calls so the registry sees
//! exactly one tarball download even when 8 callers race for the same
//! package.

use std::sync::Arc;

use octofhir_canonical_manager::{
    CanonicalManager, FcmConfig, OptimizationConfig, RegistryConfig, StorageConfig,
};

use crate::common::{create_test_registry_with_packages, setup_test_env};

const CONCURRENCY: usize = 8;
const PACKAGE: &str = "test.package";
const VERSION: &str = "1.0.0";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_install_serialises_via_install_lock() {
    if crate::common::test_helpers::should_skip_net() {
        eprintln!("skipping net-bound test");
        return;
    }

    let temp = setup_test_env();
    let registry = create_test_registry_with_packages().await;

    let config = FcmConfig {
        registry: RegistryConfig {
            url: registry.url(),
            timeout: 30,
            retry_attempts: 1,
            // Pin lookups to the mock — public-registry fallbacks would
            // mask flakes from the mock under contention.
            fallbacks: vec![],
            ..RegistryConfig::default()
        },
        packages: vec![],
        local_packages: vec![],
        resource_directories: vec![],
        storage: StorageConfig {
            cache_dir: temp.path().join("cache"),
            packages_dir: temp.path().join("packages"),
            max_cache_size: "100MB".to_string(),
            connection_pool_size: 8,
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
            durable_writes: true,
        },
    };

    let manager = Arc::new(
        CanonicalManager::new(config)
            .await
            .expect("manager construction"),
    );

    // Hold a barrier so all tasks reach the install entry before any
    // makes it past the lock. Without the barrier the first task can
    // finish before the others even spawn, defeating the test.
    let barrier = Arc::new(tokio::sync::Barrier::new(CONCURRENCY));

    let mut handles = Vec::with_capacity(CONCURRENCY);
    for _ in 0..CONCURRENCY {
        let m = manager.clone();
        let b = barrier.clone();
        handles.push(tokio::spawn(async move {
            b.wait().await;
            m.install_package(PACKAGE, VERSION).await
        }));
    }

    for h in handles {
        h.await
            .expect("task join")
            .expect("install_package must succeed under contention");
    }

    let received = registry.received_requests().await;
    let tarball_hits = received
        .iter()
        .filter(|r| {
            let p = r.url.path();
            p.ends_with(&format!("/{PACKAGE}-{VERSION}.tgz"))
        })
        .count();

    assert_eq!(
        tarball_hits,
        1,
        "exactly one tarball fetch expected under per-package lock; \
         saw {tarball_hits}: {:?}",
        received
            .iter()
            .map(|r| r.url.to_string())
            .collect::<Vec<_>>()
    );

    let installed = manager.list_packages().await.expect("list_packages");
    assert!(
        installed.iter().any(|p| p.contains(PACKAGE)),
        "package must be visible after concurrent installs: {installed:?}"
    );
}
