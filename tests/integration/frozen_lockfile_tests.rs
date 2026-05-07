//! Coverage for `--frozen-lockfile` install semantics: drift detection,
//! match round-trip, and missing-lockfile rejection.

use octofhir_canonical_manager::{
    CanonicalManager, FcmConfig, InstallOptions, OptimizationConfig, PackageSpec, RegistryConfig,
    StorageConfig,
    error::{FcmError, StorageError},
};

use crate::common::{create_test_registry_with_packages, setup_test_env};

const PACKAGE: &str = "test.package";
const VERSION: &str = "1.0.0";

fn config_with_registry(temp: &std::path::Path, registry_url: String) -> FcmConfig {
    FcmConfig {
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
    }
}

fn one_spec() -> Vec<PackageSpec> {
    vec![PackageSpec {
        name: PACKAGE.to_string(),
        version: VERSION.to_string(),
        priority: 1,
        url: None,
    }]
}

#[tokio::test]
async fn frozen_install_succeeds_when_lockfile_matches() {
    if crate::common::test_helpers::should_skip_net() {
        eprintln!("skipping net-bound test");
        return;
    }
    let temp = setup_test_env();
    let registry = create_test_registry_with_packages().await;
    let manager = CanonicalManager::new(config_with_registry(temp.path(), registry.url()))
        .await
        .expect("manager");

    manager
        .install_package(PACKAGE, VERSION)
        .await
        .expect("initial install");

    manager
        .write_lockfile(temp.path())
        .await
        .expect("write lockfile");

    let opts = InstallOptions {
        frozen_lockfile: true,
        project_root: Some(temp.path().to_path_buf()),
    };
    manager
        .install_packages_batch_with_options(one_spec(), opts)
        .await
        .expect("frozen install over matching lockfile must succeed");
}

#[tokio::test]
async fn frozen_install_fails_on_integrity_drift() {
    if crate::common::test_helpers::should_skip_net() {
        eprintln!("skipping net-bound test");
        return;
    }
    let temp = setup_test_env();
    let registry = create_test_registry_with_packages().await;
    let manager = CanonicalManager::new(config_with_registry(temp.path(), registry.url()))
        .await
        .expect("manager");

    manager
        .install_package(PACKAGE, VERSION)
        .await
        .expect("install");
    manager
        .write_lockfile(temp.path())
        .await
        .expect("write lockfile");

    let lock_path = temp.path().join("fcm.lock");
    let body = tokio::fs::read_to_string(&lock_path)
        .await
        .expect("read lockfile");
    // Replace whatever sha512 we just locked with a deterministic
    // wrong value of the right shape; build_lockfile will produce a
    // different hash on the next call and assert_lockfile_unchanged
    // must surface the diff.
    let tampered_line = "integrity = \"sha512-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=\"";
    let tampered = body
        .lines()
        .map(|l| {
            if l.trim_start().starts_with("integrity = ") {
                tampered_line.to_string()
            } else {
                l.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join("\n");
    tokio::fs::write(&lock_path, tampered)
        .await
        .expect("rewrite lockfile");

    let opts = InstallOptions {
        frozen_lockfile: true,
        project_root: Some(temp.path().to_path_buf()),
    };
    let err = manager
        .install_packages_batch_with_options(one_spec(), opts)
        .await
        .expect_err("frozen install must fail on integrity drift");

    match err {
        FcmError::Storage(StorageError::LockfileDrift { details }) => {
            assert!(
                details.contains("integrity"),
                "drift message must mention integrity, got: {details}"
            );
            assert!(
                details.contains(PACKAGE),
                "drift message must name package, got: {details}"
            );
        }
        other => panic!("expected LockfileDrift, got {other:?}"),
    }

    // Lockfile on disk must remain untouched (still the tampered bytes).
    let after = tokio::fs::read_to_string(&lock_path)
        .await
        .expect("read lockfile after");
    assert!(
        after.contains("AAAAAAAAAAA"),
        "frozen install must not rewrite fcm.lock"
    );
}

#[tokio::test]
async fn frozen_install_fails_when_lockfile_missing() {
    if crate::common::test_helpers::should_skip_net() {
        eprintln!("skipping net-bound test");
        return;
    }
    let temp = setup_test_env();
    let registry = create_test_registry_with_packages().await;
    let manager = CanonicalManager::new(config_with_registry(temp.path(), registry.url()))
        .await
        .expect("manager");

    let opts = InstallOptions {
        frozen_lockfile: true,
        project_root: Some(temp.path().to_path_buf()),
    };
    let err = manager
        .install_packages_batch_with_options(one_spec(), opts)
        .await
        .expect_err("frozen install must fail without lockfile");

    match err {
        FcmError::Storage(StorageError::LockfileDrift { details }) => {
            assert!(
                details.contains("fcm.lock"),
                "missing-lockfile error must mention fcm.lock, got: {details}"
            );
        }
        other => panic!("expected LockfileDrift, got {other:?}"),
    }

    // The package must NOT have been installed: missing-lockfile is a
    // pre-flight check.
    assert!(
        manager
            .list_packages()
            .await
            .expect("list_packages")
            .is_empty(),
        "frozen install must abort before downloading anything"
    );
}
