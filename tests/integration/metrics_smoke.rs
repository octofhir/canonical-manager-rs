//! Smoke test to ensure metrics-off mode doesn't panic

use octofhir_canonical_manager::{CanonicalManager, FcmConfig};

use crate::common::setup_test_env;

#[tokio::test]
async fn metrics_off_smoke_no_panics() {
    let tmp = setup_test_env();
    let mut cfg = FcmConfig::test_config(tmp.path());
    // Ensure metrics are off in optimization config
    cfg.optimization.enable_metrics = false;

    // Manager should initialize and basic calls should not panic
    let manager = CanonicalManager::new(cfg).await.expect("manager init");

    // With no packages, resolve should error but not panic
    let res = manager.resolve("http://example.com/StructureDefinition/none").await;
    assert!(res.is_err());
}

