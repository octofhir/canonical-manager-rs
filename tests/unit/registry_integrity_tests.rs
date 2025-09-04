use octofhir_canonical_manager::config::{RegistryConfig, PackageSpec};
use octofhir_canonical_manager::registry::RegistryClient;
use crate::common::create_test_registry_with_packages;

#[tokio::test]
async fn download_succeeds_with_valid_integrity() {
    if crate::common::test_helpers::should_skip_net() { eprintln!("skipping net-bound test"); return; }
    let registry = create_test_registry_with_packages().await;

    let cfg = RegistryConfig { url: registry.url(), timeout: 5, retry_attempts: 1 };
    let tmp = tempfile::TempDir::new().unwrap();
    let client = RegistryClient::new(&cfg, tmp.path().to_path_buf()).await.unwrap();

    let spec = PackageSpec { name: "test.package".into(), version: "1.0.0".into(), priority: 1 };
    let result = client.download_package(&spec).await;
    assert!(result.is_ok(), "download should succeed with computed checksums");
}

#[tokio::test]
async fn download_fails_on_bad_shasum() {
    if crate::common::test_helpers::should_skip_net() { eprintln!("skipping net-bound test"); return; }
    // Build a registry with a package having wrong shasum
    let mut registry = crate::common::mock_registry::MockRegistry::new().await;
    let mut pkg = crate::common::mock_registry::MockPackageData::new("bad.checksum.pkg", "1.0.0");
    pkg.shasum = Some("0000000000000000000000000000000000000000".into()); // 40 hex wrong
    registry.add_package(pkg);
    registry.setup_mocks().await;

    let cfg = RegistryConfig { url: registry.url(), timeout: 5, retry_attempts: 1 };
    let tmp = tempfile::TempDir::new().unwrap();
    let client = RegistryClient::new(&cfg, tmp.path().to_path_buf()).await.unwrap();

    let spec = PackageSpec { name: "bad.checksum.pkg".into(), version: "1.0.0".into(), priority: 1 };
    let result = client.download_package(&spec).await;
    assert!(result.is_err(), "download should fail on checksum mismatch");
}

#[tokio::test]
async fn metadata_uses_etag_and_304() {
    if crate::common::test_helpers::should_skip_net() { eprintln!("skipping net-bound test"); return; }
    let registry = create_test_registry_with_packages().await;
    let cfg = RegistryConfig { url: registry.url(), timeout: 5, retry_attempts: 1 };
    let tmp = tempfile::TempDir::new().unwrap();
    let client = RegistryClient::new(&cfg, tmp.path().to_path_buf()).await.unwrap();

    // First request populates cache and validators
    let _ = client.get_package_metadata("test.package", "1.0.0").await.unwrap();
    // Second request should send If-None-Match and get 304 from server
    let _ = client.get_package_metadata("test.package", "1.0.0").await.unwrap();

    // Verify at least one request had If-None-Match for this package
    let requests = registry.received_requests().await;
    let mut saw_conditional = false;
    for req in requests {
        if req.url.path().ends_with("/test.package") {
            if req.headers.contains_key("if-none-match") { saw_conditional = true; break; }
        }
    }
    assert!(saw_conditional, "expected conditional request with If-None-Match");
}
