//! Package metadata lookups: registry URLs with a path, fallback registries, retries, and the
//! error for a missing package. All served by a local mock registry.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use octofhir_canonical_manager::RegistryConfig;
use octofhir_canonical_manager::registry::RegistryClient;
use serde_json::json;
use tempfile::TempDir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

fn metadata(name: &str, server: &MockServer) -> serde_json::Value {
    json!({
        "name": name,
        "dist-tags": { "latest": "1.0.0" },
        "versions": {
            "1.0.0": {
                "name": name,
                "version": "1.0.0",
                "fhirVersions": ["6.0.0"],
                "dist": { "tarball": format!("{}/{name}-1.0.0.tgz", server.uri()) }
            }
        }
    })
}

async fn client(url: String, fallbacks: Vec<String>) -> (RegistryClient, TempDir) {
    let config = RegistryConfig {
        url,
        fallbacks,
        retry_attempts: 3,
        timeout: 10,
        ..RegistryConfig::default()
    };
    let cache = TempDir::new().unwrap();
    let client = RegistryClient::new(&config, cache.path().to_path_buf())
        .await
        .unwrap();
    (client, cache)
}

#[tokio::test]
async fn a_registry_url_with_a_path_needs_no_trailing_slash() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/packages/hl7.fhir.r6.core"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(metadata("hl7.fhir.r6.core", &server)),
        )
        .mount(&server)
        .await;
    let (client, _cache) = client(format!("{}/packages", server.uri()), Vec::new()).await;
    let found = client
        .get_package_metadata("hl7.fhir.r6.core", "1.0.0")
        .await
        .unwrap();
    assert_eq!(found.version, "1.0.0");
}

#[tokio::test]
async fn metadata_comes_from_a_fallback_when_the_primary_lacks_the_package() {
    let primary = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&primary)
        .await;
    let fallback = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/packages/hl7.fhir.r6.core"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(metadata("hl7.fhir.r6.core", &fallback)),
        )
        .mount(&fallback)
        .await;
    let (client, _cache) =
        client(primary.uri(), vec![format!("{}/packages", fallback.uri())]).await;
    let found = client
        .get_package_metadata("hl7.fhir.r6.core", "1.0.0")
        .await
        .unwrap();
    assert_eq!(found.name, "hl7.fhir.r6.core");
}

/// Fails with 503 a number of times, then serves the metadata.
struct Flaky {
    failures: usize,
    calls: Arc<AtomicUsize>,
    body: serde_json::Value,
}

impl Respond for Flaky {
    fn respond(&self, _: &Request) -> ResponseTemplate {
        if self.calls.fetch_add(1, Ordering::SeqCst) < self.failures {
            ResponseTemplate::new(503)
        } else {
            ResponseTemplate::new(200).set_body_json(&self.body)
        }
    }
}

#[tokio::test]
async fn metadata_requests_are_retried_on_server_errors() {
    let server = MockServer::start().await;
    let calls = Arc::new(AtomicUsize::new(0));
    Mock::given(method("GET"))
        .and(path("/test.package"))
        .respond_with(Flaky {
            failures: 2,
            calls: calls.clone(),
            body: metadata("test.package", &server),
        })
        .mount(&server)
        .await;
    let (client, _cache) = client(server.uri(), Vec::new()).await;
    let found = client.get_package_metadata("test.package", "1.0.0").await;
    assert!(found.is_ok(), "{found:?}");
    assert_eq!(calls.load(Ordering::SeqCst), 3);
}

#[tokio::test]
async fn a_missing_package_is_reported_by_name_and_version_without_retries() {
    let server = MockServer::start().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    Mock::given(method("GET"))
        .respond_with(move |_: &Request| {
            counter.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(404)
        })
        .mount(&server)
        .await;
    let (client, _cache) = client(server.uri(), Vec::new()).await;
    let error = client
        .get_package_metadata("no.such.package", "2.0.0")
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("no.such.package@2.0.0"), "{error}");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}
