//! Wiremock-based tests for `registry_clients`: NpmRegistryClient,
//! Packages2Client, RedundantRegistryClient.
//!
//! These tests don't hit any real registry — every endpoint is stubbed via
//! `wiremock::MockServer`. Covers the three behaviours that matter most for
//! the P0 milestone:
//!
//! - `NpmRegistryClient::search` parses both PascalCase
//!   (`packages.fhir.org`-style) and camelCase
//!   (`packages2.fhir.org`-style) catalog responses.
//! - `Packages2Client::download` performs the tarball-direct GET against
//!   `{base}/{name}/{version}` and verifies the bytes round-trip to disk.
//! - `RedundantRegistryClient::search` fans out and dedupes results from
//!   heterogeneous backends. (Pure-stub orchestrator tests live inside
//!   `src/registry_clients.rs`; this file exercises the real wire format.)

use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use octofhir_canonical_manager::config::{PackageSpec, RegistryConfig};
use octofhir_canonical_manager::registry_clients::{
    FhirRegistryClient, GetIgRegistryClient, NpmRegistryClient, Packages2Client,
    RedundantRegistryClient, SearchQuery,
};

/// Minimal valid .tgz body produced by hand for tests that just need the
/// download path to round-trip bytes. Not actually a valid tarball — the
/// download client doesn't unpack, just stores.
fn fake_tarball_bytes() -> Vec<u8> {
    b"\x1f\x8b\x08\x00fakefake\x00\x03\x03\x00\x00\x00\x00\x00\x00\x00\x00\x00".to_vec()
}

#[tokio::test]
async fn npm_client_search_parses_pascalcase_catalog() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/catalog"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"Name": "hl7.fhir.us.core", "Description": "US Core", "FhirVersion": "R4"},
            {"Name": "hl7.fhir.r4.core", "Description": "R4 Base"}
        ])))
        .mount(&server)
        .await;
    // Also mount /{name} so RegistryClient construction doesn't choke on
    // the unrelated metadata path. Not strictly needed for the search test.
    Mock::given(method("GET"))
        .and(path("/hl7.fhir.us.core"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;

    let config = RegistryConfig {
        url: server.uri(),
        timeout: 30,
        retry_attempts: 1,
        ..RegistryConfig::default()
    };
    let temp = TempDir::new().unwrap();
    let client = NpmRegistryClient::new(&config, temp.path().to_path_buf())
        .await
        .unwrap();

    let hits = client
        .search(&SearchQuery::by_name("core"))
        .await
        .expect("search should succeed");
    assert_eq!(hits.len(), 2, "expected both catalog rows: {hits:?}");
    assert!(hits.iter().any(|h| h.name == "hl7.fhir.us.core"));
    assert!(hits.iter().any(|h| h.name == "hl7.fhir.r4.core"));
}

#[tokio::test]
async fn packages2_client_search_parses_camelcase_catalog() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/catalog"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"name": "hl7.fhir.r4.core", "version": "4.0.1", "fhirVersion": "4.0.1",
             "canonical": "http://hl7.org/fhir", "kind": "fhir.core"},
            {"name": "hl7.fhir.r5.core", "version": "5.0.0", "fhirVersion": "5.0.0",
             "canonical": "http://hl7.org/fhir", "kind": "fhir.core"}
        ])))
        .mount(&server)
        .await;

    let temp = TempDir::new().unwrap();
    let client = Packages2Client::new(&server.uri(), temp.path().to_path_buf(), 30)
        .await
        .unwrap();

    let hits = client.search(&SearchQuery::by_name("core")).await.unwrap();
    assert_eq!(hits.len(), 2);
    let r4 = hits.iter().find(|h| h.name == "hl7.fhir.r4.core").unwrap();
    assert_eq!(r4.version.as_deref(), Some("4.0.1"));
    assert_eq!(r4.fhir_version.as_deref(), Some("4.0.1"));
    assert_eq!(r4.canonical.as_deref(), Some("http://hl7.org/fhir"));
}

#[tokio::test]
async fn packages2_client_list_versions_returns_not_supported() {
    let server = MockServer::start().await;
    let temp = TempDir::new().unwrap();
    let client = Packages2Client::new(&server.uri(), temp.path().to_path_buf(), 30)
        .await
        .unwrap();
    let err = client.list_versions("anything").await.unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("not supported"),
        "expected NotSupported, got: {msg}"
    );
}

#[tokio::test]
async fn packages2_client_download_streams_tarball_to_disk() {
    let server = MockServer::start().await;
    let body = fake_tarball_bytes();
    Mock::given(method("GET"))
        .and(path("/hl7.fhir.r4.core/4.0.1"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(body.clone())
                .insert_header("content-type", "application/octet-stream"),
        )
        .mount(&server)
        .await;

    let temp = TempDir::new().unwrap();
    let client = Packages2Client::new(&server.uri(), temp.path().to_path_buf(), 30)
        .await
        .unwrap();

    let spec = PackageSpec {
        name: "hl7.fhir.r4.core".to_string(),
        version: "4.0.1".to_string(),
        priority: 1,
        url: None,
    };
    let dl = client.download(&spec, None).await.expect("download");
    assert!(
        dl.file_path.exists(),
        "downloaded file should exist on disk"
    );
    let contents = std::fs::read(&dl.file_path).unwrap();
    assert_eq!(contents, body, "downloaded bytes should match server body");
    // No `.part` leftovers.
    let part = dl.file_path.with_extension("tgz.part");
    assert!(!part.exists(), "stale .part should not remain");
}

#[tokio::test]
async fn redundant_client_dedupes_across_heterogeneous_backends() {
    // Backend A: NPM-style /catalog returning PascalCase rows.
    let a = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/catalog"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"Name": "hl7.fhir.r4.core", "Description": "core R4"}
        ])))
        .mount(&a)
        .await;
    // Mount a no-op /{name} so RegistryClient construction is happy on
    // any incidental ETag probe. Not strictly required.
    Mock::given(method("GET"))
        .and(path("/hl7.fhir.r4.core"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&a)
        .await;

    // Backend B: Packages2-style /catalog returning camelCase rows.
    let b = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/catalog"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
            {"name": "hl7.fhir.r4.core", "version": "4.0.1"},
            {"name": "hl7.fhir.us.core", "version": "7.0.0"}
        ])))
        .mount(&b)
        .await;

    let temp = TempDir::new().unwrap();
    let cfg_a = RegistryConfig {
        url: a.uri(),
        timeout: 30,
        retry_attempts: 1,
        ..RegistryConfig::default()
    };
    let npm = NpmRegistryClient::new(&cfg_a, temp.path().join("a"))
        .await
        .unwrap();
    let p2 = Packages2Client::new(&b.uri(), temp.path().join("b"), 30)
        .await
        .unwrap();
    let chain = RedundantRegistryClient::new(vec![Arc::new(npm), Arc::new(p2)]);

    let hits = chain.search(&SearchQuery::by_name("core")).await.unwrap();
    // Two distinct names; r4.core appears in both backends but with
    // different version info — orchestrator dedupes by (name, version):
    //   (r4.core, None) from A
    //   (r4.core, Some("4.0.1")) from B
    //   (us.core, Some("7.0.0")) from B
    // = 3 hits.
    assert_eq!(hits.len(), 3, "expected dedupe by (name,version): {hits:?}");
    assert!(
        hits.iter()
            .any(|h| h.name == "hl7.fhir.r4.core" && h.version.is_none())
    );
    assert!(
        hits.iter()
            .any(|h| h.name == "hl7.fhir.r4.core" && h.version.as_deref() == Some("4.0.1"))
    );
    assert!(hits.iter().any(|h| h.name == "hl7.fhir.us.core"));
}

#[tokio::test]
async fn redundant_client_download_falls_through_on_404() {
    // Backend A: 404 on the package.
    let a = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&a)
        .await;
    // Backend B: serves the tarball.
    let b = MockServer::start().await;
    let body = fake_tarball_bytes();
    Mock::given(method("GET"))
        .and(path("/hl7.fhir.r4.core/4.0.1"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(body.clone()))
        .mount(&b)
        .await;

    let temp = TempDir::new().unwrap();
    let cfg_a = RegistryConfig {
        url: a.uri(),
        timeout: 30,
        retry_attempts: 1,
        ..RegistryConfig::default()
    };
    let npm = NpmRegistryClient::new(&cfg_a, temp.path().join("a"))
        .await
        .unwrap();
    let p2 = Packages2Client::new(&b.uri(), temp.path().join("b"), 30)
        .await
        .unwrap();
    let chain = RedundantRegistryClient::new(vec![Arc::new(npm), Arc::new(p2)]);

    let spec = PackageSpec {
        name: "hl7.fhir.r4.core".to_string(),
        version: "4.0.1".to_string(),
        priority: 1,
        url: None,
    };
    let dl = chain.download(&spec, None).await.unwrap();
    let contents = std::fs::read(&dl.file_path).unwrap();
    assert_eq!(contents, body);
    // Path should be under backend B's cache subdir.
    assert!(
        dl.file_path.starts_with(temp.path().join("b")),
        "expected file to be in b/ cache dir, got: {}",
        dl.file_path.display()
    );
    // Sanity: the file is not in a/ (the failing backend's dir).
    let _ = Path::new(&dl.file_path);
}

#[tokio::test]
async fn getig_client_search_and_list_versions_return_not_supported() {
    let server = MockServer::start().await;
    let temp = TempDir::new().unwrap();
    let client = GetIgRegistryClient::new(&server.uri(), temp.path().to_path_buf(), 30)
        .await
        .unwrap();

    let s_err = client.search(&SearchQuery::by_name("x")).await.unwrap_err();
    assert!(
        format!("{s_err}").contains("not supported"),
        "expected NotSupported, got: {s_err}"
    );
    let lv_err = client.list_versions("x").await.unwrap_err();
    assert!(
        format!("{lv_err}").contains("not supported"),
        "expected NotSupported, got: {lv_err}"
    );
}

#[tokio::test]
async fn getig_client_download_follows_dist_tarball() {
    let server = MockServer::start().await;
    let body = fake_tarball_bytes();
    // Two-step: stub returns dist.tarball pointing to /tarball/foo.tgz on
    // the same MockServer; the client follows and stores bytes.
    let tarball_url = format!("{}/tarball/foo.tgz", server.uri());
    Mock::given(method("GET"))
        .and(path("/pkgs/hl7.fhir.r4.core/4.0.1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "dist": { "tarball": tarball_url }
        })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/tarball/foo.tgz"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(body.clone()))
        .mount(&server)
        .await;

    let temp = TempDir::new().unwrap();
    let client = GetIgRegistryClient::new(&server.uri(), temp.path().to_path_buf(), 30)
        .await
        .unwrap();

    let spec = PackageSpec {
        name: "hl7.fhir.r4.core".to_string(),
        version: "4.0.1".to_string(),
        priority: 1,
        url: None,
    };
    let dl = client.download(&spec, None).await.expect("download");
    assert!(dl.file_path.exists());
    let contents = std::fs::read(&dl.file_path).unwrap();
    assert_eq!(contents, body);
    let part = dl.file_path.with_extension("tgz.part");
    assert!(!part.exists(), "stale .part should not remain");
}

#[tokio::test]
async fn getig_client_download_404_maps_to_package_not_found() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/pkgs/missing.pkg/0.0.1"))
        .respond_with(ResponseTemplate::new(404))
        .mount(&server)
        .await;
    let temp = TempDir::new().unwrap();
    let client = GetIgRegistryClient::new(&server.uri(), temp.path().to_path_buf(), 30)
        .await
        .unwrap();
    let spec = PackageSpec {
        name: "missing.pkg".to_string(),
        version: "0.0.1".to_string(),
        priority: 1,
        url: None,
    };
    let err = client.download(&spec, None).await.unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.to_lowercase().contains("not found") || msg.contains("missing.pkg"),
        "expected PackageNotFound, got: {msg}"
    );
}
