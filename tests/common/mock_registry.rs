//! Mock registry implementation for testing

use serde_json::json;
use std::collections::HashMap;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, path_regex},
};

/// Mock package data for testing
#[derive(Debug, Clone)]
pub struct MockPackageData {
    pub name: String,
    pub version: String,
    pub description: Option<String>,
    pub fhir_versions: Vec<String>,
    pub dependencies: HashMap<String, String>,
    pub canonical: Option<String>,
    pub tarball_url: Option<String>,
    pub content: Option<Vec<u8>>, // Package content for download
}

impl MockPackageData {
    pub fn new(name: &str, version: &str) -> Self {
        Self {
            name: name.to_string(),
            version: version.to_string(),
            description: Some(format!("Test package {name}")),
            fhir_versions: vec!["4.0.1".to_string()],
            dependencies: HashMap::new(),
            canonical: Some(format!("http://example.com/{name}")),
            tarball_url: None,
            content: None,
        }
    }

    pub fn with_dependencies(mut self, deps: HashMap<String, String>) -> Self {
        self.dependencies = deps;
        self
    }

    pub fn with_content(mut self, content: Vec<u8>) -> Self {
        self.content = Some(content);
        self
    }

    pub fn with_description(mut self, description: &str) -> Self {
        self.description = Some(description.to_string());
        self
    }
}

/// Mock registry server for testing
pub struct MockRegistry {
    server: MockServer,
    packages: HashMap<String, MockPackageData>,
    should_fail: bool,
    failure_count: usize,
    response_delay: Option<std::time::Duration>,
}

impl MockRegistry {
    /// Create a new mock registry
    pub async fn new() -> Self {
        let server = MockServer::start().await;

        Self {
            server,
            packages: HashMap::new(),
            should_fail: false,
            failure_count: 0,
            response_delay: None,
        }
    }

    /// Get the base URL of the mock registry
    pub fn url(&self) -> String {
        format!("{}/", self.server.uri())
    }

    /// Add a package to the mock registry
    pub fn add_package(&mut self, package_data: MockPackageData) {
        let package_key = package_data.name.clone();

        // Set tarball URL if not already set
        let mut package = package_data;
        if package.tarball_url.is_none() {
            package.tarball_url = Some(format!(
                "{}/-/{}-{}.tgz",
                self.url().trim_end_matches('/'),
                package.name,
                package.version
            ));
        }

        self.packages.insert(package_key, package);
    }

    /// Configure the registry to simulate network failures
    pub fn simulate_network_error(&mut self) {
        self.should_fail = true;
    }

    /// Configure the registry to fail for a specific number of requests
    pub fn simulate_temporary_failure(&mut self, failure_count: usize) {
        self.failure_count = failure_count;
    }

    /// Add response delay to simulate slow network
    pub fn with_delay(&mut self, delay: std::time::Duration) {
        self.response_delay = Some(delay);
    }

    /// Setup mock endpoints for all registered packages
    pub async fn setup_mocks(&self) {
        for (package_name, package_data) in &self.packages {
            self.setup_package_metadata_mock(package_name, package_data)
                .await;
            self.setup_package_download_mock(package_data).await;
        }

        // Setup failure mocks if configured
        if self.should_fail {
            self.setup_failure_mocks().await;
        }
    }

    /// Setup metadata endpoint for a package
    async fn setup_package_metadata_mock(
        &self,
        package_name: &str,
        package_data: &MockPackageData,
    ) {
        let metadata_response = self.create_npm_metadata_response(package_data);

        let mut response_template = ResponseTemplate::new(200).set_body_json(&metadata_response);

        if let Some(delay) = self.response_delay {
            response_template = response_template.set_delay(delay);
        }

        Mock::given(method("GET"))
            .and(path(package_name))
            .respond_with(response_template)
            .mount(&self.server)
            .await;
    }

    /// Setup download endpoint for a package
    async fn setup_package_download_mock(&self, package_data: &MockPackageData) {
        let download_path = format!("/-/{}-{}.tgz", package_data.name, package_data.version);

        let content = package_data
            .content
            .clone()
            .unwrap_or_else(|| self.create_mock_tarball(&package_data.name, &package_data.version));

        let mut response_template = ResponseTemplate::new(200)
            .set_body_bytes(content)
            .insert_header("content-type", "application/gzip");

        if let Some(delay) = self.response_delay {
            response_template = response_template.set_delay(delay);
        }

        Mock::given(method("GET"))
            .and(path(download_path))
            .respond_with(response_template)
            .mount(&self.server)
            .await;
    }

    /// Setup failure responses
    async fn setup_failure_mocks(&self) {
        if self.should_fail {
            // Return 500 for all requests
            Mock::given(method("GET"))
                .respond_with(ResponseTemplate::new(500))
                .mount(&self.server)
                .await;
        }
    }

    /// Create NPM-style metadata response
    fn create_npm_metadata_response(&self, package_data: &MockPackageData) -> serde_json::Value {
        let mut versions = HashMap::new();

        let version_info = json!({
            "name": package_data.name,
            "version": package_data.version,
            "description": package_data.description,
            "fhirVersions": package_data.fhir_versions,
            "dependencies": package_data.dependencies,
            "canonical": package_data.canonical,
            "dist": {
                "tarball": package_data.tarball_url,
                "shasum": "mock-shasum",
                "integrity": "sha512-mock-integrity"
            }
        });

        versions.insert(package_data.version.clone(), version_info);

        json!({
            "name": package_data.name,
            "versions": versions,
            "dist-tags": {
                "latest": package_data.version
            }
        })
    }

    /// Create a mock tarball with minimal package structure
    fn create_mock_tarball(&self, name: &str, version: &str) -> Vec<u8> {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        use tar::{Builder, Header};

        // Create a minimal package.json
        let package_json = json!({
            "name": name,
            "version": version,
            "description": format!("Mock package {}", name),
            "fhirVersions": ["4.0.1"],
            "dependencies": {}
        });

        let package_json_str = serde_json::to_string_pretty(&package_json).unwrap();

        // Create a simple FHIR resource for testing
        let test_resource = json!({
            "resourceType": "StructureDefinition",
            "id": "test-structure",
            "url": format!("http://example.com/{}/StructureDefinition/test", name),
            "name": "TestStructure",
            "status": "active",
            "kind": "resource",
            "abstract": false,
            "type": "Patient"
        });

        let test_resource_str = serde_json::to_string_pretty(&test_resource).unwrap();

        // Create tar.gz archive
        let mut buf = Vec::new();
        {
            let encoder = GzEncoder::new(&mut buf, Compression::default());
            let mut tar = Builder::new(encoder);

            // Add package.json
            let mut header = Header::new_gnu();
            header.set_path("package/package.json").unwrap();
            header.set_size(package_json_str.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            tar.append(&header, package_json_str.as_bytes()).unwrap();

            // Add test resource
            let mut header2 = Header::new_gnu();
            header2
                .set_path("package/StructureDefinition-test.json")
                .unwrap();
            header2.set_size(test_resource_str.len() as u64);
            header2.set_mode(0o644);
            header2.set_cksum();
            tar.append(&header2, test_resource_str.as_bytes()).unwrap();

            tar.into_inner().unwrap().finish().unwrap();
        }

        buf
    }

    /// Add a custom mock response
    pub async fn add_custom_mock(&self, path_pattern: &str, response: ResponseTemplate) {
        Mock::given(method("GET"))
            .and(path_regex(path_pattern))
            .respond_with(response)
            .mount(&self.server)
            .await;
    }

    /// Reset all mocks
    pub async fn reset(&self) {
        self.server.reset().await;
    }

    /// Verify that a request was made
    pub async fn verify_request_made(&self, method: &str, path: &str) {
        let requests = self.server.received_requests().await.unwrap();
        let found = requests
            .iter()
            .any(|req| req.method.as_str() == method && req.url.path() == path);
        assert!(found, "Expected request {method} {path} was not made");
    }
}

/// Helper function to create a mock registry with common test packages
pub async fn create_test_registry_with_packages() -> MockRegistry {
    let mut registry = MockRegistry::new().await;

    // Add core FHIR package
    let core_package =
        MockPackageData::new("hl7.fhir.r4.core", "4.0.1").with_description("FHIR R4 Core Package");
    registry.add_package(core_package);

    // Add US Core package with dependency
    let mut us_core_deps = HashMap::new();
    us_core_deps.insert("hl7.fhir.r4.core".to_string(), "4.0.1".to_string());

    let us_core_package = MockPackageData::new("hl7.fhir.us.core", "6.1.0")
        .with_description("US Core Implementation Guide")
        .with_dependencies(us_core_deps);
    registry.add_package(us_core_package);

    // Add test package
    let test_package = MockPackageData::new("test.package", "1.0.0")
        .with_description("Test package for unit tests");
    registry.add_package(test_package);

    registry.setup_mocks().await;
    registry
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mock_registry_creation() {
        let registry = MockRegistry::new().await;
        let url = registry.url();

        assert!(url.starts_with("http://"));
        assert!(url.ends_with("/"));
    }

    #[tokio::test]
    async fn test_add_package() {
        let mut registry = MockRegistry::new().await;
        let package = MockPackageData::new("test.package", "1.0.0");

        registry.add_package(package);
        assert!(registry.packages.contains_key("test.package"));
    }

    #[tokio::test]
    async fn test_create_test_registry_with_packages() {
        let registry = create_test_registry_with_packages().await;

        assert!(registry.packages.contains_key("hl7.fhir.r4.core"));
        assert!(registry.packages.contains_key("hl7.fhir.us.core"));
        assert!(registry.packages.contains_key("test.package"));
    }
}
