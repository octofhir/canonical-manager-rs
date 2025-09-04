use octofhir_canonical_manager::package::PackageExtractor;
use octofhir_canonical_manager::registry::{PackageDownload, PackageMetadata};
use octofhir_canonical_manager::PackageSpec;
use serde_json::json;
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use tempfile::TempDir;

fn create_valid_tgz(path: &std::path::Path) {
    // Build a minimal package tar.gz with package/package.json and a resource
    let tgz = fs::File::create(path).unwrap();
    let enc = flate2::write::GzEncoder::new(tgz, flate2::Compression::default());
    let mut tar = tar::Builder::new(enc);

    // package/package.json
    let manifest = json!({
        "name": "atomic.package",
        "version": "1.0.0",
        "fhirVersions": ["4.0.1"],
        "dependencies": {}
    })
    .to_string();
    let mut header = tar::Header::new_gnu();
    header.set_size(manifest.as_bytes().len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    tar.append_data(&mut header, "package/package.json", manifest.as_bytes())
        .unwrap();

    // package/StructureDefinition-sample.json
    let resource = json!({
        "resourceType": "StructureDefinition",
        "id": "sample",
        "url": "http://example.com/StructureDefinition/sample"
    })
    .to_string();
    let mut header2 = tar::Header::new_gnu();
    header2.set_size(resource.as_bytes().len() as u64);
    header2.set_mode(0o644);
    header2.set_cksum();
    tar.append_data(&mut header2, "package/StructureDefinition-sample.json", resource.as_bytes())
        .unwrap();

    tar.into_inner().unwrap().finish().unwrap();
}

#[tokio::test]
async fn extraction_is_atomic_and_cleans_temp() {
    let temp = TempDir::new().unwrap();
    let cache_dir = temp.path().to_path_buf();
    let extractor = PackageExtractor::new(cache_dir.clone());

    // Create a valid tar.gz
    let tgz_path = temp.path().join("atomic-package-1.0.0.tgz");
    create_valid_tgz(&tgz_path);

    let spec = PackageSpec { name: "atomic.package".into(), version: "1.0.0".into(), priority: 1 };
    let metadata = PackageMetadata { name: spec.name.clone(), version: spec.version.clone(), description: None, fhir_version: "4.0.1".into(), dependencies: HashMap::new(), canonical_base: None };
    let download = PackageDownload { spec, file_path: tgz_path.clone(), metadata };

    let extracted = extractor.extract_package(download).await.expect("extraction should succeed");

    // Final directory exists
    assert!(extracted.extraction_path.exists());
    // No temp directories with tmp- prefix remain under parent
    let parent = extracted.extraction_path.parent().unwrap();
    for entry in fs::read_dir(parent).unwrap() {
        let p = entry.unwrap().path();
        let name = p.file_name().unwrap().to_string_lossy();
        assert!(
            !name.contains("tmp-"),
            "temporary directory should be cleaned: {}",
            name
        );
    }
}

#[tokio::test]
async fn download_writes_part_then_renames() {
    use octofhir_canonical_manager::registry::RegistryClient;
    use octofhir_canonical_manager::RegistryConfig;
    use crate::common::create_test_registry_with_packages;

    let registry = create_test_registry_with_packages().await;
    let config = RegistryConfig { url: registry.url(), timeout: 10, retry_attempts: 1 };
    let temp = TempDir::new().unwrap();
    let client = RegistryClient::new(&config, temp.path().to_path_buf()).await.unwrap();

    let spec = PackageSpec { name: "test.package".into(), version: "1.0.0".into(), priority: 1 };
    let result = client.download_package(&spec).await.expect("download ok");

    // Final file exists
    assert!(result.file_path.exists());
    // .part file does not remain
    let part = result
        .file_path
        .with_file_name(format!("{}.part", result.file_path.file_name().unwrap().to_string_lossy()));
    assert!(!part.exists(), ".part should have been renamed away");
}

