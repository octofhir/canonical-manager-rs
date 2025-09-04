use octofhir_canonical_manager::binary_storage::BinaryStorage;
use octofhir_canonical_manager::package::{ExtractedPackage, FhirResource, PackageManifest};
use octofhir_canonical_manager::resolver::{CanonicalResolver, ResolutionConfig, VersionPreference};
use octofhir_canonical_manager::StorageConfig;
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;

#[tokio::test]
async fn resolve_pipe_version_exact_match() {
    let tmp = TempDir::new().unwrap();
    let cfg = StorageConfig { 
        cache_dir: tmp.path().join("cache"),
        index_dir: tmp.path().join("index"),
        packages_dir: tmp.path().join("packages"),
        max_cache_size: "10MB".into(),
    };
    let storage = BinaryStorage::new(cfg).await.unwrap();

    let base = "http://example.com/StructureDefinition/test-resolve";
    let p1 = make_pkg("pkg.a", "1.0.0", &format!("{}/1.0.0", base));
    let p2 = make_pkg("pkg.b", "2.0.0", &format!("{}/2.0.0", base));
    storage.add_package(&p1).await.unwrap();
    storage.add_package(&p2).await.unwrap();

    let resolver = CanonicalResolver::new(Arc::new(storage));
    let resolved = resolver.resolve(&format!("{}|1.0.0", base)).await.unwrap();
    assert_eq!(resolved.resource.version.as_deref(), Some("1.0.0"));
}

#[tokio::test]
async fn compatible_selection_picks_highest_same_major() {
    let tmp = TempDir::new().unwrap();
    let cfg = StorageConfig { 
        cache_dir: tmp.path().join("cache"),
        index_dir: tmp.path().join("index"),
        packages_dir: tmp.path().join("packages"),
        max_cache_size: "10MB".into(),
    };
    let storage = BinaryStorage::new(cfg).await.unwrap();
    let base = "http://example.com/StructureDefinition/compat";
    for ver in ["1.0.0", "1.2.0", "2.0.0"] {
        let pkg = make_pkg(&format!("compat.{ver}"), ver, &format!("{}/{ver}", base));
        storage.add_package(&pkg).await.unwrap();
    }
    let storage = Arc::new(storage);
    let config = ResolutionConfig { version_preference: VersionPreference::Compatible, ..Default::default() };
    let resolver = CanonicalResolver::with_config(storage, config);
    let resolved = resolver.resolve(&format!("{}|1.0.0", base)).await.unwrap();
    assert_eq!(resolved.resource.version.as_deref(), Some("1.2.0"));
}

#[tokio::test]
async fn latest_selected_when_no_version_present_and_duplicate_base() {
    let tmp = TempDir::new().unwrap();
    let cfg = StorageConfig { 
        cache_dir: tmp.path().join("cache"),
        index_dir: tmp.path().join("index"),
        packages_dir: tmp.path().join("packages"),
        max_cache_size: "10MB".into(),
    };
    let storage = BinaryStorage::new(cfg).await.unwrap();
    // Two resources share the same canonical URL (no version in path), differing by metadata.version.
    let base = "http://example.com/StructureDefinition/latest-no-version";
    let p1 = make_pkg("pkg.l1", "1.0.0", base);
    let p2 = make_pkg("pkg.l2", "2.0.0", base);
    storage.add_package(&p1).await.unwrap();
    storage.add_package(&p2).await.unwrap(); // last wins in exact index

    let resolver = CanonicalResolver::new(Arc::new(storage));
    let resolved = resolver.resolve(base).await.unwrap();
    assert_eq!(resolved.resource.version.as_deref(), Some("2.0.0"));
}

fn make_pkg(name: &str, version: &str, url: &str) -> ExtractedPackage {
    let res = json!({
        "resourceType": "StructureDefinition",
        "id": "r",
        "url": url,
        "version": version,
    });
    let path = std::env::temp_dir().join(format!("{name}-{version}-r.json"));
    std::fs::write(&path, serde_json::to_string_pretty(&res).unwrap()).unwrap();
    let resource = FhirResource {
        resource_type: "StructureDefinition".into(),
        id: "r".into(),
        url: Some(url.into()),
        version: Some(version.into()),
        content: res,
        file_path: path,
    };
    let manifest = PackageManifest {
        name: name.into(),
        version: version.into(),
        fhir_versions: Some(vec!["4.0.1".into()]),
        dependencies: HashMap::new(),
        canonical: None,
        jurisdiction: None,
        package_type: None,
        title: None,
        description: None,
    };
    ExtractedPackage { name: name.into(), version: version.into(), manifest, resources: vec![resource], extraction_path: std::env::temp_dir() }
}

