//! Test fixtures and sample data

use serde_json::json;
use std::path::Path;

/// Create a sample FHIR StructureDefinition resource
pub fn create_sample_structure_definition(id: &str, url: &str) -> serde_json::Value {
    json!({
        "resourceType": "StructureDefinition",
        "id": id,
        "url": url,
        "name": format!("{}Structure", id.replace("-", "")),
        "title": format!("{} Structure Definition", id),
        "status": "active",
        "kind": "resource",
        "abstract": false,
        "type": "Patient",
        "baseDefinition": "http://hl7.org/fhir/StructureDefinition/Patient",
        "description": format!("Test structure definition for {}", id)
    })
}

/// Create a sample FHIR ValueSet resource
pub fn create_sample_value_set(id: &str, url: &str) -> serde_json::Value {
    json!({
        "resourceType": "ValueSet",
        "id": id,
        "url": url,
        "name": format!("{}ValueSet", id.replace("-", "")),
        "title": format!("{} Value Set", id),
        "status": "active",
        "description": format!("Test value set for {}", id),
        "compose": {
            "include": [
                {
                    "system": "http://hl7.org/fhir/administrative-gender",
                    "concept": [
                        {
                            "code": "male",
                            "display": "Male"
                        },
                        {
                            "code": "female",
                            "display": "Female"
                        }
                    ]
                }
            ]
        }
    })
}

/// Create a sample FHIR CodeSystem resource
pub fn create_sample_code_system(id: &str, url: &str) -> serde_json::Value {
    json!({
        "resourceType": "CodeSystem",
        "id": id,
        "url": url,
        "name": format!("{}CodeSystem", id.replace("-", "")),
        "title": format!("{} Code System", id),
        "status": "active",
        "content": "complete",
        "description": format!("Test code system for {}", id),
        "concept": [
            {
                "code": "active",
                "display": "Active",
                "definition": "The account is available for use."
            },
            {
                "code": "inactive",
                "display": "Inactive",
                "definition": "The account is not available for use."
            }
        ]
    })
}

/// Create a sample package manifest (package.json)
pub fn create_sample_package_manifest(name: &str, version: &str) -> serde_json::Value {
    json!({
        "name": name,
        "version": version,
        "description": format!("Sample FHIR package: {}", name),
        "canonical": format!("http://example.com/{}", name),
        "fhirVersions": ["4.0.1"],
        "dependencies": {},
        "keywords": ["fhir", "test"],
        "author": "Test Suite",
        "maintainers": [
            {
                "name": "Test Maintainer",
                "email": "test@example.com"
            }
        ]
    })
}

/// Create a sample FHIR SearchParameter resource
#[allow(dead_code)]
pub fn create_sample_search_parameter(
    id: &str,
    url: &str,
    code: &str,
    base: &str,
    param_type: &str,
) -> serde_json::Value {
    json!({
        "resourceType": "SearchParameter",
        "id": id,
        "url": url,
        "name": format!("{}SearchParam", id.replace("-", "")),
        "status": "active",
        "code": code,
        "base": [base],
        "type": param_type,
        "description": format!("Search parameter for {} on {}", code, base),
        "expression": format!("{}.{}", base, code),
        "xpath": format!("f:{}/f:{}", base, code)
    })
}

/// Create sample configuration files
pub fn create_sample_config() -> String {
    r#"# FHIR Canonical Manager Test Configuration

[registry]
url = "https://fs.get-ig.org/pkgs/"
timeout = 30
retry_attempts = 3

[[packages]]
name = "hl7.fhir.r4.core"
version = "4.0.1"
priority = 1

[[packages]]
name = "hl7.fhir.us.core"
version = "6.1.0"
priority = 2

[storage]
cache_dir = ".fcm/cache"
index_dir = ".fcm/index"
packages_dir = ".fcm/packages"
max_cache_size = "1GB"
"#
    .to_string()
}

/// Create an invalid configuration for error testing
pub fn create_invalid_config() -> String {
    r#"# Invalid FHIR Canonical Manager Configuration

[registry]
url = "not-a-valid-url"
timeout = 0
retry_attempts = -1

[[packages]]
name = ""
version = ""
priority = 0

[storage]
cache_dir = ""
index_dir = ""
packages_dir = ""
max_cache_size = "invalid-size"
"#
    .to_string()
}

/// Write test fixtures to the filesystem
pub fn setup_test_fixtures(fixtures_dir: &Path) -> std::io::Result<()> {
    // Create directories
    let configs_dir = fixtures_dir.join("configs");
    let packages_dir = fixtures_dir.join("packages");
    let resources_dir = fixtures_dir.join("resources");

    std::fs::create_dir_all(&configs_dir)?;
    std::fs::create_dir_all(&packages_dir)?;
    std::fs::create_dir_all(&resources_dir)?;

    // Write sample configurations
    std::fs::write(
        configs_dir.join("valid_config.toml"),
        create_sample_config(),
    )?;
    std::fs::write(
        configs_dir.join("invalid_config.toml"),
        create_invalid_config(),
    )?;

    // Write sample resources
    let structure_def = create_sample_structure_definition(
        "test-patient",
        "http://example.com/StructureDefinition/test-patient",
    );
    std::fs::write(
        resources_dir.join("StructureDefinition-test-patient.json"),
        serde_json::to_string_pretty(&structure_def)?,
    )?;

    let value_set =
        create_sample_value_set("test-values", "http://example.com/ValueSet/test-values");
    std::fs::write(
        resources_dir.join("ValueSet-test-values.json"),
        serde_json::to_string_pretty(&value_set)?,
    )?;

    let code_system =
        create_sample_code_system("test-codes", "http://example.com/CodeSystem/test-codes");
    std::fs::write(
        resources_dir.join("CodeSystem-test-codes.json"),
        serde_json::to_string_pretty(&code_system)?,
    )?;

    // Write sample package manifest
    let manifest = create_sample_package_manifest("test.package", "1.0.0");
    std::fs::write(
        packages_dir.join("package.json"),
        serde_json::to_string_pretty(&manifest)?,
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_create_sample_structure_definition() {
        let resource = create_sample_structure_definition("test-id", "http://example.com/test");

        assert_eq!(resource["resourceType"], "StructureDefinition");
        assert_eq!(resource["id"], "test-id");
        assert_eq!(resource["url"], "http://example.com/test");
        assert_eq!(resource["status"], "active");
    }

    #[test]
    fn test_create_sample_package_manifest() {
        let manifest = create_sample_package_manifest("test.package", "1.0.0");

        assert_eq!(manifest["name"], "test.package");
        assert_eq!(manifest["version"], "1.0.0");
        assert_eq!(manifest["fhirVersions"][0], "4.0.1");
    }

    #[test]
    fn test_setup_test_fixtures() {
        let temp_dir = TempDir::new().unwrap();
        let result = setup_test_fixtures(temp_dir.path());

        assert!(result.is_ok());
        assert!(temp_dir.path().join("configs/valid_config.toml").exists());
        assert!(
            temp_dir
                .path()
                .join("resources/StructureDefinition-test-patient.json")
                .exists()
        );
        assert!(temp_dir.path().join("packages/package.json").exists());
    }
}
