//! Unit tests for configuration module

use octofhir_canonical_manager::error::{ConfigError, Validate};
use octofhir_canonical_manager::{FcmConfig, PackageSpec, RegistryConfig, StorageConfig};
use std::path::PathBuf;
use tempfile::TempDir;

/// Test basic configuration creation and defaults
#[test]
fn test_config_defaults() {
    let config = FcmConfig::default();

    assert_eq!(config.registry.url, "https://fs.get-ig.org/pkgs/");
    assert_eq!(config.registry.timeout, 30);
    assert_eq!(config.registry.retry_attempts, 3);
    assert!(config.packages.is_empty());
    assert_eq!(config.storage.max_cache_size, "1GB");
}

/// Test configuration validation - valid config
#[test]
fn test_valid_config_validation() {
    let config = FcmConfig::default();
    let result = config.validate();
    assert!(result.is_ok(), "Valid config should pass validation");
}

/// Test configuration validation - invalid registry URL
#[test]
fn test_invalid_registry_url_validation() {
    let mut config = FcmConfig::default();
    config.registry.url = "not-a-valid-url".to_string();

    let result = config.validate();
    assert!(result.is_err());

    if let Err(e) = result {
        if let octofhir_canonical_manager::FcmError::Config(ConfigError::InvalidRegistryUrl {
            url,
        }) = e
        {
            assert_eq!(url, "not-a-valid-url");
        } else {
            panic!("Expected InvalidRegistryUrl error, got: {e:?}");
        }
    }
}

/// Test configuration validation - zero timeout
#[test]
fn test_zero_timeout_validation() {
    let mut config = FcmConfig::default();
    config.registry.timeout = 0;

    let result = config.validate();
    assert!(result.is_err());
}

/// Test configuration validation - invalid package spec
#[test]
fn test_invalid_package_spec_validation() {
    let mut config = FcmConfig::default();
    config.packages.push(PackageSpec {
        name: "".to_string(),
        version: "1.0.0".to_string(),
        priority: 1,
    });

    let result = config.validate();
    assert!(result.is_err());
}

/// Test registry config validation
#[test]
fn test_registry_config_validation() {
    let mut registry_config = RegistryConfig::default();

    // Valid config should pass
    assert!(registry_config.validate().is_ok());

    // Invalid URL should fail
    registry_config.url = "invalid-url".to_string();
    assert!(registry_config.validate().is_err());

    // Non-HTTP scheme should fail
    registry_config.url = "ftp://example.com".to_string();
    assert!(registry_config.validate().is_err());

    // Zero timeout should fail
    registry_config.url = "https://example.com".to_string();
    registry_config.timeout = 0;
    assert!(registry_config.validate().is_err());

    // Excessive timeout should fail
    registry_config.timeout = 500;
    assert!(registry_config.validate().is_err());
}

/// Test storage config validation
#[test]
fn test_storage_config_validation() {
    let temp_dir = TempDir::new().unwrap();
    let mut storage_config = StorageConfig {
        cache_dir: temp_dir.path().join("cache"),
        packages_dir: temp_dir.path().join("packages"),
        max_cache_size: "1GB".to_string(),
    };

    // Valid config should pass
    assert!(storage_config.validate().is_ok());

    // Empty cache dir should fail
    storage_config.cache_dir = PathBuf::new();
    assert!(storage_config.validate().is_err());

    // Invalid cache size format should fail
    storage_config.cache_dir = temp_dir.path().join("cache");
    storage_config.max_cache_size = "invalid-size".to_string();
    assert!(storage_config.validate().is_err());

    // Valid cache size formats should pass
    storage_config.max_cache_size = "100MB".to_string();
    assert!(storage_config.validate().is_ok());

    storage_config.max_cache_size = "1GB".to_string();
    assert!(storage_config.validate().is_ok());

    storage_config.max_cache_size = "500KB".to_string();
    assert!(storage_config.validate().is_ok());
}

/// Test package spec validation
#[test]
fn test_package_spec_validation() {
    let mut package_spec = PackageSpec {
        name: "test.package".to_string(),
        version: "1.0.0".to_string(),
        priority: 1,
    };

    // Valid spec should pass
    assert!(package_spec.validate().is_ok());

    // Empty name should fail
    package_spec.name = "".to_string();
    assert!(package_spec.validate().is_err());

    // Empty version should fail
    package_spec.name = "test.package".to_string();
    package_spec.version = "".to_string();
    assert!(package_spec.validate().is_err());

    // Version not starting with digit should fail
    package_spec.version = "alpha-1".to_string();
    assert!(package_spec.validate().is_err());

    // Zero priority should fail
    package_spec.version = "1.0.0".to_string();
    package_spec.priority = 0;
    assert!(package_spec.validate().is_err());
}

/// Test configuration file loading and parsing
#[tokio::test]
async fn test_config_file_loading() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("fcm.toml");

    let config_content = r#"
[registry]
url = "https://test-registry.com/"
timeout = 60
retry_attempts = 5

[[packages]]
name = "test.package"
version = "1.0.0"
priority = 1

[storage]
cache_dir = "test_cache"
index_dir = "test_index"
packages_dir = "test_packages"
max_cache_size = "500MB"
"#;

    std::fs::write(&config_path, config_content).unwrap();

    let config = FcmConfig::from_file(&config_path).await.unwrap();

    assert_eq!(config.registry.url, "https://test-registry.com/");
    assert_eq!(config.registry.timeout, 60);
    assert_eq!(config.registry.retry_attempts, 5);
    assert_eq!(config.packages.len(), 1);
    assert_eq!(config.packages[0].name, "test.package");
    assert_eq!(config.packages[0].version, "1.0.0");
    assert_eq!(config.storage.max_cache_size, "500MB");
}

/// Test configuration file loading with invalid content
#[tokio::test]
async fn test_invalid_config_file_loading() {
    let temp_dir = TempDir::new().unwrap();
    let config_path = temp_dir.path().join("invalid.toml");

    let invalid_config = r#"
[registry]
url = "not-a-valid-url"
timeout = 0

[[packages]]
name = ""
version = ""
"#;

    std::fs::write(&config_path, invalid_config).unwrap();

    let result = FcmConfig::from_file(&config_path).await;
    assert!(result.is_err(), "Invalid config should fail to load");
}

/// Test environment variable overrides
#[test]
fn test_env_overrides() {
    // Set environment variables
    unsafe {
        std::env::set_var("FCM_REGISTRY_URL", "https://custom-registry.com/");
        std::env::set_var("FCM_CACHE_DIR", "/custom/cache");
    }

    let mut config = FcmConfig::default();
    config.apply_env_overrides();

    assert_eq!(config.registry.url, "https://custom-registry.com/");
    assert_eq!(config.storage.cache_dir, PathBuf::from("/custom/cache"));

    // Clean up
    unsafe {
        std::env::remove_var("FCM_REGISTRY_URL");
        std::env::remove_var("FCM_CACHE_DIR");
    }
}

/// Test package management functions
#[test]
fn test_package_management() {
    let mut config = FcmConfig::default();

    // Add package
    config.add_package("test.package", "1.0.0", Some(1));
    assert_eq!(config.packages.len(), 1);
    assert_eq!(config.get_package("test.package").unwrap().version, "1.0.0");

    // Update existing package
    config.add_package("test.package", "2.0.0", Some(1));
    assert_eq!(config.packages.len(), 1);
    assert_eq!(config.get_package("test.package").unwrap().version, "2.0.0");

    // Add another package
    config.add_package("another.package", "1.0.0", Some(2));
    assert_eq!(config.packages.len(), 2);

    // Test priority sorting
    assert_eq!(config.packages[0].priority, 1);
    assert_eq!(config.packages[1].priority, 2);

    // Remove package
    let removed = config.remove_package("test.package");
    assert!(removed);
    assert_eq!(config.packages.len(), 1);
    assert!(config.get_package("test.package").is_none());

    // Try to remove non-existent package
    let not_removed = config.remove_package("nonexistent.package");
    assert!(!not_removed);
}

/// Test path expansion functionality
#[test]
fn test_path_expansion() {
    let config = FcmConfig::default();
    let expanded = config.get_expanded_storage_config();

    // The expanded config should have the same values since we don't use ~ in defaults
    assert_eq!(expanded.cache_dir, config.storage.cache_dir);
    assert_eq!(expanded.packages_dir, config.storage.packages_dir);
}

/// Test configuration serialization and deserialization
#[test]
fn test_config_serialization() {
    let mut config = FcmConfig::default();
    config.add_package("test.package", "1.0.0", Some(1));

    // Serialize to TOML
    let toml_string = toml::to_string(&config).unwrap();
    assert!(toml_string.contains("test.package"));
    assert!(toml_string.contains("1.0.0"));

    // Deserialize back
    let deserialized: FcmConfig = toml::from_str(&toml_string).unwrap();
    assert_eq!(deserialized.packages.len(), 1);
    assert_eq!(deserialized.packages[0].name, "test.package");
    assert_eq!(deserialized.packages[0].version, "1.0.0");

    // Validate deserialized config
    assert!(deserialized.validate().is_ok());
}
