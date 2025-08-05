//! Configuration management for FHIR Canonical Manager

use crate::error::{ConfigError, Result, Validate};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Main configuration structure for the FHIR Canonical Manager.
///
/// This structure holds all configuration settings including registry configuration,
/// package specifications, and storage settings.
///
/// # Example
///
/// ```rust,no_run
/// use octofhir_canonical_manager::config::FcmConfig;
///
/// // Load configuration from default location
/// let config = FcmConfig::load().unwrap();
/// println!("Registry URL: {}", config.registry.url);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FcmConfig {
    pub registry: RegistryConfig,
    pub packages: Vec<PackageSpec>,
    pub storage: StorageConfig,
}

/// Configuration for FHIR package registry connection.
///
/// Contains settings for connecting to and downloading from FHIR package registries,
/// including timeout and retry parameters.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_manager::config::RegistryConfig;
///
/// let config = RegistryConfig {
///     url: "https://packages.fhir.org/packages/".to_string(),
///     timeout: 30,
///     retry_attempts: 3,
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RegistryConfig {
    #[serde(default = "default_registry_url")]
    pub url: String,
    #[serde(default = "default_timeout")]
    pub timeout: u64,
    #[serde(default = "default_retry_attempts")]
    pub retry_attempts: u32,
}

/// Specification for a FHIR package to be managed.
///
/// Defines a specific package with its name, version, and priority for resolution.
/// Lower priority numbers indicate higher precedence during canonical URL resolution.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_manager::config::PackageSpec;
///
/// let spec = PackageSpec {
///     name: "hl7.fhir.us.core".to_string(),
///     version: "6.1.0".to_string(),
///     priority: 1,
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageSpec {
    pub name: String,
    pub version: String,
    #[serde(default = "default_priority")]
    pub priority: u32,
}

/// Configuration for local storage paths and cache settings.
///
/// Defines where packages, cache files, and search indices are stored locally.
/// All paths can use tilde (`~`) expansion for home directory references.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_manager::config::StorageConfig;
/// use std::path::PathBuf;
///
/// let config = StorageConfig {
///     cache_dir: PathBuf::from("~/.fcm/cache"),
///     index_dir: PathBuf::from("~/.fcm/index"),
///     packages_dir: PathBuf::from("~/.fcm/packages"),
///     max_cache_size: "1GB".to_string(),
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    pub cache_dir: PathBuf,
    pub index_dir: PathBuf,
    pub packages_dir: PathBuf,
    #[serde(default = "default_max_cache_size")]
    pub max_cache_size: String,
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self {
            url: default_registry_url(),
            timeout: default_timeout(),
            retry_attempts: default_retry_attempts(),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        let home_dir = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
        let fcm_dir = home_dir.join(".fcm");

        Self {
            cache_dir: fcm_dir.join("cache"),
            index_dir: fcm_dir.join("index"),
            packages_dir: fcm_dir.join("packages"),
            max_cache_size: default_max_cache_size(),
        }
    }
}

impl FcmConfig {
    /// Loads configuration from the default location (fcm.toml in current directory).
    ///
    /// If no configuration file exists, returns a default configuration.
    /// Environment variable overrides are automatically applied.
    ///
    /// # Returns
    ///
    /// * `Ok(FcmConfig)` - The loaded and validated configuration
    /// * `Err` - If configuration file exists but is invalid
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use octofhir_canonical_manager::config::FcmConfig;
    ///
    /// let config = FcmConfig::load()?;
    /// println!("Loaded {} packages", config.packages.len());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn load() -> Result<Self> {
        let config_path = Self::default_config_path();
        if config_path.exists() {
            let mut config = Self::from_file(&config_path)?;
            config.apply_env_overrides();
            Ok(config)
        } else {
            let mut config = Self::default();
            config.apply_env_overrides();
            Ok(config)
        }
    }

    /// Loads configuration from a specific file path.
    ///
    /// The file must be in TOML format and contain valid configuration.
    /// The configuration is validated after loading.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the configuration file
    ///
    /// # Returns
    ///
    /// * `Ok(FcmConfig)` - The loaded and validated configuration
    /// * `Err` - If file cannot be read or contains invalid configuration
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use octofhir_canonical_manager::config::FcmConfig;
    /// use std::path::Path;
    ///
    /// let config = FcmConfig::from_file(Path::new("custom-config.toml"))?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn from_file(path: &std::path::Path) -> Result<Self> {
        let content = std::fs::read_to_string(path).map_err(|_| ConfigError::InvalidFile {
            path: path.to_path_buf(),
        })?;

        let config: Self = toml::from_str(&content)?;
        config.validate()?;
        Ok(config)
    }

    /// Returns the default configuration file path (fcm.toml in current directory).
    ///
    /// # Returns
    ///
    /// The path where the configuration file is expected to be located.
    ///
    /// # Example
    ///
    /// ```rust
    /// use octofhir_canonical_manager::config::FcmConfig;
    ///
    /// let path = FcmConfig::default_config_path();
    /// println!("Config path: {}", path.display());
    /// ```
    pub fn default_config_path() -> PathBuf {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join("fcm.toml")
    }

    /// Validates the current configuration for correctness.
    ///
    /// Checks registry URL format, timeout values, package specifications,
    /// and storage configuration for validity.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Configuration is valid
    /// * `Err` - Configuration contains invalid values with details
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use octofhir_canonical_manager::config::FcmConfig;
    ///
    /// let config = FcmConfig::load()?;
    /// config.validate()?;
    /// println!("Configuration is valid");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn validate(&self) -> Result<()> {
        // Use the Validate trait implementation
        <Self as Validate>::validate(self).map_err(|e| e.into())
    }

    /// Applies environment variable overrides to the configuration.
    ///
    /// The following environment variables are supported:
    /// - `FCM_REGISTRY_URL` - Override registry URL
    /// - `FCM_CACHE_DIR` - Override cache directory
    /// - `FCM_INDEX_DIR` - Override index directory
    /// - `FCM_PACKAGES_DIR` - Override packages directory
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use octofhir_canonical_manager::config::FcmConfig;
    ///
    /// let mut config = FcmConfig::default();
    /// config.apply_env_overrides();
    /// ```
    pub fn apply_env_overrides(&mut self) {
        // Override registry URL if set
        if let Ok(url) = std::env::var("FCM_REGISTRY_URL") {
            self.registry.url = url;
        }

        // Override cache directory if set
        if let Ok(cache_dir) = std::env::var("FCM_CACHE_DIR") {
            self.storage.cache_dir = PathBuf::from(cache_dir);
        }

        // Override index directory if set
        if let Ok(index_dir) = std::env::var("FCM_INDEX_DIR") {
            self.storage.index_dir = PathBuf::from(index_dir);
        }

        // Override packages directory if set
        if let Ok(packages_dir) = std::env::var("FCM_PACKAGES_DIR") {
            self.storage.packages_dir = PathBuf::from(packages_dir);
        }
    }

    /// Creates a default configuration file at the default location.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Configuration file created successfully
    /// * `Err` - If file already exists or cannot be written
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A configuration file already exists at the default location
    /// - The file cannot be written due to permissions or I/O issues
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use octofhir_canonical_manager::config::FcmConfig;
    ///
    /// FcmConfig::create_default_config()?;
    /// println!("Default configuration created");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn create_default_config() -> Result<()> {
        let config_path = Self::default_config_path();
        if config_path.exists() {
            return Err(ConfigError::ValidationFailed {
                message: "Configuration file already exists".to_string(),
            }
            .into());
        }

        let default_config = Self::default();
        let toml_content = toml::to_string_pretty(&default_config)?;

        std::fs::write(&config_path, toml_content)?;
        Ok(())
    }

    /// Saves the current configuration to the default configuration file.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Configuration saved successfully
    /// * `Err` - If file cannot be written
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use octofhir_canonical_manager::config::FcmConfig;
    ///
    /// let mut config = FcmConfig::load()?;
    /// config.add_package("hl7.fhir.us.core", "6.1.0", Some(1));
    /// config.save()?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn save(&self) -> Result<()> {
        let config_path = Self::default_config_path();
        let toml_content = toml::to_string_pretty(self)?;
        std::fs::write(&config_path, toml_content)?;
        Ok(())
    }

    /// Expand tilde paths to full paths
    fn expand_path(path: &Path) -> PathBuf {
        if path.starts_with("~") {
            if let Some(home_dir) = dirs::home_dir() {
                let path_str = path.to_string_lossy();
                let expanded = path_str.replacen("~", home_dir.to_string_lossy().as_ref(), 1);
                return PathBuf::from(expanded);
            }
        }
        path.to_path_buf()
    }

    /// Returns storage configuration with expanded paths.
    ///
    /// Expands tilde (`~`) references to the user's home directory.
    ///
    /// # Returns
    ///
    /// A new `StorageConfig` with all paths expanded to absolute paths.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use octofhir_canonical_manager::config::FcmConfig;
    ///
    /// let config = FcmConfig::load()?;
    /// let storage = config.get_expanded_storage_config();
    /// println!("Cache dir: {}", storage.cache_dir.display());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn get_expanded_storage_config(&self) -> StorageConfig {
        StorageConfig {
            cache_dir: Self::expand_path(&self.storage.cache_dir),
            index_dir: Self::expand_path(&self.storage.index_dir),
            packages_dir: Self::expand_path(&self.storage.packages_dir),
            max_cache_size: self.storage.max_cache_size.clone(),
        }
    }

    /// Create test configuration
    #[cfg(test)]
    pub fn test_config(temp_dir: &std::path::Path) -> Self {
        Self {
            registry: RegistryConfig::default(),
            packages: vec![],
            storage: StorageConfig {
                cache_dir: temp_dir.join("cache"),
                index_dir: temp_dir.join("index"),
                packages_dir: temp_dir.join("packages"),
                max_cache_size: "100MB".to_string(),
            },
        }
    }
}

impl FcmConfig {
    /// Adds a package to the configuration or updates an existing one.
    ///
    /// If a package with the same name already exists, it will be replaced.
    /// Packages are automatically sorted by priority after addition.
    ///
    /// # Arguments
    ///
    /// * `name` - The package name (e.g., "hl7.fhir.us.core")
    /// * `version` - The package version (e.g., "6.1.0")
    /// * `priority` - Optional priority (defaults to 1, lower numbers = higher priority)
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use octofhir_canonical_manager::config::FcmConfig;
    ///
    /// let mut config = FcmConfig::default();
    /// config.add_package("hl7.fhir.us.core", "6.1.0", Some(1));
    /// config.add_package("hl7.fhir.r4.core", "4.0.1", None); // Uses priority 1
    /// ```
    pub fn add_package(&mut self, name: &str, version: &str, priority: Option<u32>) {
        let package = PackageSpec {
            name: name.to_string(),
            version: version.to_string(),
            priority: priority.unwrap_or(1),
        };

        // Remove existing package with same name if it exists
        self.packages.retain(|p| p.name != name);

        // Add new package
        self.packages.push(package);

        // Sort by priority (lower numbers = higher priority)
        self.packages.sort_by_key(|p| p.priority);
    }

    /// Removes a package from the configuration by name.
    ///
    /// # Arguments
    ///
    /// * `name` - The package name to remove
    ///
    /// # Returns
    ///
    /// `true` if a package was removed, `false` if no package with that name was found.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use octofhir_canonical_manager::config::FcmConfig;
    ///
    /// let mut config = FcmConfig::default();
    /// config.add_package("test.package", "1.0.0", None);
    ///
    /// let removed = config.remove_package("test.package");
    /// assert!(removed);
    ///
    /// let not_found = config.remove_package("nonexistent.package");
    /// assert!(!not_found);
    /// ```
    pub fn remove_package(&mut self, name: &str) -> bool {
        let original_len = self.packages.len();
        self.packages.retain(|p| p.name != name);
        self.packages.len() < original_len
    }

    /// Retrieves a package specification by name.
    ///
    /// # Arguments
    ///
    /// * `name` - The package name to look up
    ///
    /// # Returns
    ///
    /// * `Some(&PackageSpec)` - If a package with the given name exists
    /// * `None` - If no package with that name is found
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use octofhir_canonical_manager::config::FcmConfig;
    ///
    /// let config = FcmConfig::load()?;
    /// if let Some(package) = config.get_package("hl7.fhir.us.core") {
    ///     println!("Found package version: {}", package.version);
    /// }
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn get_package(&self, name: &str) -> Option<&PackageSpec> {
        self.packages.iter().find(|p| p.name == name)
    }

    /// Returns a list of all configured package names.
    ///
    /// # Returns
    ///
    /// A vector containing the names of all configured packages.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use octofhir_canonical_manager::config::FcmConfig;
    ///
    /// let config = FcmConfig::load()?;
    /// let names = config.list_package_names();
    /// println!("Configured packages: {:?}", names);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn list_package_names(&self) -> Vec<String> {
        self.packages.iter().map(|p| p.name.clone()).collect()
    }
}

// Default value functions
fn default_registry_url() -> String {
    "https://fs.get-ig.org/pkgs/".to_string()
}
fn default_timeout() -> u64 {
    30
}
fn default_retry_attempts() -> u32 {
    3
}
fn default_priority() -> u32 {
    1
}
fn default_max_cache_size() -> String {
    "1GB".to_string()
}

/// Implement the Validate trait for FcmConfig
impl Validate for FcmConfig {
    type Error = ConfigError;

    fn validate(&self) -> std::result::Result<(), Self::Error> {
        // Validate registry URL
        url::Url::parse(&self.registry.url).map_err(|_| ConfigError::InvalidRegistryUrl {
            url: self.registry.url.clone(),
        })?;

        // Validate registry timeout
        if self.registry.timeout == 0 {
            return Err(ConfigError::ValidationFailed {
                message: "Registry timeout must be greater than 0".to_string(),
            });
        }

        // Validate retry attempts is reasonable
        if self.registry.retry_attempts == 0 {
            return Err(ConfigError::ValidationFailed {
                message: "Registry retry attempts must be greater than 0".to_string(),
            });
        }

        if self.registry.retry_attempts > 10 {
            return Err(ConfigError::ValidationFailed {
                message: "Registry retry attempts should not exceed 10".to_string(),
            });
        }

        // Validate packages
        for package in &self.packages {
            if package.name.is_empty() {
                return Err(ConfigError::InvalidPackageSpec {
                    spec: format!("{}@{}", package.name, package.version),
                });
            }

            if package.version.is_empty() {
                return Err(ConfigError::InvalidPackageSpec {
                    spec: format!("{}@{}", package.name, package.version),
                });
            }

            // Basic version format validation (semver-like)
            if !package
                .version
                .chars()
                .next()
                .unwrap_or('a')
                .is_ascii_digit()
            {
                return Err(ConfigError::InvalidPackageSpec {
                    spec: format!(
                        "{}@{} - version must start with a digit",
                        package.name, package.version
                    ),
                });
            }

            // Validate priority is reasonable
            if package.priority == 0 {
                return Err(ConfigError::ValidationFailed {
                    message: format!("Package {} priority must be greater than 0", package.name),
                });
            }

            if package.priority > 100 {
                return Err(ConfigError::ValidationFailed {
                    message: format!("Package {} priority should not exceed 100", package.name),
                });
            }
        }

        // Validate storage configuration
        self.storage.validate()?;

        Ok(())
    }
}

/// Implement the Validate trait for RegistryConfig
impl Validate for RegistryConfig {
    type Error = ConfigError;

    fn validate(&self) -> std::result::Result<(), Self::Error> {
        // Validate URL
        let parsed_url =
            url::Url::parse(&self.url).map_err(|_| ConfigError::InvalidRegistryUrl {
                url: self.url.clone(),
            })?;

        // Ensure URL uses HTTP or HTTPS
        if parsed_url.scheme() != "http" && parsed_url.scheme() != "https" {
            return Err(ConfigError::InvalidRegistryUrl {
                url: self.url.clone(),
            });
        }

        // Validate timeout
        if self.timeout == 0 {
            return Err(ConfigError::ValidationFailed {
                message: "Registry timeout must be greater than 0".to_string(),
            });
        }

        if self.timeout > 300 {
            return Err(ConfigError::ValidationFailed {
                message: "Registry timeout should not exceed 300 seconds".to_string(),
            });
        }

        Ok(())
    }
}

/// Implement the Validate trait for StorageConfig
impl Validate for StorageConfig {
    type Error = ConfigError;

    fn validate(&self) -> std::result::Result<(), Self::Error> {
        // Validate that paths are not empty
        if self.cache_dir.as_os_str().is_empty() {
            return Err(ConfigError::ValidationFailed {
                message: "Cache directory cannot be empty".to_string(),
            });
        }

        if self.index_dir.as_os_str().is_empty() {
            return Err(ConfigError::ValidationFailed {
                message: "Index directory cannot be empty".to_string(),
            });
        }

        if self.packages_dir.as_os_str().is_empty() {
            return Err(ConfigError::ValidationFailed {
                message: "Packages directory cannot be empty".to_string(),
            });
        }

        // Validate max cache size format (basic validation)
        if !self.max_cache_size.is_empty() {
            let size_str = self.max_cache_size.to_lowercase();
            let valid_suffixes = ["b", "kb", "mb", "gb", "tb"];
            let has_valid_suffix = valid_suffixes
                .iter()
                .any(|&suffix| size_str.ends_with(suffix));

            if !has_valid_suffix {
                return Err(ConfigError::ValidationFailed {
                    message: format!(
                        "Invalid cache size format: '{}'. Use formats like '1GB', '500MB', etc.",
                        self.max_cache_size
                    ),
                });
            }
        }

        Ok(())
    }
}

/// Implement the Validate trait for PackageSpec
impl Validate for PackageSpec {
    type Error = ConfigError;

    fn validate(&self) -> std::result::Result<(), Self::Error> {
        if self.name.is_empty() {
            return Err(ConfigError::InvalidPackageSpec {
                spec: format!("{}@{}", self.name, self.version),
            });
        }

        if self.version.is_empty() {
            return Err(ConfigError::InvalidPackageSpec {
                spec: format!("{}@{}", self.name, self.version),
            });
        }

        // Basic version format validation
        if !self.version.chars().next().unwrap_or('a').is_ascii_digit() {
            return Err(ConfigError::InvalidPackageSpec {
                spec: format!(
                    "{}@{} - version must start with a digit",
                    self.name, self.version
                ),
            });
        }

        // Validate priority
        if self.priority == 0 {
            return Err(ConfigError::ValidationFailed {
                message: format!("Package {} priority must be greater than 0", self.name),
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_default_config() {
        let config = FcmConfig::default();
        assert_eq!(config.registry.url, "https://fs.get-ig.org/pkgs/");
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation() {
        let temp_dir = TempDir::new().unwrap();
        let config = FcmConfig::test_config(temp_dir.path());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_validation_failures() {
        let mut config = FcmConfig::default();

        // Test invalid URL
        config.registry.url = "not-a-url".to_string();
        assert!(config.validate().is_err());

        // Test zero timeout
        config.registry.url = "https://example.com".to_string();
        config.registry.timeout = 0;
        assert!(config.validate().is_err());

        // Test invalid package name
        config.registry.timeout = 30;
        config.packages.push(PackageSpec {
            name: "".to_string(),
            version: "1.0.0".to_string(),
            priority: 1,
        });
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_package_management() {
        let mut config = FcmConfig::default();

        // Add package
        config.add_package("test.package", "1.0.0", Some(1));
        assert_eq!(config.packages.len(), 1);
        assert_eq!(config.get_package("test.package").unwrap().version, "1.0.0");

        // Update package (should replace existing)
        config.add_package("test.package", "2.0.0", Some(1));
        assert_eq!(config.packages.len(), 1);
        assert_eq!(config.get_package("test.package").unwrap().version, "2.0.0");

        // Remove package
        assert!(config.remove_package("test.package"));
        assert_eq!(config.packages.len(), 0);
        assert!(!config.remove_package("nonexistent"));
    }

    #[test]
    fn test_config_serialization() {
        let mut config = FcmConfig::default();
        config.add_package("hl7.fhir.us.core", "6.1.0", Some(1));

        let toml_content = toml::to_string_pretty(&config).unwrap();
        assert!(toml_content.contains("hl7.fhir.us.core"));
        assert!(toml_content.contains("6.1.0"));

        let deserialized: FcmConfig = toml::from_str(&toml_content).unwrap();
        assert_eq!(deserialized.packages.len(), 1);
        assert_eq!(deserialized.packages[0].name, "hl7.fhir.us.core");
    }

    #[test]
    fn test_path_expansion() {
        let path = PathBuf::from("~/test");
        let expanded = FcmConfig::expand_path(&path);

        if let Some(home_dir) = dirs::home_dir() {
            assert!(expanded.starts_with(home_dir));
            assert!(expanded.ends_with("test"));
        }
    }

    #[test]
    fn test_env_overrides() {
        unsafe {
            std::env::set_var("FCM_REGISTRY_URL", "https://custom-registry.com");
            std::env::set_var("FCM_CACHE_DIR", "/custom/cache");
        }

        let mut config = FcmConfig::default();
        config.apply_env_overrides();

        assert_eq!(config.registry.url, "https://custom-registry.com");
        assert_eq!(config.storage.cache_dir, PathBuf::from("/custom/cache"));

        // Clean up
        unsafe {
            std::env::remove_var("FCM_REGISTRY_URL");
            std::env::remove_var("FCM_CACHE_DIR");
        }
    }

    #[test]
    fn test_config_file_operations() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("fcm.toml");

        // Create a config
        let mut config = FcmConfig::default();
        config.add_package("test.package", "1.0.0", Some(1));

        // Save to file
        let toml_content = toml::to_string_pretty(&config).unwrap();
        fs::write(&config_path, toml_content).unwrap();

        // Load from file
        let loaded_config = FcmConfig::from_file(&config_path).unwrap();
        assert_eq!(loaded_config.packages.len(), 1);
        assert_eq!(loaded_config.packages[0].name, "test.package");
    }
}
