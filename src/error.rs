//! Error types for the FHIR Canonical Manager

use thiserror::Error;

/// Main result type used throughout the FHIR Canonical Manager library.
///
/// This is a convenience type alias that uses `FcmError` as the error type.
/// Most functions in this library return this result type.
///
/// # Example
///
/// ```rust,no_run
/// use octofhir_canonical_manager::error::Result;
///
/// fn example_operation() -> Result<String> {
///     Ok("Success".to_string())
/// }
/// ```
pub type Result<T> = std::result::Result<T, FcmError>;

/// Main error type for the FHIR Canonical Manager.
///
/// This enum encompasses all possible errors that can occur within the library,
/// providing a unified error handling interface with automatic conversions from
/// various underlying error types.
///
/// # Example
///
/// ```rust,no_run
/// use octofhir_canonical_manager::error::FcmError;
/// use octofhir_canonical_manager::error::ConfigError;
///
/// // Errors automatically convert to FcmError
/// let config_error = ConfigError::ValidationFailed {
///     message: "Invalid URL".to_string()
/// };
/// let fcm_error: FcmError = config_error.into();
/// ```
#[derive(Error, Debug)]
pub enum FcmError {
    #[error("Configuration error: {0}")]
    Config(#[from] ConfigError),

    #[error("Registry error: {0}")]
    Registry(#[from] RegistryError),

    #[error("Package error: {0}")]
    Package(#[from] PackageError),

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Resolution error: {0}")]
    Resolution(#[from] ResolutionError),

    #[error("Search error: {0}")]
    Search(#[from] SearchError),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Network error: {0}")]
    Network(#[from] reqwest::Error),

    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("TOML deserialize error: {0}")]
    TomlDe(#[from] toml::de::Error),

    #[error("TOML serialize error: {0}")]
    TomlSer(#[from] toml::ser::Error),
}

/// Errors related to configuration loading, parsing, and validation.
///
/// These errors occur when working with FCM configuration files,
/// environment variables, or configuration validation.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_manager::error::ConfigError;
///
/// let error = ConfigError::InvalidRegistryUrl {
///     url: "not-a-url".to_string()
/// };
/// println!("Configuration error: {}", error);
/// ```
#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("Invalid configuration file: {path}")]
    InvalidFile { path: std::path::PathBuf },

    #[error("Missing required field: {field}")]
    MissingField { field: String },

    #[error("Invalid registry URL: {url}")]
    InvalidRegistryUrl { url: String },

    #[error("Invalid package specification: {spec}")]
    InvalidPackageSpec { spec: String },

    #[error("Configuration validation failed: {message}")]
    ValidationFailed { message: String },

    #[error("Invalid configuration value for {key}: {value}")]
    InvalidValue { key: String, value: String },
}

/// Errors related to FHIR package registry operations.
///
/// These errors occur when downloading packages, querying package metadata,
/// or communicating with FHIR package registries.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_manager::error::RegistryError;
///
/// let error = RegistryError::PackageNotFound {
///     name: "example.package".to_string(),
///     version: "1.0.0".to_string(),
/// };
/// println!("Registry error: {}", error);
/// ```
#[derive(Error, Debug)]
pub enum RegistryError {
    #[error("Package not found: {name}@{version}")]
    PackageNotFound { name: String, version: String },

    #[error("Registry unavailable: {url}")]
    RegistryUnavailable { url: String },

    #[error("Authentication failed")]
    AuthenticationFailed,

    #[error("Rate limit exceeded")]
    RateLimitExceeded,

    #[error("Invalid package metadata: {message}")]
    InvalidMetadata { message: String },
}

/// Errors related to FHIR package processing and extraction.
///
/// These errors occur when extracting, parsing, or validating FHIR packages
/// after they have been downloaded from a registry.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_manager::error::PackageError;
///
/// let error = PackageError::ValidationFailed {
///     message: "Invalid package structure".to_string()
/// };
/// println!("Package error: {}", error);
/// ```
#[derive(Error, Debug)]
pub enum PackageError {
    #[error("Invalid package format: {message}")]
    InvalidFormat { message: String },

    #[error("Package extraction failed: {message}")]
    ExtractionFailed { message: String },

    #[error("Package validation failed: {message}")]
    ValidationFailed { message: String },

    #[error("Unsupported FHIR version: {version}")]
    UnsupportedFhirVersion { version: String },

    #[error("Missing package manifest")]
    MissingManifest,
}

/// Errors related to local storage and indexing operations.
///
/// These errors occur when working with the local storage system,
/// including database operations, file system access, and search indexing.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_manager::error::StorageError;
///
/// let error = StorageError::ResourceNotFound {
///     canonical_url: "http://example.com/Patient/1".to_string()
/// };
/// println!("Storage error: {}", error);
/// ```
#[derive(Error, Debug)]
pub enum StorageError {
    #[error("Index corruption detected: {message}")]
    IndexCorruption { message: String },

    #[error("Storage initialization failed: {message}")]
    InitializationFailed { message: String },

    #[error("Resource not found: {canonical_url}")]
    ResourceNotFound { canonical_url: String },

    #[error("Index update failed: {message}")]
    IndexUpdateFailed { message: String },

    #[error("Database error: {message}")]
    DatabaseError { message: String },

    #[error("Invalid resource: {message}")]
    InvalidResource { message: String },
}

/// Errors related to canonical URL resolution.
///
/// These errors occur when attempting to resolve canonical URLs to
/// FHIR resources, including cases where URLs cannot be found or
/// resolution is ambiguous.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_manager::error::ResolutionError;
///
/// let error = ResolutionError::CanonicalUrlNotFound {
///     url: "http://example.com/missing".to_string()
/// };
/// println!("Resolution error: {}", error);
/// ```
#[derive(Error, Debug)]
pub enum ResolutionError {
    #[error("Canonical URL not found: {url}")]
    CanonicalUrlNotFound { url: String },

    #[error("Ambiguous resolution for: {url}")]
    AmbiguousResolution { url: String },

    #[error("Invalid canonical URL: {url}")]
    InvalidCanonicalUrl { url: String },

    #[error("Resolution timeout for: {url}")]
    ResolutionTimeout { url: String },
}

/// Errors related to search operations.
///
/// These errors occur when performing searches across FHIR resources,
/// including query parsing errors and search timeouts.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_manager::error::SearchError;
///
/// let error = SearchError::InvalidQuery {
///     message: "Invalid regex pattern".to_string()
/// };
/// println!("Search error: {}", error);
/// ```
#[derive(Error, Debug)]
pub enum SearchError {
    #[error("Invalid search query: {message}")]
    InvalidQuery { message: String },

    #[error("Search timeout")]
    SearchTimeout,

    #[error("Search index unavailable")]
    IndexUnavailable,

    #[error("Too many search results: {count}")]
    TooManyResults { count: usize },
}

/// Trait for validating configuration and data structures.
///
/// This trait provides a common interface for validating various
/// configuration structures and ensuring they contain valid data.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_manager::error::Validate;
/// use octofhir_canonical_manager::error::ConfigError;
///
/// struct MyConfig {
///     url: String,
/// }
///
/// impl Validate for MyConfig {
///     type Error = ConfigError;
///     
///     fn validate(&self) -> Result<(), Self::Error> {
///         if self.url.is_empty() {
///             Err(ConfigError::ValidationFailed {
///                 message: "URL cannot be empty".to_string()
///             })
///         } else {
///             Ok(())
///         }
///     }
/// }
/// ```
pub trait Validate {
    type Error;
    fn validate(&self) -> std::result::Result<(), Self::Error>;
}

/// Trait for adding context to errors.
///
/// This trait provides methods to add contextual information to errors,
/// making them more informative for debugging and user feedback.
///
/// # Example
///
/// ```rust,no_run
/// use octofhir_canonical_manager::error::{ErrorContext, Result};
///
/// fn example_operation() -> Result<String> {
///     std::fs::read_to_string("config.toml")
///         .with_context("Failed to read configuration file")
///         .map_err(Into::into)
/// }
/// ```
pub trait ErrorContext<T> {
    fn with_context<C>(self, context: C) -> std::result::Result<T, FcmError>
    where
        C: std::fmt::Display + Send + Sync + 'static;

    fn with_context_lazy<C, F>(self, f: F) -> std::result::Result<T, FcmError>
    where
        C: std::fmt::Display + Send + Sync + 'static,
        F: FnOnce() -> C;
}

impl<T, E> ErrorContext<T> for std::result::Result<T, E>
where
    E: Into<FcmError>,
{
    fn with_context<C>(self, _context: C) -> std::result::Result<T, FcmError>
    where
        C: std::fmt::Display + Send + Sync + 'static,
    {
        self.map_err(|e| {
            // For now, just return the original error
            // In the future, we could implement error chaining
            e.into()
        })
    }

    fn with_context_lazy<C, F>(self, f: F) -> std::result::Result<T, FcmError>
    where
        C: std::fmt::Display + Send + Sync + 'static,
        F: FnOnce() -> C,
    {
        self.map_err(|e| {
            let _context = f();

            // For now, just return the original error
            // In the future, we could implement error chaining
            e.into()
        })
    }
}
