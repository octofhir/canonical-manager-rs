//! CLI error handling with recovery suggestions

use crate::error::*;
use colored::Colorize;
use std::error::Error;

/// Trait for providing user-friendly error messages with recovery suggestions
pub trait CliError {
    fn user_message(&self) -> String;
    fn recovery_suggestion(&self) -> Option<String>;
}

impl CliError for FcmError {
    fn user_message(&self) -> String {
        match self {
            FcmError::Config(e) => format!("{} Configuration Error: {}", "⚙️".red(), e),
            FcmError::Registry(e) => format!("{} Registry Error: {}", "🌐".red(), e),
            FcmError::Package(e) => format!("{} Package Error: {}", "📦".red(), e),
            FcmError::Storage(e) => format!("{} Storage Error: {}", "💾".red(), e),
            FcmError::Resolution(e) => format!("{} Resolution Error: {}", "🔍".red(), e),
            FcmError::Search(e) => format!("{} Search Error: {}", "🔎".red(), e),
            FcmError::Io(e) => format!("{} File System Error: {}", "📁".red(), e),
            FcmError::Network(e) => format!("{} Network Error: {}", "🌐".red(), e),
            FcmError::Json(e) => format!("{} JSON Error: {}", "📝".red(), e),
            FcmError::TomlDe(e) => format!("{} TOML Parse Error: {}", "📝".red(), e),
            FcmError::TomlSer(e) => format!("{} TOML Write Error: {}", "📝".red(), e),
        }
    }

    fn recovery_suggestion(&self) -> Option<String> {
        match self {
            FcmError::Config(e) => e.recovery_suggestion(),
            FcmError::Registry(e) => e.recovery_suggestion(),
            FcmError::Package(e) => e.recovery_suggestion(),
            FcmError::Storage(e) => e.recovery_suggestion(),
            FcmError::Resolution(e) => e.recovery_suggestion(),
            FcmError::Search(e) => e.recovery_suggestion(),
            FcmError::Network(e) => {
                if e.is_timeout() {
                    Some(format!(
                        "{} Request timed out - this may be due to slow network or server issues\n{} Try increasing the timeout in configuration or try again later\n{} Check if you're behind a proxy that requires configuration",
                        "💡".yellow(),
                        "💡".yellow(),
                        "💡".yellow()
                    ))
                } else if e.is_connect() {
                    Some(format!(
                        "{} Cannot connect to the registry server\n{} Check your internet connection\n{} Verify the registry URL is correct in your configuration\n{} Try using a different registry as a fallback",
                        "💡".yellow(),
                        "💡".yellow(),
                        "💡".yellow(),
                        "💡".yellow()
                    ))
                } else {
                    Some(format!(
                        "{} Network error occurred: {}\n{} Check your internet connection and try again\n{} If behind a proxy, configure proxy settings",
                        "💡".yellow(),
                        e,
                        "💡".yellow(),
                        "💡".yellow()
                    ))
                }
            }
            FcmError::Io(e) if e.kind() == std::io::ErrorKind::PermissionDenied => Some(format!(
                "{} Check file permissions or run with appropriate privileges",
                "💡".yellow()
            )),
            _ => None,
        }
    }
}

impl CliError for ConfigError {
    fn user_message(&self) -> String {
        self.to_string()
    }

    fn recovery_suggestion(&self) -> Option<String> {
        match self {
            ConfigError::InvalidFile { path } => Some(format!(
                "{} Check that {} exists and is a valid TOML file\n{} Run 'fcm init' to create a new configuration",
                "💡".yellow(),
                path.display().to_string().bright_white(),
                "💡".yellow()
            )),
            ConfigError::MissingField { field } => Some(format!(
                "{} Add the '{}' field to your configuration file\n{} See the documentation for configuration examples",
                "💡".yellow(),
                field.bright_white(),
                "💡".yellow()
            )),
            ConfigError::InvalidRegistryUrl { url } => Some(format!(
                "{} Check the registry URL: {}\n{} The URL should start with http:// or https://",
                "💡".yellow(),
                url.bright_white(),
                "💡".yellow()
            )),
            ConfigError::InvalidPackageSpec { spec: _ } => Some(format!(
                "{} Package specifications should be in the format: name@version\n{} Example: hl7.fhir.us.core@6.1.0",
                "💡".yellow(),
                "💡".yellow()
            )),
            _ => None,
        }
    }
}

impl CliError for RegistryError {
    fn user_message(&self) -> String {
        self.to_string()
    }

    fn recovery_suggestion(&self) -> Option<String> {
        match self {
            RegistryError::PackageNotFound {
                name: _,
                version: _,
            } => Some(format!(
                "{} Check the package name and version are correct\n{} Search available packages at https://fs.get-ig.org/pkgs/",
                "💡".yellow(),
                "💡".yellow()
            )),
            RegistryError::RegistryUnavailable { url } => Some(format!(
                "{} Check if {} is accessible\n{} Try again later or check your network settings",
                "💡".yellow(),
                url.bright_white(),
                "💡".yellow()
            )),
            RegistryError::AuthenticationFailed => Some(format!(
                "{} Check your registry credentials\n{} Some registries require authentication tokens",
                "💡".yellow(),
                "💡".yellow()
            )),
            RegistryError::RateLimitExceeded => Some(format!(
                "{} You've exceeded the registry's rate limit\n{} Wait a few minutes before trying again",
                "💡".yellow(),
                "💡".yellow()
            )),
            _ => None,
        }
    }
}

impl CliError for PackageError {
    fn user_message(&self) -> String {
        self.to_string()
    }

    fn recovery_suggestion(&self) -> Option<String> {
        match self {
            PackageError::InvalidFormat { .. } => Some(format!(
                "{} The package appears to be corrupted\n{} Try downloading it again",
                "💡".yellow(),
                "💡".yellow()
            )),
            PackageError::UnsupportedFhirVersion { version } => Some(format!(
                "{} This tool supports FHIR R4 and R5\n{} The package uses FHIR version {}",
                "💡".yellow(),
                "💡".yellow(),
                version.bright_white()
            )),
            _ => None,
        }
    }
}

impl CliError for StorageError {
    fn user_message(&self) -> String {
        self.to_string()
    }

    fn recovery_suggestion(&self) -> Option<String> {
        match self {
            StorageError::IndexCorruption { .. } => Some(format!(
                "{} The search index appears to be corrupted\n{} Run 'fcm update' to rebuild the index",
                "💡".yellow(),
                "💡".yellow()
            )),
            StorageError::ResourceNotFound { canonical_url } => Some(format!(
                "{} No resource found with URL: {}\n{} Check if the package containing this resource is installed",
                "💡".yellow(),
                canonical_url.bright_white(),
                "💡".yellow()
            )),
            _ => None,
        }
    }
}

impl CliError for ResolutionError {
    fn user_message(&self) -> String {
        self.to_string()
    }

    fn recovery_suggestion(&self) -> Option<String> {
        match self {
            ResolutionError::CanonicalUrlNotFound { url } => Some(format!(
                "{} No resource found for: {}\n{} Install the package containing this resource",
                "💡".yellow(),
                url.bright_white(),
                "💡".yellow()
            )),
            ResolutionError::AmbiguousResolution { url } => Some(format!(
                "{} Multiple resources found for: {}\n{} Use package priorities to resolve conflicts",
                "💡".yellow(),
                url.bright_white(),
                "💡".yellow()
            )),
            ResolutionError::InvalidCanonicalUrl { url } => Some(format!(
                "{} Invalid canonical URL format: {}\n{} Canonical URLs should follow FHIR specifications",
                "💡".yellow(),
                url.bright_white(),
                "💡".yellow()
            )),
            _ => None,
        }
    }
}

impl CliError for SearchError {
    fn user_message(&self) -> String {
        self.to_string()
    }

    fn recovery_suggestion(&self) -> Option<String> {
        match self {
            SearchError::InvalidQuery { .. } => Some(format!(
                "{} Check your search query syntax\n{} Use quotes for phrase searches",
                "💡".yellow(),
                "💡".yellow()
            )),
            SearchError::IndexUnavailable => Some(format!(
                "{} The search index is not available\n{} Run 'fcm update' to rebuild the index",
                "💡".yellow(),
                "💡".yellow()
            )),
            SearchError::TooManyResults { count } => Some(format!(
                "{} Your search returned {} results\n{} Try a more specific search query or use --limit",
                "💡".yellow(),
                count.to_string().bright_white(),
                "💡".yellow()
            )),
            _ => None,
        }
    }
}

/// Get exit code for error type
pub fn get_exit_code(error: &FcmError) -> i32 {
    match error {
        FcmError::Config(_) => 5,
        FcmError::Registry(RegistryError::PackageNotFound { .. }) => 4,
        FcmError::Network(_) | FcmError::Registry(RegistryError::RegistryUnavailable { .. }) => 3,
        _ => 1,
    }
}

/// Print error with recovery suggestions and comprehensive logging
pub fn print_error_with_suggestions(error: &FcmError) {
    // Log the error for debugging
    log_error_details(error);

    // Print user-friendly error message
    eprintln!("{}", error.user_message());

    if let Some(suggestion) = error.recovery_suggestion() {
        eprintln!("\n{suggestion}");
    }

    // Add context-specific help
    print_contextual_help(error);

    eprintln!("\n{} Run 'fcm --help' for more information", "ℹ️".blue());

    // In verbose mode or if FCM_DEBUG is set, show error chain
    if std::env::var("FCM_DEBUG").is_ok()
        || std::env::var("RUST_LOG")
            .map(|s| s.contains("debug"))
            .unwrap_or(false)
    {
        eprintln!("\n{} Debug information:", "🔧".dimmed());
        print_error_chain(error, 0);
    }
}

/// Log error details for debugging and monitoring
fn log_error_details(error: &FcmError) {
    // Create structured error context
    let error_type = match error {
        FcmError::Config(_) => "configuration",
        FcmError::Registry(_) => "registry",
        FcmError::Package(_) => "package",
        FcmError::Storage(_) => "storage",
        FcmError::Resolution(_) => "resolution",
        FcmError::Search(_) => "search",
        FcmError::Io(_) => "io",
        FcmError::Network(_) => "network",
        FcmError::Json(_) => "json",
        FcmError::TomlDe(_) => "toml_deserialize",
        FcmError::TomlSer(_) => "toml_serialize",
    };

    tracing::error!(
        error_type = error_type,
        error_message = %error,
        "FCM operation failed"
    );

    // Log specific error details based on type
    match error {
        FcmError::Registry(RegistryError::PackageNotFound { name, version }) => {
            tracing::warn!(
                package_name = name,
                package_version = version,
                "Package not found in registry"
            );
        }
        FcmError::Network(e) => {
            tracing::error!(
                is_timeout = e.is_timeout(),
                is_connect = e.is_connect(),
                is_request = e.is_request(),
                "Network error details"
            );
        }
        _ => {}
    }
}

/// Print contextual help based on error type
fn print_contextual_help(error: &FcmError) {
    match error {
        FcmError::Config(_) => {
            eprintln!("\n{} Configuration help:", "📖".blue());
            eprintln!("  • Check your fcm.toml file for syntax errors");
            eprintln!("  • Use 'fcm config show' to view current configuration");
            eprintln!("  • Use 'fcm init' to create a new configuration file");
        }
        FcmError::Registry(RegistryError::PackageNotFound { .. }) => {
            eprintln!("\n{} Package discovery help:", "📖".blue());
            eprintln!("  • Search for packages at https://fs.get-ig.org/pkgs/");
            eprintln!("  • Check if the package name and version are correct");
            eprintln!("  • Some packages may only be available in specific registries");
        }
        FcmError::Network(_) => {
            eprintln!("\n{} Network troubleshooting:", "📖".blue());
            eprintln!("  • Test connectivity: curl -I https://fs.get-ig.org/pkgs");
            eprintln!("  • Check proxy settings if behind corporate firewall");
            eprintln!("  • Try with --verbose flag for detailed logging");
        }
        _ => {}
    }
}

/// Print error chain for debugging
fn print_error_chain(error: &FcmError, depth: usize) {
    let indent = "  ".repeat(depth);
    eprintln!("{indent}• {error}");

    // Print source chain if available
    let mut source = error.source();
    let mut chain_depth = depth + 1;
    while let Some(err) = source {
        let chain_indent = "  ".repeat(chain_depth);
        eprintln!("{chain_indent}↳ {err}");
        source = err.source();
        chain_depth += 1;

        // Prevent infinite loops
        if chain_depth > 10 {
            eprintln!("{}↳ ...", "  ".repeat(chain_depth));
            break;
        }
    }
}
