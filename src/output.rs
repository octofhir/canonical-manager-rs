//! Output formatting utilities for consistent user experience

use colored::Colorize;

/// Utilities for consistent user-facing output formatting.
///
/// Provides standardized output functions with colored icons and consistent
/// formatting for different types of messages. All output goes to stdout.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_manager::output::Output;
///
/// Output::success("Package installed successfully");
/// Output::error("Failed to download package");
/// Output::info("Starting package resolution...");
/// ```
pub struct Output;

impl Output {
    /// Prints a success message with a green checkmark icon.
    ///
    /// # Arguments
    ///
    /// * `msg` - The success message to display
    ///
    /// # Example
    ///
    /// ```rust
    /// use octofhir_canonical_manager::output::Output;
    ///
    /// Output::success("Package downloaded successfully");
    /// // Output: ✅ Package downloaded successfully
    /// ```
    pub fn success(msg: &str) {
        println!("{} {}", "✅".green(), msg);
    }

    /// Prints an error message with a red X icon.
    ///
    /// # Arguments
    ///
    /// * `msg` - The error message to display
    ///
    /// # Example
    ///
    /// ```rust
    /// use octofhir_canonical_manager::output::Output;
    ///
    /// Output::error("Failed to connect to registry");
    /// // Output: ❌ Failed to connect to registry
    /// ```
    pub fn error(msg: &str) {
        println!("{} {}", "❌".red(), msg);
    }

    /// Prints a warning message with a yellow warning icon.
    ///
    /// # Arguments
    ///
    /// * `msg` - The warning message to display
    ///
    /// # Example
    ///
    /// ```rust
    /// use octofhir_canonical_manager::output::Output;
    ///
    /// Output::warning("Package version not found, using latest");
    /// // Output: ⚠️  Package version not found, using latest
    /// ```
    pub fn warning(msg: &str) {
        println!("{} {}", "⚠️".yellow(), msg);
    }

    /// Prints an informational message with a blue info icon.
    ///
    /// # Arguments
    ///
    /// * `msg` - The information message to display
    ///
    /// # Example
    ///
    /// ```rust
    /// use octofhir_canonical_manager::output::Output;
    ///
    /// Output::info("Using registry: https://packages.fhir.org");
    /// // Output: ℹ️  Using registry: https://packages.fhir.org
    /// ```
    pub fn info(msg: &str) {
        println!("{} {}", "ℹ️".blue(), msg);
    }

    /// Prints result data without any prefix or icon.
    ///
    /// Used for outputting structured data or final results that should
    /// be easily parseable by other tools.
    ///
    /// # Arguments
    ///
    /// * `msg` - The result data to display
    ///
    /// # Example
    ///
    /// ```rust
    /// use octofhir_canonical_manager::output::Output;
    ///
    /// Output::result("http://hl7.org/fhir/Patient");
    /// // Output: http://hl7.org/fhir/Patient
    /// ```
    pub fn result(msg: &str) {
        println!("{msg}");
    }

    /// Prints a section header with bold cyan formatting.
    ///
    /// Used to separate different sections of output with a visual divider.
    ///
    /// # Arguments
    ///
    /// * `title` - The section title to display
    ///
    /// # Example
    ///
    /// ```rust
    /// use octofhir_canonical_manager::output::Output;
    ///
    /// Output::section("Installed Packages");
    /// // Output: (blank line)
    ///         // Installed Packages (in bold cyan)
    /// ```
    pub fn section(title: &str) {
        println!("\n{}", title.bold().cyan());
    }

    /// Prints a list item with bullet point indentation.
    ///
    /// Used for displaying items in a bulleted list format.
    ///
    /// # Arguments
    ///
    /// * `msg` - The list item text to display
    ///
    /// # Example
    ///
    /// ```rust
    /// use octofhir_canonical_manager::output::Output;
    ///
    /// Output::list_item("hl7.fhir.us.core@6.1.0");
    /// // Output:   • hl7.fhir.us.core@6.1.0
    /// ```
    pub fn list_item(msg: &str) {
        println!("  • {msg}");
    }
}

/// Utilities for progress and status reporting.
///
/// Provides functions for reporting operation progress and status updates.
/// All output goes to stderr to avoid interfering with structured data output.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_manager::output::Progress;
///
/// Progress::start("Downloading package");
/// Progress::step("Extracting archive");
/// Progress::status("Indexing resources");
/// Progress::complete("Package installed");
/// ```
pub struct Progress;

impl Progress {
    /// Prints the start of an operation with a spinning icon.
    ///
    /// # Arguments
    ///
    /// * `operation` - Description of the operation being started
    ///
    /// # Example
    ///
    /// ```rust
    /// use octofhir_canonical_manager::output::Progress;
    ///
    /// Progress::start("Downloading package");
    /// // Output: 🔄 Downloading package... (to stderr)
    /// ```
    pub fn start(operation: &str) {
        eprintln!("{} {}...", "🔄".cyan(), operation);
    }

    /// Prints a progress step with dimmed formatting.
    ///
    /// # Arguments
    ///
    /// * `msg` - Description of the current step
    ///
    /// # Example
    ///
    /// ```rust
    /// use octofhir_canonical_manager::output::Progress;
    ///
    /// Progress::step("Extracting files from archive");
    /// // Output:   Extracting files from archive (dimmed, to stderr)
    /// ```
    pub fn step(msg: &str) {
        eprintln!("  {}", msg.dimmed());
    }

    /// Prints operation completion with a green checkmark.
    ///
    /// # Arguments
    ///
    /// * `msg` - Completion message to display
    ///
    /// # Example
    ///
    /// ```rust
    /// use octofhir_canonical_manager::output::Progress;
    ///
    /// Progress::complete("Package extraction finished");
    /// // Output: ✅ Package extraction finished (to stderr)
    /// ```
    pub fn complete(msg: &str) {
        eprintln!("{} {}", "✅".green(), msg);
    }

    /// Prints an intermediate status update with a note icon.
    ///
    /// # Arguments
    ///
    /// * `msg` - Status message to display
    ///
    /// # Example
    ///
    /// ```rust
    /// use octofhir_canonical_manager::output::Progress;
    ///
    /// Progress::status("Found 150 FHIR resources");
    /// // Output: 📝 Found 150 FHIR resources (to stderr)
    /// ```
    pub fn status(msg: &str) {
        eprintln!("{} {}", "📝".blue(), msg);
    }
}

/// Formats a package name and version with consistent coloring.
///
/// Returns a formatted string with the package name in green and
/// version in bright white, following the pattern `name@version`.
///
/// # Arguments
///
/// * `name` - The package name
/// * `version` - The package version
///
/// # Returns
///
/// Formatted string with color codes for terminal display.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_manager::output::format_package;
///
/// let formatted = format_package("hl7.fhir.us.core", "6.1.0");
/// println!("{}", formatted);
/// // Output: hl7.fhir.us.core@6.1.0 (with colors)
/// ```
pub fn format_package(name: &str, version: &str) -> String {
    format!("{}@{}", name.green(), version.bright_white())
}

/// Formats a canonical URL with consistent coloring.
///
/// Returns the URL formatted in bright blue for better visibility
/// in terminal output.
///
/// # Arguments
///
/// * `url` - The canonical URL to format
///
/// # Returns
///
/// Formatted string with color codes for terminal display.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_manager::output::format_url;
///
/// let formatted = format_url("http://hl7.org/fhir/Patient");
/// println!("{}", formatted);
/// // Output: http://hl7.org/fhir/Patient (in bright blue)
/// ```
pub fn format_url(url: &str) -> String {
    url.bright_blue().to_string()
}

/// Formats a FHIR resource type with consistent coloring.
///
/// Returns the resource type formatted in yellow for easy identification
/// in terminal output.
///
/// # Arguments
///
/// * `resource_type` - The FHIR resource type to format
///
/// # Returns
///
/// Formatted string with color codes for terminal display.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_manager::output::format_resource_type;
///
/// let formatted = format_resource_type("Patient");
/// println!("{}", formatted);
/// // Output: Patient (in yellow)
/// ```
pub fn format_resource_type(resource_type: &str) -> String {
    resource_type.yellow().to_string()
}
