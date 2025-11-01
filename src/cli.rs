//! CLI interface for FCM

use crate::CanonicalManager;
use crate::SearchParameterInfo;
use crate::output::{Output, Progress, format_package};
use crate::search::SearchResult;
use clap::{Parser, Subcommand};
use serde_json;
use std::fs;
use std::path::PathBuf;

#[cfg(feature = "cli")]
use colored::Colorize;

/// Progress context for tracking multi-level package installation
#[cfg(feature = "cli")]
pub struct ProgressContext {
    pub current: u64,
    pub total: u64,
}

/// Progress implementation for streaming downloads
#[cfg(feature = "cli")]
pub struct StreamProgress {
    last_percent_shown: std::sync::atomic::AtomicU64,
}

#[cfg(feature = "cli")]
impl Default for StreamProgress {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamProgress {
    pub fn new() -> Self {
        Self {
            last_percent_shown: std::sync::atomic::AtomicU64::new(0),
        }
    }
}

#[cfg(feature = "cli")]
impl crate::registry::DownloadProgress for StreamProgress {
    fn on_start(&self, total: Option<u64>) {
        if let Some(total) = total {
            Progress::step(&format!("Starting download: {total} bytes"));
        } else {
            Progress::step("Starting download: unknown size");
        }
    }

    fn on_progress(&self, downloaded: u64, total: Option<u64>) {
        // Show text-based progress updates every 25% for large downloads
        if let Some(total) = total {
            let percent = (downloaded as f64 / total as f64 * 100.0) as u64;
            let last_shown = self
                .last_percent_shown
                .load(std::sync::atomic::Ordering::Relaxed);
            if percent > last_shown && percent.is_multiple_of(25) && downloaded < total {
                Progress::step(&format!(
                    "Progress: {percent}% ({downloaded} / {total} bytes)"
                ));
                self.last_percent_shown
                    .store(percent, std::sync::atomic::Ordering::Relaxed);
            }
        }
    }

    fn on_complete(&self) {
        // Completion handled by caller
    }
}

#[cfg(feature = "cli")]
impl Default for ProgressContext {
    fn default() -> Self {
        Self::new()
    }
}

impl ProgressContext {
    pub fn new() -> Self {
        Self {
            current: 0,
            total: 0,
        }
    }

    pub fn set_total(&mut self, total: u64, message: &str) {
        self.total = total;
        self.current = 0;
        Progress::status(&format!("{message} (0/{total})"));
    }

    pub fn create_stream_progress(&self) -> StreamProgress {
        StreamProgress::new()
    }

    pub fn increment(&mut self, message: &str) {
        self.current += 1;
        Progress::step(&format!("{} ({}/{})", message, self.current, self.total));
    }

    pub fn finish(&self, message: &str) {
        Progress::complete(message);
    }
}

/// FHIR Canonical Manager CLI
#[derive(Parser, Debug)]
#[command(name = "fcm")]
#[command(about = "FHIR Canonical Manager - Library-first FHIR package management")]
#[command(version = "0.1.0")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,

    #[arg(short, long)]
    pub config: Option<PathBuf>,

    #[arg(short, long)]
    pub verbose: bool,
}

/// CLI commands
#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Initialize FCM configuration
    Init,
    /// Install FHIR packages
    Install(InstallCommand),
    /// Remove installed packages
    Remove(RemoveCommand),
    /// List installed packages
    List,
    /// Search for resources
    Search(SearchCommand),
    /// Get search parameters for a resource type
    SearchParams(SearchParamsCommand),
    /// Resolve canonical URLs
    Resolve(ResolveCommand),
    /// Update package indexes
    Update,
    /// Manage configuration
    Config(ConfigCommand),
}

/// Install command
#[derive(Parser, Debug)]
pub struct InstallCommand {
    /// Package specification (name@version)
    pub package: Option<String>,

    /// Install all packages from fcm.toml
    #[arg(long)]
    pub all: bool,
}

/// Remove command
#[derive(Parser, Debug)]
pub struct RemoveCommand {
    /// Package name to remove
    pub package: String,
}

/// Search command
#[derive(Parser, Debug)]
pub struct SearchCommand {
    /// Search query
    pub query: Option<String>,

    /// Filter by resource type
    #[arg(long)]
    pub resource_type: Option<String>,

    /// Filter by package
    #[arg(long)]
    pub package: Option<String>,

    /// Canonical URL pattern
    #[arg(long)]
    pub canonical: Option<String>,

    /// Maximum number of results
    #[arg(long, default_value = "10")]
    pub limit: usize,

    /// Output format (text, json)
    #[arg(long, default_value = "text")]
    pub format: String,
}

/// Search parameters command
#[derive(Parser, Debug)]
pub struct SearchParamsCommand {
    /// Resource type to get search parameters for
    pub resource_type: String,

    /// Output format (json, table, csv)
    #[arg(long, default_value = "table")]
    pub format: String,
}

/// Resolve command
#[derive(Parser, Debug)]
pub struct ResolveCommand {
    /// Canonical URL to resolve
    pub url: Option<String>,

    /// Batch resolve from file
    #[arg(long)]
    pub batch: Option<PathBuf>,

    /// Output format (text, json)
    #[arg(long, default_value = "text")]
    pub format: String,

    /// Show resolution path details
    #[arg(long)]
    pub details: bool,
}

/// Config command
#[derive(Parser, Debug)]
pub struct ConfigCommand {
    #[command(subcommand)]
    pub action: ConfigAction,
}

/// Config actions
#[derive(Subcommand, Debug)]
pub enum ConfigAction {
    /// Show current configuration
    Show,
    /// Set configuration value
    Set { key: String, value: String },
    /// Reset to defaults
    Reset,
}

/// Run the CLI
pub async fn run() -> crate::Result<()> {
    let cli = Cli::parse();

    // Initialize logging with structured format and error context
    let log_level = if cli.verbose { "debug" } else { "info" };
    let env_filter = format!("octofhir_canonical_manager={log_level},octofhir-fcm={log_level}");

    tracing_subscriber::fmt()
        .with_env_filter(&env_filter)
        .with_target(cli.verbose)
        .with_thread_ids(cli.verbose)
        .with_file(cli.verbose)
        .with_line_number(cli.verbose)
        .init();

    tracing::info!(
        "Starting FHIR Canonical Manager CLI v{}",
        env!("CARGO_PKG_VERSION")
    );
    if cli.verbose {
        tracing::debug!("Verbose logging enabled");
        tracing::debug!("CLI arguments: {:?}", cli);
    }

    // Handle commands
    let result = match cli.command {
        Commands::Init => handle_init(&cli).await,
        Commands::Install(ref cmd) => handle_install(&cli, cmd).await,
        Commands::Remove(ref cmd) => handle_remove(&cli, cmd).await,
        Commands::List => handle_list(&cli).await,
        Commands::Search(ref cmd) => handle_search(&cli, cmd).await,
        Commands::SearchParams(ref cmd) => handle_search_params(&cli, cmd).await,
        Commands::Resolve(ref cmd) => handle_resolve(&cli, cmd).await,
        Commands::Update => handle_update(&cli).await,
        Commands::Config(ref cmd) => handle_config(&cli, cmd).await,
    };

    // Handle errors with suggestions
    if let Err(e) = &result {
        crate::cli_error::print_error_with_suggestions(e);
        let exit_code = crate::cli_error::get_exit_code(e);
        std::process::exit(exit_code);
    }

    result
}

/// Handle init command
async fn handle_init(cli: &Cli) -> crate::Result<()> {
    Progress::start("Initializing FCM configuration");

    let config_path = cli
        .config
        .clone()
        .unwrap_or_else(|| std::env::current_dir().unwrap().join("fcm.toml"));

    if config_path.exists() {
        Output::warning(&format!(
            "Configuration file already exists: {}",
            config_path.display()
        ));
        return Ok(());
    }

    // Create default configuration
    let default_config = r#"# FHIR Canonical Manager Configuration
# This is a sample configuration file

[registry]
timeout = 30
retry_attempts = 3

[[packages]]
name = "hl7.fhir.r4.core"
version = "4.0.1"
priority = 1

[storage]
cache_dir = ".fcm/cache"
index_dir = ".fcm/index"
packages_dir = ".fcm/packages"
max_cache_size = "1GB"
"#;

    fs::write(&config_path, default_config)?;
    Output::success(&format!(
        "Created configuration file: {}",
        config_path.display().to_string().cyan()
    ));
    Output::info("Edit the file to customize your settings");

    Ok(())
}

/// Handle install command
async fn handle_install(cli: &Cli, cmd: &InstallCommand) -> crate::Result<()> {
    let config = load_config(cli).await?;
    let manager = CanonicalManager::new(config).await?;

    if cmd.all {
        Progress::start("Installing all packages from configuration");
        let config = load_config(cli).await?;

        let mut current = 0;
        let total = config.packages.len();

        for package_config in &config.packages {
            current += 1;
            Progress::step(&format!(
                "Installing {} ({}/{})",
                format_package(&package_config.name, &package_config.version),
                current,
                total
            ));

            match manager
                .install_package(&package_config.name, &package_config.version)
                .await
            {
                Ok(_) => {
                    Output::success(&format!(
                        "Successfully installed {} (including dependencies)",
                        format_package(&package_config.name, &package_config.version)
                    ));
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
        Output::success("All packages installed successfully!");
    } else if let Some(package_spec) = &cmd.package {
        Progress::start(&format!("Installing package: {package_spec}"));

        let (name, version) = parse_package_spec(package_spec)?;

        Progress::step("Counting dependencies");
        let total_packages = manager.count_packages_to_install(&name, &version).await?;

        Progress::step(&format!(
            "Installing {total_packages} packages (including dependencies)"
        ));

        // Create progress context for download tracking
        let mut progress = ProgressContext::new();
        progress.set_total(
            total_packages as u64,
            &format!("Installing {total_packages} packages"),
        );

        match manager
            .install_package_with_progress(&name, &version, &mut progress)
            .await
        {
            Ok(_) => {
                progress.finish("All packages installed successfully!");
                Output::success(&format!(
                    "Successfully installed {} and {} dependencies",
                    package_spec,
                    total_packages - 1
                ));
            }
            Err(e) => {
                Output::error("Installation failed");
                return Err(e);
            }
        }
    } else {
        Output::error("Please specify a package to install or use --all");
        std::process::exit(1);
    }

    Ok(())
}

/// Handle remove command
async fn handle_remove(cli: &Cli, cmd: &RemoveCommand) -> crate::Result<()> {
    let config = load_config(cli).await?;
    let manager = CanonicalManager::new(config).await?;

    Progress::start(&format!("Removing package: {}", cmd.package.cyan()));

    let (name, version) = parse_package_spec(&cmd.package)?;

    match manager.remove_package(&name, &version).await {
        Ok(_) => Output::success(&format!("Successfully removed {}", cmd.package.green())),
        Err(e) => {
            return Err(e);
        }
    }

    Ok(())
}

/// Handle list command
async fn handle_list(cli: &Cli) -> crate::Result<()> {
    let config = load_config(cli).await?;
    let manager = CanonicalManager::new(config).await?;

    Output::section("Installed packages:");

    let packages = manager.list_packages().await?;

    if packages.is_empty() {
        Output::info("No packages installed");
        return Ok(());
    }

    for package in packages {
        Output::list_item(&format!("📦 {}", package.bright_white()));
    }

    Ok(())
}

/// Handle search command
async fn handle_search(cli: &Cli, cmd: &SearchCommand) -> crate::Result<()> {
    let config = load_config(cli).await?;
    let manager = CanonicalManager::new(config).await?;

    Progress::start("Searching for resources");

    let mut search_builder = manager.search().await;

    if let Some(query) = &cmd.query {
        search_builder = search_builder.text(query);
    }

    if let Some(resource_type) = &cmd.resource_type {
        search_builder = search_builder.resource_type(resource_type);
    }

    if let Some(package) = &cmd.package {
        search_builder = search_builder.package(package);
    }

    if let Some(canonical) = &cmd.canonical {
        search_builder = search_builder.canonical_pattern(canonical);
    }

    search_builder = search_builder.limit(cmd.limit);

    let results = search_builder.execute().await?;

    match cmd.format.as_str() {
        "json" => print_search_results_json(&results)?,
        _ => print_search_results_text(&results),
    }

    Ok(())
}

/// Handle search params command
async fn handle_search_params(cli: &Cli, cmd: &SearchParamsCommand) -> crate::Result<()> {
    let config = load_config(cli).await?;
    let manager = CanonicalManager::new(config).await?;

    Progress::start(&format!(
        "Getting search parameters for resource type: {}",
        cmd.resource_type
    ));

    let search_params = manager.get_search_parameters(&cmd.resource_type).await?;

    Progress::complete(&format!("Found {} search parameters", search_params.len()));

    match cmd.format.as_str() {
        "json" => print_search_params_json(&search_params)?,
        "csv" => print_search_params_csv(&search_params)?,
        _ => print_search_params_table(&search_params),
    }

    Ok(())
}

/// Handle resolve command
async fn handle_resolve(cli: &Cli, cmd: &ResolveCommand) -> crate::Result<()> {
    let config = load_config(cli).await?;
    let manager = CanonicalManager::new(config).await?;

    if let Some(batch_file) = &cmd.batch {
        Progress::start(&format!(
            "Batch resolving URLs from: {}",
            batch_file.display().to_string().bright_white()
        ));

        let content = fs::read_to_string(batch_file)?;
        let urls: Vec<String> = content
            .lines()
            .map(|line| line.trim().to_string())
            .filter(|line| !line.is_empty())
            .collect();

        let results = manager.batch_resolve(&urls).await?;

        match cmd.format.as_str() {
            "json" => print_resolve_results_json(&results, cmd.details)?,
            _ => print_resolve_results_text(&results, cmd.details),
        }
    } else if let Some(url) = &cmd.url {
        Progress::start(&format!("Resolving URL: {}", url.bright_white()));

        let result = manager.resolve(url).await?;

        match cmd.format.as_str() {
            "json" => print_resolve_result_json(&result, cmd.details)?,
            _ => print_resolve_result_text(&result, cmd.details),
        }
    } else {
        Output::error("Please specify a URL to resolve or use --batch");
        std::process::exit(1);
    }

    Ok(())
}

/// Handle update command
async fn handle_update(cli: &Cli) -> crate::Result<()> {
    let config = load_config(cli).await?;

    Progress::start("Checking package indexes");
    Progress::step("Verifying SQLite indexes");

    let _manager = CanonicalManager::new(config).await?;

    // SQLite automatically maintains all indexes, no rebuild needed
    Progress::step("SQLite indexes are automatically maintained");

    Output::success("Package indexes verified - SQLite maintains them automatically!");

    Ok(())
}

/// Handle config command
async fn handle_config(cli: &Cli, cmd: &ConfigCommand) -> crate::Result<()> {
    match &cmd.action {
        ConfigAction::Show => {
            Output::section("Current configuration:");

            let config = load_config(cli).await?;
            let config_toml = toml::to_string_pretty(&config)?;
            Output::result(&config_toml.bright_white().to_string());
        }
        ConfigAction::Set { key, value } => {
            handle_config_set(cli, key, value).await?;
        }
        ConfigAction::Reset => {
            handle_config_reset(cli).await?;
        }
    }

    Ok(())
}

/// Handle config set command
async fn handle_config_set(cli: &Cli, key: &str, value: &str) -> crate::Result<()> {
    let config_path = cli
        .config
        .clone()
        .unwrap_or_else(|| std::env::current_dir().unwrap().join("fcm.toml"));

    if !config_path.exists() {
        Output::error(&format!(
            "Configuration file not found: {}",
            config_path.display()
        ));
        Output::info("Run 'fcm init' to create a configuration file first");
        return Ok(());
    }

    // Load current config
    let mut config = load_config(cli).await?;

    // Parse and set the configuration value
    match key {
        "registry.timeout" => {
            let timeout: u64 = value.parse().map_err(|_| {
                crate::error::FcmError::Config(crate::error::ConfigError::InvalidValue {
                    key: key.to_string(),
                    value: value.to_string(),
                })
            })?;
            config.registry.timeout = timeout;
        }
        "registry.retry_attempts" => {
            let attempts: u32 = value.parse().map_err(|_| {
                crate::error::FcmError::Config(crate::error::ConfigError::InvalidValue {
                    key: key.to_string(),
                    value: value.to_string(),
                })
            })?;
            config.registry.retry_attempts = attempts;
        }
        "storage.max_cache_size" => {
            config.storage.max_cache_size = value.to_string();
        }
        _ => {
            Output::error(&format!("Unknown configuration key: {key}"));
            Output::info("Available keys:");
            Output::list_item("registry.timeout");
            Output::list_item("registry.retry_attempts");
            Output::list_item("storage.max_cache_size");
            return Ok(());
        }
    }

    // Write updated config back to file
    let config_toml = toml::to_string_pretty(&config)?;
    fs::write(&config_path, config_toml)?;

    Output::success(&format!(
        "Configuration updated: {} = {}",
        key.cyan(),
        value.bright_white()
    ));
    Ok(())
}

/// Handle config reset command
async fn handle_config_reset(cli: &Cli) -> crate::Result<()> {
    let config_path = cli
        .config
        .clone()
        .unwrap_or_else(|| std::env::current_dir().unwrap().join("fcm.toml"));

    if config_path.exists() {
        fs::remove_file(&config_path)?;
        Output::info(&format!(
            "Configuration file deleted: {}",
            config_path.display()
        ));
    }

    // Create new default configuration
    handle_init(cli).await?;

    Ok(())
}

/// Load configuration from file or default location
async fn load_config(cli: &Cli) -> crate::Result<crate::FcmConfig> {
    if let Some(config_path) = &cli.config {
        crate::FcmConfig::from_file(config_path).await
    } else {
        crate::FcmConfig::load().await
    }
}

/// Parse package specification (name@version)
fn parse_package_spec(spec: &str) -> crate::Result<(String, String)> {
    if let Some((name, version)) = spec.split_once('@') {
        Ok((name.to_string(), version.to_string()))
    } else {
        Err(crate::error::FcmError::Config(
            crate::error::ConfigError::InvalidPackageSpec {
                spec: spec.to_string(),
            },
        ))
    }
}

/// Print search results in text format
fn print_search_results_text(results: &SearchResult) {
    Output::section(&format!(
        "Found {} results (in {:?}):",
        results.total_count.to_string().bright_white(),
        results.query_time
    ));

    if results.resources.is_empty() {
        Output::info("No results found");
        return;
    }

    for (i, result) in results.resources.iter().enumerate() {
        Output::result(&format!(
            "\n{}. {} {}",
            (i + 1).to_string().bright_blue(),
            result
                .resource
                .url
                .as_deref()
                .unwrap_or("<no URL>")
                .bright_white(),
            format!("(score: {:.2})", result.score).dimmed()
        ));
        Output::result(&format!(
            "   {}: {}",
            "Type".dimmed(),
            result.resource.resource_type.cyan()
        ));
        Output::result(&format!(
            "   {}: {}@{}",
            "Package".dimmed(),
            result.index.package_name.yellow(),
            result.index.package_version.yellow()
        ));

        if !result.highlights.is_empty() {
            Output::result(&format!("   {}:", "Highlights".dimmed()));
            for highlight in &result.highlights {
                Output::result(&format!(
                    "     {}: {}",
                    highlight.field.cyan(),
                    highlight.fragment.bright_white()
                ));
            }
        }
    }
}

/// Print search results in JSON format
fn print_search_results_json(results: &SearchResult) -> crate::Result<()> {
    let json = serde_json::to_string_pretty(results)?;
    Output::result(&json);
    Ok(())
}

/// Print resolve results in text format
fn print_resolve_results_text(results: &[crate::resolver::ResolvedResource], details: bool) {
    Output::section(&format!(
        "Resolved {} URLs:",
        results.len().to_string().bright_white()
    ));

    for (i, result) in results.iter().enumerate() {
        print_resolve_result_text_single(result, i + 1, details);
    }
}

/// Print single resolve result in text format
fn print_resolve_result_text(result: &crate::resolver::ResolvedResource, details: bool) {
    print_resolve_result_text_single(result, 1, details);
}

/// Print single resolve result helper
fn print_resolve_result_text_single(
    result: &crate::resolver::ResolvedResource,
    index: usize,
    details: bool,
) {
    Output::result(&format!(
        "\n{}. {}",
        index.to_string().bright_blue(),
        result.canonical_url.bright_white()
    ));
    Output::result(&format!(
        "   {} {}: {}",
        "✅".green(),
        "Resolved".dimmed(),
        result.resource.resource_type.cyan()
    ));
    Output::result(&format!(
        "   {} {}: {}@{}",
        "📦".blue(),
        "Package".dimmed(),
        result.package_info.name.yellow(),
        result.package_info.version.yellow()
    ));

    if details {
        Output::result(&format!(
            "   {} {}: {:?}",
            "🔍".cyan(),
            "Resolution".dimmed(),
            result.resolution_path
        ));
        Output::result(&format!(
            "   {} {}: {}",
            "📄".yellow(),
            "File".dimmed(),
            result
                .resource
                .file_path
                .display()
                .to_string()
                .bright_white()
        ));
    }
}

/// Print resolve results in JSON format
fn print_resolve_results_json(
    results: &[crate::resolver::ResolvedResource],
    _details: bool,
) -> crate::Result<()> {
    let json = serde_json::to_string_pretty(results)?;
    Output::result(&json);
    Ok(())
}

/// Print single resolve result in JSON format
fn print_resolve_result_json(
    result: &crate::resolver::ResolvedResource,
    _details: bool,
) -> crate::Result<()> {
    let json = serde_json::to_string_pretty(result)?;
    Output::result(&json);
    Ok(())
}

/// Print search parameters in table format
fn print_search_params_table(params: &[SearchParameterInfo]) {
    use colored::Colorize;

    if params.is_empty() {
        Output::info("No search parameters found for this resource type");
        return;
    }

    // Calculate column widths
    let max_code = params
        .iter()
        .map(|p| p.code.len())
        .max()
        .unwrap_or(4)
        .max(4);
    let max_name = params
        .iter()
        .map(|p| p.name.len())
        .max()
        .unwrap_or(4)
        .max(4);
    let max_type = params
        .iter()
        .map(|p| p.type_field.len())
        .max()
        .unwrap_or(4)
        .max(4);

    // Print header
    Output::section("Search Parameters:");
    Output::result(&format!(
        "{:width_code$}  {:width_name$}  {:width_type$}  {}",
        "Code".bright_white().bold(),
        "Name".bright_white().bold(),
        "Type".bright_white().bold(),
        "Description".bright_white().bold(),
        width_code = max_code,
        width_name = max_name,
        width_type = max_type
    ));
    Output::result(&format!(
        "{:width_code$}  {:width_name$}  {:width_type$}  {}",
        "-".repeat(max_code).dimmed(),
        "-".repeat(max_name).dimmed(),
        "-".repeat(max_type).dimmed(),
        "-".repeat(40).dimmed(),
        width_code = max_code,
        width_name = max_name,
        width_type = max_type
    ));

    // Print rows
    for param in params {
        let description = param
            .description
            .as_ref()
            .map(|d| {
                if d.len() > 60 {
                    format!("{}...", &d[..57])
                } else {
                    d.clone()
                }
            })
            .unwrap_or_else(|| "".to_string());

        Output::result(&format!(
            "{:width_code$}  {:width_name$}  {:width_type$}  {}",
            param.code.cyan(),
            param.name.bright_white(),
            param.type_field.yellow(),
            description.dimmed(),
            width_code = max_code,
            width_name = max_name,
            width_type = max_type
        ));
    }

    Output::result(&format!(
        "\n{} search parameters found",
        params.len().to_string().bright_green()
    ));
}

/// Print search parameters in JSON format
fn print_search_params_json(params: &[SearchParameterInfo]) -> crate::Result<()> {
    let json = serde_json::to_string_pretty(params)?;
    Output::result(&json);
    Ok(())
}

/// Print search parameters in CSV format
fn print_search_params_csv(params: &[SearchParameterInfo]) -> crate::Result<()> {
    // Print CSV header
    Output::result("code,name,type,base,description,expression,xpath,url,status");

    // Print rows
    for param in params {
        let base = param.base.join(";");
        let description = param.description.as_deref().unwrap_or("");
        let expression = param.expression.as_deref().unwrap_or("");
        let xpath = param.xpath.as_deref().unwrap_or("");
        let url = param.url.as_deref().unwrap_or("");
        let status = param.status.as_deref().unwrap_or("");

        // Escape CSV fields that contain commas, quotes, or newlines
        let escape_csv = |s: &str| -> String {
            if s.contains(',') || s.contains('"') || s.contains('\n') {
                format!("\"{}\"", s.replace('"', "\"\""))
            } else {
                s.to_string()
            }
        };

        Output::result(&format!(
            "{},{},{},{},{},{},{},{},{}",
            escape_csv(&param.code),
            escape_csv(&param.name),
            escape_csv(&param.type_field),
            escape_csv(&base),
            escape_csv(description),
            escape_csv(expression),
            escape_csv(xpath),
            escape_csv(url),
            escape_csv(status)
        ));
    }

    Ok(())
}
