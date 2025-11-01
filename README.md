# FHIR Canonical Manager

[![Crates.io](https://img.shields.io/crates/v/octofhir-canonical-manager.svg)](https://crates.io/crates/octofhir-canonical-manager)
[![Documentation](https://docs.rs/octofhir-canonical-manager/badge.svg)](https://docs.rs/octofhir-canonical-manager)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](#license)

A library-first solution for managing FHIR Implementation Guide packages, providing fast canonical URL resolution and resource search capabilities.

## Features

- 📦 **Package Management**: Install, update, and remove FHIR packages from registries
- 🔍 **Fast Resolution**: Lightning-fast canonical URL resolution with SQLite B-tree indexes (~7ms average)
- 🔎 **Advanced Search**: Query FHIR resources by type, package, and other criteria
- 🔧 **Search Parameters**: Retrieve FHIR SearchParameter definitions by resource type
- 🗄️ **SQLite Backend**: Industry-standard SQLite with WAL mode for reliability and performance
- 🏗️ **Library First**: Clean API for embedding in your applications
- 🖥️ **CLI Tool**: Optional command-line interface for interactive use
- 🌐 **Registry Support**: Compatible with standard FHIR package registries
- ⚡ **Async/Await**: Built with modern async Rust for performance
- 🔄 **Multi-Version**: Support multiple FHIR versions simultaneously
- 📁 **Local Packages**: Load packages from local directories

## Quick Start

### Library Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
octofhir-canonical-manager = "0.1"
tokio = { version = "1.0", features = ["full"] }
```

Basic usage:

```rust
use octofhir_canonical_manager::{CanonicalManager, FcmConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration
    let config = FcmConfig::load()?;
    let manager = CanonicalManager::new(config).await?;
    
    // Install a FHIR package
    manager.install_package("hl7.fhir.us.core", "6.1.0").await?;
    
    // Resolve a canonical URL
    let resource = manager.resolve(
        "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient"
    ).await?;
    
    println!("Found resource: {}", resource.id);
    
    // Search for resources
    let results = manager.search()
        .resource_type("StructureDefinition")
        .package("hl7.fhir.us.core")
        .execute().await?;
        
    println!("Found {} structure definitions", results.len());
    
    // Get search parameters for a resource type
    let search_params = manager.get_search_parameters("Patient").await?;
    println!("Found {} search parameters for Patient", search_params.len());
    
    Ok(())
}
```

### CLI Usage

Install the CLI tool:

```bash
cargo install octofhir-canonical-manager --features cli
```

Initialize and use:

```bash
# Initialize configuration
octofhir-fcm init

# Install packages
octofhir-fcm install hl7.fhir.us.core@6.1.0

# Search resources
octofhir-fcm search "Patient" --resource-type StructureDefinition

# Get search parameters for a resource type
octofhir-fcm search-params Patient
octofhir-fcm search-params Patient --format json
octofhir-fcm search-params Patient --format csv

# Resolve canonical URLs
octofhir-fcm resolve "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient"

# List installed packages
octofhir-fcm list
```

## Configuration

Create a `fcm.toml` configuration file:

```toml
[registry]
url = "https://fs.get-ig.org/pkgs/"
timeout = 60
retry_attempts = 5

[[packages]]
name = "hl7.fhir.r4.core"
version = "4.0.1"
priority = 1

[storage]
cache_dir = ".fcm/cache"
packages_dir = ".fcm/packages"
max_cache_size = "2GB"

# Optimization settings (SQLite handles indexing automatically)
[optimization]
# Parallel processing
parallel_workers = 8
batch_size = 100

# Change detection
enable_checksums = true
checksum_algorithm = "blake3"
checksum_cache_size = 10000

# Performance monitoring
enable_metrics = true
metrics_interval = "30s"
```

### Configuration Options

#### Registry Settings
- `url`: FHIR package registry URL
- `timeout`: Network timeout in seconds
- `retry_attempts`: Number of retry attempts for failed requests

#### Storage Settings
- `cache_dir`: Directory for package downloads cache
- `packages_dir`: Directory for extracted packages
- `max_cache_size`: Maximum cache size (e.g., "2GB", "500MB")

#### Optimization Settings
- `parallel_workers`: Number of workers for parallel package processing
- `batch_size`: Batch size for parallel operations
- `enable_checksums`: Enable package checksum validation for change detection
- `checksum_algorithm`: Algorithm to use (`blake3`, `sha256`, `sha1`)
- `checksum_cache_size`: Size of checksum cache
- `enable_metrics`: Enable performance metrics collection
- `metrics_interval`: Metrics collection interval

**Note**: SQLite handles indexing, compression, and memory mapping automatically - no manual configuration needed!

## API Overview

### Core Types

- **`CanonicalManager`**: Main entry point for all operations
- **`FcmConfig`**: Configuration management
- **`CanonicalResolver`**: Fast URL resolution engine
- **`SearchEngine`**: Advanced resource search capabilities
- **`SqliteStorage`**: SQLite-based storage with automatic indexing

### Key Methods

```rust
// Package management
manager.install_package("package-name", "version").await?;
manager.remove_package("package-name", "version").await?;
manager.list_packages().await?;

// Batch installation (optimized for multiple packages)
manager.install_packages_batch(&packages).await?;

// Resource resolution
let resource = manager.resolve("canonical-url").await?;
let resources = manager.batch_resolve(&urls).await?;

// Resolution with FHIR version
let resource = manager.resolve_with_fhir_version(
    "http://hl7.org/fhir/StructureDefinition/Patient",
    "4.0.1"
).await?;

// Search functionality
let results = manager.search()
    .resource_type("StructureDefinition")
    .package("hl7.fhir.us.core")
    .canonical_pattern("Patient")
    .execute().await?;

// Search parameter retrieval
let search_params = manager.get_search_parameters("Patient").await?;
for param in search_params {
    println!("{}: {} ({})", param.code, param.name, param.type_field);
}

// Load local packages
manager.load_from_directory("/path/to/package", Some("my.package@1.0.0")).await?;
manager.load_resources_from_directory("/path/to/resources").await?;
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `init` | Initialize FCM configuration |
| `install <package>[@version]` | Install FHIR package |
| `remove <package>[@version]` | Remove FHIR package |
| `list` | List installed packages |
| `search <query>` | Search for resources |
| `search-params <resource-type>` | Get search parameters for a resource type |
| `resolve <url>` | Resolve canonical URL |
| `update` | Update package indexes |

## Performance

The canonical manager is optimized for speed and reliability:

### SQLite Backend Benefits
- **B-tree Indexes**: O(1) canonical URL lookups without manual index management (~7ms average query time)
- **WAL Mode**: Write-Ahead Logging for better concurrency and crash recovery
- **Single-Query Joins**: Eliminates separate package lookups for faster resolution
- **Atomic Transactions**: Ensures data consistency across operations
- **Automatic Optimization**: SQLite handles compression, memory mapping, and index maintenance

### Additional Optimizations
- **Batch Installation**: Install N packages with single index rebuild instead of N rebuilds
- **Streaming Downloads**: Efficient package downloads with progress tracking
- **Change Detection**: Blake3 checksums for fast package change detection
- **Parallel Processing**: Configurable worker threads for package extraction
- **Dependency Resolution**: Automatic handling of package dependencies
- **Smart Caching**: Minimizes network requests and disk I/O

### Benchmark Results
- Canonical URL resolution: ~7ms average
- Package installation: Optimized with batch operations
- Search queries: Fast text indexing with SQLite FTS5
- Multi-version support: Efficient storage with deduplication

## Architecture

### Storage Layer
- **SqliteStorage**: Industry-standard SQLite database with:
  - B-tree indexes for fast lookups
  - JSON storage for flexible resource data
  - WAL mode for concurrency
  - Atomic transactions for consistency

### Resolution Strategies
1. **Exact Match**: Direct B-tree index lookup (fastest)
2. **Version Fallback**: Find latest version of base URL
3. **Fuzzy Matching**: Levenshtein distance for typo tolerance

### Search Capabilities
- Full-text search with inverted text index
- Filter by resource type, package, URL patterns
- Faceted search with relevance scoring
- SearchParameter resource queries

## Development

```bash
# Run tests
just test

# Check code quality
just check

# Fix formatting and linting
just fix-all

# Prepare for publishing
just prepare-publish

# Generate documentation
just docs

# Run examples
cargo run --example search_parameters
cargo run --example configuration
```

## Examples

See the [examples](examples/) directory for more usage examples:
- `basic_usage.rs`: Basic package management and resolution
- `search_parameters.rs`: Working with FHIR SearchParameters
- `configuration.rs`: Configuration management

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT License ([LICENSE-MIT](LICENSE-MIT))

at your option.

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

## Links

- [Documentation](https://docs.rs/octofhir-canonical-manager)
- [Crates.io](https://crates.io/crates/octofhir-canonical-manager)
- [Repository](https://github.com/octofhir/canonical-manager-rs)
- [Issue Tracker](https://github.com/octofhir/canonical-manager-rs/issues)

---

Made with ❤️ by OctoFHIR Team 🐙🦀
