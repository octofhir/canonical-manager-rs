# FHIR Canonical Manager

[![Crates.io](https://img.shields.io/crates/v/octofhir-canonical-manager.svg)](https://crates.io/crates/octofhir-canonical-manager)
[![Documentation](https://docs.rs/octofhir-canonical-manager/badge.svg)](https://docs.rs/octofhir-canonical-manager)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](#license)

A library-first solution for managing FHIR Implementation Guide packages, providing fast canonical URL resolution and resource search capabilities.

## Features

- 📦 **Package Management**: Install, update, and remove FHIR packages from registries
- 🔍 **Fast Resolution**: Lightning-fast canonical URL resolution with indexing
- 🔎 **Advanced Search**: Query FHIR resources by type, package, and other criteria  
- 🏗️ **Library First**: Clean API for embedding in your applications
- 🖥️ **CLI Tool**: Optional command-line interface for interactive use
- 🌐 **Registry Support**: Compatible with standard FHIR package registries
- ⚡ **Async/Await**: Built with modern async Rust for performance

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
octofhir-fcm search "Patient" --type StructureDefinition

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
timeout = 30
retry_attempts = 3

[[packages]]
name = "hl7.fhir.us.core"
version = "6.1.0"
priority = 1

[storage]
cache_dir = "~/.fcm/cache"
index_dir = "~/.fcm/index"
packages_dir = "~/.fcm/packages"
max_cache_size = "1GB"
```

## API Overview

### Core Types

- **`CanonicalManager`**: Main entry point for all operations
- **`FcmConfig`**: Configuration management
- **`CanonicalResolver`**: Fast URL resolution engine
- **`SearchEngine`**: Advanced resource search capabilities

### Key Methods

```rust
// Package management
manager.install_package("package-name", "version").await?;
manager.remove_package("package-name", "version").await?;
manager.list_packages().await?;

// Resource resolution
let resource = manager.resolve("canonical-url").await?;
let resources = manager.batch_resolve(&urls).await?;

// Search functionality
let results = manager.search()
    .resource_type("StructureDefinition")
    .package("hl7.fhir.us.core")
    .canonical_url_contains("Patient")
    .execute().await?;
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `init` | Initialize FCM configuration |
| `install <package>[@version]` | Install FHIR package |
| `remove <package>[@version]` | Remove FHIR package |
| `list` | List installed packages |
| `search <query>` | Search for resources |
| `resolve <url>` | Resolve canonical URL |
| `rebuild-index` | Rebuild search index |

## Performance

The canonical manager is optimized for speed:

- **Binary Storage**: High-performance binary storage using bincode serialization and lz4_flex compression
- **In-Memory Caching**: Fast lookups with intelligent caching for read-heavy workloads
- **Atomic Operations**: Data integrity with atomic write operations and backup functionality
- **Streaming Downloads**: Efficient package downloads with progress
- **Dependency Resolution**: Automatic handling of package dependencies
- **Smart Caching**: Minimizes network requests and disk I/O

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
```

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
