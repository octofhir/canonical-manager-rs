# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is **octofhir-canonical-manager**, a Rust library for managing FHIR Implementation Guide packages with fast canonical URL resolution and search capabilities. It's a library-first design with an optional CLI tool.

## Essential Commands

### Development Workflow
```bash
# Primary commands using just (justfile)
just test                # Run all tests (unit, integration, doctests)
just check              # Format check + lint + test
just fix-all            # Auto-fix formatting and clippy issues
just publish-dry-run    # Validate package is ready for publishing

# Individual commands
just lint               # Run clippy with -D warnings
just fmt-check          # Check formatting without fixing
just build              # Standard cargo build
just run -- <args>      # Run CLI tool with arguments
# Examples:
# just run search-params Patient --format json  # Get Patient search parameters as JSON
# just run search-params Patient --format csv   # Get Patient search parameters as CSV
```

### Testing
```bash
cargo test --all        # All tests (unit, integration, doctests)
cargo test --doc        # Run doctests only
cargo test unit_tests   # Run unit tests only
cargo test integration_tests  # Run integration tests only
cargo test <test_name>  # Run specific test
cargo test test_search_parameter_retrieval  # Test search parameter functionality
```

### Publishing
```bash
just prepare-publish    # Full validation before publishing
just publish-dry-run    # Test packaging without publishing
```

### GitHub Workflows
- **CI** (`.github/workflows/ci.yml`): Runs on all PRs and pushes to main
  - Tests on stable and beta Rust (with rustfmt and clippy components)
  - Clippy linting with warnings as errors
  - Format checking
  - Documentation building
- **Publish** (`.github/workflows/publish.yml`): Manual workflow for publishing to crates.io
  - Triggered manually from GitHub Actions tab
  - Requires typing "publish" to confirm
  - Checks if version already exists on crates.io to prevent duplicates
  - Runs full validation before publishing
  - Creates git tag after successful publish
  - Requires `CARGO_REGISTRY_TOKEN` secret in repository settings

## Architecture Overview

### Core Components

**CanonicalManager** (`lib.rs`)
- Main facade providing unified API for all operations
- Orchestrates package installation, resolution, search, and search parameter retrieval
- Manages dependencies between components
- Provides `get_search_parameters()` method for FHIR SearchParameter resource filtering

**SqliteStorage** (`sqlite_storage.rs`)
- Industry-standard SQLite database with rusqlite for FHIR package and resource storage
- WAL (Write-Ahead Logging) mode for better concurrency and crash recovery
- B-tree indexes for O(1) canonical URL lookups without manual index management
- Single-query JOINs eliminate separate package lookups for faster resolution
- Atomic transactions ensure data consistency
- Batch installation API to eliminate O(n²) index rebuild problem


**CanonicalResolver** (`resolver.rs`)
- Multi-strategy resolution system:
  1. Exact match (fastest)
  2. Version fallback (find latest version of base URL)
  3. Fuzzy matching (Levenshtein distance)

**RegistryClient** (`registry.rs`)
- Downloads packages from FHIR registries with fallback support
- Primary: user-configured URL, fallbacks: fs.get-ig.org, packages.fhir.org
- Streaming downloads with progress tracking and retry logic

**SearchEngine** (`search.rs`)
- Inverted text index for full-text search
- Fluent query builder API with filters (type, package, URL patterns)
- Faceted search with relevance scoring and suggestions
- Supports SearchParameter resource queries for FHIR metadata

**PackageExtractor** (`package.rs`)
- Extracts .tgz FHIR packages and validates structure
- Parses package manifests and FHIR resources
- Handles dependency resolution during installation

### Key Data Flows

1. **Package Installation**: Registry query → Download → Extract → Validate → SqliteStorage.add_package() → SQLite transaction commit
2. **Batch Installation**: Multiple packages → SqliteStorage.install_packages_batch() → Single index rebuild → Better performance for N packages
3. **URL Resolution**: SQLite B-tree index lookup (single JOIN query) → Version fallback → Fuzzy match
4. **Search**: Text index → Filter → Score → Paginate → Resource loading from SQLite

### Configuration

**fcm.toml** structure:
- `[registry]`: Registry URL, timeouts, retry settings
- `[[packages]]`: Package specs with name, version, priority
- `[storage]`: Local directory paths and cache settings

**rust-toolchain.toml**: Ensures consistent Rust toolchain across environments
- Uses stable channel with rustfmt and clippy components
- Supports common target platforms (Linux, macOS, Windows)

**Licensing**: Dual-licensed under MIT OR Apache-2.0
- `LICENSE-MIT` and `LICENSE-APACHE` files included in published package
- `CLAUDE.md`, `.github/`, and `rust-toolchain.toml` excluded from crates.io

Environment variable overrides: `FCM_REGISTRY_URL`, `FCM_CACHE_DIR`, etc.

## Important Implementation Details

### Testing Strategy
- **MockRegistry** in tests creates proper .tgz tarballs (not JSON)
- **Version fallback** logic requires precise URL base matching
- **Doctests** use correct crate name `octofhir_canonical_manager::`

### Error Handling
- Comprehensive typed errors in `error.rs`
- Graceful degradation for non-critical failures
- Registry fallbacks when primary registry fails

### Performance Considerations
- **SqliteStorage**: B-tree indexes provide O(1) canonical URL lookups without manual maintenance
- **WAL Mode**: Write-Ahead Logging enables better concurrency and faster writes
- **Single-Query Resolution**: JOINs eliminate separate package lookups (7ms average query time)
- **Batch Installation**: Install N packages with single index rebuild instead of N rebuilds
- **Streaming Downloads**: Large package downloads use streaming to minimize memory usage
- **JSON Storage**: SQLite's native JSON support enables efficient resource storage and querying

### Code Conventions
- All async operations use Tokio
- Extensive use of `Arc<>` for shared ownership
- Builder patterns for complex query construction
- Comprehensive documentation with examples

### CLI vs Library
- Library functionality is always available
- CLI features gated behind `#[cfg(feature = "cli")]`
- Progress tracking and colored output only in CLI mode
- New `search-params` command supports table, JSON, and CSV output formats

### Search Parameter Support
- **SearchParameterInfo** struct (`lib.rs:100-172`) for typed FHIR SearchParameter data
- **CLI Command**: `search-params <resource-type> [--format table|json|csv]`
- **Library Method**: `manager.get_search_parameters("ResourceType").await?`
- Filters SearchParameter resources by `base` field matching the target resource type

## Key Files to Understand

- `src/lib.rs` - Main API and component orchestration with batch installation support
- `src/sqlite_storage.rs` - SQLite-based storage with WAL mode and B-tree indexes
- `src/unified_storage.rs` - Unified storage facade (simplified to use only SqliteStorage)
- `src/resolver.rs` - Resolution strategies and version handling
- `src/registry.rs` - Network operations and registry interactions
- `src/search.rs` - Full-text search engine with inverted index
- `src/config.rs` - Configuration management and validation
- `src/cli.rs` - CLI interface including search-params command and output formatters
- `tests/common/mock_registry.rs` - Test infrastructure (creates proper tarballs)
- `tests/common/fixtures.rs` - Test fixtures including FHIR resource samples

## Common Issues

- **Doctest imports**: Always use `octofhir_canonical_manager::` crate name
- **Test failures**: Check MockRegistry creates valid .tgz files, not JSON
- **Version resolution**: Ensure base URL matching logic is precise
- **Search not finding resources**: Verify package installation completed successfully - SqliteStorage uses atomic transactions
- **Database corruption**: SQLite WAL mode provides crash recovery; database file is at `~/.maki/index/fhir.db`
- **Performance issues**: Check SQLite indexes are created; query performance should be ~7ms for canonical URL lookups
- **Batch installation**: Use `install_packages_batch()` for multiple packages to avoid O(n²) index rebuilds
- **Clippy warnings**: Pay attention to unused variables and dead code flags

---

Made with ❤️ by OctoFHIR Team 🐙🦀