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
```

### Testing
```bash
cargo test --all        # All tests (26 unit + 19 integration + 63 additional + 103 doctests)
cargo test --doc        # Run doctests only
cargo test unit_tests   # Run unit tests only
cargo test integration_tests  # Run integration tests only
cargo test <test_name>  # Run specific test
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
- Orchestrates package installation, resolution, and search
- Manages dependencies between components

**BinaryStorage** (`binary_storage.rs`)
- High-performance binary storage system using bincode serialization + lz4 compression
- In-memory cache with atomic disk persistence
- Replaces SQLite for better read performance while maintaining data integrity
- O(1) canonical URL resolution via in-memory HashMap cache
- Automatic backup and atomic write operations for reliability


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

**PackageExtractor** (`package.rs`)
- Extracts .tgz FHIR packages and validates structure
- Parses package manifests and FHIR resources
- Handles dependency resolution during installation

### Key Data Flows

1. **Package Installation**: Registry query → Download → Extract → Validate → BinaryStorage.add_package() → Index + Cache update
2. **URL Resolution**: In-memory cache lookup → Version fallback → Fuzzy match → BinaryStorage.get_resource() 
3. **Search**: Text index → Filter → Score → Paginate → Resource loading from BinaryStorage
4. **Cache Rebuild**: Package extraction → Resource indexing → Atomic in-memory cache update → Compressed binary serialization to disk

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
- **BinaryStorage**: In-memory cache for O(1) canonical URL resolution
- **Compression**: LZ4 compression reduces disk I/O while maintaining fast decompression
- **Atomic Operations**: All cache updates are atomic to prevent inconsistent states
- **Streaming Downloads**: Large package downloads use streaming to minimize memory usage
- **Incremental Indexing**: Packages are indexed individually during installation
- **Binary Serialization**: Bincode provides faster serialization than JSON for storage

### Code Conventions
- All async operations use Tokio
- Extensive use of `Arc<>` for shared ownership
- Builder patterns for complex query construction
- Comprehensive documentation with examples

### CLI vs Library
- Library functionality is always available
- CLI features gated behind `#[cfg(feature = "cli")]`
- Progress tracking and colored output only in CLI mode

## Key Files to Understand

- `src/lib.rs` - Main API and component orchestration
- `src/binary_storage.rs` - High-performance binary storage with in-memory caching
- `src/resolver.rs` - Resolution strategies and version handling
- `src/registry.rs` - Network operations and registry interactions
- `src/search.rs` - Full-text search engine with inverted index
- `src/config.rs` - Configuration management and validation
- `tests/common/mock_registry.rs` - Test infrastructure (creates proper tarballs)

## Common Issues

- **Doctest imports**: Always use `octofhir_canonical_manager::` crate name
- **Test failures**: Check MockRegistry creates valid .tgz files, not JSON
- **Version resolution**: Ensure base URL matching logic is precise
- **Search not finding resources**: Verify cache rebuild after package installation - BinaryStorage.add_package() should update in-memory cache atomically
- **Storage corruption**: Use BinaryStorage.integrity_check() to verify file consistency
- **Performance issues**: Check if in-memory cache is properly loaded from binary storage file
- **Clippy warnings**: Pay attention to unused variables and dead code flags

---

Made with ❤️ by OctoFHIR Team 🐙🦀