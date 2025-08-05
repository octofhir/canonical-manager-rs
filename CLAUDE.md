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
cargo test --all        # All tests (29 unit + 19 integration + 63 additional + 118 doctests)
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
  - Tests on stable and beta Rust
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

**IndexedStorage** (`storage.rs`)
- Dual storage: SQLite database + in-memory HashMap cache
- Handles package metadata, resource indexing, and fast canonical URL lookups
- Critical for performance: O(1) canonical URL resolution via cache

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

1. **Package Installation**: Registry query → Download → Extract → Validate → Store → Index
2. **URL Resolution**: Cache lookup → Version fallback → Fuzzy match → Resource load
3. **Search**: Text index → Filter → Score → Paginate

### Configuration

**fcm.toml** structure:
- `[registry]`: Registry URL, timeouts, retry settings
- `[[packages]]`: Package specs with name, version, priority
- `[storage]`: Local directory paths and cache settings

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
- In-memory cache for canonical URL resolution is critical
- Streaming downloads for large packages
- Incremental indexing when packages change

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
- `src/storage.rs` - Storage architecture with caching strategy
- `src/resolver.rs` - Resolution strategies and version handling
- `src/registry.rs` - Network operations and registry interactions
- `src/config.rs` - Configuration management and validation
- `tests/common/mock_registry.rs` - Test infrastructure (creates proper tarballs)

## Common Issues

- **Doctest imports**: Always use `octofhir_canonical_manager::` crate name
- **Test failures**: Check MockRegistry creates valid .tgz files, not JSON
- **Version resolution**: Ensure base URL matching logic is precise
- **Clippy warnings**: Pay attention to unused variables and dead code flags