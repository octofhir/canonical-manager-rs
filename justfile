# FHIR Canonical Manager - Development Commands

# Run all tests
test:
    cargo test --features test-utils

# Run tests with output
test-verbose:
    cargo test --features test-utils -- --nocapture

# Run linting
lint:
    cargo clippy

# Fix all clippy issues (allows dirty working directory)
clippy-fix:
    cargo clippy --fix --allow-dirty

# Format code
fmt:
    cargo fmt --all

# Fix all formatting issues
fmt-fix:
    cargo fmt --all

# Check formatting
fmt-check:
    cargo fmt -- --check

# Fix all formatting and clippy issues
fix-all: fmt-fix clippy-fix

# Run all checks (format, lint, test)
check: fmt-check lint test

# Build the project
build:
    cargo build

# Build in release mode
build-release:
    cargo build --release

# Clean build artifacts
clean:
    cargo clean

# Install locally
install:
    cargo install --path .

# Show dependencies
deps:
    cargo tree

# Update dependencies
update:
    cargo update

# Prepare library for publishing (check everything is ready)
prepare-publish: fmt-check lint test
    cargo check --release
    cargo doc --no-deps
    @echo "✅ Library is ready for publishing"

# Dry-run publish to crates.io
publish-dry-run: prepare-publish
    cargo publish --dry-run

# Publish to crates.io (use with caution!)
publish: prepare-publish
    @echo "🚨 This will publish to crates.io. Continue? (y/N)"
    @read confirm && [ "$$confirm" = "y" ] && cargo publish || echo "Cancelled"

# Run the CLI tool
run *args:
    cargo run --bin octofhir-fcm -- {{args}}

# Run with release optimizations
run-release *args:
    cargo run --release --bin octofhir-fcm -- {{args}}

# Generate documentation
docs:
    cargo doc --open

# Show package information
info:
    cargo metadata --format-version 1 | jq '.packages[] | select(.name == "octofhir-canonical-manager")'

# Benchmark (if benchmarks exist)
bench:
    cargo bench

# Coverage report (requires cargo-tarpaulin)
coverage:
    cargo tarpaulin --out html

# Security audit (requires cargo-audit)
audit:
    cargo audit