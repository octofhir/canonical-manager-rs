# Contributing to FHIR Canonical Manager

Thank you for your interest in contributing to the FHIR Canonical Manager! We welcome contributions from the community and are grateful for any help you can provide.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Making Changes](#making-changes)
- [Testing](#testing)
- [Code Style](#code-style)
- [Submitting Changes](#submitting-changes)
- [Issue Guidelines](#issue-guidelines)
- [Pull Request Process](#pull-request-process)
- [Release Process](#release-process)

## Code of Conduct

This project adheres to a code of conduct that we expect all contributors to follow. By participating in this project, you agree to abide by its terms. Please be respectful and constructive in all interactions.

## Getting Started

### Prerequisites

- **Rust**: Install the latest stable version of Rust from [rustup.rs](https://rustup.rs/)
- **Git**: Version control system
- **Just**: Task runner (install with `cargo install just`)

### Development Setup

1. **Fork and Clone**
   ```bash
   git clone https://github.com/yourusername/canonical-manager-rs.git
   cd canonical-manager-rs
   ```

2. **Install Dependencies**
   ```bash
   cargo build
   ```

3. **Run Tests**
   ```bash
   just test
   ```

4. **Check Code Quality**
   ```bash
   just check
   ```

## Making Changes

### Branching Strategy

- `main` - Production-ready code
- `develop` - Integration branch for features
- `feature/xyz` - Feature branches
- `fix/xyz` - Bug fix branches
- `docs/xyz` - Documentation improvements

### Development Workflow

1. **Create a Feature Branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make Your Changes**
   - Write clean, well-documented code
   - Add tests for new functionality
   - Update documentation as needed

3. **Run Quality Checks**
   ```bash
   just fix-all  # Fix formatting and linting issues
   just test     # Run all tests
   just check    # Run comprehensive checks
   ```

4. **Commit Changes**
   ```bash
   git add .
   git commit -m "feat: add your feature description"
   ```

## Testing

### Running Tests

```bash
# Run all tests
just test

# Run tests with output
just test-verbose

# Run specific test
cargo test test_name

# Run integration tests
cargo test --test integration_tests
```

### Test Coverage

```bash
# Generate coverage report (requires cargo-tarpaulin)
just coverage
```

### Writing Tests

- **Unit Tests**: Place in the same file as the code being tested
- **Integration Tests**: Place in `tests/` directory
- **Examples**: Ensure all examples in `examples/` directory compile and run

Example test structure:
```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_functionality() {
        let temp_dir = TempDir::new().unwrap();
        let config = FcmConfig::test_config(temp_dir.path());
        
        // Test your functionality here
        assert!(result.is_ok());
    }
}
```

## Code Style

### Formatting and Linting

We use standard Rust tooling for code quality:

```bash
# Format code
just fmt

# Check formatting
just fmt-check

# Run linting
just lint

# Fix all issues automatically
just fix-all
```

### Guidelines

- **Documentation**: All public APIs must have rustdoc comments
- **Error Handling**: Use the project's error types (`FcmError`)
- **Async Code**: Prefer `async/await` over manual futures
- **Naming**: Use descriptive names following Rust conventions
- **Comments**: Write clear, concise comments explaining *why*, not *what*

### Documentation Standards

```rust
/// Brief description of the function.
///
/// Longer description with more details about the function's behavior,
/// parameters, and return values. Include examples when helpful.
///
/// # Arguments
///
/// * `param1` - Description of the first parameter
/// * `param2` - Description of the second parameter
///
/// # Returns
///
/// Description of what the function returns and when.
///
/// # Errors
///
/// Description of error conditions and what errors may be returned.
///
/// # Examples
///
/// ```rust,no_run
/// use octofhir_canonical_manager::CanonicalManager;
/// 
/// let manager = CanonicalManager::new(config).await?;
/// let result = manager.some_function("param").await?;
/// ```
pub async fn some_function(&self, param: &str) -> Result<String> {
    // Implementation
}
```

## Submitting Changes

### Before Submitting

1. **Run All Checks**
   ```bash
   just prepare-publish
   ```

2. **Update Documentation**
   - Update rustdoc comments
   - Update README.md if needed
   - Add examples if applicable

3. **Write Tests**
   - Add unit tests for new functionality
   - Add integration tests for complex features
   - Ensure all tests pass

### Commit Message Format

Use conventional commits for clear history:

```
type(scope): description

[optional body]

[optional footer]
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

Examples:
```
feat(resolver): add version-specific canonical URL resolution
fix(storage): handle concurrent access to SQLite database
docs: update README with new configuration options
```

## Issue Guidelines

### Reporting Bugs

When reporting bugs, please include:

1. **Description**: Clear description of the issue
2. **Reproduction Steps**: Step-by-step instructions to reproduce
3. **Expected Behavior**: What should happen
4. **Actual Behavior**: What actually happens
5. **Environment**: OS, Rust version, crate version
6. **Configuration**: Relevant configuration (sanitized)

### Feature Requests

For feature requests, please include:

1. **Use Case**: Why is this feature needed?
2. **Proposed Solution**: How should it work?
3. **Alternatives**: Other ways to achieve the same goal
4. **Examples**: Code examples or mockups

## Pull Request Process

### PR Checklist

- [ ] Code follows project style guidelines
- [ ] Self-review of code completed
- [ ] Tests added for new functionality
- [ ] All tests pass locally
- [ ] Documentation updated
- [ ] CHANGELOG.md updated (if applicable)
- [ ] Breaking changes noted in PR description

### PR Template

```markdown
## Description
Brief description of changes made.

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] Manual testing completed

## Checklist
- [ ] Code follows style guidelines
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] Tests pass locally
```

### Review Process

1. **Automated Checks**: CI runs tests and linting
2. **Code Review**: Maintainer review for code quality
3. **Testing**: Manual testing of new features
4. **Approval**: At least one maintainer approval required
5. **Merge**: Squash and merge to main branch

## Release Process

### Versioning

We follow [Semantic Versioning](https://semver.org/):

- `MAJOR`: Breaking changes
- `MINOR`: New features (backward compatible)
- `PATCH`: Bug fixes (backward compatible)

### Release Steps

1. **Update Version**
   ```bash
   # Update Cargo.toml version
   # Update CHANGELOG.md
   ```

2. **Create Release**
   ```bash
   git tag v0.1.0
   git push origin v0.1.0
   ```

3. **Publish to Crates.io**
   ```bash
   just publish
   ```

## Development Tips

### Useful Commands

```bash
# Quick development cycle
just fix-all && just test

# Check specific feature
cargo test --features cli

# Build documentation locally
just docs

# Security audit
just audit

# Benchmark performance
just bench
```

### IDE Setup

**VS Code Extensions:**
- rust-analyzer
- CodeLLDB (for debugging)
- Even Better TOML

**Configuration:**
Add to `.vscode/settings.json`:
```json
{
    "rust-analyzer.cargo.features": ["cli"],
    "rust-analyzer.checkOnSave.command": "clippy"
}
```

### Debugging

```bash
# Run with debug logging
RUST_LOG=debug cargo run --example basic_usage

# Run with specific module logging
RUST_LOG=octofhir_canonical_manager::storage=trace cargo test
```

## Getting Help

- **Documentation**: Check the [API docs](https://docs.rs/octofhir-canonical-manager)
- **Examples**: Look at examples in the `examples/` directory
- **Issues**: Search existing issues on GitHub
- **Discussions**: Start a discussion for questions

## Recognition

Contributors will be acknowledged in:
- `CONTRIBUTORS.md` file
- Release notes for significant contributions
- GitHub contributor graph

Thank you for contributing to FHIR Canonical Manager! 🎉

---

Made with ❤️ by OctoFHIR Team 🐙🦀