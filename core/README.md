# OctoFHIR Canonical Core

Local FHIR package extraction, storage, indexing, search and canonical URL
resolution. The default normal/build dependency graph contains no HTTP or TLS
implementation. Tokio supports asynchronous filesystem and storage operations.

```toml
[dependencies]
octofhir-canonical-core = { version = "0.2.11", default-features = false }
```

Implement `PackageStore` and `SearchStorage`, then construct a local manager with
`CanonicalManager::new_with_storage`. This constructor never creates an HTTP
client. Use `new_with_components` to supply a custom `AsyncRegistry` transport;
metadata and downloads both use that adapter.

Optional features:

- `sqlite`: bundled SQLite backend and `CanonicalManager::new`.
- `http`: built-in HTTP registry clients and TLS.
- `cli`: command implementation; enables `sqlite` and `http`.
- `fuzzy-search`: fuzzy canonical resolution.
- `metrics`: metrics collection.

Remote operations without a registry adapter return an explicit error. The
`FcmError::Network(reqwest::Error)` variant is available only with `http`.

The existing `octofhir-canonical-manager` package re-exports this core and retains
its previous imports, features and HTTP behavior, including when default
features are disabled. Consumers of that facade do not need to migrate.

See the repository's `docs/offline-core.md` for integration details.
