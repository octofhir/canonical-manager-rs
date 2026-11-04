# Offline canonical package management

`octofhir-canonical-manager` keeps its existing imports and features, including
HTTP support when default features are disabled. Existing downstream storage
implementations do not need to change their trait implementations.

For local package extraction, storage, indexing, search and canonical resolution,
use the new core package:

```toml
[dependencies]
octofhir-canonical-core = { version = "0.2.11", default-features = false }
```

The core has no HTTP or TLS dependencies in its normal/build dependency graph.
It still uses Tokio for asynchronous filesystem and storage operations. Add
`sqlite` for the bundled backend, or supply `PackageStore` and `SearchStorage`:

```rust,ignore
use octofhir_canonical_core::CanonicalManager;

let manager = CanonicalManager::new_with_storage(config, packages, search).await?;
manager.load_from_directory(path, None).await?;
let resource = manager.resolve(canonical_url).await?;
```

`new_with_storage` never constructs an HTTP client. Remote operations return an
explicit missing-adapter error. `new_with_components` accepts an `AsyncRegistry`
implementation for a custom transport; it also avoids creating a fallback HTTP
client. Metadata and downloads are both dispatched through that adapter.

Enable `http` on the core to use built-in registry clients, or keep using the
compatibility manager package. Enabling `cli` enables HTTP and SQLite. Cargo
unifies features when the same process uses both packages: a server using the
HTTP facade intentionally builds the HTTP implementation once.

The core's `FcmError::Network(reqwest::Error)` variant exists only with `http`.
The compatibility manager always enables it, preserving its public error API.
