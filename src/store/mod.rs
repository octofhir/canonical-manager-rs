//! Content-addressable virtual store for FHIR packages.
//!
//! Per-machine global store at `<cache_dir>/store/` with file-level CAS
//! and a deterministic per-project lockfile (`fcm.lock`).
//!
//! # Backend boundary
//!
//! This module is **SQLite-backend-specific install infrastructure**.
//! It is invoked by the install pipeline above the
//! [`crate::traits::PackageStore`] and [`crate::traits::SearchStorage`]
//! interfaces and is *not* surfaced on those traits. Custom backends
//! (Postgres etc.) are free to ignore CAS entirely.
//!
//! New features here go through [`crate::CanonicalManager`] or
//! [`crate::sqlite_storage::SqliteStorage`], never through new methods
//! on the frozen trait surface.
//!
//! # Where to look
//!
//! | Topic | Module |
//! |---|---|
//! | Path conventions, store root resolution, fanout layout | [`paths`] |
//! | File-level CAS (SHA-256 hex, 2-char fanout) | [`cas`] |
//! | Reflink → hardlink → copy linker ladder + capability probing | [`linker`] |
//! | Per-package cross-process install locks (`fs4`) | [`lock`] |
//! | `fcm.lock` schema, deterministic serde, integrity SRI | [`lockfile`] |
//! | `~/.fhir/packages/` cross-tool compat shim | [`fhir_cache`] |
//!
//! # Disk layout
//!
//! ```text
//! ~/.fcm/store/                # global store (per-drive on cross-FS setups)
//! ├── blobs/                   # file CAS, sha-256 hex, 2-char fanout
//! │   ├── 00/0aa1bbcc...
//! │   └── ff/...
//! ├── tarballs/                # original .tgz CAS, blake3 hex, 2-char fanout
//! │   ├── 00/...
//! │   └── ff/...
//! ├── tmp/                     # staging for atomic moves
//! ├── locks/                   # per-package install locks
//! └── store.json               # store metadata (version, capabilities)
//! ```
//!
//! Project-local artefacts live next to `fcm.toml`:
//!
//! ```text
//! my-project/
//! ├── fcm.toml
//! ├── fcm.lock                 # deterministic, committed to git
//! └── fhir-modules/
//!     ├── <name> -> .fcm/<name>@<version>/package        # symlink
//!     └── .fcm/<name>@<version>/package/
//!         ├── package.json     # reflinked / hardlinked from CAS
//!         └── *.json
//! ```
//!
pub mod cas;
pub mod fhir_cache;
pub mod linker;
pub mod lock;
pub mod lockfile;
pub mod materialize;
pub mod paths;

pub use cas::{BlobHash, FileCas};
pub use fhir_cache::{default_fhir_cache_root, populate_fhir_cache};
pub use linker::{LinkCapability, Linker, LinkerMode};
pub use lock::PackageInstallLock;
pub use lockfile::{LockedPackage, Lockfile};
pub use materialize::{MaterialiseReport, materialise_dir, materialise_package};
pub use paths::StorePaths;
