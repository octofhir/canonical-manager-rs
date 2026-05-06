//! Content-addressable virtual store for FHIR packages.
//!
//! Implements the P2 architecture from `docs/refactor/VIRTUAL_STORE_DESIGN.md`:
//! per-machine global store at `~/.fcm/store/` with file-level CAS, plus a
//! deterministic per-project lockfile (`fcm.lock`).
//!
//! # Backend boundary
//!
//! This module is **SQLite-backend-specific install infrastructure**. It
//! is invoked by the install pipeline above the [`crate::traits::PackageStore`]
//! and [`crate::traits::SearchStorage`] interfaces and is *not* surfaced
//! on those traits. Downstream consumers that implement the storage
//! traits against Postgres (or any other backend) are not required to
//! use this module — they can store packages however they want as long
//! as they honour the trait contract.
//!
//! Practical implication: when you add functionality here (new CAS
//! features, new linker modes, lockfile fields), do *not* add
//! corresponding methods to `PackageStore` / `SearchStorage`. Wire them
//! into [`crate::CanonicalManager`] or [`crate::sqlite_storage::SqliteStorage`]
//! directly so the trait surface stays frozen.
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
//! # What's NOT yet wired
//!
//! Phase A (this milestone) lands the foundations: CAS, linker, lockfile,
//! locks, schema v2. Phase B (separate work) wires per-project virtual
//! layout materialisation, the `~/.fhir/packages/` shim, and the
//! `fcm store …` CLI. See PLAN.md P2 for the full breakdown.

pub mod cas;
pub mod linker;
pub mod lock;
pub mod lockfile;
pub mod materialize;
pub mod paths;

pub use cas::{BlobHash, FileCas};
pub use linker::{LinkCapability, Linker, LinkerMode};
pub use lock::PackageInstallLock;
pub use lockfile::{LockedPackage, Lockfile};
pub use materialize::{MaterialiseReport, materialise_dir, materialise_package};
pub use paths::StorePaths;
