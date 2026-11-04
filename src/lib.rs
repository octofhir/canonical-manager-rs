//! FHIR Canonical Manager compatibility facade.
//!
//! Existing features and import paths are preserved. Consumers that only need local
//! storage, package extraction, search and canonical resolution can depend on
//! `octofhir-canonical-core` with default features disabled to omit HTTP and TLS.
pub use octofhir_canonical_core::*;
