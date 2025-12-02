use crate::domain::{PackageInfo, ResourceIndex};
use crate::error::Result;
use crate::package::FhirResource;
use std::collections::HashMap;

// Domain traits to decouple infrastructure from callers

pub trait Registry {
    fn get_package_metadata(
        &self,
        name: &str,
        version: &str,
    ) -> crate::error::Result<crate::registry::PackageMetadata>;
    fn download_package(
        &self,
        spec: &crate::config::PackageSpec,
    ) -> crate::error::Result<crate::registry::PackageDownload>;
}

#[async_trait::async_trait]
pub trait AsyncRegistry {
    async fn get_package_metadata(
        &self,
        name: &str,
        version: &str,
    ) -> crate::error::Result<crate::registry::PackageMetadata>;
    async fn download_package(
        &self,
        spec: &crate::config::PackageSpec,
    ) -> crate::error::Result<crate::registry::PackageDownload>;
}

#[async_trait::async_trait]
pub trait PackageStore {
    async fn add_package(&self, package: &crate::package::ExtractedPackage) -> Result<()>;
    async fn remove_package(&self, name: &str, version: &str) -> Result<bool>;
    async fn find_resource(&self, canonical_url: &str) -> Result<Option<ResourceIndex>>;
    async fn list_packages(&self) -> Result<Vec<PackageInfo>>;
}

/// Trait for search storage operations.
///
/// This trait abstracts the storage backend used for resolving canonical URLs
/// and searching FHIR resources. It allows different implementations (SQLite,
/// PostgreSQL, etc.) to be used interchangeably.
#[async_trait::async_trait]
pub trait SearchStorage: Send + Sync {
    /// Find a resource by its canonical URL
    async fn find_resource(&self, canonical_url: &str) -> Result<Option<ResourceIndex>>;

    /// Find a resource by canonical URL and FHIR version
    async fn find_resource_with_fhir_version(
        &self,
        canonical_url: &str,
        fhir_version: &str,
    ) -> Result<Option<ResourceIndex>>;

    /// Find all resources matching a base URL pattern
    async fn find_by_base_url(&self, base_url: &str) -> Result<Vec<ResourceIndex>>;

    /// Find the latest resource version by base URL
    async fn find_latest_by_base_url(&self, base_url: &str) -> Result<Option<ResourceIndex>>;

    /// Find a resource by its name field (case-insensitive)
    async fn find_resource_by_name(&self, name: &str) -> Result<Option<ResourceIndex>>;

    /// Find resources by exact resource type and ID match (case-insensitive)
    async fn find_by_type_and_id(
        &self,
        resource_type: String,
        id: String,
    ) -> Result<Vec<ResourceIndex>>;

    /// Find resources by exact resource type and name match (case-insensitive)
    async fn find_by_type_and_name(
        &self,
        resource_type: String,
        name: String,
    ) -> Result<Vec<ResourceIndex>>;

    /// Find resource by key with type filtering and priority ordering
    async fn find_resource_info(
        &self,
        key: &str,
        types: Option<&[&str]>,
        exclude_extensions: bool,
        sort_by_priority: bool,
    ) -> Result<Option<ResourceIndex>>;

    /// Find all matching resources by key with type filtering
    async fn find_resource_infos(
        &self,
        key: &str,
        types: Option<&[&str]>,
        limit: Option<usize>,
    ) -> Result<Vec<ResourceIndex>>;

    /// List all base FHIR resource type names for a given FHIR version
    async fn list_base_resource_type_names(&self, fhir_version: &str) -> Result<Vec<String>>;

    /// Get the full FHIR resource content from a resource index
    async fn get_resource(&self, resource_index: &ResourceIndex) -> Result<FhirResource>;

    /// Get all cache entries (for building text index)
    async fn get_cache_entries(&self) -> HashMap<String, ResourceIndex>;

    /// List all installed packages
    async fn list_packages(&self) -> Result<Vec<PackageInfo>>;
}
