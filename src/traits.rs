use crate::error::Result;

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
    async fn find_resource(
        &self,
        canonical_url: &str,
    ) -> Result<Option<crate::sqlite_storage::ResourceIndex>>;
    async fn list_packages(&self) -> Result<Vec<crate::sqlite_storage::PackageInfo>>;
}
