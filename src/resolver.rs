//! Canonical URL resolution

use crate::error::{ResolutionError, Result};
use crate::package::FhirResource;
use crate::storage::{IndexedStorage, PackageInfo, ResourceMetadata};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, info, warn};
use url::Url;

/// Service for resolving canonical URLs to FHIR resources.
///
/// The `CanonicalResolver` provides sophisticated canonical URL resolution
/// with support for version fallbacks, fuzzy matching, and configurable
/// resolution strategies. It works with the indexed storage system to
/// efficiently locate FHIR resources.
///
/// # Example
///
/// ```rust,no_run
/// use octofhir_canonical_manager::resolver::CanonicalResolver;
/// use octofhir_canonical_manager::storage::IndexedStorage;
/// use octofhir_canonical_manager::config::StorageConfig;
/// use std::sync::Arc;
/// use std::path::PathBuf;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = StorageConfig {
///     cache_dir: PathBuf::from("/tmp/cache"),
///     index_dir: PathBuf::from("/tmp/index"),
///     packages_dir: PathBuf::from("/tmp/packages"),
///     max_cache_size: "1GB".to_string(),
/// };
/// let storage = Arc::new(IndexedStorage::new(config).await?);
/// let resolver = CanonicalResolver::new(storage);
///
/// let resolved = resolver.resolve("http://hl7.org/fhir/Patient").await?;
/// println!("Resolved to: {}", resolved.canonical_url);
/// # Ok(())
/// # }
/// ```
pub struct CanonicalResolver {
    storage: Arc<IndexedStorage>,
    resolution_config: ResolutionConfig,
}

/// Configuration for canonical URL resolution behavior.
///
/// Controls how the resolver handles version preferences, fuzzy matching,
/// and package priorities during resolution.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_manager::resolver::{ResolutionConfig, VersionPreference};
///
/// let config = ResolutionConfig {
///     package_priorities: vec!["hl7.fhir.us.core".to_string()],
///     version_preference: VersionPreference::Latest,
///     fuzzy_matching_threshold: 0.8,
///     enable_fuzzy_matching: true,
/// };
/// ```
#[derive(Debug, Clone)]
pub struct ResolutionConfig {
    pub package_priorities: Vec<String>,
    pub version_preference: VersionPreference,
    pub fuzzy_matching_threshold: f64,
    pub enable_fuzzy_matching: bool,
}

/// Strategy for resolving version conflicts during canonical URL resolution.
///
/// Determines how the resolver should handle cases where multiple versions
/// of a resource exist for the same canonical URL.
///
/// # Variants
///
/// * `Latest` - Always prefer the newest version
/// * `Exact` - Only match exact versions specified
/// * `Compatible` - Use semantic versioning compatibility rules
#[derive(Debug, Clone)]
pub enum VersionPreference {
    Latest,
    Exact,
    Compatible,
}

impl Default for ResolutionConfig {
    fn default() -> Self {
        Self {
            package_priorities: vec![],
            version_preference: VersionPreference::Latest,
            fuzzy_matching_threshold: 0.8,
            enable_fuzzy_matching: true,
        }
    }
}

/// Result of successful canonical URL resolution.
///
/// Contains the resolved FHIR resource along with metadata about
/// how the resolution was performed and which package provided the resource.
///
/// # Example
///
/// ```rust,no_run
/// # use octofhir_canonical_manager::resolver::ResolvedResource;
/// # fn example(resolved: ResolvedResource) {
/// println!("Resolved URL: {}", resolved.canonical_url);
/// println!("Resource type: {}", resolved.resource.resource_type);
/// println!("From package: {}@{}", resolved.package_info.name, resolved.package_info.version);
/// # }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolvedResource {
    pub canonical_url: String,
    pub resource: FhirResource,
    pub package_info: PackageInfo,
    pub resolution_path: ResolutionPath,
    pub metadata: ResourceMetadata,
}

/// Information about how a canonical URL was resolved.
///
/// Provides transparency into the resolution process, indicating whether
/// an exact match was found or if fallback mechanisms were used.
///
/// # Variants
///
/// * `ExactMatch` - Direct match found for the requested URL
/// * `VersionFallback` - Resolved by falling back to a different version
/// * `PackageFallback` - Resolved by searching alternative packages
/// * `FuzzyMatch` - Resolved using fuzzy string matching
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResolutionPath {
    ExactMatch,
    VersionFallback { requested: String, resolved: String },
    PackageFallback { packages: Vec<String> },
    FuzzyMatch { similarity: f64 },
}

impl CanonicalResolver {
    /// Creates a new canonical resolver with default configuration.
    ///
    /// # Arguments
    ///
    /// * `storage` - Shared reference to the indexed storage system
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use octofhir_canonical_manager::resolver::CanonicalResolver;
    /// use octofhir_canonical_manager::storage::IndexedStorage;
    /// use octofhir_canonical_manager::config::StorageConfig;
    /// use std::sync::Arc;
    /// use std::path::PathBuf;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = StorageConfig {
    ///     cache_dir: PathBuf::from("/tmp/cache"),
    ///     index_dir: PathBuf::from("/tmp/index"),
    ///     packages_dir: PathBuf::from("/tmp/packages"),
    ///     max_cache_size: "1GB".to_string(),
    /// };
    /// let storage = Arc::new(IndexedStorage::new(config).await?);
    /// let resolver = CanonicalResolver::new(storage);
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(storage: Arc<IndexedStorage>) -> Self {
        Self {
            storage,
            resolution_config: ResolutionConfig::default(),
        }
    }

    /// Creates a new canonical resolver with custom configuration.
    ///
    /// # Arguments
    ///
    /// * `storage` - Shared reference to the indexed storage system
    /// * `config` - Custom resolution configuration
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use octofhir_canonical_manager::resolver::{CanonicalResolver, ResolutionConfig, VersionPreference};
    /// use octofhir_canonical_manager::storage::IndexedStorage;
    /// use octofhir_canonical_manager::config::StorageConfig;
    /// use std::sync::Arc;
    /// use std::path::PathBuf;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let storage_config = StorageConfig {
    ///     cache_dir: PathBuf::from("/tmp/cache"),
    ///     index_dir: PathBuf::from("/tmp/index"),
    ///     packages_dir: PathBuf::from("/tmp/packages"),
    ///     max_cache_size: "1GB".to_string(),
    /// };
    /// let storage = Arc::new(IndexedStorage::new(storage_config).await?);
    ///
    /// let config = ResolutionConfig {
    ///     version_preference: VersionPreference::Latest,
    ///     enable_fuzzy_matching: false,
    ///     ..Default::default()
    /// };
    ///
    /// let resolver = CanonicalResolver::with_config(storage, config);
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_config(storage: Arc<IndexedStorage>, config: ResolutionConfig) -> Self {
        Self {
            storage,
            resolution_config: config,
        }
    }

    /// Resolves a canonical URL to a FHIR resource.
    ///
    /// Attempts to find a FHIR resource matching the given canonical URL using
    /// various resolution strategies including exact matching, version fallbacks,
    /// and fuzzy matching (if enabled).
    ///
    /// # Arguments
    ///
    /// * `canonical_url` - The canonical URL to resolve
    ///
    /// # Returns
    ///
    /// * `Ok(ResolvedResource)` - Successfully resolved resource with metadata
    /// * `Err(ResolutionError)` - If no matching resource could be found
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::resolver::CanonicalResolver;
    /// # async fn example(resolver: CanonicalResolver) -> Result<(), Box<dyn std::error::Error>> {
    /// let resolved = resolver.resolve("http://hl7.org/fhir/Patient").await?;
    /// println!("Resource type: {}", resolved.resource.resource_type);
    /// println!("Resolution method: {:?}", resolved.resolution_path);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn resolve(&self, canonical_url: &str) -> Result<ResolvedResource> {
        info!("Resolving canonical URL: {}", canonical_url);

        // Step 1: Try exact match
        if let Some(resource_index) = self.storage.find_by_canonical(canonical_url) {
            debug!("Found exact match for canonical URL");
            return self
                .build_resolved_resource(canonical_url, resource_index, ResolutionPath::ExactMatch)
                .await;
        }

        // Step 2: Try version fallback (remove version from URL or find versioned variants)
        if let Ok(parsed_url) = Url::parse(canonical_url) {
            let base_url = self.extract_base_url(&parsed_url)?;

            // Try to find latest version for the base URL
            if let Some(resource_index) = self.find_latest_version(&base_url).await? {
                // Only use version fallback if we found a different URL
                if resource_index.canonical_url != canonical_url {
                    debug!("Found version fallback match");
                    let resolved_url = resource_index.canonical_url.clone();
                    return self
                        .build_resolved_resource(
                            canonical_url,
                            resource_index,
                            ResolutionPath::VersionFallback {
                                requested: canonical_url.to_string(),
                                resolved: resolved_url,
                            },
                        )
                        .await;
                }
            }
        }

        // Step 3: Try fuzzy matching if enabled
        if self.resolution_config.enable_fuzzy_matching {
            if let Some((resource_index, similarity)) = self.fuzzy_match(canonical_url).await? {
                debug!("Found fuzzy match with similarity: {:.2}", similarity);
                return self
                    .build_resolved_resource(
                        canonical_url,
                        resource_index,
                        ResolutionPath::FuzzyMatch { similarity },
                    )
                    .await;
            }
        }

        Err(ResolutionError::CanonicalUrlNotFound {
            url: canonical_url.to_string(),
        }
        .into())
    }

    /// Resolves a canonical URL to a specific version of a FHIR resource.
    ///
    /// Attempts to find a FHIR resource matching both the canonical URL and
    /// the specified version. This is more restrictive than regular resolution.
    ///
    /// # Arguments
    ///
    /// * `canonical_url` - The canonical URL to resolve
    /// * `version` - The specific version to match
    ///
    /// # Returns
    ///
    /// * `Ok(ResolvedResource)` - Successfully resolved resource with exact version
    /// * `Err(ResolutionError)` - If no matching resource with that version is found
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::resolver::CanonicalResolver;
    /// # async fn example(resolver: CanonicalResolver) -> Result<(), Box<dyn std::error::Error>> {
    /// let resolved = resolver.resolve_with_version(
    ///     "http://hl7.org/fhir/Patient",
    ///     "4.0.1"
    /// ).await?;
    /// println!("Found version: {:?}", resolved.resource.version);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn resolve_with_version(
        &self,
        canonical_url: &str,
        version: &str,
    ) -> Result<ResolvedResource> {
        info!(
            "Resolving canonical URL: {} with version: {}",
            canonical_url, version
        );

        // First try with version-specific URL if not already included
        let versioned_url = if canonical_url.contains(&format!("/{version}")) {
            canonical_url.to_string()
        } else {
            format!("{}/{}", canonical_url.trim_end_matches('/'), version)
        };

        // Try exact match with versioned URL
        if let Some(resource_index) = self.storage.find_by_canonical(&versioned_url) {
            debug!("Found exact version match");
            return self
                .build_resolved_resource(canonical_url, resource_index, ResolutionPath::ExactMatch)
                .await;
        }

        // Try to find resource with specific version in metadata
        let resources_by_type = self.get_resources_by_base_url(canonical_url).await?;
        for resource_index in resources_by_type {
            if let Some(resource_version) = &resource_index.metadata.version {
                if resource_version == version {
                    debug!("Found resource with matching version in metadata");
                    return self
                        .build_resolved_resource(
                            canonical_url,
                            resource_index,
                            ResolutionPath::ExactMatch,
                        )
                        .await;
                }
            }
        }

        Err(ResolutionError::CanonicalUrlNotFound {
            url: format!("{canonical_url}@{version}"),
        }
        .into())
    }

    /// Resolves multiple canonical URLs in a single operation.
    ///
    /// Attempts to resolve each URL independently, returning all successful
    /// resolutions. Failed resolutions are logged but do not prevent other
    /// URLs from being resolved.
    ///
    /// # Arguments
    ///
    /// * `urls` - Slice of canonical URLs to resolve
    ///
    /// # Returns
    ///
    /// * `Ok(Vec<ResolvedResource>)` - Vector of successfully resolved resources
    /// * `Err(ResolutionError)` - Only if all URLs failed to resolve
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::resolver::CanonicalResolver;
    /// # async fn example(resolver: CanonicalResolver) -> Result<(), Box<dyn std::error::Error>> {
    /// let urls = vec![
    ///     "http://hl7.org/fhir/Patient".to_string(),
    ///     "http://hl7.org/fhir/Observation".to_string(),
    /// ];
    ///
    /// let resolved = resolver.batch_resolve(&urls).await?;
    /// println!("Resolved {} out of {} URLs", resolved.len(), urls.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn batch_resolve(&self, urls: &[String]) -> Result<Vec<ResolvedResource>> {
        info!("Batch resolving {} URLs", urls.len());

        let mut results = Vec::new();
        let mut errors = Vec::new();

        for url in urls {
            match self.resolve(url).await {
                Ok(resolved) => results.push(resolved),
                Err(e) => {
                    warn!("Failed to resolve URL {}: {}", url, e);
                    errors.push((url.clone(), e));
                }
            }
        }

        if results.is_empty() && !errors.is_empty() {
            return Err(ResolutionError::CanonicalUrlNotFound {
                url: format!("Batch resolution failed for all {} URLs", urls.len()),
            }
            .into());
        }

        debug!(
            "Batch resolved {}/{} URLs successfully",
            results.len(),
            urls.len()
        );
        Ok(results)
    }

    /// Returns a list of all canonical URLs available in storage.
    ///
    /// This provides a complete inventory of all FHIR resources that can
    /// be resolved by this resolver instance.
    ///
    /// # Returns
    ///
    /// Vector of all canonical URLs currently indexed in storage.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::resolver::CanonicalResolver;
    /// # fn example(resolver: CanonicalResolver) {
    /// let urls = resolver.list_canonical_urls();
    /// println!("Total resources available: {}", urls.len());
    /// for url in urls.iter().take(5) {
    ///     println!("  {}", url);
    /// }
    /// # }
    /// ```
    pub fn list_canonical_urls(&self) -> Vec<String> {
        self.storage.get_all_canonical_urls()
    }

    /// Extract base URL by removing version components
    fn extract_base_url(&self, parsed_url: &Url) -> Result<String> {
        let path = parsed_url.path();
        let segments: Vec<&str> = path.split('/').collect();

        // Look for version patterns like /1.0.0, /v1.0.0, etc.
        let mut base_segments = Vec::new();
        for segment in &segments {
            if segment.is_empty() {
                continue;
            }

            // Check if this looks like a version (starts with v or is numeric)
            if self.looks_like_version(segment) {
                break;
            }
            base_segments.push(*segment);
        }

        let mut base_url = parsed_url.clone();
        base_url.set_path(&format!("/{}", base_segments.join("/")));
        Ok(base_url.to_string())
    }

    /// Check if a string looks like a version identifier
    fn looks_like_version(&self, segment: &str) -> bool {
        // Version patterns: 1.0.0, v1.0.0, 1.0, v1.0, etc.
        if let Some(stripped) = segment.strip_prefix('v') {
            return stripped.chars().next().is_some_and(|c| c.is_ascii_digit());
        }

        // Check if starts with digit and contains dots
        segment.chars().next().is_some_and(|c| c.is_ascii_digit()) && segment.contains('.')
    }

    /// Find the latest version of a resource by base URL
    async fn find_latest_version(
        &self,
        base_url: &str,
    ) -> Result<Option<crate::storage::ResourceIndex>> {
        let cache_entries = self.storage.get_cache_entries();

        let mut matching_resources = Vec::new();
        for (url, resource_index) in cache_entries {
            if self.is_version_of_base_url(&url, base_url)? {
                matching_resources.push(resource_index);
            }
        }

        if matching_resources.is_empty() {
            return Ok(None);
        }

        // Sort by version if available, otherwise by canonical URL
        matching_resources.sort_by(|a, b| {
            match (&a.metadata.version, &b.metadata.version) {
                (Some(v1), Some(v2)) => self.compare_versions(v2, v1), // Reverse for latest first
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => b.canonical_url.cmp(&a.canonical_url),
            }
        });

        Ok(matching_resources.into_iter().next())
    }

    /// Check if a URL is a version of a base URL
    fn is_version_of_base_url(&self, url: &str, base_url: &str) -> Result<bool> {
        if let (Ok(parsed_url), Ok(parsed_base)) = (Url::parse(url), Url::parse(base_url)) {
            if parsed_url.scheme() != parsed_base.scheme()
                || parsed_url.host() != parsed_base.host()
            {
                return Ok(false);
            }

            let url_path = parsed_url.path().trim_end_matches('/');
            let base_path = parsed_base.path().trim_end_matches('/');

            // Check if URL path starts with base path followed by version-like segment
            if let Some(remainder) = url_path.strip_prefix(base_path) {
                if let Some(version_part) = remainder.strip_prefix('/') {
                    return Ok(self.looks_like_version(version_part));
                }
            }
        }

        Ok(false)
    }

    /// Simple version comparison (semantic versioning-like)
    fn compare_versions(&self, v1: &str, v2: &str) -> std::cmp::Ordering {
        let parse_version = |v: &str| -> Vec<u32> {
            v.trim_start_matches('v')
                .split('.')
                .filter_map(|s| s.parse::<u32>().ok())
                .collect()
        };

        let version1 = parse_version(v1);
        let version2 = parse_version(v2);

        version1.cmp(&version2)
    }

    /// Get all resources that match a base URL pattern
    async fn get_resources_by_base_url(
        &self,
        base_url: &str,
    ) -> Result<Vec<crate::storage::ResourceIndex>> {
        let cache_entries = self.storage.get_cache_entries();

        let mut matching_resources = Vec::new();
        for (url, resource_index) in cache_entries {
            if url.starts_with(base_url) || self.is_version_of_base_url(&url, base_url)? {
                matching_resources.push(resource_index);
            }
        }

        Ok(matching_resources)
    }

    /// Perform fuzzy matching on canonical URLs
    async fn fuzzy_match(
        &self,
        canonical_url: &str,
    ) -> Result<Option<(crate::storage::ResourceIndex, f64)>> {
        let cache_entries = self.storage.get_cache_entries();

        let mut best_match = None;
        let mut best_similarity = 0.0;

        for (url, resource_index) in cache_entries {
            let similarity = self.calculate_similarity(canonical_url, &url);

            if similarity > self.resolution_config.fuzzy_matching_threshold
                && similarity > best_similarity
            {
                best_similarity = similarity;
                best_match = Some(resource_index);
            }
        }

        if let Some(resource_index) = best_match {
            Ok(Some((resource_index, best_similarity)))
        } else {
            Ok(None)
        }
    }

    /// Calculate string similarity using Levenshtein distance
    fn calculate_similarity(&self, s1: &str, s2: &str) -> f64 {
        let len1 = s1.len();
        let len2 = s2.len();

        if len1 == 0 {
            return if len2 == 0 { 1.0 } else { 0.0 };
        }
        if len2 == 0 {
            return 0.0;
        }

        let max_len = len1.max(len2);
        let distance = self.levenshtein_distance(s1, s2);

        1.0 - (distance as f64 / max_len as f64)
    }

    /// Calculate Levenshtein distance between two strings
    fn levenshtein_distance(&self, s1: &str, s2: &str) -> usize {
        let chars1: Vec<char> = s1.chars().collect();
        let chars2: Vec<char> = s2.chars().collect();
        let len1 = chars1.len();
        let len2 = chars2.len();

        let mut matrix = vec![vec![0; len2 + 1]; len1 + 1];

        for (i, item) in matrix.iter_mut().enumerate().take(len1 + 1) {
            item[0] = i;
        }
        for j in 0..=len2 {
            matrix[0][j] = j;
        }

        for i in 1..=len1 {
            for j in 1..=len2 {
                let cost = if chars1[i - 1] == chars2[j - 1] { 0 } else { 1 };
                matrix[i][j] = (matrix[i - 1][j] + 1)
                    .min(matrix[i][j - 1] + 1)
                    .min(matrix[i - 1][j - 1] + cost);
            }
        }

        matrix[len1][len2]
    }

    /// Build a resolved resource from a resource index
    async fn build_resolved_resource(
        &self,
        requested_url: &str,
        resource_index: crate::storage::ResourceIndex,
        resolution_path: ResolutionPath,
    ) -> Result<ResolvedResource> {
        // Get the full resource content
        let resource = self.storage.get_resource(&resource_index).await?;

        // Get package info
        let packages = self.storage.list_packages().await?;
        let package_info = packages
            .into_iter()
            .find(|p| {
                p.name == resource_index.package_name && p.version == resource_index.package_version
            })
            .ok_or_else(|| ResolutionError::CanonicalUrlNotFound {
                url: format!(
                    "Package {}@{} not found",
                    resource_index.package_name, resource_index.package_version
                ),
            })?;

        Ok(ResolvedResource {
            canonical_url: requested_url.to_string(),
            resource,
            package_info,
            resolution_path,
            metadata: resource_index.metadata,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StorageConfig;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_resolver_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            cache_dir: temp_dir.path().join("cache"),
            index_dir: temp_dir.path().join("index"),
            packages_dir: temp_dir.path().join("packages"),
            max_cache_size: "100MB".to_owned(),
        };

        let storage = Arc::new(IndexedStorage::new(config).await.unwrap());
        let resolver = CanonicalResolver::new(storage);

        assert!(resolver.list_canonical_urls().is_empty());
    }

    #[tokio::test]
    async fn test_url_not_found() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            cache_dir: temp_dir.path().join("cache"),
            index_dir: temp_dir.path().join("index"),
            packages_dir: temp_dir.path().join("packages"),
            max_cache_size: "100MB".to_owned(),
        };

        let storage = Arc::new(IndexedStorage::new(config).await.unwrap());
        let resolver = CanonicalResolver::new(storage);

        let result = resolver.resolve("http://example.com/missing").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_version_detection() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            cache_dir: temp_dir.path().join("cache"),
            index_dir: temp_dir.path().join("index"),
            packages_dir: temp_dir.path().join("packages"),
            max_cache_size: "100MB".to_owned(),
        };

        let storage = Arc::new(IndexedStorage::new(config).await.unwrap());
        let resolver = CanonicalResolver::new(storage);

        assert!(resolver.looks_like_version("1.0.0"));
        assert!(resolver.looks_like_version("v1.0.0"));
        assert!(resolver.looks_like_version("2.1"));
        assert!(!resolver.looks_like_version("patient"));
        assert!(!resolver.looks_like_version("v"));
    }
}
