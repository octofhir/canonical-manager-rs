//! Canonical URL resolution

use crate::binary_storage::{BinaryStorage, PackageInfo, ResourceMetadata};
use crate::domain::{CanonicalWithVersion, PackageVersion};
use crate::error::{ResolutionError, Result};
use crate::package::FhirResource;
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
/// use octofhir_canonical_manager::binary_storage::BinaryStorage;
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
/// let storage = Arc::new(BinaryStorage::new(config).await?);
/// let resolver = CanonicalResolver::new(storage);
///
/// let resolved = resolver.resolve("http://hl7.org/fhir/Patient").await?;
/// println!("Resolved to: {}", resolved.canonical_url);
/// # Ok(())
/// # }
/// ```
pub struct CanonicalResolver {
    storage: Arc<BinaryStorage>,
    resolution_config: ResolutionConfig,
    #[cfg(feature = "fuzzy-search")]
    fuzzy_index: crate::fuzzy::NGramIndex,
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
///     #[cfg(feature = "fuzzy-search")]
///     fuzzy_max_candidates: 200,
/// };
/// ```
#[derive(Debug, Clone)]
pub struct ResolutionConfig {
    pub package_priorities: Vec<String>,
    pub version_preference: VersionPreference,
    pub fuzzy_matching_threshold: f64,
    pub enable_fuzzy_matching: bool,
    #[cfg(feature = "fuzzy-search")]
    pub fuzzy_max_candidates: usize,
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
            enable_fuzzy_matching: false,
            #[cfg(feature = "fuzzy-search")]
            fuzzy_max_candidates: 200,
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
    /// use octofhir_canonical_manager::binary_storage::BinaryStorage;
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
    /// let storage = Arc::new(BinaryStorage::new(config).await?);
    /// let resolver = CanonicalResolver::new(storage);
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(storage: Arc<BinaryStorage>) -> Self {
        #[cfg(feature = "fuzzy-search")]
        let fuzzy_index = {
            let urls: Vec<String> = storage.get_cache_entries().keys().cloned().collect();
            crate::fuzzy::NGramIndex::build_from_urls(urls)
        };
        Self {
            storage,
            resolution_config: ResolutionConfig::default(),
            #[cfg(feature = "fuzzy-search")]
            fuzzy_index,
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
    /// use octofhir_canonical_manager::binary_storage::BinaryStorage;
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
    /// let storage = Arc::new(BinaryStorage::new(storage_config).await?);
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
    pub fn with_config(storage: Arc<BinaryStorage>, config: ResolutionConfig) -> Self {
        #[cfg(feature = "fuzzy-search")]
        let fuzzy_index = {
            let urls: Vec<String> = storage.get_cache_entries().keys().cloned().collect();
            crate::fuzzy::NGramIndex::build_from_urls(urls)
        };
        Self {
            storage,
            resolution_config: config,
            #[cfg(feature = "fuzzy-search")]
            fuzzy_index,
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
    #[tracing::instrument(name = "resolver.resolve", skip(self), fields(canonical = %canonical_url))]
    pub async fn resolve(&self, canonical_url: &str) -> Result<ResolvedResource> {
        info!("Resolving canonical URL: {}", canonical_url);
        #[cfg(feature = "metrics")]
        let start = std::time::Instant::now();

        // Support `url|version` form per FHIR semantics
        let parsed = CanonicalWithVersion::parse(canonical_url);
        let base = parsed.canonical.clone();

        // Step 1: Try exact match
        if let Some(resource_index) = self.storage.find_resource(&base).await? {
            debug!("Found exact match for canonical URL");
            return self
                .build_resolved_resource(&base, resource_index, ResolutionPath::ExactMatch)
                .await;
        }

        // Step 2: If a version is specified, try to resolve exact or compatible version
        if let Some(ver) = parsed.version.clone() {
            if let Some(resource_index) = self.resolve_with_version_candidates(&base, &ver).await? {
                debug!("Resolved via version-specific selection");
                return self
                    .build_resolved_resource(&base, resource_index, ResolutionPath::ExactMatch)
                    .await;
            }
        }

        // Step 3: Try fuzzy matching first if enabled
        if self.resolution_config.enable_fuzzy_matching {
            if let Some((resource_index, similarity)) = self.fuzzy_match(&base).await? {
                debug!("Found fuzzy match with similarity: {:.2}", similarity);
                return self
                    .build_resolved_resource(
                        &base,
                        resource_index,
                        ResolutionPath::FuzzyMatch { similarity },
                    )
                    .await;
            }
        }

        // Step 4: Try version fallback only when the last path segment looks like a version
        if let Ok(parsed_url) = Url::parse(&base) {
            let base_url = self.extract_base_url(&parsed_url)?;
            let last_seg = parsed_url
                .path()
                .trim_end_matches('/')
                .rsplit('/')
                .next()
                .unwrap_or("");
            let looks_like_ver = self.looks_like_version(last_seg);

            if looks_like_ver {
                // Try to find latest version for the base URL via storage pre-index
                if let Some(resource_index) =
                    self.storage.find_latest_by_base_url(&base_url).await?
                {
                    // Only use version fallback if we found a different URL
                    if resource_index.canonical_url != base {
                        debug!("Found version fallback match");
                        let resolved_url = resource_index.canonical_url.clone();
                        return self
                            .build_resolved_resource(
                                &base,
                                resource_index,
                                ResolutionPath::VersionFallback {
                                    requested: base.to_string(),
                                    resolved: resolved_url,
                                },
                            )
                            .await;
                    }
                }
            }
        }

        let result = Err(ResolutionError::CanonicalUrlNotFound { url: base }.into());

        #[cfg(feature = "metrics")]
        {
            metrics::histogram!("resolve_latency_ms", start.elapsed().as_secs_f64() * 1000.0);
        }

        result
    }

    async fn resolve_with_version_candidates(
        &self,
        base_canonical: &str,
        desired: &PackageVersion,
    ) -> Result<Option<crate::binary_storage::ResourceIndex>> {
        // Collect candidates under the same base
        let mut candidates = self.storage.find_by_base_url(base_canonical).await?;
        if candidates.is_empty() {
            return Ok(None);
        }

        // Try exact metadata.version match
        if let Some(exact) = candidates.iter().find(|ri| {
            ri.metadata
                .version
                .as_ref()
                .map(|v| v == &desired.original)
                .unwrap_or(false)
        }) {
            return Ok(Some(exact.clone()));
        }

        // If compatibility policy is enabled, select highest compatible version (same major via caret)
        if matches!(
            self.resolution_config.version_preference,
            VersionPreference::Compatible
        ) {
            if let Some(want) = &desired.semver {
                let req = semver::VersionReq::parse(&format!("^{want}"))
                    .unwrap_or(semver::VersionReq::STAR);
                candidates.sort_by(|a, b| {
                    let va = a.metadata.version.as_deref().unwrap_or("");
                    let vb = b.metadata.version.as_deref().unwrap_or("");
                    match (
                        semver::Version::parse(va.trim_start_matches('v')),
                        semver::Version::parse(vb.trim_start_matches('v')),
                    ) {
                        (Ok(va), Ok(vb)) => vb.cmp(&va),
                        _ => vb.cmp(va),
                    }
                });
                for ri in candidates {
                    if let Some(vs) = ri.metadata.version.as_deref() {
                        if let Ok(v) = semver::Version::parse(vs.trim_start_matches('v')) {
                            if req.matches(&v) {
                                return Ok(Some(ri));
                            }
                        }
                    }
                }
            }
        }
        Ok(None)
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
        if let Some(resource_index) = self.storage.find_resource(&versioned_url).await? {
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
        self.storage.get_cache_entries().keys().cloned().collect()
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

    // Find the latest version of a resource by base URL
    // Helper methods for legacy fallback have been removed in favor of storage pre-index lookups

    /// Get all resources that match a base URL pattern
    async fn get_resources_by_base_url(
        &self,
        base_url: &str,
    ) -> Result<Vec<crate::binary_storage::ResourceIndex>> {
        // Use storage pre-index for base URL lookups
        self.storage.find_by_base_url(base_url).await
    }

    /// Perform fuzzy matching on canonical URLs
    async fn fuzzy_match(
        &self,
        canonical_url: &str,
    ) -> Result<Option<(crate::binary_storage::ResourceIndex, f64)>> {
        #[cfg(feature = "fuzzy-search")]
        {
            let max_candidates = self.resolution_config.fuzzy_max_candidates;
            let candidates = self
                .fuzzy_index
                .query(canonical_url, max_candidates)
                .into_iter()
                .map(|(u, _)| u)
                .collect::<Vec<_>>();
            let cache = self.storage.get_cache_entries();
            let mut best: Option<(crate::binary_storage::ResourceIndex, f64)> = None;
            for u in candidates {
                if let Some(idx) = cache.get(&u) {
                    let sim = self.calculate_similarity(canonical_url, &u);
                    if sim > self.resolution_config.fuzzy_matching_threshold {
                        if let Some((_, best_sim)) = &best {
                            if sim > *best_sim {
                                best = Some((idx.clone(), sim));
                            }
                        } else {
                            best = Some((idx.clone(), sim));
                        }
                    }
                }
            }
            Ok(best)
        }
        #[cfg(not(feature = "fuzzy-search"))]
        {
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
        resource_index: crate::binary_storage::ResourceIndex,
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

        let storage = Arc::new(BinaryStorage::new(config).await.unwrap());
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

        let storage = Arc::new(BinaryStorage::new(config).await.unwrap());
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

        let storage = Arc::new(BinaryStorage::new(config).await.unwrap());
        let resolver = CanonicalResolver::new(storage);

        assert!(resolver.looks_like_version("1.0.0"));
        assert!(resolver.looks_like_version("v1.0.0"));
        assert!(resolver.looks_like_version("2.1"));
        assert!(!resolver.looks_like_version("patient"));
        assert!(!resolver.looks_like_version("v"));
    }
}
