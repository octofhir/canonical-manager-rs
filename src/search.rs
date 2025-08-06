//! Search engine for FHIR resources

use crate::binary_storage::{BinaryStorage, ResourceIndex};
use crate::error::Result;
use crate::package::FhirResource;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use tracing::{debug, info};

/// Full-text search engine for FHIR resources.
///
/// Provides comprehensive search capabilities across indexed FHIR resources
/// with support for text queries, filtering, faceted search, and result ranking.
/// The search engine builds an inverted text index for fast query processing.
///
/// # Example
///
/// ```rust,no_run
/// use octofhir_canonical_manager::search::{SearchEngine, SearchQuery};
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
/// let engine = SearchEngine::new(storage);
///
/// let query = SearchQuery {
///     text: Some("Patient".to_string()),
///     resource_types: vec!["Patient".to_string()],
///     ..Default::default()
/// };
///
/// let results = engine.search(&query).await?;
/// println!("Found {} resources", results.total_count);
/// # Ok(())
/// # }
/// ```
pub struct SearchEngine {
    storage: Arc<BinaryStorage>,
    text_index: TextIndex,
    filters: FilterEngine,
    // Simple LRU-style cache for search results
    search_cache: RwLock<HashMap<String, (SearchResult, Instant)>>,
    cache_ttl: Duration,
}

/// Inverted text index for fast full-text search.
///
/// Maintains a mapping from search terms to the canonical URLs of resources
/// that contain those terms. This enables efficient text-based queries
/// across all indexed FHIR resources.
///
/// Uses HashSet for deduplication and RwLock for thread-safe concurrent access,
/// optimized for high-load scenarios.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_manager::search::TextIndex;
///
/// let mut index = TextIndex::new();
/// index.add_resource("http://example.com/Patient/1", "Patient John Doe");
///
/// let results = index.search("patient");
/// assert_eq!(results.len(), 1);
/// ```
#[derive(Debug)]
pub struct TextIndex {
    // Memory-optimized inverted index: term -> set of resource URLs
    // Using HashSet for deduplication and faster intersection operations
    index: RwLock<HashMap<String, HashSet<String>>>,
    // Stop words to filter out during tokenization
    stop_words: HashSet<String>,
}

/// Engine for applying filters to search results.
///
/// Provides filtering capabilities based on resource type, package name,
/// canonical URL patterns, and version constraints. Filters are applied
/// after the initial text search to narrow down results.
#[derive(Debug, Clone)]
pub struct FilterEngine {
    // Currently empty, can be extended with more sophisticated filtering
}

/// Aggregated statistics about search results.
///
/// Provides counts of resources grouped by various attributes such as
/// resource type, package, and version. Useful for building faceted
/// search interfaces.
///
/// # Example
///
/// ```rust,no_run
/// # use octofhir_canonical_manager::search::SearchFacets;
/// # fn example(facets: SearchFacets) {
/// println!("Resource types:");
/// for (resource_type, count) in facets.resource_types {
///     println!("  {}: {} resources", resource_type, count);
/// }
/// # }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchFacets {
    pub resource_types: HashMap<String, usize>,
    pub packages: HashMap<String, usize>,
    pub versions: HashMap<String, usize>,
}

/// Builder for constructing complex search queries.
///
/// Provides a fluent interface for building search queries with various
/// filters and constraints. Helps ensure queries are well-formed and
/// makes complex query construction more readable.
///
/// # Example
///
/// ```rust,no_run
/// use octofhir_canonical_manager::search::SearchQueryBuilder;
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
///
/// let results = SearchQueryBuilder::new(storage)
///     .text("Patient")
///     .resource_type("Patient")
///     .package("hl7.fhir.us.core")
///     .limit(10)
///     .execute()
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct SearchQueryBuilder {
    storage: Arc<BinaryStorage>,
    query: SearchQuery,
}

/// Structure representing a search query with various filters and constraints.
///
/// Encapsulates all search parameters including text queries, resource type filters,
/// package constraints, and pagination settings.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_manager::search::{SearchQuery, VersionConstraint};
///
/// let query = SearchQuery {
///     text: Some("Patient demographics".to_string()),
///     resource_types: vec!["Patient".to_string(), "Person".to_string()],
///     packages: vec!["hl7.fhir.us.core".to_string()],
///     limit: Some(20),
///     offset: Some(0),
///     ..Default::default()
/// };
/// ```
#[derive(Debug, Clone, Default, Hash, Serialize, Deserialize)]
pub struct SearchQuery {
    pub text: Option<String>,
    pub resource_types: Vec<String>,
    pub packages: Vec<String>,
    pub canonical_pattern: Option<String>,
    pub version_constraints: Vec<VersionConstraint>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

/// Constraint specifying a required package version in search results.
///
/// Used to restrict search results to resources from specific package versions.
/// This is useful when you need resources from a particular version of a
/// FHIR implementation guide.
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_manager::search::VersionConstraint;
///
/// let constraint = VersionConstraint {
///     package: "hl7.fhir.us.core".to_string(),
///     version: "6.1.0".to_string(),
/// };
/// ```
#[derive(Debug, Clone, Hash, Serialize, Deserialize)]
pub struct VersionConstraint {
    pub package: String,
    pub version: String,
}

/// Results of a search operation.
///
/// Contains the matching resources along with metadata about the search
/// such as total count and query execution time.
///
/// # Example
///
/// ```rust,no_run
/// # use octofhir_canonical_manager::search::SearchResult;
/// # fn example(result: SearchResult) {
/// println!("Found {} resources in {:?}", result.total_count, result.query_time);
/// for resource_match in result.resources {
///     println!("  {}: score {:.2}", resource_match.resource.resource_type, resource_match.score);
/// }
/// # }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchResult {
    pub resources: Vec<ResourceMatch>,
    pub total_count: usize,
    pub query_time: Duration,
}

/// A single resource match from search results.
///
/// Contains the matched FHIR resource, its index metadata, relevance score,
/// and highlighted text fragments showing where matches occurred.
///
/// # Example
///
/// ```rust,no_run
/// # use octofhir_canonical_manager::search::ResourceMatch;
/// # fn example(resource_match: ResourceMatch) {
/// println!("Resource: {} (score: {:.2})",
///          resource_match.resource.resource_type,
///          resource_match.score);
///
/// for highlight in resource_match.highlights {
///     println!("  {}: {}", highlight.field, highlight.fragment);
/// }
/// # }
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceMatch {
    pub resource: FhirResource,
    pub index: ResourceIndex,
    pub score: f64,
    pub highlights: Vec<SearchHighlight>,
}

/// Highlighted text fragment showing where a search term matched.
///
/// Contains the field name where the match occurred and the text fragment
/// with highlighted search terms (typically marked with HTML tags).
///
/// # Example
///
/// ```rust
/// use octofhir_canonical_manager::search::SearchHighlight;
///
/// let highlight = SearchHighlight {
///     field: "canonical_url".to_string(),
///     fragment: "http://hl7.org/fhir/<mark>Patient</mark>".to_string(),
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchHighlight {
    pub field: String,
    pub fragment: String,
}

impl Default for TextIndex {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for TextIndex {
    fn clone(&self) -> Self {
        let index_guard = self.index.read().unwrap();
        Self {
            index: RwLock::new(index_guard.clone()),
            stop_words: self.stop_words.clone(),
        }
    }
}

impl TextIndex {
    /// Creates a new empty text index with common stop words.
    ///
    /// # Example
    ///
    /// ```rust
    /// use octofhir_canonical_manager::search::TextIndex;
    ///
    /// let index = TextIndex::new();
    /// ```
    pub fn new() -> Self {
        // Common English stop words for medical/FHIR context
        let stop_words = [
            "a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "has", "he", "in",
            "is", "it", "its", "of", "on", "that", "the", "to", "was", "will", "with", "the",
            "this", "but", "they", "have", "had", "what", "said", "each", "which", "she", "do",
            "how", "their", "if", "up", "out", "many", "then", "them", "these", "so", "some",
            "her", "would", "make", "like", "into", "him", "time", "two", "more", "go", "no",
            "way", "could", "my", "than", "first", "been", "call", "who", "oil", "sit", "now",
            "find", "down", "day", "did", "get", "come", "made", "may", "part",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();

        Self {
            index: RwLock::new(HashMap::new()),
            stop_words,
        }
    }

    /// Adds a resource's content to the text index.
    ///
    /// Tokenizes the content and updates the inverted index to include
    /// the canonical URL for each term found in the content.
    /// Thread-safe and optimized for concurrent access.
    ///
    /// # Arguments
    ///
    /// * `canonical_url` - The canonical URL of the resource
    /// * `content` - The text content to index
    ///
    /// # Example
    ///
    /// ```rust
    /// use octofhir_canonical_manager::search::TextIndex;
    ///
    /// let mut index = TextIndex::new();
    /// index.add_resource(
    ///     "http://example.com/Patient/1",
    ///     "Patient resource for John Doe"
    /// );
    /// ```
    pub fn add_resource(&self, canonical_url: &str, content: &str) {
        let terms = self.tokenize(content);
        if terms.is_empty() {
            return;
        }

        let mut index = self.index.write().unwrap();
        for term in terms {
            index
                .entry(term)
                .or_default()
                .insert(canonical_url.to_string());
        }
    }

    /// Searches for resources containing all specified terms.
    ///
    /// Returns canonical URLs of resources that contain all terms in the query.
    /// The search is case-insensitive and requires all terms to be present.
    /// Uses efficient set intersection for optimal performance.
    ///
    /// # Arguments
    ///
    /// * `query` - The search query string
    ///
    /// # Returns
    ///
    /// Vector of canonical URLs for resources matching all terms.
    ///
    /// # Example
    ///
    /// ```rust
    /// use octofhir_canonical_manager::search::TextIndex;
    ///
    /// let mut index = TextIndex::new();
    /// index.add_resource("http://example.com/Patient/1", "Patient John Doe");
    ///
    /// let results = index.search("patient john");
    /// assert_eq!(results.len(), 1);
    /// ```
    pub fn search(&self, query: &str) -> Vec<String> {
        let terms = self.tokenize(query);
        if terms.is_empty() {
            return Vec::new();
        }

        let index = self.index.read().unwrap();

        // Find the smallest set first for efficient intersection
        let mut term_sets: Vec<&HashSet<String>> = Vec::new();
        for term in &terms {
            if let Some(urls) = index.get(term) {
                term_sets.push(urls);
            } else {
                // If any term is not found, no results
                return Vec::new();
            }
        }

        if term_sets.is_empty() {
            return Vec::new();
        }

        // Sort by set size to start with smallest set for efficiency
        term_sets.sort_by_key(|set| set.len());

        // Start with the smallest set and intersect with others
        let mut result: HashSet<String> = term_sets[0].clone();
        for set in term_sets.iter().skip(1) {
            result = result.intersection(set).cloned().collect();
            if result.is_empty() {
                break; // Early termination if no intersection
            }
        }

        result.into_iter().collect()
    }

    /// Tokenize text into searchable terms with stop word filtering.
    ///
    /// Processes text by:
    /// - Converting to lowercase
    /// - Splitting on whitespace and punctuation
    /// - Filtering out stop words
    /// - Removing terms shorter than 3 characters
    /// - Cleaning punctuation from term boundaries
    fn tokenize(&self, text: &str) -> Vec<String> {
        text.to_lowercase()
            .split_whitespace()
            .flat_map(|word| {
                // Split on common punctuation while preserving alphanumeric content
                word.split(|c: char| !c.is_alphanumeric() && c != '-' && c != '_')
            })
            .map(|s| s.trim_matches(|c: char| !c.is_alphanumeric()))
            .filter(|s| s.len() >= 3) // Only index terms with 3+ characters
            .filter(|s| !self.stop_words.contains(*s)) // Filter out stop words
            .filter(|s| !s.chars().all(|c| c.is_numeric())) // Filter out pure numbers
            .map(|s| s.to_string())
            .collect()
    }

    /// Add a method to remove a resource from the index (for incremental updates)
    pub fn remove_resource(&self, canonical_url: &str) {
        let mut index = self.index.write().unwrap();

        // Remove the URL from all term sets
        for (_, url_set) in index.iter_mut() {
            url_set.remove(canonical_url);
        }

        // Remove empty term entries to save memory
        index.retain(|_, url_set| !url_set.is_empty());
    }

    /// Get index statistics for monitoring
    pub fn get_stats(&self) -> (usize, usize) {
        let index = self.index.read().unwrap();
        let term_count = index.len();
        let total_entries: usize = index.values().map(|set| set.len()).sum();
        (term_count, total_entries)
    }
}

impl Default for FilterEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl FilterEngine {
    /// Creates a new filter engine.
    ///
    /// # Example
    ///
    /// ```rust
    /// use octofhir_canonical_manager::search::FilterEngine;
    ///
    /// let filter_engine = FilterEngine::new();
    /// ```
    pub fn new() -> Self {
        Self {}
    }

    /// Applies search query filters to a list of resources.
    ///
    /// Filters resources based on resource type, package constraints,
    /// canonical URL patterns, and version constraints specified in the query.
    ///
    /// # Arguments
    ///
    /// * `resources` - Vector of resource indices to filter
    /// * `query` - Search query containing filter criteria
    ///
    /// # Returns
    ///
    /// Filtered vector of resource indices matching the query filters.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use octofhir_canonical_manager::search::{FilterEngine, SearchQuery};
    /// use octofhir_canonical_manager::binary_storage::ResourceIndex;
    ///
    /// # fn example(resources: Vec<ResourceIndex>) {
    /// let filter_engine = FilterEngine::new();
    /// let query = SearchQuery {
    ///     resource_types: vec!["Patient".to_string()],
    ///     ..Default::default()
    /// };
    ///
    /// let filtered = filter_engine.apply_filters(resources, &query);
    /// # }
    /// ```
    pub fn apply_filters(
        &self,
        resources: Vec<ResourceIndex>,
        query: &SearchQuery,
    ) -> Vec<ResourceIndex> {
        let mut filtered = resources;

        // Filter by resource types
        if !query.resource_types.is_empty() {
            filtered.retain(|r| query.resource_types.contains(&r.resource_type));
        }

        // Filter by packages
        if !query.packages.is_empty() {
            filtered.retain(|r| {
                query.packages.iter().any(|pkg| {
                    if pkg.contains('@') {
                        // Package with version
                        let package_spec = format!("{}@{}", r.package_name, r.package_version);
                        package_spec == *pkg
                    } else {
                        // Package name only
                        r.package_name == *pkg
                    }
                })
            });
        }

        // Filter by canonical URL pattern
        if let Some(pattern) = &query.canonical_pattern {
            if let Ok(regex) = Regex::new(pattern) {
                filtered.retain(|r| regex.is_match(&r.canonical_url));
            } else {
                // Fallback to simple string matching if regex is invalid
                filtered.retain(|r| r.canonical_url.contains(pattern));
            }
        }

        // Apply version constraints
        for constraint in &query.version_constraints {
            filtered.retain(|r| {
                r.package_name == constraint.package && r.package_version == constraint.version
            });
        }

        filtered
    }
}

impl SearchEngine {
    /// Creates a new search engine with the given storage backend.
    ///
    /// Automatically builds a text index from all resources currently in storage.
    ///
    /// # Arguments
    ///
    /// * `storage` - Shared reference to the indexed storage system
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use octofhir_canonical_manager::search::SearchEngine;
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
    /// let engine = SearchEngine::new(storage);
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(storage: Arc<BinaryStorage>) -> Self {
        let mut engine = Self {
            storage,
            text_index: TextIndex::new(),
            filters: FilterEngine::new(),
            search_cache: RwLock::new(HashMap::new()),
            cache_ttl: Duration::from_secs(300), // 5 minutes cache TTL
        };

        // Build only metadata index (fast) - content will be indexed on-demand
        engine.build_metadata_index();

        engine
    }

    /// Build metadata index from current storage (fast - no file I/O)
    fn build_metadata_index(&mut self) {
        debug!("Building text index from storage");
        let cache_entries = self.storage.get_cache_entries();

        for (canonical_url, resource_index) in cache_entries {
            // Index the canonical URL itself
            self.text_index.add_resource(&canonical_url, &canonical_url);

            // Index the resource type
            self.text_index
                .add_resource(&canonical_url, &resource_index.resource_type);

            // Index the resource ID
            self.text_index
                .add_resource(&canonical_url, &resource_index.metadata.id);

            // Index package information
            let package_text = format!(
                "{} {}",
                resource_index.package_name, resource_index.package_version
            );
            self.text_index.add_resource(&canonical_url, &package_text);

            // Index metadata fields
            if let Some(version) = &resource_index.metadata.version {
                self.text_index.add_resource(&canonical_url, version);
            }
            if let Some(status) = &resource_index.metadata.status {
                self.text_index.add_resource(&canonical_url, status);
            }
            if let Some(publisher) = &resource_index.metadata.publisher {
                self.text_index.add_resource(&canonical_url, publisher);
            }

            // Skip loading actual FHIR resource content during initialization for performance
            // Content will be indexed on-demand when needed or during scheduled indexing
        }

        let (term_count, total_entries) = self.text_index.get_stats();
        debug!(
            "Text index built with {} terms and {} total entries",
            term_count, total_entries
        );
    }

    /// Add a single resource to the text index (for incremental updates)
    pub fn add_resource_to_index(&self, canonical_url: &str, resource_index: &ResourceIndex) {
        // Index the canonical URL itself
        self.text_index.add_resource(canonical_url, canonical_url);

        // Index the resource type
        self.text_index
            .add_resource(canonical_url, &resource_index.resource_type);

        // Index the resource ID
        self.text_index
            .add_resource(canonical_url, &resource_index.metadata.id);

        // Index package information
        let package_text = format!(
            "{} {}",
            resource_index.package_name, resource_index.package_version
        );
        self.text_index.add_resource(canonical_url, &package_text);

        // Index metadata fields
        if let Some(version) = &resource_index.metadata.version {
            self.text_index.add_resource(canonical_url, version);
        }
        if let Some(status) = &resource_index.metadata.status {
            self.text_index.add_resource(canonical_url, status);
        }
        if let Some(publisher) = &resource_index.metadata.publisher {
            self.text_index.add_resource(canonical_url, publisher);
        }

        // Try to index actual FHIR resource content
        if let Ok(fhir_resource) = self.storage.get_resource(resource_index) {
            // Index the resource content as JSON string for full-text search
            let content_str = serde_json::to_string(&fhir_resource.content).unwrap_or_default();
            self.text_index.add_resource(canonical_url, &content_str);

            // Index specific FHIR fields if available
            if let Some(description) = fhir_resource.content.get("description") {
                if let Some(desc_str) = description.as_str() {
                    self.text_index.add_resource(canonical_url, desc_str);
                }
            }
            if let Some(title) = fhir_resource.content.get("title") {
                if let Some(title_str) = title.as_str() {
                    self.text_index.add_resource(canonical_url, title_str);
                }
            }
            if let Some(name) = fhir_resource.content.get("name") {
                if let Some(name_str) = name.as_str() {
                    self.text_index.add_resource(canonical_url, name_str);
                }
            }
        }

        debug!("Added resource {} to text index", canonical_url);
    }

    /// Remove a resource from the text index (for incremental updates)
    pub fn remove_resource_from_index(&self, canonical_url: &str) {
        self.text_index.remove_resource(canonical_url);
        debug!("Removed resource {} from text index", canonical_url);
    }

    /// Get index statistics for monitoring
    pub fn get_index_stats(&self) -> (usize, usize) {
        self.text_index.get_stats()
    }

    /// Generate a cache key from a search query
    fn generate_cache_key(&self, query: &SearchQuery) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        query.text.hash(&mut hasher);
        query.resource_types.hash(&mut hasher);
        query.packages.hash(&mut hasher);
        query.canonical_pattern.hash(&mut hasher);
        query.limit.hash(&mut hasher);
        query.offset.hash(&mut hasher);
        query.version_constraints.hash(&mut hasher);

        format!("search_{:x}", hasher.finish())
    }

    /// Get cached search result if available and not expired
    fn get_cached_result(&self, cache_key: &str) -> Option<SearchResult> {
        let cache = self.search_cache.read().unwrap();
        if let Some((result, timestamp)) = cache.get(cache_key) {
            if timestamp.elapsed() < self.cache_ttl {
                return Some(result.clone());
            }
        }
        None
    }

    /// Cache a search result
    fn cache_result(&self, cache_key: &str, result: &SearchResult) {
        let mut cache = self.search_cache.write().unwrap();

        // Simple cache size management - remove oldest entries if cache gets too large
        if cache.len() > 1000 {
            let oldest_key = cache
                .iter()
                .min_by_key(|(_, (_, timestamp))| timestamp)
                .map(|(key, _)| key.clone());

            if let Some(key) = oldest_key {
                cache.remove(&key);
            }
        }

        cache.insert(cache_key.to_string(), (result.clone(), Instant::now()));
    }

    /// Clear the search cache
    pub fn clear_cache(&self) {
        let mut cache = self.search_cache.write().unwrap();
        cache.clear();
        debug!("Search cache cleared");
    }

    /// Executes a search query and returns matching resources.
    ///
    /// Performs text search (if specified), applies filters, calculates relevance
    /// scores, and returns paginated results with highlights and metadata.
    ///
    /// # Arguments
    ///
    /// * `query` - The search query to execute
    ///
    /// # Returns
    ///
    /// * `Ok(SearchResult)` - Search results with matched resources
    /// * `Err` - If search execution fails
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::search::{SearchEngine, SearchQuery};
    /// # async fn example(engine: SearchEngine) -> Result<(), Box<dyn std::error::Error>> {
    /// let query = SearchQuery {
    ///     text: Some("Patient demographics".to_string()),
    ///     resource_types: vec!["Patient".to_string()],
    ///     limit: Some(10),
    ///     ..Default::default()
    /// };
    ///
    /// let results = engine.search(&query).await?;
    /// println!("Found {} resources", results.total_count);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn search(&self, query: &SearchQuery) -> Result<SearchResult> {
        let start_time = Instant::now();
        info!("Executing search query: {:?}", query);

        // Generate cache key from query
        let cache_key = self.generate_cache_key(query);

        // Check cache first
        if let Some(cached_result) = self.get_cached_result(&cache_key) {
            debug!("Returning cached search result for key: {}", cache_key);
            return Ok(cached_result);
        }

        // Get all resources from storage
        let mut candidate_resources = self.get_candidate_resources(query).await?;

        // Apply filters
        candidate_resources = self.filters.apply_filters(candidate_resources, query);

        // Calculate scores and sort
        let mut scored_resources = self.score_resources(candidate_resources, query).await?;
        scored_resources.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let total_count = scored_resources.len();

        // Apply pagination
        let offset = query.offset.unwrap_or(0);
        let limit = query.limit.unwrap_or(50).min(1000); // Max 1000 results

        let paginated_resources = scored_resources
            .into_iter()
            .skip(offset)
            .take(limit)
            .collect();

        let query_time = start_time.elapsed();
        debug!(
            "Search completed in {:?}, found {} results",
            query_time, total_count
        );

        let result = SearchResult {
            resources: paginated_resources,
            total_count,
            query_time,
        };

        // Cache the result
        self.cache_result(&cache_key, &result);

        Ok(result)
    }

    /// Get candidate resources based on query
    async fn get_candidate_resources(&self, query: &SearchQuery) -> Result<Vec<ResourceIndex>> {
        if let Some(text) = &query.text {
            // Text search - use text index
            let matching_urls = self.text_index.search(text);
            let cache_entries = self.storage.get_cache_entries();

            Ok(cache_entries
                .into_iter()
                .filter(|(url, _)| matching_urls.contains(url))
                .map(|(_, resource_index)| resource_index)
                .collect())
        } else {
            // No text search - return all resources
            let cache_entries = self.storage.get_cache_entries();
            Ok(cache_entries.into_values().collect())
        }
    }

    /// Score resources based on relevance
    async fn score_resources(
        &self,
        resources: Vec<ResourceIndex>,
        query: &SearchQuery,
    ) -> Result<Vec<ResourceMatch>> {
        let mut matches = Vec::new();

        for resource_index in resources {
            let score = self.calculate_score(&resource_index, query);
            let highlights = self.generate_highlights(&resource_index, query).await?;

            // Load the full resource content
            let resource = self.storage.get_resource(&resource_index)?;

            matches.push(ResourceMatch {
                resource,
                index: resource_index,
                score,
                highlights,
            });
        }

        Ok(matches)
    }

    /// Calculate relevance score for a resource
    fn calculate_score(&self, resource_index: &ResourceIndex, query: &SearchQuery) -> f64 {
        let mut score = 1.0;

        // Boost score for exact resource type matches
        if !query.resource_types.is_empty()
            && query.resource_types.contains(&resource_index.resource_type)
        {
            score += 2.0;
        }

        // Boost score for exact package matches
        if !query.packages.is_empty() {
            for package in &query.packages {
                if package.contains('@') {
                    let package_spec = format!(
                        "{}@{}",
                        resource_index.package_name, resource_index.package_version
                    );
                    if package_spec == *package {
                        score += 3.0;
                    }
                } else if resource_index.package_name == *package {
                    score += 1.5;
                }
            }
        }

        // Boost score for canonical URL pattern matches
        if let Some(pattern) = &query.canonical_pattern {
            if resource_index.canonical_url.contains(pattern) {
                score += 1.0;
            }
        }

        // Text search scoring
        if let Some(text) = &query.text {
            let search_text = format!(
                "{} {} {} {} {}",
                resource_index.canonical_url,
                resource_index.resource_type,
                resource_index.metadata.id,
                resource_index.package_name,
                resource_index.metadata.publisher.as_deref().unwrap_or("")
            )
            .to_lowercase();

            let text_lower = text.to_lowercase();
            let query_terms: Vec<&str> = text_lower.split_whitespace().collect();
            let mut term_matches = 0;

            for term in query_terms {
                if search_text.contains(term) {
                    term_matches += 1;
                    score += 0.5;
                }
            }

            // Boost for multiple term matches
            if term_matches > 1 {
                score += term_matches as f64 * 0.3;
            }
        }

        score
    }

    /// Generate search highlights
    async fn generate_highlights(
        &self,
        resource_index: &ResourceIndex,
        query: &SearchQuery,
    ) -> Result<Vec<SearchHighlight>> {
        let mut highlights = Vec::new();

        if let Some(text) = &query.text {
            let text_lower = text.to_lowercase();
            let query_terms: Vec<&str> = text_lower.split_whitespace().collect();

            // Highlight canonical URL
            for term in &query_terms {
                if resource_index.canonical_url.to_lowercase().contains(term) {
                    highlights.push(SearchHighlight {
                        field: "canonical_url".to_string(),
                        fragment: self.highlight_text(&resource_index.canonical_url, term),
                    });
                }
            }

            // Highlight resource type
            for term in &query_terms {
                if resource_index.resource_type.to_lowercase().contains(term) {
                    highlights.push(SearchHighlight {
                        field: "resource_type".to_string(),
                        fragment: self.highlight_text(&resource_index.resource_type, term),
                    });
                }
            }

            // Highlight package name
            for term in &query_terms {
                if resource_index.package_name.to_lowercase().contains(term) {
                    highlights.push(SearchHighlight {
                        field: "package_name".to_string(),
                        fragment: self.highlight_text(&resource_index.package_name, term),
                    });
                }
            }
        }

        Ok(highlights)
    }

    /// Highlight matching text
    fn highlight_text(&self, text: &str, term: &str) -> String {
        let case_insensitive_replace = |s: &str, from: &str| -> String {
            let lower_s = s.to_lowercase();
            let lower_from = from.to_lowercase();

            if let Some(pos) = lower_s.find(&lower_from) {
                let mut result = s.to_string();
                let actual_match = &s[pos..pos + from.len()];
                result = result.replace(actual_match, &format!("<mark>{actual_match}</mark>"));
                result
            } else {
                s.to_string()
            }
        };

        case_insensitive_replace(text, term)
    }

    /// Provides search term suggestions based on a prefix.
    ///
    /// Returns potential completions for the given prefix by matching against
    /// resource types, package names, and indexed terms. Useful for implementing
    /// autocomplete functionality.
    ///
    /// # Arguments
    ///
    /// * `prefix` - The prefix to find completions for (minimum 2 characters)
    ///
    /// # Returns
    ///
    /// Vector of suggested completions, limited to 10 results.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::search::SearchEngine;
    /// # fn example(engine: SearchEngine) {
    /// let suggestions = engine.suggest_completions("pat");
    /// for suggestion in suggestions {
    ///     println!("Suggestion: {}", suggestion);
    /// }
    /// # }
    /// ```
    pub fn suggest_completions(&self, prefix: &str) -> Vec<String> {
        if prefix.len() < 2 {
            return Vec::new();
        }

        let prefix_lower = prefix.to_lowercase();
        let mut suggestions = Vec::new();

        // Suggest resource types
        let cache_entries = self.storage.get_cache_entries();
        let mut resource_types: Vec<String> = cache_entries
            .values()
            .map(|index| index.resource_type.clone())
            .filter(|rt| rt.to_lowercase().starts_with(&prefix_lower))
            .collect();
        resource_types.sort();
        resource_types.dedup();
        suggestions.extend(resource_types.into_iter().take(5));

        // Suggest package names
        let mut package_names: Vec<String> = cache_entries
            .values()
            .map(|index| index.package_name.clone())
            .filter(|pn| pn.to_lowercase().starts_with(&prefix_lower))
            .collect();
        package_names.sort();
        package_names.dedup();
        suggestions.extend(package_names.into_iter().take(5));

        // Suggest terms from text index
        let mut index_terms: Vec<String> = self
            .text_index
            .index
            .read()
            .unwrap()
            .keys()
            .filter(|term| term.starts_with(&prefix_lower))
            .cloned()
            .collect();
        index_terms.sort();
        suggestions.extend(index_terms.into_iter().take(5));

        suggestions.sort();
        suggestions.dedup();
        suggestions.into_iter().take(10).collect()
    }

    /// Generates search facets for the given query.
    ///
    /// Returns aggregated counts of resources grouped by resource type,
    /// package, and version. Useful for building faceted search interfaces
    /// that show result distribution across different categories.
    ///
    /// # Arguments
    ///
    /// * `query` - The search query to generate facets for
    ///
    /// # Returns
    ///
    /// * `Ok(SearchFacets)` - Faceted counts of matching resources
    /// * `Err` - If facet generation fails
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::search::{SearchEngine, SearchQuery};
    /// # async fn example(engine: SearchEngine) -> Result<(), Box<dyn std::error::Error>> {
    /// let query = SearchQuery {
    ///     text: Some("Patient".to_string()),
    ///     ..Default::default()
    /// };
    ///
    /// let facets = engine.get_facets(&query)?;
    /// println!("Resource types: {:?}", facets.resource_types);
    /// # Ok(())
    /// # }
    /// ```
    pub fn get_facets(&self, query: &SearchQuery) -> Result<SearchFacets> {
        // Get candidate resources
        let cache_entries = self.storage.get_cache_entries();
        let resources: Vec<ResourceIndex> = if let Some(text) = &query.text {
            let matching_urls = self.text_index.search(text);
            cache_entries
                .into_iter()
                .filter(|(url, _)| matching_urls.contains(url))
                .map(|(_, resource_index)| resource_index)
                .collect()
        } else {
            cache_entries.into_values().collect()
        };

        // Apply existing filters (except the ones we're faceting on)
        let filtered_resources = self.filters.apply_filters(resources, query);

        // Count facets
        let mut resource_types = HashMap::new();
        let mut packages = HashMap::new();
        let mut versions = HashMap::new();

        for resource in filtered_resources {
            *resource_types.entry(resource.resource_type).or_insert(0) += 1;
            *packages.entry(resource.package_name.clone()).or_insert(0) += 1;
            if let Some(version) = resource.metadata.version {
                *versions.entry(version).or_insert(0) += 1;
            }
        }

        Ok(SearchFacets {
            resource_types,
            packages,
            versions,
        })
    }
}

impl SearchQueryBuilder {
    /// Creates a new search query builder.
    ///
    /// # Arguments
    ///
    /// * `storage` - Shared reference to the indexed storage system
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use octofhir_canonical_manager::search::SearchQueryBuilder;
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
    /// let builder = SearchQueryBuilder::new(storage);
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(storage: Arc<BinaryStorage>) -> Self {
        Self {
            storage,
            query: SearchQuery::default(),
        }
    }

    /// Adds a text search query.
    ///
    /// # Arguments
    ///
    /// * `text` - The text to search for across resource content
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::search::SearchQueryBuilder;
    /// # use std::sync::Arc;
    /// # fn example(builder: SearchQueryBuilder) {
    /// let builder = builder.text("Patient demographics");
    /// # }
    /// ```
    pub fn text(mut self, text: &str) -> Self {
        self.query.text = Some(text.to_string());
        self
    }

    /// Adds a resource type filter.
    ///
    /// Can be called multiple times to search for multiple resource types.
    ///
    /// # Arguments
    ///
    /// * `resource_type` - The FHIR resource type to filter by (e.g., "Patient")
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::search::SearchQueryBuilder;
    /// # fn example(builder: SearchQueryBuilder) {
    /// let builder = builder
    ///     .resource_type("Patient")
    ///     .resource_type("Person");
    /// # }
    /// ```
    pub fn resource_type(mut self, resource_type: &str) -> Self {
        self.query.resource_types.push(resource_type.to_string());
        self
    }

    /// Adds a package filter.
    ///
    /// Can specify just package name or package@version format.
    /// Can be called multiple times to search across multiple packages.
    ///
    /// # Arguments
    ///
    /// * `package` - Package name or "name@version" format
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::search::SearchQueryBuilder;
    /// # fn example(builder: SearchQueryBuilder) {
    /// let builder = builder
    ///     .package("hl7.fhir.us.core")
    ///     .package("hl7.fhir.r4.core@4.0.1");
    /// # }
    /// ```
    pub fn package(mut self, package: &str) -> Self {
        self.query.packages.push(package.to_string());
        self
    }

    /// Sets a canonical URL pattern filter.
    ///
    /// Supports regex patterns for flexible URL matching.
    ///
    /// # Arguments
    ///
    /// * `pattern` - Regex pattern or simple string to match against canonical URLs
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::search::SearchQueryBuilder;
    /// # fn example(builder: SearchQueryBuilder) {
    /// let builder = builder.canonical_pattern(r"http://hl7\.org/fhir/.*Patient.*");
    /// # }
    /// ```
    pub fn canonical_pattern(mut self, pattern: &str) -> Self {
        self.query.canonical_pattern = Some(pattern.to_string());
        self
    }

    /// Sets the maximum number of results to return.
    ///
    /// # Arguments
    ///
    /// * `limit` - Maximum number of results (up to 1000)
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::search::SearchQueryBuilder;
    /// # fn example(builder: SearchQueryBuilder) {
    /// let builder = builder.limit(50);
    /// # }
    /// ```
    pub fn limit(mut self, limit: usize) -> Self {
        self.query.limit = Some(limit);
        self
    }

    /// Adds a version constraint for a specific package.
    ///
    /// Only returns resources from the specified version of the given package.
    ///
    /// # Arguments
    ///
    /// * `package` - The package name to constrain
    /// * `version` - The specific version required
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::search::SearchQueryBuilder;
    /// # fn example(builder: SearchQueryBuilder) {
    /// let builder = builder.version_constraint("hl7.fhir.us.core", "6.1.0");
    /// # }
    /// ```
    pub fn version_constraint(mut self, package: &str, version: &str) -> Self {
        self.query.version_constraints.push(VersionConstraint {
            package: package.to_string(),
            version: version.to_string(),
        });
        self
    }

    /// Sets the result offset for pagination.
    ///
    /// # Arguments
    ///
    /// * `offset` - Number of results to skip
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::search::SearchQueryBuilder;
    /// # fn example(builder: SearchQueryBuilder) {
    /// let builder = builder.offset(20); // Skip first 20 results
    /// # }
    /// ```
    pub fn offset(mut self, offset: usize) -> Self {
        self.query.offset = Some(offset);
        self
    }

    /// Executes the constructed search query.
    ///
    /// # Returns
    ///
    /// * `Ok(SearchResult)` - The search results
    /// * `Err` - If the search execution fails
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use octofhir_canonical_manager::search::SearchQueryBuilder;
    /// # use octofhir_canonical_manager::binary_storage::BinaryStorage;
    /// # use octofhir_canonical_manager::config::StorageConfig;
    /// # use std::sync::Arc;
    /// # use std::path::PathBuf;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let config = StorageConfig {
    ///     cache_dir: PathBuf::from("/tmp/cache"),
    ///     index_dir: PathBuf::from("/tmp/index"),
    ///     packages_dir: PathBuf::from("/tmp/packages"),
    ///     max_cache_size: "1GB".to_string(),
    /// };
    /// let storage = Arc::new(BinaryStorage::new(config).await?);
    ///
    /// let results = SearchQueryBuilder::new(storage)
    ///     .text("Patient")
    ///     .resource_type("Patient")
    ///     .limit(10)
    ///     .execute()
    ///     .await?;
    ///
    /// println!("Found {} results", results.total_count);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn execute(self) -> Result<SearchResult> {
        let engine = SearchEngine::new(self.storage);
        engine.search(&self.query).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::StorageConfig;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_search_engine_creation() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            cache_dir: temp_dir.path().join("cache"),
            index_dir: temp_dir.path().join("index"),
            packages_dir: temp_dir.path().join("packages"),
            max_cache_size: "100MB".to_string(),
        };

        let storage = Arc::new(BinaryStorage::new(config).await.unwrap());
        let engine = SearchEngine::new(storage);

        // Test basic search
        let query = SearchQuery::default();
        let result = engine.search(&query).await;
        assert!(result.is_ok());

        let search_result = result.unwrap();
        assert_eq!(search_result.resources.len(), 0);
        assert_eq!(search_result.total_count, 0);
    }

    #[tokio::test]
    async fn test_text_search() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            cache_dir: temp_dir.path().join("cache"),
            index_dir: temp_dir.path().join("index"),
            packages_dir: temp_dir.path().join("packages"),
            max_cache_size: "100MB".to_string(),
        };

        let storage = Arc::new(BinaryStorage::new(config).await.unwrap());
        let engine = SearchEngine::new(storage);

        // Test text search
        let query = SearchQuery {
            text: Some("patient".to_string()),
            ..Default::default()
        };

        let result = engine.search(&query).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_search_query_builder() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            cache_dir: temp_dir.path().join("cache"),
            index_dir: temp_dir.path().join("index"),
            packages_dir: temp_dir.path().join("packages"),
            max_cache_size: "100MB".to_string(),
        };

        let storage = Arc::new(BinaryStorage::new(config).await.unwrap());

        // Test query builder
        let result = SearchQueryBuilder::new(storage)
            .text("patient")
            .resource_type("Patient")
            .package("hl7.fhir.r4.core")
            .limit(10)
            .execute()
            .await;

        assert!(result.is_ok());
    }

    #[test]
    fn test_text_index() {
        let index = TextIndex::new();

        // Add some test content
        index.add_resource(
            "http://example.com/Patient/1",
            "Patient resource for John Doe",
        );
        index.add_resource(
            "http://example.com/Observation/1",
            "Observation for patient monitoring",
        );

        // Test search
        let results = index.search("patient");
        assert_eq!(results.len(), 2);

        let results = index.search("John");
        assert_eq!(results.len(), 1);
        assert!(results.contains(&"http://example.com/Patient/1".to_string()));

        let results = index.search("monitoring");
        assert_eq!(results.len(), 1);
        assert!(results.contains(&"http://example.com/Observation/1".to_string()));
    }

    #[test]
    fn test_filter_engine() {
        let filter_engine = FilterEngine::new();

        let resources = vec![
            ResourceIndex {
                canonical_url: "http://example.com/Patient/1".to_string(),
                resource_type: "Patient".to_string(),
                package_name: "hl7.fhir.r4.core".to_string(),
                package_version: "4.0.1".to_string(),
                file_path: std::path::PathBuf::from("/test/Patient1.json"),
                metadata: crate::binary_storage::ResourceMetadata {
                    id: "patient-1".to_string(),
                    version: Some("1.0.0".to_string()),
                    status: Some("active".to_string()),
                    date: None,
                    publisher: Some("HL7".to_string()),
                },
            },
            ResourceIndex {
                canonical_url: "http://example.com/Observation/1".to_string(),
                resource_type: "Observation".to_string(),
                package_name: "custom.package".to_string(),
                package_version: "2.0.0".to_string(),
                file_path: std::path::PathBuf::from("/test/Observation1.json"),
                metadata: crate::binary_storage::ResourceMetadata {
                    id: "obs-1".to_string(),
                    version: Some("1.0.0".to_string()),
                    status: Some("final".to_string()),
                    date: None,
                    publisher: Some("Custom".to_string()),
                },
            },
        ];

        // Test resource type filter
        let query = SearchQuery {
            resource_types: vec!["Patient".to_string()],
            ..Default::default()
        };

        let filtered = filter_engine.apply_filters(resources.clone(), &query);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].resource_type, "Patient");

        // Test package filter
        let query = SearchQuery {
            packages: vec!["hl7.fhir.r4.core".to_string()],
            ..Default::default()
        };

        let filtered = filter_engine.apply_filters(resources.clone(), &query);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].package_name, "hl7.fhir.r4.core");
    }

    #[tokio::test]
    async fn test_search_suggestions() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            cache_dir: temp_dir.path().join("cache"),
            index_dir: temp_dir.path().join("index"),
            packages_dir: temp_dir.path().join("packages"),
            max_cache_size: "100MB".to_string(),
        };

        let storage = Arc::new(BinaryStorage::new(config).await.unwrap());
        let engine = SearchEngine::new(storage);

        // Test suggestions - all Vec::len() values are >= 0 by definition
        let _suggestions = engine.suggest_completions("pa");
        // No need to check >= 0 since Vec::len() is always >= 0

        let suggestions = engine.suggest_completions("a"); // Too short
        assert_eq!(suggestions.len(), 0);
    }
}
