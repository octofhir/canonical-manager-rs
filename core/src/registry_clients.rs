//! Registry client trait + per-protocol implementations for FHIR package
//! backends.
//!
//! # Where to look
//!
//! | Topic | Symbol | Section |
//! |---|---|---|
//! | Public trait | [`FhirRegistryClient`] | `Trait` |
//! | Search query (mirrors IG Publisher's `/catalog?...`) | [`SearchQuery`] | `Types` |
//! | Search result row | [`PackageHit`] | `Types` |
//! | Wrapper around the legacy [`crate::registry::RegistryClient`] (NPM-spec backends, e.g. `packages.fhir.org`) | [`NpmRegistryClient`] | `NpmRegistryClient` |
//! | FHIRsmith / `packages2.fhir.org`-style: catalog + tarball-direct | [`Packages2Client`] | `Packages2Client` |
//! | `fs.get-ig.org` S3-flat layout: NDJSON bulk index + `pkgs/<name>/<ver>` stub | [`GetIgRegistryClient`] | `GetIgRegistryClient` |
//! | Federated orchestrator (fan-out search, first-success download) | [`RedundantRegistryClient`] | `RedundantRegistryClient` |
//!
//! # Why a trait?
//!
//! Three distinct registry protocols are in active use across the FHIR
//! ecosystem:
//!
//! - **NPM Package Specification**: `GET /{name}` returns a multi-version
//!   manifest with `dist-tags`. Used by `packages.fhir.org`,
//!   `packages.simplifier.net`, private Verdaccio mirrors.
//! - **Tarball-direct**: `GET /{name}/{version}` returns the .tgz body
//!   itself, not JSON. Used by `packages2.fhir.org/packages` (HL7 FHIRsmith).
//! - **S3-flat archive**: tarballs at `/-/{name}-{ver}.tgz`, ad-hoc JSON
//!   stubs at `/pkgs/{name}/{ver}`, bulk NDJSON dumps at root. Used by
//!   `fs.get-ig.org`.
//!
//! Search support is uneven: `packages.fhir.org` and `packages2.fhir.org`
//! both expose `/catalog?name=&canonical=&fhirversion=&prerelease=`,
//! `fs.get-ig.org` ships `pgks.ndjson.gz`, and generic NPM mirrors don't
//! support search at all (the NPM `/-/v1/search?text=` endpoint that older
//! code used returns 404 on every official FHIR registry). Backends that
//! can't fulfil an operation return [`crate::error::RegistryError::NotSupported`];
//! the orchestrator falls through to the next client in the chain.
//!
//! # Auth
//!
//! `RegistryConfig.token_env` names an env var holding a bearer token.
//! When set and present, clients pass it via `Authorization: Bearer <token>`.
//! Convention: `FCM_REGISTRY_TOKEN` is the default name applied automatically
//! when the env var is set and `token_env` is unspecified (see `config.rs`).

use crate::config::{ExtraRegistry, PackageSpec, RegistryClientType, RegistryConfig};
use crate::error::{RegistryError, Result};
use crate::registry::{DownloadProgress, PackageDownload, PackageInfo, RegistryClient};

/// Hard cap on tarball size for streaming downloads. Mirrors the value in
/// `crate::registry` — kept in sync manually because the legacy const is
/// `pub(crate)`-private.
const MAX_TARBALL_BYTES: u64 = 2 * 1024 * 1024 * 1024;
use async_trait::async_trait;
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{debug, info, warn};

// =====================================================================
// Public types
// =====================================================================

/// Search parameters for the FHIR `/catalog` endpoint.
///
/// Mirrors the param set used by IG Publisher's `PackageClient.search()`
/// and the Firely `Firely.Fhir.Packages.CatalogPackagesAsync()` method.
/// All fields are optional; `Default` produces a "match-all" query.
#[derive(Debug, Clone, Default)]
pub struct SearchQuery {
    /// Substring match on the package `name` field.
    pub name: Option<String>,
    /// Match on the FHIR-IG canonical URL.
    pub canonical: Option<String>,
    /// Filter by FHIR version (`R4`, `R4B`, `R5`, etc.).
    pub fhir_version: Option<String>,
    /// When true, include prerelease/draft packages.
    pub prerelease: bool,
}

impl SearchQuery {
    /// Convenience: search by name substring.
    pub fn by_name(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            ..Default::default()
        }
    }
}

/// One hit returned by [`FhirRegistryClient::search`].
///
/// Fields beyond `name` are best-effort: `packages.fhir.org/catalog` returns
/// only `Name`/`Description`/`FhirVersion`, while `packages2.fhir.org` adds
/// `version`, `canonical`, `url`, `date`, `kind`. Callers should not assume
/// any non-name field is populated.
#[derive(Debug, Clone)]
#[allow(missing_docs)]
pub struct PackageHit {
    pub name: String,
    pub version: Option<String>,
    pub fhir_version: Option<String>,
    pub description: Option<String>,
    pub canonical: Option<String>,
    /// Stable id of the registry that produced this hit (typically the host).
    pub registry: String,
}

// =====================================================================
// Trait
// =====================================================================

/// Abstraction over heterogeneous FHIR package registry backends.
///
/// Implementations live under this module: see [`NpmRegistryClient`],
/// [`Packages2Client`], [`GetIgRegistryClient`], and the
/// [`RedundantRegistryClient`] orchestrator.
#[async_trait]
pub trait FhirRegistryClient: Send + Sync + std::fmt::Debug {
    /// Stable identifier — typically the registry host. Used in tracing
    /// spans and error messages.
    fn id(&self) -> &str;

    /// Search for packages. Backends that can't search MUST return
    /// `Err(RegistryError::NotSupported { operation: "search", .. })`.
    async fn search(&self, query: &SearchQuery) -> Result<Vec<PackageHit>>;

    /// Enumerate all known versions of `name`, newest first. Backends that
    /// can't enumerate (e.g. `packages2.fhir.org` whose `/{name}` endpoint
    /// returns HTML, not JSON) MUST return `Err(RegistryError::NotSupported)`.
    async fn list_versions(&self, name: &str) -> Result<Vec<String>>;

    /// Download `{name}@{version}` to local disk and return file path +
    /// metadata. Backends MUST verify integrity when the registry publishes
    /// hashes; for backends that don't (`packages2.fhir.org` octet-stream,
    /// `fs.get-ig.org` stubs without `shasum`), the contract is "first use
    /// trust" — the byte stream is recorded as-is, callers may compute and
    /// pin a hash themselves.
    async fn download(
        &self,
        spec: &PackageSpec,
        progress: Option<&dyn DownloadProgress>,
    ) -> Result<PackageDownload>;
}

// =====================================================================
// NpmRegistryClient — wraps the legacy concrete RegistryClient
// =====================================================================

/// NPM-spec adapter: wraps the existing concrete [`RegistryClient`] so it
/// fits the trait contract.
///
/// Used for any registry that returns `dist-tags`/`versions` JSON at
/// `GET /{name}` — `packages.fhir.org`, `packages.simplifier.net`,
/// private Verdaccio. Search is delegated to the wrapped client's
/// `/catalog`-based implementation.
pub struct NpmRegistryClient {
    inner: RegistryClient,
    id: String,
}

impl std::fmt::Debug for NpmRegistryClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // RegistryClient doesn't impl Debug; print its id only.
        f.debug_struct("NpmRegistryClient")
            .field("id", &self.id)
            .finish()
    }
}

impl NpmRegistryClient {
    /// Construct a client against the registry described by `config`.
    /// `cache_dir` receives downloaded tarballs; it is created if missing.
    pub async fn new(config: &RegistryConfig, cache_dir: PathBuf) -> Result<Self> {
        let id = url::Url::parse(&config.url)
            .ok()
            .and_then(|u| u.host_str().map(String::from))
            .unwrap_or_else(|| config.url.clone());
        let inner = RegistryClient::new(config, cache_dir).await?;
        Ok(Self { inner, id })
    }

    /// Like `new`, but reuses a shared `reqwest::Client`. See
    /// [`crate::registry::build_default_http_client`] for the
    /// canonical builder.
    pub async fn new_with_client(
        config: &RegistryConfig,
        cache_dir: PathBuf,
        http: reqwest::Client,
    ) -> Result<Self> {
        let id = url::Url::parse(&config.url)
            .ok()
            .and_then(|u| u.host_str().map(String::from))
            .unwrap_or_else(|| config.url.clone());
        let inner = RegistryClient::new_with_client(config, cache_dir, http).await?;
        Ok(Self { inner, id })
    }

    /// Borrow the inner concrete client. Use sparingly — anything new
    /// should go through the trait.
    pub fn inner(&self) -> &RegistryClient {
        &self.inner
    }
}

#[async_trait]
impl FhirRegistryClient for NpmRegistryClient {
    fn id(&self) -> &str {
        &self.id
    }

    async fn search(&self, query: &SearchQuery) -> Result<Vec<PackageHit>> {
        // Existing inner method already hits /catalog with proper case-tolerant
        // parsing. Pass through name; richer params (canonical, fhir_version,
        // prerelease) are not yet plumbed — TODO once we move /catalog params
        // into RegistryClient itself.
        let q = query.name.clone().unwrap_or_default();
        let infos: Vec<PackageInfo> = self.inner.search_packages(&q).await?;
        let id = self.id.clone();
        Ok(infos
            .into_iter()
            .map(|p| PackageHit {
                name: p.name,
                version: p.versions.first().cloned(),
                fhir_version: None,
                description: p.description,
                canonical: None,
                registry: id.clone(),
            })
            .collect())
    }

    async fn list_versions(&self, name: &str) -> Result<Vec<String>> {
        self.inner.list_versions(name).await
    }

    async fn download(
        &self,
        spec: &PackageSpec,
        progress: Option<&dyn DownloadProgress>,
    ) -> Result<PackageDownload> {
        self.inner
            .download_package_with_progress(spec, progress)
            .await
    }
}

// =====================================================================
// Packages2Client — `packages2.fhir.org` (FHIRsmith)
// =====================================================================

/// Client for `packages2.fhir.org/packages` and equivalent FHIRsmith hosts.
///
/// Search uses the same `{base}/catalog?name=...` shape as
/// [`NpmRegistryClient`] (FHIRsmith returns camelCase JSON). Download is
/// **direct**: `GET {base}/{name}/{version}` returns
/// `application/octet-stream` — the `.tgz` bytes — with no JSON manifest,
/// no `shasum`, no integrity header. We trust the bytes on first use.
///
/// `list_versions` is unsupported here: `GET {base}/{name}` returns an HTML
/// page, not the NPM-shaped JSON listing. Callers needing version
/// enumeration should fall back to a different registry in the chain.
#[derive(Debug)]
pub struct Packages2Client {
    client: reqwest::Client,
    base: String,
    cache_dir: PathBuf,
    id: String,
}

impl Packages2Client {
    /// Construct a client against the `packages2.fhir.org`-style endpoint
    /// at `base_url`. `cache_dir` receives downloaded tarballs.
    pub async fn new(base_url: &str, cache_dir: PathBuf, timeout_secs: u64) -> Result<Self> {
        let http = crate::registry::build_default_http_client(timeout_secs)?;
        Self::new_with_client(base_url, cache_dir, http).await
    }

    /// Like `new`, but reuses a shared `reqwest::Client`.
    pub async fn new_with_client(
        base_url: &str,
        cache_dir: PathBuf,
        http: reqwest::Client,
    ) -> Result<Self> {
        let parsed = url::Url::parse(base_url).map_err(|e| RegistryError::InvalidMetadata {
            message: format!("Invalid Packages2 base URL '{base_url}': {e}"),
        })?;
        let id = parsed.host_str().unwrap_or(base_url).to_string();
        tokio::fs::create_dir_all(&cache_dir).await?;
        Ok(Self {
            client: http,
            base: base_url.trim_end_matches('/').to_string(),
            cache_dir,
            id,
        })
    }
}

#[async_trait]
impl FhirRegistryClient for Packages2Client {
    fn id(&self) -> &str {
        &self.id
    }

    async fn search(&self, query: &SearchQuery) -> Result<Vec<PackageHit>> {
        let mut params: Vec<(&str, String)> = Vec::new();
        if let Some(n) = &query.name {
            params.push(("name", n.clone()));
        }
        if let Some(c) = &query.canonical {
            params.push(("canonical", c.clone()));
        }
        if let Some(fv) = &query.fhir_version {
            params.push(("fhirversion", fv.clone()));
        }
        if query.prerelease {
            params.push(("prerelease", "true".to_string()));
        }

        let url = format!("{}/catalog", self.base);
        debug!("Packages2 search GET {url} {params:?}");
        let resp = self
            .client
            .get(&url)
            .query(&params)
            .send()
            .await
            .map_err(|e| RegistryError::InvalidMetadata {
                message: format!("Packages2 search request failed: {e}"),
            })?;

        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND
            || status == reqwest::StatusCode::METHOD_NOT_ALLOWED
        {
            return Err(RegistryError::NotSupported {
                client_id: self.id.clone(),
                operation: "search",
            }
            .into());
        }
        if !status.is_success() {
            return Err(RegistryError::RegistryUnavailable { url }.into());
        }

        let entries: Vec<CatalogEntry> =
            resp.json()
                .await
                .map_err(|e| RegistryError::InvalidMetadata {
                    message: format!("Invalid /catalog JSON from {}: {e}", self.id),
                })?;

        let id = self.id.clone();
        Ok(entries
            .into_iter()
            .map(|e| PackageHit {
                name: e.name,
                version: e.version,
                fhir_version: e.fhir_version,
                description: e.description,
                canonical: e.canonical,
                registry: id.clone(),
            })
            .collect())
    }

    async fn list_versions(&self, _name: &str) -> Result<Vec<String>> {
        // FHIRsmith returns HTML at GET /packages/{name}, not NPM JSON.
        Err(RegistryError::NotSupported {
            client_id: self.id.clone(),
            operation: "list_versions",
        }
        .into())
    }

    async fn download(
        &self,
        spec: &PackageSpec,
        progress: Option<&dyn DownloadProgress>,
    ) -> Result<PackageDownload> {
        // Warm-cache fast path: cached tarball wins over an HTTP GET.
        // packages2.fhir.org tarballs are immutable per (name, version);
        // a cached file is authoritative.
        let file_path = self
            .cache_dir
            .join(format!("{}-{}.tgz", spec.name, spec.version));
        if tokio::fs::metadata(&file_path).await.is_ok() {
            debug!(
                "Packages2 cache hit for {}@{} — skipping HTTP GET",
                spec.name, spec.version
            );
            if let Some(p) = progress {
                let size = tokio::fs::metadata(&file_path).await.ok().map(|m| m.len());
                p.on_start(size);
                p.on_complete();
            }
            return Ok(PackageDownload {
                spec: spec.clone(),
                file_path,
                metadata: crate::registry::PackageMetadata {
                    name: spec.name.clone(),
                    version: spec.version.clone(),
                    fhir_version: "unknown".to_string(),
                    description: None,
                    dependencies: Default::default(),
                    canonical_base: None,
                },
            });
        }

        // Tarball-direct: GET {base}/{name}/{version} → octet-stream tarball.
        let url = format!("{}/{}/{}", self.base, spec.name, spec.version);
        info!(
            "Packages2 direct download {}@{} from {url}",
            spec.name, spec.version
        );

        let resp =
            self.client
                .get(&url)
                .send()
                .await
                .map_err(|e| RegistryError::InvalidMetadata {
                    message: format!("Packages2 download request failed: {e}"),
                })?;

        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(RegistryError::PackageNotFound {
                name: spec.name.clone(),
                version: spec.version.clone(),
            }
            .into());
        }
        if !status.is_success() {
            return Err(RegistryError::RegistryUnavailable { url }.into());
        }

        let total = resp.content_length();
        if let Some(p) = progress {
            p.on_start(total);
        }

        let part_path = file_path.with_extension("tgz.part");
        let mut file = tokio::fs::File::create(&part_path).await?;
        let mut downloaded: u64 = 0;
        let mut stream = resp.bytes_stream();
        use futures_util::StreamExt;
        use tokio::io::AsyncWriteExt;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| RegistryError::InvalidMetadata {
                message: format!("Packages2 stream error: {e}"),
            })?;
            file.write_all(&chunk).await?;
            downloaded += chunk.len() as u64;
            if downloaded > MAX_TARBALL_BYTES {
                let _ = tokio::fs::remove_file(&part_path).await;
                return Err(RegistryError::InvalidMetadata {
                    message: format!(
                        "Packages2 download of {}@{} exceeded {} bytes limit",
                        spec.name, spec.version, MAX_TARBALL_BYTES
                    ),
                }
                .into());
            }
            if let Some(p) = progress {
                p.on_progress(downloaded, total);
            }
        }
        file.sync_all().await?;
        drop(file);
        tokio::fs::rename(&part_path, &file_path).await?;
        if let Some(p) = progress {
            p.on_complete();
        }

        Ok(PackageDownload {
            spec: spec.clone(),
            file_path,
            metadata: crate::registry::PackageMetadata {
                name: spec.name.clone(),
                version: spec.version.clone(),
                fhir_version: "unknown".to_string(),
                description: None,
                dependencies: Default::default(),
                canonical_base: None,
            },
        })
    }
}

// =====================================================================
// GetIgRegistryClient — `fs.get-ig.org` S3-flat archive
// =====================================================================

/// Client for `fs.get-ig.org`-style S3-flat archives.
///
/// Search uses the gzipped bulk index `pgks.ndjson.gz` cached locally with
/// a 1-hour TTL (matches the upstream `Cache-Control: max-age=3600`).
/// Download follows the `pkgs/{name}/{version}` JSON stub's `dist.tarball`
/// pointer (which lives under `/-/{name}-{version}.tgz`). Version listing
/// is supported via S3 XML `?prefix=pkgs/{name}/`.
///
/// **Status:** stub. Full implementation lands in P0.10 follow-up.
/// Until then `search`/`list_versions` return empty/`NotSupported` so the
/// orchestrator can be wired without blocking on this client. The
/// `download` path is straightforward and IS implemented (follows the
/// stub-then-tarball indirection).
#[derive(Debug)]
pub struct GetIgRegistryClient {
    client: reqwest::Client,
    base: String,
    cache_dir: PathBuf,
    id: String,
}

impl GetIgRegistryClient {
    /// Construct a client against the `fs.get-ig.org`-style S3 archive at
    /// `base_url`. `cache_dir` receives downloaded tarballs.
    pub async fn new(base_url: &str, cache_dir: PathBuf, timeout_secs: u64) -> Result<Self> {
        let http = crate::registry::build_default_http_client(timeout_secs)?;
        Self::new_with_client(base_url, cache_dir, http).await
    }

    /// Like `new`, but reuses a shared `reqwest::Client`.
    pub async fn new_with_client(
        base_url: &str,
        cache_dir: PathBuf,
        http: reqwest::Client,
    ) -> Result<Self> {
        let parsed = url::Url::parse(base_url).map_err(|e| RegistryError::InvalidMetadata {
            message: format!("Invalid get-ig base URL '{base_url}': {e}"),
        })?;
        let id = parsed.host_str().unwrap_or(base_url).to_string();
        tokio::fs::create_dir_all(&cache_dir).await?;
        Ok(Self {
            client: http,
            base: base_url.trim_end_matches('/').to_string(),
            cache_dir,
            id,
        })
    }
}

#[async_trait]
impl FhirRegistryClient for GetIgRegistryClient {
    fn id(&self) -> &str {
        &self.id
    }

    async fn search(&self, _query: &SearchQuery) -> Result<Vec<PackageHit>> {
        // TODO P0.10 follow-up: download pgks.ndjson.gz (cache 1h), grep.
        Err(RegistryError::NotSupported {
            client_id: self.id.clone(),
            operation: "search",
        }
        .into())
    }

    async fn list_versions(&self, _name: &str) -> Result<Vec<String>> {
        // TODO: parse S3 XML at GET /?prefix=pkgs/{name}/
        Err(RegistryError::NotSupported {
            client_id: self.id.clone(),
            operation: "list_versions",
        }
        .into())
    }

    async fn download(
        &self,
        spec: &PackageSpec,
        progress: Option<&dyn DownloadProgress>,
    ) -> Result<PackageDownload> {
        // Two-step: fetch the small JSON stub at /pkgs/{name}/{version},
        // then follow `dist.tarball`. The stub omits `shasum` so there is
        // no integrity check at this layer — TOFU model.
        #[derive(Deserialize)]
        struct GetIgStub {
            dist: GetIgDist,
        }
        #[derive(Deserialize)]
        struct GetIgDist {
            tarball: String,
        }

        // Warm-cache fast path: if the tarball already lives on disk,
        // skip both HTTP round trips entirely. get-ig publishes immutable
        // tarballs per (name, version), so a cached file is authoritative.
        let file_path = self
            .cache_dir
            .join(format!("{}-{}.tgz", spec.name, spec.version));
        if tokio::fs::metadata(&file_path).await.is_ok() {
            debug!(
                "get-ig cache hit for {}@{} — skipping stub + tarball GETs",
                spec.name, spec.version
            );
            if let Some(p) = progress {
                let size = tokio::fs::metadata(&file_path).await.ok().map(|m| m.len());
                p.on_start(size);
                p.on_complete();
            }
            return Ok(PackageDownload {
                spec: spec.clone(),
                file_path,
                metadata: crate::registry::PackageMetadata {
                    name: spec.name.clone(),
                    version: spec.version.clone(),
                    fhir_version: "unknown".to_string(),
                    description: None,
                    dependencies: Default::default(),
                    canonical_base: None,
                },
            });
        }

        let stub_url = format!("{}/pkgs/{}/{}", self.base, spec.name, spec.version);
        debug!("get-ig stub GET {stub_url}");
        let stub_resp = self.client.get(&stub_url).send().await.map_err(|e| {
            RegistryError::InvalidMetadata {
                message: format!("get-ig stub request failed: {e}"),
            }
        })?;
        let status = stub_resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(RegistryError::PackageNotFound {
                name: spec.name.clone(),
                version: spec.version.clone(),
            }
            .into());
        }
        if !status.is_success() {
            return Err(RegistryError::RegistryUnavailable { url: stub_url }.into());
        }
        let stub: GetIgStub =
            stub_resp
                .json()
                .await
                .map_err(|e| RegistryError::InvalidMetadata {
                    message: format!("Invalid get-ig stub JSON: {e}"),
                })?;

        let tarball_url = stub.dist.tarball;
        info!(
            "get-ig download {}@{} via {tarball_url}",
            spec.name, spec.version
        );
        let tar_resp = self.client.get(&tarball_url).send().await.map_err(|e| {
            RegistryError::InvalidMetadata {
                message: format!("get-ig tarball request failed: {e}"),
            }
        })?;
        if !tar_resp.status().is_success() {
            return Err(RegistryError::RegistryUnavailable { url: tarball_url }.into());
        }

        let total = tar_resp.content_length();
        if let Some(p) = progress {
            p.on_start(total);
        }
        let part_path = file_path.with_extension("tgz.part");
        let mut file = tokio::fs::File::create(&part_path).await?;
        let mut downloaded: u64 = 0;
        let mut stream = tar_resp.bytes_stream();
        use futures_util::StreamExt;
        use tokio::io::AsyncWriteExt;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| RegistryError::InvalidMetadata {
                message: format!("get-ig stream error: {e}"),
            })?;
            file.write_all(&chunk).await?;
            downloaded += chunk.len() as u64;
            if downloaded > MAX_TARBALL_BYTES {
                let _ = tokio::fs::remove_file(&part_path).await;
                return Err(RegistryError::InvalidMetadata {
                    message: format!(
                        "get-ig download of {}@{} exceeded {} bytes limit",
                        spec.name, spec.version, MAX_TARBALL_BYTES
                    ),
                }
                .into());
            }
            if let Some(p) = progress {
                p.on_progress(downloaded, total);
            }
        }
        file.sync_all().await?;
        drop(file);
        tokio::fs::rename(&part_path, &file_path).await?;
        if let Some(p) = progress {
            p.on_complete();
        }

        Ok(PackageDownload {
            spec: spec.clone(),
            file_path,
            metadata: crate::registry::PackageMetadata {
                name: spec.name.clone(),
                version: spec.version.clone(),
                fhir_version: "unknown".to_string(),
                description: None,
                dependencies: Default::default(),
                canonical_base: None,
            },
        })
    }
}

// =====================================================================
// RedundantRegistryClient — federated orchestrator
// =====================================================================

/// Composes a chain of [`FhirRegistryClient`]s.
///
/// **Search:** fan out across all clients in parallel via
/// `tokio::task::JoinSet`. Per-client errors are logged and skipped; the
/// caller sees the union of successful results, deduped by `(name, version)`
/// (or `(name, None)` when version is unknown).
///
/// **Download / list_versions:** sequential first-success. Tries each client
/// in order, returns the first `Ok`. If every client returns
/// `NotSupported`, propagates `NotSupported`; otherwise propagates the
/// first non-`NotSupported` error from the last attempt.
///
/// Construction takes a non-empty client list. An empty chain is a logic
/// bug, hence the assertion in [`Self::new`].
pub struct RedundantRegistryClient {
    clients: Vec<Arc<dyn FhirRegistryClient>>,
    id: String,
}

impl std::fmt::Debug for RedundantRegistryClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedundantRegistryClient")
            .field("id", &self.id)
            .field(
                "clients",
                &self.clients.iter().map(|c| c.id()).collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl RedundantRegistryClient {
    /// Build a chain over `clients`. Order matters: `download` and
    /// `list_versions` try clients sequentially first-success; `search`
    /// fans out in parallel.
    pub fn new(clients: Vec<Arc<dyn FhirRegistryClient>>) -> Self {
        assert!(
            !clients.is_empty(),
            "RedundantRegistryClient requires at least one backend"
        );
        let id = clients.iter().map(|c| c.id()).collect::<Vec<_>>().join("+");
        Self { clients, id }
    }

    /// Build the default chain from a [`RegistryConfig`].
    ///
    /// Order: primary NPM client first, then one NPM client per `fallbacks`
    /// entry, then one client per `extra` entry (dispatched by
    /// [`RegistryClientType`]). The cache directory is shared — each client
    /// gets a per-host subdirectory so artefacts don't collide.
    pub async fn from_config(config: &RegistryConfig, cache_dir: PathBuf) -> Result<Self> {
        let http = crate::registry::build_default_http_client(config.timeout)?;
        Self::from_config_with_client(config, cache_dir, http).await
    }

    /// Build the default chain reusing a shared `reqwest::Client`. All
    /// sub-clients use clones of the same client so they share a single
    /// `hyper` connection pool — TCP/TLS handshakes are amortised across
    /// requests to the same registry host.
    pub async fn from_config_with_client(
        config: &RegistryConfig,
        cache_dir: PathBuf,
        http: reqwest::Client,
    ) -> Result<Self> {
        let mut clients: Vec<Arc<dyn FhirRegistryClient>> = Vec::new();

        // Primary: always NPM-spec.
        let primary_dir = cache_dir.join("primary");
        let primary = NpmRegistryClient::new_with_client(config, primary_dir, http.clone()).await?;
        clients.push(Arc::new(primary));

        // NPM fallbacks.
        for (i, fallback_url) in config.fallbacks.iter().enumerate() {
            let mut fb_config = config.clone();
            fb_config.url = fallback_url.clone();
            // Avoid recursion via the same fallbacks list inside each client.
            fb_config.fallbacks = Vec::new();
            fb_config.extra = Vec::new();
            let fb_dir = cache_dir.join(format!("fallback_{i}"));
            match NpmRegistryClient::new_with_client(&fb_config, fb_dir, http.clone()).await {
                Ok(c) => clients.push(Arc::new(c)),
                Err(e) => warn!("Skipping fallback registry {fallback_url}: {e}"),
            }
        }

        // Extra non-NPM backends.
        for (i, extra) in config.extra.iter().enumerate() {
            let extra_dir = cache_dir.join(format!("extra_{i}"));
            match extra_client_with_http(extra, extra_dir, config.timeout, http.clone()).await {
                Ok(c) => clients.push(c),
                Err(e) => warn!(
                    "Skipping extra registry {} ({:?}): {e}",
                    extra.url, extra.client_type
                ),
            }
        }

        Ok(Self::new(clients))
    }
}

async fn extra_client_with_http(
    spec: &ExtraRegistry,
    cache_dir: PathBuf,
    _timeout_secs: u64,
    http: reqwest::Client,
) -> Result<Arc<dyn FhirRegistryClient>> {
    Ok(match spec.client_type {
        RegistryClientType::Npm => {
            let cfg = RegistryConfig {
                url: spec.url.clone(),
                token_env: spec.token_env.clone(),
                ..RegistryConfig::default()
            };
            Arc::new(NpmRegistryClient::new_with_client(&cfg, cache_dir, http).await?)
        }
        RegistryClientType::TarballDirect => {
            Arc::new(Packages2Client::new_with_client(&spec.url, cache_dir, http).await?)
        }
        RegistryClientType::S3Flat => {
            Arc::new(GetIgRegistryClient::new_with_client(&spec.url, cache_dir, http).await?)
        }
    })
}

#[allow(dead_code)]
async fn extra_client(
    spec: &ExtraRegistry,
    cache_dir: PathBuf,
    timeout_secs: u64,
) -> Result<Arc<dyn FhirRegistryClient>> {
    let http = crate::registry::build_default_http_client(timeout_secs)?;
    extra_client_with_http(spec, cache_dir, timeout_secs, http).await
}

#[async_trait]
impl FhirRegistryClient for RedundantRegistryClient {
    fn id(&self) -> &str {
        &self.id
    }

    async fn search(&self, query: &SearchQuery) -> Result<Vec<PackageHit>> {
        // Fan out in parallel; log + skip per-client errors.
        let mut joinset: tokio::task::JoinSet<(String, Result<Vec<PackageHit>>)> =
            tokio::task::JoinSet::new();
        for client in &self.clients {
            let client = Arc::clone(client);
            let q = query.clone();
            let cid = client.id().to_string();
            joinset.spawn(async move {
                let r = client.search(&q).await;
                (cid, r)
            });
        }

        let mut all: Vec<PackageHit> = Vec::new();
        let mut last_err: Option<RegistryError> = None;
        let mut any_ok = false;
        while let Some(joined) = joinset.join_next().await {
            match joined {
                Ok((cid, Ok(hits))) => {
                    any_ok = true;
                    debug!("registry {cid} returned {} hits", hits.len());
                    all.extend(hits);
                }
                Ok((cid, Err(e))) => {
                    debug!("registry {cid} search failed: {e}");
                    if let crate::error::FcmError::Registry(re) = e {
                        last_err = Some(re);
                    }
                }
                Err(je) => warn!("search task panicked: {je}"),
            }
        }

        if !any_ok {
            return Err(last_err
                .unwrap_or(RegistryError::NotSupported {
                    client_id: self.id.clone(),
                    operation: "search",
                })
                .into());
        }

        // Dedupe by (name, version) — keep first occurrence which preserves
        // the per-client order (primary first).
        let mut seen: std::collections::HashSet<(String, Option<String>)> =
            std::collections::HashSet::new();
        all.retain(|h| seen.insert((h.name.clone(), h.version.clone())));
        all.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(all)
    }

    async fn list_versions(&self, name: &str) -> Result<Vec<String>> {
        let mut last_real_err: Option<RegistryError> = None;
        for client in &self.clients {
            match client.list_versions(name).await {
                Ok(v) => return Ok(v),
                Err(crate::error::FcmError::Registry(RegistryError::NotSupported { .. })) => {
                    continue;
                }
                Err(crate::error::FcmError::Registry(e)) => {
                    debug!("list_versions on {} failed: {e}", client.id());
                    last_real_err = Some(e);
                }
                Err(e) => {
                    debug!("list_versions on {} failed: {e}", client.id());
                    return Err(e);
                }
            }
        }
        Err(last_real_err
            .unwrap_or(RegistryError::NotSupported {
                client_id: self.id.clone(),
                operation: "list_versions",
            })
            .into())
    }

    async fn download(
        &self,
        spec: &PackageSpec,
        progress: Option<&dyn DownloadProgress>,
    ) -> Result<PackageDownload> {
        let mut last_real_err: Option<RegistryError> = None;
        for client in &self.clients {
            match client.download(spec, progress).await {
                Ok(d) => {
                    info!(
                        "downloaded {}@{} from {}",
                        spec.name,
                        spec.version,
                        client.id()
                    );
                    return Ok(d);
                }
                Err(crate::error::FcmError::Registry(RegistryError::NotSupported { .. })) => {
                    continue;
                }
                Err(crate::error::FcmError::Registry(
                    e @ RegistryError::PackageNotFound { .. },
                )) => {
                    // Not found on this backend — try the next one.
                    debug!("download on {} returned NotFound: {e}", client.id());
                    last_real_err = Some(e);
                    continue;
                }
                Err(crate::error::FcmError::Registry(e)) => {
                    warn!("download on {} failed: {e}", client.id());
                    last_real_err = Some(e);
                    continue;
                }
                Err(e) => {
                    warn!("download on {} failed: {e}", client.id());
                    return Err(e);
                }
            }
        }
        Err(last_real_err
            .unwrap_or(RegistryError::PackageNotFound {
                name: spec.name.clone(),
                version: spec.version.clone(),
            })
            .into())
    }
}

// =====================================================================
// Shared catalog entry type (used by NPM-style and FHIRsmith backends)
// =====================================================================

/// Same shape as `crate::registry::CatalogEntry` but exposed at this layer
/// for [`Packages2Client`]. Casing-tolerant via `#[serde(alias)]`.
#[derive(Debug, Deserialize, Clone)]
struct CatalogEntry {
    #[serde(alias = "Name")]
    name: String,
    #[serde(default, alias = "Version")]
    version: Option<String>,
    // packages.fhir.org uses PascalCase `FhirVersion`; packages2.fhir.org
    // uses camelCase `fhirVersion`. Accept both.
    #[serde(default, alias = "FhirVersion", alias = "fhirVersion")]
    fhir_version: Option<String>,
    #[serde(default, alias = "Description")]
    description: Option<String>,
    #[serde(default, alias = "Canonical")]
    canonical: Option<String>,
    #[serde(default, alias = "Url")]
    #[allow(dead_code)]
    url: Option<String>,
    #[serde(default, alias = "Date")]
    #[allow(dead_code)]
    date: Option<String>,
    #[serde(default, alias = "Kind")]
    #[allow(dead_code)]
    kind: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::registry::PackageMetadata;
    use std::collections::HashMap;
    use std::path::Path;

    /// In-memory mock client for orchestrator unit tests.
    #[derive(Debug, Clone)]
    struct StubClient {
        id: String,
        search_result: std::result::Result<Vec<PackageHit>, RegistryError>,
        list_result: std::result::Result<Vec<String>, RegistryError>,
        download_result: std::result::Result<PathBuf, RegistryError>,
    }

    impl StubClient {
        fn new(id: &str) -> Self {
            Self {
                id: id.to_string(),
                search_result: Ok(Vec::new()),
                list_result: Err(RegistryError::NotSupported {
                    client_id: id.to_string(),
                    operation: "list_versions",
                }),
                download_result: Err(RegistryError::PackageNotFound {
                    name: "default".into(),
                    version: "default".into(),
                }),
            }
        }

        fn with_search(mut self, r: std::result::Result<Vec<PackageHit>, RegistryError>) -> Self {
            self.search_result = r;
            self
        }
        fn with_list(mut self, r: std::result::Result<Vec<String>, RegistryError>) -> Self {
            self.list_result = r;
            self
        }
        fn with_download(mut self, r: std::result::Result<PathBuf, RegistryError>) -> Self {
            self.download_result = r;
            self
        }
    }

    #[async_trait]
    impl FhirRegistryClient for StubClient {
        fn id(&self) -> &str {
            &self.id
        }
        async fn search(&self, _q: &SearchQuery) -> Result<Vec<PackageHit>> {
            self.search_result.clone().map_err(Into::into)
        }
        async fn list_versions(&self, _name: &str) -> Result<Vec<String>> {
            self.list_result.clone().map_err(Into::into)
        }
        async fn download(
            &self,
            spec: &PackageSpec,
            _progress: Option<&dyn DownloadProgress>,
        ) -> Result<PackageDownload> {
            self.download_result
                .clone()
                .map(|file_path| PackageDownload {
                    spec: spec.clone(),
                    file_path,
                    metadata: PackageMetadata {
                        name: spec.name.clone(),
                        version: spec.version.clone(),
                        fhir_version: "unknown".into(),
                        description: None,
                        dependencies: HashMap::new(),
                        canonical_base: None,
                    },
                })
                .map_err(Into::into)
        }
    }

    fn pkg(name: &str, version: &str) -> PackageSpec {
        PackageSpec {
            name: name.into(),
            version: version.into(),
            priority: 1,
            url: None,
        }
    }

    fn hit(name: &str, version: &str, registry: &str) -> PackageHit {
        PackageHit {
            name: name.into(),
            version: Some(version.into()),
            fhir_version: None,
            description: None,
            canonical: None,
            registry: registry.into(),
        }
    }

    #[tokio::test]
    async fn search_fans_out_and_dedupes() {
        let a = StubClient::new("a").with_search(Ok(vec![hit("hl7.fhir.r4.core", "4.0.1", "a")]));
        let b = StubClient::new("b").with_search(Ok(vec![
            hit("hl7.fhir.r4.core", "4.0.1", "b"), // dup of (name,version)
            hit("hl7.fhir.us.core", "7.0.0", "b"),
        ]));
        let orch = RedundantRegistryClient::new(vec![Arc::new(a), Arc::new(b)]);
        let hits = orch.search(&SearchQuery::default()).await.unwrap();
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().any(|h| h.name == "hl7.fhir.r4.core"));
        assert!(hits.iter().any(|h| h.name == "hl7.fhir.us.core"));
    }

    #[tokio::test]
    async fn search_skips_failing_client() {
        let a = StubClient::new("a").with_search(Err(RegistryError::RegistryUnavailable {
            url: "http://a".into(),
        }));
        let b = StubClient::new("b").with_search(Ok(vec![hit("p", "1.0.0", "b")]));
        let orch = RedundantRegistryClient::new(vec![Arc::new(a), Arc::new(b)]);
        let hits = orch.search(&SearchQuery::default()).await.unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].name, "p");
    }

    #[tokio::test]
    async fn search_propagates_when_all_fail() {
        let a = StubClient::new("a").with_search(Err(RegistryError::RegistryUnavailable {
            url: "http://a".into(),
        }));
        let b = StubClient::new("b").with_search(Err(RegistryError::RegistryUnavailable {
            url: "http://b".into(),
        }));
        let orch = RedundantRegistryClient::new(vec![Arc::new(a), Arc::new(b)]);
        let r = orch.search(&SearchQuery::default()).await;
        assert!(r.is_err());
    }

    #[tokio::test]
    async fn list_versions_skips_not_supported() {
        let a = StubClient::new("a"); // default = NotSupported
        let b = StubClient::new("b").with_list(Ok(vec!["1.0.0".into(), "0.9.0".into()]));
        let orch = RedundantRegistryClient::new(vec![Arc::new(a), Arc::new(b)]);
        let v = orch.list_versions("x").await.unwrap();
        assert_eq!(v, vec!["1.0.0", "0.9.0"]);
    }

    #[tokio::test]
    async fn download_first_success_wins() {
        let a = StubClient::new("a"); // default = PackageNotFound
        let b = StubClient::new("b").with_download(Ok(Path::new("/tmp/ok").to_path_buf()));
        let c = StubClient::new("c").with_download(Ok(Path::new("/tmp/never").to_path_buf()));
        let orch = RedundantRegistryClient::new(vec![Arc::new(a), Arc::new(b), Arc::new(c)]);
        let d = orch.download(&pkg("p", "1"), None).await.unwrap();
        assert_eq!(d.file_path, Path::new("/tmp/ok"));
    }

    #[tokio::test]
    async fn download_skips_not_supported_then_propagates_real_error() {
        let a = StubClient::new("a"); // default = PackageNotFound (real error)
        let b = StubClient::new("b").with_download(Err(RegistryError::NotSupported {
            client_id: "b".into(),
            operation: "download",
        }));
        let orch = RedundantRegistryClient::new(vec![Arc::new(a), Arc::new(b)]);
        let r = orch.download(&pkg("p", "1"), None).await;
        // PackageNotFound is the most useful error (the "real" failure),
        // not the NotSupported follow-up.
        assert!(matches!(
            r.unwrap_err(),
            crate::error::FcmError::Registry(RegistryError::PackageNotFound { .. })
        ));
    }

    #[test]
    #[should_panic(expected = "RedundantRegistryClient requires at least one backend")]
    fn empty_chain_panics() {
        let _ = RedundantRegistryClient::new(Vec::new());
    }
}
