//! Domain types: canonical URLs and versions

use serde::{Deserialize, Serialize};

/// Normalized canonical URL newtype.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CanonicalUrl(String);

impl CanonicalUrl {
    /// Parse and normalize a canonical URL string.
    /// - Lowercase host
    /// - Remove trailing slash (except root)
    /// - Preserve percent-encoding
    #[allow(clippy::result_unit_err)]
    pub fn parse(s: &str) -> Result<Self, ()> {
        match url::Url::parse(s) {
            Ok(mut u) => {
                if let Some(host) = u.host_str() {
                    let _ = u.set_host(Some(&host.to_ascii_lowercase()));
                }
                // Normalize path: drop trailing slash unless root
                let mut path = u.path().to_string();
                if path.len() > 1 && path.ends_with('/') {
                    path.pop();
                    u.set_path(&path);
                }
                Ok(Self(u.to_string()))
            }
            Err(_) => Err(()),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<CanonicalUrl> for String {
    fn from(c: CanonicalUrl) -> Self {
        c.0
    }
}

/// Package/resource version represented using semver when possible, preserving original.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PackageVersion {
    pub original: String,
    #[serde(skip)]
    pub semver: Option<semver::Version>,
}

impl PackageVersion {
    pub fn parse(s: &str) -> Self {
        let trimmed = s.trim_start_matches('v');
        let semver = semver::Version::parse(trimmed).ok();
        Self {
            original: s.to_string(),
            semver,
        }
    }
}

impl PartialOrd for PackageVersion {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for PackageVersion {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (&self.semver, &other.semver) {
            (Some(a), Some(b)) => a.cmp(b),
            _ => self.original.cmp(&other.original),
        }
    }
}

/// Canonical with optional version using `url|version` syntax.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CanonicalWithVersion {
    pub canonical: String,
    pub version: Option<PackageVersion>,
}

impl CanonicalWithVersion {
    pub fn parse(input: &str) -> Self {
        if let Some((u, v)) = input.split_once('|') {
            let canonical = CanonicalUrl::parse(u)
                .map(|c| c.0)
                .unwrap_or_else(|_| u.trim().to_string());
            let version =
                Some(PackageVersion::parse(v.trim())).filter(|pv| !pv.original.is_empty());
            Self { canonical, version }
        } else {
            let canonical = CanonicalUrl::parse(input)
                .map(|c| c.0)
                .unwrap_or_else(|_| input.trim().to_string());
            Self {
                canonical,
                version: None,
            }
        }
    }
}

/// FHIR spec version family
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FhirVersion {
    R4,
    R4B,
    R5,
    Other(String),
}

impl FhirVersion {
    pub fn parse(s: &str) -> Self {
        let lower = s.trim().to_ascii_lowercase();
        match lower.as_str() {
            "r4" | "4.0.1" | "4.0.0" => FhirVersion::R4,
            "r4b" | "4.3.0" => FhirVersion::R4B,
            "r5" | "5.0.0" | "5.0.1" => FhirVersion::R5,
            other => FhirVersion::Other(other.to_string()),
        }
    }
}
