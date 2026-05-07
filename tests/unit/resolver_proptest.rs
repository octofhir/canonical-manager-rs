//! Property-based tests for canonical-URL parsing and version semantics.
//!
//! These tests don't touch storage — they exercise the pure parsing
//! invariants on `CanonicalUrl`, `PackageVersion`, and
//! `CanonicalWithVersion`. The goal is to catch regressions in the
//! resolver's URL/version handling under randomized input.

use octofhir_canonical_manager::domain::{
    CanonicalUrl, CanonicalWithVersion, FhirVersion, PackageVersion,
};
use proptest::prelude::*;

/// Generate plausible canonical-URL strings: scheme + host + path.
fn arb_canonical_url() -> impl Strategy<Value = String> {
    let scheme = prop_oneof![Just("http"), Just("https")];
    let host = "[a-z][a-z0-9-]{0,15}(\\.[a-z][a-z0-9-]{0,15}){1,3}";
    let path_seg = "[a-zA-Z0-9_-]{1,12}";
    (scheme, host, prop::collection::vec(path_seg, 0..5)).prop_map(|(scheme, host, segs)| {
        if segs.is_empty() {
            format!("{scheme}://{host}/")
        } else {
            format!("{scheme}://{host}/{}", segs.join("/"))
        }
    })
}

/// Generate semver-shaped version strings, plus some non-semver strings.
fn arb_version() -> impl Strategy<Value = String> {
    prop_oneof![
        (0u32..50, 0u32..50, 0u32..50).prop_map(|(a, b, c)| format!("{a}.{b}.{c}")),
        (0u32..50, 0u32..50, 0u32..50).prop_map(|(a, b, c)| format!("{a}.{b}.{c}-rc.1")),
        Just("dev".to_string()),
        Just("snapshot".to_string()),
        Just("4.0.1".to_string()),
        Just("5.0.0".to_string()),
    ]
}

proptest! {
    /// Parsing a valid URL twice yields the same normalized form
    /// (idempotency).
    #[test]
    fn canonical_url_parse_is_idempotent(s in arb_canonical_url()) {
        let once = CanonicalUrl::parse(&s).expect("valid URL parses");
        let twice = CanonicalUrl::parse(once.as_str()).expect("normalized URL re-parses");
        prop_assert_eq!(once.as_str(), twice.as_str());
    }

    /// Host-case differences round-trip to the same normalized URL. Path
    /// case is preserved (only the host is lowercased).
    #[test]
    fn canonical_url_host_case_normalised(
        scheme in prop_oneof![Just("http"), Just("https")],
        host in "[a-z][a-z0-9-]{0,15}(\\.[a-z][a-z0-9-]{0,15}){1,3}",
        path in "[a-zA-Z0-9_/-]{0,30}",
    ) {
        let lower = format!("{scheme}://{host}/{path}");
        let upper = format!("{scheme}://{}/{path}", host.to_ascii_uppercase());
        let a = CanonicalUrl::parse(&lower);
        let b = CanonicalUrl::parse(&upper);
        if let (Ok(a), Ok(b)) = (a, b) {
            prop_assert_eq!(a.as_str(), b.as_str());
        }
    }

    /// Trailing slashes (except root) collapse during normalization.
    #[test]
    fn canonical_url_trailing_slash_stripped(s in arb_canonical_url()) {
        if !s.ends_with('/') {
            let with_slash = format!("{s}/");
            if let (Ok(a), Ok(b)) = (CanonicalUrl::parse(&s), CanonicalUrl::parse(&with_slash)) {
                prop_assert_eq!(a.as_str(), b.as_str());
            }
        }
    }

    /// PackageVersion::parse always succeeds (it stores the original even
    /// if semver fails).
    #[test]
    fn package_version_parse_total(v in arb_version()) {
        let pv = PackageVersion::parse(&v);
        prop_assert_eq!(pv.original, v);
    }

    /// PackageVersion ordering is total: cmp(a,b) and cmp(b,a) are
    /// opposites (or both Equal).
    #[test]
    fn package_version_cmp_antisymmetric(a in arb_version(), b in arb_version()) {
        let pa = PackageVersion::parse(&a);
        let pb = PackageVersion::parse(&b);
        let ab = pa.cmp(&pb);
        let ba = pb.cmp(&pa);
        prop_assert_eq!(ab, ba.reverse());
    }

    /// CanonicalWithVersion round-trips: parse(`url|ver`) carries the URL
    /// and version through.
    #[test]
    fn cwv_parse_pipe_roundtrip(url in arb_canonical_url(), ver in arb_version()) {
        let combined = format!("{url}|{ver}");
        let cwv = CanonicalWithVersion::parse(&combined);
        // canonical may be normalized; just check it's non-empty.
        prop_assert!(!cwv.canonical.is_empty());
        prop_assert!(cwv.version.is_some());
        let pv = cwv.version.expect("version present");
        prop_assert_eq!(pv.original, ver);
    }

    /// CanonicalWithVersion::parse without `|` yields no version.
    #[test]
    fn cwv_parse_no_pipe_no_version(url in arb_canonical_url()) {
        prop_assume!(!url.contains('|'));
        let cwv = CanonicalWithVersion::parse(&url);
        prop_assert!(cwv.version.is_none());
    }

    /// Trailing pipe with empty version: version stays None (not Some("")).
    #[test]
    fn cwv_parse_empty_version_after_pipe(url in arb_canonical_url()) {
        let combined = format!("{url}|");
        let cwv = CanonicalWithVersion::parse(&combined);
        prop_assert!(cwv.version.is_none());
    }

    /// FhirVersion round-trips canonical short codes.
    #[test]
    fn fhir_version_canonical_codes(code in prop_oneof![
        Just("R4"), Just("r4"), Just("4.0.1"), Just("4.0.0"),
        Just("R4B"), Just("r4b"), Just("4.3.0"),
        Just("R5"), Just("r5"), Just("5.0.0"), Just("5.0.1"),
    ]) {
        let fv = FhirVersion::parse(code);
        prop_assert!(matches!(
            fv,
            FhirVersion::R4 | FhirVersion::R4B | FhirVersion::R5
        ));
    }

    /// Unknown FHIR codes fall through to Other.
    #[test]
    fn fhir_version_unknown_falls_back(s in "[a-z]{2,8}") {
        prop_assume!(!matches!(s.as_str(), "r4" | "r4b" | "r5"));
        let fv = FhirVersion::parse(&s);
        prop_assert!(matches!(fv, FhirVersion::Other(_)));
    }
}
