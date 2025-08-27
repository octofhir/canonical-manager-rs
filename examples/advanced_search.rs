//! Advanced search example for FHIR Canonical Manager
//!
//! This example demonstrates advanced search capabilities:
//! - Complex query building
//! - Filtering by multiple criteria
//! - Result analysis and reporting

use octofhir_canonical_manager::{CanonicalManager, FcmConfig};
use std::collections::HashMap;
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("🔧 Setting up FHIR Canonical Manager...");

    let config = FcmConfig::load().await.unwrap_or_else(|_| {
        println!("Using default configuration");
        FcmConfig::default()
    });

    let manager = CanonicalManager::new(config).await?;

    // Ensure we have some packages installed
    println!("📦 Ensuring test packages are available...");
    let test_packages = vec![("hl7.fhir.us.core", "6.1.0"), ("hl7.fhir.r4.core", "4.0.1")];

    for (name, version) in test_packages {
        if let Err(e) = manager.install_package(name, version).await {
            println!("⚠️  Package {name} may already be installed: {e}");
        }
    }

    println!("\n🔍 ==> SEARCH EXAMPLE 1: Find all StructureDefinitions");
    let structure_definitions = manager
        .search()
        .await
        .resource_type("StructureDefinition")
        .execute()
        .await?;

    println!(
        "Found {} StructureDefinitions",
        structure_definitions.total_count
    );

    // Analyze by package
    let mut by_package: HashMap<String, usize> = HashMap::new();
    for resource in &structure_definitions.resources {
        *by_package
            .entry(resource.index.package_name.clone())
            .or_insert(0) += 1;
    }

    println!("📊 Distribution by package:");
    for (package, count) in by_package {
        println!("  • {package}: {count} resources");
    }

    println!("\n🔍 ==> SEARCH EXAMPLE 2: US Core Patient profiles");
    let patient_profiles = manager
        .search()
        .await
        .resource_type("StructureDefinition")
        .package("hl7.fhir.us.core")
        .canonical_pattern(".*Patient.*")
        .execute()
        .await?;

    println!(
        "Found {} US Core Patient-related profiles:",
        patient_profiles.total_count
    );
    for profile in &patient_profiles.resources {
        println!(
            "  • {} -> {}",
            profile.resource.id, profile.index.canonical_url
        );
    }

    println!("\n🔍 ==> SEARCH EXAMPLE 3: ValueSets in US Core");
    let valuesets = manager
        .search()
        .await
        .resource_type("ValueSet")
        .package("hl7.fhir.us.core")
        .execute()
        .await?;

    println!("Found {} ValueSets in US Core:", valuesets.total_count);
    for vs in valuesets.resources.iter().take(10) {
        println!("  • {}", vs.resource.id);
    }
    if valuesets.total_count > 10 {
        println!("  ... and {} more", valuesets.total_count - 10);
    }

    println!("\n🔍 ==> SEARCH EXAMPLE 4: Search by URL pattern");
    let fhir_core_resources = manager
        .search()
        .await
        .canonical_pattern("http://hl7.org/fhir/StructureDefinition/.*")
        .execute()
        .await?;

    println!(
        "Found {} FHIR core resources",
        fhir_core_resources.total_count
    );

    // Group by resource type
    let mut by_type: HashMap<String, usize> = HashMap::new();
    for resource in &fhir_core_resources.resources {
        *by_type
            .entry(resource.resource.resource_type.clone())
            .or_insert(0) += 1;
    }

    println!("📊 Resource type distribution:");
    let mut type_counts: Vec<_> = by_type.into_iter().collect();
    type_counts.sort_by(|a, b| b.1.cmp(&a.1)); // Sort by count descending

    for (resource_type, count) in type_counts.iter().take(10) {
        println!("  • {resource_type}: {count}");
    }

    println!("\n🔍 ==> SEARCH EXAMPLE 5: Cross-package search");
    let all_patient_resources = manager
        .search()
        .await
        .canonical_pattern(".*Patient.*")
        .execute()
        .await?;

    println!(
        "Found {} Patient-related resources across all packages:",
        all_patient_resources.total_count
    );

    // Analyze distribution
    let mut package_distribution: HashMap<String, Vec<String>> = HashMap::new();
    for resource in &all_patient_resources.resources {
        package_distribution
            .entry(resource.index.package_name.clone())
            .or_default()
            .push(resource.resource.id.clone());
    }

    for (package, resources) in package_distribution {
        println!("  📦 {}: {} resources", package, resources.len());
        for resource_id in resources.iter().take(3) {
            println!("     • {resource_id}");
        }
        if resources.len() > 3 {
            println!("     ... and {} more", resources.len() - 3);
        }
    }

    println!("\n🔍 ==> SEARCH EXAMPLE 6: Complex filtering");
    let complex_search = manager
        .search()
        .await
        .resource_type("StructureDefinition")
        .canonical_pattern(".*us-core.*")
        .package("hl7.fhir.us.core")
        .execute()
        .await?;

    println!(
        "Found {} US Core StructureDefinitions:",
        complex_search.total_count
    );

    // Show some details
    for resource in complex_search.resources.iter().take(5) {
        println!("  • ID: {}", resource.resource.id);
        println!("    URL: {}", resource.index.canonical_url);
        println!(
            "    Package: {} v{}",
            resource.index.package_name, resource.index.package_version
        );
        println!();
    }

    println!("🎯 Advanced search examples completed!");
    println!("💡 Tip: You can chain multiple search criteria for precise filtering");

    Ok(())
}
