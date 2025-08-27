//! Basic usage example for FHIR Canonical Manager
//!
//! This example demonstrates the most common operations:
//! - Loading configuration
//! - Installing packages
//! - Resolving canonical URLs
//! - Basic search functionality

use octofhir_canonical_manager::{CanonicalManager, FcmConfig};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("🔧 Loading FCM configuration...");

    // Load configuration from fcm.toml or use defaults
    let config = match FcmConfig::load().await {
        Ok(config) => {
            println!("✅ Loaded configuration from fcm.toml");
            config
        }
        Err(_) => {
            println!("⚠️  No fcm.toml found, using default configuration");
            FcmConfig::default()
        }
    };

    // Initialize the canonical manager
    println!("🚀 Initializing FHIR Canonical Manager...");
    let manager = CanonicalManager::new(config).await?;

    // Install a FHIR package
    println!("📦 Installing hl7.fhir.us.core package...");
    match manager.install_package("hl7.fhir.us.core", "6.1.0").await {
        Ok(_) => println!("✅ Package installed successfully"),
        Err(e) => println!("⚠️  Package may already be installed: {e}"),
    }

    // List installed packages
    println!("📋 Listing installed packages:");
    let packages = manager.list_packages().await?;
    for package in &packages {
        println!("  • {package}");
    }

    // Resolve a canonical URL
    println!("🔍 Resolving canonical URL...");
    let canonical_url = "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient";

    match manager.resolve(canonical_url).await {
        Ok(resource) => {
            println!(
                "✅ Resolved: {} (from {})",
                resource.resource.resource_type, resource.package_info.name
            );
            println!("   Canonical URL: {}", resource.canonical_url);
            println!("   Version: {}", resource.package_info.version);
        }
        Err(e) => {
            println!("❌ Failed to resolve: {e}");
        }
    }

    // Perform a search
    println!("🔎 Searching for Patient resources...");
    let search_results = manager
        .search()
        .await
        .resource_type("StructureDefinition")
        .canonical_pattern(".*Patient.*")
        .execute()
        .await?;

    println!(
        "📊 Found {} Patient-related resources:",
        search_results.total_count
    );
    for (i, result) in search_results.resources.iter().take(5).enumerate() {
        println!(
            "  {}. {} ({})",
            i + 1,
            result.resource.id,
            result.index.canonical_url
        );
    }

    if search_results.total_count > 5 {
        println!("     ... and {} more", search_results.total_count - 5);
    }

    // Batch resolve multiple URLs
    println!("⚡ Batch resolving multiple URLs...");
    let urls = vec![
        "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient".to_string(),
        "http://hl7.org/fhir/us/core/StructureDefinition/us-core-organization".to_string(),
        "http://hl7.org/fhir/us/core/StructureDefinition/us-core-practitioner".to_string(),
    ];

    let batch_results = manager.batch_resolve(&urls).await?;
    println!(
        "✅ Resolved {}/{} URLs in batch",
        batch_results.len(),
        urls.len()
    );

    for result in batch_results {
        println!(
            "  • {}: {}",
            result.resource.resource_type, result.resource.id
        );
    }

    println!("🎉 Example completed successfully!");

    Ok(())
}
