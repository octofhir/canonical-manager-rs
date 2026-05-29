//! Package management example for FHIR Canonical Manager
//!
//! This example demonstrates:
//! - Installing and removing packages
//! - Managing package dependencies
//! - Working with package configurations
//! - Rebuilding indices

use octofhir_canonical_manager::{CanonicalManager, FcmConfig};
use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("🔧 Package Management Example");
    println!("=============================\n");

    // Load or create configuration
    let mut config = match FcmConfig::load().await {
        Ok(config) => {
            println!("✅ Loaded existing configuration");
            config
        }
        Err(_) => {
            println!("📝 Creating new configuration");
            FcmConfig::default()
        }
    };

    // Add some packages to the configuration
    println!("📦 Adding packages to configuration...");
    config.add_package("hl7.fhir.us.core", "6.1.0", Some(1));
    config.add_package("hl7.fhir.r4.core", "4.0.1", Some(2));

    // Save configuration
    match config.save().await {
        Ok(_) => println!("✅ Configuration saved to fcm.toml"),
        Err(e) => println!("⚠️  Could not save configuration: {e}"),
    }

    // Initialize manager
    let manager = CanonicalManager::new(config).await?;

    println!("\n📋 Current installed packages:");
    let installed = manager.list_packages().await?;
    if installed.is_empty() {
        println!("  (none)");
    } else {
        for package in &installed {
            println!("  • {package}");
        }
    }

    // Install packages with dependency resolution
    println!("\n⬇️  Installing packages...");

    let packages_to_install = vec![("hl7.fhir.us.core", "6.1.0"), ("hl7.fhir.r4.core", "4.0.1")];

    for (name, version) in packages_to_install {
        println!("📦 Installing {name}@{version}...");

        match manager.install_package(name, version).await {
            Ok(_) => {
                println!("  ✅ Successfully installed {name}@{version}");
            }
            Err(e) => {
                println!("  ⚠️  Installation failed or package already exists: {e}");
            }
        }
    }

    // List packages after installation
    println!("\n📋 Packages after installation:");
    let installed_after = manager.list_packages().await?;
    for package in &installed_after {
        println!("  • {package}");
    }

    // Demonstrate search functionality after package installation
    println!("\n🔍 Testing package contents...");

    let search_results = manager
        .search()
        .await
        .package("hl7.fhir.us.core")
        .execute()
        .await?;

    println!(
        "📊 Found {} resources in hl7.fhir.us.core",
        search_results.total_count
    );

    // Show distribution by resource type
    use std::collections::HashMap;
    let mut type_counts: HashMap<String, usize> = HashMap::new();

    for resource in &search_results.resources {
        *type_counts
            .entry(resource.resource.resource_type.clone())
            .or_insert(0) += 1;
    }

    println!("📈 Resource type distribution:");
    let mut sorted_types: Vec<_> = type_counts.into_iter().collect();
    sorted_types.sort_by_key(|b| std::cmp::Reverse(b.1));

    for (resource_type, count) in sorted_types.iter().take(5) {
        println!("  • {resource_type}: {count}");
    }

    // Test resolution after rebuild
    println!("\n🎯 Testing canonical URL resolution...");
    let test_urls = vec![
        "http://hl7.org/fhir/us/core/StructureDefinition/us-core-patient",
        "http://hl7.org/fhir/StructureDefinition/Patient",
    ];

    for url in test_urls {
        match manager.resolve(url).await {
            Ok(resource) => {
                println!("  ✅ {url}");
                println!("     -> Resource: {}", resource.resource.resource_type);
                println!("     -> Package: {}", resource.package_info.name);
            }
            Err(e) => {
                println!("  ❌ {url}: {e}");
            }
        }
    }

    // Demonstrate package removal (commented out to avoid breaking the example)
    println!("\n🗑️  Package removal example (demonstration only):");
    println!("   To remove a package, use:");
    println!("   manager.remove_package(\"package-name\", \"version\").await?;");

    /*
    // Uncomment to actually remove packages
    println!("🗑️  Removing test package...");
    match manager.remove_package("hl7.fhir.us.core", "6.1.0").await {
        Ok(_) => println!("✅ Package removed successfully"),
        Err(e) => println!("❌ Failed to remove package: {}", e),
    }

    // List packages after removal
    println!("📋 Packages after removal:");
    let final_packages = manager.list_packages().await?;
    for package in &final_packages {
        println!("  • {}", package);
    }
    */

    println!("\n🎉 Package management example completed!");
    println!("\n💡 Tips:");
    println!("   • Use fcm.toml to pre-configure packages");
    println!("   • Package installation includes dependency resolution");
    println!("   • The search index is automatically rebuilt after installations");
    println!("   • Use rebuild_index() if you need to refresh manually");

    Ok(())
}
