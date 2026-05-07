//! Configuration management example for FHIR Canonical Manager
//!
//! This example demonstrates:
//! - Creating and customizing configuration
//! - Working with registry settings
//! - Managing storage directories
//! - Loading and saving configuration files

use octofhir_canonical_manager::{FcmConfig, RegistryConfig, StorageConfig};
use std::error::Error;
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("⚙️  Configuration Management Example");
    println!("====================================\n");

    // Create a default configuration
    println!("📝 Creating default configuration...");
    let mut config = FcmConfig::default();

    println!("✅ Default configuration created");
    println!("   Registry URL: {}", config.registry.url);
    println!("   Cache Dir: {}", config.storage.cache_dir.display());
    println!("   Packages Dir: {}", config.storage.packages_dir.display());

    // Customize registry settings
    println!("\n🌐 Customizing registry settings...");
    config.registry = RegistryConfig {
        url: "https://build.fhir.org/ig/qas.json".to_string(),
        timeout: 60,       // Longer timeout for slower connections
        retry_attempts: 5, // More retries for reliability
        ..RegistryConfig::default()
    };

    println!("✅ Registry settings updated");
    println!("   New URL: {}", config.registry.url);
    println!("   Timeout: {}s", config.registry.timeout);
    println!("   Retry attempts: {}", config.registry.retry_attempts);

    // Customize storage settings
    println!("\n💾 Customizing storage settings...");
    config.storage = StorageConfig {
        cache_dir: PathBuf::from("/tmp/fcm-example/cache"),
        packages_dir: PathBuf::from("/tmp/fcm-example/packages"),
        max_cache_size: "2GB".to_string(), // Larger cache
        connection_pool_size: 32,
        fhir_cache_compat: false,
    };

    println!("✅ Storage settings updated");
    println!("   Cache: {}", config.storage.cache_dir.display());
    println!("   Packages: {}", config.storage.packages_dir.display());
    println!("   Max cache size: {}", config.storage.max_cache_size);

    // Add some packages to the configuration
    println!("\n📦 Adding packages to configuration...");
    config.add_package("hl7.fhir.us.core", "6.1.0", Some(1));
    config.add_package("hl7.fhir.r4.core", "4.0.1", Some(2));
    config.add_package("hl7.fhir.us.mcode", "3.0.0", None); // No specific priority

    println!(
        "✅ Added {} packages to configuration",
        config.packages.len()
    );
    for (i, package) in config.packages.iter().enumerate() {
        println!(
            "   {}. {}@{} (priority: {})",
            i + 1,
            package.name,
            package.version,
            package.priority
        );
    }

    // Validate the configuration
    println!("\n🔍 Validating configuration...");
    match config.validate() {
        Ok(_) => println!("✅ Configuration is valid"),
        Err(e) => {
            println!("❌ Configuration validation failed: {e}");
            return Err(e.into());
        }
    }

    // Save the configuration
    println!("\n💾 Saving configuration to fcm.toml...");
    match config.save().await {
        Ok(_) => {
            println!("✅ Configuration saved successfully");
            println!("   File: fcm.toml");
        }
        Err(e) => {
            println!("⚠️  Could not save configuration: {e}");
        }
    }

    // Load configuration back
    println!("\n📖 Loading configuration from file...");
    match FcmConfig::load().await {
        Ok(loaded_config) => {
            println!("✅ Configuration loaded successfully");
            println!("   Registry: {}", loaded_config.registry.url);
            println!("   Packages: {}", loaded_config.packages.len());
        }
        Err(e) => {
            println!("❌ Failed to load configuration: {e}");
        }
    }

    // Load from a specific file
    println!("\n📁 Loading configuration from specific file...");
    match FcmConfig::from_file(std::path::Path::new("fcm.toml")).await {
        Ok(file_config) => {
            println!("✅ Configuration loaded from fcm.toml");
            println!(
                "   Total packages configured: {}",
                file_config.packages.len()
            );
        }
        Err(e) => {
            println!("⚠️  Could not load from specific file: {e}");
        }
    }

    // Demonstrate working with expanded paths
    println!("\n🔧 Working with expanded storage paths...");
    let expanded_storage = config.get_expanded_storage_config();
    println!("✅ Expanded paths resolved:");
    println!("   Cache: {}", expanded_storage.cache_dir.display());
    println!("   Packages: {}", expanded_storage.packages_dir.display());

    // Demonstrate package management
    println!("\n📝 Package management operations...");
    let mut working_config = config.clone();

    // Remove a package
    working_config.remove_package("hl7.fhir.us.mcode");
    println!("✅ Removed hl7.fhir.us.mcode package");
    println!("   Remaining packages: {}", working_config.packages.len());

    // Add another package
    working_config.add_package("hl7.fhir.us.davinci-pdex", "2.0.0", Some(3));
    println!("✅ Added hl7.fhir.us.davinci-pdex package");
    println!("   Total packages: {}", working_config.packages.len());

    // Show final package list
    println!("\n📋 Final package configuration:");
    for (i, package) in working_config.packages.iter().enumerate() {
        println!(
            "   {}. {}@{} (priority: {})",
            i + 1,
            package.name,
            package.version,
            package.priority
        );
    }

    println!("\n🎉 Configuration example completed!");
    println!("\n💡 Tips:");
    println!("   • Use fcm.toml for persistent configuration");
    println!("   • Adjust registry timeout for slow connections");
    println!("   • Set appropriate cache sizes for your use case");
    println!("   • Use package priorities to control loading order");

    Ok(())
}
