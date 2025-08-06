//! Example demonstrating search parameter functionality
//!
//! This example shows how to retrieve and work with FHIR SearchParameter resources
//! for different resource types using the octofhir-canonical-manager library.

use octofhir_canonical_manager::{CanonicalManager, FcmConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Load configuration
    let config = FcmConfig::load().unwrap_or_else(|_| {
        // Use default configuration if none exists
        FcmConfig::default()
    });

    let manager = CanonicalManager::new(config).await?;

    // Example 1: Get search parameters for Patient resource type
    println!("🔍 Searching for Patient search parameters...");
    let patient_params = manager.get_search_parameters("Patient").await?;

    if patient_params.is_empty() {
        println!("No search parameters found for Patient resource type.");
        println!("This might happen if:");
        println!("  - No FHIR packages with SearchParameter resources are installed");
        println!("  - The installed packages don't contain SearchParameter definitions");
        println!("Try installing a core FHIR package first:");
        println!("  manager.install_package(\"hl7.fhir.r4.core\", \"4.0.1\").await?;");
    } else {
        println!(
            "Found {} search parameters for Patient:",
            patient_params.len()
        );

        // Display first few parameters
        for (i, param) in patient_params.iter().take(5).enumerate() {
            println!(
                "  {}. {}: {} ({})",
                i + 1,
                param.code,
                param.name,
                param.type_field
            );
            if let Some(description) = &param.description {
                println!("     Description: {description}");
            }
            if let Some(expression) = &param.expression {
                println!("     Expression: {expression}");
            }
            println!();
        }

        if patient_params.len() > 5 {
            println!("  ... and {} more", patient_params.len() - 5);
        }
    }

    // Example 2: Get search parameters for different resource types
    let resource_types = vec!["Observation", "Condition", "Medication", "Procedure"];

    println!("\n📊 Search parameter counts by resource type:");
    for resource_type in resource_types {
        let params = manager.get_search_parameters(resource_type).await?;
        println!("  {}: {} parameters", resource_type, params.len());
    }

    // Example 3: Analyze search parameter types
    println!("\n🔬 Analyzing search parameter types for Patient...");
    if !patient_params.is_empty() {
        let mut type_counts = std::collections::HashMap::new();

        for param in &patient_params {
            *type_counts.entry(&param.type_field).or_insert(0) += 1;
        }

        println!("Parameter types distribution:");
        for (param_type, count) in type_counts {
            println!("  {param_type}: {count} parameters");
        }
    }

    Ok(())
}
