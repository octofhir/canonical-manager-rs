//! FCM CLI binary

use octofhir_canonical_manager::cli;

#[tokio::main]
async fn main() {
    human_panic::setup_panic!();
    // Exit codes:
    // 0 - Success
    // 1 - General error
    // 2 - Invalid arguments/usage error
    // 3 - Network/connectivity error
    // 4 - Package not found
    // 5 - Configuration error

    if let Err(_e) = cli::run().await {
        // Error handling with proper exit codes is done in cli::run()
        // We just need to ensure we don't print additional messages
        std::process::exit(1);
    }
}
