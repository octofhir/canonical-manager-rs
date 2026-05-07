//! Compare install throughput with `OptimizationConfig::durable_writes
//! = true` vs `false`. Not a Criterion bench; a one-shot runner that
//! prints elapsed time for a single batch install at each setting so
//! the durability trade-off shows up as a wall-clock number.
//!
//! Run with `cargo bench --bench durable_writes`.

mod common;

use std::time::Instant;

use octofhir_canonical_manager::config::StorageConfig;
use octofhir_canonical_manager::sqlite_storage::SqliteStorage;
use tempfile::TempDir;
use tokio::runtime::Runtime;

const PACKAGES: usize = 30;
const RESOURCES_PER_PACKAGE: usize = 50;

async fn measure(durable: bool) -> std::time::Duration {
    let temp = TempDir::new().unwrap();
    let cfg = StorageConfig {
        cache_dir: temp.path().join("cache"),
        packages_dir: temp.path().join("packages"),
        max_cache_size: "1GB".to_string(),
        connection_pool_size: 4,
        fhir_cache_compat: false,
    };
    let storage = SqliteStorage::new_with_durability(cfg, durable)
        .await
        .unwrap();
    let packages =
        common::build_extracted_packages(RESOURCES_PER_PACKAGE, PACKAGES, temp.path()).await;

    let start = Instant::now();
    storage.add_packages_batch(packages).await.unwrap();
    let elapsed = start.elapsed();
    drop(storage);
    elapsed
}

fn main() {
    let runtime = Runtime::new().expect("tokio runtime");
    runtime.block_on(async {
        // Warm-up run so disk caches and tokio worker startup don't
        // skew the first measurement.
        let _ = measure(true).await;

        let durable = measure(true).await;
        let no_sync = measure(false).await;

        let saved = durable.saturating_sub(no_sync);
        let pct = if durable.as_secs_f64() > 0.0 {
            (saved.as_secs_f64() / durable.as_secs_f64()) * 100.0
        } else {
            0.0
        };

        println!();
        println!(
            "Durable-writes A/B ({PACKAGES} packages × {RESOURCES_PER_PACKAGE} resources, single add_packages_batch)"
        );
        println!("  durable=true  : {durable:?}");
        println!("  durable=false : {no_sync:?}");
        println!("  saved         : {saved:?}  ({pct:.1}%)");
        println!();
        println!("Markdown row:");
        println!(
            "| {PACKAGES} pkg × {RESOURCES_PER_PACKAGE} res | {durable:?} | {no_sync:?} | **{pct:.1}%** |"
        );
    });
}
