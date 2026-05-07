//! Parallel-download concurrency sweep.
//!
//! `RegistryClient::download_packages_parallel` hardcodes
//! `buffer_unordered(8)`. Real registries (`packages.fhir.org`,
//! `fs.get-ig.org`) sit behind ~50 ms RTT; we want to know whether 8 is
//! still the right value or the curve flattens earlier / keeps falling
//! at 16 or 32.
//!
//! Strategy: stand up a `wiremock` server with a fixed 50 ms response
//! delay, then download N packages with concurrency K ∈ {4, 8, 16, 32}.
//! Lower-bound expected wall time is `ceil(N / K) × delay` — but in
//! practice TCP setup, body size, and reqwest internals all matter.
//!
//! Run with `cargo bench --bench download_parallelism`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures_util::{StreamExt, stream};
use tokio::runtime::Runtime;
use wiremock::matchers::{method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

const PACKAGES: usize = 32;
const RTT: Duration = Duration::from_millis(50);
const PAYLOAD_BYTES: usize = 32 * 1024;

async fn setup_mock() -> MockServer {
    let server = MockServer::start().await;
    let body = vec![0u8; PAYLOAD_BYTES];
    Mock::given(method("GET"))
        .and(path_regex(r"^/.*\.tgz$"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_bytes(body)
                .set_delay(RTT),
        )
        .mount(&server)
        .await;
    server
}

async fn measure(server: &MockServer, concurrency: usize) -> Duration {
    let client = Arc::new(reqwest::Client::new());
    let urls: Vec<String> = (0..PACKAGES)
        .map(|i| format!("{}/pkg-{i}.tgz", server.uri()))
        .collect();

    let start = Instant::now();
    let _: Vec<_> = stream::iter(urls)
        .map(|url| {
            let client = client.clone();
            async move {
                let bytes = client
                    .get(&url)
                    .send()
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap();
                bytes.len()
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;
    start.elapsed()
}

fn main() {
    let runtime = Runtime::new().expect("tokio runtime");
    runtime.block_on(async {
        let server = setup_mock().await;

        // Warm-up: TLS / connection-pool init costs flow into the first
        // measurement. Discard.
        let _ = measure(&server, 4).await;

        let mut rows: Vec<(usize, Duration)> = Vec::new();
        for k in [4usize, 8, 16, 32] {
            // Two trials per concurrency, take the minimum so a stray
            // GC / scheduler hitch doesn't mark a winner the loser.
            let a = measure(&server, k).await;
            let b = measure(&server, k).await;
            let best = a.min(b);
            rows.push((k, best));
        }

        let lower_bound = RTT * (PACKAGES as u32);
        let baseline = rows.iter().find(|(k, _)| *k == 8).map(|(_, d)| *d);

        println!();
        println!(
            "Download parallelism sweep ({PACKAGES} requests × {RTT:?} RTT, {PAYLOAD_BYTES}-byte body)"
        );
        println!("  ideal lower bound (serial): {lower_bound:?}");
        println!();
        println!("| concurrency | elapsed | vs current default (8) |");
        println!("|---|---|---|");
        for (k, d) in &rows {
            let delta = match baseline {
                Some(b) if *k != 8 => {
                    let saved = b.as_secs_f64() - d.as_secs_f64();
                    let pct = saved / b.as_secs_f64() * 100.0;
                    format!("{pct:+.1}%")
                }
                _ => String::from("baseline"),
            };
            println!("| {k} | {d:?} | {delta} |");
        }
    });
}
