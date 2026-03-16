use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clap::Parser;
use tokio::sync::Barrier;

use sqd_worker::controller::worker::{QueryType, Worker};
use sqd_worker::storage::manager::StateManager;

const DATASET: &str = "ethereum-mainnet";
const CHUNK_ID: &str = "0017881390/0017881390-0017882786-32ee9457";
const QUERY: &str = r#"{"type": "evm"}"#;

fn tests_data() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
}

fn setup_workdir(tmp: &std::path::Path) {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
    let encoded = URL_SAFE_NO_PAD.encode(DATASET.as_bytes());
    let chunk_dir = tmp.join(&encoded).join(CHUNK_ID);
    std::fs::create_dir_all(&chunk_dir).unwrap();

    let src = tests_data().join("0017881390/0017881390-0017882786-32ee9457");
    for entry in std::fs::read_dir(&src).unwrap() {
        let entry = entry.unwrap();
        let file_name = entry.file_name();
        std::os::unix::fs::symlink(entry.path(), chunk_dir.join(&file_name)).unwrap();
    }
}

async fn create_worker(workdir: &std::path::Path, parallel_queries: usize) -> Arc<Worker> {
    let workdir = camino::Utf8PathBuf::from_path_buf(workdir.to_path_buf()).unwrap();
    let args = sqd_worker::cli::Args::parse_from([
        "test",
        "--data-dir",
        "/dev/null",
        "--key",
        "/dev/null",
        "--rpc-url",
        "http://localhost",
        "--l1-rpc-url",
        "http://localhost",
    ]);
    let keypair = sqd_network_transport::Keypair::generate_ed25519();
    let peer_id = keypair.public().to_peer_id();
    Arc::new(
        StateManager::new(workdir, 1, peer_id, args)
            .await
            .map(|sm| Worker::new(sm, parallel_queries))
            .unwrap(),
    )
}

struct LoadTestResult {
    total: usize,
    succeeded: usize,
    overloaded: usize,
    other_errors: usize,
    elapsed: Duration,
}

impl std::fmt::Display for LoadTestResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let qps = self.succeeded as f64 / self.elapsed.as_secs_f64();
        let loss_pct = if self.total > 0 {
            self.overloaded as f64 / self.total as f64 * 100.0
        } else {
            0.0
        };
        write!(
            f,
            "total={:<5} ok={:<5} overloaded={:<5} errors={:<3} elapsed={:>8.2?} success_qps={:>8.0} loss={:>5.1}%",
            self.total, self.succeeded, self.overloaded, self.other_errors,
            self.elapsed, qps, loss_pct,
        )
    }
}

/// Spawn `n` queries that all start simultaneously via a barrier,
/// testing how many the worker can accept at once.
async fn burst_test(worker: &Arc<Worker>, n: usize) -> LoadTestResult {
    let succeeded = Arc::new(AtomicUsize::new(0));
    let overloaded = Arc::new(AtomicUsize::new(0));
    let other_errors = Arc::new(AtomicUsize::new(0));
    let barrier = Arc::new(Barrier::new(n));

    let start = Instant::now();

    let handles: Vec<_> = (0..n)
        .map(|_| {
            let worker = worker.clone();
            let succeeded = succeeded.clone();
            let overloaded = overloaded.clone();
            let other_errors = other_errors.clone();
            let barrier = barrier.clone();
            tokio::spawn(async move {
                barrier.wait().await;
                let result = worker
                    .run_query(
                        QUERY,
                        DATASET.to_string(),
                        Some((17881390, 17882786)),
                        CHUNK_ID,
                        None,
                        QueryType::PlainQuery,
                    )
                    .await;
                match result {
                    Ok(_) => {
                        succeeded.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(sqd_worker::query::result::QueryError::ServiceOverloaded) => {
                        overloaded.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        other_errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect();

    futures::future::join_all(handles).await;
    let elapsed = start.elapsed();

    LoadTestResult {
        total: n,
        succeeded: succeeded.load(Ordering::Relaxed),
        overloaded: overloaded.load(Ordering::Relaxed),
        other_errors: other_errors.load(Ordering::Relaxed),
        elapsed,
    }
}

/// Sustained load: fire queries continuously from `concurrency` tasks for `duration`.
async fn sustained_test(
    worker: &Arc<Worker>,
    concurrency: usize,
    duration: Duration,
) -> LoadTestResult {
    let succeeded = Arc::new(AtomicUsize::new(0));
    let overloaded = Arc::new(AtomicUsize::new(0));
    let other_errors = Arc::new(AtomicUsize::new(0));
    let total = Arc::new(AtomicUsize::new(0));

    let start = Instant::now();
    let deadline = start + duration;

    let handles: Vec<_> = (0..concurrency)
        .map(|_| {
            let worker = worker.clone();
            let succeeded = succeeded.clone();
            let overloaded = overloaded.clone();
            let other_errors = other_errors.clone();
            let total = total.clone();
            tokio::spawn(async move {
                while Instant::now() < deadline {
                    total.fetch_add(1, Ordering::Relaxed);
                    let result = worker
                        .run_query(
                            QUERY,
                            DATASET.to_string(),
                            Some((17881390, 17882786)),
                            CHUNK_ID,
                            None,
                            QueryType::PlainQuery,
                        )
                        .await;
                    match result {
                        Ok(_) => {
                            succeeded.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(sqd_worker::query::result::QueryError::ServiceOverloaded) => {
                            overloaded.fetch_add(1, Ordering::Relaxed);
                        }
                        Err(_) => {
                            other_errors.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            })
        })
        .collect();

    futures::future::join_all(handles).await;
    let elapsed = start.elapsed();

    LoadTestResult {
        total: total.load(Ordering::Relaxed),
        succeeded: succeeded.load(Ordering::Relaxed),
        overloaded: overloaded.load(Ordering::Relaxed),
        other_errors: other_errors.load(Ordering::Relaxed),
        elapsed,
    }
}

fn main() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let tmp = tempfile::tempdir().unwrap();
    setup_workdir(tmp.path());

    println!("=== Query Throughput & Loss Benchmark ===\n");
    println!("  Query: full chunk scan (blocks 17881390-17882786)\n");

    // --- Burst test: all queries hit fetch_add simultaneously via barrier ---
    println!("== Burst Test (barrier-synchronized, all queries start at once) ==\n");

    for parallel_queries in [1, 5, 10, 20] {
        let worker = rt.block_on(create_worker(tmp.path(), parallel_queries));
        println!("--- Worker capacity: {parallel_queries} parallel queries ---");
        for burst_size in [10, 50, 100, 500] {
            let result = rt.block_on(burst_test(&worker, burst_size));
            println!("  burst={burst_size:>3}: {result}");
        }
        println!();
    }

    // --- Sustained load test ---
    println!("== Sustained Load Test (continuous queries for 3s) ==\n");

    let duration = Duration::from_secs(3);
    for parallel_queries in [1, 5, 10, 20] {
        let worker = rt.block_on(create_worker(tmp.path(), parallel_queries));
        println!("--- Worker capacity: {parallel_queries} parallel queries ---");
        for concurrency in [10, 50, 100, 200] {
            let result = rt.block_on(sustained_test(&worker, concurrency, duration));
            println!("  concurrency={concurrency:>3}: {result}");
        }
        println!();
    }
}
