//! The shadow comparison through the worker binary in its runner mode.

// One test only: it reads the process-wide metrics.

#[path = "shadow/evm_chunk.rs"]
mod evm_chunk;

use std::{
    ops::Deref,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use camino::Utf8PathBuf;
use prometheus_client::{encoding::text::encode, registry::Registry};
use sqd_worker::{
    metrics,
    shadow::{Runner, Shadow, ShadowQuery, ShadowSettings, RUNNER_ARG},
};
use tempfile::TempDir;

use evm_chunk::*;

struct ChunkGuard {
    path: Utf8PathBuf,
    _dir: TempDir,
    released: Arc<AtomicBool>,
}

impl Deref for ChunkGuard {
    type Target = Utf8PathBuf;

    fn deref(&self) -> &Utf8PathBuf {
        &self.path
    }
}

impl Drop for ChunkGuard {
    fn drop(&mut self) {
        self.released.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn the_worker_binary_runs_a_shadowed_query_in_its_own_process() {
    let mut registry = Registry::default();
    metrics::register_metrics(&mut registry, "test".to_owned());
    let shadow = Arc::new(
        Shadow::new(ShadowSettings {
            percent: 100.0,
            slow_ratio: 2.0,
            slow_margin: Duration::from_millis(50),
            max_in_flight: 1,
            threads: 2,
            timeout: Duration::from_secs(60),
            log_limit: 20,
            runner: Runner {
                program: env!("CARGO_BIN_EXE_sqd-worker").into(),
                args: vec![RUNNER_ARG.to_owned()],
            },
        })
        .unwrap(),
    );
    let chunk = write_chunk(64);
    let released = Arc::new(AtomicBool::new(false));

    // The worker answers clients on this pool. It stays full for the whole
    // run, so the run must not need any of its threads.
    let pool_taken = Arc::new(AtomicBool::new(true));
    for _ in 0..sqd_polars::POOL.current_num_threads() {
        let pool_taken = pool_taken.clone();
        sqd_polars::POOL.spawn(move || {
            while pool_taken.load(Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(5));
            }
        });
    }

    shadow.submit(
        ShadowQuery {
            dataset: Arc::new("s3://ethereum-mainnet".to_owned()),
            chunk_id: Arc::from("0000000100/0000000100-0000000104-00000000"),
            query: token_logs_query().to_string(),
            block_range: (FIRST_BLOCK, LAST_BLOCK),
            schema: Arc::new(catalog(None)),
        },
        ChunkGuard {
            path: chunk.path().to_str().unwrap().into(),
            _dir: chunk,
            released: released.clone(),
        },
    );

    let deadline = Instant::now() + Duration::from_secs(60);
    while !released.load(Ordering::SeqCst) {
        assert!(Instant::now() < deadline, "the run did not finish");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    pool_taken.store(false, Ordering::SeqCst);

    let mut text = String::new();
    encode(&mut text, &registry).unwrap();
    assert!(
        text.contains(r#"shadow_runs_total{kind="evm",outcome="match"} 1"#),
        "{text}"
    );
}
