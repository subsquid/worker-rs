//! Shadow traffic for the new query engine (`sqd-query-engine`).
//!
//! A sampled share of legacy-engine queries is answered as usual. When the
//! answer is ready, a runner process runs the legacy engine and the new one on
//! the same chunk with the same block range. The client never waits for it and
//! never sees its result. A query whose dataset type has no schema for the new
//! engine is not shadowed.
//!
//! The runner is the worker binary started with [`RUNNER_ARG`]. A process of
//! its own is the only way to keep a run off the threads that answer clients:
//! the legacy engine's Polars operations run on Polars' pool, of which there is
//! one per process. The runner also has a lower CPU priority, and an engine
//! that aborts takes only the runner down.
//!
//! A difference in values, an error only one engine raises, a runner crash and
//! a run where the new engine is much slower are logged with the query that
//! reproduces them and counted in metrics.
//!
//! Temporary: it goes away when the new engine replaces the legacy one.

mod compare;
mod run;

#[cfg(test)]
#[path = "../../tests/shadow/evm_chunk.rs"]
mod evm_chunk;

use std::{
    collections::HashSet,
    hash::{Hash, Hasher},
    ops::Deref,
    path::PathBuf,
    process::Stdio,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::Context;
use camino::Utf8PathBuf;
use parking_lot::Mutex;
use sqd_query_engine::metadata::DatasetDescription;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

pub use self::run::{serve_runner, RUNNER_ARG};
use self::{
    compare::Difference,
    run::{Outcome, Run},
};
use crate::metrics::{self, ShadowOutcome};

/// How much of a query a log line carries: a query can list tens of thousands
/// of addresses.
const MAX_LOGGED_QUERY_BYTES: usize = 16 * 1024;
/// The window [`ShadowSettings::log_limit`] counts log lines in.
const LOG_WINDOW: Duration = Duration::from_secs(300);

pub type ShadowRef = Arc<Shadow>;

#[derive(Debug, Clone)]
pub struct ShadowSettings {
    pub percent: f64,
    /// A run is slow when the new engine takes this many times the legacy time...
    pub slow_ratio: f64,
    /// ...and at least this much longer.
    pub slow_margin: Duration,
    /// Runs at once, each a runner process. A query that finds every slot
    /// taken is skipped.
    pub max_in_flight: usize,
    /// Threads of each runner; both engines run on them.
    pub threads: usize,
    /// A runner still going after this is killed and the run counted as crashed.
    pub timeout: Duration,
    /// Detailed log lines per [`LOG_WINDOW`], so that one hot query cannot
    /// flood the logs; 0 turns them off.
    pub log_limit: usize,
    pub runner: Runner,
}

/// The command that starts a runner.
#[derive(Debug, Clone)]
pub struct Runner {
    pub program: PathBuf,
    pub args: Vec<String>,
}

impl Runner {
    /// This executable, started with [`RUNNER_ARG`].
    pub fn current_exe() -> std::io::Result<Self> {
        Ok(Self {
            program: std::env::current_exe()?,
            args: vec![RUNNER_ARG.to_owned()],
        })
    }
}

pub struct Shadow {
    settings: ShadowSettings,
    in_flight: AtomicUsize,
    log_window: Mutex<LogWindow>,
}

impl Shadow {
    pub fn new(settings: ShadowSettings) -> anyhow::Result<Self> {
        anyhow::ensure!(
            (0.0..=100.0).contains(&settings.percent),
            "shadow percent must be between 0 and 100, got {}",
            settings.percent
        );
        metrics::set_shadow_percent(settings.percent);

        Ok(Self {
            settings,
            in_flight: AtomicUsize::new(0),
            log_window: Mutex::new(LogWindow {
                started: Instant::now(),
                logged: HashSet::new(),
            }),
        })
    }

    /// Whether the next query is shadowed.
    pub fn sample(&self) -> bool {
        rand::random::<f64>() * 100.0 < self.settings.percent
    }

    /// Compare the engines on `query` in the background. `chunk` keeps the
    /// chunk's directory on disk until the runner is gone. Call it within a
    /// Tokio runtime.
    pub fn submit<G>(self: &Arc<Self>, query: ShadowQuery, chunk: G)
    where
        G: Deref<Target = Utf8PathBuf> + Send + 'static,
    {
        let kind = query.kind().to_owned();
        metrics::report_shadow_query(&kind);

        let Some(slot) = self.try_reserve() else {
            metrics::report_shadow_run(&kind, ShadowOutcome::Skipped);
            return;
        };

        let shadow = self.clone();
        tokio::spawn(async move {
            let outcome = shadow.execute(&query, chunk.as_str()).await;
            shadow.record(&query, &outcome);
            drop(chunk);
            drop(slot);
        });
    }

    fn try_reserve(self: &Arc<Self>) -> Option<InFlight> {
        let previous = self.in_flight.fetch_add(1, Ordering::SeqCst);
        if previous < self.settings.max_in_flight {
            metrics::report_shadow_run_started();
            Some(InFlight(self.clone()))
        } else {
            self.in_flight.fetch_sub(1, Ordering::SeqCst);
            None
        }
    }

    async fn execute(&self, query: &ShadowQuery, chunk_dir: &str) -> Outcome {
        let run = Run {
            chunk_dir: chunk_dir.to_owned(),
            query: query.query.clone(),
            block_range: query.block_range,
            schema: DatasetDescription::clone(&query.schema),
        };

        match self.execute_in_runner(&run).await {
            Ok(outcome) => outcome,
            Err(err) => Outcome::Crashed {
                reason: format!("{err:#}"),
            },
        }
    }

    /// Returns once the runner has exited.
    async fn execute_in_runner(&self, run: &Run) -> anyhow::Result<Outcome> {
        let request = serde_json::to_vec(run)?;
        let runner = &self.settings.runner;
        let mut child = tokio::process::Command::new(&runner.program)
            .args(&runner.args)
            .env("POLARS_MAX_THREADS", self.settings.threads.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .context("couldn't start the runner")?;
        let mut stdin = child.stdin.take().context("no runner stdin")?;
        let mut stdout = child.stdout.take().context("no runner stdout")?;

        let exchange = async {
            // A runner that dies early breaks the pipe; its exit status says more.
            let written = stdin.write_all(&request).await;
            drop(stdin);
            let mut answer = Vec::new();
            stdout.read_to_end(&mut answer).await?;
            let status = child.wait().await?;
            anyhow::Ok((written, status, answer))
        };
        let exchanged = tokio::time::timeout(self.settings.timeout, exchange).await;

        let Ok(exchanged) = exchanged else {
            child.kill().await?;
            anyhow::bail!("killed after {:?}", self.settings.timeout);
        };
        let (written, status, answer) = exchanged?;
        anyhow::ensure!(status.success(), "the runner exited with {status}");
        written.context("couldn't send the run")?;

        read_outcome(&answer)
    }

    fn record(&self, query: &ShadowQuery, outcome: &Outcome) {
        let kind = query.kind();
        let label = outcome.label();
        metrics::report_shadow_run(kind, label);

        let slow = match outcome {
            Outcome::Compared { legacy, new, .. } => {
                let slow = self.is_slow(*legacy, *new);
                metrics::report_shadow_timing(kind, *legacy, *new, slow);
                slow
            }
            Outcome::Failed { .. } | Outcome::Crashed { .. } => false,
        };

        let reason = match label {
            ShadowOutcome::Mismatch
            | ShadowOutcome::LegacyError
            | ShadowOutcome::NewError
            | ShadowOutcome::Crashed => label.as_str(),
            ShadowOutcome::Match if slow => "slow",
            _ => return,
        };
        let fingerprint = query.fingerprint();
        let claimed = self
            .log_window
            .lock()
            .claim(fingerprint, reason, self.settings.log_limit);
        if !claimed {
            return;
        }

        match outcome {
            Outcome::Compared {
                difference: Some(difference),
                ..
            } => log_difference(query, outcome, difference),
            Outcome::Compared { .. } => log_slow(query, outcome),
            Outcome::Failed { legacy, new } => log_failure(query, legacy, new),
            Outcome::Crashed { reason } => log_crash(query, reason),
        }
    }

    fn is_slow(&self, legacy: Duration, new: Duration) -> bool {
        let margin = new.saturating_sub(legacy);
        let ratio_exceeded = new.as_secs_f64() > legacy.as_secs_f64() * self.settings.slow_ratio;

        ratio_exceeded && margin >= self.settings.slow_margin
    }
}

/// An engine may print to stdout too; the outcome is the last line.
fn read_outcome(stdout: &[u8]) -> anyhow::Result<Outcome> {
    let last_line = stdout
        .split(|&byte| byte == b'\n')
        .rev()
        .find(|line| !line.is_empty())
        .context("the runner wrote no outcome")?;

    serde_json::from_slice(last_line).context("couldn't read the runner's outcome")
}

/// A sampled query on one chunk, as the worker served it.
pub struct ShadowQuery {
    pub dataset: Arc<String>,
    pub chunk_id: Arc<str>,
    /// The request as the client sent it; each engine parses it itself.
    pub query: String,
    /// The range from the network message, which overrides the one in the query.
    pub block_range: (u64, u64),
    pub schema: Arc<DatasetDescription>,
}

impl ShadowQuery {
    /// The dataset type; a bounded set, unlike datasets, so it labels metrics.
    fn kind(&self) -> &str {
        &self.schema.name
    }

    /// The query with its block range and parent hash removed: what stays the
    /// same while a client follows the chain.
    fn fingerprint(&self) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        self.dataset.hash(&mut hasher);
        match serde_json::from_str::<serde_json::Value>(&self.query) {
            Ok(serde_json::Value::Object(mut query)) => {
                for key in ["fromBlock", "toBlock", "parentBlockHash"] {
                    query.remove(key);
                }
                serde_json::Value::Object(query)
                    .to_string()
                    .hash(&mut hasher);
            }
            _ => self.query.hash(&mut hasher),
        }
        hasher.finish()
    }

    /// The query with the block range the worker ran it with, which replays the
    /// run against any copy of the chunk.
    fn replay_query(&self) -> String {
        let Ok(serde_json::Value::Object(mut query)) = serde_json::from_str(&self.query) else {
            return truncate_query(self.query.clone());
        };
        let (from_block, to_block) = self.block_range;
        query.insert("fromBlock".into(), from_block.into());
        query.insert("toBlock".into(), to_block.into());
        truncate_query(serde_json::Value::Object(query).to_string())
    }
}

struct InFlight(ShadowRef);

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.in_flight.fetch_sub(1, Ordering::SeqCst);
        metrics::report_shadow_run_finished();
    }
}

// warn, not error: the Sentry layer turns an error event into a Sentry issue.
fn log_difference(query: &ShadowQuery, outcome: &Outcome, difference: &Difference) {
    let Outcome::Compared {
        legacy,
        new,
        legacy_blocks,
        new_blocks,
        aligned_end,
        ..
    } = outcome
    else {
        return;
    };
    tracing::warn!(
        dataset = %query.dataset,
        chunk = %query.chunk_id,
        line = difference.line,
        block = difference.block,
        path = difference.path,
        legacy = difference.legacy,
        new = difference.new,
        legacy_blocks,
        new_blocks,
        aligned_end,
        legacy_ms = legacy.as_secs_f64() * 1000.0,
        new_ms = new.as_secs_f64() * 1000.0,
        query = query.replay_query(),
        query_bytes = query.query.len(),
        "shadow engine answered differently"
    );
}

fn log_failure(query: &ShadowQuery, legacy: &Result<(), String>, new: &Result<(), String>) {
    tracing::warn!(
        dataset = %query.dataset,
        chunk = %query.chunk_id,
        legacy_error = legacy.as_ref().err(),
        new_error = new.as_ref().err(),
        query = query.replay_query(),
        query_bytes = query.query.len(),
        "shadow engine failed where the other answered"
    );
}

fn log_crash(query: &ShadowQuery, reason: &str) {
    tracing::warn!(
        dataset = %query.dataset,
        chunk = %query.chunk_id,
        reason,
        query = query.replay_query(),
        query_bytes = query.query.len(),
        "shadow runner crashed"
    );
}

fn log_slow(query: &ShadowQuery, outcome: &Outcome) {
    let Outcome::Compared {
        legacy,
        new,
        new_blocks,
        aligned_end,
        ..
    } = outcome
    else {
        return;
    };
    tracing::warn!(
        dataset = %query.dataset,
        chunk = %query.chunk_id,
        legacy_ms = legacy.as_secs_f64() * 1000.0,
        new_ms = new.as_secs_f64() * 1000.0,
        ratio = new.as_secs_f64() / legacy.as_secs_f64().max(f64::MIN_POSITIVE),
        blocks = new_blocks,
        aligned_end,
        query = query.replay_query(),
        query_bytes = query.query.len(),
        "shadow engine is slower"
    );
}

fn truncate_query(mut query: String) -> String {
    if query.len() > MAX_LOGGED_QUERY_BYTES {
        let mut cut = MAX_LOGGED_QUERY_BYTES;
        while !query.is_char_boundary(cut) {
            cut -= 1;
        }
        query.truncate(cut);
        query.push_str("…<truncated>");
    }
    query
}

fn panic_message(panic: &Box<dyn std::any::Any + Send>) -> &str {
    panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("non-string panic payload")
}

/// The queries logged in detail in the current [`LOG_WINDOW`].
struct LogWindow {
    started: Instant,
    /// Query fingerprints and reasons.
    logged: HashSet<(u64, &'static str)>,
}

impl LogWindow {
    /// Whether this query may be logged in detail for this reason now.
    fn claim(&mut self, fingerprint: u64, reason: &'static str, limit: usize) -> bool {
        if self.started.elapsed() >= LOG_WINDOW {
            self.started = Instant::now();
            self.logged.clear();
        }
        self.logged.len() < limit && self.logged.insert((fingerprint, reason))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;

    use serde_json::json;
    use tempfile::TempDir;

    use super::evm_chunk::*;
    use super::*;

    fn shadow(runner: Runner, timeout: Duration) -> ShadowRef {
        let settings = ShadowSettings {
            percent: 100.0,
            slow_ratio: 2.0,
            slow_margin: Duration::from_millis(50),
            max_in_flight: 1,
            threads: 2,
            timeout,
            log_limit: 20,
            runner,
        };
        Arc::new(Shadow::new(settings).unwrap())
    }

    /// A runner that runs `script` in a shell.
    fn sh(script: &str) -> Runner {
        Runner {
            program: "/bin/sh".into(),
            args: vec!["-c".to_owned(), script.to_owned()],
        }
    }

    fn shadow_query(query: &serde_json::Value) -> ShadowQuery {
        ShadowQuery {
            dataset: Arc::new("s3://ethereum-mainnet".to_owned()),
            chunk_id: Arc::from("0000000100/0000000100-0000000104-00000000"),
            query: query.to_string(),
            block_range: (FIRST_BLOCK, LAST_BLOCK),
            schema: Arc::new(catalog(None)),
        }
    }

    /// Keeps a chunk on disk and says when it is released.
    struct ChunkGuard {
        path: Utf8PathBuf,
        _dir: TempDir,
        released: Arc<AtomicBool>,
    }

    impl ChunkGuard {
        fn new(dir: TempDir) -> (Self, Arc<AtomicBool>) {
            let released = Arc::new(AtomicBool::new(false));
            let guard = Self {
                path: dir.path().to_str().unwrap().into(),
                _dir: dir,
                released: released.clone(),
            };
            (guard, released)
        }
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

    async fn wait_until(released: &AtomicBool, limit: Duration) {
        let deadline = Instant::now() + limit;
        while !released.load(Ordering::SeqCst) {
            assert!(Instant::now() < deadline, "the run did not finish");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[test]
    fn the_runner_writes_an_outcome_the_worker_reads_past_other_output() {
        let chunk = write_chunk(64);
        let run = Run {
            chunk_dir: chunk.path().to_str().unwrap().to_owned(),
            query: token_logs_query().to_string(),
            block_range: (FIRST_BLOCK, LAST_BLOCK),
            schema: catalog(None),
        };
        let mut stdout = b"an engine's own line\n".to_vec();

        run::serve(&serde_json::to_vec(&run).unwrap(), &mut stdout).unwrap();

        let outcome = read_outcome(&stdout).unwrap();
        assert!(
            matches!(outcome.label(), ShadowOutcome::Match),
            "{outcome:?}"
        );
    }

    #[tokio::test]
    async fn a_runner_that_aborts_is_a_crash() {
        let shadow = shadow(
            sh("cat > /dev/null; kill -ABRT $$"),
            Duration::from_secs(60),
        );

        let outcome = shadow
            .execute(&shadow_query(&token_logs_query()), "/nonexistent")
            .await;

        let Outcome::Crashed { reason } = &outcome else {
            panic!("{outcome:?}");
        };
        assert!(reason.contains("signal"), "{reason}");
    }

    #[tokio::test]
    async fn a_runner_past_the_timeout_is_killed_and_releases_its_chunk() {
        let shadow = shadow(sh("exec sleep 60"), Duration::from_millis(100));
        let (chunk, released) = ChunkGuard::new(tempfile::tempdir().unwrap());
        let crashed = metrics::shadow_run_count("evm", ShadowOutcome::Crashed);

        shadow.submit(shadow_query(&token_logs_query()), chunk);

        wait_until(&released, Duration::from_secs(10)).await;
        assert!(metrics::shadow_run_count("evm", ShadowOutcome::Crashed) > crashed);
        assert_eq!(shadow.in_flight.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn a_query_that_finds_every_slot_taken_is_not_run() {
        let shadow = shadow(sh("exit 0"), Duration::from_secs(60));
        let _taken = shadow.try_reserve().unwrap();

        assert!(shadow.try_reserve().is_none());
        assert_eq!(shadow.in_flight.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn the_log_window_logs_each_query_shape_once_and_at_most_the_limit() {
        let mut window = LogWindow {
            started: Instant::now(),
            logged: HashSet::new(),
        };

        assert!(window.claim(1, "mismatch", 2));
        assert!(!window.claim(1, "mismatch", 2));
        assert!(window.claim(1, "slow", 2));
        assert!(!window.claim(2, "mismatch", 2));
        assert!(!window.claim(3, "mismatch", 0));
    }

    #[test]
    fn the_fingerprint_ignores_the_block_range() {
        let mut query = token_logs_query();
        let first = shadow_query(&query).fingerprint();

        query["fromBlock"] = json!(FIRST_BLOCK);
        query["parentBlockHash"] = json!("0x00");
        let same_shape = shadow_query(&query).fingerprint();

        query["logs"][0]["address"] = json!([OTHER]);
        let other_shape = shadow_query(&query).fingerprint();

        assert_eq!(first, same_shape);
        assert_ne!(first, other_shape);
    }
}
