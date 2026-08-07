//! Conformance harness (spec/13 §harness architecture), Phase 0 skeleton.
//!
//! ```text
//!  HC-1 scheduler sim ──(network state IB-40, assignment IB-41)──▶ ┌─────────┐
//!  HC-2 origin stub ────(chunk files IB-42; byte ledger)─────────▶ │   SUT   │
//!  HC-8 registry stub ──(epoch, allocations IB-43)───────────────▶ │         │
//!  HC-3 portal driver ──(signed queries IB-10)───────────────────▶ └────┬────┘
//!                 HC-5 structural validators ◀── responses, logs ───────┘
//! ```
//!
//! The SUT is the production subsystems, reached through the production functions. The
//! libp2p transport and the `P2PController` event loops are not in the loop; see `UNCOVERED`.

#![allow(dead_code)] // Capabilities land before the tests that consume them.

use std::sync::Arc;
use std::time::Duration;

use camino::Utf8PathBuf;
use futures::StreamExt;
use sqd_messages::{query_error, Query, QueryExecuted, QueryLogs};
use sqd_network_transport::{protocol, Keypair, PeerId};
use tokio_util::sync::CancellationToken;

use sqd_worker::cli::Args;
use sqd_worker::compute_units::{allocations_checker::AllocationsChecker, RateLimitStatus};
use sqd_worker::controller::assignments;
use sqd_worker::controller::experimental_engine::{run_schemas_refresh_loop, QuerySchemaRegistry};
use sqd_worker::controller::p2p;
use sqd_worker::controller::worker::{OutputFormat, QueryType, Worker};
use sqd_worker::logs_storage::LogsStorage;
use sqd_worker::storage::manager::StateManager;

pub mod corpus;
pub mod origin;
pub mod portal;
pub mod registry;
pub mod scheduler;
pub mod seed;
pub mod stub;
pub mod validators;

use origin::Origin;
use portal::Portal;
use registry::Registry;
use scheduler::{Assignment, AssignmentFault, ChunkPlacement, Scheduler};
use seed::{Seed, SeedReporter};
use stub::HttpStub;

/// What this harness does **not** exercise, so an absent test is never read as a pass.
pub const UNCOVERED: &[&str] = &[
    "libp2p transport: IB-1/2 message limits, stream timeouts, the P-EVENT-QUEUE drop path",
    "P2PController intake: P-Q-QUEUE capacity (RP-1 step 4), reject fan-out (ADR-9), \
     assignment pending-queue and its head-of-line blocking (GAP-9)",
    "HC-4 reference model, HC-6 metric scraper, HC-7 kill/restart, HC-9/10 load and benchmarks",
];

/// How long the harness waits for a convergence condition before failing.
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(30);
const POLL_INTERVAL: Duration = Duration::from_millis(20);

const SCHEMA_MANIFEST_PATH: &str = "/query-schemas.yml";
const SCHEMA_PATH: &str = "/schemas/evm.yaml";

/// The dataset every generated chunk belongs to.
pub const DATASET: &str = "s3://conformance-evm";

/// Outcome of driving one query through the admission and delivery path.
#[allow(clippy::large_enum_variant)] // built once per query; size is irrelevant
pub enum Served {
    /// Rejected at RP-1 steps 1–3 or 5: no compute unit spent, no log record.
    PreAdmission {
        response: sqd_messages::QueryResult,
        reason: query_error::Err,
    },
    /// Admitted: the wire response and the log record that must accompany it (INV-32).
    Admitted {
        response: sqd_messages::QueryResult,
        log: QueryExecuted,
    },
}

impl Served {
    pub fn response(&self) -> &sqd_messages::QueryResult {
        match self {
            Self::PreAdmission { response, .. } | Self::Admitted { response, .. } => response,
        }
    }

    #[track_caller]
    pub fn expect_admitted(&self) -> (&sqd_messages::QueryResult, &QueryExecuted) {
        match self {
            Self::Admitted { response, log } => (response, log),
            Self::PreAdmission { reason, .. } => {
                panic!("expected an admitted query, got pre-admission rejection: {reason:?}")
            }
        }
    }
}

pub struct Harness {
    pub seed: Seed,
    pub scheduler: Scheduler,
    pub origin: Origin,
    pub registry: Registry,
    pub portal: Portal,
    pub worker: Arc<Worker>,
    pub allocations: Arc<AllocationsChecker>,
    pub logs: LogsStorage,
    pub worker_id: PeerId,

    keypair: Keypair,
    schemas: Arc<QuerySchemaRegistry>,
    schema_stub: HttpStub,
    assignment_stream:
        std::pin::Pin<Box<dyn futures::Stream<Item = assignments::AssignmentUpdate>>>,
    assignment_client: reqwest::Client,
    shutdown: CancellationToken,
    // Dropped last: the data dir must outlive every subsystem that writes into it.
    data_dir: tempfile::TempDir,
    _reporter: SeedReporter,
}

/// Options a test may vary. Defaults mirror the shipped configuration (spec/15).
pub struct Config {
    pub parallel_queries: usize,
    pub concurrent_downloads: usize,
    /// Portal allocation for the epoch, in compute units.
    pub compute_units: u64,
    pub download_backoff_max: Duration,
    pub file_timeout: Duration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            parallel_queries: 20,
            concurrent_downloads: 3,
            // Refill interval is epoch_length / CUs; keep it ~1 ms so tests aren't paced by it.
            compute_units: 1_000_000,
            // Shipped defaults are 300 s / 60 s — too long to wait on a provoked failure.
            download_backoff_max: Duration::from_millis(200),
            file_timeout: Duration::from_secs(5),
        }
    }
}

impl Harness {
    pub async fn start() -> Self {
        Self::with_config(Config::default()).await
    }

    pub async fn with_config(config: Config) -> Self {
        let seed = Seed::from_env();
        let reporter = SeedReporter::new(seed);

        let mut identity_rng = seed.stream("worker-identity");
        let keypair = keypair_from(&mut identity_rng);
        let worker_id = keypair.public().to_peer_id();

        let mut portal_rng = seed.stream("portal-identity");
        let portal = Portal::new(&mut portal_rng);

        let scheduler = Scheduler::start(seed.stream("assignment-builder")).await;
        let origin = Origin::start().await;
        let registry = Registry::new(&[portal.peer_id()], config.compute_units);

        let schema_stub = HttpStub::start().await;
        schema_stub.put(
            SCHEMA_MANIFEST_PATH,
            format!("schemas:\n  evm: {SCHEMA_PATH}\n").into_bytes(),
        );
        schema_stub.put(SCHEMA_PATH, corpus::SCHEMA_YAML.as_bytes().to_vec());

        let data_dir = tempfile::tempdir().expect("temp data dir");
        let data_path = Utf8PathBuf::from_path_buf(data_dir.path().to_path_buf())
            .expect("temp dir path is UTF-8");

        let args = build_args(&data_path, &scheduler, &schema_stub, &config);

        let state_manager = StateManager::new(
            data_path.join("worker"),
            config.concurrent_downloads,
            worker_id,
            (&args).into(),
        )
        .await
        .expect("state manager initialises");

        let schemas = Arc::new(QuerySchemaRegistry::default());
        let worker = Arc::new(Worker::new(
            state_manager,
            schemas.clone(),
            config.parallel_queries,
        ));

        let allocations = Arc::new(
            AllocationsChecker::new(
                registry.client(),
                worker_id,
                // Fast enough that a test that advances the epoch doesn't wait on P-EPOCH-POLL.
                Duration::from_millis(20),
            )
            .await
            .expect("registry stub answers the worker-id lookup"),
        );

        let logs = LogsStorage::new(data_path.join("logs.db").as_str())
            .await
            .expect("log store opens");

        let shutdown = CancellationToken::new();
        spawn_subsystems(
            &worker,
            &allocations,
            &schemas,
            schema_stub.url(SCHEMA_MANIFEST_PATH),
            worker_id,
            shutdown.clone(),
        );

        let assignment_stream = Box::pin(assignments::new_assignments_stream(
            scheduler.network_state_url(),
            // The production 60 s poll would dominate every test's wall clock.
            Duration::from_millis(20),
            args.assignment_fetch_timeout,
            Duration::from_millis(200),
            worker_id,
        ));
        let assignment_client =
            assignments::new_reqwest_client(args.assignment_fetch_timeout, worker_id);

        let harness = Self {
            seed,
            scheduler,
            origin,
            registry,
            portal,
            worker,
            allocations,
            logs,
            worker_id,
            keypair,
            schemas,
            schema_stub,
            assignment_stream,
            assignment_client,
            shutdown,
            data_dir,
            _reporter: reporter,
        };
        harness.await_schemas_loaded().await;
        harness.await_metering_ready().await;
        harness
    }

    /// Generates a chunk, hosts it on HC-2, and returns its placement for `publish`.
    pub fn host_chunk(&self, chunk: &corpus::Chunk) -> ChunkPlacement {
        self.host_chunk_for(chunk, true)
    }

    /// Same, but `assigned` decides whether this worker's slice names the chunk (DEF-13). An
    /// unassigned chunk is still in the document and still reachable.
    pub fn host_chunk_for(&self, chunk: &corpus::Chunk, assigned: bool) -> ChunkPlacement {
        self.origin.host(chunk);
        Scheduler::placement(DATASET, &self.origin.dataset_base_url(), chunk, assigned)
    }

    /// Publishes an assignment on HC-1 and drives the worker's real intake path
    /// (IB-40 poll → IB-41 fetch → WP-2 application). Returns what was published.
    pub async fn publish_and_apply(
        &mut self,
        id: &str,
        placements: &[ChunkPlacement],
    ) -> Assignment {
        let assignment = self.publish(id, placements, AssignmentFault::None);
        let applied = self.poll_and_apply().await;
        assert!(applied, "worker rejected assignment {id}");
        assignment
    }

    pub fn publish(
        &mut self,
        id: &str,
        placements: &[ChunkPlacement],
        fault: AssignmentFault,
    ) -> Assignment {
        let worker_id = self.worker_id;
        self.scheduler.publish(id, worker_id, placements, fault)
    }

    /// Waits for the next network-state change, fetches the document and applies it.
    /// Returns what `register_assignment` reported (WP-2: a failed application changes nothing).
    pub async fn poll_and_apply(&mut self) -> bool {
        let update = tokio::time::timeout(CONVERGE_TIMEOUT, self.assignment_stream.next())
            .await
            .expect("HC-1 published an assignment within the convergence timeout")
            .expect("assignment stream is still open");

        let document =
            assignments::fetch_assignment(&update.fb_url_v1, &self.assignment_client).await;
        let document = match document {
            Ok(document) => document,
            Err(e) => panic!("assignment {} could not be fetched: {e:?}", update.id),
        };
        self.worker
            .register_assignment(document, update.id, &self.keypair)
    }

    /// Blocks until every assigned chunk is locally available (LIV-1 convergence).
    pub async fn await_all_chunks_available(&self) {
        self.await_condition("all assigned chunks available", || async {
            let status = self.worker.status().await;
            !status.unavailability_map.is_empty()
                && status.unavailability_map.iter().all(|missing| !missing)
        })
        .await;
    }

    /// Runs a query through the production admission sequence (RP-1) and delivery path,
    /// then stores the log record as `handle_query` does. RP-1's step-4 queue gate lives in
    /// the controller and is not covered; the concurrency gate in `Worker` is.
    pub async fn serve(&self, query: Query) -> Served {
        let peer_id = self.portal.peer_id();

        if let Err(reason) = p2p::validate_query(&query, peer_id, self.worker_id) {
            let response = self.sign_error(&query.query_id, reason.clone(), None);
            return Served::PreAdmission { response, reason };
        }

        let retry_after = match self.allocations.try_spend(peer_id, 1.0) {
            RateLimitStatus::Spent(retry_after) => retry_after,
            RateLimitStatus::NoAllocation => {
                let reason = query_error::Err::TooManyRequests(());
                let response = self.sign_error(&query.query_id, reason.clone(), None);
                return Served::PreAdmission { response, reason };
            }
            RateLimitStatus::Paused(retry_after) => {
                let reason = query_error::Err::TooManyRequests(());
                let response = self.sign_error(&query.query_id, reason.clone(), Some(retry_after));
                return Served::PreAdmission { response, reason };
            }
        };

        let outcome = p2p::execute(
            &*self.worker,
            &*self.allocations,
            peer_id,
            &query,
            query_type_of(&query),
        )
        .await;

        let retry_after = if matches!(
            outcome,
            Err(sqd_worker::query::result::QueryError::ServiceOverloaded)
        ) {
            Some(p2p::DEFAULT_BACKOFF)
        } else {
            retry_after
        };

        let compression = query.compression();
        let (response, logged) = p2p::build_delivery(
            &self.keypair,
            &query.query_id,
            outcome,
            retry_after,
            compression,
        )
        .await;

        let log = p2p::build_log(query, peer_id, logged);
        self.logs
            .save_log(log.clone())
            .await
            .expect("log record is stored");

        Served::Admitted { response, log }
    }

    /// A JSONL dynamic-engine query selecting every block header in the range.
    pub fn all_blocks_query(&self, chunk_id: &str, range: (u64, u64)) -> Query {
        self.portal
            .query(self.worker_id, DATASET, chunk_id, range)
            .body(
                serde_json::json!({
                    "type": "evm",
                    "includeAllBlocks": true,
                    "fields": {"block": {"number": true}}
                })
                .to_string(),
            )
            .sign()
    }

    /// The status report as the worker really builds it (IB-22).
    pub async fn status(&self) -> sqd_messages::WorkerStatus {
        p2p::get_worker_status(&self.worker, self.allocations.current_epoch()).await
    }

    /// Reads a log page the way `handle_logs_request` does — including RP-22's serving lag,
    /// so records younger than P-LOGS-LAG are withheld.
    pub async fn logs_page(&self, cursor: Option<(u64, String)>) -> QueryLogs {
        let to = sqd_worker::util::timestamp_now_ms() - protocol::MAX_TIME_LAG.as_millis() as u64;
        self.logs_page_until(cursor, to).await
    }

    /// Same read with an explicit upper bound, for asserting on records the lag still hides.
    pub async fn logs_page_until(
        &self,
        cursor: Option<(u64, String)>,
        to_timestamp_ms: u64,
    ) -> QueryLogs {
        let (from_ts, from_id) = match cursor {
            Some((ts, id)) => (ts, Some(id)),
            None => (0, None),
        };
        self.logs
            .get_logs(from_ts, to_timestamp_ms, from_id, p2p::MAX_LOGS_SIZE)
            .await
            .expect("log store answers")
    }

    /// The page-size budget the worker serves logs under — RP-22's bound for validators.
    pub fn logs_page_budget() -> usize {
        p2p::MAX_LOGS_SIZE
    }

    pub fn logs_lag() -> Duration {
        protocol::MAX_TIME_LAG
    }

    fn sign_error(
        &self,
        query_id: &str,
        err: query_error::Err,
        retry_after: Option<Duration>,
    ) -> sqd_messages::QueryResult {
        let mut msg = sqd_messages::QueryResult {
            query_id: query_id.to_owned(),
            result: Some(err.into()),
            retry_after_ms: retry_after.map(|d| d.as_millis() as u32),
            signature: Vec::new(),
        };
        msg.sign(&self.keypair).expect("error response signs");
        msg
    }

    /// Waits until the portal's bucket holds a token, stepping over the cold-start window
    /// (LIV-12, GAP-25) so tests about anything else don't race it.
    async fn await_metering_ready(&self) {
        let portal = self.portal.peer_id();
        self.await_condition("metering bucket funded (LIV-12)", || async move {
            // Probe and immediately put the token back, so readiness costs nothing.
            match self.allocations.try_spend(portal, 1.0) {
                RateLimitStatus::Spent(_) => {
                    self.allocations.refund(portal, 1.0);
                    true
                }
                _ => false,
            }
        })
        .await;
    }

    async fn await_schemas_loaded(&self) {
        self.await_condition("query schemas loaded", || async {
            self.schemas.get("evm").is_ok()
        })
        .await;
    }

    /// Polls `condition` until it holds, failing with `what` on timeout.
    pub async fn await_condition<F, Fut>(&self, what: &str, mut condition: F)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = bool>,
    {
        let deadline = tokio::time::Instant::now() + CONVERGE_TIMEOUT;
        loop {
            if condition().await {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "timed out after {CONVERGE_TIMEOUT:?} waiting for: {what} \
                     (seed 0x{:x})",
                    self.seed.value()
                );
            }
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }

    /// The on-disk directory a committed chunk lives in — for comparing against HC-2's ledger.
    pub fn chunk_dir(&self, chunk_id: &str) -> std::path::PathBuf {
        self.data_dir
            .path()
            .join("worker")
            .join(sqd_worker::types::dataset::encode_dataset(DATASET))
            .join(chunk_id)
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

fn spawn_subsystems(
    worker: &Arc<Worker>,
    allocations: &Arc<AllocationsChecker>,
    schemas: &Arc<QuerySchemaRegistry>,
    manifest_url: String,
    worker_id: PeerId,
    shutdown: CancellationToken,
) {
    let state_worker = worker.clone();
    let state_token = shutdown.clone();
    tokio::spawn(async move { state_worker.run(state_token).await });

    let alloc = allocations.clone();
    let alloc_token = shutdown.clone();
    tokio::spawn(async move { alloc.run(alloc_token).await });

    let registry = schemas.clone();
    tokio::spawn(async move {
        run_schemas_refresh_loop(
            registry,
            manifest_url,
            Duration::from_secs(3600),
            worker_id,
            shutdown,
        )
        .await
    });
}

fn build_args(
    data_dir: &Utf8PathBuf,
    scheduler: &Scheduler,
    schema_stub: &HttpStub,
    config: &Config,
) -> Args {
    use clap::Parser;

    // Parsed, not constructed: `TransportArgs`/`RpcArgs` have private fields, and the CLI is
    // how IB-32 says an operator configures this. Transport/RPC settings are inert here.
    let mut args = Args::parse_from([
        "sqd-worker",
        "--data-dir",
        data_dir.as_str(),
        "--key",
        data_dir.join("key").as_str(),
        "--assignment-url",
        &scheduler.network_state_url(),
        "--query-schemas-url",
        &schema_stub.url(SCHEMA_MANIFEST_PATH),
        "--parallel-queries",
        &config.parallel_queries.to_string(),
        "--concurrent-downloads",
        &config.concurrent_downloads.to_string(),
        "--rpc-url",
        "http://127.0.0.1:1/unused",
        "--l1-rpc-url",
        "http://127.0.0.1:1/unused",
    ]);

    // Positional/env-only settings (IB-32) have no flag to pass; set them directly.
    args.s3_timeout = config.file_timeout;
    args.s3_read_timeout = config.file_timeout;
    args.downloads_max_delay = config.download_backoff_max;
    args.assignment_fetch_timeout = Duration::from_secs(10);
    args.assignment_fetch_max_delay = Duration::from_millis(200);
    args.sentry_is_enabled = false;
    args
}

fn query_type_of(query: &Query) -> QueryType {
    match query.query_engine() {
        sqd_messages::QueryEngine::Dynamic => QueryType::ExperimentalQuery {
            output_format: match query.output_format() {
                sqd_messages::OutputFormat::ArrowIpc => OutputFormat::ArrowIpc,
                sqd_messages::OutputFormat::Jsonl => OutputFormat::JsonLines,
            },
        },
        _ => QueryType::PlainQuery,
    }
}

fn keypair_from(rng: &mut seed::SplitMix64) -> Keypair {
    use crypto_box::aead::rand_core::RngCore;
    let mut secret = [0u8; 32];
    rng.fill_bytes(&mut secret);
    Keypair::ed25519_from_bytes(secret).expect("32 bytes is a valid ed25519 key")
}

/// Every declared gap must name identifiers the spec defines, or the lists decay into prose
/// nobody can reconcile against the capability register (spec/13 MG-8). Shape alone is not
/// enough — a renumbered id keeps the shape and stops pointing at anything.
///
/// Lives here, not in one binary: it guards the harness, so it holds wherever it is used.
#[test]
fn declared_gaps_cite_the_spec() {
    let defined = spec_identifiers();
    // An inventory that failed to load would make everything below vacuous.
    assert!(
        defined.len() > 100 && defined.contains("GAP-3") && defined.contains("INV-31"),
        "spec inventory looks wrong — {} identifiers parsed out of spec/",
        defined.len()
    );

    for line in UNCOVERED.iter().chain(validators::MISSING) {
        let cited = cited_ids(line);
        assert!(
            !cited.is_empty(),
            "declared gap names no spec identifier: {line:?}"
        );
        for id in cited {
            assert!(
                defined.contains(&id),
                "declared gap cites {id}, which no spec doc defines: {line:?}"
            );
        }
    }

    // The checker must be able to fail, or the loop above proves nothing.
    assert!(cited_ids("no identifiers here, just prose").is_empty());
    assert!(cited_ids("head-of-line blocking").is_empty());
    // Prefix and number must share a token, else any capitalised word near a digit passes.
    assert!(cited_ids("ABC- 1x").is_empty());
    // A mistyped suffix is not silently truncated to the id it resembles.
    assert!(cited_ids("GAP-3x").is_empty());
    assert_eq!(cited_ids("blocked on GAP-32"), ["GAP-32"]);
    assert!(!defined.contains("NOTREAL-999"));
    assert!(!defined.contains("ABC-1"));
}

/// The `PREFIX-<digits>` tokens in `text`. The digits must run to the end of the token, so a
/// mistyped `GAP-3x` is not read as `GAP-3`. A bare continuation — the `2` of `IB-1/2` — carries
/// no prefix and is not checked.
fn cited_ids(text: &str) -> Vec<String> {
    let mut ids = Vec::new();
    for token in text.split(|c: char| !c.is_ascii_alphanumeric() && c != '-') {
        for (dash, _) in token.match_indices('-') {
            let (prefix, number) = (&token[..dash], &token[dash + 1..]);
            if !prefix.is_empty()
                && prefix.chars().all(|c| c.is_ascii_uppercase())
                && !number.is_empty()
                && number.chars().all(|c| c.is_ascii_digit())
            {
                ids.push(format!("{prefix}-{number}"));
            }
        }
    }
    ids
}

/// Identifiers the suite defines, by the three shapes `spec/tools/check_spec.py` recognises:
/// a bolded `**ID —`, an `ID —` heading, and a leading table cell.
fn spec_identifiers() -> std::collections::HashSet<String> {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("spec");
    let patterns = [
        regex::Regex::new(r"\*\*([A-Z]+)-(\d+)\s+—").unwrap(),
        regex::Regex::new(r"(?m)^#{1,6}\s+([A-Z]+)-(\d+)\s+—").unwrap(),
        regex::Regex::new(r"(?m)^\|\s*([A-Z]+)-(\d+)\b").unwrap(),
    ];

    let mut ids = std::collections::HashSet::new();
    let mut pending = vec![root.clone()];
    while let Some(dir) = pending.pop() {
        let entries = std::fs::read_dir(&dir)
            .unwrap_or_else(|e| panic!("conformance harness cannot read {}: {e}", dir.display()));
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|e| e == "md") {
                let text = std::fs::read_to_string(&path).expect("spec doc is UTF-8");
                for pattern in &patterns {
                    ids.extend(
                        pattern
                            .captures_iter(&text)
                            .map(|c| format!("{}-{}", &c[1], &c[2])),
                    );
                }
            }
        }
    }
    ids
}
