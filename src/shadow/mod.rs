//! Shadow traffic for the new query engine (`sqd-query-engine`).
//!
//! A sampled share of legacy-engine queries is answered as usual. When the
//! answer is ready, a background task runs the legacy engine and the new one on
//! the same chunk with the same block range, in random order so that neither
//! always reads a warm page cache. The client never waits for it and never sees
//! its result. A query whose dataset type has no schema for the new engine is
//! not shadowed.
//!
//! A difference in values, an error only one engine raises, and a run where the
//! new engine is much slower are logged with the query that reproduces them and
//! counted in metrics.
//!
//! Temporary: it goes away when the new engine replaces the legacy one.

mod compare;

use std::{
    collections::HashSet,
    hash::{Hash, Hasher},
    ops::Deref,
    panic::{catch_unwind, AssertUnwindSafe},
    path::Path,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context};
use camino::Utf8PathBuf;
use parking_lot::Mutex;
use sqd_query::{JsonLinesWriter, ParquetChunk};
use sqd_query_engine::{
    metadata::DatasetDescription,
    output::{execute_chunk_with, ExecOptions},
    scan::ParquetChunkReader,
};

use self::compare::Difference;
use crate::metrics::{self, ShadowOutcome};

/// How much of a query a log line carries: a query can list tens of thousands
/// of addresses.
const MAX_LOGGED_QUERY_BYTES: usize = 16 * 1024;
/// The window [`ShadowSettings::log_limit`] counts log lines in.
const LOG_WINDOW: Duration = Duration::from_secs(300);

pub type ShadowRef = Arc<Shadow>;

#[derive(Debug, Clone, Copy)]
pub struct ShadowSettings {
    pub percent: f64,
    /// A run is slow when the new engine takes this many times the legacy time...
    pub slow_ratio: f64,
    /// ...and at least this much longer.
    pub slow_margin: Duration,
    /// Runs at once. A query that finds every slot taken is skipped.
    pub max_in_flight: usize,
    /// Detailed log lines per [`LOG_WINDOW`], so that one hot query cannot
    /// flood the logs; 0 turns them off.
    pub log_limit: usize,
}

pub struct Shadow {
    settings: ShadowSettings,
    /// Runs get their own threads, so that they never queue ahead of a client's
    /// query.
    pool: rayon::ThreadPool,
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
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(settings.max_in_flight.max(1))
            .thread_name(|index| format!("shadow-{index}"))
            // Rayon aborts the process on a panic that escapes a spawned job.
            .panic_handler(|panic| {
                tracing::warn!("shadow run panicked: {}", panic_message(&panic));
            })
            .build()
            .context("failed to start the shadow threads")?;
        metrics::set_shadow_percent(settings.percent);

        Ok(Self {
            settings,
            pool,
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
    /// chunk's directory on disk until the run ends.
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
        self.pool.spawn(move || {
            let run = Run {
                query,
                chunk_dir: chunk.as_str(),
            };
            let outcome = run.execute();
            shadow.record(&run, &outcome);
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

    fn record(&self, run: &Run, outcome: &Outcome) {
        let kind = run.query.kind();
        let label = outcome.label();
        metrics::report_shadow_run(kind, label);

        let slow = match outcome {
            Outcome::Compared { legacy, new, .. } => {
                let slow = self.is_slow(*legacy, *new);
                metrics::report_shadow_timing(kind, *legacy, *new, slow);
                slow
            }
            Outcome::Failed { .. } => false,
        };

        let reason = match label {
            ShadowOutcome::Mismatch | ShadowOutcome::LegacyError | ShadowOutcome::NewError => {
                label.as_str()
            }
            ShadowOutcome::Match if slow => "slow",
            _ => return,
        };
        let fingerprint = run.query.fingerprint();
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
            } => log_difference(run, outcome, difference),
            Outcome::Compared { .. } => log_slow(run, outcome),
            Outcome::Failed { legacy, new } => log_failure(run, legacy, new),
        }
    }

    fn is_slow(&self, legacy: Duration, new: Duration) -> bool {
        let margin = new.saturating_sub(legacy);
        let ratio_exceeded = new.as_secs_f64() > legacy.as_secs_f64() * self.settings.slow_ratio;

        ratio_exceeded && margin >= self.settings.slow_margin
    }
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

struct Run<'c> {
    query: ShadowQuery,
    chunk_dir: &'c str,
}

enum Outcome {
    Compared {
        legacy: Duration,
        new: Duration,
        difference: Option<Difference>,
        legacy_blocks: usize,
        new_blocks: usize,
    },
    /// At least one engine failed; a panic counts as a failure. Both failing is
    /// counted but not logged: the engines agree that the query cannot run.
    Failed {
        legacy: Result<(), String>,
        new: Result<(), String>,
    },
}

impl Outcome {
    fn label(&self) -> ShadowOutcome {
        match self {
            Outcome::Compared {
                difference: None, ..
            } => ShadowOutcome::Match,
            Outcome::Compared { .. } => ShadowOutcome::Mismatch,
            Outcome::Failed {
                legacy: Err(_),
                new: Err(_),
            } => ShadowOutcome::BothError,
            Outcome::Failed { legacy: Err(_), .. } => ShadowOutcome::LegacyError,
            Outcome::Failed { .. } => ShadowOutcome::NewError,
        }
    }
}

struct EngineRun {
    response: Result<Response, String>,
    elapsed: Duration,
}

impl EngineRun {
    fn failed(err: &str) -> Self {
        Self {
            response: Err(err.to_owned()),
            elapsed: Duration::ZERO,
        }
    }
}

#[derive(Default)]
struct Response {
    json_lines: Vec<u8>,
    /// From the engine, not the JSON: a query need not select block numbers.
    last_block: Option<u64>,
}

struct NewPlan {
    plan: sqd_query_engine::query::Plan,
    options: ExecOptions,
}

impl Run<'_> {
    /// Plans are compiled before the clock starts, so only reading and encoding
    /// the chunk is compared.
    fn execute(&self) -> Outcome {
        let legacy_plan = guarded(|| self.legacy_plan());
        let mut new_plan = guarded(|| self.new_plan());

        let legacy = || match &legacy_plan {
            Ok(plan) => timed(|| self.run_legacy(plan)),
            Err(err) => EngineRun::failed(err),
        };
        let new = || match &new_plan {
            Ok(plan) => timed(|| self.run_new(plan)),
            Err(err) => EngineRun::failed(err),
        };
        let (legacy, mut new) = if rand::random() {
            let legacy = legacy();
            (legacy, new())
        } else {
            let new = new();
            (legacy(), new)
        };

        // Each engine cuts an answer at its own weight budget, and the two weigh
        // blocks differently. When they stop at different blocks, the new engine
        // answers again for exactly the legacy blocks, with no budget, so that
        // both answers and both times cover the same work.
        let legacy_end = legacy.response.as_ref().ok().and_then(|l| l.last_block);
        let new_end = new.response.as_ref().ok().map(|n| n.last_block);
        if let (Some(legacy_end), Some(new_end), Ok(plan)) = (legacy_end, new_end, &mut new_plan) {
            if new_end != Some(legacy_end) {
                plan.plan.to_block = Some(legacy_end);
                plan.options.weight_budget = u64::MAX;
                new = timed(|| self.run_new(plan));
            }
        }

        match (legacy.response, new.response) {
            (Ok(l), Ok(n)) => Outcome::Compared {
                legacy: legacy.elapsed,
                new: new.elapsed,
                difference: compare::first_difference(&l.json_lines, &n.json_lines),
                legacy_blocks: compare::count_lines(&l.json_lines),
                new_blocks: compare::count_lines(&n.json_lines),
            },
            (l, n) => Outcome::Failed {
                legacy: l.map(drop),
                new: n.map(drop),
            },
        }
    }

    fn legacy_plan(&self) -> anyhow::Result<sqd_query::Plan> {
        let (from_block, to_block) = self.query.block_range;
        let mut query = sqd_query::Query::from_json_bytes(self.query.query.as_bytes())?;
        query.set_first_block(from_block);
        query.set_last_block(Some(to_block));
        Ok(query.compile())
    }

    fn new_plan(&self) -> anyhow::Result<NewPlan> {
        let schema = &self.query.schema;
        let (from_block, to_block) = self.query.block_range;
        let mut query = sqd_query_engine::query::parse_query(self.query.query.as_bytes(), schema)
            .map_err(|err| anyhow!("couldn't parse the query: {err:?}"))?;
        query.from_block = from_block;
        query.to_block = Some(to_block);
        let plan = sqd_query_engine::query::compile(&query, schema)
            .map_err(|err| anyhow!("couldn't compile the query: {err:?}"))?;

        Ok(NewPlan {
            plan,
            options: ExecOptions::default(),
        })
    }

    fn run_legacy(&self, plan: &sqd_query::Plan) -> anyhow::Result<Response> {
        let chunk = ParquetChunk::new(self.chunk_dir);
        let Some(mut blocks) = plan.execute(&chunk)? else {
            return Ok(Response::default());
        };
        let last_block = blocks.last_block();

        let mut writer = JsonLinesWriter::new(Vec::new());
        writer.write_blocks(&mut blocks)?;
        Ok(Response {
            json_lines: writer.finish()?,
            last_block: Some(last_block),
        })
    }

    fn run_new(&self, plan: &NewPlan) -> anyhow::Result<Response> {
        let chunk = ParquetChunkReader::open(Path::new(self.chunk_dir))?;
        let output = execute_chunk_with(&plan.plan, &self.query.schema, &chunk, plan.options)?;
        let Some(output) = output else {
            return Ok(Response::default());
        };
        let last_block = output.last_block();

        Ok(Response {
            json_lines: output.into_json_lines(),
            last_block: Some(last_block),
        })
    }
}

// warn, not error: the Sentry layer turns an error event into a Sentry issue.
fn log_difference(run: &Run, outcome: &Outcome, difference: &Difference) {
    let Outcome::Compared {
        legacy,
        new,
        legacy_blocks,
        new_blocks,
        ..
    } = outcome
    else {
        return;
    };
    tracing::warn!(
        dataset = %run.query.dataset,
        chunk = %run.query.chunk_id,
        line = difference.line,
        block = difference.block,
        path = difference.path,
        legacy = difference.legacy,
        new = difference.new,
        legacy_blocks,
        new_blocks,
        legacy_ms = legacy.as_secs_f64() * 1000.0,
        new_ms = new.as_secs_f64() * 1000.0,
        query = run.query.replay_query(),
        query_bytes = run.query.query.len(),
        "shadow engine answered differently"
    );
}

fn log_failure(run: &Run, legacy: &Result<(), String>, new: &Result<(), String>) {
    tracing::warn!(
        dataset = %run.query.dataset,
        chunk = %run.query.chunk_id,
        legacy_error = legacy.as_ref().err(),
        new_error = new.as_ref().err(),
        query = run.query.replay_query(),
        query_bytes = run.query.query.len(),
        "shadow engine failed where the other answered"
    );
}

fn log_slow(run: &Run, outcome: &Outcome) {
    let Outcome::Compared {
        legacy,
        new,
        new_blocks,
        ..
    } = outcome
    else {
        return;
    };
    tracing::warn!(
        dataset = %run.query.dataset,
        chunk = %run.query.chunk_id,
        legacy_ms = legacy.as_secs_f64() * 1000.0,
        new_ms = new.as_secs_f64() * 1000.0,
        ratio = new.as_secs_f64() / legacy.as_secs_f64().max(f64::MIN_POSITIVE),
        blocks = new_blocks,
        query = run.query.replay_query(),
        query_bytes = run.query.query.len(),
        "shadow engine is slower"
    );
}

fn timed(run: impl FnOnce() -> anyhow::Result<Response>) -> EngineRun {
    let started = Instant::now();
    let response = guarded(run);
    EngineRun {
        response,
        elapsed: started.elapsed(),
    }
}

/// An engine's error or panic, as the text a log line carries.
fn guarded<T>(run: impl FnOnce() -> anyhow::Result<T>) -> Result<T, String> {
    match catch_unwind(AssertUnwindSafe(run)) {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(err)) => Err(format!("{err:#}")),
        Err(panic) => Err(format!("panicked: {}", panic_message(&panic))),
    }
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

    use arrow::array::{
        ArrayRef, StringArray, TimestampMillisecondArray, UInt16Array, UInt32Array, UInt64Array,
    };
    use arrow::datatypes::{Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use serde_json::json;
    use tempfile::TempDir;

    use super::*;

    /// A copy of the pinned engine revision's catalog; refresh it when the pin
    /// moves.
    const EVM_CATALOG: &str = include_str!("../../tests/metadata/evm.yaml");
    const FIRST_BLOCK: u64 = 100;
    const LAST_BLOCK: u64 = 104;
    const TOKEN: &str = "0xdac17f958d2ee523a2206206994597c13d831ec7";
    const OTHER: &str = "0x4444444444444444444444444444444444444444";
    const TRANSFER: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

    /// An EVM chunk in the layout the network stores, two transactions with a
    /// log each per block. `data_size` is the weight each log claims, which
    /// need not be its real size.
    fn write_chunk(data_size: u64) -> TempDir {
        let dir = tempfile::tempdir().unwrap();
        let blocks: Vec<u64> = (FIRST_BLOCK..=LAST_BLOCK).collect();
        let hash = |block: u64| format!("0x{block:064x}");

        write_table(
            &dir,
            "blocks",
            vec![
                ("number", u32s(blocks.iter().map(|&b| b as u32))),
                ("hash", strings(blocks.iter().map(|&b| hash(b)))),
                ("parent_hash", strings(blocks.iter().map(|&b| hash(b - 1)))),
                (
                    "timestamp",
                    (Arc::new(TimestampMillisecondArray::from_iter_values(
                        blocks
                            .iter()
                            .map(|&b| (1_700_000_000 + b as i64 * 12) * 1000),
                    )) as ArrayRef)
                        .into(),
                ),
            ],
        );

        let rows: Vec<(u64, u16)> = blocks
            .iter()
            .flat_map(|&b| [(b, 0u16), (b, 1u16)])
            .collect();
        let tx_hash = |&(b, i): &(u64, u16)| format!("0x{:064x}", b * 10 + i as u64);
        let address = |&(_, i): &(u64, u16)| if i == 0 { TOKEN } else { OTHER }.to_owned();

        write_table(
            &dir,
            "transactions",
            vec![
                ("block_number", u32s(rows.iter().map(|&(b, _)| b as u32))),
                ("transaction_index", u16s(rows.iter().map(|&(_, i)| i))),
                ("hash", strings(rows.iter().map(tx_hash))),
                ("from", strings(rows.iter().map(|_| OTHER.to_owned()))),
                ("to", strings(rows.iter().map(address))),
                (
                    "input",
                    strings(rows.iter().map(|_| "0xa9059cbb".to_owned())),
                ),
                ("input_size", u64s(rows.iter().map(|_| 4))),
                (
                    "value",
                    strings(rows.iter().map(|&(b, _)| format!("0x{b:x}"))),
                ),
            ],
        );
        write_table(
            &dir,
            "logs",
            vec![
                ("block_number", u32s(rows.iter().map(|&(b, _)| b as u32))),
                ("log_index", u16s(rows.iter().map(|&(_, i)| i))),
                ("transaction_index", u16s(rows.iter().map(|&(_, i)| i))),
                ("transaction_hash", strings(rows.iter().map(tx_hash))),
                ("address", strings(rows.iter().map(address))),
                (
                    "data",
                    strings(rows.iter().map(|&(b, _)| format!("0x{b:064x}"))),
                ),
                ("data_size", u64s(rows.iter().map(|_| data_size))),
                ("topic0", strings(rows.iter().map(|_| TRANSFER.to_owned()))),
                (
                    "topic1",
                    strings(rows.iter().map(|&(b, _)| format!("0x{:064x}", b + 1))),
                ),
                (
                    "topic2",
                    strings(rows.iter().map(|&(b, _)| format!("0x{:064x}", b + 2))),
                ),
                (
                    "topic3",
                    strings(rows.iter().map(|_| String::new())).nullable(),
                ),
            ],
        );
        dir
    }

    fn write_table(dir: &TempDir, name: &str, columns: Vec<(&str, Column)>) {
        let fields: Vec<Field> = columns
            .iter()
            .map(|(name, column)| Field::new(*name, column.0.data_type().clone(), column.1))
            .collect();
        let arrays = columns.into_iter().map(|(_, column)| column.0).collect();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();

        let file = std::fs::File::create(dir.path().join(format!("{name}.parquet"))).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    /// A column and whether it is nullable.
    struct Column(ArrayRef, bool);

    impl Column {
        /// Empty strings become nulls, as absent topics are stored.
        fn nullable(self) -> Self {
            let strings = self.0.as_any().downcast_ref::<StringArray>().unwrap();
            let values: StringArray = strings
                .iter()
                .map(|value| value.filter(|value| !value.is_empty()))
                .collect();
            Column(Arc::new(values), true)
        }
    }

    impl From<ArrayRef> for Column {
        fn from(array: ArrayRef) -> Self {
            Column(array, false)
        }
    }

    fn u32s(values: impl Iterator<Item = u32>) -> Column {
        (Arc::new(UInt32Array::from_iter_values(values)) as ArrayRef).into()
    }

    fn u16s(values: impl Iterator<Item = u16>) -> Column {
        (Arc::new(UInt16Array::from_iter_values(values)) as ArrayRef).into()
    }

    fn u64s(values: impl Iterator<Item = u64>) -> Column {
        (Arc::new(UInt64Array::from_iter_values(values)) as ArrayRef).into()
    }

    fn strings(values: impl Iterator<Item = String>) -> Column {
        (Arc::new(StringArray::from_iter_values(values)) as ArrayRef).into()
    }

    fn catalog(edit: Option<(&str, &str)>) -> Arc<DatasetDescription> {
        let yaml = match edit {
            Some((from, to)) => {
                assert!(EVM_CATALOG.contains(from), "the catalog has no `{from}`");
                EVM_CATALOG.replace(from, to)
            }
            None => EVM_CATALOG.to_owned(),
        };
        Arc::new(sqd_query_engine::metadata::parse_dataset_description(&yaml).unwrap())
    }

    fn shadow_query(query: &serde_json::Value, schema: Arc<DatasetDescription>) -> ShadowQuery {
        ShadowQuery {
            dataset: Arc::new("s3://ethereum-mainnet".to_owned()),
            chunk_id: Arc::from("0000000100/0000000100-0000000104-00000000"),
            query: query.to_string(),
            block_range: (FIRST_BLOCK, LAST_BLOCK),
            schema,
        }
    }

    fn execute(chunk: &TempDir, query: ShadowQuery) -> Outcome {
        Run {
            query,
            chunk_dir: chunk.path().to_str().unwrap(),
        }
        .execute()
    }

    fn token_logs_query() -> serde_json::Value {
        json!({
            "type": "evm",
            "fields": {
                "block": {"number": true, "hash": true, "timestamp": true},
                "log": {"logIndex": true, "transactionIndex": true, "address": true, "topics": true, "data": true},
                "transaction": {"hash": true, "from": true, "to": true, "value": true}
            },
            "logs": [{"address": [TOKEN], "transaction": true}]
        })
    }

    fn describe(outcome: &Outcome) -> String {
        match outcome {
            Outcome::Compared {
                difference,
                legacy_blocks,
                new_blocks,
                ..
            } => format!("{legacy_blocks} vs {new_blocks} blocks, first difference {difference:?}"),
            Outcome::Failed { legacy, new } => format!("legacy {legacy:?}, new {new:?}"),
        }
    }

    #[test]
    fn the_engines_agree_on_a_chunk_in_the_network_layout() {
        let chunk = write_chunk(64);
        let queries = [
            token_logs_query(),
            json!({
                "type": "evm",
                "fields": {
                    "transaction": {"transactionIndex": true, "hash": true, "from": true, "input": true},
                    "log": {"logIndex": true, "transactionIndex": true, "topics": true}
                },
                "transactions": [{"to": [TOKEN], "logs": true}]
            }),
            json!({
                "type": "evm",
                "includeAllBlocks": true,
                "fields": {"block": {"number": true, "hash": true, "parentHash": true}}
            }),
            json!({
                "type": "evm",
                "fields": {"block": {"number": true}},
                "logs": [{"address": ["0x5555555555555555555555555555555555555555"]}]
            }),
        ];

        for query in queries {
            let outcome = execute(&chunk, shadow_query(&query, catalog(None)));

            assert!(
                matches!(outcome.label(), ShadowOutcome::Match),
                "{query}: {}",
                describe(&outcome)
            );
        }
    }

    #[test]
    fn answers_cut_at_different_blocks_are_compared_over_the_legacy_blocks() {
        // Two logs a block: at this weight the legacy engine fits two blocks in
        // its budget and the new engine, which adds more per row, one.
        let chunk = write_chunk(5 * 1024 * 1024 - 88);
        let query = json!({"type": "evm", "fields": {"log": {"data": true}}, "logs": [{}]});
        let run = Run {
            query: shadow_query(&query, catalog(None)),
            chunk_dir: chunk.path().to_str().unwrap(),
        };
        let legacy_end = run
            .run_legacy(&run.legacy_plan().unwrap())
            .unwrap()
            .last_block;
        let new_end = run.run_new(&run.new_plan().unwrap()).unwrap().last_block;
        assert_eq!(
            (legacy_end, new_end),
            (Some(101), Some(100)),
            "the engines no longer cut this chunk apart"
        );

        let outcome = run.execute();

        let Outcome::Compared {
            difference: None,
            legacy_blocks: 2,
            new_blocks: 2,
            ..
        } = outcome
        else {
            panic!("{}", describe(&outcome));
        };
    }

    #[test]
    fn a_different_answer_is_a_mismatch_at_its_path() {
        let chunk = write_chunk(64);
        let swapped = catalog(Some((
            "columns: [ topic0, topic1, topic2, topic3 ]",
            "columns: [ topic1, topic0, topic2, topic3 ]",
        )));

        let outcome = execute(&chunk, shadow_query(&token_logs_query(), swapped));

        let Outcome::Compared {
            difference: Some(difference),
            ..
        } = &outcome
        else {
            panic!("{}", describe(&outcome));
        };
        assert_eq!(difference.block, Some(FIRST_BLOCK));
        assert_eq!(difference.path, "logs[0].topics[0]");
    }

    #[test]
    fn an_error_only_the_new_engine_raises_is_a_new_error() {
        let chunk = write_chunk(64);
        let without_address_filter = catalog(Some((
            "filters: [ address, topic0, topic1, topic2, topic3 ]",
            "filters: [ topic0, topic1, topic2, topic3 ]",
        )));

        let outcome = execute(
            &chunk,
            shadow_query(&token_logs_query(), without_address_filter),
        );

        assert!(
            matches!(outcome.label(), ShadowOutcome::NewError),
            "{}",
            describe(&outcome)
        );
    }

    fn shadow(max_in_flight: usize) -> ShadowRef {
        let settings = ShadowSettings {
            percent: 100.0,
            slow_ratio: 2.0,
            slow_margin: Duration::from_millis(50),
            max_in_flight,
            log_limit: 20,
        };
        Arc::new(Shadow::new(settings).unwrap())
    }

    /// Keeps a chunk on disk and says when it is released.
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

    #[test]
    fn a_submitted_query_is_counted_and_releases_its_chunk() {
        let shadow = shadow(1);
        let chunk = write_chunk(64);
        let released = Arc::new(AtomicBool::new(false));
        let matched = metrics::shadow_run_count("evm", ShadowOutcome::Match);

        shadow.submit(
            shadow_query(&token_logs_query(), catalog(None)),
            ChunkGuard {
                path: chunk.path().to_str().unwrap().into(),
                _dir: chunk,
                released: released.clone(),
            },
        );

        let deadline = Instant::now() + Duration::from_secs(60);
        while !released.load(Ordering::SeqCst) {
            assert!(Instant::now() < deadline, "the run did not finish");
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(metrics::shadow_run_count("evm", ShadowOutcome::Match) > matched);
    }

    #[test]
    fn a_query_that_finds_every_slot_taken_is_not_run() {
        let shadow = shadow(1);
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
        let schema = catalog(None);
        let mut query = token_logs_query();
        let first = shadow_query(&query, schema.clone()).fingerprint();

        query["fromBlock"] = json!(FIRST_BLOCK);
        query["parentBlockHash"] = json!("0x00");
        let same_shape = shadow_query(&query, schema.clone()).fingerprint();

        query["logs"][0]["address"] = json!([OTHER]);
        let other_shape = shadow_query(&query, schema).fingerprint();

        assert_eq!(first, same_shape);
        assert_ne!(first, other_shape);
    }
}
