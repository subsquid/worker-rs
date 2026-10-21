//! One shadow run, in the runner process that executes it.

use std::{
    io::{Read, Write},
    panic::{catch_unwind, AssertUnwindSafe},
    path::Path,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Context};
use serde::{Deserialize, Serialize};
use sqd_query::{JsonLinesWriter, ParquetChunk};
use sqd_query_engine::{
    metadata::DatasetDescription,
    output::{execute_chunk_with, ExecOptions},
    scan::ParquetChunkReader,
};

use super::compare::{self, Difference};
use crate::metrics::ShadowOutcome;

/// The first argument that starts the worker binary as a shadow runner.
pub const RUNNER_ARG: &str = "--shadow-runner";

/// Above the worker's, so that the kernel prefers the threads that answer
/// clients.
const RUNNER_NICENESS: i32 = 10;

/// The runner's entry point: reads a [`Run`] from stdin, executes it and
/// writes the [`Outcome`] to stdout as its last line.
pub fn serve_runner() -> anyhow::Result<()> {
    lower_priority();

    let mut request = Vec::new();
    std::io::stdin().lock().read_to_end(&mut request)?;
    serve(&request, &mut std::io::stdout().lock())
}

pub(super) fn serve(request: &[u8], output: &mut impl Write) -> anyhow::Result<()> {
    let run: Run = serde_json::from_slice(request).context("couldn't parse the run")?;

    // Both engines share this process's Polars pool: the legacy engine's
    // Polars operations run there whatever thread calls them.
    let outcome = sqd_polars::POOL.install(|| run.execute());

    serde_json::to_writer(&mut *output, &outcome)?;
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(())
}

fn lower_priority() {
    // Best effort. Linux keeps a niceness per thread; the pool's threads,
    // started later, inherit this one.
    #[cfg(unix)]
    unsafe {
        libc::setpriority(libc::PRIO_PROCESS, 0, RUNNER_NICENESS);
    }
}

/// What the worker sends a runner: the sampled query and its chunk.
#[derive(Serialize, Deserialize)]
pub(super) struct Run {
    pub chunk_dir: String,
    /// The request as the client sent it; each engine parses it itself.
    pub query: String,
    /// The range from the network message, which overrides the one in the query.
    pub block_range: (u64, u64),
    pub schema: DatasetDescription,
}

#[derive(Debug, Serialize, Deserialize)]
pub(super) enum Outcome {
    Compared {
        legacy: Duration,
        new: Duration,
        difference: Option<Difference>,
        legacy_blocks: usize,
        new_blocks: usize,
        /// Set when the first answers ended at different blocks: both engines
        /// answered again up to this block, and the times are of that run.
        aligned_end: Option<u64>,
    },
    /// At least one engine failed; a panic counts as a failure. Both failing is
    /// counted but not logged: the engines agree that the query cannot run.
    Failed {
        legacy: Result<(), String>,
        new: Result<(), String>,
    },
    /// The runner aborted, was killed at the timeout or wrote no outcome. Only
    /// the worker sets it.
    Crashed { reason: String },
}

impl Outcome {
    pub fn label(&self) -> ShadowOutcome {
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
            Outcome::Crashed { .. } => ShadowOutcome::Crashed,
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

impl Run {
    pub fn execute(&self) -> Outcome {
        let (_, to_block) = self.block_range;
        let (legacy, new) = self.run_both(to_block, ExecOptions::default());

        // Each engine cuts an answer at its own weight budget, and the two weigh
        // blocks differently. When they stop at different blocks, both answer
        // again for exactly the legacy blocks, the new engine with no budget, so
        // that both answers and both times cover the same range.
        let Some(aligned_end) = diverging_end(&legacy, &new) else {
            return compare(legacy, new, None);
        };
        let unlimited = ExecOptions {
            weight_budget: u64::MAX,
            ..ExecOptions::default()
        };
        let (legacy, new) = self.run_both(aligned_end, unlimited);

        compare(legacy, new, Some(aligned_end))
    }

    /// Runs the engines in random order, so that neither always reads a warm
    /// page cache. Plans are compiled before the clock starts, so only reading
    /// and encoding the chunk is compared.
    fn run_both(&self, to_block: u64, new_options: ExecOptions) -> (EngineRun, EngineRun) {
        let legacy_plan = guarded(|| self.legacy_plan(to_block));
        let new_plan = guarded(|| self.new_plan(to_block, new_options));

        let legacy = || match &legacy_plan {
            Ok(plan) => timed(|| self.run_legacy(plan)),
            Err(err) => EngineRun::failed(err),
        };
        let new = || match &new_plan {
            Ok(plan) => timed(|| self.run_new(plan)),
            Err(err) => EngineRun::failed(err),
        };

        if rand::random() {
            let legacy = legacy();
            (legacy, new())
        } else {
            let new = new();
            (legacy(), new)
        }
    }

    fn legacy_plan(&self, to_block: u64) -> anyhow::Result<sqd_query::Plan> {
        let (from_block, _) = self.block_range;
        let mut query = sqd_query::Query::from_json_bytes(self.query.as_bytes())?;
        query.set_first_block(from_block);
        query.set_last_block(Some(to_block));
        Ok(query.compile())
    }

    fn new_plan(&self, to_block: u64, options: ExecOptions) -> anyhow::Result<NewPlan> {
        let (from_block, _) = self.block_range;
        let mut query = sqd_query_engine::query::parse_query(self.query.as_bytes(), &self.schema)
            .map_err(|err| anyhow!("couldn't parse the query: {err:?}"))?;
        query.from_block = from_block;
        query.to_block = Some(to_block);
        let plan = sqd_query_engine::query::compile(&query, &self.schema)
            .map_err(|err| anyhow!("couldn't compile the query: {err:?}"))?;

        Ok(NewPlan { plan, options })
    }

    fn run_legacy(&self, plan: &sqd_query::Plan) -> anyhow::Result<Response> {
        let chunk = ParquetChunk::new(&self.chunk_dir);
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
        let chunk = ParquetChunkReader::open(Path::new(&self.chunk_dir))?;
        let output = execute_chunk_with(&plan.plan, &self.schema, &chunk, plan.options)?;
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

/// The legacy answer's last block, when the new answer ends elsewhere.
fn diverging_end(legacy: &EngineRun, new: &EngineRun) -> Option<u64> {
    let legacy_end = legacy.response.as_ref().ok()?.last_block?;
    let new_end = new.response.as_ref().ok()?.last_block;

    (new_end != Some(legacy_end)).then_some(legacy_end)
}

fn compare(legacy: EngineRun, new: EngineRun, aligned_end: Option<u64>) -> Outcome {
    match (legacy.response, new.response) {
        (Ok(l), Ok(n)) => Outcome::Compared {
            legacy: legacy.elapsed,
            new: new.elapsed,
            difference: compare::first_difference(&l.json_lines, &n.json_lines),
            legacy_blocks: compare::count_lines(&l.json_lines),
            new_blocks: compare::count_lines(&n.json_lines),
            aligned_end,
        },
        (l, n) => Outcome::Failed {
            legacy: l.map(drop),
            new: n.map(drop),
        },
    }
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
        Err(panic) => Err(format!("panicked: {}", super::panic_message(&panic))),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use tempfile::TempDir;

    use super::*;
    use crate::shadow::evm_chunk::*;

    fn run_of(chunk: &TempDir, query: &serde_json::Value, schema: DatasetDescription) -> Run {
        Run {
            chunk_dir: chunk.path().to_str().unwrap().to_owned(),
            query: query.to_string(),
            block_range: (FIRST_BLOCK, LAST_BLOCK),
            schema,
        }
    }

    fn describe(outcome: &Outcome) -> String {
        match outcome {
            Outcome::Compared {
                difference,
                legacy_blocks,
                new_blocks,
                ..
            } => format!("{legacy_blocks} vs {new_blocks} blocks, first difference {difference:?}"),
            other => format!("{other:?}"),
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
            let outcome = run_of(&chunk, &query, catalog(None)).execute();

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
        let run = run_of(&chunk, &query, catalog(None));
        let legacy_end = run
            .run_legacy(&run.legacy_plan(LAST_BLOCK).unwrap())
            .unwrap()
            .last_block;
        let new_end = run
            .run_new(&run.new_plan(LAST_BLOCK, ExecOptions::default()).unwrap())
            .unwrap()
            .last_block;
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
            aligned_end: Some(101),
            ..
        } = outcome
        else {
            panic!("{}", describe(&outcome));
        };
    }

    #[test]
    fn answers_that_end_together_are_not_run_again() {
        let chunk = write_chunk(64);

        let outcome = run_of(&chunk, &token_logs_query(), catalog(None)).execute();

        let Outcome::Compared {
            aligned_end: None, ..
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

        let outcome = run_of(&chunk, &token_logs_query(), swapped).execute();

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

        let outcome = run_of(&chunk, &token_logs_query(), without_address_filter).execute();

        assert!(
            matches!(outcome.label(), ShadowOutcome::NewError),
            "{}",
            describe(&outcome)
        );
    }
}
