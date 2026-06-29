use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD as base64, Engine};
use polars::{
    io::SerWriter,
    prelude::{JsonFormat, JsonWriter},
};
use prost::Message;
use sqd_assignments::Assignment;
use sqd_query::ParquetChunk;
use sql_query_plan::plan;
use substrait::proto::Plan;
use tokio_util::sync::CancellationToken;

use sqd_network_transport::{Keypair, PeerId};
use tracing::instrument;

use crate::{
    controller::{polars_target, sql_request::WorkerChunkStore},
    metrics,
    query::result::{QueryError, QueryOk, QueryResult},
    storage::manager::{self, StateManager},
    types::dataset::Dataset,
};

// Use the maximum value for the uncompressed result. After compression, the result will be smaller.
const RESPONSE_LIMIT: usize = sqd_network_transport::protocol::MAX_QUERY_RESULT_SIZE as usize;

pub enum QueryType {
    PlainQuery,
    SqlQuery,
}

pub struct Worker {
    state_manager: Arc<StateManager>,
    queries_running: AtomicUsize,
    max_parallel_queries: usize,
}

impl Worker {
    pub fn new(state_manager: StateManager, parallel_queries: usize) -> Self {
        Self {
            state_manager: Arc::new(state_manager),
            queries_running: 0.into(),
            max_parallel_queries: parallel_queries,
        }
    }

    pub fn register_assignment(
        &self,
        assignment: Assignment,
        id: impl Into<String>,
        key: &Keypair,
    ) -> bool {
        self.state_manager.set_assignment(assignment, id, key)
    }

    #[cfg(feature = "mvcc-chunks")]
    pub async fn wait_until_assignment_applied(
        &self,
        assignment_id: &str,
        cancellation_token: CancellationToken,
    ) -> bool {
        self.state_manager
            .wait_until_assignment_applied(assignment_id, cancellation_token)
            .await
    }

    pub fn _stop_downloads(&self) {
        self.state_manager._stop_downloads();
    }

    pub async fn status(&self) -> manager::Status {
        self.state_manager.current_status().await
    }

    pub async fn run_query(
        &self,
        query_str: &str,
        dataset: Dataset,
        block_range: (u64, u64),
        chunk_id: &str,
        client_id: Option<PeerId>,
        query_type: QueryType,
    ) -> QueryResult {
        let before = self.queries_running.fetch_add(1, Ordering::SeqCst);
        metrics::RUNNING_QUERIES.inc();
        let _ = scopeguard::guard((), |_| {
            self.queries_running.fetch_sub(1, Ordering::SeqCst);
            metrics::RUNNING_QUERIES.dec();
        });
        if before >= self.max_parallel_queries {
            return Err(QueryError::ServiceOverloaded);
        }

        tracing::debug!(
            "Running query from {}",
            client_id
                .map(|id| id.to_string())
                .unwrap_or("{unknown}".to_string())
        );
        match query_type {
            QueryType::PlainQuery => {
                self.execute_query(query_str, dataset, block_range, chunk_id)
                    .await
            }
            QueryType::SqlQuery => self.execute_sql_query(query_str, dataset, chunk_id).await,
        }
    }

    pub async fn run(&self, cancellation_token: CancellationToken) {
        self.state_manager.run(cancellation_token).await
    }

    #[instrument(skip_all)]
    async fn execute_query(
        &self,
        query_str: &str,
        dataset: Dataset,
        block_range: (u64, u64),
        chunk_id: &str,
    ) -> QueryResult {
        let mut query = sqd_query::Query::from_json_bytes(query_str.as_bytes())
            .map_err(|e| QueryError::BadRequest(format!("Couldn't parse query: {e:?}")))?;
        let (from_block, to_block) = block_range;

        query.set_first_block(from_block);
        query.set_last_block(Some(to_block));

        let Some(chunk_guard) = self.state_manager.clone().get_chunk(dataset, chunk_id) else {
            return Err(QueryError::NotFound);
        };

        let (tx, rx) = tokio::sync::oneshot::channel();
        sqd_polars::POOL.spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                let data_chunk = ParquetChunk::new(chunk_guard.as_str());
                let parse_timer = std::time::Instant::now();
                let plan = query.compile();
                let parse_duration = parse_timer.elapsed();
                let exec_timer = std::time::Instant::now();
                let data = Vec::with_capacity(1024 * 1024);
                let mut writer = sqd_query::JsonLinesWriter::new(data);
                let blocks = plan.execute(&data_chunk)?;
                let exec_duration = exec_timer.elapsed();
                let serialization_timer = std::time::Instant::now();
                let last_block = if let Some(mut blocks) = blocks {
                    writer.write_blocks(&mut blocks)?;
                    blocks.last_block()
                } else {
                    // No matching rows in this chunk. We used to fall back to
                    // the chunk's last_block (parsed from the chunk_id) so the
                    // portal could see progress. After NET-385 the worker
                    // treats chunk_id as opaque, so we report the query's
                    // upper bound
                    to_block
                };
                let bytes = writer.finish()?;
                let serialization_duration = serialization_timer.elapsed();

                if bytes.len() > RESPONSE_LIMIT {
                    return Err(QueryError::from(anyhow::anyhow!("Response too large")));
                }

                Ok(QueryOk::new(
                    bytes,
                    1,
                    last_block,
                    parse_duration,
                    exec_duration,
                    serialization_duration,
                ))
            }))
            .unwrap_or_else(|panic| {
                let msg = panic
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "unknown panic".to_string());
                Err(QueryError::from(anyhow::anyhow!("Query panicked: {msg}")))
            });
            tx.send(result).unwrap_or_else(|_| {
                tracing::warn!("Query runner didn't wait for the result");
            })
        });
        rx.await.unwrap_or_else(|_| {
            Err(QueryError::from(anyhow::anyhow!(
                "Query processor didn't produce a result"
            )))
        })
    }

    #[instrument(skip_all)]
    async fn execute_sql_query(
        &self,
        query_str: &str,
        dataset: Dataset,
        chunk_id: &str,
    ) -> QueryResult {
        let Ok(query_bytes) = base64.decode(query_str) else {
            return Err(QueryError::BadRequest(format!(
                "Can't decode plan '{query_str}'"
            )));
        };
        let Ok(plan) = Plan::decode(&query_bytes[..]) else {
            return Err(QueryError::BadRequest(format!(
                "Can't query plan '{query_str}'"
            )));
        };

        let Some(chunk_guard) = self
            .state_manager
            .clone()
            .get_chunk(dataset.clone(), chunk_id)
        else {
            return Err(QueryError::NotFound);
        };

        let (tx, rx) = tokio::sync::oneshot::channel();
        let local_chunk_id = chunk_id.to_owned().clone();
        sqd_polars::POOL.spawn(move || {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
                let data_source = WorkerChunkStore {
                    path: chunk_guard.as_str().to_owned(),
                };
                let parse_timer = std::time::Instant::now();
                let (context, target) = plan::transform_plan::<polars_target::PolarsTarget>(&plan)
                    .map_err(|err| anyhow::anyhow!("Transform error: {:?}", err))?;
                let lf = target
                    .compile(&context, &dataset, &[local_chunk_id], &data_source)
                    .map_err(|err| anyhow::anyhow!("Compile error: {:?}", err))?;
                let parse_duration = parse_timer.elapsed();
                let exec_timer = std::time::Instant::now();
                let mut df = match lf {
                    Some(lf) => {
                        tracing::debug!("LF Plan: {:?}", lf.describe_plan());
                        lf.collect()
                            .map_err(|err| anyhow::anyhow!("Planning error: {:?}", err))?
                    }
                    None => {
                        return Err(QueryError::from(anyhow::anyhow!("Planning error: No data")))
                    }
                };
                let exec_duration = exec_timer.elapsed();
                let serialization_timer = std::time::Instant::now();
                let mut buf = std::io::BufWriter::new(Vec::new());
                JsonWriter::new(&mut buf)
                    .with_json_format(JsonFormat::JsonLines)
                    .finish(&mut df)
                    .map_err(|err| anyhow::anyhow!("Serialization error: {:?}", err))?;
                let bytes = buf
                    .into_inner()
                    .map_err(|err| anyhow::anyhow!("Serialization error: {:?}", err))?;
                let serialization_duration = serialization_timer.elapsed();

                if bytes.len() > RESPONSE_LIMIT {
                    return Err(QueryError::from(anyhow::anyhow!("Response too large")));
                }

                Ok(QueryOk::new(
                    bytes,
                    1,
                    0,
                    parse_duration,
                    exec_duration,
                    serialization_duration,
                ))
            }))
            .unwrap_or_else(|panic| {
                let msg = panic
                    .downcast_ref::<&str>()
                    .map(|s| s.to_string())
                    .or_else(|| panic.downcast_ref::<String>().cloned())
                    .unwrap_or_else(|| "unknown panic".to_string());
                Err(QueryError::from(anyhow::anyhow!("Query panicked: {msg}")))
            });
            tx.send(result).unwrap_or_else(|_| {
                tracing::warn!("Query runner didn't wait for the result");
            })
        });
        rx.await.unwrap_or_else(|_| {
            Err(QueryError::from(anyhow::anyhow!(
                "Query processor didn't produce a result"
            )))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn run_in_pool<F>(f: F) -> QueryResult
    where
        F: FnOnce() -> QueryResult + Send + 'static,
    {
        let (tx, rx) = tokio::sync::oneshot::channel();
        sqd_polars::POOL.spawn(move || {
            let result =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)).unwrap_or_else(|panic| {
                    let msg = panic
                        .downcast_ref::<&str>()
                        .map(|s| s.to_string())
                        .or_else(|| panic.downcast_ref::<String>().cloned())
                        .unwrap_or_else(|| "unknown panic".to_string());
                    Err(QueryError::from(anyhow::anyhow!("Query panicked: {msg}")))
                });
            tx.send(result).ok();
        });
        rx.await
            .unwrap_or_else(|_| Err(QueryError::from(anyhow::anyhow!("no result"))))
    }

    #[tokio::test]
    async fn pool_panic_is_caught_with_str_message() {
        let result = run_in_pool(|| panic!("something went wrong")).await;
        let Err(QueryError::Other(msg)) = result else {
            panic!("expected QueryError::Other, got {result:?}");
        };
        assert!(msg.contains("something went wrong"), "got: {msg}");
    }

    #[tokio::test]
    async fn pool_panic_is_caught_with_string_message() {
        let result = run_in_pool(|| panic!("{}", "dynamic message".to_string())).await;
        let Err(QueryError::Other(msg)) = result else {
            panic!("expected QueryError::Other, got {result:?}");
        };
        assert!(msg.contains("dynamic message"), "got: {msg}");
    }

    #[tokio::test]
    async fn pool_panic_is_caught_from_assert() {
        let result = run_in_pool(|| {
            assert!(false, "assert message");
            #[allow(unreachable_code)]
            Ok(unreachable!())
        })
        .await;
        let Err(QueryError::Other(msg)) = result else {
            panic!("expected QueryError::Other, got {result:?}");
        };
        assert!(msg.contains("assert message"), "got: {msg}");
    }
}
