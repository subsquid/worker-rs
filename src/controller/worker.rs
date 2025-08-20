use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use sqd_assignments::Assignment;
use sqd_query::ParquetChunk;
use tokio_util::sync::CancellationToken;

use sqd_network_transport::{Keypair, PeerId};

use crate::{
    metrics,
    query::result::{QueryError, QueryOk, QueryResult},
    storage::{
        layout::DataChunk,
        manager::{self, StateManager},
    },
    types::dataset::Dataset,
};

// Use the maximum value for the uncompressed result. After compression, the result will be smaller.
const RESPONSE_LIMIT: usize = sqd_network_transport::protocol::MAX_QUERY_RESULT_SIZE as usize;

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

    pub fn _stop_downloads(&self) {
        self.state_manager._stop_downloads();
    }

    pub fn status(&self) -> manager::Status {
        self.state_manager.current_status()
    }

    pub async fn run_query(
        &self,
        query_str: &str,
        dataset: Dataset,
        block_range: Option<(u64, u64)>,
        chunk_id: &str,
        client_id: Option<PeerId>,
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
        self.execute_query(query_str, dataset, block_range, chunk_id)
            .await
    }

    pub async fn run(&self, cancellation_token: CancellationToken) {
        self.state_manager.run(cancellation_token).await
    }

    async fn execute_query(
        &self,
        query_str: &str,
        dataset: Dataset,
        block_range: Option<(u64, u64)>,
        chunk_id: &str,
    ) -> QueryResult {
        let Ok(chunk) = chunk_id.parse::<DataChunk>() else {
            return Err(QueryError::BadRequest(format!(
                "Can't parse chunk id '{chunk_id}'"
            )));
        };
        let mut query = sqd_query::Query::from_json_bytes(query_str.as_bytes())
            .map_err(|e| QueryError::BadRequest(format!("Couldn't parse query: {e:?}")))?;
        if let Some((from_block, to_block)) = block_range {
            query.set_first_block(from_block);
            query.set_last_block(Some(to_block));
        }

        let Some(chunk_guard) = self.state_manager.clone().get_chunk(dataset, chunk.clone()) else {
            return Err(QueryError::NotFound);
        };

        let (tx, rx) = tokio::sync::oneshot::channel();
        sqd_polars::POOL.spawn(move || {
            let result = (move || {
                let start_time = std::time::Instant::now();

                let data_chunk = ParquetChunk::new(chunk_guard.as_str());
                let plan = query.compile();
                let data = Vec::with_capacity(1024 * 1024);
                let mut writer = sqd_query::JsonLinesWriter::new(data);
                let blocks = plan.execute(&data_chunk)?;
                let last_block = if let Some(mut blocks) = blocks {
                    writer.write_blocks(&mut blocks)?;
                    blocks.last_block()
                } else {
                    if let Some(last_query_block) = query.last_block() {
                        std::cmp::min(last_query_block, chunk.last_block.into())
                    } else {
                        chunk.last_block.into()
                    }
                };
                let bytes = writer.finish()?;

                if bytes.len() > RESPONSE_LIMIT {
                    return Err(QueryError::Other(anyhow::anyhow!("Response too large")));
                }

                Ok(QueryOk::new(bytes, 1, last_block, start_time.elapsed()))
            })();
            tx.send(result).unwrap_or_else(|_| {
                tracing::warn!("Query runner didn't wait for the result");
            })
        });
        rx.await.unwrap_or_else(|_| {
            Err(QueryError::Other(anyhow::anyhow!(
                "Query processor didn't produce a result"
            )))
        })
    }
}
