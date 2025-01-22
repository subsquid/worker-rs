use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use sqd_messages::assignments::Assignment;
use sqd_query::ParquetChunk;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use sqd_network_transport::PeerId;

use crate::{
    metrics,
    query::result::{QueryError, QueryOk, QueryResult},
    storage::manager::{self, StateManager},
    types::dataset::Dataset,
};

// Use the maximum value for the uncompressed result. After compression, the result will be smaller.
const RESPONSE_LIMIT: usize = sqd_network_transport::protocol::MAX_QUERY_RESULT_SIZE as usize;

pub struct Worker {
    state_manager: Arc<StateManager>,
    queries_running: AtomicUsize,
    max_parallel_queries: usize,
    pub peer_id: PeerId,
}

pub struct QueryTask {
    pub dataset: Dataset,
    pub query_str: String,
    pub block_range: Option<(u64, u64)>,
    pub client_id: Option<PeerId>,
    pub response_sender: oneshot::Sender<QueryResult>,
}

impl Worker {
    pub fn new(state_manager: StateManager, parallel_queries: usize, peer_id: PeerId) -> Self {
        Self {
            state_manager: Arc::new(state_manager),
            queries_running: 0.into(),
            max_parallel_queries: parallel_queries,
            peer_id,
        }
    }

    pub fn register_assignment(
        &self,
        assignment: &Assignment,
        peer_id: &PeerId,
        secret_key: &Vec<u8>,
    ) -> bool {
        self.state_manager
            .register_assignment(assignment, peer_id, secret_key)
    }

    pub fn get_assignment_id(&self) -> Option<String> {
        self.state_manager.get_latest_assignment_id()
    }

    pub fn stop_downloads(&self) {
        self.state_manager.stop_downloads();
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
        let Ok(chunk) = chunk_id.parse() else {
            return Err(QueryError::BadRequest(format!(
                "Can't parse chunk id '{chunk_id}'"
            )));
        };
        let mut query = sqd_query::Query::from_json_bytes(query_str.as_bytes())
            .map_err(|e| QueryError::BadRequest(format!("Couldn't parse query: {e:?}")))?;
        if let Some((from_block, to_block)) = block_range {
            query.set_first_block(Some(from_block));
            query.set_last_block(Some(to_block));
        }

        let Some(chunk_guard) = self.state_manager.clone().get_chunk(dataset, chunk) else {
            return Err(QueryError::NotFound);
        };

        let (tx, rx) = tokio::sync::oneshot::channel();
        sqd_polars::POOL.spawn(move || {
            let result = (move || {
                let start_time = std::time::Instant::now();

                let chunk = ParquetChunk::new(chunk_guard.as_str());
                let plan = query.compile();
                let data = Vec::with_capacity(1024 * 1024);
                let mut writer = sqd_query::JsonLinesWriter::new(data);
                let mut blocks = plan.execute(&chunk)?;
                let last_block = blocks.last_block();
                writer.write_blocks(&mut blocks)?;
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
