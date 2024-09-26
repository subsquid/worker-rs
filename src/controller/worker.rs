use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use sqd_query::ParquetChunk;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use sqd_network_transport::PeerId;

use crate::{
    metrics,
    query::result::{QueryError, QueryOk, QueryResult},
    storage::{
        datasets_index::DatasetsIndex,
        manager::{self, StateManager},
    },
    types::{dataset::Dataset, state::ChunkSet},
};

// Use the maximum value for the uncompressed result. After compression, the result will be smaller.
const RESPONSE_LIMIT: usize = sqd_network_transport::protocol::MAX_QUERY_RESULT_SIZE as usize;

pub struct Worker {
    state_manager: Arc<StateManager>,
    queries_running: AtomicUsize,
    max_parallel_queries: usize,
    pub peer_id: Option<PeerId>,
}

pub struct QueryTask {
    pub dataset: Dataset,
    pub query_str: String,
    pub block_range: Option<(u64, u64)>,
    pub client_id: Option<PeerId>,
    pub response_sender: oneshot::Sender<QueryResult>,
}

impl Worker {
    pub fn new(state_manager: StateManager, parallel_queries: usize) -> Self {
        Self {
            state_manager: Arc::new(state_manager),
            queries_running: 0.into(),
            max_parallel_queries: parallel_queries,
            peer_id: None,
        }
    }

    pub fn with_peer_id(mut self, peer_id: PeerId) -> Self {
        self.peer_id = Some(peer_id);
        self
    }

    pub fn set_desired_chunks(&self, chunks: ChunkSet) {
        self.state_manager.set_desired_chunks(chunks);
    }

    pub fn set_datasets_index(&self, datasets_index: DatasetsIndex) {
        self.state_manager.set_datasets_index(datasets_index);
    }

    pub fn stop_downloads(&self) {
        self.state_manager.stop_downloads();
    }

    pub fn status(&self) -> manager::Status {
        self.state_manager.current_status()
    }

    pub async fn run_query(
        &self,
        query_str: String,
        dataset: Dataset,
        block_range: Option<(u64, u64)>,
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
        self.execute_query(query_str, dataset, block_range).await
    }

    pub async fn run(&self, cancellation_token: CancellationToken) {
        self.state_manager
            .run(cancellation_token.child_token())
            .await
    }

    async fn execute_query(
        &self,
        query_str: String,
        dataset: String,
        block_range: Option<(u64, u64)>,
    ) -> QueryResult {
        let mut query = sqd_query::Query::from_json_bytes(query_str.as_bytes())
            .map_err(|e| QueryError::BadRequest(format!("Couldn't parse query: {e:?}")))?;
        if let Some((from_block, to_block)) = block_range {
            query.set_first_block(Some(from_block));
            query.set_last_block(Some(to_block));
        }

        // First block may be either set by the `block_range` arg or defined in the query.
        let Some(first_block) = query.first_block() else {
            return Err(QueryError::BadRequest(
                "Query without first_block".to_owned(),
            ));
        };
        let chunk_guard = self
            .state_manager
            .clone()
            .find_chunk(&dataset, (first_block as u32).into())?;
        if chunk_guard.is_none() {
            return Err(QueryError::NotFound);
        };

        let (tx, rx) = tokio::sync::oneshot::channel();
        sqd_polars::POOL.spawn(move || {
            let result = (move || {
                let start_time = std::time::Instant::now();

                let chunk = ParquetChunk::new(chunk_guard.as_ref().unwrap().as_str());
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
