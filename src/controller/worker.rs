use std::sync::Arc;

use futures::StreamExt;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use sqd_network_transport::PeerId;

use crate::{
    metrics,
    query::{
        self,
        eth::BatchRequest,
        result::{QueryError, QueryOk, QueryResult},
    },
    run_all,
    storage::{
        datasets_index::DatasetsIndex,
        manager::{self, StateManager},
    },
    types::{dataset::Dataset, state::ChunkSet},
    util::UseOnce,
};

lazy_static::lazy_static! {
    static ref PARALLEL_QUERIES: usize = std::env::var("PARALLEL_QUERIES")
        .map(|s| s.parse().expect("Invalid PARALLEL_QUERIES"))
        .unwrap_or(3);
    static ref QUEUED_QUERIES: usize = std::env::var("QUEUED_QUERIES")
        .map(|s| s.parse().expect("Invalid QUEUED_QUERIES"))
        .unwrap_or(15);
}

// Use the maximum value for the uncompressed result. After compression, the result will be smaller.
const RESPONSE_LIMIT: usize = sqd_network_transport::protocol::MAX_QUERY_RESULT_SIZE as usize;

pub struct Worker {
    state_manager: Arc<StateManager>,
    queries_tx: mpsc::Sender<QueryTask>,
    queries_rx: UseOnce<mpsc::Receiver<QueryTask>>,
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
    pub fn new(state_manager: StateManager) -> Self {
        let (queries_tx, queries_rx) = mpsc::channel(*QUEUED_QUERIES);
        Self {
            state_manager: Arc::new(state_manager),
            queries_tx,
            queries_rx: UseOnce::new(queries_rx),
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
        let (resp_tx, resp_rx) = oneshot::channel();
        match self.queries_tx.try_send(QueryTask {
            dataset,
            query_str,
            block_range,
            client_id,
            response_sender: resp_tx,
        }) {
            Err(mpsc::error::TrySendError::Full(_)) => {
                return Err(QueryError::ServiceOverloaded);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                panic!("Query subscriber dropped");
            }
            Ok(_) => {
                metrics::PENDING_QUERIES.inc();
            }
        };
        resp_rx
            .await
            .expect("Query processor didn't produce a result")
    }

    pub async fn run(&self, cancellation_token: CancellationToken) {
        let queries_rx = self.queries_rx.take().unwrap();
        let state_manager = self.state_manager.clone();
        let state_manager_fut = state_manager.run(cancellation_token.child_token());
        let worker_fut = ReceiverStream::new(queries_rx)
            .take_until(cancellation_token.cancelled())
            .for_each_concurrent(*PARALLEL_QUERIES, |query_task| async move {
                metrics::PENDING_QUERIES.dec();
                tracing::debug!(
                    "Running query from {}",
                    query_task
                        .client_id
                        .map(|id| id.to_string())
                        .unwrap_or("{unknown}".to_string())
                );
                let result = self
                    .execute_query(
                        query_task.query_str,
                        query_task.dataset,
                        query_task.block_range,
                        RESPONSE_LIMIT,
                    )
                    .await;
                if query_task.response_sender.send(result).is_err() {
                    tracing::error!("Query result couldn't be sent");
                }
            });
        run_all!(cancellation_token, state_manager_fut, worker_fut);
        tracing::info!("Worker task finished");
    }

    // TODO: process all chunks, not only the first one
    async fn execute_query(
        &self,
        query_str: String,
        dataset: String,
        block_range: Option<(u64, u64)>,
        limit: usize,
    ) -> QueryResult {
        let mut query: BatchRequest = serde_json::from_str(query_str.as_str())
            .map_err(|e| QueryError::BadRequest(format!("Couldn't parse query: {e:?}")))?;
        if let Some((from, to)) = block_range {
            query.from_block = from;
            query.to_block = Some(to);
        }
        let chunks_guard = self
            .state_manager
            .find_chunks(&dataset, (query.from_block as u32).into())?;
        let path = chunks_guard.iter().next().cloned();
        if let Some(path) = path {
            tokio::spawn(async move {
                let start_time = tokio::time::Instant::now();
                let ctx = query::context::prepare_query_context(&path).await.unwrap();
                let result = query::processor::process_query(&ctx, query, limit).await?;
                Ok(QueryOk::new(result, 1, start_time.elapsed()))
            })
            .await
            .unwrap_or_else(|e| {
                Err(QueryError::Other(
                    anyhow::Error::new(e).context("Query processing task panicked"),
                ))
            })
        } else {
            Err(QueryError::NotFound)
        }
    }
}
