use std::sync::Arc;

use futures::{Future, StreamExt};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use subsquid_network_transport::PeerId;

use crate::{
    gateway_allocations::{self, allocations_checker::AllocationsChecker},
    metrics,
    query::result::{QueryError, QueryOk, QueryResult},
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

pub struct Worker<A: AllocationsChecker> {
    state_manager: Arc<StateManager>,
    // TODO: move allocation checking to the controller
    allocations_checker: A,
    queries_tx: mpsc::Sender<QueryTask>,
    queries_rx: UseOnce<mpsc::Receiver<QueryTask>>,
    pub peer_id: Option<PeerId>,
}

pub struct QueryTask {
    pub dataset: Dataset,
    pub query_str: String,
    pub client_id: Option<PeerId>,
    pub response_sender: oneshot::Sender<QueryResult>,
}

impl<A: AllocationsChecker> Worker<A> {
    pub fn new(state_manager: StateManager, allocations_checker: A) -> Self {
        let (queries_tx, queries_rx) = mpsc::channel(*QUEUED_QUERIES);
        Self {
            state_manager: Arc::new(state_manager),
            allocations_checker,
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

    pub fn schedule_query(
        &self,
        query_str: String,
        dataset: Dataset,
        client_id: Option<PeerId>,
    ) -> Option<impl Future<Output = QueryResult> + '_> {
        let (resp_tx, resp_rx) = oneshot::channel();
        match self.queries_tx.try_send(QueryTask {
            dataset,
            query_str,
            client_id,
            response_sender: resp_tx,
        }) {
            Err(mpsc::error::TrySendError::Full(_)) => {
                return None;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                panic!("Query subscriber dropped");
            }
            Ok(_) => {
                metrics::PENDING_QUERIES.inc();
            }
        };
        Some(async move {
            resp_rx
                .await
                .expect("Query processor didn't produce a result")
        })
    }

    pub async fn run(&self, cancellation_token: CancellationToken) {
        let queries_rx = self.queries_rx.take().unwrap();
        let state_manager = self.state_manager.clone();
        let state_manager_fut = state_manager.run(cancellation_token.child_token());
        let worker_fut = ReceiverStream::new(queries_rx)
            .take_until(cancellation_token.cancelled_owned())
            .for_each_concurrent(*PARALLEL_QUERIES, |query_task| async move {
                metrics::PENDING_QUERIES.dec();
                tracing::debug!(
                    "Running query from {}",
                    query_task
                        .client_id
                        .map(|id| id.to_string())
                        .unwrap_or("{unknown}".to_string())
                );
                let result = match self
                    .allocations_checker
                    .try_spend(query_task.client_id)
                    .await
                {
                    Ok(gateway_allocations::Status::Spent) => {
                        self.execute_query(query_task.query_str, query_task.dataset)
                            .await
                    }
                    Ok(gateway_allocations::Status::NotEnoughCU) => Err(QueryError::NoAllocation),
                    Err(e) => panic!("Couldn't check CU allocations: {e:?}"),
                };
                if query_task.response_sender.send(result).is_err() {
                    tracing::error!("Query result couldn't be sent");
                }
            });
        // TODO: cancel all the tasks if one of them finishes
        tokio::join!(state_manager_fut, worker_fut,);
    }

    // TODO: process all chunks, not only the first one
    async fn execute_query(&self, query_str: String, dataset: String) -> QueryResult {
        let query = sqn_query::Query::from_json_bytes(query_str.as_bytes())
            .map_err(|e| QueryError::BadRequest(format!("Couldn't parse query: {e:?}")))?;
        let Some(first_block) = query.first_block() else {
            return Err(QueryError::BadRequest(
                "Query without first_block".to_owned(),
            ));
        };
        let chunks_guard = self
            .state_manager
            .find_chunks(&dataset, (first_block as u32).into())?;
        let Some(path) = chunks_guard.iter().next().cloned() else {
            return Err(QueryError::NotFound);
        };
        tokio::task::spawn_blocking(move || {
            polars_core::POOL.install(move || {
                let plan = query.compile();
                let mut blocks = plan.execute(path.as_str())?;
                let data = Vec::with_capacity(1024 * 1024);
                let mut writer = sqn_query::JsonArrayWriter::new(data);
                writer.write_blocks(&mut blocks)?;
                let bytes = writer.finish()?;
                Ok(QueryOk::new(bytes, 1)?)
            })
        })
        .await
        .unwrap_or_else(|e| {
            Err(QueryError::Other(
                anyhow::Error::new(e).context("Query processing task panicked"),
            ))
        })
    }
}
