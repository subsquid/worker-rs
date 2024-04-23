use crate::{
    gateway_allocations::{self, allocations_checker::AllocationsChecker},
    query::{self, error::QueryError, eth::BatchRequest, result::QueryResult},
    storage::manager::StateManager,
    transport::Transport,
    types::dataset::Dataset,
};
use futures::{self, StreamExt};
use itertools::Itertools;
use std::{sync::Arc, time::Duration};
use subsquid_messages::{DatasetChunks, WorkerAssignment};
use tokio::{task::JoinError, time::MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument, warn};
use crate::types::state::{ChunkRef, ChunkSet};

lazy_static::lazy_static! {
    static ref PARALLEL_QUERIES: usize = std::env::var("PARALLEL_QUERIES")
        .map(|s| s.parse().expect("Invalid PARALLEL_QUERIES"))
        .unwrap_or(4);

    static ref NETWORK_POLLING_INTERVAL: Duration = Duration::from_secs(
        std::env::var("NETWORK_POLLING_INTERVAL_SECS")
            .map(|s| s.parse().expect("Invalid NETWORK_POLLING_INTERVAL_SECS"))
            .unwrap_or(30)
    );
}

pub struct Worker<T: Transport> {
    state_manager: Arc<StateManager>,
    transport: Arc<T>,
    allocations_checker: Arc<dyn AllocationsChecker>,
}

impl<T: Transport + 'static> Worker<T> {
    pub fn new(
        state_manager: Arc<StateManager>,
        transport: Arc<T>,
        allocations_checker: Arc<dyn AllocationsChecker>,
    ) -> Self {
        Self {
            state_manager,
            transport,
            allocations_checker,
        }
    }

    pub async fn run(
        &self,
        ping_interval: Duration,
        cancellation_token: CancellationToken,
        concurrent_downloads: usize,
    ) -> Result<(), JoinError> {
        let transport = self.transport.clone();
        let state_manager = self.state_manager.clone();
        let tasks = [
            (
                "ping_process",
                tokio::spawn(Self::ping_forever(
                    self.state_manager.clone(),
                    self.transport.clone(),
                    ping_interval,
                    cancellation_token.child_token(),
                )),
            ),
            (
                "assignment_handler",
                tokio::spawn(Self::handle_assignments_forever(
                    self.state_manager.clone(),
                    self.transport.clone(),
                    cancellation_token.child_token(),
                )),
            ),
            (
                "queries_handler",
                tokio::spawn(Self::handle_queries_forever(
                    self.state_manager.clone(),
                    self.transport.clone(),
                    self.allocations_checker.clone(),
                    cancellation_token.child_token(),
                )),
            ),
            (
                "transport_process",
                tokio::spawn({
                    let cancellation_token = cancellation_token.child_token();
                    async move { transport.run(cancellation_token).await }
                }),
            ),
            (
                "state_manager",
                tokio::spawn({
                    let cancellation_token = cancellation_token.child_token();
                    async move {
                        state_manager
                            .run(cancellation_token, concurrent_downloads)
                            .await
                    }
                }),
            ),
            (
                "allocations_updater",
                tokio::spawn({
                    let cancellation_token = cancellation_token.child_token();
                    let allocations_checker = self.allocations_checker.clone();
                    async move {
                        allocations_checker
                            .run(*NETWORK_POLLING_INTERVAL, cancellation_token)
                            .await
                    }
                }),
            ),
        ];
        let futures = tasks.map(|(name, task)| {
            let cancellation_token = cancellation_token.clone();
            async move {
                let result = task.await;
                debug!("Task '{name}' exited");
                // Try to shut down the whole process
                cancellation_token.cancel();
                result
            }
        });
        // Not using try_join_all because the tasks are not cancel-safe and should be waited until completion
        futures::future::join_all(futures)
            .await
            .into_iter()
            .try_collect()?;
        Ok(())
    }

    #[instrument(skip_all)]
    async fn ping_forever(
        state_manager: Arc<StateManager>,
        transport: Arc<T>,
        interval: Duration,
        cancellation_token: CancellationToken,
    ) {
        let mut timer = tokio::time::interval(interval);
        timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select!(
                _ = timer.tick() => {},
                _ = cancellation_token.cancelled() => {
                    // TODO: send pause request
                    break;
                },
            );
            debug!("Sending ping");
            let status = state_manager.current_status();
            let result = transport
                .send_ping(crate::transport::State {
                    datasets: status.available,
                    stored_bytes: status.stored_bytes,
                })
                .await;
            if let Err(err) = result {
                warn!("Couldn't send ping: {:?}", err);
            }
        }
    }

    #[instrument(skip_all)]
    async fn handle_assignments_forever(
        state_manager: Arc<StateManager>,
        transport: Arc<T>,
        cancellation_token: CancellationToken,
    ) {
        let assignments = transport
            .stream_assignments()
            .take_until(cancellation_token.cancelled());
        tokio::pin!(assignments);
        while let Some(assignment) = assignments.next().await {
            match assignment_to_chunk_set(assignment) {
                Ok(chunks) => state_manager.set_desired_chunks(chunks),
                Err(e) => warn!("Invalid assignment: {e:?}"),
            }
        }
    }

    #[instrument(skip_all)]
    async fn handle_queries_forever(
        state_manager: Arc<StateManager>,
        transport: Arc<T>,
        allocations_checker: Arc<dyn AllocationsChecker>,
        cancellation_token: CancellationToken,
    ) {
        let queries = transport.stream_queries();
        queries
            .take_until(cancellation_token.cancelled_owned())
            .for_each_concurrent(*PARALLEL_QUERIES, |query_task| {
                let state_manager = state_manager.clone();
                let allocations_checker = allocations_checker.clone();
                async move {
                    tracing::debug!("Running query from {}", query_task.peer_id);
                    let result =
                        match allocations_checker.try_spend(query_task.peer_id).await {
                            Ok(gateway_allocations::Status::Spent) => tokio::task::spawn(
                                run_query(state_manager, query_task.query, query_task.dataset),
                            )
                            .await
                            .unwrap_or_else(|e| {
                                Err(QueryError::Other(
                                    anyhow::Error::new(e).context("Query processing task panicked"),
                                ))
                            }),
                            Ok(gateway_allocations::Status::NotEnoughCU) => {
                                Err(QueryError::NoAllocation)
                            }
                            Err(e) => panic!("Couldn't check CU allocations: {e:?}"),
                        };
                    if query_task.response_sender.send(result).is_err() {
                        tracing::error!("Query result couldn't be sent");
                    }
                }
            })
            .await;
    }
}

// TODO: process all chunks, not only the first one
pub async fn run_query(
    state_manager: Arc<StateManager>,
    query: BatchRequest,
    dataset: Dataset,
) -> Result<QueryResult, QueryError> {
    let guard = state_manager.find_chunks(&dataset, (query.from_block as u32).into())?;
    let path = guard.iter().next();
    if let Some(path) = path {
        let ctx = query::context::prepare_query_context(path).await.unwrap();
        let result = query::processor::process_query(&ctx, query).await?;
        Ok(QueryResult::new(result, 1)?)
    } else {
        Err(QueryError::NotFound)
    }
}

#[inline(always)]
fn assignment_to_chunk_set(assignment: WorkerAssignment) -> anyhow::Result<ChunkSet> {
    assignment.dataset_chunks.into_iter().flat_map(|DatasetChunks { dataset_url, chunks }| {
        let dataset = Arc::new(dataset_url);
        chunks.into_iter().map(move |chunk_str| Ok(ChunkRef {
            dataset: dataset.clone(),
            chunk: chunk_str.parse()?,
        }))
    }).collect()
}

#[cfg(test)]
mod tests {
    use crate::storage::layout::DataChunk;
    use super::{Arc, assignment_to_chunk_set, ChunkRef, ChunkSet, DatasetChunks, WorkerAssignment};

    #[test]
    fn test_assignment_to_chunk_set() {
        let dataset_1 = "s3://ethereum-mainnet".to_string();
        let dataset_2 = "s3://moonbeam-evm".to_string();
        let assignment = WorkerAssignment {
            dataset_chunks: vec![DatasetChunks {
                dataset_url: dataset_1.clone(),
                chunks: vec![
                    "0000000000/0000000000-0000697499-f6275b81".to_string(),
                    "0000000000/0000697500-0000962739-4eff9837".to_string(),
                    "0007293060/0007300860-0007308199-6aeb1a56".to_string(),
                ],
            }, DatasetChunks {
                dataset_url: dataset_2.clone(),
                chunks: vec![
                    "0000000000/0000190280-0000192679-0999c6ce".to_string(),
                    "0003510340/0003515280-0003518379-b8613f00".to_string(),
                ],
            }],
        };
        let dataset_1 = Arc::new(dataset_1);
        let dataset_2 = Arc::new(dataset_2);
        let exp_chunk_set: ChunkSet = [
            ChunkRef {
                dataset: dataset_1.clone(),
                chunk: DataChunk {
                    last_block: 697499.into(),
                    first_block: 0.into(),
                    last_hash: "f6275b81".into(),
                    top: 0.into(),
                },
            }, ChunkRef {
                dataset: dataset_1.clone(),
                chunk: DataChunk {
                    last_block: 962739.into(),
                    first_block: 697500.into(),
                    last_hash: "4eff9837".into(),
                    top: 0.into(),
                },
            }, ChunkRef {
                dataset: dataset_1.clone(),
                chunk: DataChunk {
                    last_block: 7308199.into(),
                    first_block: 7300860.into(),
                    last_hash: "6aeb1a56".into(),
                    top: 7293060.into(),
                }
            },
            ChunkRef {
                dataset: dataset_2.clone(),
                chunk: DataChunk {
                    last_block: 192679.into(),
                    first_block: 190280.into(),
                    last_hash: "0999c6ce".into(),
                    top: 0.into(),
                },
            }, ChunkRef {
                dataset: dataset_2.clone(),
                chunk: DataChunk {
                    last_block: 3518379.into(),
                    first_block: 3515280.into(),
                    last_hash: "b8613f00".into(),
                    top: 3510340.into(),
                },
            },
        ].into_iter().collect();

        let chunk_set = assignment_to_chunk_set(assignment).expect("Valid assignment");
        assert_eq!(chunk_set, exp_chunk_set);
    }
}
