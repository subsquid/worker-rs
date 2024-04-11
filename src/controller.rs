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
use tokio::{task::JoinError, time::MissedTickBehavior};
use tokio_util::sync::CancellationToken;
use tracing::{debug, instrument, warn};

const PARALLEL_QUERIES: usize = 4;
const NETWORK_POLLING_INTERVAL: Duration = Duration::from_secs(30);

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
                    cancellation_token.clone(),
                )),
            ),
            (
                "assignment_handler",
                tokio::spawn(Self::handle_assignments_forever(
                    self.state_manager.clone(),
                    self.transport.clone(),
                    cancellation_token.clone(),
                )),
            ),
            (
                "queries_handler",
                tokio::spawn(Self::handle_queries_forever(
                    self.state_manager.clone(),
                    self.transport.clone(),
                    self.allocations_checker.clone(),
                    cancellation_token.clone(),
                )),
            ),
            (
                "transport_process",
                tokio::spawn({
                    let cancellation_token = cancellation_token.clone();
                    async move { transport.run(cancellation_token).await }
                }),
            ),
            (
                "state_manager",
                tokio::spawn({
                    let cancellation_token = cancellation_token.clone();
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
                    let cancellation_token = cancellation_token.clone();
                    let allocations_checker = self.allocations_checker.clone();
                    async move {
                        allocations_checker
                            .run(NETWORK_POLLING_INTERVAL, cancellation_token)
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
        while let Some(ranges) = assignments.next().await {
            let result = tokio::select!(
                result = state_manager.set_desired_ranges(ranges) => result,
                _ = cancellation_token.cancelled() => {
                    warn!("Cancel setting assignment");
                    break;
                }
            );
            if let Err(err) = result {
                warn!("Couldn't schedule assignment: {:?}", err)
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
            .for_each_concurrent(PARALLEL_QUERIES, |query_task| {
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
