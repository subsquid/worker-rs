use crate::{
    query::{self, error::QueryError, eth::BatchRequest, processor::QueryResult},
    storage::manager::StateManager,
    transport::Transport,
    types::state::Dataset,
};
use futures::{self, StreamExt};
use std::{sync::Arc, time::Duration};
use tokio::task::JoinError;
use tracing::{error, instrument, warn};

const PARALLEL_QUERIES: usize = 4;

pub struct Worker<T: Transport> {
    state_manager: Arc<StateManager>,
    transport: Arc<T>,
}

impl<T: Transport + 'static> Worker<T> {
    pub fn new(state_manager: Arc<StateManager>, transport: Arc<T>) -> Self {
        Self {
            state_manager,
            transport,
        }
    }

    pub async fn run(&self, ping_interval: Duration) -> Result<(), JoinError> {
        let transport = self.transport.clone();
        tokio::select!(
            result = tokio::spawn(Self::ping_forever(self.state_manager.clone(), self.transport.clone(), ping_interval)) => {
                error!("Ping process exited");
                result
            },
            result = tokio::spawn(Self::handle_assignments_forever(self.state_manager.clone(), self.transport.clone())) => {
                error!("Assignment handling process exited");
                result
            },
            result = tokio::spawn(Self::handle_queries_forever(self.state_manager.clone(), self.transport.clone())) => {
                error!("Queries handling process exited");
                result
            },
            result = tokio::spawn(async move { transport.run().await }) => {
                error!("Transport process exited");
                result
            },
        )
    }

    #[instrument(skip_all)]
    async fn ping_forever(state_manager: Arc<StateManager>, transport: Arc<T>, interval: Duration) {
        loop {
            let status = state_manager.current_status().await;
            let result = transport
                .send_ping(crate::transport::State {
                    datasets: status.available,
                })
                .await;
            if let Err(err) = result {
                warn!("Couldn't send ping: {:?}", err);
            }
            tokio::time::sleep(interval).await;
        }
    }

    #[instrument(skip_all)]
    async fn handle_assignments_forever(state_manager: Arc<StateManager>, transport: Arc<T>) {
        let assignments = transport.stream_assignments();
        tokio::pin!(assignments);
        while let Some(ranges) = assignments.next().await {
            let result = state_manager.set_desired_ranges(ranges).await;
            if let Err(err) = result {
                warn!("Couldn't schedule update: {:?}", err)
            }
        }
    }

    #[instrument(skip_all)]
    async fn handle_queries_forever(state_manager: Arc<StateManager>, transport: Arc<T>) {
        let queries = transport.stream_queries();
        queries
            .for_each_concurrent(PARALLEL_QUERIES, |query_task| {
                let state_manager = state_manager.clone();
                async move {
                    let result = tokio::task::spawn(run_query(
                        state_manager,
                        query_task.query,
                        query_task.dataset,
                    ))
                    .await
                    .unwrap_or_else(|e| {
                        Err(QueryError::Other(anyhow::Error::new(e)))
                    });
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
    let path = state_manager
        .find_chunk(&dataset, (query.from_block as u32).into())
        .await;
    if let Some(path) = path {
        let ctx = query::context::prepare_query_context(&path).await.unwrap();
        let result = query::processor::process_query(&ctx, query).await;
        result.map_err(From::from)
    } else {
        Err(QueryError::NotFound)
    }
}
