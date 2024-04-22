pub mod http;
pub mod p2p;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use subsquid_messages::WorkerAssignment;
use subsquid_network_transport::PeerId;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use crate::{
    query::{error::QueryError, eth::BatchRequest, result::QueryResult},
    types::{dataset::Dataset, state::Ranges},
};

#[derive(Serialize, Deserialize)]
pub struct State {
    pub datasets: Ranges,
    pub stored_bytes: u64,
}

pub struct QueryTask {
    pub dataset: Dataset,
    pub query: BatchRequest,
    pub peer_id: PeerId,
    pub response_sender: oneshot::Sender<std::result::Result<QueryResult, QueryError>>,
}

pub trait Transport: Send + Sync {
    fn send_ping(&self, state: State) -> impl futures::Future<Output = Result<()>> + Send;
    fn stream_assignments(&self) -> impl futures::Stream<Item = WorkerAssignment> + 'static + Send;
    fn stream_queries(&self) -> impl futures::Stream<Item = QueryTask> + 'static + Send;
    fn run(
        &self,
        cancellation_token: CancellationToken,
    ) -> impl futures::Future<Output = ()> + Send {
        cancellation_token.cancelled_owned()
    }
}
