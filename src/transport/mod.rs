pub mod http;
pub mod p2p;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::{
    query::{error::QueryError, eth::BatchRequest, processor::QueryResult},
    types::state::{Dataset, Ranges},
};

#[derive(Serialize, Deserialize)]
pub struct State {
    pub datasets: Ranges,
}

pub struct QueryTask {
    pub dataset: Dataset,
    pub query: BatchRequest,
    pub response_sender: oneshot::Sender<std::result::Result<QueryResult, QueryError>>,
}

pub trait Transport: Send + Sync {
    fn send_ping(&self, state: State) -> impl futures::Future<Output = Result<()>> + Send;
    fn stream_assignments(&self) -> impl futures::Stream<Item = Ranges> + 'static + Send;
    fn stream_queries(&self) -> impl futures::Stream<Item = QueryTask> + 'static + Send;
    fn run(&self) -> impl futures::Future<Output = ()> + Send {
        futures::future::pending()
    }
}
