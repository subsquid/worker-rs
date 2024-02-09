pub mod http;
pub mod p2p;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use tokio::sync::oneshot;

use crate::{
    query::eth::BatchRequest,
    types::state::{Dataset, Ranges},
};

#[derive(Serialize, Deserialize)]
pub struct State {
    pub datasets: Ranges,
}

pub struct QueryTask {
    dataset: Dataset,
    query: BatchRequest,
    response_sender: oneshot::Sender<Result<Vec<JsonValue>>>,
}

pub trait Transport: Send {
    fn send_ping(&self, state: State) -> impl futures::Future<Output = Result<()>> + Send;
    fn stream_assignments(&self) -> impl futures::Stream<Item = Ranges> + 'static;
    fn stream_queries(&self) -> impl futures::Stream<Item = QueryTask> + 'static;
}
