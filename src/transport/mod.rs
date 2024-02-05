pub mod http;

use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::types::state::Ranges;

#[derive(Serialize, Deserialize)]
pub struct State {
    pub datasets: Ranges,
}

pub trait Transport: Send {
    fn send_ping(&self, state: State) -> impl futures::Future<Output=Result<()>> + Send;
    fn subscribe_to_updates(&mut self) -> impl futures::Stream<Item = Ranges> + 'static;
}
