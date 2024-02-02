pub mod http;

use anyhow::Result;
use serde::Serialize;

use crate::types::state::Ranges;

#[derive(Serialize)]
pub struct State {
    pub ranges: Ranges,
}

pub trait Transport: Send {
    fn send_ping(&self, state: State) -> impl futures::Future<Output=Result<()>> + Send;
    fn subscribe_to_updates(&mut self) -> impl futures::Stream<Item = Ranges> + 'static;
}
