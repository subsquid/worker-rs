use crate::types::State;

trait Transport {
    fn send_ping(&self);
    fn subscribe_to_updates(&self) -> impl Iterator<Item = State>;
}