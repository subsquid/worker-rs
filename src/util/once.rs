use std::sync::Mutex;

use anyhow::{anyhow, Result};

pub struct UseOnce<T> {
    inner: Mutex<Option<T>>
}

impl<T> UseOnce<T> {
    pub fn new(value: T) -> Self {
        UseOnce {
            inner: Mutex::new(Some(value))
        }
    }

    pub fn take(&self) -> Result<T> {
        self
            .inner
            .try_lock()
            .ok()
            .and_then(|mut opt| opt.take())
            .ok_or_else(|| anyhow!("Attempted to take value twice"))
    }
}