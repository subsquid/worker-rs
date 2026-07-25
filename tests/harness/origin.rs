//! HC-2 — data origin with a byte ledger (IB-42).
//!
//! The ledger is the provenance oracle: what lands on disk must equal what this stub sent,
//! which is what makes INV-13 checkable at all.

use super::corpus::Chunk;
use super::stub::{Fault, HttpStub, Served};

pub struct Origin {
    stub: HttpStub,
}

impl Origin {
    pub async fn start() -> Self {
        Self {
            stub: HttpStub::start().await,
        }
    }

    /// Base address to put in the assignment's dataset entry. The worker joins
    /// `<base>/<chunk id>/<file name>` per IB-42, so hosting is a flat namespace.
    pub fn dataset_base_url(&self) -> String {
        format!("{}/", self.stub.base_url())
    }

    /// Publishes every file of `chunk` under its chunk id.
    pub fn host(&self, chunk: &Chunk) {
        for (name, bytes) in &chunk.files {
            self.stub.put(Self::path(&chunk.id, name), bytes.clone());
        }
    }

    pub fn path(chunk_id: &str, file: &str) -> String {
        format!("/{chunk_id}/{file}")
    }

    pub fn inject(&self, chunk_id: &str, file: &str, fault: Fault) {
        self.stub.inject(Self::path(chunk_id, file), fault);
    }

    pub fn clear_faults(&self) {
        self.stub.clear_faults();
    }

    /// Bytes the origin sent for this file — compare against what landed on disk.
    pub fn served_bytes(&self, chunk_id: &str, file: &str) -> Option<Vec<u8>> {
        self.stub.last_served(&Self::path(chunk_id, file))
    }

    pub fn fetch_count(&self, chunk_id: &str, file: &str) -> usize {
        self.stub.request_count(&Self::path(chunk_id, file))
    }

    pub fn ledger(&self) -> Vec<Served> {
        self.stub.ledger()
    }
}
