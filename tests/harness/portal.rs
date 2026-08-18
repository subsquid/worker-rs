//! HC-3 — portal driver: identity keys and signed queries (IB-10).
//!
//! Genuinely signed messages, so RP-1's real authentication runs. Every deviation a test
//! wants is a knob here, not a hand-built message elsewhere.

use sqd_messages::{Compression, OutputFormat, Query, QueryEngine};
use sqd_network_transport::{Keypair, PeerId};

use super::seed::SplitMix64;
use crypto_box::aead::rand_core::RngCore;

/// A portal identity that can sign queries.
pub struct Portal {
    keypair: Keypair,
    peer_id: PeerId,
    /// Query ids must be exactly 36 bytes (IB-10); a counter keeps them unique and stable.
    next_id: std::cell::Cell<u64>,
}

impl Portal {
    /// Derives a portal identity from the run seed, so a failing run replays with the same
    /// peer id (which the metering buckets are keyed by).
    pub fn new(rng: &mut SplitMix64) -> Self {
        let mut secret = [0u8; 32];
        rng.fill_bytes(&mut secret);
        let keypair = Keypair::ed25519_from_bytes(secret).expect("32 bytes is a valid ed25519 key");
        let peer_id = keypair.public().to_peer_id();
        Self {
            keypair,
            peer_id,
            next_id: std::cell::Cell::new(0),
        }
    }

    pub fn peer_id(&self) -> PeerId {
        self.peer_id
    }

    /// Starts a query builder for `chunk_id` over `[begin, end]`, signed for `worker`.
    pub fn query(
        &self,
        worker: PeerId,
        dataset: &str,
        chunk_id: &str,
        range: (u64, u64),
    ) -> Draft<'_> {
        let n = self.next_id.get();
        self.next_id.set(n + 1);
        Draft {
            portal: self,
            signer: worker,
            query: Query {
                // Exactly 36 bytes, as the signature payload requires.
                query_id: format!("conformance-query-{n:018}"),
                dataset: dataset.to_owned(),
                query: String::new(),
                chunk_id: chunk_id.to_owned(),
                block_range: Some(sqd_messages::Range {
                    begin: range.0,
                    end: range.1,
                }),
                timestamp_ms: sqd_worker::util::timestamp_now_ms(),
                signature: Vec::new(),
                compression: Compression::None as i32,
                query_engine: QueryEngine::Dynamic as i32,
                output_format: OutputFormat::Jsonl as i32,
                request_id: format!("conformance-request-{n}"),
                // 0 is the ingested copy, which is also what a portal that names nothing sends.
                chunk_version: 0,
            },
        }
    }
}

/// A query under construction. `sign()` finalises it.
pub struct Draft<'a> {
    portal: &'a Portal,
    signer: PeerId,
    query: Query,
}

impl Draft<'_> {
    pub fn body(mut self, body: impl Into<String>) -> Self {
        self.query.query = body.into();
        self
    }

    /// Names the copy of the chunk to read; 0 is the ingested one (IB-13). Not covered by the
    /// query signature, so a corrupted one would still verify.
    pub fn chunk_version(mut self, version: u32) -> Self {
        self.query.chunk_version = version;
        self
    }

    pub fn engine(mut self, engine: QueryEngine) -> Self {
        self.query.query_engine = engine as i32;
        self
    }

    pub fn output_format(mut self, format: OutputFormat) -> Self {
        self.query.output_format = format as i32;
        self
    }

    pub fn compression(mut self, compression: Compression) -> Self {
        self.query.compression = compression as i32;
        self
    }

    /// Overrides `timestamp_ms` — for the RP-1 freshness check.
    pub fn timestamp_ms(mut self, timestamp_ms: u64) -> Self {
        self.query.timestamp_ms = timestamp_ms;
        self
    }

    /// Sets a raw enum value, including ones outside the known set (RP-1 step 3).
    pub fn raw_query_engine(mut self, value: i32) -> Self {
        self.query.query_engine = value;
        self
    }

    pub fn raw_output_format(mut self, value: i32) -> Self {
        self.query.output_format = value;
        self
    }

    /// Signs for a different worker — the query must not verify at the one under test.
    pub fn signed_for(mut self, worker: PeerId) -> Self {
        self.signer = worker;
        self
    }

    pub fn no_block_range(mut self) -> Self {
        self.query.block_range = None;
        self
    }

    pub fn sign(mut self) -> Query {
        self.query
            .sign(&self.portal.keypair, self.signer)
            .expect("query signs");
        self.query
    }

    /// Signs, then damages the signature — for the RP-1 step 1 rejection.
    pub fn sign_corrupted(self) -> Query {
        let mut query = self.sign();
        if let Some(byte) = query.signature.first_mut() {
            *byte ^= 0xff;
        }
        query
    }
}
