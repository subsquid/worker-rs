//! HC-1 — scheduler simulator: the network-state document (IB-40) and the assignment
//! document (IB-41), served over HTTP exactly where the worker looks for them.
//!
//! Built with the real `sqd-assignments` builder, so the worker parses the layout production
//! emits, encrypted headers included. Fault knobs corrupt *inputs*, never the encoder.

use std::io::Write;

use sqd_assignments::{AssignmentBuilder, WorkerStatus};
use sqd_network_transport::PeerId;

use super::corpus::Chunk;
use super::seed::SplitMix64;
use super::stub::{Fault, HttpStub};

const NETWORK_STATE_PATH: &str = "/network-state.json";
const STORAGE_SECRET: &str = "conformance-storage-secret";

/// How an assignment deviates from well-formed. One knob per registered fault so a test
/// names the defect it is provoking (spec/13 CT-4).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AssignmentFault {
    #[default]
    None,
    /// The **first** placement's base address doesn't parse (FM-11). Per-chunk on purpose:
    /// the blast radius under test is one chunk, not the document.
    UnparseableFileUrl,
    /// The worker is in the roster but holds no chunks — REQ-25's deletion floor.
    NoChunksForWorker,
    /// A roster entry whose peer id can't be parsed (FM-12). The reader panics on it.
    CorruptRosterPeerId,
    /// Truncated gzip stream — FM-12 / GAP-4's unvalidated document.
    TruncatedDocument,
}

/// One assignment the simulator is prepared to serve.
pub struct Assignment {
    pub id: String,
    /// Chunks in this assignment, in roster order — the bit order of the DEF-13 map.
    pub chunks: Vec<ChunkPlacement>,
    path: String,
}

/// A chunk in the assignment together with where its files live.
#[derive(Clone)]
pub struct ChunkPlacement {
    pub dataset_id: String,
    pub dataset_base_url: String,
    pub chunk_id: String,
    pub first_block: u64,
    pub last_block: u64,
    pub size: u32,
    pub files: Vec<String>,
    /// Whether this worker is assigned the chunk.
    pub assigned: bool,
}

pub struct Scheduler {
    stub: HttpStub,
    rng: SplitMix64,
    published: Option<String>,
}

impl Scheduler {
    pub async fn start(rng: SplitMix64) -> Self {
        Self {
            stub: HttpStub::start().await,
            rng,
            published: None,
        }
    }

    /// The `--assignment-url` the worker under test should be pointed at.
    pub fn network_state_url(&self) -> String {
        self.stub.url(NETWORK_STATE_PATH)
    }

    pub fn stub(&self) -> &HttpStub {
        &self.stub
    }

    /// Registers a chunk placement for `chunk`, hosted at `dataset_base_url`.
    pub fn placement(
        dataset_id: &str,
        dataset_base_url: &str,
        chunk: &Chunk,
        assigned: bool,
    ) -> ChunkPlacement {
        ChunkPlacement {
            dataset_id: dataset_id.to_owned(),
            dataset_base_url: dataset_base_url.to_owned(),
            chunk_id: chunk.id.clone(),
            first_block: chunk.first_block,
            last_block: chunk.last_block,
            size: chunk.size_bytes(),
            files: chunk.files.iter().map(|(n, _)| n.clone()).collect(),
            assigned,
        }
    }

    /// Builds, gzips and serves an assignment, then points the network-state document at
    /// it. Returns what was published so tests can assert against the same chunk set.
    pub fn publish(
        &mut self,
        id: &str,
        worker: PeerId,
        placements: &[ChunkPlacement],
        fault: AssignmentFault,
    ) -> Assignment {
        let doc = self.build_document(worker, placements, fault);
        let path = format!("/assignments/{id}.fb.gz");

        let gz = gzip(&doc);
        let body = if fault == AssignmentFault::TruncatedDocument {
            gz[..gz.len() / 2].to_vec()
        } else {
            gz
        };
        self.stub.put(path.clone(), body);

        let state = serde_json::json!({
            "network": "conformance",
            "assignment": {
                "id": id,
                "fb_url_v1": self.stub.url(&path),
                "effective_from": 0,
            }
        });
        self.stub
            .put(NETWORK_STATE_PATH, serde_json::to_vec(&state).unwrap());
        self.published = Some(id.to_owned());

        Assignment {
            id: id.to_owned(),
            chunks: placements.to_vec(),
            path,
        }
    }

    /// Makes the assignment document unfetchable (GAP-9's head-of-line blocking).
    pub fn break_document(&self, assignment: &Assignment, status: u16) {
        self.stub
            .inject(assignment.path.clone(), Fault::Status(status));
    }

    pub fn repair_document(&self) {
        self.stub.clear_faults();
    }

    fn build_document(
        &mut self,
        worker: PeerId,
        placements: &[ChunkPlacement],
        fault: AssignmentFault,
    ) -> Vec<u8> {
        // Seeded `rand_core` 0.6 CSPRNG, so assignment bytes — nonces included — replay.
        let mut builder = AssignmentBuilder::new_with_rng(STORAGE_SECRET, self.rng.clone())
            .check_continuity(false);

        let mut assigned_indexes = Vec::new();
        for (index, placement) in placements.iter().enumerate() {
            let base_url = if fault == AssignmentFault::UnparseableFileUrl && index == 0 {
                "not a url"
            } else {
                &placement.dataset_base_url
            };
            // `iter_chunks` resolves membership here, not from the worker's chunk list.
            let assigned = placement.assigned && fault != AssignmentFault::NoChunksForWorker;
            builder
                .new_chunk()
                .id(&placement.chunk_id)
                .dataset_id(&placement.dataset_id)
                .dataset_base_url(base_url)
                .block_range(placement.first_block..=placement.last_block)
                .size(placement.size)
                .worker_indexes(if assigned { &[0] } else { &[] })
                .last_block_timestamp(1_700_000_000 + placement.last_block)
                .files(&placement.files)
                .finish()
                .expect("chunk is well-formed");
            if assigned {
                assigned_indexes.push(index as u32);
            }
        }
        builder.finish_dataset();
        // A fixed timestamp keeps the HMAC in the encrypted headers reproducible.
        builder.add_worker_with_timestamp(
            worker,
            WorkerStatus::Ok,
            &assigned_indexes,
            1_700_000_000,
        );

        let mut doc = builder.finish();
        if fault == AssignmentFault::CorruptRosterPeerId {
            corrupt_peer_id(&mut doc, worker);
        }
        doc
    }
}

/// Patching the encoded bytes is the only way in: the builder takes a parsed `PeerId`.
fn corrupt_peer_id(doc: &mut [u8], worker: PeerId) {
    let encoded = worker.to_bytes();
    let at = doc
        .windows(encoded.len())
        .position(|w| w == encoded)
        .expect("the roster carries the worker's peer id verbatim");
    // Not a multihash code libp2p accepts; the 38-byte struct layout stays intact.
    doc[at] = 0x01;
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(bytes).expect("gzip write");
    encoder.finish().expect("gzip finish")
}
