//! HC-1 — scheduler simulator: the network-state document (IB-40b), the worker assignment
//! document (IB-41b) and its schema bundle (IB-44b), served over HTTP exactly where the worker
//! looks for them.
//!
//! Built with the real `sqd-assignments` builder, so the worker parses the layout production
//! emits, encrypted headers included. Fault knobs corrupt *inputs*, never the encoder.

use std::collections::BTreeMap;
use std::io::Write;

use sha2::{Digest, Sha256};
use sqd_assignments::{DataChunk, WorkerAssignmentBuilder, WorkerStatus};
use sqd_network_transport::PeerId;

use super::corpus::Chunk;
use super::seed::SplitMix64;
use super::stub::{Fault, HttpStub};

const NETWORK_STATE_PATH: &str = "/network-state.json";
const SCHEMA_BUNDLE_PATH: &str = "/schema-bundle.tar.gz";
const STORAGE_SECRET: &str = "conformance-storage-secret";

pub const WRITE_SCHEMA_ID: u32 = 7;
const WRITE_SCHEMA_TABLES: [&str; 2] = ["blocks", "logs"];

/// How an assignment deviates from well-formed. One knob per registered fault so a test
/// names the defect it is provoking (spec/13 CT-4).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AssignmentFault {
    #[default]
    None,
    /// A dataset base address that doesn't parse — a document that contradicts itself, refused
    /// whole at admission (FM-12).
    UnparseableFileUrl,
    /// The worker is in the roster but holds no chunks — GAP-3's deletion floor.
    NoChunksForWorker,
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
    /// Zero for the ingested copy.
    pub version: u32,
}

impl ChunkPlacement {
    pub fn at_version(mut self, version: u32) -> Self {
        self.version = version;
        self
    }
}

pub struct Scheduler {
    stub: HttpStub,
    rng: SplitMix64,
    published: Option<String>,
    bundle_hash: String,
}

impl Scheduler {
    pub async fn start(rng: SplitMix64) -> Self {
        let stub = HttpStub::start().await;
        let archive = schema_bundle(&[(WRITE_SCHEMA_ID, super::corpus::SCHEMA_YAML)]);
        let bundle_hash = format!("sha256:{:x}", Sha256::digest(&archive));
        stub.put(SCHEMA_BUNDLE_PATH, archive);
        Self {
            stub,
            rng,
            published: None,
            bundle_hash,
        }
    }

    pub fn break_schema_bundle(&self, status: u16) {
        self.stub.inject(SCHEMA_BUNDLE_PATH, Fault::Status(status));
    }

    /// Publishes a bundle that omits the assignment's schema (FM-53c).
    pub fn publish_bundle_missing_the_assignment_schema(&mut self) {
        let archive = schema_bundle(&[(WRITE_SCHEMA_ID + 1, super::corpus::SCHEMA_YAML)]);
        self.bundle_hash = format!("sha256:{:x}", Sha256::digest(&archive));
        self.stub.put(SCHEMA_BUNDLE_PATH, archive);
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
            version: 0,
        }
    }

    pub fn generation_prefix(version: u32) -> String {
        format!("_gen/{version}")
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
        let doc = self.build_worker_document(worker, placements, fault);
        let path = format!("/assignments/{id}.fb.gz");

        let gz = gzip(&doc);
        let body = if fault == AssignmentFault::TruncatedDocument {
            gz[..gz.len() / 2].to_vec()
        } else {
            gz
        };
        self.stub.put(path.clone(), body);

        // The portal half is never fetched, but a split state that omits it resolves to
        // nothing (IB-40).
        let state = serde_json::json!({
            "network": "conformance",
            "assignment_type": "split",
            "worker_assignment": {"id": id, "fb_url": self.stub.url(&path), "version": "1"},
            "portal_assignment": {
                "id": id,
                "fb_url": self.stub.url("/portal-assignment.fb.gz"),
                "version": "1",
            },
            "schema_bundle": {
                "hash": self.bundle_hash,
                "url": self.stub.url(SCHEMA_BUNDLE_PATH),
            },
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

    pub fn break_document(&self, assignment: &Assignment, status: u16) {
        self.stub
            .inject(assignment.path.clone(), Fault::Status(status));
    }

    pub fn repair_document(&self) {
        self.stub.clear_faults();
    }

    fn build_worker_document(
        &mut self,
        worker: PeerId,
        placements: &[ChunkPlacement],
        fault: AssignmentFault,
    ) -> Vec<u8> {
        let mut builder = WorkerAssignmentBuilder::new_with_rng(STORAGE_SECRET, self.rng.clone())
            .check_continuity(false);
        builder
            .register_write_schema(WRITE_SCHEMA_ID, &WRITE_SCHEMA_TABLES)
            .expect("roster is sorted");

        // BTreeMap preserves the dataset order expected by the reader.
        let mut by_dataset: BTreeMap<&str, Vec<&ChunkPlacement>> = BTreeMap::new();
        for placement in placements {
            by_dataset
                .entry(&placement.dataset_id)
                .or_default()
                .push(placement);
        }
        for (dataset_id, placements) in by_dataset {
            let base_url = if fault == AssignmentFault::UnparseableFileUrl {
                "not a url"
            } else {
                &placements[0].dataset_base_url
            };
            let mut dataset = builder.new_dataset(dataset_id, base_url);
            for placement in placements {
                if placement.version != 0 {
                    dataset
                        .register_generation(
                            placement.version,
                            &Self::generation_prefix(placement.version),
                        )
                        .expect("generation is well-formed");
                }
                let assigned = placement.assigned && fault != AssignmentFault::NoChunksForWorker;
                dataset
                    .new_chunk()
                    .id(chunk(&placement.chunk_id))
                    .block_range(placement.first_block..=placement.last_block)
                    .size(placement.size)
                    .version(placement.version)
                    .write_schema_id(WRITE_SCHEMA_ID)
                    .worker_indexes(if assigned { &[0] } else { &[] })
                    .finish()
                    .expect("chunk is well-formed");
            }
            dataset.finish().expect("dataset is well-formed");
        }
        builder.add_worker_with_timestamp(worker, WorkerStatus::Ok, 1_700_000_000);

        builder.finish()
    }
}

fn schema_bundle(schemas: &[(u32, &str)]) -> Vec<u8> {
    let mut tar = tar::Builder::new(flate2::write::GzEncoder::new(
        Vec::new(),
        flate2::Compression::fast(),
    ));
    for (id, yaml) in schemas {
        let mut header = tar::Header::new_gnu();
        header.set_size(yaml.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        tar.append_data(&mut header, format!("{id}.yaml"), yaml.as_bytes())
            .expect("tar entry is well-formed");
    }
    tar.into_inner()
        .expect("tar finish")
        .finish()
        .expect("gzip finish")
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(bytes).expect("gzip write");
    encoder.finish().expect("gzip finish")
}

fn chunk(id: &str) -> DataChunk {
    id.parse().expect("valid chunk id")
}
