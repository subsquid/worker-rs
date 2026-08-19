//! HC-1 — scheduler simulator: the network-state document (IB-40) and the assignment
//! document (IB-41), served over HTTP exactly where the worker looks for them.
//!
//! Built with the real `sqd-assignments` builder, so the worker parses the layout production
//! emits, encrypted headers included. Fault knobs corrupt *inputs*, never the encoder.

use std::collections::BTreeMap;
use std::io::Write;

use sha2::{Digest, Sha256};
use sqd_assignments::{AssignmentBuilder, WorkerAssignmentBuilder, WorkerStatus};
use sqd_network_transport::PeerId;

use super::corpus::Chunk;
use super::seed::SplitMix64;
use super::stub::{Fault, HttpStub};

const NETWORK_STATE_PATH: &str = "/network-state.json";
const SCHEMA_BUNDLE_PATH: &str = "/schema-bundle.tar.gz";
const STORAGE_SECRET: &str = "conformance-storage-secret";

pub const WRITE_SCHEMA_ID: u32 = 7;
const WRITE_SCHEMA_TABLES: [&str; 2] = ["blocks", "logs"];

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Format {
    #[default]
    Legacy,
    Worker,
}

/// How an assignment deviates from well-formed. One knob per registered fault so a test
/// names the defect it is provoking (spec/13 CT-4).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum AssignmentFault {
    #[default]
    None,
    /// A dataset base address that doesn't parse — a document that contradicts itself, refused
    /// whole at admission (FM-12; once GAP-2's reconciler panic).
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
    format: Format,
    bundle_hash: Option<String>,
}

impl Scheduler {
    pub async fn start(rng: SplitMix64, format: Format) -> Self {
        let stub = HttpStub::start().await;
        let bundle_hash = (format == Format::Worker).then(|| {
            let archive = schema_bundle(&[(WRITE_SCHEMA_ID, super::corpus::SCHEMA_YAML)]);
            let hash = format!("sha256:{:x}", Sha256::digest(&archive));
            stub.put(SCHEMA_BUNDLE_PATH, archive);
            hash
        });
        Self {
            stub,
            rng,
            published: None,
            format,
            bundle_hash,
        }
    }

    pub fn break_schema_bundle(&self, status: u16) {
        self.stub.inject(SCHEMA_BUNDLE_PATH, Fault::Status(status));
    }

    /// Publishes a bundle that omits the assignment's schema (FM-53c).
    pub fn publish_bundle_missing_the_assignment_schema(&mut self) {
        let archive = schema_bundle(&[(WRITE_SCHEMA_ID + 1, super::corpus::SCHEMA_YAML)]);
        self.bundle_hash = Some(format!("sha256:{:x}", Sha256::digest(&archive)));
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
        let doc = match self.format {
            Format::Legacy => self.build_document(worker, placements, fault),
            Format::Worker => self.build_worker_document(worker, placements, fault),
        };
        let path = format!("/assignments/{id}.fb.gz");

        let gz = gzip(&doc);
        let body = if fault == AssignmentFault::TruncatedDocument {
            gz[..gz.len() / 2].to_vec()
        } else {
            gz
        };
        self.stub.put(path.clone(), body);

        let pointer = serde_json::json!({
            "id": id,
            "fb_url_v1": self.stub.url(&path),
            "effective_from": 0,
        });
        let state = match self.format {
            Format::Legacy => serde_json::json!({
                "network": "conformance",
                "assignment": pointer,
            }),
            Format::Worker => serde_json::json!({
                "network": "conformance",
                "worker_assignment": pointer,
                "schema_bundle": {
                    "hash": self.bundle_hash.as_deref().expect("worker format publishes a bundle"),
                    "url": self.stub.url(SCHEMA_BUNDLE_PATH),
                },
            }),
        };
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
            let base_url = if fault == AssignmentFault::UnparseableFileUrl {
                "not a url"
            } else {
                &placement.dataset_base_url
            };
            assert_eq!(
                placement.version, 0,
                "the legacy format has no chunk versions"
            );
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

        builder.finish()
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
                    .id(&placement.chunk_id)
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
