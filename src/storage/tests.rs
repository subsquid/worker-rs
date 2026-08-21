use camino::{Utf8Path as Path, Utf8PathBuf as PathBuf};
use std::collections::HashMap;

use anyhow::Context;

use super::Filesystem;

pub struct TestFilesystem {
    pub files: HashMap<PathBuf, Vec<PathBuf>>,
}

impl Filesystem for TestFilesystem {
    async fn ls_root(&self) -> anyhow::Result<Vec<PathBuf>> {
        Ok(self.files.keys().cloned().collect())
    }

    async fn ls(&self, path: impl AsRef<Path>) -> anyhow::Result<Vec<PathBuf>> {
        self.files
            .get(path.as_ref())
            .cloned()
            .with_context(|| format!("Couldn't find top dir {}", path.as_ref()))
    }
}

#[test]
fn test_chunks_with_same_block_range() {
    use sqd_assignments::AssignmentBuilder;
    use sqd_network_transport::Keypair;

    use super::datasets_index::{AssignmentBlob, DatasetsIndex};

    let chunk_a_id = "0000000000/0000000000-0000001000-abcdef12";
    let chunk_b_id = "0000000000/0000000000-0000001000-abcdef12-fork";

    let mut builder = AssignmentBuilder::new("test-secret").check_continuity(false);

    builder
        .new_chunk()
        .id(chunk_a_id)
        .dataset_id("test-dataset")
        .dataset_base_url("https://example.com/")
        .block_range(0..=1000)
        .size(1)
        .worker_indexes(&[0])
        .files(&["blocks.parquet".to_owned()])
        .finish()
        .unwrap();

    // Same block range as the first chunk, distinguished only by the trailing
    // suffix. `add_chunk` returns Err on duplicate range, but with
    // `check_continuity(false)` the chunk is still added to the buffer.
    let _ = builder
        .new_chunk()
        .id(chunk_b_id)
        .dataset_id("test-dataset")
        .dataset_base_url("https://example.com/")
        .block_range(0..=1000)
        .size(1)
        .worker_indexes(&[0])
        .files(&["blocks.parquet".to_owned()])
        .finish();

    builder.finish_dataset();

    let keypair = Keypair::generate_ed25519();
    let peer_id = keypair.public().to_peer_id();
    builder.add_worker(peer_id, sqd_assignments::WorkerStatus::Ok, &[0, 1]);

    let bytes = builder.finish();
    let assignment = sqd_assignments::Assignment::from_owned(bytes).unwrap();

    let index = DatasetsIndex::new(
        AssignmentBlob::Legacy(assignment),
        "test-asgn",
        &keypair,
        |_| unreachable!("a legacy assignment references no write schemas"),
    )
    .unwrap();

    assert_eq!(
        index.chunks().len(),
        2,
        "both suffix-distinguished chunks should be present in the index"
    );

    let chunk_a = index
        .chunks()
        .keys()
        .find(|cr| cr.chunk.as_ref() == chunk_a_id)
        .cloned()
        .expect("chunk A should be in the index");
    let chunk_b = index
        .chunks()
        .keys()
        .find(|cr| cr.chunk.as_ref() == chunk_b_id)
        .cloned()
        .expect("chunk B should be in the index");

    let files_a = index
        .list_files(&chunk_a)
        .expect("list_files for chunk A should succeed");
    let files_b = index
        .list_files(&chunk_b)
        .expect("list_files for chunk B should succeed");

    assert_eq!(files_a.len(), 1);
    assert_eq!(files_b.len(), 1);
    assert_eq!(files_a[0].name, "blocks.parquet");
    assert_eq!(files_b[0].name, "blocks.parquet");
    assert_eq!(
        files_a[0].url.as_str(),
        "https://example.com/0000000000/0000000000-0000001000-abcdef12/blocks.parquet"
    );
    assert_eq!(
        files_b[0].url.as_str(),
        "https://example.com/0000000000/0000000000-0000001000-abcdef12-fork/blocks.parquet"
    );
}

/// Worker-assignment files derive from the write-schema roster and `tables_present`.
#[cfg(test)]
mod worker_assignment {
    use sqd_assignments::{WorkerAssignment, WorkerAssignmentBuilder};
    use sqd_network_transport::Keypair;

    use crate::storage::datasets_index::{
        AssignmentBlob, ChunkSchema, DatasetsIndex, RemoteFile, UnresolvedChunk,
    };
    use crate::types::schema::SchemaId;
    use crate::types::state::ChunkRef;

    fn all_schemas_available(_: SchemaId) -> bool {
        true
    }

    const CHUNK_ID: &str = "0221000000/0221000000-0221000649-BQJdx";
    const DATASET: &str = "s3://solana-mainnet-2";
    const BASE_URL: &str = "https://solana-mainnet-2.sqd-datasets.io";

    fn try_build(
        peer_id: sqd_network_transport::PeerId,
        schema_id: u32,
        tables_present: Option<&[&str]>,
    ) -> anyhow::Result<WorkerAssignment> {
        let mut builder = WorkerAssignmentBuilder::new("test-secret").check_continuity(false);
        builder
            .register_write_schema(7, &["blocks", "logs", "transactions"])
            .unwrap();

        let mut dataset = builder.new_dataset(DATASET, BASE_URL);
        let mut chunk = dataset
            .new_chunk()
            .id(CHUNK_ID)
            .block_range(221000000..=221000649)
            .size(1000000)
            .write_schema_id(schema_id);
        if let Some(tables) = tables_present {
            chunk = chunk.tables_present(tables)?;
        }
        chunk.worker_indexes(&[0]).finish()?;
        dataset.finish()?;

        builder.add_worker(peer_id, sqd_assignments::WorkerStatus::Ok);
        Ok(WorkerAssignment::from_owned(builder.finish())?)
    }

    fn assignment(
        peer_id: sqd_network_transport::PeerId,
        schema_id: u32,
        tables_present: Option<&[&str]>,
    ) -> WorkerAssignment {
        try_build(peer_id, schema_id, tables_present).expect("assignment is well-formed")
    }

    fn rejection(
        blob: AssignmentBlob,
        keypair: &Keypair,
        schema_available: fn(SchemaId) -> bool,
    ) -> String {
        match DatasetsIndex::new(blob, "test-asgn", keypair, schema_available) {
            Err(e) => format!("{e:#}"),
            Ok(_) => panic!("assignment should have been rejected"),
        }
    }

    /// One chunk on write schema 7 with roster `tables`, at `generation`'s version if any.
    fn document(
        peer_id: sqd_network_transport::PeerId,
        base_url: &str,
        tables: &[&str],
        generation: Option<(u32, &str)>,
    ) -> Vec<u8> {
        let mut builder = WorkerAssignmentBuilder::new("test-secret").check_continuity(false);
        builder.register_write_schema(7, tables).unwrap();
        let mut dataset = builder.new_dataset(DATASET, base_url);
        if let Some((version, prefix)) = generation {
            dataset.register_generation(version, prefix).unwrap();
        }
        dataset
            .new_chunk()
            .id(CHUNK_ID)
            .block_range(221000000..=221000649)
            .size(1000000)
            .version(generation.map_or(0, |(version, _)| version))
            .write_schema_id(7)
            .worker_indexes(&[0])
            .finish()
            .unwrap();
        dataset.finish().unwrap();
        builder.add_worker(peer_id, sqd_assignments::WorkerStatus::Ok);
        builder.finish()
    }

    fn legacy_document(peer_id: sqd_network_transport::PeerId, base_url: &str) -> Vec<u8> {
        let mut builder =
            sqd_assignments::AssignmentBuilder::new("test-secret").check_continuity(false);
        builder
            .new_chunk()
            .id(CHUNK_ID)
            .dataset_id(DATASET)
            .dataset_base_url(base_url)
            .block_range(0..=1000)
            .size(1)
            .worker_indexes(&[0])
            .files(&["blocks.parquet".to_owned()])
            .finish()
            .unwrap();
        builder.finish_dataset();
        builder.add_worker(peer_id, sqd_assignments::WorkerStatus::Ok, &[0]);
        builder.finish()
    }

    fn files_of(schema_id: u32, tables_present: Option<&[&str]>) -> Vec<RemoteFile> {
        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let index = DatasetsIndex::new(
            AssignmentBlob::Worker(assignment(peer_id, schema_id, tables_present)),
            "test-asgn",
            &keypair,
            all_schemas_available,
        )
        .expect("assignment is well-formed");

        let chunk = index
            .chunks()
            .keys()
            .find(|c| c.chunk.as_ref() == CHUNK_ID)
            .cloned()
            .expect("the chunk is assigned to this worker");
        index.list_files(&chunk).expect("files resolve")
    }

    fn names(files: &[RemoteFile]) -> Vec<&str> {
        files.iter().map(|f| f.name.as_str()).collect()
    }

    #[test]
    fn roster_and_bitmap_decide_the_chunk_files() {
        let whole = files_of(7, None);
        assert_eq!(
            names(&whole),
            ["blocks.parquet", "logs.parquet", "transactions.parquet"],
            "no bitmap: the whole roster"
        );
        assert_eq!(
            whole[0].url.as_str(),
            "https://solana-mainnet-2.sqd-datasets.io/0221000000/0221000000-0221000649-BQJdx/blocks.parquet",
            "the download prefix is dataset_base_url + chunk id, with no per-chunk base_url"
        );

        let narrowed = files_of(7, Some(&["blocks", "transactions"]));
        assert_eq!(
            names(&narrowed),
            ["blocks.parquet", "transactions.parquet"],
            "a table absent from the bitmap is not downloaded"
        );
    }

    #[test]
    fn a_worker_index_answers_for_its_chunks_and_not_for_strangers() {
        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let index = DatasetsIndex::new(
            AssignmentBlob::Worker(assignment(peer_id, 7, None)),
            "test-asgn",
            &keypair,
            all_schemas_available,
        )
        .unwrap();
        let chunk = ChunkRef::new(
            std::sync::Arc::new(DATASET.to_owned()),
            std::sync::Arc::from(CHUNK_ID),
        );
        let stranger = ChunkRef::new(
            std::sync::Arc::new(DATASET.to_owned()),
            std::sync::Arc::from("nope"),
        );

        assert!(
            index.chunks().contains_key(&chunk),
            "keyed as the legacy format keys it: dataset, id, version 0"
        );
        assert_eq!(index.assignment_id(), "test-asgn");
        assert_eq!(index.status(), sqd_assignments::WorkerStatus::Ok);
        assert_eq!(
            index.chunk_schema(&chunk),
            ChunkSchema::Pinned(SchemaId::new(7))
        );
        assert_eq!(index.chunk_schema(&stranger), ChunkSchema::ByType);
        assert_eq!(
            index.list_files(&stranger),
            Err(UnresolvedChunk::NotAssigned),
            "a chunk from somewhere else is the caller's mistake, not the document's"
        );
    }

    /// A rewritten chunk uses its version key and generation prefix.
    #[test]
    fn a_republished_chunk_is_keyed_and_addressed_by_its_version() {
        const GENERATION: &str = "_bf/01HQZK3M7X8P2NVWTC4RYFGDS9";

        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let assignment = WorkerAssignment::from_owned(document(
            peer_id,
            BASE_URL,
            &["blocks"],
            Some((2, GENERATION)),
        ))
        .unwrap();

        let index = DatasetsIndex::new(
            AssignmentBlob::Worker(assignment),
            "test-asgn",
            &keypair,
            all_schemas_available,
        )
        .expect("assignment is well-formed");

        let chunk = index
            .chunks()
            .keys()
            .next()
            .expect("the chunk is assigned to this worker")
            .clone();
        assert_eq!(
            chunk.chunk.as_ref(),
            CHUNK_ID,
            "the id is what it always was"
        );
        assert_eq!(chunk.version, 2);
        assert_eq!(chunk.store_path(), format!("_v2/{CHUNK_ID}"));
        assert_eq!(
            index.list_files(&chunk).expect("files resolve")[0]
                .url
                .as_str(),
            format!("{BASE_URL}/{GENERATION}/{CHUNK_ID}/blocks.parquet"),
        );
    }

    /// A document that contradicts itself, or the store it is written to, is inapplicable whole
    /// (FM-12). The version row is corrupted after the fact — the builder refuses to emit a
    /// version with no generation — by moving the chunk's `versions` entry from 3 to 4.
    #[test]
    fn a_document_that_contradicts_itself_is_refused_whole() {
        fn none_available(_: SchemaId) -> bool {
            false
        }
        fn not_consulted(_: SchemaId) -> bool {
            unreachable!("a legacy assignment references no write schemas")
        }
        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let stranger = Keypair::generate_ed25519().public().to_peer_id();
        let long = "b".repeat(250);
        let worker = |bytes: Vec<u8>| {
            AssignmentBlob::Worker(WorkerAssignment::from_owned(bytes).expect("well-formed"))
        };
        let mut unregistered_version = document(
            peer_id,
            BASE_URL,
            &["blocks"],
            Some((3, "_bf/01HQZK3M7X8P2NVWTC4RYFGDS9")),
        );
        // A one-entry `[uint32]` vector holding 3: length 1 then the value, little-endian.
        let column: [u8; 8] = [1, 0, 0, 0, 3, 0, 0, 0];
        let at: Vec<usize> = unregistered_version
            .windows(column.len())
            .enumerate()
            .filter(|(_, window)| *window == column)
            .map(|(at, _)| at)
            .collect();
        assert_eq!(at.len(), 1, "the versions column is the one such vector");
        unregistered_version[at[0] + 4] = 4;

        let faults: Vec<(&str, AssignmentBlob, fn(SchemaId) -> bool, Vec<&str>)> = vec![
            (
                "assigned to another worker",
                AssignmentBlob::Worker(assignment(stranger, 7, None)),
                all_schemas_available,
                vec!["no assignment for this worker"],
            ),
            (
                "dataset base url that will not parse",
                worker(document(peer_id, "not a url", &["blocks"], None)),
                all_schemas_available,
                vec!["not a url", DATASET],
            ),
            (
                "version with no generation entry",
                worker(unregistered_version),
                all_schemas_available,
                vec!["version 4", "no generation"],
            ),
            (
                "roster table that is not a file name",
                worker(document(peer_id, BASE_URL, &["../escape", "blocks"], None)),
                all_schemas_available,
                vec!["../escape", "not a file name"],
            ),
            (
                "roster table too long for a file name",
                worker(document(peer_id, BASE_URL, &[&long], None)),
                all_schemas_available,
                vec!["exceed 255 bytes"],
            ),
            (
                "write schema its bundle does not carry",
                AssignmentBlob::Worker(assignment(peer_id, 7, None)),
                none_available,
                vec!["write schema 7", "schema bundle"],
            ),
            (
                "legacy dataset base url that will not parse",
                AssignmentBlob::Legacy(
                    sqd_assignments::Assignment::from_owned(legacy_document(peer_id, "not a url"))
                        .unwrap(),
                ),
                not_consulted,
                vec!["not a url"],
            ),
        ];

        for (fault, blob, schema_available, fragments) in faults {
            let message = rejection(blob, &keypair, schema_available);
            for fragment in fragments {
                assert!(message.contains(fragment), "{fault}: {message}");
            }
        }
    }
}
