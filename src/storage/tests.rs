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

    fn expect_rejected(assignment: WorkerAssignment, keypair: &Keypair) -> String {
        match DatasetsIndex::new(
            AssignmentBlob::Worker(assignment),
            "test-asgn",
            keypair,
            all_schemas_available,
        ) {
            Err(e) => format!("{e:#}"),
            Ok(_) => panic!("assignment should have been rejected"),
        }
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
    fn unset_bitmap_resolves_to_the_whole_roster() {
        let files = files_of(7, None);

        assert_eq!(
            names(&files),
            ["blocks.parquet", "logs.parquet", "transactions.parquet"]
        );
        assert_eq!(
            files[0].url.as_str(),
            "https://solana-mainnet-2.sqd-datasets.io/0221000000/0221000000-0221000649-BQJdx/blocks.parquet",
            "the download prefix is dataset_base_url + chunk id, with no per-chunk base_url"
        );
    }

    #[test]
    fn bitmap_narrows_the_roster_to_the_tables_present() {
        let files = files_of(7, Some(&["blocks", "transactions"]));

        assert_eq!(
            names(&files),
            ["blocks.parquet", "transactions.parquet"],
            "a table absent from the bitmap is not downloaded"
        );
    }

    /// Foreign blobs must not bypass write-schema roster validation.
    #[test]
    fn a_chunk_on_an_unregistered_write_schema_cannot_be_published() {
        let peer_id = Keypair::generate_ed25519().public().to_peer_id();

        let err = try_build(peer_id, 9, None)
            .err()
            .expect("the builder refuses a chunk whose write schema was never registered");
        assert!(format!("{err:#}").contains("write schema 9"), "{err:#}");
    }

    #[test]
    fn an_assignment_without_an_entry_for_this_worker_is_rejected() {
        let keypair = Keypair::generate_ed25519();
        let assigned_to_someone_else =
            assignment(Keypair::generate_ed25519().public().to_peer_id(), 7, None);

        let message = expect_rejected(assigned_to_someone_else, &keypair);
        assert!(
            message.contains("no assignment for this worker"),
            "{message}"
        );
    }

    #[test]
    fn the_chunks_write_schema_is_recoverable_from_the_index() {
        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let index = DatasetsIndex::new(
            AssignmentBlob::Worker(assignment(peer_id, 7, None)),
            "test-asgn",
            &keypair,
            all_schemas_available,
        )
        .unwrap();

        let chunk = index.chunks().keys().next().unwrap().clone();
        assert_eq!(
            index.chunk_schema(&chunk),
            ChunkSchema::Pinned(SchemaId::new(7))
        );
        let absent = ChunkRef::new(
            std::sync::Arc::new(DATASET.to_owned()),
            std::sync::Arc::from("nope"),
        );
        assert_eq!(index.chunk_schema(&absent), ChunkSchema::ByType);
    }

    /// A rewritten chunk uses its version key and generation prefix.
    #[test]
    fn a_republished_chunk_is_keyed_and_addressed_by_its_version() {
        const GENERATION: &str = "_bf/01HQZK3M7X8P2NVWTC4RYFGDS9";

        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let mut builder = WorkerAssignmentBuilder::new("test-secret").check_continuity(false);
        builder.register_write_schema(7, &["blocks"]).unwrap();
        let mut dataset = builder.new_dataset(DATASET, BASE_URL);
        dataset.register_generation(2, GENERATION).unwrap();
        dataset
            .new_chunk()
            .id(CHUNK_ID)
            .block_range(221000000..=221000649)
            .size(1000000)
            .version(2)
            .write_schema_id(7)
            .worker_indexes(&[0])
            .finish()
            .unwrap();
        dataset.finish().unwrap();
        builder.add_worker(peer_id, sqd_assignments::WorkerStatus::Ok);
        let assignment = WorkerAssignment::from_owned(builder.finish()).unwrap();

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

    /// A dataset whose base url will not parse leaves every chunk of it without an address: the
    /// document contradicts itself, so it is inapplicable whole (FM-12) rather than applied and
    /// given up on chunk by chunk.
    #[test]
    fn a_dataset_whose_base_url_will_not_parse_makes_the_document_inapplicable() {
        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let mut builder = WorkerAssignmentBuilder::new("test-secret").check_continuity(false);
        builder.register_write_schema(7, &["blocks"]).unwrap();
        let mut dataset = builder.new_dataset(DATASET, "not a url");
        dataset
            .new_chunk()
            .id(CHUNK_ID)
            .block_range(221000000..=221000649)
            .size(1000000)
            .write_schema_id(7)
            .worker_indexes(&[0])
            .finish()
            .unwrap();
        dataset.finish().unwrap();
        builder.add_worker(peer_id, sqd_assignments::WorkerStatus::Ok);
        let assignment = WorkerAssignment::from_owned(builder.finish()).unwrap();

        let message = expect_rejected(assignment, &keypair);
        assert!(message.contains("not a url"), "{message}");
        assert!(message.contains(DATASET), "names the dataset: {message}");
    }

    /// The same for a version the dataset registers no generation for: the address of every
    /// chunk at that version depends on an entry the document does not carry. The builder
    /// refuses to emit such a document, so the input is corrupted after the fact: the one
    /// chunk's `versions` column entry is moved from the registered 3 to an unregistered 4.
    #[test]
    fn a_version_without_a_generation_makes_the_document_inapplicable() {
        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let mut builder = WorkerAssignmentBuilder::new("test-secret").check_continuity(false);
        builder.register_write_schema(7, &["blocks"]).unwrap();
        let mut dataset = builder.new_dataset(DATASET, BASE_URL);
        dataset
            .register_generation(3, "_bf/01HQZK3M7X8P2NVWTC4RYFGDS9")
            .unwrap();
        dataset
            .new_chunk()
            .id(CHUNK_ID)
            .block_range(221000000..=221000649)
            .size(1000000)
            .version(3)
            .write_schema_id(7)
            .worker_indexes(&[0])
            .finish()
            .unwrap();
        dataset.finish().unwrap();
        builder.add_worker(peer_id, sqd_assignments::WorkerStatus::Ok);
        let mut document = builder.finish();

        // A one-entry `[uint32]` vector holding 3: length 1 then the value, little-endian.
        let column: [u8; 8] = [1, 0, 0, 0, 3, 0, 0, 0];
        let at: Vec<usize> = document
            .windows(column.len())
            .enumerate()
            .filter(|(_, window)| *window == column)
            .map(|(at, _)| at)
            .collect();
        assert_eq!(at.len(), 1, "the versions column is the one such vector");
        document[at[0] + 4] = 4;
        let assignment =
            WorkerAssignment::from_owned(document).expect("still a well-formed document");

        let message = expect_rejected(assignment, &keypair);
        assert!(message.contains("version 4"), "{message}");
        assert!(message.contains("no generation"), "{message}");
    }

    /// A chunk from somewhere else is the caller's mistake, not the document's.
    #[test]
    fn an_unknown_chunk_is_not_an_address_fault() {
        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let index = DatasetsIndex::new(
            AssignmentBlob::Worker(assignment(peer_id, 7, None)),
            "test-asgn",
            &keypair,
            all_schemas_available,
        )
        .expect("assignment is well-formed");

        let stranger = ChunkRef::new(
            std::sync::Arc::new(DATASET.to_owned()),
            std::sync::Arc::from("nope"),
        );
        assert_eq!(
            index.list_files(&stranger),
            Err(UnresolvedChunk::NotAssigned)
        );
    }

    /// Rejects table names that could escape the chunk directory.
    #[test]
    fn a_roster_naming_a_table_that_is_not_a_file_name_is_refused() {
        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let mut builder = WorkerAssignmentBuilder::new("test-secret").check_continuity(false);
        // Sorted, as the builder requires: '.' precedes 'b'.
        builder
            .register_write_schema(7, &["../escape", "blocks"])
            .unwrap();
        let mut dataset = builder.new_dataset(DATASET, BASE_URL);
        dataset
            .new_chunk()
            .id(CHUNK_ID)
            .block_range(221000000..=221000649)
            .size(1000000)
            .write_schema_id(7)
            .worker_indexes(&[0])
            .finish()
            .unwrap();
        dataset.finish().unwrap();
        builder.add_worker(peer_id, sqd_assignments::WorkerStatus::Ok);
        let assignment = WorkerAssignment::from_owned(builder.finish()).unwrap();

        let message = expect_rejected(assignment, &keypair);
        assert!(
            message.contains("../escape") && message.contains("not a file name"),
            "{message}"
        );
    }

    /// Recovery refuses a chunk directory whose first block lies before its top dir, so a
    /// document placing one there would download fine and fail the next start.
    #[test]
    fn a_chunk_under_the_wrong_top_dir_is_refused() {
        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let mut builder = WorkerAssignmentBuilder::new("test-secret").check_continuity(false);
        builder.register_write_schema(7, &["blocks"]).unwrap();
        let mut dataset = builder.new_dataset(DATASET, BASE_URL);
        dataset
            .new_chunk()
            // Top dir 0221001000 for a chunk that starts at 0221000000.
            .id("0221001000/0221000000-0221000649-BQJdx")
            .block_range(221000000..=221000649)
            .size(1000000)
            .write_schema_id(7)
            .worker_indexes(&[0])
            .finish()
            .unwrap();
        dataset.finish().unwrap();
        builder.add_worker(peer_id, sqd_assignments::WorkerStatus::Ok);
        let assignment = WorkerAssignment::from_owned(builder.finish()).unwrap();

        let message = expect_rejected(assignment, &keypair);
        assert!(
            message.contains("lies under top dir 221001000"),
            "{message}"
        );
    }

    /// A name the filesystem will not take fails every download of every chunk on the schema.
    #[test]
    fn a_roster_table_too_long_for_a_file_name_is_refused() {
        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let mut builder = WorkerAssignmentBuilder::new("test-secret").check_continuity(false);
        let long = "b".repeat(250);
        builder.register_write_schema(7, &[&long]).unwrap();
        let mut dataset = builder.new_dataset(DATASET, BASE_URL);
        dataset
            .new_chunk()
            .id(CHUNK_ID)
            .block_range(221000000..=221000649)
            .size(1000000)
            .write_schema_id(7)
            .worker_indexes(&[0])
            .finish()
            .unwrap();
        dataset.finish().unwrap();
        builder.add_worker(peer_id, sqd_assignments::WorkerStatus::Ok);
        let assignment = WorkerAssignment::from_owned(builder.finish()).unwrap();

        let message = expect_rejected(assignment, &keypair);
        assert!(message.contains("exceed 255 bytes"), "{message}");
    }

    #[test]
    fn an_assignment_referencing_an_unavailable_schema_is_rejected() {
        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();

        let message = match DatasetsIndex::new(
            AssignmentBlob::Worker(assignment(peer_id, 7, None)),
            "test-asgn",
            &keypair,
            |_| false,
        ) {
            Err(e) => format!("{e:#}"),
            Ok(_) => panic!("assignment should have been rejected"),
        };
        assert!(
            message.contains("write schema 7") && message.contains("schema bundle"),
            "{message}"
        );
    }

    /// The legacy format shares one base url across a dataset too, so the same rule applies.
    #[test]
    fn a_legacy_dataset_whose_base_url_will_not_parse_makes_the_document_inapplicable() {
        use sqd_assignments::AssignmentBuilder;

        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let mut builder = AssignmentBuilder::new("test-secret").check_continuity(false);
        builder
            .new_chunk()
            .id(CHUNK_ID)
            .dataset_id(DATASET)
            .dataset_base_url("not a url")
            .block_range(0..=1000)
            .size(1)
            .worker_indexes(&[0])
            .files(&["blocks.parquet".to_owned()])
            .finish()
            .unwrap();
        builder.finish_dataset();
        builder.add_worker(peer_id, sqd_assignments::WorkerStatus::Ok, &[0]);
        let assignment = sqd_assignments::Assignment::from_owned(builder.finish()).unwrap();

        let error = DatasetsIndex::new(
            AssignmentBlob::Legacy(assignment),
            "test-asgn",
            &keypair,
            |_| unreachable!("a legacy assignment references no write schemas"),
        )
        .err()
        .expect("the document is inapplicable");
        assert!(format!("{error:#}").contains("not a url"), "{error:#}");
    }

    #[test]
    fn legacy_assignments_expose_no_write_schema() {
        use sqd_assignments::AssignmentBuilder;

        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let mut builder = AssignmentBuilder::new("test-secret").check_continuity(false);
        builder
            .new_chunk()
            .id(CHUNK_ID)
            .dataset_id(DATASET)
            .dataset_base_url(BASE_URL)
            .block_range(0..=1000)
            .size(1)
            .worker_indexes(&[0])
            .files(&["blocks.parquet".to_owned()])
            .finish()
            .unwrap();
        builder.finish_dataset();
        builder.add_worker(peer_id, sqd_assignments::WorkerStatus::Ok, &[0]);
        let assignment = sqd_assignments::Assignment::from_owned(builder.finish()).unwrap();

        let index = DatasetsIndex::new(
            AssignmentBlob::Legacy(assignment),
            "test-asgn",
            &keypair,
            |_| unreachable!("a legacy assignment references no write schemas"),
        )
        .unwrap();

        let chunk = index.chunks().keys().next().unwrap().clone();
        assert_eq!(index.chunk_schema(&chunk), ChunkSchema::ByType);
    }

    #[test]
    fn chunks_are_keyed_the_same_way_as_the_legacy_format() {
        let keypair = Keypair::generate_ed25519();
        let peer_id = keypair.public().to_peer_id();
        let index = DatasetsIndex::new(
            AssignmentBlob::Worker(assignment(peer_id, 7, None)),
            "test-asgn",
            &keypair,
            all_schemas_available,
        )
        .unwrap();

        let expected = ChunkRef::new(
            std::sync::Arc::new(DATASET.to_owned()),
            std::sync::Arc::from(CHUNK_ID),
        );
        assert!(index.chunks().contains_key(&expected));
        assert_eq!(index.assignment_id(), "test-asgn");
        assert_eq!(index.status(), sqd_assignments::WorkerStatus::Ok);
    }
}
