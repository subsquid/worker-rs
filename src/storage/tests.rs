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

/// Worker assignments carry no file list: files derive from the write schema roster,
/// narrowed by `tables_present`.
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

    /// One chunk on write schema 7; `schema_id` may point at an unregistered schema.
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

    /// `DatasetsIndex` isn't `Debug` (self-referencing flatbuffer), so `expect_err` is out.
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

    /// An unregistered write schema yields no file list, so the whole assignment is refused.
    /// The builder rejects it first, as asserted here; `DatasetsIndex::new` re-checks for
    /// foreign blobs — `from_owned` validates offsets, not that `write_schema_id` resolves.
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

    /// Query execution needs the chunk's schema, else it resolves by dataset type.
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
        // A chunk this assignment doesn't name has no pinned id, so the query's dataset type
        // decides — safe because only the legacy manifest fills the type registry, and it is not
        // filled at all under `--assignment-source worker`
        // (`restored_bundle_schemas_do_not_mark_legacy_schemas_loaded`).
        assert_eq!(index.chunk_schema(&absent), ChunkSchema::ByType);
    }

    /// A rewrite is another copy of the same id, so it is keyed by its version and stored under
    /// it, and its files come from the generation prefix the dataset registers for that version.
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

    /// FM-11: a chunk the document mentions but gives no usable address for fails on its own,
    /// and says so — the worker used to conflate it with a chunk the assignment never mentioned
    /// and take the process down over the pair.
    #[test]
    fn an_unusable_address_is_told_apart_from_an_unknown_chunk() {
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

        let index = DatasetsIndex::new(
            AssignmentBlob::Worker(assignment),
            "test-asgn",
            &keypair,
            all_schemas_available,
        )
        .expect("an address is only needed to download, so admission takes this");

        let assigned = index.chunks().keys().next().unwrap().clone();
        let message = match index.list_files(&assigned) {
            Err(UnresolvedChunk::NoAddress(message)) => message,
            other => panic!("expected an unusable address, got {other:?}"),
        };
        assert!(message.contains("not a url"), "{message}");

        let stranger = ChunkRef::new(
            std::sync::Arc::new(DATASET.to_owned()),
            std::sync::Arc::from("nope"),
        );
        assert_eq!(
            index.list_files(&stranger),
            Err(UnresolvedChunk::NotAssigned),
            "a chunk from somewhere else is the caller's mistake, not the document's"
        );
    }

    /// Otherwise the failure — or a silent wrong-version match — surfaces only at query time.
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

    /// Legacy assignments pin no schema, so query-time resolution falls back to the dataset type.
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

    /// Keying is unchanged from the legacy path, so the manager's chunk bookkeeping survives.
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
