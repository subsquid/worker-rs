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

    use super::datasets_index::DatasetsIndex;

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

    let index = DatasetsIndex::new(assignment, "test-asgn", &keypair).unwrap();

    assert_eq!(
        index.chunks().len(),
        2,
        "both suffix-distinguished chunks should be present in the index"
    );

    let chunk_a = index
        .chunks()
        .keys()
        .find(|cr| cr.chunk.as_str() == chunk_a_id)
        .cloned()
        .expect("chunk A should be in the index");
    let chunk_b = index
        .chunks()
        .keys()
        .find(|cr| cr.chunk.as_str() == chunk_b_id)
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
