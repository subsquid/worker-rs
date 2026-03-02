use std::path::PathBuf;

use clap::Parser;

use sqd_worker::controller::worker::{QueryType, Worker};
use sqd_worker::storage::manager::StateManager;

const DATASET: &str = "ethereum-mainnet";
const CHUNK_ID: &str = "0017881390/0017881390-0017882786-32ee9457";

fn tests_data() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("data")
}

/// Set up a temporary working directory with symlinked test data.
///
/// Layout: `{tmpdir}/{base64(dataset)}/{top}/{first-last-hash}/` -> test parquet files
fn setup_workdir(tmp: &std::path::Path) {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
    let encoded = URL_SAFE_NO_PAD.encode(DATASET.as_bytes());
    let chunk_dir = tmp.join(&encoded).join(CHUNK_ID);
    std::fs::create_dir_all(&chunk_dir).unwrap();

    let src = tests_data().join("0017881390/0017881390-0017882786-32ee9457");
    for entry in std::fs::read_dir(&src).unwrap() {
        let entry = entry.unwrap();
        let file_name = entry.file_name();
        std::os::unix::fs::symlink(entry.path(), chunk_dir.join(&file_name)).unwrap();
    }
}

async fn create_worker(workdir: &std::path::Path) -> Worker {
    let workdir = camino::Utf8PathBuf::from_path_buf(workdir.to_path_buf()).unwrap();
    let args = sqd_worker::cli::Args::parse_from([
        "test",
        "--data-dir",
        "/dev/null",
        "--key",
        "/dev/null",
        "--rpc-url",
        "http://localhost",
        "--l1-rpc-url",
        "http://localhost",
    ]);
    let keypair = sqd_network_transport::Keypair::generate_ed25519();
    let peer_id = keypair.public().to_peer_id();
    let state_manager = StateManager::new(workdir, 1, peer_id, args).await.unwrap();
    Worker::new(state_manager, 10)
}

#[tokio::test]
async fn test_worker_executes_real_query() {
    let tmp = tempfile::tempdir().unwrap();
    setup_workdir(tmp.path());

    let worker = create_worker(tmp.path()).await;

    let result = worker
        .run_query(
            r#"{"type": "evm"}"#,
            DATASET.to_string(),
            Some((17881390, 17881400)),
            CHUNK_ID,
            None,
            QueryType::PlainQuery,
        )
        .await;

    let ok = result.expect("Query should succeed");
    assert!(!ok.data.is_empty(), "Query should return non-empty data");
    assert!(
        ok.last_block >= 17881390,
        "last_block should be at least the first requested block"
    );

    // Verify data is valid JSON lines
    let text = String::from_utf8(ok.data).expect("Data should be valid UTF-8");
    let lines: Vec<&str> = text.lines().collect();
    assert!(!lines.is_empty(), "Should have at least one JSON line");
    for line in &lines {
        let _: serde_json::Value =
            serde_json::from_str(line).expect("Each line should be valid JSON");
    }
}

#[tokio::test]
async fn test_query_not_found_for_missing_chunk() {
    let tmp = tempfile::tempdir().unwrap();
    setup_workdir(tmp.path());

    let worker = create_worker(tmp.path()).await;

    let result = worker
        .run_query(
            r#"{"type": "evm"}"#,
            DATASET.to_string(),
            None,
            "0099999999/0099999999-0099999999-deadbeef",
            None,
            QueryType::PlainQuery,
        )
        .await;

    assert!(result.is_err(), "Query for missing chunk should fail");
}
