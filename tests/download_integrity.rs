//! A chunk commits only when every file of it arrived intact (INV-13, GAP-5).
//!
//! Drives the real `ChunkDownloader` against a server that answers exact raw bytes.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use camino::Utf8PathBuf as PathBuf;
use parquet::arrow::ArrowWriter;
use sqd_worker::storage::datasets_index::RemoteFile;
use sqd_worker::storage::downloader::{ChunkDownloader, DownloadConfig};
use sqd_worker::types::state::ChunkRef;

/// Answers `/blocks.parquet` with fixed raw bytes, then closes the connection.
async fn serve(response: Vec<u8>) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let response = response.clone();
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                let _ = socket.read(&mut buf).await;
                let _ = socket.write_all(&response).await;
            });
        }
    });
    base
}

fn raw(status_line: &str, headers: &str, body: &[u8]) -> Vec<u8> {
    let mut out = format!(
        "HTTP/1.1 {status_line}\r\ncontent-length: {}\r\n{headers}connection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    out.extend_from_slice(body);
    out
}

fn parquet_table() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "number",
        DataType::UInt64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(UInt64Array::from(vec![1u64, 2, 3])) as ArrayRef],
    )
    .unwrap();
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, batch.schema(), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    buf
}

/// Runs one download of a single-file chunk to completion.
async fn download(dst: &PathBuf, url: String) -> anyhow::Result<()> {
    let peer_id = sqd_network_transport::Keypair::generate_ed25519()
        .public()
        .to_peer_id();
    let mut downloader = ChunkDownloader::new(
        peer_id,
        DownloadConfig {
            s3_timeout: Duration::from_secs(10),
            s3_read_timeout: Duration::from_millis(300),
            downloads_max_delay: Duration::from_secs(1),
            max_download_attempts: 3,
        },
    );
    let chunk = ChunkRef::new(
        Arc::new("ds".to_owned()),
        Arc::from("0000001000/0000001000-0000001999-abcdef12"),
    );
    let files = vec![RemoteFile {
        url: format!("{url}/blocks.parquet").parse().unwrap(),
        name: "blocks.parquet".to_owned(),
    }];
    downloader.start_download(chunk, dst.clone(), files, Default::default());
    downloader.downloaded().await.1
}

fn workdir() -> (tempfile::TempDir, PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let root = PathBuf::from_path_buf(dir.path().to_owned()).unwrap();
    (dir, root)
}

#[track_caller]
fn assert_nothing_committed(root: &PathBuf, dst: &PathBuf) {
    assert!(!dst.exists(), "the chunk must not be committed");
    assert_eq!(
        std::fs::read_dir(root).unwrap().count(),
        0,
        "and its staging directory must be swept"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_well_formed_table_commits() {
    let table = parquet_table();
    let url = serve(raw("200 OK", "", &table)).await;
    let (_guard, root) = workdir();
    let dst = root.join("chunk");

    download(&dst, url).await.expect("a healthy download");

    assert_eq!(std::fs::read(dst.join("blocks.parquet")).unwrap(), table);
}

/// A redirect is the response, not the file: no redirect is followed, and `error_for_status`
/// passes 3xx. The body here is a *valid table*, so only the status check can reject it.
#[tokio::test(flavor = "multi_thread")]
async fn no_redirect_is_a_download() {
    for status in [
        "301 Moved Permanently",
        "302 Found",
        "303 See Other",
        "307 Temporary Redirect",
        "308 Permanent Redirect",
    ] {
        let url = serve(raw(
            status,
            "location: https://elsewhere.example/blocks.parquet\r\n",
            &parquet_table(),
        ))
        .await;
        let (_guard, root) = workdir();
        let dst = root.join("chunk");

        let result = download(&dst, url).await;

        assert!(
            result.is_err(),
            "{status} is not a downloaded file: {result:?}"
        );
        assert_nothing_committed(&root, &dst);
    }
}

/// An origin that answers 200 with something else — an error page, a captive portal.
#[tokio::test(flavor = "multi_thread")]
async fn a_response_that_is_not_the_table_is_not_committed() {
    let url = serve(raw("200 OK", "", b"<html>Access Denied</html>")).await;
    let (_guard, root) = workdir();
    let dst = root.join("chunk");

    let result = download(&dst, url).await;

    assert!(result.is_err(), "not a parquet file: {result:?}");
    assert_nothing_committed(&root, &dst);
}

/// A parquet file's metadata lives in its footer, so any short body is unreadable — the
/// origin serving one is indistinguishable from an interrupted unframed transfer.
#[tokio::test(flavor = "multi_thread")]
async fn a_truncated_table_is_not_committed() {
    let table = parquet_table();
    let url = serve(raw("200 OK", "", &table[..table.len() - 4])).await;
    let (_guard, root) = workdir();
    let dst = root.join("chunk");

    let result = download(&dst, url).await;

    assert!(result.is_err(), "truncated table: {result:?}");
    assert_nothing_committed(&root, &dst);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_empty_table_is_not_committed() {
    let url = serve(raw("200 OK", "", b"")).await;
    let (_guard, root) = workdir();
    let dst = root.join("chunk");

    let result = download(&dst, url).await;

    assert!(result.is_err(), "empty table: {result:?}");
    assert_nothing_committed(&root, &dst);
}

/// Framed transfers make an interruption visible to hyper; these guard that it stays so.
#[tokio::test(flavor = "multi_thread")]
async fn an_interrupted_framed_transfer_fails() {
    let mut response =
        b"HTTP/1.1 200 OK\r\ncontent-length: 100000\r\nconnection: close\r\n\r\n".to_vec();
    response.extend_from_slice(&parquet_table());
    let url = serve(response).await;
    let (_guard, root) = workdir();
    let dst = root.join("chunk");

    let result = download(&dst, url).await;

    assert!(result.is_err(), "body shorter than content-length");
    assert_nothing_committed(&root, &dst);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unterminated_chunked_transfer_fails() {
    let mut response =
        b"HTTP/1.1 200 OK\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n".to_vec();
    let table = parquet_table();
    response.extend_from_slice(format!("{:x}\r\n", table.len()).as_bytes());
    response.extend_from_slice(&table);
    response.extend_from_slice(b"\r\n");
    let url = serve(response).await;
    let (_guard, root) = workdir();
    let dst = root.join("chunk");

    let result = download(&dst, url).await;

    assert!(result.is_err(), "no terminating chunk");
    assert_nothing_committed(&root, &dst);
}
