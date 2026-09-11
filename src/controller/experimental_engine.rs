//! Experimental query engine and schema loading.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context};
use reqwest::Url;
use sqd_network_transport::PeerId;
use sqd_query_engine::metadata::DatasetDescription;
use tokio::task::JoinSet;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tower::retry::backoff::{Backoff, MakeBackoff};

use crate::controller::assignments::new_reqwest_client;
use crate::controller::schema_bundle::SchemaRegistry;
use crate::controller::worker::OutputFormat;
use crate::query::result::{QueryError, QueryOk, QueryResult};
use crate::util::backoff;

const FETCH_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(serde::Deserialize)]
struct Manifest {
    /// Query "type" → schema URL (absolute or relative to the manifest URL)
    schemas: HashMap<String, String>,
}

/// Owns the CDN task. Assignment application stops it under split and starts it under legacy.
/// Dropping the owner aborts any remaining task, including during unwinding.
pub struct SchemaRefresh {
    registry: Arc<SchemaRegistry>,
    manifest_url: String,
    refresh_interval: Duration,
    peer_id: PeerId,
    shutdown: CancellationToken,
    running: Option<CancellationToken>,
    tasks: JoinSet<()>,
}

impl SchemaRefresh {
    pub fn new(
        registry: Arc<SchemaRegistry>,
        manifest_url: String,
        refresh_interval: Duration,
        peer_id: PeerId,
        shutdown: CancellationToken,
    ) -> Self {
        let mut refresh = Self {
            registry,
            manifest_url,
            refresh_interval,
            peer_id,
            shutdown,
            running: None,
            tasks: JoinSet::new(),
        };
        refresh.start();
        refresh
    }

    pub fn start(&mut self) {
        if self.running.is_some() || self.shutdown.is_cancelled() {
            return;
        }
        let token = self.shutdown.child_token();
        self.tasks.spawn(run_schemas_refresh_loop(
            Arc::clone(&self.registry),
            self.manifest_url.clone(),
            self.refresh_interval,
            self.peer_id,
            token.clone(),
        ));
        self.running = Some(token);
    }

    /// Cancellation interrupts HTTP requests and retry waits; returning guarantees the task exited.
    pub async fn stop(&mut self) {
        if let Some(token) = self.running.take() {
            token.cancel();
        }
        while let Some(result) = self.tasks.join_next().await {
            result.expect("query schemas refresh task panicked");
        }
    }

    /// Keep task failures in the assignment subsystem's fail-fast supervision tree.
    pub async fn wait_until_stopped(&mut self) {
        if self.tasks.is_empty() {
            std::future::pending::<()>().await;
        }
        self.tasks
            .join_next()
            .await
            .expect("query schemas refresh task exists")
            .expect("query schemas refresh task panicked");
        assert!(
            self.shutdown.is_cancelled(),
            "query schemas refresh task exited unexpectedly"
        );
    }
}

/// Refreshes immediately, then periodically. Failed attempts retain loaded schemas and retry.
async fn run_schemas_refresh_loop(
    registry: Arc<SchemaRegistry>,
    manifest_url: String,
    refresh_interval: Duration,
    peer_id: PeerId,
    cancellation_token: CancellationToken,
) {
    let mut timer = tokio::time::interval(refresh_interval);
    timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let client = new_reqwest_client(FETCH_TIMEOUT, peer_id);
    let mut last_manifest: Option<String> = None;
    let mut retry = backoff::exponential(Duration::from_secs(1), refresh_interval);

    // Dropping this future cancels the whole refresh, including the current HTTP request.
    cancellation_token.run_until_cancelled(async {
        loop {
            timer.tick().await;
            let mut backoff = retry.make_backoff();
            loop {
                match refresh_schemas(&registry, &manifest_url, &client, &mut last_manifest).await {
                    Ok(true) => {
                        tracing::info!("Loaded query schemas from {manifest_url}");
                        break;
                    }
                    Ok(false) => break,
                    Err(e) => {
                        tracing::warn!(error = %format!("{e:#}"), "Failed to refresh query schemas; retrying");
                        backoff.next_backoff().await;
                    }
                }
            }
        }
    }).await;
    tracing::info!("Query schemas refresh task finished");
}

/// Returns `Ok(false)` if the manifest hasn't changed. On error the previously
/// loaded schemas are kept.
async fn refresh_schemas(
    registry: &SchemaRegistry,
    manifest_url: &str,
    client: &reqwest::Client,
    last_manifest: &mut Option<String>,
) -> anyhow::Result<bool> {
    tracing::debug!("Fetching query schemas manifest from {manifest_url}");
    let manifest_body = client
        .get(manifest_url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await
        .with_context(|| format!("couldn't fetch manifest from {manifest_url}"))?;
    if last_manifest.as_ref() == Some(&manifest_body) {
        tracing::debug!("Query schemas manifest has not been changed");
        return Ok(false);
    }

    let manifest: Manifest =
        serde_yaml::from_str(&manifest_body).context("couldn't parse query schemas manifest")?;
    let base_url = Url::parse(manifest_url).context("invalid manifest URL")?;

    let mut schemas = HashMap::with_capacity(manifest.schemas.len());
    for (dataset_type, schema_url) in manifest.schemas {
        let url = base_url
            .join(&schema_url)
            .with_context(|| format!("invalid schema URL: {schema_url}"))?;
        let yaml = client
            .get(url.clone())
            .send()
            .await?
            .error_for_status()?
            .text()
            .await
            .with_context(|| format!("couldn't fetch query schema from {url}"))?;
        let desc = sqd_query_engine::metadata::parse_dataset_description(&yaml)
            .map_err(|e| anyhow!("couldn't parse query schema from {url}: {e:?}"))?;
        anyhow::ensure!(
            desc.name == dataset_type,
            "query schema name '{}' from {url} doesn't match the manifest key '{dataset_type}'",
            desc.name,
        );
        schemas.insert(dataset_type, Arc::new(desc));
    }

    registry.replace_legacy(schemas);
    *last_manifest = Some(manifest_body);
    Ok(true)
}

pub fn extract_dataset_type(query_str: &str) -> Result<String, QueryError> {
    #[derive(serde::Deserialize)]
    struct QueryTypeField {
        #[serde(rename = "type")]
        dataset_type: String,
    }
    serde_json::from_str::<QueryTypeField>(query_str)
        .map(|q| q.dataset_type)
        .map_err(|e| QueryError::BadRequest(format!("Couldn't parse query: {e}")))
}

/// Blocking — intended to be run on the query threadpool.
pub fn execute_query(
    query_str: &str,
    schema: &DatasetDescription,
    block_range: (u64, u64),
    chunk_path: &str,
    output_format: OutputFormat,
) -> QueryResult {
    let (from_block, to_block) = block_range;

    let parse_timer = std::time::Instant::now();
    let mut query = sqd_query_engine::query::parse_query(query_str.as_bytes(), schema)
        .map_err(|e| QueryError::BadRequest(format!("Couldn't parse query: {e:?}")))?;
    // The block range from the network message overrides the range in the query contents
    query.from_block = from_block;
    query.to_block = Some(to_block);
    let plan = sqd_query_engine::query::compile(&query, schema)
        .map_err(|e| QueryError::BadRequest(format!("Couldn't compile query: {e:?}")))?;
    let parse_duration = parse_timer.elapsed();

    let chunk_dir = Path::new(chunk_path);
    let exec_timer = std::time::Instant::now();
    // No blocks in the output → report the query's upper bound so the portal can
    // see progress, like the legacy engine does.
    let (bytes, last_block, exec_duration, serialization_duration) = match output_format {
        OutputFormat::JsonLines => {
            let blocks = sqd_query_engine::output::execute_plan(&plan, schema, chunk_dir)
                .map_err(QueryError::from)?;
            let exec_duration = exec_timer.elapsed();
            let ser_timer = std::time::Instant::now();
            let (bytes, last_block) = match blocks {
                Some(blocks) => {
                    let last_block = blocks.last_block();
                    (blocks.into_json_lines(), last_block)
                }
                None => (Vec::new(), to_block),
            };
            (bytes, last_block, exec_duration, ser_timer.elapsed())
        }
        OutputFormat::ArrowIpc => {
            // The worker compresses the response itself, so leave Arrow's built-in
            // compression off; `binary` emits raw byte columns instead of hex strings.
            let output =
                sqd_query_engine::output::execute_plan_arrow(&plan, schema, chunk_dir, false, true)
                    .map_err(QueryError::from)?;
            // Arrow encoding is eager, so it's all execution time, no separate step.
            let exec_duration = exec_timer.elapsed();
            let (bytes, last_block) = match output {
                Some(output) => {
                    let last_block = output.last_block();
                    (output.into_data(), last_block)
                }
                None => (Vec::new(), to_block),
            };
            (bytes, last_block, exec_duration, Duration::ZERO)
        }
    };

    if bytes.len() > super::worker::RESPONSE_LIMIT {
        return Err(QueryError::from(anyhow::anyhow!("Response too large")));
    }

    Ok(QueryOk::new(
        bytes,
        1,
        last_block,
        parse_duration,
        exec_duration,
        serialization_duration,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::controller::schema_bundle::BundleHash;
    use crate::types::schema::SchemaId;
    use arrow::array::{ArrayRef, BinaryArray, UInt32Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use serde_json::json;
    use std::fs::File;
    use std::future::IntoFuture;
    use tempfile::TempDir;

    const TEST_SCHEMA: &str = r#"
version: v2
name: evm
tables:
  blocks:
    output:
      name: block
      fields: [number]
    block_number_column: number
    sort_key: [number]
    columns:
      number:
        type: uint64
  logs:
    request:
      name: logs
      filters: []
    output:
      name: log
      fields: [transaction_index, log_index, data]
    block_number_column: block_number
    item_order_keys: [transaction_index, log_index]
    sort_key: [block_number, transaction_index, log_index]
    columns:
      block_number:
        type: uint64
      transaction_index:
        type: uint32
      log_index:
        type: uint32
      data:
        type: string
        encoding: hex_bytes
        weight: data_size
      data_size:
        type: uint64
        system: true
"#;

    fn write_parquet(path: &Path, batch: &RecordBatch) {
        let file = File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
        writer.write(batch).unwrap();
        writer.close().unwrap();
    }

    fn create_test_chunk() -> TempDir {
        let dir = tempfile::tempdir().unwrap();

        let blocks_schema = Arc::new(Schema::new(vec![Field::new(
            "number",
            DataType::UInt64,
            false,
        )]));
        let blocks = RecordBatch::try_new(
            blocks_schema,
            vec![Arc::new(UInt64Array::from(vec![10, 11, 12])) as ArrayRef],
        )
        .unwrap();
        write_parquet(&dir.path().join("blocks.parquet"), &blocks);

        let logs_schema = Arc::new(Schema::new(vec![
            Field::new("block_number", DataType::UInt64, false),
            Field::new("transaction_index", DataType::UInt32, false),
            Field::new("log_index", DataType::UInt32, false),
            Field::new("data", DataType::Binary, false),
            Field::new("data_size", DataType::UInt64, false),
        ]));
        let logs = RecordBatch::try_new(
            logs_schema,
            vec![
                Arc::new(UInt64Array::from(vec![10, 11, 12])) as ArrayRef,
                Arc::new(UInt32Array::from(vec![0, 0, 0])) as ArrayRef,
                Arc::new(UInt32Array::from(vec![0, 0, 0])) as ArrayRef,
                Arc::new(BinaryArray::from(vec![
                    b"a".as_slice(),
                    b"b".as_slice(),
                    b"c".as_slice(),
                ])) as ArrayRef,
                Arc::new(UInt64Array::from(vec![
                    15 * 1024 * 1024,
                    15 * 1024 * 1024,
                    15 * 1024 * 1024,
                ])) as ArrayRef,
            ],
        )
        .unwrap();
        write_parquet(&dir.path().join("logs.parquet"), &logs);

        dir
    }

    fn test_schema() -> DatasetDescription {
        sqd_query_engine::metadata::parse_dataset_description(TEST_SCHEMA).unwrap()
    }

    #[test]
    fn execute_query_should_return_one_json_object_per_line() {
        let chunk = create_test_chunk();
        let query = json!({
            "type": "evm",
            "includeAllBlocks": true,
            "fields": {"block": {"number": true}}
        })
        .to_string();

        let result = execute_query(
            &query,
            &test_schema(),
            (10, 12),
            chunk.path().to_str().unwrap(),
            OutputFormat::JsonLines,
        )
        .unwrap();
        let blocks: Vec<serde_json::Value> = std::str::from_utf8(&result.data)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();

        assert_eq!(
            blocks,
            vec![
                json!({"header": {"number": 10}}),
                json!({"header": {"number": 11}}),
                json!({"header": {"number": 12}}),
            ]
        );
    }

    #[test]
    fn execute_query_should_report_last_block_selected_by_response_budget() {
        let chunk = create_test_chunk();
        let query = json!({
            "type": "evm",
            "logs": [{}],
            "fields": {"log": {"data": true}}
        })
        .to_string();

        let result = execute_query(
            &query,
            &test_schema(),
            (10, 12),
            chunk.path().to_str().unwrap(),
            OutputFormat::JsonLines,
        )
        .unwrap();

        assert_eq!(result.last_block, 10);
    }

    #[test]
    fn execute_query_should_report_range_end_when_output_is_empty() {
        let chunk = create_test_chunk();
        let query = json!({
            "type": "evm",
            "logs": [{}],
            "fields": {"log": {"data": true}}
        })
        .to_string();

        // The chunk holds blocks 10..=12; a range beyond them selects nothing.
        let result = execute_query(
            &query,
            &test_schema(),
            (100, 200),
            chunk.path().to_str().unwrap(),
            OutputFormat::JsonLines,
        )
        .unwrap();

        assert!(result.data.is_empty());
        assert_eq!(result.last_block, 200);
    }

    #[test]
    fn execute_query_arrow_output_differs_from_json() {
        let chunk = create_test_chunk();
        let query = json!({
            "type": "evm",
            "includeAllBlocks": true,
            "fields": {"block": {"number": true}}
        })
        .to_string();
        let path = chunk.path().to_str().unwrap();

        let json = execute_query(
            &query,
            &test_schema(),
            (10, 12),
            path,
            OutputFormat::JsonLines,
        )
        .unwrap();
        let arrow = execute_query(
            &query,
            &test_schema(),
            (10, 12),
            path,
            OutputFormat::ArrowIpc,
        )
        .unwrap();

        // Same block range reported, but Arrow IPC bytes are a distinct, non-JSON encoding.
        assert_eq!(arrow.last_block, json.last_block);
        assert!(!arrow.data.is_empty());
        assert_ne!(arrow.data, json.data);
        assert!(std::str::from_utf8(&arrow.data)
            .ok()
            .and_then(|s| s.lines().next())
            .and_then(|l| serde_json::from_str::<serde_json::Value>(l).ok())
            .is_none());
    }

    #[test]
    fn extracts_dataset_type() {
        let query = r#"{"type": "evm", "fromBlock": 0, "logs": [{}]}"#;
        assert_eq!(extract_dataset_type(query).unwrap(), "evm");
        assert!(matches!(
            extract_dataset_type(r#"{"fromBlock": 0}"#),
            Err(QueryError::BadRequest(_))
        ));
        assert!(matches!(
            extract_dataset_type("not json"),
            Err(QueryError::BadRequest(_))
        ));
    }

    #[test]
    fn parses_manifest() {
        let manifest: Manifest = serde_yaml::from_str(
            r#"
            schemas:
              evm: /sqd-network/query-schemas/evm.yaml
              solana: https://example.com/solana.yaml
            unknown_future_key: ignored
            "#,
        )
        .unwrap();
        assert_eq!(manifest.schemas.len(), 2);

        let base =
            Url::parse("https://cdn.subsquid.io/sqd-network/mainnet/query-schemas.yml").unwrap();
        assert_eq!(
            base.join(&manifest.schemas["evm"]).unwrap().as_str(),
            "https://cdn.subsquid.io/sqd-network/query-schemas/evm.yaml"
        );
        assert_eq!(
            base.join(&manifest.schemas["solana"]).unwrap().as_str(),
            "https://example.com/solana.yaml"
        );
    }

    #[tokio::test]
    async fn fetches_schemas_from_http_server() {
        const SCHEMA: &str = r#"
version: v2
name: evm
tables:
  blocks:
    output:
      name: block
      fields: [number, hash]
    block_number_column: number
    sort_key: [number]
    columns:
      number:
        type: uint64
      hash:
        type: string
        encoding: hex_bytes
"#;
        let app = axum::Router::new()
            .route(
                "/sqd-network/mainnet/query-schemas.yml",
                axum::routing::get(|| async {
                    "schemas:\n  evm: /sqd-network/query-schemas/evm.yaml\n"
                }),
            )
            .route(
                "/sqd-network/query-schemas/evm.yaml",
                axum::routing::get(|| async { SCHEMA }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(axum::serve(listener, app).into_future());

        let registry = SchemaRegistry::memory();
        let client = reqwest::Client::new();
        let manifest_url = format!("http://{addr}/sqd-network/mainnet/query-schemas.yml");
        let mut last_manifest = None;

        let updated = refresh_schemas(&registry, &manifest_url, &client, &mut last_manifest)
            .await
            .unwrap();
        assert!(updated);
        assert_eq!(registry.get_by_type("evm").unwrap().name, "evm");

        // unchanged manifest
        let updated = refresh_schemas(&registry, &manifest_url, &client, &mut last_manifest)
            .await
            .unwrap();
        assert!(!updated);

        // fetch failure keeps the loaded schemas
        let mut last_manifest = None;
        refresh_schemas(
            &registry,
            "http://127.0.0.1:1/nothing",
            &client,
            &mut last_manifest,
        )
        .await
        .unwrap_err();
        assert!(registry.get_by_type("evm").is_ok());
    }

    #[tokio::test]
    async fn stopping_or_shutting_down_interrupts_http_and_preserves_loaded_schemas() {
        use crate::controller::schema_bundle::test_support::SCHEMA;

        for blocked_path in ["/manifest", "/evm.yaml"] {
            for shutdown_requested in [false, true] {
                let entered = Arc::new(tokio::sync::Notify::new());
                let app = axum::Router::new().fallback(axum::routing::get({
                    let entered = entered.clone();
                    move |uri: axum::http::Uri| {
                        let entered = entered.clone();
                        async move {
                            if uri.path() == blocked_path {
                                entered.notify_one();
                                std::future::pending::<()>().await;
                            }
                            "schemas:\n  evm: /evm.yaml\n"
                        }
                    }
                }));
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
                let addr = listener.local_addr().unwrap();
                let server = tokio::spawn(axum::serve(listener, app).into_future());
                let registry = Arc::new(SchemaRegistry::memory());
                let schema = Arc::new(
                    sqd_query_engine::metadata::parse_dataset_description(SCHEMA).unwrap(),
                );
                registry.replace_legacy(HashMap::from([("evm".to_owned(), schema.clone())]));
                let shutdown = CancellationToken::new();
                let mut refresh = SchemaRefresh::new(
                    registry.clone(),
                    format!("http://{addr}/manifest"),
                    Duration::from_secs(3600),
                    PeerId::random(),
                    shutdown.clone(),
                );
                tokio::time::timeout(Duration::from_secs(2), entered.notified())
                    .await
                    .unwrap();
                if shutdown_requested {
                    shutdown.cancel();
                    tokio::time::timeout(Duration::from_secs(2), refresh.wait_until_stopped())
                        .await
                        .unwrap();
                }
                tokio::time::timeout(Duration::from_secs(2), refresh.stop())
                    .await
                    .expect("must not wait for the 60-second HTTP timeout");
                assert!(refresh.tasks.is_empty());
                assert!(Arc::ptr_eq(&registry.get_by_type("evm").unwrap(), &schema));
                if shutdown_requested {
                    refresh.start();
                    assert!(refresh.tasks.is_empty(), "shutdown prevents restarting");
                }
                server.abort();
            }
        }
    }

    #[tokio::test]
    async fn restarting_refreshes_immediately_and_repeated_start_keeps_one_task() {
        use crate::controller::test_support::TestServer;
        let server = TestServer::start().await;
        let url = server.serve("/manifest", b"schemas: {}\n".to_vec(), 0);
        let registry = Arc::new(SchemaRegistry::memory());
        let mut refresh = SchemaRefresh::new(
            registry.clone(),
            url,
            Duration::from_secs(3600),
            PeerId::random(),
            CancellationToken::new(),
        );
        for expected in [1, 2] {
            refresh.start();
            assert_eq!(refresh.tasks.len(), 1);
            tokio::time::timeout(Duration::from_secs(2), async {
                while server.hits("/manifest") < expected {
                    tokio::time::sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .expect("refresh must not wait for the one-hour tick");
            refresh.stop().await;
            assert_eq!(server.hits("/manifest"), expected);
            assert!(refresh.tasks.is_empty());
        }
    }

    #[tokio::test]
    async fn stopping_interrupts_retry_backoff() {
        use crate::controller::test_support::TestServer;
        let server = TestServer::start().await;
        let registry = Arc::new(SchemaRegistry::memory());
        let mut refresh = SchemaRefresh::new(
            registry,
            server.url("/unavailable"),
            Duration::from_secs(3600),
            PeerId::random(),
            CancellationToken::new(),
        );
        // A second request establishes that the retry path has been exercised.
        tokio::time::timeout(Duration::from_secs(5), async {
            while server.hits("/unavailable") < 2 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_millis(200), refresh.stop())
            .await
            .expect("stop must not wait out the backoff");
        assert!(refresh.tasks.is_empty());
    }

    #[tokio::test]
    #[should_panic(expected = "query schemas refresh task panicked")]
    async fn refresh_panics_are_observed_by_the_owner() {
        let mut refresh = SchemaRefresh::new(
            Arc::new(SchemaRegistry::memory()),
            "unused".to_owned(),
            Duration::ZERO,
            PeerId::random(),
            CancellationToken::new(),
        );
        refresh.wait_until_stopped().await;
    }

    #[test]
    fn a_missing_schema_id_is_a_worker_fault_and_a_missing_type_is_the_client_s() {
        use sqd_messages::query_error::Err as WireErr;

        let registry = SchemaRegistry::memory();
        assert!(matches!(
            registry.get_by_type("evm"),
            Err(QueryError::Other(_))
        ));
        assert!(matches!(
            registry.get_by_id(id(7)),
            Err(QueryError::Other(_))
        ));

        registry.merge_bundle(HashMap::from([(id(7), description("evm"))]), hash(0xaa));
        registry.replace_legacy(HashMap::new());

        let wire = WireErr::from;

        assert!(
            matches!(
                wire(registry.get_by_type("solana").unwrap_err()),
                WireErr::BadRequest(_)
            ),
            "the query named the type, so the query is what is wrong"
        );
        assert!(
            matches!(
                wire(registry.get_by_type("evm").unwrap_err()),
                WireErr::BadRequest(_)
            ),
            "a bundle fills no type index: evm is held by id 7, not by type"
        );
        assert!(
            matches!(
                wire(registry.get_by_id(id(9)).unwrap_err()),
                WireErr::ServerError(_)
            ),
            "no query names a schema id: a miss is the worker's own bookkeeping, and a \
             client-blamed bad_request would tell routing clients not to retry elsewhere"
        );
    }

    fn hash(tag: u8) -> BundleHash {
        format!("sha256:{}", format!("{tag:02x}").repeat(32))
            .parse()
            .unwrap()
    }

    fn id(n: u32) -> SchemaId {
        SchemaId::new(n)
    }

    fn description(name: &str) -> Arc<DatasetDescription> {
        Arc::new(
            sqd_query_engine::metadata::parse_dataset_description(
                &TEST_SCHEMA.replace("name: evm", &format!("name: {name}")),
            )
            .unwrap(),
        )
    }
}
