//! Support for the experimental query engine (`sqd-query-engine`), which executes
//! queries against dataset schemas periodically fetched from the CDN.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context};
use arc_swap::ArcSwap;
use rand::Rng;
use reqwest::Url;
use sqd_network_transport::PeerId;
use sqd_query_engine::metadata::DatasetDescription;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;

use crate::controller::assignments::new_reqwest_client;
use crate::query::result::{QueryError, QueryOk, QueryResult};

const FETCH_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(serde::Deserialize)]
struct Manifest {
    /// Query "type" → schema URL (absolute or relative to the manifest URL)
    schemas: HashMap<String, String>,
}

#[derive(Default)]
pub struct QuerySchemaRegistry {
    schemas: ArcSwap<HashMap<String, Arc<DatasetDescription>>>,
    loaded: std::sync::atomic::AtomicBool,
}

impl QuerySchemaRegistry {
    pub fn get(&self, dataset_type: &str) -> Result<Arc<DatasetDescription>, QueryError> {
        if !self.loaded.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(QueryError::Other(
                "query schemas for the experimental engine have not been loaded yet".to_owned(),
            ));
        }
        self.schemas
            .load()
            .get(dataset_type)
            .cloned()
            .ok_or_else(|| {
                QueryError::BadRequest(format!(
                    "dataset type '{dataset_type}' is not supported by the experimental engine"
                ))
            })
    }

    fn store(&self, schemas: HashMap<String, Arc<DatasetDescription>>) {
        self.schemas.store(Arc::new(schemas));
        self.loaded
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

pub async fn run_schemas_refresh_loop(
    registry: Arc<QuerySchemaRegistry>,
    manifest_url: String,
    refresh_interval: Duration,
    peer_id: PeerId,
    cancellation_token: CancellationToken,
) {
    let mut timer = tokio::time::interval(refresh_interval);
    timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let client = new_reqwest_client(FETCH_TIMEOUT, peer_id);
    let mut last_manifest: Option<String> = None;

    loop {
        tokio::select! {
            _ = timer.tick() => {}
            _ = cancellation_token.cancelled() => break,
        }
        let mut current_delay = Duration::from_secs(1);
        loop {
            match refresh_schemas(&registry, &manifest_url, &client, &mut last_manifest).await {
                Ok(true) => {
                    tracing::info!("Loaded query schemas from {manifest_url}");
                    break;
                }
                Ok(false) => break,
                Err(e) => {
                    tracing::warn!(
                        error = ?e,
                        "Failed to refresh query schemas, retrying in {current_delay:?}"
                    );
                    let duration = rand::rng().random_range((current_delay / 2)..current_delay);
                    tokio::select! {
                        _ = tokio::time::sleep(duration) => {}
                        _ = cancellation_token.cancelled() => break,
                    }
                    current_delay = std::cmp::min(current_delay * 2, refresh_interval);
                }
            }
        }
        if cancellation_token.is_cancelled() {
            break;
        }
    }
    tracing::info!("Query schemas refresh task finished");
}

/// Returns `Ok(false)` if the manifest hasn't changed. On error the previously
/// loaded schemas are kept.
async fn refresh_schemas(
    registry: &QuerySchemaRegistry,
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

    registry.store(schemas);
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

    let exec_timer = std::time::Instant::now();
    let blocks = sqd_query_engine::output::execute_plan(&plan, schema, Path::new(chunk_path))
        .map_err(QueryError::from)?;
    let exec_duration = exec_timer.elapsed();

    let serialization_timer = std::time::Instant::now();
    let (bytes, last_block) = if let Some(blocks) = blocks {
        let last_block = blocks.last_block();
        (blocks.into_json_lines(), last_block)
    } else {
        // No blocks in the output — report the query's upper bound so the portal
        // can see progress, like the legacy engine does.
        (Vec::new(), to_block)
    };
    let serialization_duration = serialization_timer.elapsed();

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
    use arrow::array::{ArrayRef, BinaryArray, UInt32Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use serde_json::json;
    use std::fs::File;
    use std::future::IntoFuture;
    use tempfile::TempDir;

    const TEST_SCHEMA: &str = r#"
name: evm
tables:
  blocks:
    block_number_column: number
    field_name: block
    sort_key: [number]
    columns:
      number:
        type: uint64
  logs:
    block_number_column: block_number
    query_name: logs
    field_name: log
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
        json_encoding: hex
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
        )
        .unwrap();

        assert!(result.data.is_empty());
        assert_eq!(result.last_block, 200);
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
name: evm
tables:
  blocks:
    block_number_column: number
    sort_key: [number]
    columns:
      number:
        type: uint64
        stats: true
      hash:
        type: string
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

        let registry = QuerySchemaRegistry::default();
        let client = reqwest::Client::new();
        let manifest_url = format!("http://{addr}/sqd-network/mainnet/query-schemas.yml");
        let mut last_manifest = None;

        let updated = refresh_schemas(&registry, &manifest_url, &client, &mut last_manifest)
            .await
            .unwrap();
        assert!(updated);
        assert_eq!(registry.get("evm").unwrap().name, "evm");

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
        assert!(registry.get("evm").is_ok());
    }

    #[test]
    fn registry_get() {
        let registry = QuerySchemaRegistry::default();
        assert!(matches!(registry.get("evm"), Err(QueryError::Other(_))));

        registry.store(HashMap::new());
        assert!(matches!(
            registry.get("evm"),
            Err(QueryError::BadRequest(_))
        ));
    }
}
