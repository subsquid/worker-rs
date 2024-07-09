use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};

use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use subsquid_network_transport::PeerId;

use crate::{
    metrics,
    query::result::{QueryError, QueryOk, QueryResult},
    storage::{
        datasets_index::DatasetsIndex,
        manager::{self, StateManager},
    },
    types::{dataset::Dataset, state::ChunkSet},
};

lazy_static::lazy_static! {
    static ref PARALLEL_QUERIES: u8 = std::env::var("PARALLEL_QUERIES")
        .map(|s| s.parse().expect("Invalid PARALLEL_QUERIES"))
        .unwrap_or(5);
}

pub struct Worker {
    state_manager: Arc<StateManager>,
    queries_running: AtomicU8,
    pub peer_id: Option<PeerId>,
}

pub struct QueryTask {
    pub dataset: Dataset,
    pub query_str: String,
    pub client_id: Option<PeerId>,
    pub response_sender: oneshot::Sender<QueryResult>,
}

impl Worker {
    pub fn new(state_manager: StateManager) -> Self {
        Self {
            state_manager: Arc::new(state_manager),
            queries_running: 0.into(),
            peer_id: None,
        }
    }

    pub fn with_peer_id(mut self, peer_id: PeerId) -> Self {
        self.peer_id = Some(peer_id);
        self
    }

    pub fn set_desired_chunks(&self, chunks: ChunkSet) {
        self.state_manager.set_desired_chunks(chunks);
    }

    pub fn set_datasets_index(&self, datasets_index: DatasetsIndex) {
        self.state_manager.set_datasets_index(datasets_index);
    }

    pub fn stop_downloads(&self) {
        self.state_manager.stop_downloads();
    }

    pub fn status(&self) -> manager::Status {
        self.state_manager.current_status()
    }

    pub async fn run_query(
        &self,
        query_str: String,
        dataset: Dataset,
        client_id: Option<PeerId>,
    ) -> QueryResult {
        let before = self.queries_running.fetch_add(1, Ordering::SeqCst);
        let counter_guard = scopeguard::guard((), |_| {
            let before = self.queries_running.fetch_sub(1, Ordering::SeqCst);
            metrics::RUNNING_QUERIES.set(before as i64 - 1);
        });
        if before >= *PARALLEL_QUERIES {
            return Err(QueryError::ServiceOverloaded);
        }
        metrics::RUNNING_QUERIES.set(before as i64 + 1);
        let _ = counter_guard;
        tracing::debug!(
            "Running query from {}",
            client_id
                .map(|id| id.to_string())
                .unwrap_or("{unknown}".to_string())
        );
        self.execute_query(query_str, dataset).await
    }

    pub async fn run(&self, cancellation_token: CancellationToken) {
        self.state_manager
            .run(cancellation_token.child_token())
            .await
    }

    async fn execute_query(&self, query_str: String, dataset: String) -> QueryResult {
        let query = sqn_query::Query::from_json_bytes(query_str.as_bytes())
            .map_err(|e| QueryError::BadRequest(format!("Couldn't parse query: {e:?}")))?;
        let Some(first_block) = query.first_block() else {
            return Err(QueryError::BadRequest(
                "Query without first_block".to_owned(),
            ));
        };
        let chunks_guard = self
            .state_manager
            .clone()
            .find_chunks(&dataset, (first_block as u32).into())?;
        if chunks_guard.is_empty() {
            return Err(QueryError::NotFound);
        };
        let (tx, rx) = tokio::sync::oneshot::channel();
        polars_core::POOL.spawn(move || {
            let result = (move || {
                let start_time = std::time::Instant::now();

                let plan = query.compile();
                let data = Vec::with_capacity(1024 * 1024);
                let mut writer = sqn_query::JsonArrayWriter::new(data);
                for path in chunks_guard.iter() {
                    let mut blocks = plan.execute(path.as_str())?;
                    writer.write_blocks(&mut blocks)?;
                }
                let bytes = writer.finish()?;

                Ok(QueryOk::new(
                    bytes,
                    chunks_guard.len(),
                    start_time.elapsed(),
                ))
            })();
            tx.send(result).unwrap_or_else(|_| {
                tracing::warn!("Query runner didn't wait for the result");
            })
        });
        rx.await.unwrap_or_else(|_| {
            Err(QueryError::Other(anyhow::anyhow!(
                "Query processor didn't produce a result"
            )))
        })
    }
}
