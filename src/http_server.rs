use std::sync::Arc;

use crate::{
    cli::HttpArgs,
    query::{self, eth::BatchRequest},
    storage::manager::StateManager, types::state::Dataset,
};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json,
};
use datafusion::execution::{context::SessionContext, options::ParquetReadOptions};
use prometheus::{gather, TextEncoder};

async fn get_status(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let status = state.state_manager.current_status().await;
    Json(serde_json::json!({
        "router_url": state.args.router,
        "worker_id": state.args.worker_id,
        "worker_url": state.args.worker_url,
        "state": {
            "available": status.available,
            "downloading": status.downloading,
        }
    }))
}

// TODO: process all chunks, not only the first one
async fn run_query(
    State(state): State<Arc<AppState>>,
    Path(dataset): Path<Dataset>,
    Json(query): Json<BatchRequest>,
) -> Response {
    let path = state
        .state_manager
        .find_chunk(&dataset, (query.from_block as u32).into())
        .await;
    if let Some(path) = path {
        let ctx = prepare_query_context(&path).await.unwrap();
        let result = query::processor::process_query(&ctx, query).await;
        match result {
            Ok(result) => Json(result).into_response(),
            Err(err) => (
                StatusCode::BAD_REQUEST,
                format!("Couldn't execute query: {:?}", err),
            )
                .into_response(),
        }
    } else {
        (
            StatusCode::NOT_FOUND,
            "This worker doesn't have any chunks in requested range",
        )
            .into_response()
    }
}

async fn get_metrics() -> String {
    let encoder = TextEncoder::new();
    encoder
        .encode_to_string(&gather())
        .expect("Failed to encode metrics")
}

pub struct Server {
    router: axum::Router,
    port: u16,
}

pub struct AppState {
    state_manager: Arc<StateManager>,
    args: HttpArgs,
}

impl Server {
    pub fn new(state_manager: Arc<StateManager>, args: HttpArgs) -> Self {
        let port = args.port;
        let router = axum::Router::new()
            .route("/status", get(get_status))
            .route("/query/:dataset", post(run_query))
            .route("/metrics", get(get_metrics))
            .with_state(Arc::new(AppState {
                state_manager,
                args,
            }))
            .layer(tower_http::catch_panic::CatchPanicLayer::new());
        Self { router, port }
    }

    pub async fn run(self) -> anyhow::Result<()> {
        let listener = tokio::net::TcpListener::bind(("0.0.0.0", self.port)).await?;
        axum::serve(listener, self.router).await?;
        Ok(())
    }
}

async fn prepare_query_context(path: &std::path::Path) -> anyhow::Result<SessionContext> {
    let ctx = SessionContext::new();
    ctx.register_parquet(
        "blocks",
        path.join("blocks.parquet").to_string_lossy().as_ref(),
        ParquetReadOptions::default(),
    )
    .await?;
    ctx.register_parquet(
        "transactions",
        path.join("transactions.parquet").to_string_lossy().as_ref(),
        ParquetReadOptions::default(),
    )
    .await?;
    ctx.register_parquet(
        "logs",
        path.join("logs.parquet").to_string_lossy().as_ref(),
        ParquetReadOptions::default(),
    )
    .await?;
    ctx.register_parquet(
        "traces",
        path.join("traces.parquet").to_string_lossy().as_ref(),
        ParquetReadOptions::default(),
    )
    .await?;
    ctx.register_parquet(
        "statediffs",
        path.join("statediffs.parquet").to_string_lossy().as_ref(),
        ParquetReadOptions::default(),
    )
    .await?;
    Ok(ctx)
}
