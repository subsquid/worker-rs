use std::sync::Arc;

use crate::{
    cli::HttpArgs, controller, query::eth::BatchRequest, storage::manager::StateManager,
    types::state::Dataset,
};

use axum::{
    extract::{Path, State},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json,
};
use prometheus::{gather, TextEncoder};
use tokio_util::sync::CancellationToken;

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

async fn run_query(
    State(state): State<Arc<AppState>>,
    Path(dataset): Path<Dataset>,
    Json(query): Json<BatchRequest>,
) -> Response {
    controller::run_query(state.state_manager.clone(), query, dataset)
        .await
        .map(|result| Json(result))
        .into_response()
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
            .layer(sentry_tower::NewSentryLayer::new_from_top())
            .layer(sentry_tower::SentryHttpLayer::with_transaction())
            .layer(tower_http::catch_panic::CatchPanicLayer::new());
        Self { router, port }
    }

    pub async fn run(self, cancellation_token: CancellationToken) -> anyhow::Result<()> {
        let listener = tokio::net::TcpListener::bind(("0.0.0.0", self.port)).await?;
        axum::serve(listener, self.router)
            .with_graceful_shutdown(cancellation_token.cancelled_owned())
            .await?;
        Ok(())
    }
}
