use std::sync::Arc;

use crate::{
    cli::HttpArgs, controller, query::eth::BatchRequest, storage::manager::StateManager,
    types::dataset::Dataset,
};

use axum::{
    extract::Path,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json,
};
use prometheus::{gather, TextEncoder};
use tokio_util::sync::CancellationToken;

async fn get_status(state_manager: Arc<StateManager>, args: HttpArgs) -> Json<serde_json::Value> {
    let status = state_manager.current_status();
    Json(serde_json::json!({
        "router_url": args.router,
        "worker_id": args.worker_id,
        "worker_url": args.worker_url,
        "state": {
            "available": status.available,
            "downloading": status.downloading,
        }
    }))
}

async fn run_query(
    state_manager: Arc<StateManager>,
    Path(dataset): Path<Dataset>,
    Json(query): Json<BatchRequest>,
) -> Response {
    controller::run_query(state_manager, query, dataset)
        .await
        .map(|result| result.raw_data)
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
}

impl Server {
    pub fn with_http(state_manager: Arc<StateManager>, args: HttpArgs) -> Self {
        let router = axum::Router::new()
            .route(
                "/status",
                get({
                    let state_manager = state_manager.clone();
                    move || get_status(state_manager, args)
                }),
            )
            .route(
                "/query/:dataset",
                post({
                    let state_manager = state_manager.clone();
                    move |path, body| run_query(state_manager, path, body)
                }),
            )
            .route("/metrics", get(get_metrics));
        let router = Self::add_common_layers(router);
        Self { router }
    }

    pub fn with_p2p() -> Self {
        let router = axum::Router::new()
            .route("/metrics", get(get_metrics));
        let router = Self::add_common_layers(router);
        Self { router }
    }

    pub async fn run(self, port: u16, cancellation_token: CancellationToken) -> anyhow::Result<()> {
        let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
        axum::serve(listener, self.router)
            .with_graceful_shutdown(cancellation_token.cancelled_owned())
            .await?;
        Ok(())
    }

    fn add_common_layers(router: axum::Router) -> axum::Router {
        router
            .layer(sentry_tower::NewSentryLayer::new_from_top())
            .layer(sentry_tower::SentryHttpLayer::with_transaction())
            .layer(tower_http::catch_panic::CatchPanicLayer::new())
    }
}
