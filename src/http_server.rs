use std::sync::Arc;

use crate::{cli::HttpArgs, controller::worker::Worker, types::dataset::Dataset};

use axum::{
    extract::Path,
    http::{header, HeaderMap},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json,
};
use prometheus_client::{encoding::text::encode, registry::Registry};
use reqwest::StatusCode;
use tokio_util::sync::CancellationToken;

async fn get_status(worker: Arc<Worker>, args: Option<HttpArgs>) -> Json<serde_json::Value> {
    let status = worker.status();
    match args {
        Some(args) => Json(serde_json::json!({
            "router_url": args.router,
            "worker_id": args.worker_id,
            "worker_url": args.worker_url,
            "state": {
                "available": status.available,
                "downloading": status.downloading,
            }
        })),
        None => Json(serde_json::json!({
            "state": {
                "available": status.available,
                "downloading": status.downloading,
            }
        })),
    }
}

async fn get_peer_id(worker: Arc<Worker>) -> (StatusCode, String) {
    match worker.peer_id {
        Some(peer_id) => (StatusCode::OK, peer_id.to_string()),
        None => (StatusCode::NOT_FOUND, "".to_owned()),
    }
}

async fn run_query(
    worker: Arc<Worker>,
    Path(dataset): Path<Dataset>,
    query_str: String,
) -> Response {
    if let Some(future) = worker.schedule_query(query_str, dataset, None) {
        future.await.map(|result| result.raw_data).into_response()
    } else {
        Response::builder()
            .status(529)
            .body("Worker is overloaded".into())
            .unwrap()
    }
}

async fn get_metrics(registry: Arc<Registry>) -> impl IntoResponse {
    lazy_static::lazy_static! {
        static ref HEADERS: HeaderMap = {
            let mut headers = HeaderMap::new();
            headers.insert(
                header::CONTENT_TYPE,
                "application/openmetrics-text; version=1.0.0; charset=utf-8"
                    .parse()
                    .unwrap(),
            );
            headers
        };
    }

    let mut buffer = String::new();
    encode(&mut buffer, &registry).unwrap();

    (HEADERS.clone(), buffer)
}

pub struct Server {
    router: axum::Router,
}

impl Server {
    pub fn new(worker: Arc<Worker>, args: Option<HttpArgs>, metrics_registry: Registry) -> Self {
        let metrics_registry = Arc::new(metrics_registry);
        let router = axum::Router::new()
            .route(
                "/worker/status",
                get({
                    let worker = worker.clone();
                    move || get_status(worker, args)
                }),
            )
            .route(
                "/worker/peer-id",
                get({
                    let worker = worker.clone();
                    move || get_peer_id(worker)
                }),
            )
            .route(
                "/query/:dataset",
                post({
                    let worker = worker.clone();
                    move |path, body| run_query(worker, path, body)
                }),
            )
            .route("/metrics", get(move || get_metrics(metrics_registry)));
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
