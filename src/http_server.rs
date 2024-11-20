use std::sync::Arc;

use crate::metrics;

use axum::{
    http::{header, HeaderMap},
    response::IntoResponse,
    routing::get,
    Json,
};
use prometheus_client::{encoding::text::encode, registry::Registry};
use sqd_network_transport::PeerId;
use tokio_util::sync::CancellationToken;

async fn get_status() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "state": {
            "available": metrics::CHUNKS_AVAILABLE.get(),
            "downloading": metrics::CHUNKS_DOWNLOADING.get(),
        }
    }))
}

async fn get_peer_id(peer_id: PeerId) -> String {
    peer_id.to_string()
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
    pub fn new(peer_id: PeerId, metrics_registry: Registry) -> Self {
        let metrics_registry = Arc::new(metrics_registry);
        let router = axum::Router::new()
            .route("/worker/status", get(get_status))
            .route("/worker/peer-id", get(move || get_peer_id(peer_id)))
            .route("/metrics", get(move || get_metrics(metrics_registry)));
        let router = Self::add_common_layers(router);
        Self { router }
    }

    pub async fn run(self, port: u16, cancellation_token: CancellationToken) -> anyhow::Result<()> {
        let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
        axum::serve(listener, self.router)
            .with_graceful_shutdown(cancellation_token.cancelled_owned())
            .await?;
        tracing::info!("HTTP server finished");
        Ok(())
    }

    fn add_common_layers(router: axum::Router) -> axum::Router {
        router
            .layer(sentry_tower::NewSentryLayer::new_from_top())
            .layer(sentry_tower::SentryHttpLayer::with_transaction())
            .layer(tower_http::catch_panic::CatchPanicLayer::new())
    }
}
