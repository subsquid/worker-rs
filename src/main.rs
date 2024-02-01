mod cli;
mod query;
mod storage;
mod transport;
mod types;
mod util;

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use clap::Parser;
use cli::Args;
use futures::StreamExt;
use storage::{downloader::Downloader, manager::StateManager};
use tracing::warn;
use tracing_subscriber::{filter, layer::SubscriberExt, util::SubscriberInitExt, Layer};
use transport::{http::HttpTransport, Transport};

fn setup_tracing() -> Result<()> {
    opentelemetry::global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
    let tracer = opentelemetry_jaeger::new_agent_pipeline()
        .with_service_name("archive-rust")
        .install_simple()?;
    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);

    let fmt = tracing_subscriber::fmt::layer()
        .compact()
        .with_filter(filter::LevelFilter::INFO);
    tracing_subscriber::registry()
        .with(fmt)
        .with(opentelemetry)
        .try_init()?;
    Ok(())
}

fn ping_forever(
    state_manager: Arc<StateManager>,
    transport: impl Transport + 'static,
    interval: Duration,
) {
    tokio::spawn(async move {
        loop {
            let status = state_manager.current_status().await;
            let result = transport
                .send_ping(transport::State {
                    ranges: status.available,
                })
                .await;
            if let Err(err) = result {
                warn!("Couldn't send ping: {:?}", err);
            }
            tokio::time::sleep(interval).await;
        }
    });
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    setup_tracing()?;

    let downloader = Downloader::new(args.concurrent_downloads * 4);
    let state_manager =
        StateManager::new(args.data_dir, downloader, args.concurrent_downloads).await?;
    let mut transport = HttpTransport::new(args.worker_id, args.worker_url, args.router);
    let mut state_updates = transport.subscribe_to_updates();

    ping_forever(state_manager.clone(), transport, args.ping_interval_sec);

    while let Some(ranges) = state_updates.next().await {
        state_manager.set_desired_ranges(ranges).await?;
    }
    unreachable!("Updates receiver closed unexpectedly");
}
