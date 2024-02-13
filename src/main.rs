mod cli;
mod controller;
mod http_server;
mod query;
mod storage;
mod transport;
mod types;
mod util;

use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use cli::Args;
use controller::Worker;
use http_server::Server;
use storage::{downloader::Downloader, manager::StateManager};
use tracing_subscriber::{filter, layer::SubscriberExt, util::SubscriberInitExt, Layer};
use transport::{http::HttpTransport, p2p::P2PTransport};

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
        .with(sentry::integrations::tracing::layer())
        .try_init()?;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    setup_tracing()?;

    let _sentry_guard;
    if let Some(sentry_dsn) = &args.sentry_dsn {
        _sentry_guard = sentry::init((sentry_dsn.as_str(), sentry::ClientOptions {
                release: sentry::release_name!(),
                traces_sample_rate: 1.0,
            ..Default::default()
        }));
    }

    let downloader = Downloader::new(args.concurrent_downloads * 4);
    let state_manager =
        StateManager::new(args.data_dir.clone(), downloader, args.concurrent_downloads).await?;

    match args.mode {
        cli::Mode::Http(http_args) => {
            let transport = Arc::new(HttpTransport::new(
                http_args.worker_id.clone(),
                http_args.worker_url.clone(),
                http_args.router.clone(),
            ));
            let worker = Worker::new(state_manager.clone(), transport);
            tokio::select!(
                result = worker.run(args.ping_interval_sec) => {
                    tracing::error!("Worker exited");
                    result.map_err(From::from)
                },
                result = Server::new(state_manager, http_args).run() => {
                    tracing::error!("HTTP server routine exited");
                    result
                }
            )?;
        }
        cli::Mode::P2P {
            scheduler_id,
            transport: transport_args,
        } => {
            let transport = Arc::new(P2PTransport::from_cli(transport_args, scheduler_id).await?);
            let worker = Worker::new(state_manager.clone(), transport.clone());
            worker.run(args.ping_interval_sec).await?;
        }
    };
    Ok(())
}
