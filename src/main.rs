mod cli;
mod http_server;
mod query;
mod storage;
mod transport;
mod types;
mod util;

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use clap::Parser;
use cli::Args;
use futures::{Stream, StreamExt};
use http_server::Server;
use storage::{downloader::Downloader, manager::StateManager};
use tracing::{instrument, warn};
use tracing_subscriber::{filter, layer::SubscriberExt, util::SubscriberInitExt, Layer};
use transport::{http::HttpTransport, p2p::P2PTransport, Transport};
use types::state::Ranges;

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

async fn ping_forever(
    state_manager: Arc<StateManager>,
    transport: Arc<impl Transport>,
    interval: Duration,
) {
    loop {
        let status = state_manager.current_status().await;
        let result = transport
            .send_ping(transport::State {
                datasets: status.available,
            })
            .await;
        if let Err(err) = result {
            warn!("Couldn't send ping: {:?}", err);
        }
        tokio::time::sleep(interval).await;
    }
}

async fn handle_updates_forever(
    state_manager: Arc<StateManager>,
    mut updates: impl Stream<Item = Ranges> + Unpin,
) {
    while let Some(ranges) = updates.next().await {
        let result = state_manager.set_desired_ranges(ranges).await;
        if let Err(err) = result {
            warn!("Couldn't schedule update: {:?}", err)
        }
    }
    unreachable!("Updates receiver closed unexpectedly");
}

#[instrument(skip(state_manager, transport))]
async fn process_assignments(
    state_manager: Arc<StateManager>,
    transport: Arc<impl Transport>,
    interval: Duration,
) {
    let state_updates = transport.stream_assignments();
    tokio::pin!(state_updates);
    futures::join!(
        ping_forever(state_manager.clone(), transport, interval),
        handle_updates_forever(state_manager, state_updates),
    );
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
            tokio::spawn(process_assignments(
                state_manager.clone(),
                transport,
                args.ping_interval_sec,
            ));
            // TODO: move it to HttpTransport
            Server::new(state_manager, http_args).run().await?;
        }
        cli::Mode::P2P {
            scheduler_id,
            transport: transport_args,
        } => {
            let transport = Arc::new(P2PTransport::from_cli(transport_args, scheduler_id).await?);
            let copy = transport.clone();
            tokio::spawn(process_assignments(
                state_manager.clone(),
                copy,
                args.ping_interval_sec,
            ));
            transport.run().await;
        }
    };
    Ok(())
}
