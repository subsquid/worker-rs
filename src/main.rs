use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use tokio_util::sync::CancellationToken;
use tracing::warn;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

use worker_rust::cli::{self, Args, P2PArgs};
use worker_rust::controller::Worker;
use worker_rust::gateway_allocations::allocations_checker::{self, AllocationsChecker};
use worker_rust::http_server::Server as HttpServer;
use worker_rust::storage::manager::StateManager;
use worker_rust::transport::http::HttpTransport;
use worker_rust::transport::p2p::create_p2p_transport;

fn setup_tracing() -> Result<()> {
    let fmt = tracing_subscriber::fmt::layer()
        .compact()
        .with_filter(EnvFilter::from_default_env());
    tracing_subscriber::registry()
        .with(fmt)
        .with(sentry::integrations::tracing::layer())
        .try_init()?;
    Ok(())
}

fn setup_sentry(args: &Args) -> Option<sentry::ClientInitGuard> {
    if let Some(dsn) = &args.sentry_dsn {
        Some(sentry::init((
            dsn.as_str(),
            sentry::ClientOptions {
                release: sentry::release_name!(),
                traces_sample_rate: 1.0,
                ..Default::default()
            },
        )))
    } else {
        None
    }
}

fn create_cancellation_token() -> Result<CancellationToken> {
    use tokio::signal::unix::{signal, SignalKind};

    let token = CancellationToken::new();
    let copy = token.clone();
    let mut sigint = signal(SignalKind::interrupt())?;
    let mut sigterm = signal(SignalKind::terminate())?;
    tokio::spawn(async move {
        tokio::select!(
            _ = sigint.recv() => {
                copy.cancel();
            },
            _ = sigterm.recv() => {
                copy.cancel();
            },
        );
    });
    Ok(token)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    setup_tracing()?;
    let _sentry_guard = setup_sentry(&args);

    let state_manager = Arc::new(StateManager::new(args.data_dir.clone()).await?);

    let cancellation_token = create_cancellation_token()?;

    match args.mode {
        cli::Mode::Http(http_args) => {
            let transport = Arc::new(HttpTransport::new(
                http_args.worker_id.clone(),
                http_args.worker_url.clone(),
                http_args.router.clone(),
            ));
            let worker = Worker::new(
                state_manager.clone(),
                transport,
                Arc::new(allocations_checker::NoopAllocationsChecker {}),
            );
            let worker_future = worker.run(
                args.ping_interval_sec,
                cancellation_token.clone(),
                args.concurrent_downloads,
            );
            let server_future = tokio::spawn(
                HttpServer::new(state_manager, http_args).run(cancellation_token.clone()),
            );
            let result = worker_future.await;
            server_future.await??;
            result?;
        }
        cli::Mode::P2P(P2PArgs {
            scheduler_id,
            logs_collector_id,
            transport: transport_args,
            rpc,
            ..
        }) => {
            let transport = Arc::new(
                create_p2p_transport(
                    transport_args,
                    scheduler_id,
                    logs_collector_id,
                    args.data_dir.join("logs.db"),
                )
                .await?,
            );
            let allocations_checker: Arc<dyn AllocationsChecker> = if let Some(rpc) = rpc {
                Arc::new(
                    allocations_checker::RpcAllocationsChecker::new(
                        &rpc,
                        transport.local_peer_id(),
                    )
                    .await?,
                )
            } else {
                warn!("RPC endpoint was not provided. Skipping gateway allocations checks");
                Arc::new(allocations_checker::NoopAllocationsChecker {})
            };
            let worker = Worker::new(
                state_manager.clone(),
                transport.clone(),
                allocations_checker,
            );
            worker
                .run(
                    args.ping_interval_sec,
                    cancellation_token,
                    args.concurrent_downloads,
                )
                .await?;
        }
    };
    Ok(())
}
