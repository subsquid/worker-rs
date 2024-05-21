use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use prometheus_client::metrics::info::Info;
use subsquid_network_transport::P2PTransportBuilder;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

use subsquid_worker::cli::{self, Args, P2PArgs};
use subsquid_worker::controller::Worker;
use subsquid_worker::gateway_allocations::allocations_checker::{self, AllocationsChecker};
use subsquid_worker::http_server::Server as HttpServer;
use subsquid_worker::metrics;
use subsquid_worker::storage::manager::StateManager;
use subsquid_worker::transport::http::HttpTransport;
use subsquid_worker::transport::p2p::create_p2p_transport;

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
    args.sentry_dsn.as_ref().map(|dsn| {
        sentry::init((
            dsn.as_str(),
            sentry::ClientOptions {
                release: sentry::release_name!(),
                traces_sample_rate: args.sentry_traces_sample_rate,
                ..Default::default()
            },
        ))
    })
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
    dotenv::dotenv().ok();
    let args = Args::parse();
    setup_tracing()?;
    let _sentry_guard = setup_sentry(&args);

    let mut metrics_registry = Default::default();

    let state_manager = Arc::new(StateManager::new(args.data_dir.join("worker")).await?);

    let cancellation_token = create_cancellation_token()?;

    match args.mode {
        cli::Mode::Http(http_args) => {
            let info = Info::new(vec![(
                "version".to_owned(),
                env!("CARGO_PKG_VERSION").to_owned(),
            )]);
            metrics::register_metrics(&mut metrics_registry, info);
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
            let (_, server_result) = tokio::try_join!(
                worker.run(
                    args.ping_interval_sec,
                    cancellation_token.clone(),
                    args.concurrent_downloads,
                ),
                tokio::spawn(
                    HttpServer::with_http(state_manager, http_args, metrics_registry)
                        .run(args.port, cancellation_token.clone()),
                )
            )?;
            server_result?;
        }
        cli::Mode::P2P(P2PArgs {
            scheduler_id,
            logs_collector_id,
            transport: transport_args,
            ..
        }) => {
            subsquid_network_transport::metrics::register_metrics(&mut metrics_registry);
            let transport_builder = P2PTransportBuilder::from_cli(transport_args).await?;
            let contract_client = transport_builder.contract_client();
            let transport = Arc::new(
                create_p2p_transport(
                    transport_builder,
                    scheduler_id,
                    logs_collector_id,
                    args.data_dir,
                )
                .await?,
            );
            let allocations_checker: Arc<dyn AllocationsChecker> = Arc::new(
                allocations_checker::RpcAllocationsChecker::new(
                    contract_client,
                    transport.local_peer_id(),
                )
                .await?,
            );

            let info = Info::new(vec![
                ("version".to_owned(), env!("CARGO_PKG_VERSION").to_owned()),
                ("peer_id".to_owned(), transport.local_peer_id().to_string()),
            ]);
            metrics::register_metrics(&mut metrics_registry, info);
            metrics::register_p2p_metrics(&mut metrics_registry);

            let worker = Worker::new(
                state_manager.clone(),
                transport.clone(),
                allocations_checker,
            );
            let (_, server_result) = tokio::try_join!(
                worker.run(
                    args.ping_interval_sec,
                    cancellation_token.clone(),
                    args.concurrent_downloads,
                ),
                tokio::spawn(
                    HttpServer::with_p2p(metrics_registry)
                        .run(args.port, cancellation_token.clone()),
                )
            )?;
            server_result?;
        }
    };
    Ok(())
}
