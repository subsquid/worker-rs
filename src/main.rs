// Subsquid worker, a core part of the Subsquid network.
// Copyright (C) 2024 Subsquid Labs GmbH

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.

// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use prometheus_client::metrics::info::Info;
use subsquid_network_transport::P2PTransportBuilder;
use subsquid_worker::controller::http::HttpController;
use subsquid_worker::controller::p2p::create_p2p_controller;
use subsquid_worker::controller::worker::Worker;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

use subsquid_worker::cli::{self, Args, P2PArgs};
use subsquid_worker::gateway_allocations::allocations_checker;
use subsquid_worker::http_server::Server as HttpServer;
use subsquid_worker::metrics;
use subsquid_worker::storage::manager::StateManager;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

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

    let state_manager =
        StateManager::new(args.data_dir.join("worker"), args.concurrent_downloads).await?;

    let cancellation_token = create_cancellation_token()?;

    match args.mode {
        cli::Mode::Http(http_args) => {
            let info = Info::new(vec![(
                "version".to_owned(),
                env!("CARGO_PKG_VERSION").to_owned(),
            )]);
            metrics::register_metrics(&mut metrics_registry, info);
            let worker = Arc::new(Worker::new(state_manager));
            let controller = HttpController::new(
                worker.clone(),
                args.ping_interval,
                http_args.worker_id.clone(),
                http_args.worker_url.clone(),
                http_args.router.clone(),
            );
            let (_, server_result) = tokio::join!(
                controller.run(cancellation_token.clone()),
                tokio::spawn(
                    HttpServer::new(worker, Some(http_args), metrics_registry)
                        .run(args.port, cancellation_token.clone()),
                )
            );
            server_result??;
        }
        cli::Mode::P2P(P2PArgs {
            scheduler_id,
            logs_collector_id,
            network_polling_interval,
            transport: transport_args,
            ..
        }) => {
            subsquid_network_transport::metrics::register_metrics(&mut metrics_registry);
            let transport_builder = P2PTransportBuilder::from_cli(transport_args).await?;
            let peer_id = transport_builder.local_peer_id();
            let info = Info::new(vec![
                ("version".to_owned(), env!("CARGO_PKG_VERSION").to_owned()),
                ("peer_id".to_owned(), peer_id.to_string()),
            ]);
            metrics::register_metrics(&mut metrics_registry, info);
            metrics::register_p2p_metrics(&mut metrics_registry);

            let allocations_checker = allocations_checker::AllocationsChecker::new(
                transport_builder.contract_client(),
                peer_id,
                network_polling_interval,
            )
            .await?;
            let worker = Arc::new(Worker::new(state_manager).with_peer_id(peer_id));

            let controller_fut = async {
                tokio::select! {
                    _ = cancellation_token.cancelled() => {
                    },
                    controller = create_p2p_controller(
                        worker.clone(),
                        transport_builder,
                        allocations_checker,
                        scheduler_id,
                        logs_collector_id,
                        args.data_dir,
                        args.ping_interval,
                    ) => {
                        controller?.run(cancellation_token.clone()).await;
                    }
                }
                anyhow::Ok(())
            };

            let (_, server_result) = tokio::join!(
                controller_fut,
                tokio::spawn(
                    HttpServer::new(worker.clone(), None, metrics_registry)
                        .run(args.port, cancellation_token.clone()),
                )
            );
            server_result??;
        }
    };
    Ok(())
}
