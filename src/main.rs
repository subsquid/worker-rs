// SQD worker, a core part of the SQD network.
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

use std::borrow::Cow;
use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use prometheus_client::metrics::info::Info;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};

use sqd_network_transport::{get_agent_info, AgentInfo, P2PTransportBuilder};

use crate::cli::Args;
use crate::controller::p2p::create_p2p_controller;
use crate::controller::worker::Worker;
use crate::http_server::Server as HttpServer;
use crate::storage::manager::StateManager;

mod cli;
mod compute_units;
mod controller;
mod http_server;
mod logs_storage;
mod metrics;
mod query;
mod storage;
mod types;
mod util;

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

// This function is guaranteed to run before any other threads are spawned.
fn init_single_threaded(args: &Args) -> anyhow::Result<()> {
    // Some internal libraries use the global pool for their operations. This pool's parallelism
    // is initialized with the value of the environment variable. Hence, the only way to guarantee
    // the desired parallelism is to set the environment variable from the code.
    let threads = args.query_threads.unwrap_or_else(|| {
        std::thread::available_parallelism()
            .map(|x| x.get())
            .unwrap_or(1)
    });
    unsafe {
        std::env::set_var("POLARS_MAX_THREADS", threads.to_string());
    }
    Ok(())
}

fn setup_tracing(args: &Args) -> Result<()> {
    let env_filter = tracing_subscriber::EnvFilter::builder().parse_lossy(
        std::env::var(tracing_subscriber::EnvFilter::DEFAULT_ENV).unwrap_or("info".to_string()),
    );
    let fmt = tracing_subscriber::fmt::layer()
        .compact()
        .with_span_events(if args.log_span_durations {
            FmtSpan::CLOSE
        } else {
            FmtSpan::NONE
        })
        .with_filter(env_filter);
    tracing_subscriber::registry()
        .with(fmt)
        .with(sentry::integrations::tracing::layer())
        .try_init()?;
    Ok(())
}

fn setup_sentry(args: &Args, peer_id: String) -> Option<sentry::ClientInitGuard> {
    args.sentry_dsn.as_ref().map(|dsn| {
        sentry::init((
            dsn.as_str(),
            sentry::ClientOptions {
                release: sentry::release_name!(),
                environment: Some(args.transport.rpc.network.to_string().into()),
                server_name: Some(peer_id.into()),
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

async fn run(mut args: Args) -> anyhow::Result<()> {
    setup_tracing(&args)?;
    args.fill_defaults(); // tracing should be initialized at this point
    let args_clone = args.clone();

    let state_manager =
        StateManager::new(args.data_dir.join("worker"), args.concurrent_downloads).await?;

    let agent_info = get_agent_info!();
    let transport_builder = P2PTransportBuilder::from_cli(args.transport, agent_info).await?;
    let peer_id = transport_builder.local_peer_id();
    let info = Info::new(vec![(
        "version".to_owned(),
        env!("CARGO_PKG_VERSION").to_owned(),
    )]);
    let mut metrics_registry = prometheus_client::registry::Registry::with_labels(
        [(Cow::Borrowed("worker_id"), Cow::Owned(peer_id.to_string()))].into_iter(),
    );
    sqd_network_transport::metrics::register_metrics(&mut metrics_registry);
    metrics::register_metrics(&mut metrics_registry, info);

    let _sentry_guard = setup_sentry(&args_clone, peer_id.to_string());

    let worker = Arc::new(Worker::new(state_manager, args.parallel_queries));

    let cancellation_token = create_cancellation_token()?;
    let controller_fut = async {
        let controller = tokio::select! {
            controller = create_p2p_controller(worker, transport_builder, args_clone) => Arc::new(controller?),
            _ = cancellation_token.cancelled() => return Ok(()),
        };
        controller.run(cancellation_token.clone()).await;
        anyhow::Ok(())
    };

    let (controller_result, server_result) = run_all!(
        cancellation_token,
        controller_fut,
        tokio::spawn(
            HttpServer::new(peer_id, metrics_registry)
                .run(args.prometheus_port, cancellation_token.child_token())
        ),
    );
    controller_result?;
    server_result??;

    tracing::info!("Shutting down");
    Ok(())
}

fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();
    let args = Args::parse();

    init_single_threaded(&args)?;

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(run(args))
}
