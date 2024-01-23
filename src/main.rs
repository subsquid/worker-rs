mod controller;
mod query;
mod storage;
mod transport;
mod types;
mod util;

use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
};

use anyhow::Result;
use futures::StreamExt;
use itertools::Itertools;
use lazy_static::lazy_static;
use storage::{
    downloader::Downloader, layout, layout::DataChunk, manager::StateManager, s3_fs::S3Filesystem,
};
use tracing::instrument;
use tracing_subscriber::{filter, layer::SubscriberExt, util::SubscriberInitExt, Layer};

lazy_static! {
    static ref CONCURRENCY: usize = std::env::var("CONCURRENT_DOWNLOADS")
        .ok()
        .and_then(|x| x.parse().ok())
        .unwrap_or(3);
    static ref CHUNKS: usize = std::env::var("CHUNKS")
        .ok()
        .and_then(|x| x.parse().ok())
        .unwrap_or(5);
}

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

#[instrument(ret)]
async fn run() -> Result<()> {
    let downloader = Downloader::new(CONCURRENCY.to_owned() * 4);
    let rfs = S3Filesystem::with_bucket("arbitrum-goerli")?;
    // eprintln!("{:?}", rfs.ls("0000000000/").await);
    let chunks: Vec<DataChunk> = layout::stream_chunks(&rfs, None, None)
        .take(CHUNKS.to_owned())
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .try_collect()?;
    let state = StateManager::new(PathBuf::from("tmp"), downloader, CONCURRENCY.to_owned()).await?;
    state
        .set_desired_state(HashMap::from_iter([(
            "arbitrum-goerli".to_owned(),
            HashSet::from_iter(chunks),
        )]))
        .await;
    state.wait_sync().await;
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_tracing()?;
    run().await
}
