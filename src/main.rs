mod controller;
mod query;
mod storage;
mod transport;
mod types;
mod util;

use anyhow::Result;
use futures::StreamExt;
use itertools::Itertools;
use lazy_static::lazy_static;
use opentelemetry::global;
use std::path::PathBuf;
use storage::{downloader::Downloader, layout, s3_fs::S3Filesystem};
use tracing::instrument;
use tracing_subscriber::{
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

lazy_static! {
    static ref CONCURRENCY: usize = std::env::var("CONCURRENT_DOWNLOADS")
        .ok()
        .and_then(|x| x.parse().ok())
        .unwrap_or(3);
}

fn setup_tracing() -> Result<()> {
    global::set_text_map_propagator(opentelemetry_jaeger::Propagator::new());
    let tracer = opentelemetry_jaeger::new_agent_pipeline()
        .with_service_name("archive-rust")
        .install_simple()?;
    let opentelemetry = tracing_opentelemetry::layer().with_tracer(tracer);
    tracing_subscriber::registry()
        .with(opentelemetry)
        .try_init()?;
    Ok(())
}

#[instrument(ret)]
async fn run() -> Result<()> {
    let downloader = Downloader::new(CONCURRENCY.to_owned());
    let rfs = S3Filesystem::with_bucket("arbitrum-goerli")?;
    // eprintln!("{:?}", rfs.ls("0000000000/").await);
    let chunks: Vec<_> = layout::stream_chunks(&rfs, None, None)
        .take(5)
        .collect()
        .await;
    let mut futures = Vec::new();
    for chunk in chunks {
        let chunk = chunk?;
        futures.push(downloader.download_dir(
            &rfs,
            chunk.path(),
            PathBuf::from("tmp").join(chunk.path()),
        ));
    }
    let results = futures::future::join_all(futures).await;
    results.into_iter().try_collect()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_tracing()?;
    run().await
}
