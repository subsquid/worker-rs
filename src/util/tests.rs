use anyhow::Result;
use tracing_subscriber::filter;

pub fn setup_tracing() -> Result<()> {
    tracing_subscriber::fmt()
        .compact()
        .with_max_level(filter::LevelFilter::INFO)
        .init();
    Ok(())
}
