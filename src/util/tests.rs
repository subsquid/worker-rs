use anyhow::Result;
use tracing_subscriber::EnvFilter;

pub fn setup_tracing() -> Result<()> {
    tracing_subscriber::fmt()
        .compact()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    Ok(())
}
