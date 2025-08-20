use anyhow::Result;
use camino::Utf8PathBuf as PathBuf;
use tracing_subscriber::EnvFilter;

pub fn tests_data() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/data")
}

pub fn _setup_tracing() -> Result<()> {
    tracing_subscriber::fmt()
        .compact()
        .with_env_filter(EnvFilter::from_default_env())
        .init();
    Ok(())
}
