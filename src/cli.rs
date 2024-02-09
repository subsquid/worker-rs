use std::{path::PathBuf, time::Duration};

use anyhow::Result;
use clap::Parser;
use subsquid_network_transport::cli::TransportArgs;

#[derive(Parser)]
pub struct Args {
    /// Directory to keep in the data and state of this worker (defaults to cwd)
    #[clap(
        long,
        value_name = "DIR",
        default_value = ".",
        hide_default_value(true)
    )]
    pub data_dir: PathBuf,

    #[command(subcommand)]
    pub mode: Mode,

    #[clap(env, hide(true))]
    pub aws_access_key_id: Option<String>,

    #[clap(env, hide(true))]
    pub aws_secret_access_key: Option<String>,

    #[clap(env, hide(true))]
    pub aws_s3_endpoint: Option<String>,

    #[clap(env, hide(true), default_value = "auto")]
    pub aws_region: String,

    #[clap(env, hide(true), default_value_t = 3)]
    pub concurrent_downloads: usize,

    #[clap(env, hide(true), value_parser=parse_seconds, default_value = "3")]
    pub ping_interval_sec: Duration,

    #[clap(env, hide(true))]
    pub sentry_dsn: Option<String>,
}

#[derive(clap::Args)]
pub struct HttpArgs {
    /// URL of the router to connect to
    #[clap(long, value_name = "URL")]
    pub router: String,

    /// Unique id of this worker
    #[clap(long, value_name = "UID")]
    pub worker_id: String,

    /// Externally visible URL of this worker
    #[clap(long, value_name = "URL")]
    pub worker_url: String,

    /// Port to listen on
    #[clap(short, long, default_value_t = 8000)]
    pub port: u16,
}

#[derive(clap::Subcommand)]
pub enum Mode {
    Http(HttpArgs),
    P2P {
        /// Peer ID of the scheduler
        #[clap(long)]
        scheduler_id: String,

        #[command(flatten)]
        transport: TransportArgs,
    },
}

fn parse_seconds(s: &str) -> Result<Duration> {
    Ok(Duration::from_secs(s.parse()?))
}
