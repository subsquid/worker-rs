use std::time::Duration;

use anyhow::Result;
use camino::Utf8PathBuf as PathBuf;
use clap::Parser;
use sqd_assignments::AssignmentType;
use sqd_network_transport::TransportArgs;

/// Default for `--max-download-attempts`: how many times a chunk download may
/// fail before the worker gives up on it until the next assignment. Attempts
/// are spaced by the downloader's global backoff, so the budget is not burned
/// in a tight loop.
pub const DEFAULT_MAX_DOWNLOAD_ATTEMPTS: u8 = 5;

#[derive(Parser, Clone)]
#[command(version)]
pub struct Args {
    /// Directory to keep in the data and state of this worker (defaults to cwd)
    #[clap(long, env, value_name = "DIR")]
    pub data_dir: PathBuf,

    /// Port to listen on
    #[clap(short, long, env, default_value_t = 8000, alias = "port")]
    pub prometheus_port: u16,

    /// P2P port to listen on
    #[clap(long, env = "LISTEN_PORT", default_value_t = 12345)]
    pub p2p_port: u16,

    // Optional public IP address to advertise
    #[clap(long, env)]
    pub public_ip: Option<String>,

    #[clap(long, env, default_value_t = 20)]
    pub parallel_queries: usize,

    #[clap(long, env, default_value_t = 3)]
    pub concurrent_downloads: usize,

    #[clap(long, env, hide(true), value_parser=parse_seconds, default_value = "60")]
    pub s3_timeout: Duration,

    #[clap(long, env, hide(true), value_parser=parse_seconds, default_value = "3")]
    pub s3_read_timeout: Duration,

    #[clap(long, env = "DOWNLOADS_MAX_DELAY_SEC", hide(true), value_parser=parse_seconds, default_value = "300")]
    pub downloads_max_delay: Duration,

    /// How many times a chunk download may fail before it is given up on
    /// until the next assignment
    #[clap(long, env, hide(true), default_value_t = DEFAULT_MAX_DOWNLOAD_ATTEMPTS, value_parser = clap::value_parser!(u8).range(1..))]
    pub max_download_attempts: u8,

    #[clap(long, env)]
    pub query_threads: Option<usize>,

    #[clap(long, env, default_value = "")]
    pub assignment_url: String,

    /// Overrides the type the network state names itself (IB-40); unset, the state decides.
    #[clap(long, env, hide(true), value_parser = parse_assignment_type)]
    pub assignment_source: Option<AssignmentType>,

    /// URL of the query schemas manifest for the experimental query engine
    /// (defaults to a per-network CDN location)
    #[clap(long, env, default_value = "")]
    pub query_schemas_url: String,

    #[clap(long, env = "QUERY_SCHEMAS_REFRESH_INTERVAL_SEC", hide(true), value_parser=parse_seconds, default_value = "3600")]
    pub query_schemas_refresh_interval: Duration,

    /// Share of legacy-engine queries, in percent, that the new query engine
    /// shadows: the chunk is answered again by both engines in the background
    /// and the answers and timings compared. 0 starts no shadow threads.
    #[clap(long, env, hide(true), default_value_t = 0.0, value_parser = parse_percent)]
    pub shadow_percent: f64,

    /// A shadow run is slow, and logged, when the new engine takes more than
    /// this many times the legacy engine's time...
    #[clap(long, env, hide(true), default_value_t = 2.0)]
    pub shadow_slow_ratio: f64,

    /// ...and at least this many milliseconds longer.
    #[clap(long, env = "SHADOW_SLOW_MARGIN_MS", hide(true), value_parser = parse_millis, default_value = "50")]
    pub shadow_slow_margin: Duration,

    /// Shadow runs at once, each on its own thread. A query that finds every
    /// one taken is not shadowed.
    #[clap(long, env, hide(true), default_value_t = 2)]
    pub shadow_max_in_flight: usize,

    /// Shadow log lines for differences, failures and slow runs per 5 minutes,
    /// at most one per query shape; 0 turns them off.
    #[clap(long, env, hide(true), default_value_t = 20)]
    pub shadow_log_limit: usize,

    #[clap(long, env = "NETWORK_POLLING_INTERVAL_SEC", hide(true), value_parser=parse_seconds, default_value = "30"
    )]
    pub network_polling_interval: Duration,

    #[clap(long, env = "ASSIGNMENT_CHECK_INTERVAL_SEC", hide(true), value_parser=parse_seconds, default_value = "60")]
    pub assignment_check_interval: Duration,

    #[clap(long, env = "ASSIGNMENT_CHECK_MAX_DELAY_SEC", hide(true), value_parser=parse_seconds, default_value = "14400")]
    pub assignment_fetch_max_delay: Duration,

    #[clap(long, env = "ASSIGNMENT_FETCH_TIMEOUT_SEC", hide(true), value_parser=parse_seconds, default_value = "300")]
    pub assignment_fetch_timeout: Duration,

    #[clap(long, env, hide(true), default_value_t = false)]
    pub log_span_durations: bool,

    #[command(flatten)]
    pub transport: TransportArgs,

    #[clap(long, env, hide(true))]
    pub sentry_dsn: Option<String>,

    #[clap(long, env, hide(true), default_value_t = 0.001)]
    pub sentry_traces_sample_rate: f32,

    // `long` so the env-only arg isn't silently positional; `ArgAction::Set`
    // (not the bool default `SetTrue`) so SENTRY_IS_ENABLED=false parses as a
    // value instead of counting as "flag present".
    #[clap(long, env, hide(true), default_value_t = true, action = clap::ArgAction::Set)]
    pub sentry_is_enabled: bool,
}

fn parse_seconds(s: &str) -> Result<Duration> {
    Ok(Duration::from_secs(s.parse()?))
}

fn parse_millis(s: &str) -> Result<Duration> {
    Ok(Duration::from_millis(s.parse()?))
}

fn parse_percent(value: &str) -> Result<f64> {
    let percent: f64 = value.parse()?;
    anyhow::ensure!(
        (0.0..=100.0).contains(&percent),
        "{percent} is not between 0 and 100"
    );
    Ok(percent)
}

fn parse_assignment_type(value: &str) -> Result<AssignmentType> {
    match value {
        "legacy" => Ok(AssignmentType::Legacy),
        "split" => Ok(AssignmentType::Split),
        _ => anyhow::bail!("expected \"legacy\" or \"split\", got \"{value}\""),
    }
}

impl Args {
    pub fn fill_defaults(&mut self) {
        match self.transport.rpc.network {
            sqd_contract_client::Network::Tethys => {
                if self.transport.boot_nodes.is_empty() {
                    self.transport.boot_nodes.extend_from_slice(&[
                        "12D3KooWSRvKpvNbsrGbLXGFZV7GYdcrYNh4W2nipwHHMYikzV58 /dns4/testnet.subsquid.io/udp/22445/quic-v1"
                            .parse().unwrap(),
                        "12D3KooWQC9tPzj2ShLn39RFHS5SGbvbP2pEd7bJ61kSW2LwxGSB /dns4/testnet.subsquid.io/udp/22446/quic-v1"
                            .parse().unwrap(),
                    ]);
                }
            }
            sqd_contract_client::Network::Mainnet => {
                if self.transport.boot_nodes.is_empty() {
                    self.transport.boot_nodes.extend_from_slice(&[
                        "12D3KooW9tLMANc4Vnxp27Ypyq8m8mUv45nASahj3eSnMbGWSk1b /dns4/mainnet.subsquid.io/udp/22445/quic-v1".parse().unwrap(),
                        "12D3KooWEhPC7rsHAcifstVwJ3Cj55sWn7zXWuHrtAQUCGhGYnQz /dns4/mainnet.subsquid.io/udp/22446/quic-v1".parse().unwrap(),
                    ]);
                }
            }
        };

        if self.transport.p2p_listen_addrs.is_empty() {
            self.transport.p2p_listen_addrs.push(
                format!("/ip4/0.0.0.0/udp/{}/quic-v1", self.p2p_port)
                    .parse()
                    .unwrap(),
            );
        } else {
            tracing::warn!("Overriding P2P port with P2P_LISTEN_ADDRS");
        }

        if let Some(ip) = self.public_ip.as_ref() {
            if self.transport.p2p_public_addrs.is_empty() {
                self.transport.p2p_public_addrs.push(
                    format!("/ip4/{}/udp/{}/quic-v1", ip, self.p2p_port)
                        .parse()
                        .expect("Invalid public IP"),
                );
            } else {
                tracing::warn!("Overriding provided public IP with P2P_PUBLIC_ADDRS");
            }
        }

        if self.assignment_url.is_empty() {
            match self.transport.rpc.network {
                sqd_contract_client::Network::Mainnet => {
                    self.assignment_url =
                        "https://metadata.sqd-datasets.io/network-state-mainnet.json".to_owned();
                }
                sqd_contract_client::Network::Tethys => {
                    self.assignment_url =
                        "https://metadata.sqd-datasets.io/network-state-tethys.json".to_owned();
                }
            }
        }

        if self.query_schemas_url.is_empty() {
            match self.transport.rpc.network {
                sqd_contract_client::Network::Mainnet => {
                    self.query_schemas_url =
                        "https://cdn.subsquid.io/sqd-network/mainnet/query-schemas-v2.yml"
                            .to_owned();
                }
                sqd_contract_client::Network::Tethys => {
                    self.query_schemas_url =
                        "https://cdn.subsquid.io/sqd-network/testnet/query-schemas-v2.yml"
                            .to_owned();
                }
            }
        }

        if self.sentry_dsn.is_none() {
            self.sentry_dsn = Some("https://f97ffef7e96eb533d4c527ce62e4cfbf@o1149243.ingest.us.sentry.io/4507056936779776".to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(args: &[&str]) -> Result<Args, clap::Error> {
        let base = [
            "sqd-worker",
            "--data-dir",
            "/tmp",
            "--key",
            "/tmp/key",
            "--rpc-url",
            "http://127.0.0.1:1/unused",
            "--l1-rpc-url",
            "http://127.0.0.1:1/unused",
        ];
        Args::try_parse_from(base.into_iter().chain(args.iter().copied()))
    }

    /// The pin is optional — unset, the state decides — and takes the state's own values.
    #[test]
    fn the_assignment_pin_takes_the_wire_vocabulary_or_nothing() {
        assert_eq!(parse(&[]).unwrap().assignment_source, None);
        for (value, pinned) in [
            ("legacy", AssignmentType::Legacy),
            ("split", AssignmentType::Split),
        ] {
            let args = parse(&["--assignment-source", value]).unwrap();
            assert_eq!(args.assignment_source, Some(pinned), "{value}");
        }
        for refused in ["worker", "Legacy", ""] {
            assert!(
                parse(&["--assignment-source", refused]).is_err(),
                "{refused:?}"
            );
        }
    }

    #[test]
    fn the_shadow_is_off_unless_a_share_between_0_and_100_is_given() {
        assert_eq!(parse(&[]).unwrap().shadow_percent, 0.0);
        assert_eq!(
            parse(&["--shadow-percent", "2.5"]).unwrap().shadow_percent,
            2.5
        );
        for refused in ["-1", "100.5", "NaN", "half"] {
            assert!(
                parse(&["--shadow-percent", refused]).is_err(),
                "{refused:?}"
            );
        }
    }
}
