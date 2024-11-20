use std::time::Duration;

use anyhow::Result;
use camino::Utf8PathBuf as PathBuf;
use clap::Parser;
use sqd_network_transport::TransportArgs;

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

    #[clap(long, env)]
    pub query_threads: Option<usize>,

    #[clap(env = "HEARTBEAT_INTERVAL_SEC", hide(true), value_parser=parse_seconds, default_value = "55", alias = "ping-interval")]
    pub heartbeat_interval: Duration,

    #[clap(env = "NETWORK_POLLING_INTERVAL_SEC", hide(true), value_parser=parse_seconds, default_value = "30"
    )]
    pub network_polling_interval: Duration,

    #[clap(env = "ASSIGNMENT_CHECK_INTERVAL_SEC", hide(true), value_parser=parse_seconds, default_value = "60")]
    pub assignment_check_interval: Duration,

    #[command(flatten)]
    pub transport: TransportArgs,

    #[clap(env, hide(true))]
    pub sentry_dsn: Option<String>,

    #[clap(env, hide(true), default_value_t = 0.001)]
    pub sentry_traces_sample_rate: f32,
}

fn parse_seconds(s: &str) -> Result<Duration> {
    Ok(Duration::from_secs(s.parse()?))
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
                        "12D3KooWS5N8ygU6fRy4EZtzdHf4QZnkCaZrwCha9eYKH3LwNvsP /dns4/mainnet.subsquid.io/udp/32445/quic-v1".parse().unwrap(),
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

        if self.sentry_dsn.is_none() {
            self.sentry_dsn = Some("https://f97ffef7e96eb533d4c527ce62e4cfbf@o1149243.ingest.us.sentry.io/4507056936779776".to_string());
        }
    }
}
