# SQD Network Worker (Rust)

A worker node for [SQD Network](https://sqd.dev/network), written in Rust. A worker downloads its assigned slice of the network's data lake from persistent storage (currently S3) and answers data queries that reference those chunks.

This is the Rust implementation of the worker. The previous Python version lives in [subsquid/archive.py](https://github.com/subsquid/archive.py/tree/master).

## What it is

SQD Network is a decentralized data lake. The dataset is split into chunks that are distributed across many worker nodes. Each worker:

- Receives a chunk assignment, then downloads the assigned chunks from persistent storage (S3) into a local data directory.
- Joins the network over a libp2p peer-to-peer transport (QUIC) using a peer ID, and serves queries routed to the chunks it holds.
- Executes queries against its local data using a query engine built on [Polars](https://pola.rs) and the [SQD query crate](https://github.com/subsquid/data).
- Exposes an HTTP endpoint with status and Prometheus metrics.

The crate is published as `sqd-worker`. It depends on shared network crates from [subsquid/sqd-network](https://github.com/subsquid/sqd-network) (transport, messages, contract client, assignments) and query crates from [subsquid/data](https://github.com/subsquid/data).

For protocol details, see the [network RFC](https://github.com/subsquid/specs/tree/main/network-rfc).

## Running a worker

If you want to operate a worker on SQD Network, follow the worker setup guide in the docs:

https://docs.sqd.dev/subsquid-network/participate/worker/

## Build

Requires the Rust toolchain pinned in `rust-toolchain.toml` (Rust 1.89). Native build dependencies: `protobuf-compiler`, `pkg-config`, `libssl-dev`, and `libsqlite3-dev`.

```bash
cargo build --release
```

The binary is produced at `target/release/sqd-worker`.

### Docker

A `Dockerfile` is provided. It builds the worker with `cargo-chef` for layer caching and produces an image whose entrypoint is the worker binary:

```bash
docker build -t sqd-worker .
```

## Usage

The worker is configured through command-line flags or the equivalent environment variables (most flags also read from `env`). Key options:

| Flag | Env | Default | Description |
|---|---|---|---|
| `--data-dir` | `DATA_DIR` | (required) | Directory for the worker's data and state |
| `--prometheus-port` | `PROMETHEUS_PORT` | `8000` | Port for the HTTP status and metrics server |
| `--p2p-port` | `LISTEN_PORT` | `12345` | P2P (QUIC) port to listen on |
| `--public-ip` | `PUBLIC_IP` | (none) | Public IP address to advertise to peers |
| `--parallel-queries` | `PARALLEL_QUERIES` | `20` | Maximum number of queries processed in parallel |
| `--concurrent-downloads` | `CONCURRENT_DOWNLOADS` | `3` | Maximum number of concurrent chunk downloads |
| `--query-threads` | `QUERY_THREADS` | (CPU count) | Threads used by the query engine |
| `--assignment-url` | `ASSIGNMENT_URL` | network-dependent | URL of the chunk assignment / network state |

Network selection and boot nodes come from the transport arguments (see `--help`). When the network is set to `mainnet` or `tethys`, default boot nodes and the assignment URL are filled in automatically.

Run `sqd-worker --help` for the full list of options.

## HTTP endpoints

The HTTP server (on `--prometheus-port`) exposes:

- `GET /worker/status`: JSON reporting the number of chunks available and downloading.
- `GET /worker/peer-id`: the worker's libp2p peer ID.
- `GET /metrics`: Prometheus metrics in OpenMetrics text format.

## Documentation

- SQD docs: https://docs.sqd.dev
- SQD Network: https://docs.sqd.dev/en/network
- Worker setup: https://docs.sqd.dev/subsquid-network/participate/worker/

## License

AGPL-3.0-or-later. See [LICENSE.md](LICENSE.md).
