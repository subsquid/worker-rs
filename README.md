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
| `--assignment-source` | `ASSIGNMENT_SOURCE` | (the network's) | Pins which assignment the worker reads: `legacy` or `split` (see below) |

Network selection and boot nodes come from the transport arguments (see `--help`). When the network is set to `mainnet` or `tethys`, default boot nodes and the assignment URL are filled in automatically.

### Split assignments

The network state names an `assignment_type` — `legacy` or `split` — and the worker reads the
assignments it names, re-reading it on every poll. `--assignment-source` overrides that, which
is how a single worker is switched over ahead of the network, or held back during the migration;
left unset, the network decides and no worker has to be reconfigured to follow it.

Under `split` the worker reads the state's `worker_assignment` pointer and its `schema_bundle`
instead of the legacy shared `assignment`, and never falls back to the legacy one. The state
must also publish a `portal_assignment`: the worker does not read it, but a state that declares
`split` without it is one the network has not finished publishing, and the worker waits — each
such poll counting under `network_state_unresolved{reason}`, so a network stuck that way is
alertable rather than silent. This changes five things:

- **Chunk contents** come from the assignment's inline write-schema rosters, narrowed by each
  chunk's `tables_present` bitmap, rather than from a per-chunk file list.
- **A chunk may be republished.** Each chunk carries a `version`: 0 is the copy ingest wrote,
  anything else a batch job's rewrite, stored by the network under that generation's own prefix.
  The version is part of the chunk's identity, so a rewrite is downloaded rather than assumed to
  be the copy already held, and it is stored under
  `<data-dir>/worker/<base64url(dataset)>/_v<version>/<chunk id>` while version 0 stays where it
  has always been. A query names the copy it wants in `chunk_version`, defaulting to 0 — the
  ingested one — so a portal reads a rewrite by naming its version, and a version this worker
  does not hold answers `not_found`.
- **Query schemas** come from the network state's `schema_bundle` (a gzipped tar of
  `<schema_id>.yaml`, verified against its `sha256:` hash) rather than from
  `--query-schemas-url`, which the assignment's own chunks no longer consult. That manifest is
  still polled under every assignment type: it answers for chunks the assignment in force does
  not pin — held from an earlier one, or held before any applies — and unlike the bundle it is
  never stored, so it is empty after a restart until the first fetch lands. Bundles are *merged*
  into
  `<data-dir>/schemas/<id>.yaml` rather than replacing what is there: chunks on disk outlive the
  bundle that described them, only the current bundle is published, and no schema can be fetched
  by id, so a schema dropped locally would strand data permanently. The store is read back at
  startup, so a restart answers for the chunks it already holds without waiting for a download.
- **An assignment is validated against its bundle.** The assignment is applied only if its
  accompanying bundle was fetched *and* carries every schema the assignment references; failing
  either, the previous assignment stays in force and no schemas from an invalid pair are installed. Schemas accumulated
  from earlier bundles keep answering queries, but do not stand in for the bundle a new
  assignment came with — applying half a pair would report an assignment as held that the worker
  only half-holds. A bundle that does not cover its assignment is a scheduler fault and raises
  `schema_bundle_mismatches`. Correcting the bundle is enough to recover: the worker reads the
  assignment pointer and the bundle as one announcement, so a new bundle hash re-offers the
  assignment with it and the scheduler need not publish a new assignment id.
- **Assignments are applied strictly in order**, each waiting for the previous one to settle,
  instead of immediately on arrival.

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
