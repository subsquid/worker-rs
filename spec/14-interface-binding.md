# 14 — Interface binding

Home doc for `IB`. Bands: IB-1..9 transport · IB-10..19 query surface · IB-20..29 logs
and status surface · IB-30..39 operator surface · IB-40..49 input-side binding.

This is the only normative document that names the concrete surface. Everything here is
*observable contract*; internals remain out of scope. **Anything not specified here is
unspecified — do not pin it in tests.** Binding changes update this file and CT-5 in
the same change (IB-9).

## Transport

**IB-1 — Peer transport.** The worker serves libp2p request/response protocols over
QUIC. The worker's network identity is its libp2p peer id; signature keys are the peer
identity keys. Peers are additionally gated by the network's on-chain membership
whitelist at the transport layer.

**IB-2 — Protocol table.**

| Protocol id | Request | Response | Max request | Max response | Stream read/write timeout |
|---|---|---|---|---|---|
| `/sqd/query/1.1.0` | `Query` (protobuf) | `QueryResult` | P-Q-MSG-MAX | P-RESP-MAX | P-STREAM-TIMEOUT |
| `/sqd/sql_query/1.0.0` | `Query` (query = base64url Substrait plan) | `QueryResult` | P-SQL-MSG-MAX | P-RESP-MAX | P-STREAM-TIMEOUT |
| `/sqd/worker_logs/1.1.0` | `LogsRequest` | `QueryLogs` | 100 B | P-LOGS-RESP-CEIL | P-STREAM-TIMEOUT |
| `/sqd/worker_status/1.0.0` | (empty) | `Heartbeat` | 0 B | P-LOGS-RESP-CEIL | P-STREAM-TIMEOUT |

Transport-level violations (oversized, undecodable, empty body, queue overflow) produce
stream resets with no typed response — the RP-20 *(no response)* row. The intake chain
per protocol is: accept buffer (P-Q-ACCEPT-BUF) → parsed-request queue (P-Q-REQ-BUF) →
shared lossy event queue (P-EVENT-QUEUE, drops counted by the transport's dropped-events
metric) → the per-surface intake queue (P-Q-QUEUE); overflow at any stage is such a
drop.

**IB-4 — Auxiliary protocols.** The transport additionally serves: libp2p identify
(agent string `sqd-worker/<version>`), Kademlia DHT in server mode, autonat, ping, and
`/sqd/noise/0.0.1` — a bandwidth-probe endpoint that answers any member peer with an
unbounded random-byte stream (resource hazard: HZ-13). These carry no application
semantics and are outside the conformance surface; tests MUST NOT pin their behavior
beyond existence.

**IB-3 — Status is pull-only.** No broadcast/heartbeat publishing exists; the status
protocol answers with a cached snapshot (ADR-16). The `Heartbeat` message is unsigned
(OQ-4).

**IB-9 — Versioning rule.** Protocol ids version the surface; a wire-visible change
bumps the protocol id or a message field number, never silently changes semantics of an
existing field. This file and the CT-5 conformance corpus change in the same commit.

## Query surface

**IB-10 — `Query` message fields.**

| Field | Type | Binding semantics |
|---|---|---|
| `query_id` | string | MUST be exactly 36 bytes (signature payload precondition); portal-chosen; unique per query (RP-2, GAP-12) |
| `dataset` | string | exact match against assignment dataset ids |
| `query` | string | engine-specific body; interpretation per `query_engine` |
| `chunk_id` | string | opaque exact-match key (DEF-3) |
| `block_range` | `{begin, end}` uint64 | required in practice (absence → post-admission `bad_request`); overrides any range in the query body (the SQL surface ignores it — GAP-24) |
| `timestamp_ms` | uint64 | freshness check window P-TS-WINDOW |
| `signature` | bytes | covers fields per RP-2 |
| `compression` | enum `GZIP(0) / NONE(1) / ZSTD(2)` | response encoding; proto default = GZIP; **not** signature-covered |
| `query_engine` | enum `DEFAULT(0) / DYNAMIC(1)` | engine selector; signature-covered |
| `output_format` | enum `JSONL(0) / ARROW_IPC(1)` | ARROW_IPC legal only with DYNAMIC; signature-covered |
| `request_id` | string | echoed into the log record; otherwise ignored |

**IB-11 — `QueryResult`.** `query_id`, oneof `ok {data, last_block}` /
`err {bad_request | not_found | server_error | too_many_requests | server_overloaded}`,
optional `retry_after_ms`, `signature`. `retry_after_ms` appears on overload/bucket
rejections; the admission-time slow-down advisory MAY additionally ride on any
post-admission response — success or error (RP-4, test-pinned). Downgraded results
carry none.

**IB-12 — Output formats.** `JSONL`: one JSON object per line, ascending block order.
`ARROW_IPC`: per-table Arrow IPC streams, raw binary columns, no Arrow-level
compression (response-level compression applies instead; ADR-11). SQL surface: JSONL
rows of the plan's result; `last_block` is 0 (a signed constant — SQL results carry no
progress semantics; register row GAP-24).

**IB-13 — Error mapping.** Abstract class (RP-20) → wire variant, and the signed
payload:

| Abstract | Wire | Signature covers |
|---|---|---|
| `bad_request` | `err.bad_request(string)` | query id + class code (message text unauthenticated) |
| `not_found` | `err.not_found(string)` | query id + class code |
| `too_many_requests` | `err.too_many_requests` | query id + class code |
| `server_overloaded` | `err.server_overloaded` | query id + class code |
| `server_error` | `err.server_error(string)` | query id + class code |
| success | `ok` | query id ‖ sha3-256(compressed payload bytes) ‖ last_block |

⚠ Two `server_error` message strings are de-facto frozen contracts until GAP-30/31
resolve (OQ-7): `unexpected base block: expected …, but got …#0x…` (the anchor-mismatch
verdict, RP-20 — portals convert it into their fork-recovery conflict) and
`Response too large` (the engine-level oversize verdict — portals convert it into a
narrow-the-query rejection).

## Logs and status surface

**IB-20 — `LogsRequest`.** `{from_timestamp_ms, last_received_query_id?}` — the DEF-14
cursor. Response `QueryLogs{queries_executed[], has_more}`; page bounded by
P-LOGS-RESP-MAX (a margin below P-LOGS-RESP-CEIL; OQ-2). Records ordered by
⟨timestamp_ms, query_id⟩; only records with `timestamp_ms ≤ now − P-LOGS-LAG` are
served. The response is unsigned; record authenticity rests on each record's embedded
client-signed query.

**IB-21 — `QueryExecuted` record.** Client id, the full original `Query` (signature
included), `exec_time_micros`, a stage-timing report (parse/exec/serialize/compress/
sign), receipt-side `timestamp_ms`, `worker_version`, and oneof
`ok {uncompressed_data_size, sha3-256(uncompressed data), last_block}` / the RP-20
error. Note the hash-basis asymmetry with IB-13's success signature (OQ-5).

**IB-22 — `Heartbeat` message.** `{version, assignment_id, missing_chunks: BitString,
stored_bytes?, current_epoch?, last_applied_assignment_id?}`. `missing_chunks` is the
DEF-13 map, deflate-compressed bit bytes with declared size and ones-count; bit order
is chunk-ref order (OQ-1). `assignment_id` is `""` before the first application.
`last_applied_assignment_id` is absent in shipped builds (OQ-3).

## Operator surface

**IB-30 — HTTP endpoints.** Bound on the operator port (default 8000), unauthenticated,
GET only: `/worker/status` → JSON `{"state":{"available":n,"downloading":n}}` ·
`/worker/peer-id` → text peer id · `/metrics` → OpenMetrics text.

**IB-31 — Metric names.** The OB signals bind to Prometheus families, all labeled
`worker_id`: `chunks_available/downloading/pending` (OB-1),
`chunks_downloaded/failed_download/removed` counters (OB-4), `used_storage_bytes`
(OB-5), `running_queries` (OB-6), `num_queries_executed{status}` (OB-7),
`query_result_size_bytes` (OB-8), `worker_status{worker_status}` (OB-12 assessed-state
component), `worker_info_info{version}`. Signals OB-9/10/11/13 and the missing OB-4/7
breakdowns have no binding yet — GAP-17/GAP-23 track the additions.

**IB-32 — Configuration surface.** Flags/env (defaults live in the registry):

| Setting | Env | Binds |
|---|---|---|
| `--data-dir` | `DATA_DIR` | store root (required) |
| `--prometheus-port` | `PROMETHEUS_PORT` | IB-30 port |
| `--p2p-port` / `--public-ip` | `LISTEN_PORT` / `PUBLIC_IP` | transport addresses |
| `--key` | `KEY_PATH` | identity key file (required) |
| `--parallel-queries` | `PARALLEL_QUERIES` | P-Q-PAR |
| `--concurrent-downloads` | `CONCURRENT_DOWNLOADS` | P-DL-CONC |
| `--query-threads` | `QUERY_THREADS` | engine pool width |
| `--assignment-url` | `ASSIGNMENT_URL` | network-state document address |
| (positional) | `S3_TIMEOUT` / `S3_READ_TIMEOUT` | P-DL-FILE-TIMEOUT / P-DL-STALL-TIMEOUT |
| (positional) | `DOWNLOADS_MAX_DELAY_SEC` | P-DL-BACKOFF-MAX |
| (positional) | `ASSIGNMENT_CHECK_INTERVAL_SEC` / `ASSIGNMENT_FETCH_TIMEOUT_SEC` / `ASSIGNMENT_CHECK_MAX_DELAY_SEC` | P-ASSIGN-POLL / P-ASSIGN-FETCH-TIMEOUT / P-ASSIGN-RETRY-MAX |
| (positional) | `NETWORK_POLLING_INTERVAL_SEC` | P-EPOCH-POLL |
| `--query-schemas-url` (+ refresh env) | `QUERY_SCHEMAS_URL` | schema registry address / P-SCHEMA-REFRESH |
| `--rpc-url`, `--l1-rpc-url`, `--network`, contract addresses | `RPC_URL` … | chain registry |
| (positional) | `SENTRY_DSN` / `SENTRY_IS_ENABLED` | crash telemetry (on by default) |

Duration settings parse as whole seconds. Misconfiguration behavior is FM-50.

## Input-side binding (what simulators/stubs implement)

**IB-40 — Network-state document.** HTTPS GET at the assignment URL returning JSON
`{network, assignment: {id, fb_url_v1, effective_from, …}}`; `effective_from` is
currently ignored by the worker (OQ-8). HC-1 serves this.

**IB-41 — Assignment document.** HTTPS GET at `fb_url_v1`: a gzip-compressed
FlatBuffers document — dataset table (ids, base addresses), per-dataset chunk tables
(id, base address, file name→address map, declared size, summaries, per-chunk
worker-index lists — the live chunk→worker mapping), worker roster (peer ids, assessed
state, encrypted HTTP headers per worker; the roster-side chunk list is deprecated;
encryption is crypto-box against the worker's identity key). HC-1 must be able to emit
well-formed and deliberately malformed instances (FM-11/12 corpus).

**IB-42 — Data origin.** Plain HTTPS GET per file at
`join(dataset_base, chunk_base, file_url)` with the decrypted headers attached;
redirects disabled; per-request timeout P-DL-FILE-TIMEOUT, read stall bound
P-DL-STALL-TIMEOUT. HC-2 serves these and ledgers every byte.

**IB-43 — Chain registry reads.** Worker id lookup by peer id at startup; epoch number
and per-operator CU allocations polled every P-EPOCH-POLL. HC-8 stubs these.

**IB-44 — Schema registry.** HTTPS GET YAML manifest mapping dataset types to schema
documents, refreshed every P-SCHEMA-REFRESH with unchanged-body short-circuit; fetch
failure keeps previous schemas (FM-53).
