# ADR-11 — Engine selection by wire enum; columnar output format

Status: Accepted (historical)

## Context

Row-oriented JSON output dominated query cost. The repository's benchmark report
(bench branch, fixture chunk) measured the serialization+compression pipeline at
18–34× cheaper with Arrow IPC + zstd than JSONL + gzip; Protobuf and FlatBuffers were
evaluated and rejected (smaller but slower end-to-end; no zero-copy benefit since the
response is compressed anyway). A second query engine ("dynamic") with schema-driven
dataset support needed a rollout path that doesn't break old clients.

## Decision

The query message carries two orthogonal enums: `query_engine` (DEFAULT → legacy
engine; DYNAMIC → the schema-driven engine) and `output_format` (JSONL; ARROW_IPC,
legal only with DYNAMIC). Protobuf's zero-default makes old clients land on
legacy+JSONL automatically. Both enums are covered by the query signature. Arrow-level
compression stays off — the worker compresses the whole response. Gating is
portal-driven: no worker-side flag disables the dynamic engine; the schema registry
(CDN manifest, hourly refresh, keep-previous-on-failure) is the operational gate.

## Consequences

Backward-compatible phased rollout (workers → portal → SDKs); the measured 18–34×
serialization headroom becomes reachable. Rejected alternative recorded: the report
also recommended zstd level 1; the worker ships the library default level instead —
an open easy win. Shapes RP-12, IB-10/12, REQ-1.
