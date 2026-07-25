# ADR-2 — Binary assignment format over JSON

Status: Accepted (historical)

## Context

Assignments were JSON. At network scale, parsing dominated intake: a commit records
"the JSON parsing takes much more time than fetching with some setups", which had
forced repeated retuning of fetch timeouts (30 s → 90 s → 1200 s) because the timeout
covered parsing too.

## Decision

The assignment document is a gzip-compressed FlatBuffers file (`fb_url_v1`); the worker
holds the buffer and reads it zero-copy through index handles. The fetch timeout was
rescoped to the network fetch only.

## Consequences

Intake cost is dominated by transfer, not parsing. The worker keeps the whole
network-wide document in memory (HZ-6: applying scans all datasets × chunks).
Zero-copy reading motivated skipping buffer verification — see ADR-3. Shapes WP-1/2,
IB-41.
