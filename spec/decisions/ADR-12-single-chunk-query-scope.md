# ADR-12 — Single-chunk query scope

Status: Accepted (historical)

## Context

A query could conceivably span every chunk the worker holds for a dataset. That makes
result size, execution time, snapshot semantics, and billing granularity all
store-shaped instead of request-shaped, and couples portals' routing to worker-local
layout.

## Decision

Every query addresses exactly one chunk, named by id. Portals plan multi-chunk reads
by fanning out one query per chunk (network-wide, across workers) and resuming within
a chunk via `last_block`.

## Consequences

Read isolation collapses to a single pin (DEF-15) — no multi-chunk snapshot machinery.
Metering per chunk-fraction is well-defined (DEF-24). Portals own range planning.
`num_read_chunks`-style signals are constant by construction. Shapes NG1, RP-10,
CN-2.
