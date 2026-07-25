# ADR-4 — Opaque chunk identity, suffix forks

Status: Accepted (historical)

## Context

Chunk ids originally encoded the block range, and the read path derived range facts
(e.g. a result's `last_block`) from the id. That conflation produced a family of
duplicate-id and wrong-progress defects, and made chunk replacement (same range, new
content — forks) impossible. The PR record states: "The chunk id shouldn't be used as a
source of truth for the data that it corresponds to."

## Decision

The chunk id is an opaque lookup key. Block-range truth comes from the query's range
and the chunk's content. Two chunks may cover the identical range (suffix-distinguished
forks); partially overlapping ranges remain illegal. Empty results report the
requested range's end.

## Consequences

Chunk replacement and forks become possible; a whole defect class retires. Layout
validation must special-case identical ranges (INV-3). One residual violation: the
metering chip still parses the id (GAP-13). Shapes DEF-3, RP-11, INV-3.
