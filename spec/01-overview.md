# 01 — Overview

## What it is

The worker is one node of a horizontally-sharded, decentralized data lake. The network's
datasets are split into immutable **chunks**; a network **scheduler** publishes an
**assignment** mapping chunks to workers. Each worker continuously reconciles its local
**chunk store** against its slice of the assignment — downloading newly assigned chunks
from a **data origin**, deleting unassigned ones — and serves **queries** from
**portals** against the chunks it holds.

The hot path the system exists for: a portal sends a signed query naming one dataset,
one chunk, and a block range; the worker validates and meters it, evaluates it against
the local chunk data, and returns a signed result whose `last_block` tells the portal
how far it got. Everything else — assignment intake, downloads, eviction, status
reporting, log delivery — exists to keep that path serving fresh, correct data.

The worker is economically accountable: each admitted query consumes **compute units
(CU)** from the querying operator's per-epoch allocation, and every admitted query
produces a log record that collectors pull as evidence of work performed.

## Actors

| Actor | Direction | Role | Verified how |
|---|---|---|---|
| Portal | inbound | sends queries, receives signed results | per-query signature bound to sender identity and this worker (RP-2) |
| Scheduler | inbound (polled) | publishes network-state and assignment documents | transport security only; document contents trusted (ADR-3) |
| Data origin | inbound (polled) | serves chunk files named by the assignment | credentials from the assignment; payload unverified (GAP-5) |
| Logs collector | inbound | pulls query-execution log records | network membership only; no per-request auth (GAP-22) |
| Chain registry | inbound (polled) | epoch number, CU allocations, peer membership | trusted read |
| Schema registry | inbound (polled) | dataset schemas for the dynamic engine: a CDN manifest, or the network state's schema bundle under `USE_WORKER_ASSIGNMENTS` | transport security only; the bundle is content-hash verified |
| Operator (human) | config | runs the process, sets configuration, watches metrics | — |

## Design goals

- **G1 — Correct partial results.** A query response is always sound: data derives only
  from committed chunk content, and `last_block` honestly bounds coverage. → REQ-1,
  REQ-4, INV-21/22, RP-11.
- **G2 — Convergent replication.** The store converges to the assignment without manual
  intervention, atomically per chunk, safely across crashes. → REQ-2, REQ-3, LIV-1,
  CN-1, INV-40.
- **G3 — Metered, accountable service.** Work is charged per operator against on-chain
  allocations, and every admitted query is provable via its log record. → REQ-21,
  REQ-12, INV-15, INV-32.
- **G4 — Robust residency.** No input — hostile query, malformed assignment, flaky
  origin — terminates the process or corrupts the store. → REQ-24, FM-1, INV-36.
- **G5 — Honest self-reporting.** Status, heartbeat, and metrics reflect real state
  within bounded staleness. → REQ-11, REQ-13, LIV-6, INV-30/31.

## Non-goals

- **NG1 — Multi-chunk queries.** A query addresses exactly one chunk; cross-chunk
  aggregation is the portal's job (ADR-12). Tests must not pin any multi-chunk behavior.
- **NG2 — Assignment ordering.** The worker treats assignment identifiers as opaque and
  applies assignments in arrival order; ordering and supersession semantics are
  scheduler-owned (ADR-16).
- **NG3 — Query-language semantics.** What a query *means* over chunk data is the
  embedded query engines' contract, external to this spec. The worker spec constrains
  the envelope: admission, metering, coverage, boundary emission (RP-11), size bounds, error taxonomy (see the
  free-variable declaration in 13).
- **NG4 — Data authenticity.** The worker does not attest that origin data is the true
  chain history; it serves what the assignment names. Verification is a network-level
  concern.
- **NG5 — Multi-tenant fairness beyond CU metering.** CU metering is the only per-client
  fairness mechanism; the worker promises no per-portal scheduling fairness (see
  LIV-11 for the *cross-dataset* download bound, which is promised).

## Trust model

| Actor | Trusted with | Must never be able to cause |
|---|---|---|
| Portal | nothing; every query verified and metered | process termination, store corruption, unmetered work, reading another operator's data context |
| Scheduler | store contents (it decides what the worker holds) | process termination via a malformed document (REQ-24); silent total store wipe is bounded by REQ-25 |
| Data origin | chunk payload bytes | process termination; a bad payload may only yield per-chunk failure, never store-wide unavailability |
| Logs collector | reading execution logs | write access to anything; unbounded resource use |
| Chain registry | metering inputs (epoch, allocations) | process termination on unavailability; stale data degrades metering, never safety |
| Operator | full control | — (misconfiguration SHOULD fail fast at startup, FM-50) |

## Lifecycle at a glance

Dataflow:

```
 scheduler ──(poll: network-state, assignment)──▶ ┌────────────┐
 data origin ──(poll: chunk files)──────────────▶ │            │──▶ chunk store (disk)
 chain registry ──(poll: epoch, allocations)────▶ │   WORKER   │──▶ log store (disk)
 schema registry ──(poll: schema manifest)──────▶ │            │──▶ metrics/status (pull)
 portal ──(query)───────────────────────────────▶ │            │──▶ signed result
 collector ──(logs request)─────────────────────▶ └────────────┘──▶ log records
```

Chunk lifecycle (one line): `assigned → pending → downloading → available ⇄ pinned → unassigned → removed`.

Process lifecycle (one line): `start → recover store (sweep residue, adopt committed chunks) → confirm on-chain registration (poll until listed; nothing is served before — FM-54) → accept queries (degraded: no assignment yet) → converge to assignment → steady serve/reconcile → shutdown (bounded drain)`.
