# 03 — Data model

Home doc for `DEF`. Bands: DEF-1..9 primitives and entities · DEF-10..19 state ·
DEF-20..29 metering and actors · DEF-30..39 inputs and operations.

## Primitives and entities

**DEF-1 — Dataset.** An opaque string naming a logical data collection (e.g. a chain's
history). Equality is exact string equality; the worker never interprets dataset names.

**DEF-2 — Chunk.** The unit of data placement: an immutable set of named **table files**
covering a contiguous **block range** `[first, last]` (`first, last ∈ ℕ`,
`first ≤ last`) within one dataset. A chunk's file list, byte content, and range are
fixed at creation; the worker never mutates chunk content.

**DEF-3 — Chunk id.** An opaque string identifying a chunk within its dataset. Equality
is exact string equality. Two distinct chunks MAY cover the identical block range
(**suffix forks**, ADR-4); ids are the only identity. The id is *never* a source of
truth for the chunk's range on the read path (ADR-4); the sole sanctioned parse of an id
into a range is the metering chip computation (DEF-24 — a known architectural debt,
GAP-13).

**DEF-4 — Chunk ref.** The triple ⟨dataset, chunk id, version⟩ — the global key of a
chunk. The **version** says which copy: 0 is what ingest wrote, anything else a batch
job's rewrite of it, published under its own storage prefix (IB-41b). Legacy assignments
carry no versions, so every chunk of one is version 0, and a query naming no version asks
for 0. All state sets (DEF-10) contain chunk refs. The worker orders chunk refs
lexicographically by ⟨dataset, chunk id, version⟩ — version last, so a rewrite sorts
beside the copy it replaces rather than after every chunk in its dataset; this ordering
defines the availability map (DEF-13).

**DEF-5 — Assignment.** A network-published document: an identifier (opaque, ADR-16),
per-dataset chunk lists (each chunk with its file names, download addresses, and
declared size), a worker roster with per-worker chunk subsets and an assessed **worker
state** (ok / unreliable / deprecated version / unsupported version), and per-worker
encrypted download credentials. The worker's slice: the chunk refs listed for it.

**DEF-6 — Chunk store.** The worker's persistent store of chunk data. A chunk at version 0
is stored under its dataset exactly where it always was, so a store written before versions
existed adopts unchanged; every other version is stored under a subtree of its own, so two
copies of one id never contend for one directory and a restart can tell which copy it
holds. Its layout
invariants (no partial range overlaps within a dataset and version; identical-range forks legal) are
INV-3; the store is the single source of truth for what is available (INV-2) — there is
no separate manifest or journal.

**DEF-7 — Log record.** The durable evidence of one admitted query: ⟨query id (unique,
INV-5), receipt timestamp, client identity, the full original query, outcome (success
summary: uncompressed size, content hash, `last_block` — or the error), execution
timings, worker version⟩.

**DEF-8 — Log store.** The persistent, bounded-retention sequence of log records,
ordered by ⟨timestamp, query id⟩ (the **log cursor** order, DEF-14).

## State

**DEF-10 — Core state tuple.** The worker's entire mutable model state:

```
S = ⟨ A, D, N, X, L, Q ⟩
A : set of chunk refs   — available: committed, queryable
D : set of chunk refs   — downloading: fetch in flight
N : set of chunk refs   — desired: the applied assignment's slice for this worker
X : assignment | ⊥      — the applied assignment document (⊥ before the first)
L : chunk ref ⇀ ℕ⁺      — pins: per-chunk count of executing queries
Q : sequence of log records (bounded by retention, WP-17)
```

Derived values:

- `P = N \ A \ D` — **pending**: assigned, not yet held or in flight.
- `U : N → bool` — the **unavailability map** (DEF-13).
- `dom(L) ⊆ A` — only available chunks can be pinned (INV-4).

Well-formedness is defined by the structural invariants INV-1..5 (single source of
truth: [07-invariants.md](07-invariants.md)).

**DEF-11 — Committed chunk.** A chunk whose entire file set has been atomically
published into the chunk store (WP-12). Commitment is per-chunk; there is no
per-assignment commit (CN-1).

**DEF-12 — Pin.** A lease on an available chunk taken for the duration of one query's
execution. While `L(c) > 0`, chunk `c` is exempt from eviction (INV-12). Pins are not
persistent; recovery resets `L = ∅`.

**DEF-13 — Unavailability map.** A bit sequence with one bit per desired chunk in
chunk-ref order (DEF-4), bit = 1 iff the chunk is not available. Reported in the
heartbeat (RP-21). Because DEF-4 sorts on the version last, and an assignment names one
version per chunk id, a reader that knows only the ids computes the same order. ⚠ Whether
this order matches the scheduler's interpretation for suffix forks is OQ-1.

**DEF-14 — Log cursor.** The pair ⟨timestamp, query id⟩ under lexicographic order; the
resumption token of log delivery (RP-22). Records are served only once their timestamp
is at least P-LOGS-LAG old, because record order and timestamp order may disagree near
the head (ADR-13).

**DEF-15 — Snapshot (unit of read isolation).** One pinned chunk. A query observes
exactly one committed chunk version for its whole execution; there is no multi-chunk or
store-wide snapshot (NG1, CN-2).

## Metering and actors

**DEF-20 — Portal.** A network client that sends queries. Identified by its transport
peer identity, which is also its signature identity (RP-2).

**DEF-21 — Operator and cluster.** The on-chain accountable party. Each portal maps to
one operator; all portals of one operator's cluster share that operator's CU budget.

**DEF-22 — Epoch.** The network's metering period, read from the chain registry. CU
allocations are per operator per epoch; allocations refresh when the observed epoch
increases.

**DEF-23 — Compute unit (CU) and bucket.** The admission currency. Each operator has a
token bucket: capacity P-CU-BURST tokens, refill rate `allocation / epoch length`.
Admission spends 1.0 token; execution may refund a fraction (DEF-24).

**DEF-24 — Chip.** The fraction of a chunk a query actually covered:
`chip = clamp(active range length / chunk range length, 0..1)`, computed from the
queried range intersected with the chunk's range. Net cost of an admitted query =
`chip`; the refund `1 − chip` is applied after execution regardless of outcome, except
overload rejections, which keep the full unit (ADR-6).

**DEF-25 — Admission.** The bar a query passes to become billable: signature and
timestamp verification, envelope validation, capacity reservation, CU spend — in that
order (RP-1, ADR-7). Everything past admission is logged (INV-32); nothing before it is.

**DEF-26 — Delivery.** The pair ⟨wire response, log record⟩ produced from a single
execution outcome (ADR-8). A **downgrade** replaces a success that cannot be shipped
(oversized, unsignable) with the taxonomy's `server_error` in *both* halves (INV-23).

## Inputs and operations

**DEF-30 — Input events.** Everything that can change model state:

| Event | Content | Delivery | Meaning |
|---|---|---|---|
| assignment-published | network-state document naming a new assignment id | polled every P-ASSIGN-POLL; at-least-once; last-writer-wins | triggers fetch + WP-10 |
| chunk-fetched | one chunk's complete file set | pull from data origin; retried per WP-13 | enables WP-12 |
| query | signed query message | request/response; exactly-once per stream | RP path; pins, meters, appends log |
| logs-request | cursor | request/response | RP-22 read |
| status-request | — | request/response | RP-21 read |
| epoch-tick | epoch number + allocations | polled every P-EPOCH-POLL | refreshes DEF-23 buckets |
| schema-refresh | schema manifest (legacy) or schema bundle (`--assignment-source worker`) | polled every P-SCHEMA-REFRESH / carried by the network state | updates dynamic-engine registry |
| clock-tick | time | — | drives WP-17 log pruning, bucket refill |
| process-crash / restart | — | — | WP-15 recovery |

**DEF-31 — Transition summary.** Semantics live in [04-mutations.md](04-mutations.md):

| Transition | One line |
|---|---|
| WP-10 apply-assignment | replace X and N wholesale; recompute P; A untouched |
| WP-11 fetch-start | move one pending chunk into D, bounded by P-DL-CONC |
| WP-12 commit | downloaded chunk becomes atomically available: D → A |
| WP-13 abort | failed/cancelled fetch leaves D; re-pends if still desired |
| WP-14 evict | unassigned, unpinned chunk leaves A and the store |
| WP-15 recover | restart: adopt committed chunks, sweep residue, forget the assignment |
| WP-16 log-append | admitted query appends exactly one record to Q |
| WP-17 log-prune | records older than P-LOGS-RETENTION leave Q |

**DEF-32 — Worker-state policy.** The assignment's per-worker assessed state maps to an
operational posture:

| Assessed state | Posture |
|---|---|
| ok | full service |
| unreliable | full service; alarm level raised (OB-12) |
| deprecated version | full service; upgrade alarm |
| unsupported version | alarm; service expectations network-defined (explicitly unspecified here) |
| absent from roster | previous assignment stays in force; alarm (FM-12) |

## Terminology cross-reference

| Codebase term | Spec term |
|---|---|
| `desired` / `available` / `downloading` / `to_download` | N / A / D / P (DEF-10) |
| `ChunkRef` | chunk ref (DEF-4) |
| `DatasetsIndex` | applied assignment X (DEF-10) |
| chunk guard / `locks` | pin (DEF-12) |
| `missing_chunks` bitstring | unavailability map (DEF-13) |
| `Heartbeat` / `WorkerStatus` message | status report (RP-21) |
| admission bar / `AdmittedQuery` | admission (DEF-25) |
| `build_delivery` / `Logged` | delivery (DEF-26) |
| `allocation_chip` | chip (DEF-24) |
| workdir | chunk store (DEF-6) |
| `logs.db` | log store (DEF-8) |
| temp-prefixed directory | transient residue (RS-6) |
