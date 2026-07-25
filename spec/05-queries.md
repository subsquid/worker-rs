# 05 — Queries

Home doc for `RP`. Bands: RP-1..9 admission · RP-10..19 result contract ·
RP-20..29 status and log reads, error taxonomy.

## Operations

| Operation | Kind | Contract |
|---|---|---|
| chunk query | read + meter + log | RP-1..15 |
| status read | watermark read | RP-21 |
| logs read | cursor read | RP-22 |
| local status / metrics read | operator read | RP-23, OB |

## Admission

**RP-1 — Admission sequence.** [MUST] A query passes, in order: (1) authentication —
signature verifies against the sender's identity and binds this worker's identity;
(2) freshness — timestamp within P-TS-WINDOW of the worker clock (clock discipline:
CN-8; rejection class: the RP-20 freshness verdict); (3) envelope
validation — recognized engine and output-format selectors, and a legal
engine/format pairing; (4) capacity — a queue slot is reserved (else `server_overloaded`
with a P-RETRY-HINT); (5) metering — 1.0 CU is spent from the sending portal's (DEF-20) operator bucket
(DEF-21) — else `too_many_requests`, with a retry hint iff the operator has an
allocation.
Order is load-bearing: a spent CU always corresponds to an enqueued query (ADR-7).
Failures at steps 1–5 are **pre-admission**: no CU, no log record.

**RP-2 — Authentication scope.** [MUST] The signature covers the query id, this worker's
identity, the timestamp, dataset, query body, chunk id, block range, and the
engine/format selectors — a signed query cannot be replayed at another worker nor have
its semantics altered in transit. ⚠ Replay of the identical query at the same worker
within P-TS-WINDOW is currently not prevented (GAP-12). Compression choice is outside
the signature (accepted: it affects encoding, not semantics — but see OQ-5).

**RP-3 — Post-admission validation.** [MUST] Remaining envelope checks (compression
selector, presence of the block range) happen after admission: failures cost the full CU
and are logged (ADR-7 draws the billable line at admission, and the test-pinned policy
is that malformed-but-admitted queries pay).

**RP-4 — Concurrency and overload.** [MUST] At most P-Q-PAR queries execute
concurrently [enforcement is intent, currently violated: GAP-1]; the intake queue holds
P-Q-QUEUE per protocol surface with P-Q-STREAMS concurrent message handlers; excess
yields `server_overloaded` + P-RETRY-HINT. An overload rejection after admission keeps
the CU (ADR-6). Rejection responses themselves are bounded by P-REJECT-CONC concurrent
signing tasks; beyond that the connection is dropped unanswered (ADR-9).

**RP-5 — Timeout and cancellation.** Every admitted query SHOULD complete or fail
within P-Q-DEADLINE ⚠ (OQ-6), and execution SHOULD be cancelled when the requester
disconnects. [Both are intent, currently violated: no deadline, no cancellation —
GAP-8.] Response write timeouts are the transport's (IB-2).

## Result contract

**RP-10 — Scope.** [MUST] A query addresses exactly one chunk (DEF-2) of one dataset
(DEF-1) (NG1, ADR-12). The chunk must be available at execution start; the effective range is the
query's block range clipped to the chunk's actual content. The worker MUST NOT reject a
range disjoint from the chunk — it returns an empty result (RP-11 rules apply).

**RP-11 — Coverage and progress.** [MUST] A successful result covers a gap-free prefix
of the effective range: `covered = [range.begin, last_block]`. Rules:
- Every block in `covered` that the query selects is emitted completely (no partial
  blocks — truncation granularity is the whole block).
- `last_block ≤ range.end` always; `last_block` = the highest block actually evaluated.
- Boundary emission: when at least one block is evaluated, the result includes a record
  for the first and last evaluated block — header-only when the query selects nothing
  from them — so the coverage cursor is recoverable from the data alone; portal-side
  client resumption load-bears on this. [MUST — intent; provided today by the legacy
  engine's weight-0 boundary pinning, dynamic-engine conformance unverified: GAP-32.]
- Empty result: a range that evaluates zero blocks (disjoint from the chunk's content,
  RP-10) yields empty data with `last_block = range.end`; an evaluated range never
  yields zero records (boundary emission above).
- Progress guarantee: a successful response always advances the client — either
  `last_block ≥ range.begin` (at least one block evaluated) or the result is the
  empty-selection case covering everything.
- Coverage is recoverable by the client from `last_block` alone, even with zero emitted
  rows.

**RP-12 — Emission order.** [MUST] Row-oriented output emits blocks in ascending block
order, one record per line; columnar output emits per-table streams whose row order
within a table is ascending by block. Determinism: byte-identical results for identical
⟨query, chunk content, format⟩ modulo the declared free variables (13 §free variables).

**RP-13 — Truncation is normal.** [MUST] The engine stops early when the result-size
budget P-RESP-BUDGET is reached, keeping at least one whole block. Early stop is
signalled *only* through `last_block < range.end` — there is no error, no flag; clients
MUST treat it as success and resume. The client recovery algorithm (normative — this is
a conformance driver):

```
next := range.begin
while next ≤ range.end:
    r := query(chunk, [next, range.end])
    if r is error: handle per taxonomy (RP-20); retry or reroute
    else: consume(r.data); next := r.last_block + 1
```

**RP-14 — Size bounds.** [MUST] Uncompressed result data never exceeds P-RESP-MAX; the
encoded response message never exceeds the transport's response ceiling (IB-2). A result
that would exceed either is **downgraded** to `server_error` in both response and log
(DEF-26). A single block whose emission alone exceeds the budget is such a downgrade,
not an infinite truncation loop. [The two oversize paths currently emit divergent
`server_error` strings: GAP-31.]

**RP-15 — Result integrity.** [MUST] The response is signed over ⟨query id, content
hash, `last_block`⟩; the log record carries the uncompressed content hash and size
(OQ-5 tracks the compressed-vs-uncompressed discrepancy with portal-side records).
Error responses are signed over ⟨query id, error class⟩ — the class is authenticated,
the message text is not (IB-13).

## Status and log reads, errors

**RP-20 — Error taxonomy.** [MUST] The closed set of query outcomes. No other outcome
class exists; message strings are advisory and unstable; classes are stable surface.

| Class | Trigger | CU | Logged | Retryable |
|---|---|---|---|---|
| `bad_request` | authentication, envelope, unparseable/uncompilable query, unknown table/column | pre-admission: 0 · post: 1·chip | pre: no · post: yes | no (fix and resend) |
| `not_found` | chunk not available (never assigned, not yet fetched, or evicted) | full refundable chip path | yes | yes — reroute, or retry after the availability map shows the chunk |
| `too_many_requests` | operator bucket empty or no allocation | 0 | no | yes after hint; without allocation, only after on-chain change |
| `server_overloaded` | capacity exhausted (queue or concurrency) | pre-admission: 0 · post-admission: 1 (ADR-6) | post only | yes after P-RETRY-HINT |
| `server_error` | execution failure, downgrade (RP-14), signing failure, internal fault, freshness rejection (pre-admission — verdict below) | 1·chip (freshness: 0) | yes (freshness: no) | unknown; safe to retry (idempotent reads; freshness: with a fresh timestamp) |
| *(no response)* | undecodable request, reject fan-out exhausted, transport limits | 0 | no | yes (treat as transient) |

Errors never carry result data (INV-20). A `not_found` does not distinguish its three
causes (GAP-19 tracks adding machine-readable subcodes); the availability map (RP-21) is
the sanctioned disambiguator for status consumers (the scheduler) — query clients
reroute on `not_found` without reading it.

**Freshness verdict.** [MUST — intent, currently violated: GAP-33] A rejection whose
only cause is timestamp freshness (RP-1 step 2) is a worker-fault outcome: it surfaces
as `server_error`, never `bad_request`, because its reference input is the worker's own
clock and the worker cannot tell a stale sender from its own skew (INV-26, FM-55,
ADR-20). Accounting keeps the pre-admission rule: no CU, no log record (ADR-7). Clients
retry with a fresh timestamp; systematic freshness rejections are the worker's own
alarm signal (OB-15), never grounds to blame the requester. A machine-readable
staleness verdict awaits the OQ-7 surface revision.

**Anchor-mismatch verdict.** [MUST — intent, currently violated: GAP-30] A query whose
body carries a continuation anchor (the expected parent hash of the first selected
block) that the chunk's data contradicts MUST fail with a machine-distinguishable
verdict carrying the canonical block reference at the anchor height; portals convert it
into their fork-recovery conflict response. Today the verdict exists only as the legacy
engine's `server_error` message text; until a stable surface lands (OQ-7), the exact
strings are a de-facto frozen contract (IB-13).

**RP-21 — Status read.** [MUST] On request, the worker returns its status report:
version, applied assignment id (empty until the first application), the unavailability
map (DEF-13), stored bytes, current epoch (absent until first observed). Honesty and
freshness: all fields derive from one coherent state snapshot (INV-30) no older than
P-HB-STALENESS [freshness intent, currently violated when store walks are slow:
GAP-15]. The assignment id only ever advances to ids of successfully applied
assignments; a failed application never changes it (WP-2). Status is currently unsigned
(OQ-4).

**RP-22 — Logs read.** [MUST] A logs request carries a cursor (DEF-14). The response
returns records strictly after the cursor, in cursor order, whose timestamps are at
least P-LOGS-LAG old, truncated to P-LOGS-RESP-MAX bytes with a has-more flag.
Guarantees: at-least-once delivery within the retention window; no gaps and no
reordering for a client that resumes from the last received cursor; records older than
P-LOGS-RETENTION MAY be gone regardless of delivery (WP-17). Requests beyond the
read-queue bound P-LOGS-QUEUE are dropped unanswered.

**RP-23 — Local reads.** [MUST] The operator surface exposes: chunk-count state, the
worker's network identity, and the full metrics inventory (OB). These are
operator-facing and unauthenticated by design; they never expose query contents.

**RP-24 — Read-side resource bounds.** [MUST] Result construction memory is bounded per
query by a constant multiple of P-RESP-MAX ⚠ (P-MEM-CEIL tracks the process-wide
target); a slow or dead reader costs at most the transport write timeout (IB-2), after
which the response is abandoned (the implied minimum reader rate for large results is
HZ-14). Log reads are bounded by P-LOGS-RESP-MAX per response. Status reads MUST be
bounded by P-STATUS-CONC concurrent handlers [intent, currently violated — unbounded
task spawn: GAP-29].
