# 02 — Requirements

Home doc for `REQ` and `OQ`. Bands: REQ-1..9 core flow · REQ-10..19 management ·
REQ-20..29 quality. Acceptance *status* lives in the matrix in
[13-conformance-tdd.md](13-conformance-tdd.md), not here.

## Core flow

**REQ-1 — Serve chunk queries.** [MUST]
Given a well-formed, authenticated query naming a dataset, a chunk the worker holds, and
a block range, the worker returns a result computed from that chunk's committed content,
signed by the worker, in the requested output format and compression.
*Acceptance:* a portal-driver query against a known chunk returns a decodable, signed
result whose content matches the reference evaluation of the same query over the same
chunk files; every structural validator (13 §validators) passes.
*Trace:* ADR-11, ADR-12; INV-20/21/25, RP-10..15.

**REQ-2 — Acquire assigned data.** [MUST]
The worker discovers its assignment by polling and downloads every assigned chunk it
lacks, without operator intervention. Each chunk becomes queryable atomically — a chunk
is either fully present and served, or absent; partially downloaded data is never
visible.
*Acceptance:* publish an assignment of N chunks against a stub origin; within the LIV-1
bound the worker's heartbeat reports zero missing chunks and each chunk answers queries;
killing the worker mid-download never yields a queryable partial chunk (CT-2).
*Trace:* ADR-1, ADR-2; WP-10..12, CN-1, LIV-1/2, INV-40.

**REQ-3 — Release unassigned data.** [MUST]
Chunks absent from the current assignment are deleted — but never while a query is
executing against them, and only through the explicit eviction transition.
*Acceptance:* shrink the assignment while a long query holds the evicted chunk: the
query completes from intact data; the chunk's space is reclaimed within P-EVICT-BOUND of
the query's end.
*Trace:* WP-14, RS-1/2/4, INV-11/12, LIV-4.

**REQ-4 — Honest progress reporting.** [MUST]
Every successful result carries `last_block`: the upper bound of the range actually
covered. A truncated result (size budget reached) reports the last block included; an
empty result reports the requested range's end. A portal resuming from
`last_block + 1` never skips and never re-reads data.
*Acceptance:* for a query whose full result exceeds P-RESP-BUDGET, the concatenation of
results obtained by repeated resumption equals the reference evaluation of the whole
range, with no overlap and no gap.
*Trace:* ADR-4; RP-11/13, INV-22.

## Management

**REQ-10 — Assignment intake by polling.** [MUST]
The worker polls the network-state document every P-ASSIGN-POLL, fetches a changed
assignment within P-ASSIGN-FETCH-TIMEOUT, and applies it in arrival order. A fetch
failure is retried with backoff bounded by P-ASSIGN-RETRY-MAX; a failed or inapplicable
assignment leaves the previously applied assignment fully in force.
*Acceptance:* against a stub scheduler, an assignment change is observable in the
heartbeat's assignment id within P-ASSIGN-POLL + P-ASSIGN-FETCH-TIMEOUT + P-HB-INTERVAL;
serving a corrupt document then a good one converges to the good one.
*Trace:* ADR-1, ADR-16; WP-1..4, LIV-1, FM-10..14.

**REQ-11 — Status reporting.** [MUST]
On request, the worker reports: its version, the applied assignment id, a per-chunk
availability map for the assignment's chunk list, stored bytes, and the current epoch.
The report is internally coherent — all fields derive from one state snapshot — and no
staler than P-HB-STALENESS. [Coherence is intent, currently violated: GAP-11.]
*Acceptance:* flip the assignment while status requests stream; no response ever pairs
assignment id A with an availability map computed for assignment B (CT-3).
*Trace:* ADR-16; RP-21, INV-30, LIV-6, OB-1..3.

**REQ-12 — Execution logging and pull delivery.** [MUST]
Every admitted query — success or error — produces exactly one durable log record
(identity, the original query, outcome summary, timings). Collectors pull records by
cursor; records are retained at least P-LOGS-RETENTION. Queries rejected before
admission produce no record (ADR-7).
*Acceptance:* run K admitted queries, crash and restart, then pull: exactly K records
(no loss for queries whose response was sent, modulo ADR-13's accepted crash window),
resumable by cursor with no duplicates or gaps across pages.
*Trace:* ADR-7, ADR-8, ADR-13; WP-16/17, RP-22, INV-23/32, CN-6.

**REQ-13 — Operational transparency.** [MUST]
The worker exposes pull-based operational metrics and a machine-readable local status
surface sufficient to decide every liveness property in 08 from the outside
(12 §mapping).
*Acceptance:* every OB signal exists at the binding surface (IB-30) and moves when its
underlying state moves; the "lying metrics" harness rule (12) passes.
*Trace:* OB-1..14, IB-30/31.

## Quality

**REQ-20 — Query authentication.** [MUST]
A query is served only if its signature verifies against the sending peer's identity,
binds this specific worker, and its timestamp is within P-TS-WINDOW of the worker's
clock. Responses are signed so the portal can prove result provenance.
*Acceptance:* mutated-field, wrong-worker, expired, and unsigned queries are all
rejected with the taxonomy's `bad_request` before any CU is spent; a valid response
verifies against the worker's identity.
*Trace:* RP-2, INV-25, IB-13.

**REQ-21 — Compute metering.** [MUST]
Each admitted query charges one CU against the querying operator's per-epoch allocation
at admission; after execution, the unused fraction for a partial-range query is
refunded. Operators without allocation, or with an exhausted budget, are rejected with a
retry hint where one exists. An overload rejection after admission keeps the charged
unit (ADR-6).
*Acceptance:* CU-conservation property test: for any admitted query sequence, net spend
per operator equals the sum of covered-fraction chips, except overload rejections which
cost 1; rejections before admission cost 0.
*Trace:* ADR-6, ADR-7; RP-3/4, INV-15, WP-16.

**REQ-22 — Overload rejection, not collapse.** [MUST]
Beyond declared concurrency bounds (P-Q-QUEUE, P-Q-PAR, P-REJECT-CONC), the worker sheds
load with typed, retryable rejections carrying a P-RETRY-HINT, and recovers full service
when pressure subsides. Resource use under flood is bounded.
*Acceptance:* a query storm at W-QPS-STORM yields only typed rejections (no stream
resets below the reject fan-out bound, no process death, memory below P-MEM-CEIL);
service latency recovers to the S1 SLO within LIV-8's bound after the storm.
*Trace:* ADR-9; RP-4, LIV-8, FM-40..44, SLI-7.

**REQ-23 — Crash-only chunk store.** [MUST]
The chunk store needs no clean shutdown: after a process crash at any point, recovery
adopts exactly the committed chunks, sweeps all transient residue, and serves queries
with no manual repair. Power-loss durability of chunk payloads is a declared, bounded
exception. [Payload integrity after power loss is intent, currently violated: GAP-5.]
*Acceptance:* kill-point matrix (CT-2) over every transition: recovered available set ≡
some committed prefix; no residue accumulates across repeated kills (RS-6).
*Trace:* CN-3/4/5, INV-40..42, WP-15, WP-23.

**REQ-24 — Hostile-input robustness.** [MUST — intent, currently violated: GAP-4]
No input — query bytes, assignment document, origin payload, log request — may terminate
the process, corrupt either store, or cause unbounded memory growth. Malformed input
yields a typed error (queries) or a rejected-and-retried document (assignments), with an
alarm.
*Acceptance:* fuzz corpus over both surfaces (CT-9) and the input-fault corpus (CT-4)
run to completion with the process alive and both stores intact.
*Trace:* ADR-18; FM-1, FM-10..23, INV-36.

**REQ-25 — Bounded reconciliation blast radius.** [SHOULD]
A single assignment application SHOULD NOT delete more than the P-DEL-FLOOR fraction of
the store; a wipe-inducing assignment is held, alarmed, and re-evaluated on every pass.
The hold is bounded: it lapses after P-DEL-HOLD-MAX, so a shrink the network keeps
republishing is eventually obeyed and the held bytes stay accountable to RS-3. An
operator override releases it sooner.
*Acceptance:* publish an assignment dropping all chunks: the store retains its data, an
alarm level is raised (OB-12), a subsequent restoring assignment resumes normal
reconciliation, and — absent one — the eviction proceeds once P-DEL-HOLD-MAX elapses.
*Trace:* ADR-17; WP-14, RS-5, FM-13.

## Explicitly unspecified

Tests MUST NOT pin any of the following; each is free to change without notice:

- The order and timing of downloads beyond the WP-13 fairness rule; which eligible chunk
  starts first.
- Byte-level output of compression; anything about temporary on-disk names or the
  encoding of dataset directory names.
- Result byte layout beyond the format contracts in 14 (e.g. field order inside a line).
- Relative order of log records sharing a timestamp beyond the RP-22 cursor rule.
- Behavior when two processes share a store with the *same* identity (only the
  different-identity case is specified, CN-9).
- The scheduling of background maintenance between transitions, provided maintenance
  transparency (CN-7) holds.

## Open questions

| OQ | Question | Blocking | Owner |
|---|---|---|---|
| OQ-1 | Is the availability map's bit order (worker's sorted chunk order) identical to the scheduler's assignment chunk order for suffix-forked chunks? A mismatch silently corrupts scheduler-side interpretation. | REQ-11 acceptance; IB-12 | network/scheduler team |
| OQ-2 | The log response ceiling reserves a margin below the transport maximum (P-LOGS-RESP-MAX); why the margin is needed is unrecorded. Right-size or document. | IB-20 | worker team |
| OQ-3 | The multi-version chunk-application feature (staged assignment tracking) is compiled out of shipped builds while tests exercise it. Ship it or retire it? | 13 matrix honesty | worker team |
| OQ-4 | Status reports are unsigned. Is unauthenticated status by design, or should it be signed like results? | RP-21 | network team |
| OQ-5 | The worker's log records hash the uncompressed result while the portal's records hash the compressed bytes; cross-checking will systematically mismatch. Which is canonical? | INV-23, GAP-14 | network team |
| OQ-6 | No per-query execution deadline exists (P-Q-DEADLINE ⚠). What bound should the network promise? Portals abandon an attempt at 60 s, so the worker bound must sit strictly below (target: 55 s) for timeout verdicts to ever be observed by them. | LIV-3, GAP-8 | worker team |
| OQ-7 | Portal fork recovery depends on distinguishing the worker's anchor-mismatch verdict, which today exists only as an unstable `server_error` message string (GAP-30, GAP-31). Promote it to a stable wire surface (error variant or subcode, jointly with GAP-19's subcode design), or pin the strings in IB-13? | RP-20, IB-13 | network + portal teams |
| OQ-8 | The network-state document's `effective_from` is honored by portals (fleet-coordinated cutover) but ignored by the worker (IB-40): during the window the worker may already reshuffle chunks that portals still route to under the previous assignment. Should WP-1 delay application until the effective time? | WP-1, IB-40 | network/scheduler team |
