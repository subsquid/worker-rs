# 04 — Mutations

Home doc for `WP`. Bands: WP-1..9 intake and loop rules · WP-10..17 transition catalog ·
WP-20..29 commit, errors, restart continuity.

## The reconciliation loop

Conceptually (normative pseudocode; scheduling between steps is free; the transitions
are summarized in DEF-31):

```
loop:
    wait for: assignment applied | fetch finished | pin released | shutdown
    cancel fetches no longer desired                    # WP-13
    for c in A where c ∉ N and L(c) = 0: evict(c)       # WP-14
    while |D| < P-DL-CONC and P ≠ ∅:
        c := next_pending()                             # WP-3 fairness
        fetch(c)                                        # WP-11
```

**WP-1 — Assignment intake.** [MUST] The worker polls the network-state document every
P-ASSIGN-POLL and, on a changed assignment id, fetches the assignment document (DEF-5,
bounded by P-ASSIGN-FETCH-TIMEOUT) and applies it (WP-10). An unchanged id is a no-op.
Fetch/poll failures retry with jittered exponential backoff from P-ASSIGN-RETRY-BASE:
the poll stage caps at P-ASSIGN-RETRY-MAX, the document stage at P-ASSIGN-POLL, since a
pair is announced once and that backoff is the only thing that returns to it. A poll
failure is a state the worker cannot read at all — transport, or a body that is not a
JSON object. A state that reads but is not applicable — no pointer for the worker's mode,
a pointer that will not decode or names no document to fetch, or under
`--assignment-source worker` no usable bundle reference — is not a failure: nothing is
announced, the worker re-reads it at P-ASSIGN-POLL, and an unusable pointer alarms (OB-18)
as an unusable bundle reference does (OB-16, FM-53d); the backoff ladder would only delay
noticing that the scheduler has fixed it. Retries of
one stage MUST NOT starve intake of newer assignments: any newer announcement ends the wait
at once and supersedes the one that failed — a newer assignment, which is tried next, or the
network back on the pair in force, which retracts the failed one. A document rejected by
WP-2 is not retried at all — the verdict is a property of the document, not of the attempt.
⚠ Application timing relative to the document's declared effective time is OQ-8
(applied immediately today).

**WP-2 — Intake validation.** [MUST — intent, currently violated: GAP-4] Before
application, the assignment document is structurally validated and its decompressed size
bounded by P-ASSIGN-SIZE-MAX ⚠ (ADR-18). A document that fails validation, lacks an
entry for this worker, or fails credential decryption is rejected whole: the applied
assignment X, N, and the reported assignment id remain those of the last good
assignment, and an alarm is raised (FM-12). Per-item tolerance: an individual malformed
download address or credential entry MUST degrade to per-chunk fetch failure, never
process failure (FM-11).

**WP-3 — Fetch fairness.** [SHOULD] `next_pending()` picks a chunk from the dataset with
the fewest pending chunks (smallest-backlog-first), so a small or lagging dataset
completes ahead of a bulk backfill. Within a dataset, order is unspecified (02
§explicitly-unspecified).

**WP-4 — Single applier.** [MUST] At most one assignment application executes at a time;
an application in progress is never interrupted by a newer arrival (ADR-16), however many
arrive: under `--assignment-source worker` it runs to its verdict — applied or stalled —
first. Arrivals coalesce to the newest, so a pair the network has moved past is never
started; a stalled application yields to the newest at once.

## Transition catalog

**WP-10 — apply-assignment.**
*Pre:* a fetched assignment document that passed WP-2; no other application in progress.
*Post:* `X′ = new document`, `N′ = its slice for this worker`, `P′ = N′ \ A \ D`.
`A`, `D`, `L`, `Q` unchanged — application never deletes or interrupts anything by
itself; deletions happen only via subsequent WP-14. Applying a document yielding
`N′ = N` changes `X′` and, if fetches were given up under the previous budget (WP-13),
returns them to `P′` — an unchanged slice is not a no-op for reconciliation, which the
application must wake — and is otherwise a no-op.

**WP-11 — fetch-start.**
*Pre:* `c ∈ P`, `|D| < P-DL-CONC`.
*Post:* `D′ = D ∪ {c}` (so `c ∉ P′`). Fetch work happens outside the model state; only
WP-12/13 conclude it.

**WP-12 — commit.**
*Pre:* `c ∈ D`; every file of `c` fully retrieved.
*Post:* `c`'s file set is published into the chunk store by a single atomic namespace
operation (CN-1); `A′ = A ∪ {c}`, `D′ = D \ {c}`. Visibility to queries begins at this
transition and not before. If `c ∉ N` by commit time, the chunk is still committed and
then evicted by a later WP-14 (commit does not consult N).

**WP-13 — abort.**
*Pre:* `c ∈ D`; the fetch failed, timed out (per-file bound P-DL-FILE-TIMEOUT, stall
bound P-DL-STALL-TIMEOUT), or was cancelled (no longer desired).
*Post:* `D′ = D \ {c}`; all partial data of `c` is transient residue (RS-6), never
visible. If `c ∈ N′`, then `c ∈ P′` — failed fetches retry while desired, under backoff
from P-DL-BACKOFF-BASE capped at P-DL-BACKOFF-MAX, until the per-assignment attempt limit
is reached; the next assignment restores the budget. A fetch that failed because the
document carries no address for `c` (FM-11) is given up on at once instead, since a
document does not change between attempts. The backoff scope
SHOULD be per-origin or per-chunk so one failing chunk does not throttle others.
[Scope is intent, currently violated — the backoff is global: GAP-7.] Abort-path
noise from transient-name collisions is a registered hazard (HZ-10).

**WP-14 — evict.**
*Pre:* `c ∈ A`, `c ∉ N`, `L(c) = 0`.
*Post:* `A′ = A \ {c}`; the chunk leaves the store namespace atomically, then its bytes
are reclaimed (RS-4 two-phase; interactions: RS-9). This is the **only** transition by which committed data
leaves the store (INV-11). Eviction failure is a per-chunk degraded state with an alarm,
not process failure. [Intent, currently violated — eviction failure panics: GAP-6.]

**WP-15 — recover.**
*Pre:* process start.
*Post:* `A′ = { committed chunks found in the store }`, `D′ = ∅`, `L′ = ∅`, `X′ = ⊥`,
`N′ = A′` (nothing is deleted or fetched until the first assignment applies), `Q′ = `
the durable log records. All transient residue is swept (RS-6). Recovery MUST tolerate
unrecognized entries in the store (skip-and-alarm, CN-10); a malformed *layout* of
recognized chunks is a startup failure (INV-3). ⚠ Skip-vs-fail granularity for corrupt
individual chunks is GAP-20 / ADR-18.

**WP-16 — log-append.**
*Pre:* a query passed admission (DEF-25) and its delivery (DEF-26) was built.
*Post:* `Q′ = Q ⧺ [record]`. Exactly one record per admitted query (INV-32); the record
and the wire response derive from the same outcome (INV-23). Append failure is alarmed;
it never affects the response. Ordering: the response MAY be sent before the record is
durable (ADR-13 — accepted crash window).

**WP-17 — log-prune.**
*Pre:* records with timestamp older than `now − P-LOGS-RETENTION` exist.
*Post:* those records leave Q and their space is reclaimable (RS-7). Pruning is
oblivious to delivery: an undelivered record past retention is lost (accepted, cited by
RS-7); collectors must poll within the retention window.

## Commit, errors, restart

**WP-20 — Commit point and no-partial-visibility.** [MUST] The commit point of a chunk
is WP-12's single atomic publish. No query, status report, or metric may ever observe a
chunk in a state between "absent" and "fully committed". The same applies in reverse to
eviction.

**WP-21 — Idempotency under redelivery.** [MUST] Re-applying the currently applied
assignment is a no-op. Re-fetching an already available chunk is never scheduled
(`P = N \ A \ D` guarantees it); a commit colliding with an existing committed chunk of
the same ref MUST NOT corrupt the existing chunk — the incumbent wins and the incoming
copy is discarded as residue. [Incumbent-wins is intent; today the collision wedges into
an eternal retry: GAP-20.]

**WP-22 — Error classification.** [MUST] Mutation-path errors are **transient**
(retried under backoff: fetch failures, origin unavailability, assignment fetch
failures) or **integrity** (alarmed, quarantined, never silently retried forever:
layout violations, commit collisions, eviction failures). A transient error that
persists beyond P-STALL-MAX becomes an alarm (LIV-9) — retry forever, but never
silently. No input content may terminate the process (FM-1).

**WP-23 — Restart continuity.** [MUST] After WP-15, the worker resumes from exactly the
recovered committed state: previously committed chunks serve queries immediately;
nothing is evicted until an assignment applies (WP-15's `N′ = A′`); the first applied
assignment then drives normal reconciliation. Recovery is idempotent (CN-5) — crashing
during recovery and recovering again yields the same state.
