# 07 — Invariants

Home doc for `INV`. Bands: 1..9 structural · 10..19 transition legality ·
20..29 response semantics · 30..34 reporting · 35..39 isolation ·
40..49 durability/recovery. Scope tags per README conventions.

## Structural

**INV-1 — Set partition.** [state]
`A ∩ D = ∅`, and the pending set is exactly `P = N \ A \ D` at every observable point.
A chunk is in at most one of {available, downloading}; nothing is fetched that is
already held or in flight.
*Why:* prevents double downloads, phantom availability, and lost chunks between sets.
*Check:* CT-1 — model-conformance assertion after every transition; state exposed via
OB-1 gauges and the status read.

**INV-2 — Store correspondence.** [state]
`A` = exactly the set of committed chunks physically present in the chunk store. No
manifest exists to diverge from reality; a chunk is available iff its committed form is
present.
*Why:* the store is self-describing; any cached availability that outlives the data
serves queries from nothing.
*Check:* CT-1/CT-2 — after arbitrary transition + crash sequences, compare the status
read's implied A against a direct store scan.

**INV-3 — Layout well-formedness.** [state]
Within a dataset, the block ranges of any two distinct committed chunks are either
disjoint or identical (suffix forks, ADR-4). Partial overlaps never exist among adopted
chunks.
*Why:* overlapping ranges make coverage (RP-11) ambiguous and progress unsound.
*Check:* CT-2/CT-4 — recovery over corpus stores with crafted overlaps must refuse
adoption; property tests never produce an overlapping store.

**INV-4 — Pin validity.** [state]
`dom(L) ⊆ A` and every pin count is positive: only available chunks are pinned, and a
pin outlives neither its query nor its chunk's availability entry being consumed by
eviction (which INV-12 forbids while pinned).
*Why:* a pin on a nonexistent chunk is a dangling read; an unpinned executing query is
an eviction race.
*Check:* CT-1/CT-3 — assertion in the reference model; concurrency swarm evicting under
load.

**INV-5 — Log-store well-formedness.** [state]
Log records have unique query ids; the sequence is totally ordered by the cursor
⟨timestamp, query id⟩; every record's timestamp is within the retention window or the
record is eligible for pruning.
*Why:* duplicate ids break at-least-once delivery accounting and billing evidence.
*Check:* CT-1/CT-7 — structural validator over pulled logs; soak with replayed ids.

## Transition legality

**INV-10 — Frame condition.** [transition]
Model state changes only via the cataloged transitions WP-10..17, each triggered by its
input event (DEF-30). Absent inputs, state is constant: no background process changes
`A`, `N`, `D`, `L`, or unexpired `Q`.
*Why:* catches whole classes of bugs — spontaneous eviction, maintenance mutating
logical state, phantom downloads.
*Check:* CT-1 — quiescence: with inputs stopped, two spaced state observations are
identical (modulo log pruning at the retention edge).

**INV-11 — Destructive ops are explicit.** [transition]
Committed data leaves the store only via WP-14 (evict) and log records only via WP-17
(prune). No other path — recovery, maintenance, error handling, incoming downloads —
deletes committed data.
*Why:* data loss must be enumerable; "cleanup" code paths are where stores lose data.
*Check:* CT-2 — kill-point matrix asserts recovered stores never lack a chunk that was
committed and never evicted; ledger comparison (HC-2).

**INV-12 — Eviction legality.** [transition]
WP-14 fires only for chunks that are simultaneously: committed (`c ∈ A`), unassigned
(`c ∉ N`), and unpinned (`L(c) = 0`). Mass eviction is additionally bounded by REQ-25.
*Why:* protects executing queries and assigned data from any reconciliation bug.
*Check:* CT-1/CT-3 — model conformance; swarm test querying chunks while shrinking
assignments.

**INV-13 — Commit provenance.** [transition]
A chunk committed by WP-12 contains exactly the files the assignment names for it, with
exactly the bytes the data origin served for those files.
*Why:* provenance fidelity — the store must be a faithful replica; silent truncation or
substitution poisons every future query.
*Check:* CT-1 — HC-2's origin ledger is the oracle: compare committed files against
served bytes.

**INV-14 — Application atomicity.** [transition]
WP-10 replaces `X` and `N` wholesale in one transition; no observation sees a mix of
two assignments' desired sets, and `A`/`D`/`L` are untouched by application itself.
*Why:* torn desired-sets produce spurious eviction/fetch churn and incoherent status.
*Check:* CT-3 — status reads race assignment flips; each observed N must equal some
applied assignment's slice.

**INV-15 — Metering conservation.** [transition]
Per operator: net CU spend = Σ chips of its admitted queries, +1 per post-admission
overload rejection (ADR-6), +0 per pre-admission rejection. A CU is spent iff a query
was admitted (ADR-7); refunds never exceed the spend they offset; bucket level never
exceeds P-CU-BURST.
*Why:* the economic contract; over-charging steals from operators, under-charging
invites overload.
*Check:* CT-1 — CU-conservation property test against the reference bucket model
(exists today as unit tests; promote to property form).

## Response semantics

**INV-20 — Response soundness.** [response]
Every admitted query yields exactly one response, drawn from the closed taxonomy
(RP-20), signed. Error responses carry no result data. No admitted query yields zero
or two responses (unanswered drops are pre-admission only, RP-20's last row).
*Why:* portals must be able to treat the response stream as authoritative.
*Check:* CT-5 — interface conformance: response accounting per injected query.

**INV-21 — Result provenance.** [response]
Successful result data derives exclusively from the pinned chunk's committed content
via the declared evaluation function — never from partial downloads, evicted data,
another chunk, or another query's buffers.
*Why:* cross-chunk or torn reads are silent data corruption at the consumer.
*Check:* CT-1/CT-3 — HC-2 ledger + reference evaluation comparison, raced against
churn.

**INV-22 — Coverage honesty.** [response]
`last_block` obeys RP-11 exactly: never exceeds the requested end; equals the truncation
point on early stop; equals range end on empty selection; resumption per RP-13 loses
nothing and duplicates nothing.
*Why:* the entire incremental-consumption protocol rests on this one number.
*Check:* CT-1 — resumption-equivalence property test (chunked reads ≡ whole read).

**INV-23 — Delivery-log agreement.** [response]
The wire response and the log record of one query agree on the outcome class and
content summary; a downgrade (RP-14) is a downgrade in both. No path produces a
successful response with an error log or vice versa.
*Why:* billing disputes are adjudicated from logs; divergence is unauditable.
*Check:* CT-5 — compare pulled log records against received responses per query id.

**INV-24 — Response size bound.** [response]
No response's uncompressed data exceeds P-RESP-MAX; no encoded response exceeds the
transport ceiling (IB-2); oversized results downgrade rather than truncate mid-block.
*Why:* transport-level failures on oversize look like network faults and are
undebuggable.
*Check:* CT-4/CT-6 — boundary corpus at the budget edges.

**INV-25 — Signature validity.** [response]
Every response (success or error) verifies against the worker's network identity; the
signed payload binds the query id (and for successes, content hash and `last_block`).
*Why:* unforgeable provenance is the network's trust anchor for served data.
*Check:* CT-5 — verify every response in every suite run.

**INV-26 — Fault attribution.** [response]
`bad_request` is returned only for defects decidable as a pure function of the request
bytes and network-public data (signature, envelope, query text). A rejection whose
verdict depends on worker-local state — capacity (RP-4), store content or integrity
(FM-32), clock freshness (RP-20 freshness verdict) — surfaces in a worker-fault class,
never as a client error (ADR-20). [Known-violated: freshness — GAP-33; store
corruption — GAP-5 (FM-32).]
*Why:* clients treat client-fault classes as terminal and do not reroute;
misattribution converts one worker's local condition into unrecoverable client-visible
failures.
*Check:* CT-4 — induce each worker-local condition (skewed clock, corrupt store,
saturated capacity) against valid queries; assert the outcome class is never
`bad_request`.

## Reporting

**INV-30 — Status coherence.** [response]
All fields of one status report derive from a single state snapshot: the unavailability
map is computed against the same assignment whose id the report carries, and its length
equals that assignment's chunk-list length. [Known-violated: GAP-11.]
*Why:* a scheduler indexing the map by the wrong assignment misroutes the whole
network's queries.
*Check:* CT-3 — status reads raced against assignment application; length and id
cross-check.

**INV-31 — Metrics honesty.** [state]
Every OB gauge equals its model quantity at observation (within one update interval):
available/downloading/pending counts match the sets; the running-query gauge matches
in-flight admitted queries; counters count what their names claim. [Partially violated:
the pre-admission and outcome counters still misreport — GAP-17. The running-query gauge
is now honest and CT-6-checked.]
*Why:* operators act on these numbers; a lying gauge converts incidents into mysteries.
*Check:* CT-1/CT-6 — scraper cross-checks gauges against harness-known state
(12 §lying-metrics rule).

**INV-32 — Log completeness.** [transition]
Exactly one log record per admitted query — including downgrades, post-admission
errors, and overload keeps. No record for pre-admission rejections. [Known-violated at
the margins: duplicate query ids and oversized records are silently dropped — GAP-12,
GAP-14.]
*Why:* the worker's revenue and the network's audit trail.
*Check:* CT-5/CT-7 — per-query-id reconciliation of injected load vs pulled logs.

## Isolation

**INV-35 — Cross-client isolation.** [response]
No query's response or log record is influenced by another client's concurrent
activity, beyond shared-capacity rejections (RP-4) and metering of the *same*
operator. One operator's exhausted bucket never affects another's admission.
*Why:* multi-tenancy; noisy neighbors must cost capacity, not correctness.
*Check:* CT-8 — noisy-neighbor swarm with per-client result verification.

**INV-36 — Query fault containment.** [response]
A failing query — engine panic, resource exhaustion inside evaluation, malformed body —
yields a taxonomy error for that query only. It never terminates the process, poisons
the engine pool, or fails other queries. (ADR-5.)
*Why:* one bad query must never be a denial of service on the worker.
*Check:* CT-4 — fault corpus including known panic triggers; process-liveness
assertion.

**INV-37 — Store/query non-interference.** [state]
Reconciliation (downloads, evictions, sweeps) never blocks admitted queries beyond
declared capacity limits, and queries never block reconciliation indefinitely (pins
defer individual evictions, LIV-4 bounds the deferral).
*Why:* the writer/reader/maintenance non-interference matrix collapses to this pair.
*Check:* CT-3/CT-6 — throughput of each side measured while the other saturates.

## Durability / recovery

**INV-40 — Recovery soundness.** [recovery]
Post-recovery `A` ⊆ committed-and-never-evicted chunks, with full committed content
(CN-3, CN-4 tiers). No partial chunk, residue, or foreign entry is ever adopted.
*Why:* recovery is the last line against corruption becoming service.
*Check:* CT-2 — kill-point matrix, store diffed against the commit ledger.

**INV-41 — Residue convergence.** [recovery]
Transient residue (partial downloads, in-flight eviction remnants) is fully swept by
the next recovery, and repeated crash/recover cycles do not accumulate residue or
grow the store without bound (RS-6).
*Why:* crash leftovers must not pin disk or wedge future commits.
*Check:* CT-2/CT-7 — crash-loop soak; store size and entry census monotonicity.

**INV-42 — Recovery idempotence.** [recovery]
Two consecutive recoveries with no intervening inputs adopt identical state (CN-5);
recovery itself never triggers WP-14 or WP-11.
*Why:* recovery code that mutates is recovery code that destroys under repeated crash.
*Check:* CT-2 — double-recovery comparison built into every kill-point run.

**INV-43 — Log-record durability.** [recovery]
Every log record appended before a crash is present after recovery (CN-6), modulo the
ADR-13 in-flight window. Pruning respects retention even across restarts.
*Why:* billing evidence must survive exactly as promised, no more (privacy) and no
less (revenue).
*Check:* CT-2 — logs pulled after crash compared to pre-crash acknowledged appends.

## Reading the catalog in tests

- Every CT-1 property run asserts INV-1..5 after each transition and INV-10..15 across
  each transition, using the reference model (13) as oracle.
- Every response in any suite passes the structural validators, which encode
  INV-20..25.
- CT-2 (kill-point matrix) owns INV-40..43; CT-3 (concurrency swarms) owns INV-14, 30,
  37 and races INV-4/12/21.
- CT-4/CT-9 (fault + fuzz corpus) own INV-36 and probe INV-3/24.
- CT-5/CT-7/CT-8 own the accounting and isolation set: INV-15, 20, 23, 32, 35.
