# 12 — Observability

Home doc for `OB`. Band: OB-1..15. Signals are named abstractly; the concrete metric
names, types, and endpoints are IB-30/31. Every signal is pull-readable by the operator
surface; cardinality is bounded (OB-14).

## Required signals

**OB-1 — Chunk-state gauges.** [MUST] Current sizes of A (available), D (downloading),
and P (pending). These are the INV-1 witnesses.

**OB-2 — Applied assignment identity.** [MUST] The applied assignment id as reported in
status (RP-21), readable locally.

**OB-3 — Unavailability summary.** [MUST] Count of desired-but-unavailable chunks (the
ones-count of DEF-13); zero is the LIV-1 witness.

**OB-4 — Reconciliation counters.** [MUST] Monotonic counters: chunks committed, fetch
aborts (distinguishing failure from cancellation ⚠ — today conflated, register row
GAP-17), chunks evicted.

**OB-5 — Space accounting.** [MUST] Stored bytes of the chunk store (RS-3's witness)
and of the log store (RS-7's witness ⚠ — the latter does not exist today, GAP-17).

**OB-6 — Query concurrency gauge.** [MUST] Currently executing admitted queries.

**OB-7 — Query outcome counters.** [MUST] Admitted-query count by outcome class
(RP-20), plus pre-admission rejection counters by cause (overload, no-allocation,
bucket-empty, freshness — OB-15, invalid) ⚠ — pre-admission causes are invisible today
(GAP-17).

**OB-8 — Result-size distribution.** [MUST] Histogram of uncompressed result sizes with
usable buckets ⚠ [today's histograms have no buckets — GAP-17].

**OB-9 — Metering state.** [MUST] Current epoch gauge; per-outcome CU counters
sufficient to audit INV-15 externally at operator granularity.

**OB-10 — Lifecycle phases.** [MUST] Timestamps/gauges marking: process start, store
recovery done, on-chain registration confirmed (FM-54), accepting queries (LIV-5
witness), first assignment applied. [The registration phase is invisible today —
GAP-28.]

**OB-11 — Progress heartbeat.** [MUST] A signal that distinguishes *idle* (no pending
work) from *stalled* (pending work, no progress): e.g. last-commit timestamp + pending
gauge. LIV-9's witness — a monitoring system must be able to derive "stalled for >
P-STALL-MAX" from exported signals alone.

**OB-12 — Alarm states.** [MUST] Reason-coded, level-readable alarm signals (plus edge
events in logs) for at least: assessed worker state (DEF-32), assignment intake failing
(FM-10/12), fetch quarantine (FM-22), eviction/reclamation failure (FM-31), store
integrity refusal (FM-32), deletion-floor hold (REQ-25), sustained clock-skew
suspicion (FM-55). One current-state read answers "is anything wrong, and why".

**OB-13 — Assignment age.** [MUST ⚠ — does not exist today: GAP-23] Time since the last
successfully applied assignment, so a silently-dropped worker is externally detectable
(FM-14).

**OB-14 — Bounded cardinality.** [MUST] No signal's label space grows with untrusted
input (per-client or per-chunk labels are forbidden; per-outcome-class and per-dataset
are the ceiling).

**OB-15 — Clock-skew visibility.** [MUST ⚠ — does not exist today: GAP-33] A
freshness-rejection counter (the OB-7 breakdown) and a gauge estimating the
worker-clock offset from authenticated queries' timestamps, so a skewed worker is
diagnosable from its own metrics alone (FM-55) and the P-SKEW-ALARM alarm (OB-12) has
a level-readable witness. Scalar signals only (OB-14).

**OB-18 — Refused assignments.** [MUST] A counter of announced pairs rejected as
unusable (FM-12) — no entry for this worker, a write schema with no roster, a bundle that
doesn't cover the document, a document that cannot be read at all, a network-state pointer
that will not decode or names no document to fetch (counted per poll while it persists,
like FM-53d). A refusal keeps the
previous assignment in force, so nothing else moves: the chunk gauges hold, the reported
id holds, and a worker starved of usable documents looks exactly like one whose network
has gone quiet. This is the signal that separates them; which refusal it was stays in the
log. Scalar signal only (OB-14). Bound in IB-31.

**OB-17 — Unaddressable chunks.** [MUST] A counter of chunks the applied assignment
carries no usable address for: a base url that will not parse, or a version whose dataset
registers no generation (FM-11). It must be distinguishable from an ordinary download
failure — both leave the chunk missing and both move `chunks_failed_download`, but one is
an origin that may come back and the other is a document that will still be unusable after
every retry, so without the distinction a scheduler publishing bad addresses reads exactly
like a flaky origin. Scalar signal only (OB-14). Bound in IB-31.

**OB-16 — Schema-source health.** [MUST] Whether a schema source is loaded, a counter of
failures to load one, and a counter of **pairs the scheduler published that do not hold
together**: an assignment refused because its bundle does not cover it (FM-53c), or a state
naming an assignment without a usable bundle (FM-53d). The last is the scheduler's invariant breaking
rather than the worker's, and it must be distinguishable from an ordinary intake failure —
a worker refusing every assignment because the pair it is served diverges looks identical
to one that cannot reach the network. Under `--assignment-source worker` a bundle that never
installs blocks every assignment (FM-53b) while no other signal moves — the chunk gauges
simply freeze, which reads exactly like a quiet network — so this is the only witness
that separates the two. Scalar signals only (OB-14). Bound in IB-31; the legacy
manifest's fetch failures (FM-53) have no signal yet.

## Property → observable mapping

Every LIV property must be decidable from exported signals:

| Property | Decided by |
|---|---|
| LIV-1/2 | OB-1 (P→0), OB-3 (→0), OB-4 commits advancing |
| LIV-3 | OB-6 returning to baseline, OB-7 outcome advancing |
| LIV-4 | OB-4 evictions advancing, OB-5 falling |
| LIV-5 | OB-10 phase timestamps |
| LIV-6 | OB-2/3 flip latency after a state change |
| LIV-7 | log delivery lag (SLI-8) via collector-side reconciliation |
| LIV-8 | OB-7 rejection counters rising then stopping; SLI-1 recovery |
| LIV-9/13 | OB-11 vs OB-12 (stall must surface as alarm) |
| LIV-10 | process exit + OB-10 on next start |
| LIV-11 | OB-4 per run under fault injection (per-dataset commit progress ⚠ — needs a per-dataset breakdown or harness-side inference; GAP-17) |
| LIV-12 | OB-9 epoch gauge; first-admission timing |
| LIV-14 | OB-5 trend over soak |

## The lying-metrics rule

For the harness, an incorrect signal is a failure of the same severity as an incorrect
response: CT runs cross-check every OB gauge/counter against harness-known ground truth
(injected load, ledger state, model state) and fail on divergence beyond one update
interval. A metric that cannot be cross-checked is a spec bug — extend the signal or
the harness until it can. (Current known liars are registered: GAP-17's
dead counter and bucketless histograms.)
