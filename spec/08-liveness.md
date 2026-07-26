# 08 — Liveness

Home doc for `LIV`. Band: LIV-1..14. Every property names its precondition, bound, and
witness observable; a bound symbol marked ⚠ has no ratified target yet
(ratification: ADR-19; values in [15-parameters.md](15-parameters.md)).

## §0 — Environmental definitions

Liveness claims hold only under these conditions; outside them the required behavior is
09's (degrade + alarm, never silent hang):

- **Healthy scheduler**: the network-state and assignment documents are reachable,
  well-formed, and include this worker, with fetch latency within
  P-ASSIGN-FETCH-TIMEOUT.
- **Healthy origin**: every assigned chunk file downloads successfully within
  P-DL-FILE-TIMEOUT without stalls beyond P-DL-STALL-TIMEOUT.
- **Healthy registry**: epoch and allocation reads succeed within the polling cadence.
- **Adequate resources**: free disk ≥ assigned-data size + RS-3's excess bound; memory
  within P-MEM-CEIL ⚠; CPU not saturated by co-tenants.
- **Quiescent**: no input events (DEF-30) arriving; in-flight work drained.

## Properties

**LIV-1 — Assignment convergence.**
*Pre:* healthy scheduler + origin + adequate resources; an assignment is published and
stable.
*Bound:* P-ASSIGN-POLL + P-ASSIGN-FETCH-TIMEOUT + (pending-bytes / W-DL-RATE) +
P-CONVERGE-SLACK ⚠.
*Witness:* heartbeat reports the new assignment id with an all-zero unavailability map
(OB-2/3).
*Check:* CT-1, CT-6.

**LIV-2 — Download progress.**
*Pre:* `P ≠ ∅`, healthy origin, free capacity (`|D| <` P-DL-CONC).
*Bound:* a WP-12 commit occurs within P-DL-FILE-TIMEOUT × W-CHUNK-FILES +
P-DL-BACKOFF-MAX.
*Witness:* the committed-chunks counter advances (OB-4).
*Check:* CT-1.

**LIV-3 — Query termination.**
*Pre:* an admitted query.
*Bound:* a signed response (or downgrade) within P-Q-DEADLINE ⚠. [Currently unbounded:
GAP-8; OQ-6.]
*Witness:* response observed by the driver; running-query gauge returns to baseline
(OB-6).
*Check:* CT-1, CT-4.

**LIV-4 — Eviction convergence.**
*Pre:* `c ∈ A`, `c ∉ N`, last pin released; process alive.
*Bound:* store namespace removal within P-EVICT-BOUND ⚠ of the pin release, or within
P-DEL-HOLD-MAX + P-EVICT-BOUND when REQ-25's deletion floor withholds the batch — in
both cases without requiring any further input event. [Currently violated for the
pin-release path: eviction waits for the next loop wake-up, potentially forever —
GAP-6. The floor's hold has its own timer.]
*Witness:* evicted-chunks counter advances (OB-4); stored-bytes gauge falls (OB-5).
*Check:* CT-1, CT-3.

**LIV-5 — Startup bound.**
*Pre:* process start over a store of W-CHUNKS committed chunks; no corruption; on-chain
registration confirmed (an unregistered worker polls the registry and serves nothing
until listed — FM-54).
*Bound:* accepting queries within P-START-ACCEPT ⚠; acceptance is decoupled from
assignment intake (PF-4) — a dead scheduler delays convergence, never acceptance
(degraded service: `not_found`/empty status until the first application).
*Witness:* status endpoint responds; lifecycle phase timestamps (OB-10).
*Check:* CT-2, CT-6.

**LIV-6 — Status freshness.**
*Pre:* process alive.
*Bound:* every status report reflects state no older than P-HB-STALENESS. [At risk
when store walks run long: GAP-15; probes HZ-1/7.]
*Witness:* harness flips state and polls status until reflected; elapsed ≤ bound.
*Check:* CT-3, CT-6.

**LIV-7 — Log delivery progress.**
*Pre:* collector polls at least every P-LOGS-RETENTION − P-LOGS-LAG with resumable
cursors.
*Bound:* every admitted query's record is delivered before pruning; each response page
advances the cursor or sets has-more = false.
*Witness:* per-query-id reconciliation (CT-5's accounting).
*Check:* CT-5, CT-7.

**LIV-8 — Overload shed-and-recover.**
*Pre:* query pressure exceeding capacity for a finite interval, then subsiding.
*Bound:* during: typed rejections within P-REJECT-LATENCY ⚠; after: S1-scenario SLOs
restored within P-RECOVER-BOUND ⚠ with no restart.
*Witness:* rejection counters (OB-7) rise then stop; latency SLI returns to target.
*Check:* CT-6, CT-8.

**LIV-9 — Stall budget.**
*Pre:* work exists (pending chunks with healthy origin, or queued queries).
*Bound:* zero progress (no commit, no response) for longer than P-STALL-MAX ⚠ never
occurs silently — either progress resumes or an alarm level (OB-12) is raised.
*Witness:* progress heartbeat (OB-11) vs alarm state.
*Check:* CT-7.

**LIV-10 — Graceful shutdown.**
*Pre:* shutdown requested.
*Bound:* process exit within P-SHUTDOWN-BOUND; outcome crash-equivalent or better (every
in-flight query either completes or its client sees a transport close; the store needs
no repair beyond normal recovery).
*Witness:* exit code + subsequent clean recovery (CT-2 reuses this).
*Check:* CT-2.

**LIV-11 — No cross-dataset starvation.**
*Pre:* ≥2 datasets with pending chunks; one origin path failing, others healthy.
*Bound:* healthy datasets' commits proceed within LIV-2's bound; a failing chunk delays
only itself. [Currently violated: the retry backoff is global — GAP-7; probe HZ-2.]
*Witness:* per-dataset commit progress under partial-fault injection.
*Check:* CT-8.

**LIV-12 — Metering convergence.**
*Pre:* healthy registry; an operator's allocation exists on-chain (epoch semantics:
DEF-22).
*Bound:* the operator's queries admit within P-EPOCH-POLL + P-CONVERGE-SLACK ⚠ of
process start or epoch change — allocations are re-read only when the observed epoch
increases (DEF-22), so a mid-epoch on-chain allocation change becomes effective at the
next epoch boundary (the cold-start rejection window is bounded — GAP-25 tracks
shrinking it).
*Witness:* first admitted query per operator after start; epoch gauge (OB-9).
*Check:* CT-5.

**LIV-13 — Divergence is convergence-or-alarm.**
*Pre:* any persistent external contradiction — unfetchable assignment, unfixable chunk
(commit collision), undeletable eviction target, unreachable registry.
*Bound:* within P-STALL-MAX ⚠, the condition is either resolved by retry or surfaced as
a reason-coded alarm (OB-12) while the rest of the worker keeps serving. Silent
infinite retry of the same failing operation without an alarm is a violation.
*Witness:* alarm state with reason; continued service on unaffected paths.
*Check:* CT-4, CT-7.

**LIV-14 — Reclamation keep-up.**
*Pre:* steady assignment churn at W-CHURN-RATE.
*Bound:* store size stays within RS-3's amplification bound at all times — reclamation
keeps pace with eviction; residue and pruned-log space do not trend upward across
P-SOAK-WINDOW ⚠.
*Witness:* stored-bytes gauge (OB-5) trend over soak.
*Check:* CT-7.
