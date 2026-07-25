# 11 — Performance

Home doc for `SLI`, `PF`, `HZ`, and the workload model `W-*`. Bands: SLI-1..8 ·
PF-1..5 · HZ-1..15.

## SLI definitions

All black-box measurable by the harness (HC-6/HC-9/HC-10).

| # | SLI | Definition (one line) |
|---|---|---|
| SLI-1 | query latency | time from query send to last response byte, per outcome class, p50/p95/p99 |
| SLI-2 | query throughput | successful responses per second at a fixed offered load |
| SLI-3 | rejection rate | fraction of well-formed queries answered `server_overloaded` or dropped |
| SLI-4 | download throughput | committed chunk bytes per second while pending work exists |
| SLI-5 | assignment application latency | publish of a new assignment id → heartbeat reports it applied |
| SLI-6 | convergence latency | publish → all-zero unavailability map (LIV-1's witness) |
| SLI-7 | status staleness | state change → change visible in a status read (LIV-6's witness) |
| SLI-8 | log delivery lag | query response sent → its record retrievable by a collector |

## SLO table

Targets are ⚠ provisional pending ADR-19; baselines are the honest current knowledge
(measured numbers where the repo records them, "unknown" otherwise — the CT-6 baseline
run replaces every "unknown").

| SLI | Scenario | Target ⚠ | Known baseline |
|---|---|---|---|
| SLI-1 p95 | S1 steady | P-SLO-Q-P95 | unknown; row-oriented encoding dominates cost — columnar+fast-compression measured 18–34× cheaper on the serialization stage (bench branch, fixture chunk) |
| SLI-1 p99 | S2 query-storm | P-SLO-Q-P99-STORM | unknown |
| SLI-3 | S1 steady | 0 | assumed ~0; unmeasured |
| SLI-3 | S2 query-storm | ≤ P-SLO-REJECT-STORM (all typed, none dropped below the ADR-9 bound) | unknown |
| SLI-4 | S5 backfill | P-SLO-DL-RATE | unknown; bounded by P-DL-CONC and origin |
| SLI-5 | S3 churn | P-SLO-ASSIGN-APPLY | ≤ ~P-ASSIGN-POLL + fetch time observed informally; unmeasured |
| SLI-6 | S5 backfill | P-SLO-CONVERGE | unknown |
| SLI-7 | S1 steady | P-HB-STALENESS | **violated at scale**: the status path walks the full store; "minutes on a busy disk at ~30k chunks" (recorded in the store-walk fix rationale) — GAP-15 |
| SLI-8 | S1 steady | P-SLO-LOG-LAG (≥ P-LOGS-LAG by construction) | unknown |

## Resource-bound requirements

**PF-1 — Derivable memory ceiling.** [MUST] Peak memory is bounded by a formula over
configuration: P-MEM-CEIL ⚠ ≈ base + (concurrent queries × the RP-24 per-query multiple
of P-RESP-MAX) + assignment-document size + fetch buffers. No unbounded queue contributes
(the execution-queue depth, reject fan-out, and intake queues are all P-bounded; the
post-admission execution backlog is bounded by P-Q-PAR).

**PF-2 — End-to-end backpressure.** [MUST] Every producer-consumer edge is bounded:
transport intake buffers (P-Q-ACCEPT-BUF, P-Q-REQ-BUF, and the lossy shared event queue
P-EVENT-QUEUE), intake queues (P-Q-QUEUE), execution concurrency (P-Q-PAR), reject
signing (P-REJECT-CONC), log-read queue (P-LOGS-QUEUE), status reads (P-STATUS-CONC ⚠ —
unbounded today, GAP-29), fetch concurrency (P-DL-CONC), assignment intake
(coalesce-to-newest, WP-4). Saturation propagates as typed rejection outward — or a
transport-level drop at the outer buffers (RP-20's *(no response)* row) — never as
unbounded buffering inward.

**PF-3 — Maintenance budget, two-sided.** [MUST] Background maintenance (sweeps,
reclamation, accounting walks) consumes bounded resources (upper side) *and* is
guaranteed a minimum cadence under load (lower side) so debt cannot grow unboundedly
(LIV-14). Store-size accounting specifically must not serialize the reconciliation
loop (RS-8).

**PF-4 — Startup work scheduling.** [MUST] Startup cost is proportional to store size
only through the recovery scan and residue sweep; neither repeats per loop thereafter.
Acceptance of queries does not wait on any network dependency (LIV-5).

**PF-5 — Benchmarking regime.** [MUST — harness obligation] CT-6 maintains committed
baselines for every SLI under S1–S6, characterizes the saturation knee (offered load
where SLI-1 p99 departs its plateau), includes overload phases (beyond-knee load
with recovery measurement, LIV-8), and runs the executor-contention probes HZ-8/9. Regressions beyond P-PERF-NOISE gate per MG-6.

## Workload model

| Param | Meaning | Reference value ⚠ |
|---|---|---|
| W-CHUNKS | committed chunks in the store | 30 000 (observed scale in fix rationale) |
| W-DATASETS | datasets in the assignment | 20 |
| W-CHUNK-BYTES-MAX | largest single chunk | 2 GiB |
| W-CHUNK-FILES | table files per chunk | 5 |
| W-QPS | steady offered query load | 50/s |
| W-QPS-STORM | storm offered load | 20 × W-QPS |
| W-DL-RATE | healthy origin sustained transfer rate | 100 MiB/s |
| W-CHURN-RATE | chunks entering/leaving the assignment per hour | 5 % of W-CHUNKS |
| W-OPERATORS | distinct operators querying | 30 |
| W-LOG-RATE | log-record bytes per second at W-QPS | derived: W-QPS × mean record size |

Named scenarios (the SLO table's scenario column): **S1 steady** (W-QPS spread over
W-DATASETS datasets, W-CHUNK-FILES files per chunk, stable assignment) · **S2
query-storm** (W-QPS-STORM burst, stable assignment) · **S3 assignment-churn**
(W-CHURN-RATE flips under S1 load) · **S4 cold-start** (restart over W-CHUNKS store
under S1 load) · **S5 backfill** (empty store, full assignment) · **S6 noisy-neighbor**
(one dataset's origin failing + one of the W-OPERATORS operators at bucket exhaustion,
under S1).

## Hazard register

Mechanism → threatened property → probe. Hazards are timeless risk pointers; dated
defects live in the gap register (13).

| # | Mechanism | Threatens | Probe |
|---|---|---|---|
| HZ-1 | full-store accounting walk inside the reconciliation loop | LIV-2/4/6, SLI-7, RS-8 | S4/S5 with W-CHUNKS store: measure loop iteration time vs store size |
| HZ-2 | global (not per-origin) fetch retry backoff | LIV-11, SLI-4 | S6: one dataset 404s; measure healthy dataset's SLI-4 |
| HZ-3 | no execution deadline or disconnect cancellation | LIV-3/8, PF-1 | slow-query flood with disconnecting clients; watch slot occupancy |
| HZ-4 | result built via multiple full-size buffers (uncompressed + compressed + encoded) | PF-1 | concurrent max-size queries; peak RSS vs P-MEM-CEIL |
| HZ-5 | log-store size high-water behavior | RS-7, LIV-14 | S2 burst then idle soak; on-disk size trend |
| HZ-6 | assignment application scans the whole network document | SLI-5, PF-2 | apply latency vs document size scaling probe |
| HZ-7 | status snapshot deep-copies the full chunk sets | SLI-7, HZ-1 interplay | status-read latency vs W-CHUNKS |
| HZ-8 | blocking filesystem calls on async executors | LIV-6, SLI-1 tail | slow-disk injection (HC-2 delay on store I/O); p99 shift |
| HZ-9 | compression/signing on shared executor threads | SLI-1 tail under mix | mixed large/small query storm; small-query p99 |
| HZ-10 | millisecond-resolution transient-name collisions | WP-13 noise, RS-6 | rapid re-fetch of one chunk; abort-storm counter |
| HZ-11 | pin refcount width vs concurrency ceiling | INV-4/12 | saturation probe pinning one chunk at max concurrency |
| HZ-12 | unbounded decompression of the assignment document | FM-12, PF-1 | HC-1 serves a decompression bomb; RSS bound |
| HZ-13 | the auxiliary noise protocol answers any member peer with an unbounded random-byte stream (IB-4) | PF-1/PF-2 margins (egress, CPU) | open N noise streams; egress and CPU stay bounded |
| HZ-14 | the whole response must be written within P-STREAM-TIMEOUT, so a P-RESP-MAX response needs ≥ P-RESP-MAX / P-STREAM-TIMEOUT (~12.5 MiB/s) reader throughput; slower readers turn large results into resets — and long client-side cooldowns | REQ-1 for large results, SLI-1 tail | throttled-reader driver at max-size responses; reset rate vs reader rate |
| HZ-15 | flood-shed stream resets (ADR-9) are indistinguishable from timeouts to portals, which apply their longest per-worker cooldown plus a congestion signal — a brief flood can evict the worker from portal pools for minutes | LIV-8 as observed by clients, SLI-3 | CT-6 storm phase: driver mimics portal cooldown policy; measure post-storm re-uptake |

## Benchmark numbers on record

From the repository's benchmark report (bench branch, fixture chunk of 1 397 blocks),
serialization+compression stage only: the current row-oriented default versus the
columnar fast path = 34× (blocks table), 21× (logs), 18× (transactions); versus a
binary row encoding ≈ 13–16×. These motivated ADR-11 and seed the S1 SLI-1 baseline
once CT-6 first runs.
