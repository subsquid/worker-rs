# 15 — Parameter registry

**Mutable doc #2.** As of: 2026-08-19, baseline `8d3f0c3`. Every `P-*` symbol used
anywhere in the suite has a row. **Observed** = what the implementation does today
(configuration default where operator-settable). **Target** = the intended bound; ⚠ =
proposed, unratified — ratification lands via the ADR named in the row (ADR-19 for the
batch unless stated).

## Assignment intake

| Parameter | Role | Observed | Target |
|---|---|---|---|
| P-ASSIGN-POLL | network-state poll period (WP-1) | 60 s | same |
| P-ASSIGN-FETCH-TIMEOUT | assignment document fetch timeout (WP-1) | 300 s | same |
| P-ASSIGN-RETRY-BASE | intake retry backoff base (WP-1) | 1 s, jittered exponential in both stages | same |
| P-ASSIGN-RETRY-MAX | intake retry backoff cap (WP-1) | 14 400 s (poll stage; the document stage caps at P-ASSIGN-POLL) | same |
| P-ASSIGN-SIZE-MAX | decompressed assignment size bound (WP-2, HZ-12) | **unbounded** — GAP-4 | 512 MiB ⚠ (ADR-18) |

## Downloads

| Parameter | Role | Observed | Target |
|---|---|---|---|
| P-DL-CONC | concurrent chunk fetches (WP-11) | 3 | same |
| P-DL-FILE-TIMEOUT | per-file fetch timeout; chunk watchdog = × file count (WP-13) | 60 s | same |
| P-DL-STALL-TIMEOUT | per-read stall bound (WP-13) | 3 s | same |
| P-DL-BACKOFF-BASE | fetch retry backoff base (WP-13) | 100 ms | same |
| P-DL-BACKOFF-MAX | fetch retry backoff cap (WP-13) | 300 s, **global scope** — GAP-7 | 300 s, per-origin scope ⚠ |

## Query service

| Parameter | Role | Observed | Target |
|---|---|---|---|
| P-Q-PAR | concurrent executing queries (RP-4) | 20, enforced | same |
| P-Q-QUEUE | intake queue depth per protocol surface (RP-4) | 16 | same |
| P-Q-STREAMS | concurrent message handlers per protocol surface (RP-4) | 32 | same |
| P-Q-ACCEPT-BUF | transport accept buffer per protocol surface (IB-2) | 128 | same |
| P-Q-REQ-BUF | transport parsed-request queue per protocol surface (IB-2) | 128 | same |
| P-EVENT-QUEUE | shared transport→worker event queue, lossy with drop counting (IB-2, PF-2) | 100 | same |
| P-STATUS-CONC | concurrent status-read handlers (RP-24) | **unbounded** — GAP-29 | 16 ⚠ |
| P-REJECT-CONC | concurrent rejection-signing bound (ADR-9) | 64 | same |
| P-Q-DEADLINE | per-query execution deadline (RP-5, LIV-3) | **none** — GAP-8 | 55 s ⚠ (OQ-6 — strictly below the portal's 60 s per-attempt abandonment) |
| P-Q-MSG-MAX | query message ceiling (IB-2) | 4 MiB + 1 KiB | same |
| P-SQL-MSG-MAX | SQL-surface message ceiling (IB-2) | 257 KiB | same |
| P-RESP-MAX | uncompressed result ceiling (RP-14); transport response ceiling (IB-2) | 250 MiB | same |
| P-RESP-BUDGET | engine early-stop result budget (RP-13) | 20 MiB | same |
| P-RETRY-HINT | retry-after on overload rejections (RP-4) | 1 000 ms | same |
| P-TS-WINDOW | query timestamp freshness window (RP-1) | 60 s | same |
| P-SKEW-ALARM | estimated clock-offset magnitude raising the FM-55 alarm (OB-15) | no signal exists — GAP-33 | P-TS-WINDOW / 2 ⚠ (ADR-20) |
| P-STREAM-TIMEOUT | transport stream read/write timeout (IB-2) | 20 s | same |
| P-MEM-CEIL | process memory ceiling, derivable per PF-1 | unmeasured; per-query peak ≈ 3 × result size (HZ-4) | formula ratified with ADR-19 ⚠ |

## Metering

| Parameter | Role | Observed | Target |
|---|---|---|---|
| P-CU-BURST | token-bucket capacity (DEF-23) | 3.0; latent edge defects tracked in GAP-21 | same |
| P-EPOCH-POLL | registry poll period (LIV-12) | 30 s | same |

## Status and logs

| Parameter | Role | Observed | Target |
|---|---|---|---|
| P-HB-INTERVAL | status snapshot refresh period (RP-21) | 60 s | same |
| P-HB-STALENESS | status staleness bound (LIV-6, SLI-7) | violated at scale: refresh + full store walk, "minutes" observed at ~30k chunks — GAP-15 | 90 s ⚠ |
| P-LOGS-RETENTION | log-record retention (WP-17) | 2 h | same |
| P-LOGS-LAG | log serving lag floor (RP-22, DEF-14) | 60 s | same |
| P-LOGS-RESP-MAX | log response page budget (RP-22) | 10 MiB − 100 KiB (margin unexplained — OQ-2) | derive from P-LOGS-RESP-CEIL ⚠ |
| P-LOGS-RESP-CEIL | transport ceiling for log/status responses (IB-2) | 10 MiB | same |
| P-LOGS-QUEUE | log-read queue depth (RP-22) | 4 | same |
| P-SCHEMA-REFRESH | schema-manifest refresh period (IB-44); inert under `--assignment-source worker`, where schemas arrive with the network state (IB-44b) | 3 600 s | same |

## Lifecycle and reconciliation bounds

| Parameter | Role | Observed | Target |
|---|---|---|---|
| P-START-ACCEPT | start → accepting queries (LIV-5) | unmeasured; recovery scan + residue sweep dominate | 120 s at W-CHUNKS ⚠ |
| P-EVICT-BOUND | pin-release → reclamation (LIV-4) | **unbounded** — GAP-6 | 60 s ⚠ |
| P-STALL-MAX | zero-progress-with-work alarm bound (LIV-9/13) | no alarm exists — GAP-17 | 600 s ⚠ |
| P-DEL-FLOOR | max store fraction deletable per application without override (REQ-25) | no floor — GAP-3 | 50 % ⚠ (ADR-17) |
| P-CONVERGE-SLACK | scheduling slack in convergence bounds (LIV-1/12) | — | 60 s ⚠ |
| P-RECOVER-BOUND | overload-end → SLOs restored (LIV-8) | unmeasured | 60 s ⚠ |
| P-REJECT-LATENCY | rejection response latency under storm (LIV-8) | unmeasured | 1 s ⚠ |
| P-SHUTDOWN-BOUND | shutdown drain bound (LIV-10) | 5 s (+1 s executor drain) | same |
| P-SOAK-WINDOW | soak-trend evaluation window (LIV-14) | — | 24 h ⚠ |
| P-QUIESCE-GAP | scrape gap defining harness quiescence (13) | — | 5 s ⚠ |

## SLO targets (all ⚠ until ADR-19; baselines in 11)

| Parameter | Role | Observed | Target |
|---|---|---|---|
| P-SLO-Q-P95 | SLI-1 p95, S1 | unmeasured | 2 s ⚠ |
| P-SLO-Q-P99-STORM | SLI-1 p99, S2 | unmeasured | 10 s ⚠ |
| P-SLO-REJECT-STORM | SLI-3 ceiling, S2 | unmeasured | typed-only; ≤ overflow fraction ⚠ |
| P-SLO-DL-RATE | SLI-4 floor, S5 | unmeasured | 0.8 × min(W-DL-RATE, P-DL-CONC × per-fetch rate) ⚠ |
| P-SLO-ASSIGN-APPLY | SLI-5 bound, S3 | unmeasured | P-ASSIGN-POLL + P-ASSIGN-FETCH-TIMEOUT + 60 s ⚠ |
| P-SLO-CONVERGE | SLI-6 bound, S5 | unmeasured | LIV-1 formula ⚠ |
| P-SLO-LOG-LAG | SLI-8 bound, S1 | ≥ P-LOGS-LAG by construction | P-LOGS-LAG + 60 s ⚠ |

## Merge-gate thresholds

| Parameter | Role | Observed | Target |
|---|---|---|---|
| P-GATE-SPEC-ERR | MG-1: allowed linter errors | 0 (enforced in CI) | 0 |
| P-GATE-PROP-RATCHET | MG-2: matrix regression allowance | row-completeness enforced; ratchet manual | 0 regressions ⚠ |
| P-COV-DIFF | MG-3: changed-line coverage floor | not instrumented | 80 % ⚠ |
| P-COV-TOTAL | MG-3: whole-repo coverage floor | not instrumented | 60 %, ratchet-up ⚠ |
| P-GATE-PR-TIME | MG-4: PR-gate wall-clock budget | — | 10 min ⚠ |
| P-GATE-NIGHTLY | MG-5: nightly-suite budget | — | 4 h ⚠ |
| P-PERF-NOISE | MG-6: benchmark noise band | — | 5 % ⚠ |
| P-FLAKE-RETRY | MG-8: retry budget before quarantine | — | 1 ⚠ |
| P-GATE-LINT | MG-7: pinned lint deny level | formatter + lint at deny(correctness, suspicious) | same |
