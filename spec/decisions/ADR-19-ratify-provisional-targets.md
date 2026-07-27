# ADR-19 — Ratify provisional SLO targets and gate thresholds

Status: Proposed

## Context

The suite introduces bounds that exist nowhere today: liveness bounds (P-Q-DEADLINE,
P-EVICT-BOUND, P-STALL-MAX, P-START-ACCEPT, …), the SLO-target symbols, the memory
ceiling formula (P-MEM-CEIL), and merge-gate thresholds (P-COV-DIFF, P-COV-TOTAL,
P-PERF-NOISE, P-FLAKE-RETRY, P-GATE-PR-TIME, P-GATE-NIGHTLY, P-GATE-PROP-RATCHET).
Every ⚠ row in 15-parameters.md whose row does not name another ADR belongs to this
batch. The numbers proposed there are engineering estimates, not measurements.

## Decision

Proposed: run the CT-6 baseline suite (S1–S6) once the Phase-0 harness exists, replace
every "unmeasured" observation in the registry with data, then ratify each target —
adjusting where baselines prove the estimate wrong. Gate thresholds ratchet upward
only after ratification.

## Consequences

Targets become commitments with evidence; the registry's ⚠ marks disappear in one
reviewed change. Until then, all ⚠ bounds are advisory and MG-6 cannot block.
