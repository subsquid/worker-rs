# SQD Network Worker — behavioral specification

The worker is a node of a decentralized data lake. It continuously reconciles a local
chunk store against a network-published assignment — downloading assigned data chunks
from a data origin and deleting unassigned ones — and serves signed, metered data
queries against the chunks it holds, while reporting its status and delivering
execution logs to network collectors.

This suite specifies what the worker MUST do (not how the current code does it), records
why it is that way (the decision log), and carries a full conformance program: a
reference model as test oracle, a test-class taxonomy, a traceability matrix, and a
prioritized gap register capturing where today's implementation deviates from intent.

**Tier: conformance (full depth). Shape: stateful service.** The worker owns persistent
state (the chunk store and the query-log store), mutates it from an input stream
(assignments, downloads), and answers queries — the classic stateful-service module set
applies: mutations (04), query contract (05), consistency/durability (06), and a
retention/space lifecycle (10).

## Document map

| Doc | Contents | Normative? |
|---|---|---|
| [01-overview.md](01-overview.md) | context, actors, goals, non-goals, trust model, lifecycle | Yes |
| [02-requirements.md](02-requirements.md) | product requirements `REQ`, open questions `OQ` | Yes |
| [03-data-model.md](03-data-model.md) | definitions `DEF`: entities, state tuple, input events | Yes |
| [04-mutations.md](04-mutations.md) | write path `WP`: reconciliation loop, transition catalog | Yes |
| [05-queries.md](05-queries.md) | read path `RP`: admission, result contract, error taxonomy | Yes |
| [06-consistency-durability.md](06-consistency-durability.md) | `CN`: commit model, isolation, recovery contract | Yes |
| [07-invariants.md](07-invariants.md) | safety catalog `INV` | Yes |
| [08-liveness.md](08-liveness.md) | liveness catalog `LIV` | Yes |
| [09-failure-model.md](09-failure-model.md) | fault families and required responses `FM` | Yes |
| [10-retention-space.md](10-retention-space.md) | space lifecycle `RS`: deletion, reclamation, residue | Yes |
| [11-performance.md](11-performance.md) | `SLI`/SLOs, workload model `W-*`, hazards `HZ`, benchmarking `PF` | Yes |
| [12-observability.md](12-observability.md) | required signals `OB`, property→observable mapping | Yes |
| [13-conformance-tdd.md](13-conformance-tdd.md) | reference model, `CT` taxonomy, matrix, gaps `GAP`, gates `MG`, harness `HC` | **Mutable** |
| [14-interface-binding.md](14-interface-binding.md) | concrete wire/HTTP/config surface `IB` | Yes |
| [15-parameters.md](15-parameters.md) | parameter registry `P-*` | **Mutable** |
| decisions/ | ADR log, one file per decision | Append-only |

## Conventions

- **RFC 2119**: MUST / MUST NOT / SHOULD / MAY have their standard meanings. A tag like
  `[MUST — intent, currently violated: GAP-n]` states intended behavior the current
  implementation does not meet; the deviation is tracked in the gap register, never by
  weakening the requirement.
- **IDs** are stable and never renumbered. Prefixes and home documents:
  `REQ`/`OQ` → 02 · `DEF` → 03 · `WP` → 04 · `RP` → 05 · `CN` → 06 · `INV` → 07 ·
  `LIV` → 08 · `FM` → 09 · `RS` → 10 · `PF`/`SLI`/`HZ` → 11 · `OB` → 12 ·
  `CT`/`MG`/`HC`/`GAP` → 13 · `IB` → 14 · `P-*` → 15 · `ADR` → decisions/.
  Numbering is **banded** per category (bands are declared in each home doc's header);
  holes between bands are the convention working, not missing entries.
- **Parameters**: every constant (timeout, budget, cap, threshold) appears in normative
  text only as a `P-NAME` symbol. Concrete values — observed and target — live only in
  [15-parameters.md](15-parameters.md). ⚠ marks a provisional value awaiting
  ratification via an ADR.
- **Math notation**: ℕ for naturals, ⊥ for "absent", `\` for set difference, ∪ ∩ ⊆ as
  usual; ⟨…⟩ for tuples; sequences are ordered multisets.
- **Mutability rule**: exactly two documents carry dates and statuses —
  13-conformance-tdd.md and 15-parameters.md. `decisions/` only ever gains files; an
  accepted ADR is never edited except to gain `Superseded by ADR-n`. All other documents
  change only when *intended behavior* changes.
- **Scope tags** on invariants: `[state]` holds in every observable state ·
  `[transition]` relates consecutive states · `[response]` holds for every result ·
  `[recovery]` holds across crash/restart.

## Decision log

| ADR | Title | Status |
|---|---|---|
| [ADR-1](decisions/ADR-1-pull-based-assignment-distribution.md) | Pull-based assignment distribution | Accepted (historical) |
| [ADR-2](decisions/ADR-2-binary-assignment-format.md) | Binary assignment format over JSON | Accepted (historical) |
| [ADR-3](decisions/ADR-3-skip-assignment-verification.md) | Skip assignment document verification | Accepted (historical) |
| [ADR-4](decisions/ADR-4-opaque-chunk-identity.md) | Opaque chunk identity, suffix forks | Accepted (historical) |
| [ADR-5](decisions/ADR-5-panic-containment-at-engine-boundary.md) | Panic containment at the engine boundary | Accepted (historical) |
| [ADR-6](decisions/ADR-6-fractional-compute-units.md) | Fractional compute units: charge then refund | Accepted (historical) |
| [ADR-7](decisions/ADR-7-admission-bar-ordering.md) | Admission bar: reserve before spend; rejects unlogged | Accepted (historical) |
| [ADR-8](decisions/ADR-8-single-outcome-delivery.md) | Response and log built from a single outcome | Accepted (historical) |
| [ADR-9](decisions/ADR-9-bounded-reject-fanout.md) | Bounded reject fan-out | Accepted (historical) |
| [ADR-10](decisions/ADR-10-lock-ordering-discipline.md) | Lock ordering: assignment index before chunk state | Accepted (historical) |
| [ADR-11](decisions/ADR-11-engine-selection-and-arrow-output.md) | Engine selection by wire enum; columnar output format | Accepted (historical) |
| [ADR-12](decisions/ADR-12-single-chunk-query-scope.md) | Single-chunk query scope | Accepted (historical) |
| [ADR-13](decisions/ADR-13-respond-before-logging.md) | Respond before logging | Accepted (historical) |
| [ADR-14](decisions/ADR-14-fail-fast-subsystem-tree.md) | Fail-fast subsystem tree | Accepted (historical) |
| [ADR-15](decisions/ADR-15-identity-stamp-not-instance-lock.md) | Identity stamp, not an instance lock | Accepted (historical) |
| [ADR-16](decisions/ADR-16-opaque-assignment-ids-pull-only-status.md) | Opaque assignment ids; pull-only status | Accepted (historical) |
| [ADR-17](decisions/ADR-17-reconciliation-deletion-floor.md) | Reconciliation deletion floor | Proposed |
| [ADR-18](decisions/ADR-18-bounded-validated-assignment-intake.md) | Bounded, validated assignment intake | Proposed |
| [ADR-19](decisions/ADR-19-ratify-provisional-targets.md) | Ratify provisional SLO targets and gate thresholds | Proposed |

## How to use this suite

1. **Ratify**: review the three `Proposed` ADRs and every ⚠ target in
   [15-parameters.md](15-parameters.md); acceptance turns ⚠ into committed bounds.
2. **Build the harness** in the order given in
   [13-conformance-tdd.md](13-conformance-tdd.md) §build order: input simulators and
   structural validators first, then the reference model, then the kill/restart harness.
3. **Burn down the gap register** priority-first; every closure lands with the failing
   test named in its "first test" column.
4. Keep the suite honest: run `python3 spec/tools/check_spec.py spec/` (the standing
   gate, wired in CI) — it fails on dangling IDs, unregistered parameters, matrix holes,
   and decision-log drift.
