# ADR-7 — Admission bar: reserve before spend; rejects unlogged

Status: Accepted (historical)

## Context

Before the admission rework, a query could be charged and then dropped on a full
queue (spent unit, no service), and rejection paths could silently drop the request
with no typed response. Logging rejected queries would let unauthenticated or
unfunded traffic grow the log store.

## Decision

Admission order is fixed: validate identity/freshness/envelope → reserve a queue slot
→ spend the CU. A spent unit therefore always corresponds to an enqueued query.
Everything past admission is billable and always logged; everything rejected before it
produces a typed signed error (or a drop, per ADR-9) and no log record.

## Consequences

CU spend and service admission cannot diverge (INV-15). Rate-limit and overload
pressure before admission is invisible in the log store by design — it must be
observable via metrics instead (OB-7; the missing counters are GAP-17). Shapes RP-1,
DEF-25, INV-32.
