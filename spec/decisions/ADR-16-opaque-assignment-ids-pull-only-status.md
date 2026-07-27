# ADR-16 — Opaque assignment ids; pull-only status

Status: Accepted (historical)

## Context

Coordinating "which assignment is a worker on" needs either worker-side ordering
semantics or scheduler-side ones. The PR record (staged-assignment work) states the
choice explicitly: the id "is an opaque String. Worker does not parse, compare, or
order assignment IDs. Assignment ordering remains scheduler-owned." Relatedly, status
broadcast was removed with the gossip layer (ADR-1); worker-to-worker status requests
were also dropped.

## Decision

The worker: treats assignment ids as opaque; applies assignments in arrival order,
never interrupting an application in progress; coalesces a backlog to the newest; and
reports the previously applied id while a newer one is being applied. Status is served
only on request, from a periodically refreshed snapshot.

## Consequences

The worker stays simple and the scheduler owns global ordering (NG2). Honest-but-stale
reporting is legal within P-HB-STALENESS; the reporting-coherence hole (GAP-11) and
the shipped-build coalescing hole (GAP-9) are deviations from this decision's intent,
not amendments to it. Shapes WP-4, RP-21, NG2.
