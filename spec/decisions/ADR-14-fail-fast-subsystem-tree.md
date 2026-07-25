# ADR-14 — Fail-fast subsystem tree

Status: Accepted (historical)

## Context

The worker is a set of long-running loops (transport, queries, assignment intake,
reconciliation, status, logs, metering). A hand-rolled supervisor macro ran them with
ad-hoc lifetimes; partial-death states (one loop dead, the rest serving) are worse
than a clean restart under a process supervisor, because a half-alive worker keeps
its network reputation while silently not doing its job.

## Decision

All loops run under a supervision tree; any subsystem exiting for a reason other than
requested shutdown is an error that tears down the whole process (bounded by
P-SHUTDOWN-BOUND, plus a short executor drain so stuck blocking tasks cannot hold the
exit).

## Consequences

Whole-process fail-fast makes FM-1 the load-bearing requirement it is: every panic
anywhere becomes an outage, so panic paths in loops are P0-class bugs (GAP-2, GAP-6).
Operators get clean restart semantics; there is no half-alive mode. Shapes FM-1,
LIV-10.
