# ADR-10 — Lock ordering: assignment index before chunk state

Status: Accepted (historical)

## Context

The state manager guards two locks: the applied-assignment index and the chunk-set
state. A refactor merged two functions without aligning their acquisition orders,
producing a textbook AB-BA deadlock in production (worker hung: no downloads, no
status). The fix commit established a single canonical order.

## Decision

The canonical acquisition order is: assignment index → chunk state (→ the
assignment-application tracker, where that feature is compiled in). Every code path
that takes more than one of these locks takes them in this order; no lock is held
across a suspension point.

## Consequences

The deadlock class is closed while the discipline holds — but it is documented only
here and in a commit message, with no test or lint enforcing it (the gap register's
concurrency rows and CT-3 are the intended guard). Any new lock joins the order
explicitly or the invariant silently becomes three-way false. Shapes INV-37, CT-3.
