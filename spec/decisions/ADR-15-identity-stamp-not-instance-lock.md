# ADR-15 — Identity stamp, not an instance lock

Status: Accepted (historical)

## Context

A data directory reused under a different network identity would serve one identity's
data under another's signatures. Full single-instance locking (advisory file locks,
PID files) was not implemented — no commit records why; the stamp guards the observed
failure mode (key mix-ups in operator setups).

## Decision

The store carries a plain-text identity stamp; a process whose identity differs
refuses to start. Nothing prevents two processes with the *same* identity from sharing
a store.

## Consequences

Key mix-ups fail fast. Same-identity double-start remains undefined behavior on a
shared store, and the current refusal runs after recovery has already swept transient
state — both tracked (GAP-16); CN-9 specifies the intended check-before-mutate order.
Shapes CN-9, FM-51.
