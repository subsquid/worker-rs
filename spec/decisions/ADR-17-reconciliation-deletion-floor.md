# ADR-17 — Reconciliation deletion floor

Status: Proposed

## Context

Reconciliation deletes whatever the assignment stops naming. A scheduler bug, a
truncated document, or a worker briefly missing from the roster therefore wipes a
multi-terabyte store in one pass — a fleet-wide amplifier with hours-to-days of
re-download cost. An earlier code comment ("prevent accidental massive removals")
acknowledged the risk; the guard was never built and the comment was lost in a
refactor. Trade-off: any floor delays legitimate large rebalances.

## Decision

Proposed: a single assignment application may evict at most the P-DEL-FLOOR fraction
of the store's chunks. Beyond it, eviction pauses, an alarm level raises (OB-12), and
the excess is re-evaluated on the next intake cycle; an explicit operator override
releases the hold for sanctioned rebalances.

## Consequences

A wipe becomes a held, alarmed, recoverable state instead of an accident. Legitimate
mass rebalances need an override or several cycles. Encodes REQ-25, RS-2; closes
GAP-3 when accepted and implemented.
