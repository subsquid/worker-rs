# ADR-17 — Reconciliation deletion floor

Status: Accepted (2026-07-26)

## Context

Reconciliation deletes whatever the assignment stops naming. A scheduler bug, a
truncated document, or a worker briefly missing from the roster therefore wipes a
multi-terabyte store in one pass — a fleet-wide amplifier, hitting every worker at once,
with hours-to-days of re-download cost. An earlier code comment ("prevent accidental
massive removals") acknowledged the risk; the guard was never built and the comment was
lost in a refactor.

The costs run the other way too. A guard that refuses outright strands disk on a worker
the network has legitimately shrunk, and because eviction precedes fetching in the
reconciliation loop, a worker that cannot free space cannot converge on a large
rebalance without twice the headroom.

## Decision

An assignment application that would evict more than the P-DEL-FLOOR fraction of the
stored chunks evicts none of them: the batch is held whole, an alarm level is raised
(OB-12), and the same test runs again on every reconciliation pass, so a restoring
assignment releases the hold by itself. Held-but-undesired chunks stay readable —
eviction, not the assignment, is what ends availability.

The hold is a **delay, not a veto**. After P-DEL-HOLD-MAX the batch goes through, and
the reconciliation loop wakes on its own timer to apply it, so LIV-4 needs no further
input event. The window is evidence about one batch and belongs to it: a different
wipe-inducing assignment earns its own, and a member left behind by a pin keeps the
authorization the batch already earned rather than facing a second window. The bound separates the two cases by their own nature rather than by asking
an operator to tell them apart: a scheduler glitch is corrected by the next publication,
a real shrink keeps being republished for the whole window. What the wait buys is
machine self-correction; a human noticing the alarm in time is a bonus, not the premise.

The batch is held rather than trimmed to the floor, because trimming still wipes the
store, just over several passes. The operator override is the floor itself: raising it
to 1 restores unguarded reconciliation.

## Consequences

An accidental wipe becomes a held, alarmed, self-resolving state. A deliberate one costs
P-DEL-HOLD-MAX. Held bytes are a bounded excess term in RS-3 and a bounded delay in
LIV-4 — an unbounded hold would violate both outright. A scheduler bug that outlives the
window still wipes: this guards against a glitch, not a sustained fault.

Not in conflict with RS-1's ban on age-based eviction: a held chunk still leaves because
the assignment stopped naming it, only later. Encodes REQ-25, RS-2, and closes the
missing-floor gap.
