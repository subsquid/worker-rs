# ADR-8 — Response and log built from a single outcome

Status: Accepted (historical)

## Context

The wire response and the log record were built by separate code paths; they could
disagree — a result recorded as success but delivered as an error (oversize discovered
late), or a served query missing from the log. Billing disputes are adjudicated from
logs, so divergence is unauditable.

## Decision

One function produces both halves of the delivery from one execution outcome. A
result that cannot be shipped (oversized, unsignable) is downgraded to `server_error`
in the response *and* the log, atomically at build time.

## Consequences

INV-23 becomes structurally enforceable and cheap to test. Residual asymmetry: a
built-then-undeliverable response (transport failure after build) still logs as
success — accepted, bounded by transport timeouts. Shapes DEF-26, INV-23, RP-14.
