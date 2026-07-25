# ADR-3 — Skip assignment document verification

Status: Accepted (historical)

## Context

FlatBuffers offers a verifying parse and an unchecked one. On real network-scale
assignments the verifier "took more than a minute" per intake (commit rationale), on a
document fetched every change over HTTPS from network-operated infrastructure.

## Decision

Parse the assignment with the unchecked reader. Trust rests on transport security and
the operator of the assignment endpoint.

## Consequences

Intake is fast. A corrupted or hostile document reaches an unverified binary reader —
undefined behavior is theoretically in scope, and the decompressed size is unbounded
(GAP-4). The spec keeps REQ-24 as intent and ADR-18 proposes the bounded/validated
replacement; until then this ADR records the accepted risk. Shapes WP-2, FM-12.
