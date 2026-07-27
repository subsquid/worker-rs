# ADR-6 — Fractional compute units: charge then refund

Status: Accepted (historical)

## Context

Charging the actual covered fraction at admission would let a portal issue many tiny
queries for the price of one, overloading the worker while paying almost nothing —
admission cost must reflect *work admitted*, not work eventually done. But flat
per-query pricing overcharges partial-chunk queries.

## Decision

Admission spends a full 1.0 CU up front; after execution the unused fraction
(1 − chip) is refunded, regardless of success or error. A post-admission overload
rejection keeps the whole unit — a deliberate reversal of the original refund-on-
overload behavior (changed in the p2p-hardening rework, pinned by test): admission
capacity was consumed, and refunding it would make overloading the worker free.

## Consequences

Burst abuse is priced at full CUs; honest partial queries net-pay their fraction.
Overload keeps are a small overcharge under pressure — accepted. The chip computation
still parses chunk ids (GAP-13). Shapes DEF-23/24, INV-15, REQ-21, RP-4.
