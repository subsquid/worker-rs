# ADR-9 — Bounded reject fan-out

Status: Accepted (historical)

## Context

Every typed rejection is signed. Under a query flood, unbounded concurrent signing
tasks would let an attacker convert cheap requests into expensive signature work — a
self-inflicted denial of service. The in-code rationale: past the bound, "the response
is dropped — a cheap stream reset — rather than spawning unbounded signing tasks under
a flood."

## Decision

Concurrent rejection signing is capped (P-REJECT-CONC). Beyond the cap, the connection
is dropped with no response.

## Consequences

Flood cost stays bounded; graceful degradation ends at the cap — portals beyond it see
transport resets instead of retry hints (RP-20's *(no response)* row), which is
accepted as the flood posture. Shapes RP-4, FM-42.
