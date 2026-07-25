# ADR-1 — Pull-based assignment distribution

Status: Accepted (historical)

## Context

Early versions pushed assignments to workers over the p2p layer (gossipsub broadcast;
a scheduler among the boot nodes). Gossipsub's memory cost on workers was significant,
and push fan-out ties scheduler availability to fleet convergence. The gossip protocol
was first disabled by default, then removed outright ("Reduce RAM by removing gossipsub
protocol"), and the scheduler was dropped from the boot nodes.

## Decision

Workers pull: poll a CDN-hosted network-state document on a fixed interval, and fetch
the referenced assignment document when its id changes. No push path exists.

## Consequences

Convergence latency is bounded below by the poll interval (P-ASSIGN-POLL) — accepted.
Scheduler and workers are decoupled through static content that scales to any fleet
size. Assignment freshness becomes the worker's responsibility to monitor (OB-13,
FM-14). Shapes WP-1, REQ-10, LIV-1.
