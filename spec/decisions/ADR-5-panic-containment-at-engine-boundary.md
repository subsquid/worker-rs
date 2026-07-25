# ADR-5 — Panic containment at the engine boundary

Status: Accepted (historical)

## Context

A panic inside the query-engine thread pool aborted the whole process (rayon pool
panics are not recoverable by the caller), taking down every in-flight query and the
node's network presence — hit in production. Alternatives: run queries in
subprocesses (heavy), or guarantee panic-freedom upstream (unenforceable across
engine dependencies).

## Decision

Every engine invocation is wrapped in a panic-catching boundary; a panic becomes a
typed `server_error` for that query only, with the payload preserved in the message.

## Consequences

One malicious or buggy query cannot end service (INV-36, G4). Panics outside the
boundary (result compression, signing, response plumbing) still escalate — the
boundary's edges are a standing test target (CT-4). Shapes INV-36, FM-1.
