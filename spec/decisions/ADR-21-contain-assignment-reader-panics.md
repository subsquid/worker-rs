# ADR-21 — Contain assignment-reader panics

Status: Accepted (2026-07-26)

## Context

ADR-3 accepted parsing assignment documents unverified, so the reader works on bytes
nothing has checked. It panics on input it cannot make sense of — an unparseable roster
peer id is the proven case — and ADR-14's supervision tree turns any panic in the
intake loop into a process exit. That is FM-1 violated by a document anyone upstream of
the worker can publish. Fixing it properly means ADR-18's structural validation, which
is a larger change and is not ratified.

## Decision

Reading a document is a panic-catching boundary, the same shape ADR-5 gave the query
engine: a panic below it becomes a rejected document (FM-12), the last good assignment
stays in force, and an alarm is raised. The task boundary above it repeats the catch, so
a panic on a path the reader does not own still costs one document.

Containment is not validation. Per-item degradation (FM-11's "this chunk's address is
unusable") stays a typed `None` on the address path, not a caught panic, and the
unvalidated-parse hazard (GAP-4) is unchanged: a document crafted to corrupt memory
rather than to panic is still out of scope until ADR-18.

## Consequences

FM-1 holds against the documented panic paths without waiting on ADR-18, at the cost of
a coarse failure mode — a panic loses the whole document, including the chunks that
were readable. Sentry still sees the panic, so containment does not hide the bug.
Shapes FM-1, FM-12, REQ-24.
