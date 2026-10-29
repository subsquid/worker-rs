# ADR-24 — Read split assignments only

Status: Accepted

## Context

ADR-23 let the worker read either assignment type, so that the fleet could follow the
network through the migration to split assignments. The migration is over. On
2026-10-29 the network states of mainnet and tethys both named `assignment_type: split`,
and neither carried the legacy `assignment` blob.

Two readers cost more than one. The legacy reader is unverified (ADR-3), so a damaged
legacy document can reach the reader as undefined input. It also kept a second document
format alive in the harness and the tests, and a per-poll type switch in the applier.

## Decision

The worker reads only `split` states (IB-40). A state that names `legacy`, or names no
type, is not applicable. The worker then applies nothing, keeps the assignment in force,
and counts the poll under reason `assignment_type` (OB-19, FM-53e). The worker never
reads the legacy `assignment` blob, and its shape cannot make the state unreadable.

The `--assignment-source` override goes. The network state alone names the type, as
ADR-23 decided, but only one type is now readable.

The worker verifies every assignment document before it reads it (IB-41b). This replaces
the trust that ADR-3 accepted.

The type registry (IB-44) stays as the fallback for chunks that the assignment in force
does not pin. It runs from startup until the first assignment applies, and nothing
starts it again.

## Consequences

A network that goes back to `legacy` stalls every worker on its last assignment. The
workers keep serving what they hold, and OB-19 shows the stall. A return to legacy
therefore needs a worker release.

An operator who passes `--assignment-source` on the command line must remove it,
because the process refuses an unknown flag. The environment variable has no effect.

GAP-4 loses its clause about the unverified document. Decompression is still unbounded,
and the verifier limits are not bounds, so ADR-18's size cap is still open.

Supersedes ADR-3 and ADR-23.
