# ADR-21 — Validate a worker assignment against its schema bundle

Status: Accepted

## Context

Under `--assignment-source worker`, every network state is required to publish both a
`worker_assignment` pointer (IB-40b/41b) and a `schema_bundle` (IB-44b). The bundle describes
the schemas the assignment may reference. Schema files are cached locally because assignments
and chunks can outlive the network state that introduced them.

## Decision

The pair is a validation boundary, not an atomic snapshot. The worker fetches either object only
when its identity is not cached, then verifies that the referenced bundle contains every
`write_schema_id` used by the assignment. If either reference is absent, either object cannot be
obtained, or coverage fails, the worker keeps its previous assignment and installs none of the
schemas newly supplied by that network state (FM-53b/53c).

After validation, schemas are installed additively and idempotently before the assignment is
applied. Existing ids with identical contents are reused; only missing ids are copied. Schema ids
are immutable: publishing different contents under an existing id rejects the update because it
would change the meaning of chunks already stored under that id. A failure while copying new ids
may leave a valid subset cached; retry completes the remainder while the previous assignment stays
in force.

## Consequences

There is no transaction spanning the schema store and assignment state. Safety comes from
validation first, immutable additive schemas second, and assignment application last. Cached
schemas keep serving chunks already on disk. Reclamation must key on schemas used by chunks on
disk because old bundles cannot be fetched by id or hash. Extends ADR-11: under
`--assignment-source worker` the operational gate is the bundle (IB-44b) — the CDN manifest is
not polled at all — and a bundle that fails to install blocks the assignment rather than
degrading (FM-53b).
