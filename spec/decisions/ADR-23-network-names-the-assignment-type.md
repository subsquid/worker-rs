# ADR-23 — The network state names the assignment type; the CLI only pins it

Status: Accepted

## Context

Migrating to split assignments walks the published state through three shapes: the legacy
`assignment` alone, then that blob alongside `worker_assignment`/`portal_assignment`/
`schema_bundle` while consumers switch over, then the split blobs alone. Which blobs are
present says nothing about which are authoritative, so a consumer picking by presence would
switch the moment the scheduler *started* publishing the split set.

`--assignment-source legacy|worker` made the choice a property of the worker process instead:
the fleet moved only as fast as its operators reconfigured and restarted it, and the scheduler
had no say. `sqd-assignments` now publishes an `assignment_type` in the state and resolves the
blobs it names (`NetworkState::resolve`), which puts the decision where the migration is driven
from.

## Decision

The resolved type is the state's `assignment_type`, read afresh on every poll.
`--assignment-source legacy|split` overrides it and nothing else does; unset — the default —
the state decides. Its values are the state's own words, not a second vocabulary for them.

The type therefore belongs to the announcement rather than to the process (IB-40): the document
format read, whether a schema bundle is a prerequisite, and whether application waits on a
settle all follow the type the poll resolved, so a network that switches type switches a running
worker without a restart.

Resolution is `sqd-assignments`', not the worker's. A `split` state must therefore publish
`portal_assignment` before the worker will proceed, even though the worker never reads it.

## Consequences

A fleet moves when the scheduler says so, and a single worker can still be moved ahead of it —
or held back — by pinning.

One cost, and one thing that stops being a decision. Whether to poll the schema manifest (IB-44)
was settled by the type at startup, which a per-poll type cannot do; it is now polled
unconditionally. That is not a concession: the type-keyed registry answers for any chunk the
assignment in force does not pin — held from an earlier assignment, or held before any assignment
applies — which happens under `split` as much as under `legacy`. Gating it on the type was
costing a pinned worker the chunks it holds and could serve.

The cost is that a `split` state that omits the portal's half stalls every worker. That shape is
indistinguishable from a migration still under way, so it is counted by reason at every poll
rather than alarmed on once (OB-19) — what an operator acts on is how long it lasts (FM-53e).

Extends ADR-21 and ADR-22: what they call `--assignment-source worker` is a resolved type of
`split`. ADR-21's requirement that such a state publish both a `worker_assignment` and a
`schema_bundle` is now `resolve`'s to enforce, and it requires `portal_assignment` besides.
