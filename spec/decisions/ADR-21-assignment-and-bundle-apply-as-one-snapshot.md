# ADR-21 — An assignment and its schema bundle apply as one snapshot

Status: Accepted

## Context

Under `--assignment-source worker` the network state publishes two things that change
independently: a `worker_assignment` pointer (IB-40b/41b) and a `schema_bundle` (IB-44b).
Either can move without the other, so the effective sequence is a chain of pairs — a
state publishing `A₁,S₁` then `A₂` then `S₂` yields `(A₁,S₁) (A₂,S₁) (A₂,S₂)` — and the
worker reconciles against the tail rather than replaying the chain.

Two situations make it tempting to apply half a pair. The bundle may fail to download
while the schemas the assignment needs are already loaded from earlier bundles, so the
worker could serve it. And the bundle may download but not cover the assignment's write
schemas, which the loaded set may nonetheless cover. In both, the worker can answer every
query the assignment implies, and refusing looks like self-inflicted downtime: the fleet
carries a stale assignment while a healthy worker sits on data it could serve.

## Decision

Neither half applies without the other. An assignment is admitted only if its accompanying
bundle was fetched *and* covers every `write_schema_id` the assignment references; failing
either, the assignment is refused whole and the previous one stays in force (FM-53b/53c).
Coverage by the locally accumulated schemas is not a substitute — **admission requires the
pair; serving uses whatever is loaded**. A bundle that does not cover its assignment breaks
a scheduler invariant and always alarms (OB-16), whether or not the worker could have
proceeded.

The reason is not caution about schemas, it is that the two are one state. A worker that
applies an assignment whose bundle it could not obtain reports that assignment id as
applied (IB-22), and the network reads that as "this worker holds this snapshot" — a claim
it cannot support, because the half it skipped is the half that says what the data means.
Divergence between the pair is a publisher fault, and a worker that papers over it converts
a loud, local, fixable failure into a quiet fleet-wide inconsistency in which schedulers
route on assignment ids the workers behind them do not really implement. Rejected
alternative: admit on local coverage and alarm — it optimizes for the availability of one
worker at the cost of the network's ability to trust an applied id, and it makes the
worker's answer depend on which bundles that worker happened to see, so two workers on the
same assignment can disagree about whether they hold it.

## Consequences

A broken or lagging bundle publisher stalls assignment application fleet-wide rather than
producing workers that silently disagree; the alarm is the signal, and the stall is
bounded by the publisher being fixed. Assignment application inherits the bundle's
availability, so `P-ASSIGN-*` timing depends on IB-44b being served as reliably as
IB-41b. The locally accumulated schema set keeps serving chunks already on disk, including
those written against schemas no current bundle carries — it is only barred from admitting
new assignments. Constrains FM-53b/53c and IB-41b; the accumulated set is irrecoverable
(no by-id or by-hash schema fetch exists), so anything reclaiming it must key on the write
schemas of chunks actually on disk.
