# ADR-20 — Freshness rejections are worker-fault outcomes

Status: Proposed

## Context

Admission rejects queries whose timestamp falls outside P-TS-WINDOW of the worker clock
(RP-1). The verdict's reference input is the worker's own clock — worker-local mutable
state — so the worker cannot tell a stale sender from its own skew. Today the rejection
is typed `bad_request`, a terminal client-fault class: portals treat `bad_request` as
"fix the request" and do not reroute (their dependency contract classifies it terminal),
so one worker with a skewed clock silently converts a fraction of the network's valid
queries into client-visible terminal errors — no reroute, no local signal, no alarm.
The cost asymmetry favors self-blame: misattributing to the client kills the request
unrecoverably; misattributing to the worker costs one rerouted attempt, and since
portals sign each attempt afresh, a genuinely stale timestamp self-heals on retry.

## Decision

A rejection whose only cause is timestamp freshness is attributed to the worker: it
surfaces as `server_error`, never `bad_request`, with pre-admission accounting
unchanged (no CU, no log record — ADR-7's billable line stays at admission). FM-32
already applies this rule to store faults; INV-26 generalizes it: `bad_request` is
reserved for verdicts that are a pure function of the request bytes and network-public
data. The worker additionally exposes its freshness-rejection rate and an estimated
clock offset (OB-15) and alarms past P-SKEW-ALARM (FM-55). Rejected alternative:
portal-side special-casing — the misattribution originates here, and portals could only
key on unstable message strings (the GAP-30 failure mode). A machine-readable staleness
verdict (carrying the worker's clock reading) rides the OQ-7 error-surface revision;
reclassification is chosen now because it requires no wire schema or client change.

## Consequences

A skewed worker is routed around by existing client behavior and is diagnosable from
its own metrics, instead of silently poisoning requests. A client that really sends
stale timestamps costs the fleet reroutes rather than being told to fix itself —
accepted until the OQ-7 surface can say "stale timestamp" in-band. Amends RP-1, RP-20,
and FM-41; adds INV-26, FM-55, OB-15, P-SKEW-ALARM; opens GAP-33 (closes when the
reclassification, the signals, and the alarm land).
