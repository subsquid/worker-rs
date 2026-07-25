# ADR-18 — Bounded, validated assignment intake

Status: Proposed

## Context

ADR-3 accepted skipping document verification when the verifier cost over a minute.
Since then: the panic on a malformed per-chunk address is proven remote-triggerable
(GAP-2), decompression is unbounded (GAP-4/HZ-12), file names from the document reach
filesystem paths without traversal checks, and origin payloads commit unverified
(GAP-5). The one-minute measurement predates the binary-format rework and has not
been re-taken.

## Decision

Proposed: (a) cap decompressed document size at P-ASSIGN-SIZE-MAX; (b) re-measure
verified parsing on current documents — adopt it if within an acceptable intake
budget, else validate structurally on access (every address parse, name sanitization,
bounds check degrades per-chunk, never panics); (c) verify fetched files against the
assignment's declared sizes at commit time, quarantining mismatches (FM-22/23); (d)
harden recovery to quarantine-and-alarm unrecognized or colliding chunk directories
instead of wedging (GAP-20) or failing startup wholesale.

## Consequences

FM-1/REQ-24 become achievable; per-chunk blast radius (FM-3) becomes real on the
intake path. Costs: bounded extra intake latency, a size field honored at commit, and
a quarantine mechanism with an operator surface. Supersedes ADR-3's risk acceptance
once accepted (ADR-3 then gains its supersession mark).
