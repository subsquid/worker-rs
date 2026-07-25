# ADR-13 — Respond before logging

Status: Accepted (historical)

## Context

The log append is a synchronous durable write on the response path's tail. Ordering it
before the send would add store-write latency to every query and couple client-visible
latency to log-store health. The in-code rationale: "Send before logging … the
durable write stays off the response path."

## Decision

The wire response is sent first; the log record is appended after. A crash between the
two loses the records of responses already sent in that window.

## Consequences

Query latency excludes the durable write. The loss window is an accountability gap
(served work the worker cannot prove — unpaid), never a client-correctness gap.
Accepted as bounded and rare; CT-2 must measure, not eliminate, it (FM-34). Also
forces the log lag floor P-LOGS-LAG, since record order and timestamp order can
disagree near the head. Shapes WP-16, CN-6, RP-22.
