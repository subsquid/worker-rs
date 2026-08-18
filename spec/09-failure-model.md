# 09 — Failure model

Home doc for `FM`. Bands: FM-1..3 global requirements · FM-10..14 assignment-side ·
FM-20..24 origin-side · FM-30..35 storage & process · FM-40..44 client-side ·
FM-50..55 operator & auxiliary dependencies.

Response verbs, used in every table:

- **mask** — absorb; the fault is invisible outside internal retries/metrics.
- **degrade** — reduced but well-defined service, honestly reported.
- **fail-safe** — refuse the specific operation with a typed error; everything else
  continues.
- **alarm** — raise a reason-coded alarm level (OB-12) in addition to any of the above.

## Global requirements

**FM-1 — No externally-triggered termination.** [MUST — intent, currently violated:
GAP-2] No content arriving from any actor — query bytes, assignment documents, origin
payloads, log requests, registry responses, schema manifests — may terminate,
deadlock, or abort the process. The only sanctioned self-terminations: operator
shutdown, startup refusal on identity mismatch (CN-9) or invalid adopted layout
(INV-3), and unrecoverable *local* integrity faults where continuing would corrupt
committed data.

**FM-2 — Transient vs integrity.** [MUST] Every failure path is classified: transient
(retry under bounded backoff, mask/degrade) or integrity (stop the affected unit,
alarm, never retry blindly). The classification is per WP-22 on the write path and
RP-20 on the read path; LIV-13 bounds how long a transient may repeat before alarming.

**FM-3 — Blast-radius containment.** [MUST] A fault's effect is confined to its unit:
one file → its chunk; one chunk → that chunk's availability; one dataset's origin →
that dataset's convergence (LIV-11); one query → that query (INV-36); one operator's
budget → that operator (INV-35); one assignment document → intake of that document
(WP-2). Store-wide or process-wide effect from a unit fault is a violation.

## Assignment-side faults

| # | Fault | Required response |
|---|---|---|
| FM-10 | network-state/assignment endpoint unreachable or slow | mask: retry per WP-1; previous assignment stays in force; alarm past P-STALL-MAX (LIV-13) |
| FM-11 | document with malformed per-chunk entries (bad address, bad credential) | degrade: affected chunks fail per-chunk (WP-2); rest of the document applies; alarm (OB-17). Credentials are per worker in both formats, not per chunk, so one that won't decrypt is FM-12's whole-document rejection |
| FM-12 | document malformed as a whole, oversized, undecodable, or missing this worker | fail-safe + alarm (OB-18): reject whole document (WP-2); keep prior assignment; keep serving — including a document that cannot be read at all, whose reader panic is contained where it happens rather than forwarded. [Oversize unbounded today — GAP-4] |
| FM-13 | equivocating/regressive document (wipes the store, flip-flops) | degrade + alarm: REQ-25 deletion floor holds data; application order is arrival order (NG2) — flip-flops churn but never corrupt |
| FM-14 | stale document served long-term (worker dropped or endpoint frozen) | degrade + alarm: serve last-applied data honestly; assignment-age observable (OB-13) rises. [No age alarm exists today — GAP-23] |

## Origin-side faults

| # | Fault | Required response |
|---|---|---|
| FM-20 | file fetch errors (missing, denied, server error) | mask: WP-13 abort + bounded retry while desired; per-chunk blast radius (FM-3); alarm past P-STALL-MAX |
| FM-21 | slow or stalling transfers | mask: P-DL-FILE-TIMEOUT / P-DL-STALL-TIMEOUT abort, then as FM-20 |
| FM-22 | corrupt/truncated payload (wrong bytes for a named file) | fail-safe + alarm: MUST NOT commit (INV-13); quarantine the chunk after bounded retries. [No payload verification exists — GAP-5] |
| FM-23 | oversized payload (exceeds declared size) | fail-safe: abort the file at the declared-size bound ⚠ (ADR-18); as FM-20 |
| FM-24 | origin serving for one dataset down, others healthy | degrade: LIV-11 — unaffected datasets converge normally. [Global backoff couples them today — GAP-7] |

## Storage & process faults

| # | Fault | Required response |
|---|---|---|
| FM-30 | disk full | degrade + alarm: fetches abort (WP-13); committed data and query service intact; stored-bytes and alarm observables reflect it. Never an infinite silent retry loop (LIV-13) |
| FM-31 | I/O error on eviction or sweep | degrade + alarm: per-chunk quarantine; retry later. [Currently: process panic — GAP-6] |
| FM-32 | store content lost/corrupted out-of-band (operator deletion, bit rot, post-power-loss truncation) | fail-safe + alarm: affected chunk leaves service as `server_error`→quarantine, not as client-blamed `bad_request`. [Misclassified today — GAP-5] |
| FM-33 | process crash at any point (kill, panic, out-of-memory) | recover per CN-3/4/5; INV-40..43 |
| FM-34 | crash between response send and log append | accepted bounded loss (ADR-13, CN-6); no client-visible effect |
| FM-35 | log-store record corrupt/undecodable | fail-safe + alarm: skip-and-quarantine the record; log delivery and the process continue (FM-1). [Currently: a decode panic terminates the process — GAP-27] |

## Client-side faults

| # | Fault | Required response |
|---|---|---|
| FM-40 | malformed/unparseable query bytes | fail-safe: no response (nothing to bind a signed reply to), no CU, no log (RP-20) |
| FM-41 | authenticated-but-invalid query (bad signature fields, illegal envelope) | fail-safe: typed `bad_request`, pre-admission (RP-1, INV-26); stale timestamps are FM-55, not a client fault |
| FM-42 | query flood beyond capacity | degrade: typed `server_overloaded` up to P-REJECT-CONC, then connection drops (ADR-9); recover per LIV-8 |
| FM-43 | replayed query id | ⚠ currently re-executed and re-charged; intended: fail-safe reject within P-TS-WINDOW (GAP-12) |
| FM-44 | client disconnects mid-execution | mask: abandon response at transport timeout; SHOULD cancel execution (RP-5). [No cancellation today — GAP-8] |

## Operator & auxiliary dependencies

| # | Fault | Required response |
|---|---|---|
| FM-50 | misconfiguration (unparseable addresses, missing required settings) | fail-safe: refuse startup with a diagnostic — never a half-configured worker |
| FM-51 | second process, different identity, same store | fail-safe: refuse before mutating (CN-9). [Sweep-before-check today — GAP-16] |
| FM-52 | chain-registry unreachable or erroring | degrade: serve with last-known allocations/epoch; alarm past P-STALL-MAX. [Startup registry error is fatal today — GAP-2 register entry] |
| FM-53 | schema-registry unreachable or malformed manifest (IB-44, legacy mode) | degrade: keep previously loaded schemas; dynamic-engine queries fail typed `server_error` until first load |
| FM-53b | schema bundle unreachable, hash mismatch, or unusable (IB-44b, `--assignment-source worker`) | fail-safe: the jointly validated update cannot complete, so the assignment is not applied and the worker keeps serving the previous one. Valid schemas cached by an interrupted earlier attempt may remain. Retried with backoff; a bundle that fails to install is re-offered rather than recorded as consumed. Observable via OB-16 |
| FM-53c | assignment references a write schema its bundle doesn't carry (IB-41b) | fail-safe + alarm: the pair diverges, which is a scheduler-invariant break (ADR-21) — refuse the assignment whole, keep serving the previous one, and alarm whether or not the schemas already loaded would have covered it. Applying it would download chunks that cannot be queried, and under a type-keyed fallback would silently execute them against a different version of the same dataset type. A bundle is merged into the schema store rather than replacing it (IB-44b), so no assignment can lose a schema it is using to a later bundle. Recovery does not wait for a new assignment id: a corrected bundle re-announces the pair (IB-40b), so the refused assignment is reconsidered against it |
| FM-54 | worker absent from the on-chain registry at startup | degrade by design: poll the registry and serve nothing until listed; the wait is a visible lifecycle phase (OB-10) with an alarm past P-STALL-MAX (OB-12). [The wait is externally invisible today — GAP-28] |
| FM-55 | worker clock skewed beyond P-TS-WINDOW tolerance (drift, operator error) | degrade + alarm: freshness rejections surface as `server_error`, never `bad_request` (RP-20 freshness verdict — a stale sender and own skew are indistinguishable here, ADR-20); rejection rate and skew estimate observable (OB-15); alarm past P-SKEW-ALARM. [Misclassified and invisible today — GAP-33] |

## Fault → property → test class

| Fault family | Properties exercised | Test class |
|---|---|---|
| FM-10..14 | WP-1/2, INV-14, REQ-25, LIV-1/13 | CT-4 (document corpus), CT-3 (churn races) |
| FM-20..24 | WP-13, INV-13, LIV-2/11, RS-6 | CT-4 + HC-2 fault injection, CT-8 |
| FM-30..32 | WP-14/22, INV-2/40, LIV-13, RS-4..6 | CT-2, CT-4 (storage-fault injection) |
| FM-35 | FM-1, CN-6, RP-22 | CT-4 (log-store fault corpus) |
| FM-33..34 | CN-3..6, INV-40..43 | CT-2 (kill-point matrix) |
| FM-40..44 | RP-1..5, INV-20/36, LIV-8 | CT-4, CT-9 (fuzz), CT-6 (storm) |
| FM-50..55 | CN-9, LIV-12, FM-1, INV-26 | CT-2 (startup corpus), CT-4, CT-5 |
