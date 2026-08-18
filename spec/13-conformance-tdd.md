# 13 — Conformance & TDD program

Home doc for `CT`, `MG`, `HC`, `GAP`. **Mutable doc #1.** As of: **2026-08-18**
(baseline commit `f86047c`). Statuses: **C** covered · **P** partial · **U** unchecked;
`⊘` marks known-violated, `?` known-suspect.

## Harness architecture

```
 HC-1 scheduler sim ──(network-state, assignments + fault corpus)──▶ ┌─────────┐
 HC-2 origin stub ────(chunk files; byte LEDGER; fault inject)─────▶ │   SUT   │
 HC-8 registry stub ──(epoch, allocations)─────────────────────────▶ │ (black  │
 HC-3 portal driver ──(signed queries, fuzz, disconnects)──────────▶ │  box)   │
 HC-7 kill/restart ───(SIGKILL at kill-points; power-loss emu)─────▶ └────┬────┘
                                                                          │
        HC-5 structural validators ◀── responses, logs, heartbeats ───────┤
        HC-4 reference model (oracle) ◀── same inputs, replayed ──────────┤
        HC-6 observability scraper ◀── metrics, status endpoints ─────────┘
                     │
              comparator: SUT vs model at QUIESCENCE
```

**Quiescence** (comparison points): no input events in flight, the progress heartbeat
(OB-11) and all OB-1 gauges identical across two scrapes ≥ P-QUIESCE-GAP ⚠ apart. The
ledger in HC-2 makes served bytes the provenance oracle for INV-13/21.

**SUT boundary, as built.** The diagram is the target. Today the SUT is the production
subsystems assembled in-process and driven through the production entry points, not a
spawned binary: the input side (IB-40/41/42/43/44) really is over HTTP and really is a
black box, but the query surface is entered below the transport, at
`p2p::{validate_query, execute, build_delivery, build_log}`. So IB-1/2 and the
`P2PController` intake — queue capacity (RP-1 step 4), reject fan-out, the assignment
pending queue — are *not* under test. Closing that needs HC-7-style process spawning plus
a libp2p client; until then `harness::UNCOVERED` carries the list and no row in the matrix
below may claim those properties.

## Reference model (normative pseudocode)

The oracle for CT-1/CT-2/CT-3. Concurrency model: transitions are atomic and
interleave arbitrarily; the SUT may batch but every observation must equal some legal
interleaving's state.

```
state S = { A:set, D:set, N:set, X:doc|⊥, L:map, Q:seq, buckets:map }

apply_assignment(doc):                        # WP-10
    require valid(doc) and has_slice(doc, self)          # WP-2, else reject whole
    S.X := doc; S.N := slice(doc, self)                  # A, D, L, Q untouched
    assert wellformed(S)                                 # INV-1..5

fetch_start(c):                               # WP-11
    require c in pending(S) and |S.D| < P_DL_CONC
    S.D += c

commit(c, files):                             # WP-12
    require c in S.D and files == ledger_bytes(X, c)     # INV-13
    S.A += c; S.D -= c                                   # atomic visibility (CN-1)

abort(c):                                     # WP-13
    require c in S.D
    S.D -= c                                             # residue invisible (RS-6)

evict(c):                                     # WP-14
    require c in S.A and c not in S.N and S.L[c] == 0    # INV-12
    require deletion_floor_ok(S)                         # REQ-25 ⚠
    S.A -= c

recover():                                    # WP-15  (crash may precede)
    S.A := committed_survivors(); S.D := {}; S.L := {}
    S.X := ⊥; S.N := S.A; S.Q := durable_records()
    assert wellformed(S) and no_residue()                # INV-40..42

query(q):                                     # RP path — the read operation
    if not (sig_ok(q) and envelope_ok(q)):               return bad_request     # no CU
    if not fresh(q):     return server_error             # no CU, no record — worker-fault (ADR-20, INV-26)
    if no_capacity():                                    return server_overloaded
    if not spend(buckets[op(q)], 1.0):                   return too_many_requests
    # ---- admitted: exactly one log record from here (INV-32) ----
    if not post_envelope_ok(q):        out := bad_request
    elif chunk(q) not in S.A:          out := not_found
    else:
        pin(q.chunk)                                     # DEF-12; INV-4
        r := EVAL(q.body, ledger_bytes(X, q.chunk), clip(q.range, q.chunk))
        out := downgrade_if_oversized(sign(r))           # RP-14; INV-24
        unpin(q.chunk)
    refund(buckets[op(q)], 1.0 - chip(q)) unless out == server_overloaded   # ADR-6
    S.Q += record(q, out)                                # INV-23: same outcome
    return out

logs_read(cursor):  return page_after(S.Q, cursor, P_LOGS_RESP_MAX, lag=P_LOGS_LAG)
status_read():      return snapshot(X.id, unavail_map(S), stored_bytes, epoch)  # INV-30
```

**Free variables** (the SUT may legitimately vary; everything else must match the
model): (1) `EVAL`'s output bytes — checked structurally (validators) and, where HC-4
implements the reference evaluation for a query subset, byte-compared; (2) compression
byte streams (round-trip equality only); (3) fetch order/timing within WP-3's fairness
rule; (4) which of several legal interleavings occurred; (5) transient residue naming.

## Test-class taxonomy

| CT | Class | Primary properties | Needs |
|---|---|---|---|
| CT-1 | model-conformance property tests (generated transition/query sequences vs the model) | INV-1..5, INV-10..15, INV-20..24, LIV-1/2/4, RP-10..15 | HC-1..5, HC-8, HC-12 |
| CT-2 | crash-recovery kill-point matrix (kill at every transition edge; double-recovery; power-loss emulation) | INV-40..43, CN-3..5, LIV-5/10, REQ-23 | HC-1/2, HC-7 |
| CT-3 | concurrency swarms (queries × churn × downloads × status) | INV-4/12/14/21/30/37, LIV-6 | HC-1/2/3, HC-9, HC-12 |
| CT-4 | input-fault corpus (FM tables, item by item) | FM-10..24, FM-30..32, FM-40..44, INV-36, REQ-24 | HC-1/2/3 injectors |
| CT-5 | interface conformance (IB tables; response/log/CU accounting reconciliation) | IB-1..44, INV-20/23/25/32, RP-20..22, LIV-7/12, INV-15 | HC-3, HC-5/6, HC-8 |
| CT-6 | performance benchmarks (S1–S6; knee; overload+recovery) | SLI-1..8, PF-5, LIV-8 | HC-9/10, HC-6 |
| CT-7 | soak/endurance (crash loops, churn, log growth, stall detection) | LIV-9/13/14, RS-3/6/7, INV-5/41 | HC-9, HC-6, HC-7 |
| CT-8 | isolation / noisy-neighbor (S6) | INV-35, LIV-11, FM-24 | HC-1/2/8, HC-9 |
| CT-9 | fuzz (query surface; assignment surface) | REQ-24, FM-40, FM-12, INV-36 | HC-1/3 fuzzers, HC-12 |

## Structural validators (kind-agnostic, every response, no domain knowledge)

decodable per declared format · response signature verifies (INV-25) · `last_block`
within the requested range per RP-11 · error carries no data (INV-20) · row/block
membership: every emitted block within `[begin, last_block]` · ascending block order
(RP-12) · compressed payload round-trips · log pages: cursor-ordered, gap-free vs
prior page, within retention (INV-5) · heartbeat: map length = assignment slice size,
ones-count consistent (INV-30) · gauges nonnegative and consistent with set algebra
(INV-1).

## Traceability matrix (as of 2026-08-18)

Statuses reflect the actual test inventory: inline unit tests, all built unconditionally;
`state_pbt` / `state_regression` over the chunk state machine and assignment
confirmation; plus the conformance tier over the harness in `tests/harness/`: one binary
per subject — `e2e` (the smoke path, both assignment formats), `query_surface`
(admission outcomes and the RP-20 taxonomy) and `query_concurrency` (separate by
necessity, not topic: the OB signals are process-global, so a gauge assertion cannot
share a process with other query-running tests).
WP/RP/CN/RS rows are enforced through the INV/LIV rows that encode them
(see 07 §reading the catalog).

A Phase 0 row reads **P** only where the smoke path actually asserts the property; the
harness's own `UNCOVERED` and `validators::MISSING` lists name what it still cannot see,
and `declared_gaps_cite_the_spec` keeps those lists pointing at identifiers here.

### Requirements

| ID | CT | Status | Note |
|---|---|---|---|
| REQ-1 | CT-1/5 | P | engine-level output tests, plus an end-to-end signed query over a downloaded chunk (dynamic engine, JSONL only). Per-chunk schema selection unit-tested at the seam: a chunk outside the assignment in force, and one held with no assignment, are each distinct from a legacy chunk rather than resolving by dataset type |
| REQ-2 | CT-1/2 | P | smoke drives fetch→commit and compares committed bytes against HC-2's ledger; atomicity under interruption still untested (needs HC-7) |
| REQ-3 | CT-1/3 | P | set-algebra bookkeeping unit-tested; no eviction-under-load test |
| REQ-4 | CT-1 | P | last_block semantics unit-tested per engine; resumption equivalence untested |
| REQ-10 | CT-4 | P | happy-path intake driven end-to-end in both formats (IB-40 poll → IB-41 fetch → WP-2 apply, and IB-40b → IB-44b bundle → IB-41b fetch → WP-2); the controller's pending queue is outside the harness, and the fault corpus is unwritten |
| REQ-11 | CT-3 | U⊘ | coherence known-violated (GAP-11) |
| REQ-12 | CT-5 | P | ordering/pagination/cleanup unit-tested in memory; smoke adds a file-backed write-then-read with the RP-22 lag observed; durability across restart untested (HC-7) |
| REQ-13 | CT-5 | P⊘ | the running-query gauge is now CT-6-checked (rises under load, bounded by the cap, returns to zero); the GAP-17 liars remain |
| REQ-20 | CT-5 | P | RP-1 step 1 covered: an unverifiable signature is rejected with no CU and no log record. Freshness, envelope and replay untested |
| REQ-21 | CT-1/5 | P | charge/refund/overload-keep unit-tested via mock seams |
| REQ-22 | CT-6 | P | cap enforcement and its overload rejection covered by `query_concurrency`; queue-depth and reject-fan-out shedding still untested (needs the transport) |
| REQ-23 | CT-2 | U | no crash-recovery test exists |
| REQ-24 | CT-4/9 | P⊘ | one malformed input is now survivable and asserted (FM-11's unusable address); the rest of GAP-2 and GAP-4 stand |
| REQ-25 | CT-4 | U⊘ | no floor exists (GAP-3) |

### Properties

| ID | CT | Status | Note |
|---|---|---|---|
| INV-1 | CT-1 | P | set bookkeeping unit tests (state module) |
| INV-2 | CT-1/2 | U | store↔state correspondence never tested |
| INV-3 | CT-2/4 | P | layout parse/overlap/fork unit tests |
| INV-4 | CT-1/3 | U | pin validity untested; u8-width hazard (GAP-18) |
| INV-5 | CT-1/7 | P | ordering/pagination unit tests; replay-id hole known (GAP-12) |
| INV-10 | CT-1 | U | no quiescence test |
| INV-11 | CT-2 | U | |
| INV-12 | CT-1/3 | P | retain-if-locked unit-tested; never raced |
| INV-13 | CT-1 | P⊘ | smoke compares committed bytes against HC-2's ledger on the happy path; refusing corrupt origin bytes still doesn't exist (GAP-5) |
| INV-14 | CT-3 | P | one hand-built interleaving test plus `state_pbt`'s randomized runs over the confirmation critical section, all in the default build; the CT-3 race proper — status reads against assignment flips — is still absent |
| INV-15 | CT-1/5 | P | unit-tested (charge, refund, overload-keep, fractional put); chip-parse hole GAP-13 |
| INV-20 | CT-5 | U | |
| INV-21 | CT-1/3 | U | |
| INV-22 | CT-1 | P | engine unit tests cover truncation/empty cases; boundary emission unpinned (GAP-32) |
| INV-23 | CT-5 | P | downgrade-agreement + log-summary unit tests |
| INV-24 | CT-4/6 | U | boundary corpus absent |
| INV-25 | CT-5 | P | HC-5 verifies the response signature on every response it sees, success and error alike |
| INV-26 | CT-4 | U⊘ | known-violated (GAP-5 store-fault attribution, GAP-33 freshness). The bundle path attributes correctly and is unit-tested both ways: a pinned schema id the loaded bundle lacks is `server_error`, an unknown dataset type stays `bad_request`, and a chunk with no pinned id resolves by type — `server_error` in worker mode, where nothing fills that registry. A query naming a version is driven end to end: the named copy answers, and version 0 — never assigned — is `not_found` |
| INV-30 | CT-3 | U⊘ | known-violated (GAP-11). HC-5 checks map length and ones-count per read, but tearing needs a racing test |
| INV-31 | CT-1/6 | P⊘ | running-query gauge covered; the remaining counters are still known-violated (GAP-17) |
| INV-32 | CT-5/7 | P⊘ | admitted-always-logged unit-tested and now end-to-end (admitted → exactly one record; pre-admission → none); duplicate/oversize drop known (GAP-12/14) |
| INV-35 | CT-8 | U | |
| INV-36 | CT-4 | P | panic-containment unit tests (str/String/assert payloads) |
| INV-37 | CT-3/6 | U | |
| INV-40 | CT-2 | U | |
| INV-41 | CT-2/7 | U | |
| INV-42 | CT-2 | U | |
| INV-43 | CT-2 | U | |
| LIV-1 | CT-1/6 | U | the bound is unmeasured. One way it was violated is now pinned: an assignment the scheduler re-publishes after another one stalled settles again, where deciding from the last-applied id left the applier waiting on a verdict the channel no longer held |
| LIV-2 | CT-1 | U | downloader has zero tests |
| LIV-3 | CT-1/4 | U⊘ | unbounded today (GAP-8) |
| LIV-4 | CT-1/3 | U⊘ | wake-up gap (GAP-6) |
| LIV-5 | CT-2/6 | U | |
| LIV-6 | CT-3/6 | U? | at risk from store walks (GAP-15) |
| LIV-7 | CT-5/7 | P | pagination/resumption unit-tested in memory; smoke adds one file-backed page read |
| LIV-8 | CT-6/8 | U | client-side cooldown coupling under flood shed: HZ-15 |
| LIV-9 | CT-7 | U | no stall alarm exists (OB-11/12 partial — GAP-17) |
| LIV-10 | CT-2 | U | shutdown untested since the subsystem-tree rewrite |
| LIV-11 | CT-8 | U⊘ | global backoff (GAP-7) |
| LIV-12 | CT-5 | U | cold-start window known (GAP-25). The harness waits the window out before serving, so no test measures it yet |
| LIV-13 | CT-4/7 | U | |
| LIV-14 | CT-7 | U⊘ | log-store reclamation broken (GAP-10) |
| FM-1 | CT-4/9 | U⊘ | known-violated (GAP-2) |
| FM-2..3 | CT-4 | U | |
| FM-10 | CT-4 | P | the retry itself is unmeasured, but a corrected fetch location under an unchanged pair reaching a stalled retry is asserted at both ends — the poll announces the move, the applier replaces the queued copy — a location moving under an applied pair re-fetches nothing, and the network going back to the pair in force retracts a failing or queued pair, which is neither retried nor applied later (asserted mid-retry in legacy mode, where nothing waits on a settle, and at the queue directly) |
| FM-11 | CT-4 | P | an unusable address is driven end to end: the worker gives the chunk up at once rather than spending its retry budget on a document that will not change, moves OB-17 once, and converges on the next assignment — the corrected document naming the same slice, so the recovery WP-13 promises is what is driven, and a unit test pins that registering an unchanged slice wakes the reconciler. The credential half is FM-12's whole-document path, since neither format carries credentials per chunk. Which faults are per-chunk and which are whole-document is stated in FM-11 itself; the roster case is unit-tested as a refusal, the address case end to end as a per-chunk give-up |
| FM-12 | CT-4 | P⊘ | a document that cannot be read is refused rather than fatal, asserted at the applier with a corrupted roster; a roster naming a table that is not a file name is refused too, so a document cannot write a chunk's data outside its directory and still commit the chunk; unit-tested at the poll: a pointer that will not decode or names no fetch url is refused and waits at the poll cadence, a pointer the mode does not read may take any shape, and only a body that is not a JSON object is a read failure; oversize intake is still unbounded (GAP-4) |
| FM-13 | CT-4 | U⊘ | no floor (GAP-3) |
| FM-14 | CT-7 | U⊘ | no age signal (GAP-23) |
| FM-20..21 | CT-4 | U | |
| FM-22 | CT-4 | U⊘ | no verification (GAP-5) |
| FM-23 | CT-4 | U | |
| FM-24 | CT-8 | U⊘ | GAP-7 |
| FM-30 | CT-4 | U⊘ | disk-full today retries silently forever — no alarm (GAP-17) |
| FM-31 | CT-4 | U⊘ | panic path (GAP-6) |
| FM-32 | CT-4 | U⊘ | misclassified as client fault (GAP-5) |
| FM-33 | CT-2 | U | |
| FM-34 | CT-2 | U | accepted window (ADR-13) — test bounds it |
| FM-35 | CT-4 | U⊘ | corrupt log row panics the process (GAP-27) |
| FM-40..42 | CT-4/9 | U | |
| FM-43 | CT-4 | U⊘ | replay executes (GAP-12) |
| FM-44 | CT-4 | U⊘ | no cancellation (GAP-8) |
| FM-50 | CT-2 | U | |
| FM-51 | CT-2 | U⊘ | sweep-before-check (GAP-16) |
| FM-52 | CT-4 | U⊘ | fatal at startup (GAP-2) |
| FM-53 | CT-4 | P | keep-previous-schemas unit-tested with a live stub server |
| FM-53b | CT-4 | P | `e2e` drives the block end to end: an unfetchable bundle leaves the assignment unapplied and no chunk fetched. Hash mismatch, damaged cache and retry-until-installed are unit-tested; the metrics half (OB) is unasserted; `assignment_loop_pbt` adds randomized pair histories — over any sequence of refused and transient halves, neither the active assignment nor the installed bundle moves except on a pair that applied whole |
| FM-53c | CT-4 | P | unit-tested: an assignment naming a schema the bundle lacks is refused whole, and a schema survives a later bundle that omits it (bundles merge — IB-44b). ADR-21's stricter rule is driven through the harness: a bundle that omits the schema its assignment references leaves the assignment unapplied and nothing fetched, and the test fails if coverage is judged against the accumulated store instead. The OB-16 alarm is unasserted; the same property run covers refusal atomicity. IB-40b's pair announcement is asserted at the stream by `assignments_pbt` — every changed pair is emitted whole and a bundle change carries its assignment — but that a corrected bundle then gets the refused assignment reconsidered is not itself asserted |
| FM-53d | CT-4 | P | unit-tested at the poll: a state with an assignment and no bundle, and one whose bundle hash does not parse, are each answered as not-applicable rather than an error, the pair fault is counted, and the announced pair is left alone so the corrected state re-offers it whole; a state with no assignment for the mode waits the same way whatever its bundle says, uncounted, since it is indistinguishable from a network that has not migrated. That erroring instead costs hours of backoff is reasoned, not driven — no test measures the poll cadence |
| FM-54 | CT-2 | U | registration wait exists by design; externally invisible (GAP-28) |
| FM-55 | CT-4 | U⊘ | misclassified and invisible (GAP-33) |
| SLI-1..8 | CT-6 | U | no benchmark harness on the default branch |

## Gap register (as of 2026-08-18)

Priorities: **P0** active production risk · **P1** correctness hole with plausible
trigger · **P2** bounded/rare · **P3** polish. "First test" = cheapest failing test.

| GAP | Statement | Violates | Pri | First test |
|---|---|---|---|---|
| GAP-2 | Externally supplied content can terminate the process: a registry error at startup is fatal; a pathological per-chunk file count overflows the download-watchdog arithmetic (`s3_timeout * num_files as u32` — a narrowing cast into a multiply that panics). Two clauses are closed: an unaddressable chunk fails on its own (FM-11, OB-17), and a document the reader panics on is refused where the panic happens (FM-12, OB-18) — the reader still panics on a roster peer id that won't decode, which is a `sqd-assignments` fix | FM-1, FM-52, REQ-24 | P0 | HC-1 chunk listing enough files to overflow the watchdog: worker must survive, alarm, and keep serving |
| GAP-3 | No reconciliation deletion floor: one empty/short assignment wipes the whole store next pass | REQ-25, FM-13, RS-2 | P0 | publish an assignment with zero chunks for the worker: store must survive with an alarm |
| GAP-4 | Assignment intake is unverified and unbounded: the legacy document is parsed without verification (`from_owned_unchecked`) and its decompression is uncapped. The bundle half is bounded — its unpacked cap counts everything decompressed, so an entry ignored by name cannot inflate unbounded while the tar reader skips it | WP-2, FM-12, REQ-24, HZ-12 | P1 | HC-1 serves a decompression bomb and a truncated document: bounded memory, typed rejection, process alive |
| GAP-5 | No payload/content verification anywhere: corrupt origin bytes commit (INV-13), power-loss-truncated chunks are adopted (CN-4), and local corruption surfaces as client-blamed `bad_request` (FM-32) | INV-13, CN-4, FM-22, FM-32 | P1 | HC-2 corrupts one file's bytes: commit must be refused (fails today) |
| GAP-6 | Eviction is fragile: an I/O error panics the process (FM-31), and a chunk unpinned after the eviction pass is not retried until an unrelated event (LIV-4 unbounded) | LIV-4, FM-31, WP-14 | P1 | unassign a pinned chunk, release the pin, inject no further events: bytes must be reclaimed within P-EVICT-BOUND |
| GAP-7 | Fetch retry backoff is a single global value: one failing chunk throttles every dataset's downloads to P-DL-BACKOFF-MAX | LIV-11, FM-24, WP-13 | P1 | S6: one dataset 404s; healthy dataset's commit rate must stay within LIV-2 bounds |
| GAP-8 | No execution deadline and no disconnect cancellation: abandoned queries run to completion holding capacity; a slow-query flood converts to sustained overload | LIV-3, RP-5, FM-44, HZ-3 | P1 | disconnect mid-query; capacity slot must free within P-Q-DEADLINE |
| GAP-20 | A committed-namespace collision (residue or unrecognized dir at a chunk's path) wedges that chunk in an eternal fail-retry loop that also pins the global backoff at max | WP-21, RS-6, LIV-13 | P1 | pre-plant a foreign dir at an assigned chunk's committed path; expect quarantine+alarm, not an infinite loop |
| GAP-10 | Log-store space is never returned: the reclamation statement after pruning never executes; on-disk size plateaus at the high-water mark | RS-7, LIV-14 | P2 | file-backed store: burst writes, idle past retention, size must shrink |
| GAP-11 | Status reports can be torn: the availability map is computed against one assignment and labeled with another's id when application races the (slow) snapshot | INV-30, REQ-11 | P2 | race status reads against assignment flips with injected store-walk latency |
| GAP-12 | Query-id replay is not prevented: a replayed signed query re-executes and re-charges, and its log record is silently dropped on the id collision | RP-2, INV-32, FM-43 | P2 | replay an identical query within P-TS-WINDOW: expect rejection; today observe double charge + one log |
| GAP-13 | The metering chip is derived by parsing the chunk id (contradicting ADR-4): an id outside the legacy pattern silently charges full price | INV-15, DEF-24 | P2 | query 5 % of a chunk whose id defies the legacy pattern: net spend must be 0.05 |
| GAP-14 | Worker log records hash the uncompressed result; portal-side records hash compressed bytes — systematic cross-audit mismatch | INV-23 (margin), OQ-5 | P2 | blocked by OQ-5 decision |
| GAP-15 | Store-size accounting walks the entire store inside the reconciliation loop and again per status refresh: at scale, downloads serialize behind minutes-long walks and status staleness explodes | RS-8, LIV-6, SLI-7, HZ-1 | P2 | W-CHUNKS-scale store with slow-I/O injection: SLI-7 within P-HB-STALENESS |
| GAP-16 | No single-instance enforcement, and the residue sweep runs before the identity check — a mis-keyed second process destroys the incumbent's in-flight fetches before refusing | CN-9, FM-51 | P2 | start a second process with a different key: incumbent's transient state must be untouched |
| GAP-17 | Observability defects: pre-admission rejection causes uncounted (dead no-allocation counter), histograms bucketless, abort causes conflated, no log-store size signal, no stall alarm, and outcome counters record the pre-downgrade outcome (a too-large/unsignable result counts `ok` while wire and log say `server_error`) | OB-4/5/7/8, INV-31, LIV-9 | P2 | CT-5 metrics cross-check per the lying-metrics rule |
| GAP-22 | Execution logs (full query text, client ids) are served to any network member without authorization | RP-22 trust model | P2 | policy decision, then an authz conformance test |
| GAP-23 | No assignment-age observable: a worker silently dropped from assignments serves stale data indefinitely with no alarm | OB-13, FM-14 | P2 | freeze HC-1: alarm level must rise within P-STALL-MAX |
| GAP-24 | The SQL surface bypasses the result-size budget (whole result materialized), reports last_block = 0, echoes full query text into error strings, and ignores the message's `block_range` entirely | RP-14 (margin), IB-12 | P2 | SQL query with a large result: memory bound and typed downgrade |
| GAP-25 | Metering cold start: until the first registry poll completes, every query is rejected no-allocation with no retry hint | LIV-12 | P2 | first admitted query per operator within LIV-12's bound after start |
| GAP-18 | Pin refcount is a narrow fixed-width counter (HZ-11); beyond ~255 concurrent pins of one chunk it wraps and un-protects the chunk (latent: now that P-Q-PAR is enforced, only a misconfigured cap above ~255 can reach it) | INV-4 | P3 | saturation probe at the configured ceiling |
| GAP-19 | No machine-readable subcodes: `not_found` collapses never-assigned / not-yet-fetched / evicted, and `bad_request` collapses signature and envelope causes; diagnosis rides unstable message strings (the freshness misattribution is split out as GAP-33) | RP-20 | P2 | subcode conformance once designed (OQ-7) |
| GAP-21 | Metering bucket: division-by-zero latent at astronomically high allocations; an inverted predicate name invites future inversion bugs; an operator address shared by two clusters has its bucket reset to zero tokens on refresh | INV-15 (margin) | P3 | unit boundary test at the allocation ceiling |
| GAP-26 | Advisory merge gates (MG-2..6) must be promoted to blocking as their HC capabilities land | MG-2..6 | P2 | per-gate: flip to blocking in the same change that completes its HC row |
| GAP-27 | A corrupt/undecodable log-store record panics log delivery, and the fail-fast tree (ADR-14) turns that into process termination | FM-1, FM-35, INV-43 (margin) | P2 | plant a malformed row in the log store; a logs pull must skip-and-alarm with the process alive |
| GAP-28 | The operator surface binds only after on-chain registration and registry init complete, so the registration wait (FM-54) is externally indistinguishable from a hung start; no lifecycle phase or alarm marks it | OB-10, LIV-5 (witness) | P3 | start against a registry stub that never lists the worker: HTTP surface up, waiting phase visible |
| GAP-29 | Status requests spawn unbounded concurrent handler tasks; the P-STATUS-CONC bound in PF-2's inventory does not exist | PF-2, RP-24 | P3 | status-request flood: handler concurrency bounded by P-STATUS-CONC |
| GAP-30 | The anchor-mismatch verdict (stale continuation parent hash) — load-bearing for portal fork recovery — is carried only in the legacy engine's `server_error` message text, which RP-20 declares unstable; portals parse the exact string, so any wording change silently converts client-side conflict recovery into terminal errors | RP-20 (anchor verdict), OQ-7 | P1 | CT-5: a stale-anchor query yields a machine-distinguishable verdict; interim regression pins the exact string (IB-13) |
| GAP-31 | The two oversize paths emit divergent `server_error` strings (`Response too large` at the engine's uncompressed cap vs `query result too large` at the encoded-message downgrade); portals special-case only the former, so the latter degrades to a terminal generic failure client-side | RP-14, IB-13 | P2 | drive both oversize paths; assert one verdict surface |
| GAP-32 | Boundary emission (RP-11) is provided by the legacy engine's weight-0 pinning but is unverified for the dynamic engine; if the dynamic engine returns zero records for an evaluated-but-unmatched range, portal-side client resumption breaks the moment portals adopt it | RP-11 (boundary emission) | P1 | both engines: a selective query matching nothing over an evaluated range returns the boundary records, last record = coverage cursor |
| GAP-33 | Freshness rejections blame the client: a timestamp outside P-TS-WINDOW — a verdict whose reference input is the worker's own clock — is typed `bad_request`, which routing clients treat as terminal, so one skewed worker converts valid queries into client-visible terminal errors with no reroute; no skew signal or alarm exists | INV-26, RP-20 (freshness verdict), FM-55, OB-15 | P1 | CT-4: skew the SUT clock past P-TS-WINDOW; a valid signed query must yield `server_error` (never `bad_request`), OB-15 signals must move, alarm past P-SKEW-ALARM |

## Build order

1. ~~**Phase 0 — harness skeleton**~~ **done**: HC-1/2/3/8 stubs + HC-5 validators +
   HC-12 seeding; one end-to-end smoke (assign → download → query → verify → logs pull).
   Lives in `tests/harness/`, driven by one test binary per subject; the crate grew a
   library target so the tier can reach it. The SUT is assembled from the production subsystems
   and driven through the production functions, but the libp2p transport and the
   `P2PController` event loops are outside it — `harness::UNCOVERED` is the standing list.
2. **Phase 1 — P0 gaps**: a failing test per gap, then the fix. MG-4 becomes meaningful
   here. The query-concurrency gap is closed (test first, then a one-token fix; the
   register row is gone and RP-4/REQ-22/INV-31/PF-1 no longer carry the exception).
   GAP-3 remains: its HC-1 fault knob (`NoChunksForWorker`) exists and is driven, but no
   deletion floor does. GAP-2 is down to its startup and watchdog clauses.
3. **Phase 2 — correctness core**: HC-4 reference model; CT-1 property runs; CT-2
   kill-point matrix (HC-7); CT-5 accounting reconciliation. Burn P1 gaps.
4. **Phase 3 — robustness**: CT-3 swarms, CT-4 full corpus, CT-9 fuzz; P2 gaps.
5. **Phase 4 — performance regime**: HC-9/10, CT-6 baselines (replace every "unknown"
   in 11), CT-7/8; ratify ADR-19; promote MG-5/6.
   Every phase ends by updating this matrix and register.

## Merge gates

| MG | Gate | Threshold | When | Enforced by | Blocking? |
|---|---|---|---|---|---|
| MG-1 | spec integrity: the suite's linter reports zero errors | P-GATE-SPEC-ERR | per-PR | HC-13 | **yes** |
| MG-2 | property-coverage ratchet: no matrix row regresses; a PR adding a REQ/INV/LIV/FM/SLI adds its row and CT class in the same change | P-GATE-PROP-RATCHET | per-PR | HC-13 (row-completeness) + review | advisory → GAP-26 |
| MG-3 | line coverage: diff ≥ P-COV-DIFF, repo floor ≥ P-COV-TOTAL, ratchet-up only | P-COV-DIFF, P-COV-TOTAL | per-PR | HC-11 | advisory → GAP-26 |
| MG-4 | PR conformance subset: CT-1 (bounded run), CT-4, CT-5 green within P-GATE-PR-TIME | P-GATE-PR-TIME | per-PR | HC-1..5 | advisory → GAP-26 |
| MG-5 | deep classes: CT-2/3/7/8/9 green | P-GATE-NIGHTLY | nightly | HC-7, HC-9 | advisory → GAP-26 |
| MG-6 | performance: every SLI within P-PERF-NOISE of its committed baseline | P-PERF-NOISE | nightly + pre-release | HC-10 | advisory → GAP-26 |
| MG-7 | static: formatter clean, lint at the pinned deny level, dependency audit | P-GATE-LINT | per-PR | HC-14 | **yes** |
| MG-8 | failing-test-first for every GAP closure and bug fix (register's "first test" column), plus flake policy: P-FLAKE-RETRY retries, then quarantine with owner + expiry — never silent skip | P-FLAKE-RETRY | per-PR (review checklist) | HC-14 + review | **yes** (by review) |

## Harness capability register

| HC | Capability | Needed by | Status | Note |
|---|---|---|---|---|
| HC-1 | scheduler simulator: network-state + assignment documents, fault corpus (IB-40/41 and IB-40b/41b/44b) | CT-1..4, CT-8/9, MG-4/5 | **P** | `tests/harness/scheduler.rs`; real `sqd-assignments` builder over HTTP, either format per `Config::format`, worker format serving a schema bundle alongside and able to republish a chunk at a version whose files live under a generation prefix (IB-41b). Fault corpus holds 3 of the CT-4 cases (bad file URL and empty slice, both driven by `e2e`; truncated document, wired but undriven) plus two bundle faults: unfetchable (FM-53b) and not covering its assignment (FM-53c) — the rest are unwritten |
| HC-2 | data-origin stub with byte ledger + injectors: delay, stall, error, corrupt, oversize (IB-42) | CT-1..4, CT-8, MG-4/5 | **P** | `tests/harness/origin.rs`; ledger = provenance oracle, wired into the smoke test's INV-13 check. Injectors: delay, stall, status, corrupt, truncate — oversize absent |
| HC-3 | portal driver: keys, signed queries, disconnector, fuzzer (IB-10) | CT-1, CT-3..5, CT-9, MG-4 | **P** | `tests/harness/portal.rs`; seeded keys, genuinely signed queries, per-field deviation knobs. No disconnector (needs the transport) and no fuzzer |
| HC-4 | reference model as executable oracle (§model) | CT-1..3 | U | |
| HC-5 | structural validators (§validators) | CT-1..5, MG-4 | **P** | `tests/harness/validators.rs`: query response, log page, status. `validators::MISSING` names the two it can't yet do (INV-1 gauges, cross-restart INV-5) |
| HC-6 | observability scraper + quiescence gate | CT-5..7 | U | |
| HC-7 | kill/restart harness with kill-point + power-loss emulation | CT-2, CT-7, MG-5 | U | |
| HC-8 | chain-registry stub: epochs, allocations (IB-43) | CT-1, CT-5, CT-8 | **C** | `tests/harness/registry.rs`: a programmable `sqd_contract_client::Client` — epoch advance, allocation change, read failure |
| HC-9 | load/swarm driver | CT-3, CT-6..8, MG-5 | U | |
| HC-10 | benchmark runner, committed baselines, noise band | CT-6, MG-6 | U | bench-branch harness exists off-mainline; not wired |
| HC-11 | coverage instrumentation in CI | MG-3 | U | |
| HC-12 | deterministic seeding; seed recorded on failure | CT-1, CT-3, CT-9 | **C** | `tests/harness/seed.rs`: labelled streams off one root seed (`SQD_CONFORMANCE_SEED`), printed on panic; `corpus.rs` generates chunks rather than checking them in |
| HC-13 | spec linter (`tools/check_spec.py`) wired in CI | MG-1, MG-2 | **C** | this suite's standing gate |
| HC-14 | static CI toolchain: build, unit tests, formatter, pinned lint level | MG-7, MG-8 | **C** | exists on the default branch |
