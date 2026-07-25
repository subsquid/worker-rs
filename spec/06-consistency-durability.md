# 06 — Consistency and durability

Home doc for `CN`. Band: CN-1..10.

**CN-1 — Atomic per-chunk visibility.** [MUST] The unit of atomic visibility is one
chunk (DEF-11). A chunk transitions absent → fully-available in a single indivisible
namespace operation (WP-12, WP-20), and available → absent likewise (WP-14). No reader —
query, status read, recovery scan, or metric — ever observes a partial chunk. There is
no cross-chunk atomicity: a half-applied assignment is a normal state, reported
honestly via the unavailability map (DEF-13).

**CN-2 — Read isolation.** [MUST] A query's snapshot is its single pinned chunk
(DEF-15). From pin acquisition to release, the chunk's content is immutable and its
storage is not reclaimed, regardless of concurrent assignment changes or evictions
(INV-12). Lookup and pin acquisition are atomic with respect to eviction: a query
either fails `not_found` or holds a valid pin — never a dangling path (CT-3 races
this).

**CN-3 — Recovery contract.** [MUST] Recovered state ≡ some committed state, in every
field: `A` after WP-15 is exactly the set of chunks whose WP-12 commit completed and
whose WP-14 eviction did not, each with its full committed file set; derived state
(pending set, unavailability map, metrics gauges) is recomputed from that, never
restored from any cache. An eviction whose namespace removal completed stays evicted;
one that did not stays available (re-eviction follows from the next assignment
application — an eviction may thus *un-happen* across a crash, which is accepted and
cited here as the CN-3 re-adoption caveat, bounded by WP-15's `N′ = A′` rule).

**CN-4 — Durability tiers.** [MUST]
- **Process crash (kill, panic, out-of-memory): zero loss.** Every committed chunk and
  every durable log record survives fully intact; transient residue is swept (RS-6).
- **System crash (power loss, kernel fault): bounded suffix loss, well-formed.** Chunks
  committed within the unsynchronized window MAY be lost *as wholes*; what survives
  MUST be well-formed (INV-3) and content-intact. [Content intactness after system
  crash is intent, currently violated — an unsynchronized commit can survive the crash
  with truncated content and still be adopted as available: GAP-5.] Log records are
  durable at append (CN-6) with at most the in-flight record lost (ADR-13).

**CN-5 — Recovery idempotence.** [MUST] Recovery is a pure function of the store's
durable contents: crashing at any point during WP-15 and recovering again yields the
identical adopted state and sweeps the identical residue. Recovery performs no
destructive act on committed data.

**CN-6 — Log-store durability.** [MUST] A log record (DEF-7), once appended (WP-16),
survives
process and system crash. The accepted exception (ADR-13): the response for a query may
be sent before its record is durable, so a crash in that window loses at most the
records of responses already sent within it — an accountability loss, never a
correctness loss for clients.

**CN-7 — Maintenance transparency.** [MUST] Background work — residue sweeps, space
reclamation after eviction, log pruning past retention, store-size accounting — never
changes logical state: the available set, pinned chunks' content, unexpired log
records, and every response are bit-identical with maintenance on or off (metamorphic
test, CT-1).

**CN-8 — Clock independence.** [MUST] Safety never depends on wall-clock monotonicity
or accuracy: a clock jump may delay or hasten freshness-gated behavior (admission
freshness, log lag, pruning) but MUST NOT corrupt state, violate INV-*, or terminate
the process. Backward jumps MUST NOT resurrect pruned records or double-refill metering
buckets beyond P-CU-BURST.

**CN-9 — Single writer.** [MUST] One process owns a store at a time. A process
discovering a store stamped with a different network identity MUST refuse to serve —
before mutating anything, including residue sweeps. [Ordering is intent, currently
violated — the sweep runs before the identity check: GAP-16.] Same-identity concurrent
access is explicitly unspecified (02) and unenforced today (ADR-15; GAP-16 tracks the
hardening decision).

**CN-10 — Format compatibility gate.** [MUST] On startup the worker recognizes its own
store formats. Unrecognized *foreign* entries (unknown directories, stray files) are
tolerated: skipped with an alarm, never deleted, never adopted. Recognized-but-invalid
layout (INV-3 violations among adopted chunks) fails startup rather than serving
inconsistent data. A log-store schema change is applied by migration-on-open, leaving
prior data readable or explicitly migrated — never silently ignored. ⚠ Today's
schema-bump-by-table-rename leaves orphaned prior-generation data unpruned forever;
tracked via GAP-10's register entry.
