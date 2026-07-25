# 10 — Retention and space

Home doc for `RS`. Band: RS-1..9. The worker's disk footprint has two components with
different lifecycles: the **chunk store** (sized by the assignment) and the **log
store** (sized by query volume × retention).

**RS-1 — Retention policy.** [MUST] The chunk store retains exactly what the applied
assignment names — no size-based, age-based, or pressure-based eviction exists. The
policy table:

| Data | Retained while | Leaves via |
|---|---|---|
| committed chunk | in the desired set, or pinned | WP-14 only (INV-11) |
| transient residue | never intentionally | abort cleanup + recovery sweep (RS-6) |
| log record | younger than P-LOGS-RETENTION | WP-17 only |

**RS-2 — Availability floor.** [MUST] Eviction never removes: desired chunks, pinned
chunks (whatever the assignment says), or — per REQ-25 — more than the P-DEL-FLOOR
fraction of the store per application without operator override ⚠ (ADR-17). Precedence:
pin > assignment (a pinned undesired chunk stays until released; ADR-16's
never-interrupt rule is the same precedence applied to application).

**RS-3 — Excess bound (amplification).** [MUST] At every instant,
`store bytes ≤ live bytes + P-DL-CONC × W-CHUNK-BYTES-MAX + R + G`, where live = bytes
of desired∪pinned committed chunks, the second term bounds in-flight fetch space, `R` =
unswept residue (bounded by RS-6), and `G` = evicted-but-unreclaimed bytes (bounded by
LIV-4's convergence bound × W-CHURN-RATE). ⚠ The bound's slack terms need ratified
values (ADR-19). The log store is additionally bounded by
W-LOG-RATE × P-LOGS-RETENTION + the reclamation lag of RS-7.

**RS-4 — Two-phase deletion.** [MUST] Eviction is logically immediate — the chunk
leaves the namespace and the available set in WP-14's atomic step — and physically
deferred: byte reclamation follows asynchronously. Between the phases the space counts
toward `G` (RS-3) and the data is unreachable by every reader (CN-1). A crash between
phases leaves residue, converged by RS-6.

**RS-5 — Reclamation safety.** [MUST] Physical reclamation is invisible to readers:
it never touches pinned chunks (their eviction hasn't happened — INV-12), never
follows a path a reader could still resolve (namespace removal precedes byte removal),
and never crosses into sibling chunks or foreign entries (CN-10). There is no
sanctioned reader-free mode; the store always assumes live readers.

**RS-6 — Residue convergence.** [MUST] Transient residue — partial fetches, interrupted
eviction remnants — is deleted by the abort path when the process survives, and by the
next recovery sweep otherwise (WP-15). Residue never: becomes adoptable state
(INV-40), blocks a future commit of the same chunk [violated today — a residual
committed-name collision wedges the chunk forever: GAP-20], or accumulates across crash
loops (INV-41). Foreign entries are not residue and are never swept (CN-10).

**RS-7 — Log-store reclamation.** [MUST] Pruned log records' space is reclaimable and
the log store's on-disk size tracks its live contents within a bounded lag ⚠.
[Violated today: the store only ever grows to its high-water mark — GAP-10; probe
HZ-5.]
Records leave only via WP-17; a schema migration MUST NOT strand prior-generation
records as permanent dead weight (CN-10).

**RS-8 — Deletion cost bound.** [SHOULD] Eviction work per reconciliation pass is
proportional to the number of evicted chunks — not to store size — and does not block
query admission (INV-37). Store-size accounting (OB-5) SHOULD cost o(store entries) per
observation or be paced below the reconciliation cadence, within the PF-3 maintenance
budget [today it is a full store walk per loop iteration and per status refresh:
GAP-15 / HZ-1].

**RS-9 — Interactions.**
- **× forks:** identical-range chunks are distinct retention units; evicting one never
  touches the other (INV-3, ADR-4).
- **× queries:** pins defer, never veto — LIV-4 bounds the deferral after release;
  RS-2 gives pins precedence meanwhile.
- **× recovery:** an eviction interrupted before namespace removal un-happens (CN-3
  re-adoption caveat); one interrupted after converges via RS-6.
- **× liveness:** reclamation keep-up under churn is LIV-14; failure to reclaim is an
  alarmed degradation (FM-31), never silent growth (LIV-13).
- **× disk-full:** FM-30 — eviction and reclamation MUST still proceed when the disk is
  full (deletion requires no new space), which is the system's self-healing path.
