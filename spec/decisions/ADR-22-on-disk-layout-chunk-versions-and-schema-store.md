# ADR-22 — On-disk layout: chunk versions and the schema store

Status: Accepted

## Context

Two things new to `--assignment-source worker` have to live on disk, inside a data directory
whose shape already exists and already holds a fleet's worth of data.

A batch job may republish a chunk: the same dataset and chunk id, rewritten contents, served by
the network under a generation prefix of its own and announced in the worker assignment with
a non-zero `version` (IB-41b). Keyed by ⟨dataset, chunk id⟩ alone (ADR-4), a rewrite read as
the copy already held — never fetched, the old bytes served on. A rewrite therefore has to be
a different chunk to the worker: a different key, and a different place on disk. Three things
constrain how, all of them older than versions: the unavailability map is one bit per chunk in
chunk-ref order (DEF-13) and is read by a scheduler that knows only the ids; every existing
store holds only ingested copies at `worker/<dataset>/<top>/<chunk id>`; and the store is the
only record of what is held (INV-2), so a restart must tell the copies apart from their paths
alone.

Query schemas arrive in bundles (IB-44b) and have to persist: a bundle is fetched by the hash
the network currently publishes, chunks on disk outlive the bundle that described them, and no
schema can be fetched again by id or hash once the network moves on (ADR-21). So the schemas a
worker holds must be kept locally, in a shape that survives a restart and a crash mid-install.

## Decision

**Where things live.** The data directory before this decision held the chunk store, the
identity stamp (ADR-15) and the query log store. It gains one entry, `schemas/`; inside the
chunk store it gains one kind of subtree, `_v<n>/`. Everything else is unchanged. One dataset
holding chunk `0000001000/0000001000-0000001999-abcdef12` at both its ingested version and a
rewrite at version 3, beside an untouched neighbour, and a schema store with two bundles merged
and a third being staged, looks like this:

```
<data-dir>/
├── peer_id                                         identity stamp (ADR-15)          — predates this ADR
├── logs.db                                         query log store (DEF-8)         — predates this ADR
├── worker/                                         chunk store (DEF-6)             — predates this ADR
│   └── <base64url(dataset id)>/                    one directory per dataset       — predates this ADR
│       ├── 0000001000/                             top dir, ten digits: version 0 lives here
│       │   ├── 0000001000-0000001999-abcdef12/     the ingested copy, where a legacy chunk is
│       │   │   ├── blocks.parquet
│       │   │   └── logs.parquet
│       │   └── 0000002000-0000002999-bbbbbbbb/     a neighbour never rewritten
│       │       └── …
│       └── _v3/                                    NEW — this dataset's chunks at version 3
│           └── 0000001000/
│               └── 0000001000-0000001999-abcdef12/ the same id, rewritten; both copies coexist
│                   ├── blocks.parquet
│                   └── logs.parquet
└── schemas/                                        NEW — schema store (IB-44b); worker mode only
    ├── 7.yaml                                      one file per schema id, merged across bundles
    ├── 12.yaml
    └── temp-a1b2c3/                                a bundle being staged; swept at the next start
        └── 15.yaml
```

**Chunk identity.** The chunk ref is the triple ⟨dataset, chunk id, version⟩ (DEF-4). Version
0 is the ingested copy, and is what a legacy assignment names for every chunk and what a query
naming no version asks for (`chunk_version`, IB-13). The version is the *last* coordinate of
the ordering, so a rewrite sorts beside the copy it replaces and a reader that knows only the
ids computes the same DEF-13 order. Folding the version into the id string was rejected: it
would sort a rewrite after every other chunk of its dataset and silently misalign the bitmap
at both ends, exactly when generations are in use.

**Chunk layout.** Version 0 stays at `<dataset>/<top>/<chunk id>`. Version n > 0 is stored at
`<dataset>/_v<n>/<top>/<chunk id>`: one subtree per version, under the dataset (DEF-6). The
three chunks in the tree above, as refs and as paths under the dataset directory:

| chunk ref ⟨dataset, chunk id, version⟩ | path under `worker/<base64url(dataset id)>/` |
|---|---|
| ⟨D, `0000001000/0000001000-0000001999-abcdef12`, 0⟩ | `0000001000/0000001000-0000001999-abcdef12` |
| ⟨D, `0000001000/0000001000-0000001999-abcdef12`, 3⟩ | `_v3/0000001000/0000001000-0000001999-abcdef12` |
| ⟨D, `0000001000/0000002000-0000002999-bbbbbbbb`, 0⟩ | `0000001000/0000002000-0000002999-bbbbbbbb` |

In chunk-ref order these three rows are exactly the order listed — ⟨…abcdef12, 0⟩, then its
rewrite ⟨…abcdef12, 3⟩, then the next id — which is the DEF-13 bit order, and the order a
reader holding only the ids would compute.

The `_v` prefix cannot collide with a top directory (ten decimal digits), and `_v<n>` with `n`
a canonical positive decimal is the only spelling adopted — `_v0`, `_v01`, `_v+1` name no
version and their contents are invisible rather than adopted at a guessed version. Rejected:
writing a rewrite over the copy it replaces (a restart could not tell which it holds, and
deletion-before-download would have nothing to gate on); a suffix on the chunk directory
(`<chunk id>_v<n>`) — the id is an opaque key (ADR-4) that this worker's layout reader and the
other language implementations parse as `<top>/<first>-<last>-<hash>…`, so every reader would
have had to carve the suffix out; and a per-generation dataset directory — the dataset
directory is the base64url of the dataset id and is decoded as exactly that at startup.

**Coexistence and removal.** A rewrite and the copy it replaces are distinct refs at distinct
paths. Both may be held while an assignment transitions; the superseded copy is removed by
ordinary reconciliation, deletion before download (RS-3), never by the rewrite's arrival.
Each version's subtree is validated on its own terms (INV-3): a rewrite may re-cut ranges.

**Schema store.** Schemas live at `<data-dir>/schemas/<id>.yaml`, one file per schema id,
beside the chunk store and not inside it: a schema is not any one chunk's, it is shared by
every chunk written with it, and it outlives every bundle that carried it (ADR-21). The store
is keyed by the bare id — not by the bundle that brought it, and not by network. A bundle
lands in four steps, and the store is not touched until the last:

1. fetched, and verified against the hash the network published;
2. unpacked into a `temp-`-prefixed staging directory *inside* `schemas/`, so that the
   final step is a same-filesystem rename and a crash leaves only a `temp-` directory, which
   the next start sweeps;
3. each staged `<id>.yaml` is compared with the store: absent → to be installed, identical →
   already held, different → the bundle is refused (IB-44b, FM-53b);
4. the ids to be installed are renamed into place, and only then is the bundle published.

At startup the store is adopted by reading `schemas/<id>.yaml`; a file that will not parse is
removed rather than skipped (it names no meaning a chunk can be served with, and in place it
would read as a republished id and refuse every bundle carrying that id). Rejected: a
directory per bundle hash — a schema would then be addressable by the hash that brought it,
but chunks outlive bundles, nothing re-fetches a schema by id or hash, and reclamation has to
key on the ids chunks on disk were written with, so the store accumulates by id (ADR-21);
keying the store by network — not done, deliberately kept flat (see Consequences).

**Recovery.** A restart adopts every `worker/<dataset>/<top>/<chunk id>` as version 0, every
`worker/<dataset>/_v<n>/<top>/<chunk id>` as version n, and every `schemas/<id>.yaml` as
schema `id`; no manifest is introduced anywhere (INV-2).

## Consequences

No migration: a store written before versions existed adopts unchanged, `schemas/` is simply
created on first use, and legacy mode is untouched except that a query naming a version other
than 0 answers `not_found` — legacy mode never writes `schemas/` at all (its registry is the
in-memory CDN manifest, FM-53). ADR-4 is extended, not reversed — the chunk id stays an opaque
key; the key gained a coordinate. A portal reads a rewrite only by naming its version: an
unversioned query asks for 0, so a chunk the worker holds only at a non-zero version answers
`not_found` to it, and the scheduler must not publish versions before portals send
`chunk_version`. While both copies are held the store counts both toward RS-3's bound, and
reclamation of schemas (ADR-21) must key on the write schemas of chunks on disk at every
version. Because the schema store is keyed by bare id, a data directory reused across networks
whose schema ids disagree refuses the first bundle carrying a clashing id (FM-53b) until the
operator clears `schemas/`. Shapes DEF-4, DEF-6, DEF-13, INV-2, INV-3, IB-13, IB-41b, IB-44b.
