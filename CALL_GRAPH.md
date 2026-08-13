# PR 4 call graph

Temporary review aid for the worker-assignment and schema-bundle flow.

## Startup and subsystem wiring

```mermaid
flowchart TD
    Main["main() / run()"]
    State["StateManager::new"]
    SchemaManager["SchemaManager::open"]
    Registry["SchemaManager::registry"]
    Worker["Worker::new"]
    Controller["create_p2p_controller"]
    Start["P2PController::start_subsystems"]

    Main --> State
    Main --> SchemaManager
    SchemaManager --> Registry
    State --> Worker
    Registry --> Worker
    Worker --> Controller
    SchemaManager --> Controller
    Controller --> Start

    Start --> Assignments["run_assignments_loop"]
    Start --> Queries["run_queries_loop"]
    Start --> SQL["run_sql_queries_loop"]
    Start --> Storage["StateManager::run"]
    Start --> Status["logs / status"]
    Start --> Source{"Assignment source?"}
    Source -->|Legacy| CDN["run_schemas_refresh_loop"]
    Source -->|Worker| NoCDN["No CDN schema refresh"]
```

`SchemaManager` owns and serializes schema mutation. `SchemaRegistry` is the
lock-free query view. `StateManager` owns assignment and chunk state. `Worker`
connects queries to both registries.

## Network-state discovery

```mermaid
flowchart TD
    Stream["new_assignments_stream"]
    Poll["poll_network_state"]
    Fetch["fetch_network_state"]
    Mode{"Assignment source"}

    Stream --> Poll --> Fetch --> Mode
    Mode -->|Legacy| LegacyRef["Read assignment reference"]
    LegacyRef --> Visible["visible_assignment"]
    Visible --> LegacyUpdate["NetworkUpdate::Assignment"]

    Mode -->|Worker| Required["Require worker_assignment<br/>and schema_bundle"]
    Required --> ParseBundle["Parse SchemaBundle reference"]
    ParseBundle --> WorkerUpdate["NetworkUpdate::Assignment<br/>with bundle"]
    ParseBundle --> BundleOnly["NetworkUpdate::SchemaBundle"]
```

Worker-mode network state is not an application snapshot. The assignment and
bundle form a validation boundary, while installed schemas accumulate by
immutable schema ID.

## Worker-assignment application

```mermaid
sequenceDiagram
    participant P2P as run_assignments_loop
    participant SM as SchemaManager
    participant WA as Assignment fetch
    participant W as Worker
    participant DI as DatasetsIndex
    participant SR as SchemaRegistry
    participant STM as StateManager
    participant DL as Download loop

    P2P->>SM: prepare(schema_bundle)
    SM->>SM: acquire mutation lock
    SM->>SM: download, hash, unpack, parse
    SM->>SM: compare with installed schemas
    SM-->>P2P: PreparedSchemaUpdate (holds lock)

    P2P->>WA: fetch_worker_assignment()
    WA-->>P2P: Assignment
    P2P->>W: prepare_assignment()
    W->>STM: prepare_assignment()
    STM->>DI: new(assignment, roster, bundle IDs)
    DI->>DI: validate worker and roster
    DI->>DI: verify bundle covers assignment schemas
    DI-->>STM: PreparedAssignment
    STM-->>W: PreparedAssignment
    W-->>P2P: PreparedAssignment

    P2P->>SR: PreparedSchemaUpdate::install()
    SR->>SR: rename only missing schema files
    SR->>SR: publish accumulated ArcSwap snapshot
    Note over SM,SR: mutation lock released

    P2P->>W: register_prepared_assignment()
    W->>STM: set_prepared_assignment()
    STM->>STM: replace DatasetsIndex
    STM->>STM: set desired chunks
    STM->>STM: update active assignment and status
    STM->>DL: wake download loop

    P2P->>W: wait_until_assignment_settled()
    DL->>DL: remove stale chunks
    DL->>DL: download required chunks
    DL->>STM: mark assignment settled
    STM-->>W: settled
    W-->>P2P: settled
```

A rejection before registration preserves the current active assignment and
status. `SchemaManager::prepare()` precedes assignment fetching, so its guard is
held during assignment download and validation as well as filesystem mutation.

## Bundle-only update

```mermaid
flowchart TD
    Update["NetworkUpdate::SchemaBundle"]
    Absorb["absorb_update"]
    Prepare["SchemaManager::prepare"]
    Validate["Check bundle covers<br/>active assignment"]
    Valid{"Valid?"}
    Install["PreparedSchemaUpdate::install"]
    Reject["Drop prepared update"]
    Assignment["Existing assignment remains active"]

    Update --> Absorb --> Prepare --> Validate --> Valid
    Valid -->|Yes| Install --> Assignment
    Valid -->|No| Reject --> Assignment
```

This installs newly available immutable schemas without re-registering the
current assignment.

## Storage reconciliation

```mermaid
flowchart TD
    Run["StateManager::run"]
    Completed["Handle completed downloads"]
    Cancel["Cancel obsolete downloads"]
    Remove["Remove undesired chunks"]
    Index["Read current DatasetsIndex"]
    Next["State::take_next_download"]
    Files["DatasetsIndex::list_files"]
    Download["Start chunk download"]
    Applied{"State fully applied?"}
    Settled["Mark assignment settled"]

    Run --> Completed --> Cancel --> Remove --> Index --> Next
    Next -->|Chunk available| Files --> Download --> Run
    Next -->|Nothing pending| Applied
    Applied -->|Yes| Settled --> Run
    Applied -->|No| Run
```

Worker assignments derive required `<table>.parquet` files from the schema
roster and chunk bitmap. Legacy assignments carry explicit file lists.

## Query-time schema selection

```mermaid
flowchart TD
    Event["run_queries_loop"]
    Handle["handle_query"]
    Execute["execute"]
    Worker["Worker::run_query"]
    Type["extract_dataset_type"]
    Chunk["StateManager::get_query_chunk"]
    Result{"ChunkSchema"}

    Event --> Handle --> Execute --> Worker --> Type --> Chunk --> Result
    Result -->|Pinned schema ID| ByID["SchemaRegistry::get_by_id"]
    Result -->|Unpinned legacy type| ByType["SchemaRegistry::get_by_type"]
    Result -->|Unassigned| NotFound["NotFound"]
    Result -->|No assignment| Error["Server error"]
    ByID --> Engine["experimental_engine::execute_query"]
    ByType --> Engine
```

Worker-format chunks return a pinned schema ID resolved from the accumulated
registry snapshot. Old schema IDs must remain available while existing chunks
can still reference them.

## Legacy and worker differences

| Concern | Legacy | Worker assignment |
| --- | --- | --- |
| Schema source | Periodic CDN refresh | `schema_bundle` in network state |
| Assignment content | Explicit files | Schema roster and chunk bitmap |
| Schema lookup | Dataset type | Immutable schema ID |
| Schema update | Replace legacy view | Add missing schemas |
| Bundle validation | Separate refresh | Covers every assignment schema ID |
| Application | Apply assignment directly | Prepare bundle, validate assignment, install schemas, publish assignment |
