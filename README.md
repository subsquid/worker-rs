## Subsquid worker
This is a WIP Rust implementation of the Worker. The previous (Python) version can be found [here](https://github.com/subsquid/archive.py/tree/master).
A worker is a service that downloads assigned data chunks from persistent storage (currently S3) and processes incoming data queries that reference those data chunks. It can be used in two modes:
- Centralized. In this setup, the assignment is received from a centralized [router](https://github.com/subsquid/archive-router/tree/main/crates/router) that is aware of each worker and relies on it (almost) always being available. In this case, the communication between the worker and the router happens directly through HTTP requests.
- Decentralized. In this setup, the assignment comes from a [scheduler](https://github.com/subsquid/archive-router/tree/b01d86aaf9fb5e14b16c3d24eb7419d413ce8b46/crates/network-scheduler) via a [P2P communication protocol](https://github.com/subsquid/subsquid-network/tree/main/transport). The chunks can be reassigned as the workers join and leave the network.

### Current status
This project is under active development and has non-obvious usage requirements; drastic changes are expected in the near future.

Currently implemented features include:
- [x] Parallel chunks download
- [x] Handling chunk reassignments
- [ ] Ethereum query execution — partially implemented (supports `transactions` and `logs` querying, but not `traces` and `stateDiffs`)
- [ ] Substrate query execution
- [ ] Weighting queries
- [x] HTTP transport
- [x] P2P transport
- [ ] Sending logs
- [ ] Checking gateway allocations

### `query-performance` branch
This is an experimental branch that should not be merged into `master` yet.

To run benchmarks:\
`cargo bench`\
See also: [criterion.rs docs](https://bheisler.github.io/criterion.rs/book/user_guide/command_line_options.html#baselines).

To perf a benchmark:\
`perf record cargo bench --bench=query -- include_all_blocks --profile-time 30`

To build a flamegraph:\
`cargo flamegraph --bench query -- --bench include_all_blocks --profile-time 30`

To run tests and see execution plans:\
`RUST_LOG=debug,sqlparser::parser=info QUERY=include_all_blocks cargo test --release test_query -- --nocapture`