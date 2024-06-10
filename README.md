## Subsquid worker
This is a Rust implementation of the Worker. The previous (Python) version can be found [here](https://github.com/subsquid/archive.py/tree/master).
A worker is a service that downloads assigned data chunks from persistent storage (currently S3) and processes incoming data queries that reference those data chunks. It can be used in two modes:
- Centralized. In this setup, the assignment is received from a centralized [router](https://github.com/subsquid/archive-router/tree/main/crates/router) that is aware of each worker and relies on it (almost) always being available. In this case, the communication between the worker and the router happens directly through HTTP requests.
- Decentralized. In this setup, the assignment comes from a [scheduler](https://github.com/subsquid/archive-router/tree/b01d86aaf9fb5e14b16c3d24eb7419d413ce8b46/crates/network-scheduler) via a [P2P communication protocol](https://github.com/subsquid/subsquid-network/tree/main/transport). The chunks can be reassigned as the workers join and leave the network.

The full network architecture can be found [here](https://github.com/subsquid/subsquid-network-contracts/wiki/Network-architecture#panthalassa-testnet).

## Usage
You can find instructions for how to run your own worker [here](https://docs.subsquid.io/subsquid-network/participate/worker/).