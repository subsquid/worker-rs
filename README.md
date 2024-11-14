## SQD worker

This is a Rust implementation of the Worker. The previous (Python) version can be found [here](https://github.com/subsquid/archive.py/tree/master).

A worker is a service that downloads assigned data chunks from persistent storage (currently S3) and processes incoming data queries that reference those data chunks.

For details see [network RFC](https://github.com/subsquid/specs/tree/main/network-rfc) page.

## Usage

You can find instructions for how to run your own worker [here](https://docs.sqd.dev/subsquid-network/participate/worker/).