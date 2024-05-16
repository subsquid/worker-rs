#!/usr/bin/env bash

set -e

WORKER_VERSION=$(cargo metadata --no-deps --format-version=1 | jq '.packages[].version' -r)
cargo fetch
cargo vendor
BUILD=$(mktemp -d)/subsquid-worker
mkdir -pv "$BUILD"
mkdir "$BUILD/.cargo"
mkdir "$BUILD/vendor"
cp -rv Cargo.toml Cargo.lock src benches "$BUILD"
cp .env.testnet "$BUILD/.env"
cp .vendor-config.toml "$BUILD/.cargo/config.toml"
cp -rv vendor/{contract-client,subsquid-*} "$BUILD/vendor"
tar -C "$BUILD/.." --owner=root --group=root -czf "worker-$WORKER_VERSION.tar.gz" subsquid-worker
rm -r "$BUILD"