# See https://www.lpalmieri.com/posts/fast-rust-docker-builds/#cargo-chef for explanation
FROM --platform=$BUILDPLATFORM lukemathwalker/cargo-chef:0.1.66-rust-1.78-slim-bookworm AS chef
WORKDIR /app


FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json


FROM chef AS builder
RUN apt-get update && apt-get install protobuf-compiler pkg-config libssl-dev libsqlite3-dev build-essential  -y

COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=ssh cargo chef cook --release --recipe-path recipe.json

COPY . .
RUN --mount=type=ssh cargo build --release


FROM chef AS worker
RUN apt-get update && apt-get install -y net-tools libsqlite3-dev
COPY --from=builder /app/target/release/worker /app/worker

ENV P2P_LISTEN_ADDRS="/ip4/0.0.0.0/udp/12345/quic-v1"
RUN echo "PORT=\$(echo \$P2P_LISTEN_ADDRS | cut -d / -f 5); netstat -an | grep \$PORT > /dev/null" > ./healthcheck.sh && \
    chmod +x ./healthcheck.sh
HEALTHCHECK --interval=5s CMD ./healthcheck.sh

ENTRYPOINT ["/app/worker"]
