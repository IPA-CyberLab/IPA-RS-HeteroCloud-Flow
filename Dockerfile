# syntax=docker/dockerfile:1.7
FROM rust:1.96.1-bookworm AS builder

WORKDIR /source
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates ./crates
COPY migrations ./migrations
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/source/target \
    cargo build --locked --release \
      --bin flow-api \
      --bin flow-matchmaker \
      --bin flow-signaling && \
    install -D -m 0755 target/release/flow-api /output/flow-api && \
    install -D -m 0755 target/release/flow-matchmaker /output/flow-matchmaker && \
    install -D -m 0755 target/release/flow-signaling /output/flow-signaling

FROM debian:bookworm-slim AS runtime
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*
COPY --from=builder /output/flow-api /usr/local/bin/flow-api
COPY --from=builder /output/flow-matchmaker /usr/local/bin/flow-matchmaker
COPY --from=builder /output/flow-signaling /usr/local/bin/flow-signaling
USER 65532:65532
EXPOSE 8080 8081 8082
ENTRYPOINT ["/usr/local/bin/flow-api"]

