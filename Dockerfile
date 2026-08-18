# syntax=docker/dockerfile:1.7

FROM rust:1.97-bookworm AS builder

ARG TARGETARCH
WORKDIR /workspace

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY xtask ./xtask

RUN --mount=type=cache,id=heterarchy-cargo-registry-${TARGETARCH},target=/usr/local/cargo/registry \
    --mount=type=cache,id=heterarchy-cargo-target-${TARGETARCH},target=/workspace/target \
    cargo build --locked --release -p agent-broker-runtime --bin agentbrokerd && \
    cp /workspace/target/release/agentbrokerd /tmp/agentbrokerd

FROM debian:bookworm-slim AS runtime

ARG VERSION=dev
ARG VCS_REF=unknown

LABEL org.opencontainers.image.title="Agent Broker" \
      org.opencontainers.image.description="Rust provider-independent work broker runtime" \
      org.opencontainers.image.source="https://github.com/limhaneul12/heterarchy-broker" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${VCS_REF}" \
      org.opencontainers.image.licenses="MIT OR Apache-2.0"

COPY --from=builder --chown=10001:10001 /tmp/agentbrokerd /usr/local/bin/agentbrokerd

RUN mkdir -p /var/lib/agent-broker && \
    chown 10001:10001 /var/lib/agent-broker

USER 10001:10001
WORKDIR /var/lib/agent-broker

EXPOSE 8811/tcp 8812/tcp 18811/tcp
VOLUME ["/var/lib/agent-broker"]

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/agentbrokerd", "health", "--host", "127.0.0.1", "--port", "8811", "--timeout-ms", "2000"]

ENTRYPOINT ["/usr/local/bin/agentbrokerd"]
CMD ["serve", "--host", "0.0.0.0", "--port", "8811", "--container-bridge-bind", "--state-path", "/var/lib/agent-broker/broker-state.json"]
