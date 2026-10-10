# syntax=docker/dockerfile:1.7

# ---- build ----------------------------------------------------------------
# Keep in step with `rust-version` in Cargo.toml.
FROM rust:1.98-slim-trixie AS builder
WORKDIR /usr/src/lsm-rust

# Resolve and compile dependencies against a stub first, so source edits do not
# invalidate the dependency layer.
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src benches \
 && echo 'fn main() {}' > src/main.rs \
 && echo '' > src/lib.rs \
 && echo 'fn main() {}' > benches/storage.rs \
 && cargo build --release --locked --bins \
 && rm -rf src benches

COPY src ./src
COPY benches ./benches
# `--locked` fails the build rather than silently re-resolving dependencies.
RUN touch src/main.rs src/lib.rs \
 && cargo build --release --locked --bin lsm-rust \
 && strip target/release/lsm-rust

# ---- runtime --------------------------------------------------------------
# distroless/cc: glibc and libgcc only. No shell, no package manager, and a
# nonroot user baked in, so there is little for an attacker to work with.
FROM gcr.io/distroless/cc-debian13:nonroot

COPY --from=builder /usr/src/lsm-rust/target/release/lsm-rust /usr/local/bin/lsm-rust

# /data is the only writable path; mount a persistent volume here.
WORKDIR /data
VOLUME ["/data"]

# 6379 RESP, 9898 Prometheus /metrics + /healthz.
EXPOSE 6379 9898

# Inside a container the listener must bind all interfaces to be reachable;
# network exposure is then governed by the orchestrator (Service, NetworkPolicy,
# security groups). Set LSM_REQUIREPASS_FILE to require AUTH.
ENV LSM_ADDR=0.0.0.0:6379 \
    LSM_DATA_DIR=/data \
    LSM_METRICS_ADDR=0.0.0.0:9898

USER nonroot:nonroot

HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD ["/usr/local/bin/lsm-rust", "healthcheck"]

ENTRYPOINT ["/usr/local/bin/lsm-rust"]
CMD ["serve"]
