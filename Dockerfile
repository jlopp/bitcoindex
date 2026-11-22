# syntax=docker/dockerfile:1.7

# ---------------------------------------------------------------------------
# Builder — compiles the `bidx` binary.
#
# RocksDB's build needs a full C/C++ toolchain and libclang (bindgen). We also
# bake in the two workarounds the repo documents for modern GCC/clang:
#   * BINDGEN_EXTRA_CLANG_ARGS points bindgen at .build-shim/include (gcc
#     builtins) so `stdbool.h` resolves.
#   * CXXFLAGS=-include cstdint works around rocksdb 8.x's reliance on
#     transitive <cstdint> includes that GCC 15 no longer provides.
# ---------------------------------------------------------------------------
FROM rust:1-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
        clang \
        libclang-dev \
        llvm-dev \
        cmake \
        pkg-config \
        libzmq3-dev \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Warm the dependency cache: copy manifests first, build a stub, then copy the
# real sources. Keeps rebuilds fast when only crate code changes.
COPY Cargo.toml Cargo.lock ./
COPY crates/bidx-core/Cargo.toml    crates/bidx-core/
COPY crates/bidx-parser/Cargo.toml  crates/bidx-parser/
COPY crates/bidx-store/Cargo.toml   crates/bidx-store/
COPY crates/bidx-live/Cargo.toml    crates/bidx-live/
COPY crates/bidx-loader/Cargo.toml  crates/bidx-loader/
COPY crates/bidx-serve/Cargo.toml   crates/bidx-serve/
COPY crates/bidx-cli/Cargo.toml     crates/bidx-cli/
RUN set -eux; \
    for c in bidx-core bidx-parser bidx-store bidx-live bidx-loader bidx-serve bidx-cli; do \
        mkdir -p "crates/$c/src"; \
        [ -f "crates/$c/src/lib.rs" ] || echo '' > "crates/$c/src/lib.rs"; \
    done; \
    echo 'fn main() {}' > crates/bidx-cli/src/main.rs; \
    cargo build --release -p bidx-cli || true

# Real sources + the bindgen shim, then the actual build.
COPY .build-shim ./.build-shim
COPY crates ./crates
# The stub build above recorded fingerprints for the placeholder sources.
# cargo's freshness check is mtime-based, and `COPY` preserves the mtimes from
# the build context — which for a fresh clone are *older* than the stubs just
# written. Without correcting that, cargo believes the real sources are
# unchanged and links the empty stubs, so `bidx-store` fails to resolve
# `bidx_core::Hash32` et al. Bump every source mtime past the stubs so cargo
# recompiles our crates; third-party deps stay cached.
RUN set -eux; \
    find crates -name '*.rs' -exec touch {} +; \
    export BINDGEN_EXTRA_CLANG_ARGS="-I/src/.build-shim/include"; \
    export CXXFLAGS="-include cstdint"; \
    cargo build --release --locked -p bidx-cli; \
    cp target/release/bidx /usr/local/bin/bidx

# ---------------------------------------------------------------------------
# Runtime — slim image with the binary plus the ClickHouse client used by
# `bidx init-db` / `bidx load` (the loader shells out to `clickhouse-client`).
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        libzmq5 \
        libstdc++6 \
        curl \
        gnupg \
    && mkdir -p /etc/apt/keyrings \
    # The deb repo is signed by a subkey of the key published at
    # packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key. The legacy
    # /clickhouse/clickhouse.pub path now returns 403, so use the rpm key
    # path (it verifies the deb `InRelease` signature).
    && curl -fsSL https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key \
         -o /etc/apt/keyrings/clickhouse.asc \
    && echo "deb [signed-by=/etc/apt/keyrings/clickhouse.asc] https://packages.clickhouse.com/deb stable main" \
         > /etc/apt/sources.list.d/clickhouse.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends clickhouse-client \
    && rm -rf /var/lib/apt/lists/*

# Non-root runtime user; data dirs are the operator's mount points.
RUN useradd --system --create-home --uid 10001 bidx \
    && mkdir -p /data/utxo /data/out /data/blocks \
    && chown -R bidx:bidx /data

COPY --from=builder /usr/local/bin/bidx /usr/local/bin/bidx

USER bidx
WORKDIR /home/bidx

# The serving API (bidx serve) and metrics endpoints bind here.
EXPOSE 8432 9101

ENTRYPOINT ["/usr/local/bin/bidx"]
CMD ["--help"]
