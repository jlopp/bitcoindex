#!/usr/bin/env bash
# Build environment for bitcoindex.
#
# Source this before any `cargo` invocation:
#
#     source scripts/perf/env.sh
#     cargo build --release -p bidx-cli
#
# Every setting here exists because of a concrete failure on a modern Linux
# toolchain; they are not cargo-culted. CI (ubuntu-latest) installs matching
# apt packages and does not need most of this.
#
#   LIBCLANG_PATH
#     librocksdb-sys runs bindgen, which dlopens libclang at runtime. Distro
#     packages install `libclang-<ver>.so.1` but not the bare `libclang.so`
#     that the `clang-sys` loader probes for first. Pointing at the versioned
#     lib directory makes the probe succeed without a `libclang-dev` install.
#
#   BINDGEN_EXTRA_CLANG_ARGS
#     bindgen parses RocksDB's public C headers and needs the C standard
#     headers on its include path. When libclang's builtin resource dir
#     doesn't contain them (common with a versioned LLVM install), the repo's
#     `.build-shim/include` copies of gcc's builtins fill the gap.
#
#   CXXFLAGS=-include cstdint
#     RocksDB 8.10's `db/blob/blob_file_meta.h` uses `uint64_t` without
#     including <cstdint>, relying on a transitive include that GCC <= 14
#     happened to provide. GCC 15 tightened this and the file no longer
#     compiles. Force-including <cstdint> into every C++ TU is the minimal
#     fix. It must NOT go in CFLAGS: that would leak into assembly sources
#     (zstd's huf_decompress_amd64.S) and break them.
#
#   CARGO_TARGET_DIR
#     Unset so the build lands in the repo's own `target/`. Sandboxes often
#     export a throwaway cache dir, which makes incremental builds vanish.

set -a

# Locate libclang: prefer an explicit override, then the versioned distro
# layout, then llvm-config if present.
if [ -z "${LIBCLANG_PATH:-}" ]; then
    for candidate in /usr/lib/llvm-*/lib /usr/lib/llvm-*/lib64; do
        if compgen -G "${candidate}/libclang*.so*" > /dev/null; then
            LIBCLANG_PATH="${candidate}"
            break
        fi
    done
fi
export LIBCLANG_PATH

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
if [ -d "${REPO_ROOT}/.build-shim/include" ]; then
    export BINDGEN_EXTRA_CLANG_ARGS="-I${REPO_ROOT}/.build-shim/include"
fi

# Only if the compiler is GCC 15 or newer, where RocksDB 8.10 fails to build
# without it. Older compilers build fine and don't need the extra include.
_gcc_major="$(gcc -dumpversion 2>/dev/null | cut -d. -f1)"
if [ -n "${_gcc_major}" ] && [ "${_gcc_major}" -ge 15 ] 2>/dev/null; then
    export CXXFLAGS="${CXXFLAGS:-} -include cstdint"
fi
unset _gcc_major

unset CFLAGS
unset CARGO_TARGET_DIR

set +a
