#!/usr/bin/env bash
# Source this before any cargo build/test in this workspace:
#
#   source scripts/dev-env.sh
#   cargo test --release --workspace
#
# It exports the three workarounds RocksDB's build needs on a modern distro,
# so you do not have to remember them (or the right libclang path) each time:
#
#   * BINDGEN_EXTRA_CLANG_ARGS -> the .build-shim/include directory, so bindgen
#     can find stdbool.h et al. libclang's resource dir ships without them and
#     never falls back to gcc's copy. Generated here by scripts/build-shim.sh.
#   * LIBCLANG_PATH -> where libclang actually lives on this machine, which is
#     NOT always /usr/lib/llvm-<version>/lib (Debian/Ubuntu put it in the
#     multiarch lib dir).
#   * CXXFLAGS -> `-include cstdint`, for rocksdb 8.x's reliance on a
#     transitive <cstdint> that GCC 15 no longer provides.
#
# Idempotent: safe to source repeatedly. Values you set yourself are kept.

# Must be sourced, not executed, so the exports reach your shell.
if [ "${BASH_SOURCE[0]}" = "${0}" ]; then
    echo "dev-env: source this file, do not execute it:" >&2
    echo "  source scripts/dev-env.sh" >&2
    exit 1
fi

# Resolve the repo root from this file's location, so it works from any cwd.
_dev_env_root="$(
    cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd
)"

# --- 1. the bindgen header shim --------------------------------------------
if [ ! -f "$_dev_env_root/.build-shim/include/stdbool.h" ]; then
    echo "dev-env: generating the bindgen header shim" >&2
    if ! "$_dev_env_root/scripts/build-shim.sh" >/dev/null; then
        echo "dev-env: build-shim.sh failed (is gcc installed?)" >&2
        return 1
    fi
fi

# bindgen resolves this relative to each crate's build directory, not the
# workspace root, so it must be absolute.
export BINDGEN_EXTRA_CLANG_ARGS="-I$_dev_env_root/.build-shim/include"

# --- 2. libclang ------------------------------------------------------------
# Accept an already-valid LIBCLANG_PATH rather than second-guessing the user.
libclang_ok() {
    [ -n "${1:-}" ] || return 1
    # bindgen looks for libclang.so, libclang-*.so, libclang.so.*, libclang-*.so.*
    [ -n "$(find "$1" -maxdepth 1 -name 'libclang*.so*' 2>/dev/null | head -1)" ]
}

if ! libclang_ok "${LIBCLANG_PATH:-}"; then
    _libclang_dir=""
    # Prefer ldconfig (authoritative for what the loader will find), then fall
    # back to the conventional locations for distros that lack it.
    if command -v ldconfig >/dev/null 2>&1; then
        _libclang_dir="$(
            ldconfig -p 2>/dev/null \
                | awk '/libclang(-[0-9.]+)?\.so/ { print $NF; exit }' \
                | xargs -r dirname 2>/dev/null
        )"
    fi
    if ! libclang_ok "$_libclang_dir"; then
        _libclang_dir=""
        for _d in /usr/lib/llvm-*/lib \
                  /usr/lib/"$(uname -m)"-linux-gnu \
                  /usr/lib/aarch64-linux-gnu \
                  /usr/lib64 /usr/local/lib; do
            if libclang_ok "$_d"; then
                _libclang_dir="$_d"
                break
            fi
        done
    fi
    if libclang_ok "$_libclang_dir"; then
        export LIBCLANG_PATH="$_libclang_dir"
    else
        echo "dev-env: could not find libclang; install it, e.g.:" >&2
        echo "  sudo apt-get install -y clang libclang-dev llvm-dev cmake pkg-config libzmq3-dev" >&2
        echo "  then: export LIBCLANG_PATH=/path/to/dir/containing/libclang.so" >&2
        return 1
    fi
fi

# --- 3. rocksdb's transitive <cstdint> -------------------------------------
# Append rather than clobber, in case the caller passed their own flags.
case "${CXXFLAGS:-}" in
    *"-include cstdint"*) ;;
    *) export CXXFLAGS="${CXXFLAGS:+$CXXFLAGS }-include cstdint" ;;
esac

echo "dev-env: LIBCLANG_PATH=$LIBCLANG_PATH" >&2
echo "dev-env: BINDGEN_EXTRA_CLANG_ARGS=$BINDGEN_EXTRA_CLANG_ARGS" >&2
echo "dev-env: CXXFLAGS=$CXXFLAGS" >&2
