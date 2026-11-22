#!/usr/bin/env bash
# Create the bindgen header shim at $1 (default: .build-shim/include).
#
# Why this exists: librocksdb-sys runs bindgen, which parses RocksDB's public C
# headers through libclang. libclang resolves the C standard headers
# (`stdbool.h`, `stddef.h`, ...) from its builtin resource directory, and the
# distro `libclang-dev` packages ship that directory *without* them. libclang
# never falls back to gcc's own copy, so the parse fails with
#
#     'stdbool.h' file not found
#
# The fix is to put gcc's copies on bindgen's include path via
# BINDGEN_EXTRA_CLANG_ARGS. That is all this script does: it copies the six
# headers gcc provides but libclang's resource dir lacks.
#
# The directory is machine-specific and gitignored (`.build-shim/`), so it is
# generated — never committed and never COPYed from the build context. The
# Dockerfile and the release workflow call this same script.

set -euo pipefail

out="${1:-.build-shim/include}"
mkdir -p "$out"

gcc_include="$(gcc -print-file-name=include)"
if [ ! -d "$gcc_include" ]; then
    echo "build-shim: gcc -print-file-name=include gave no directory: $gcc_include" >&2
    exit 1
fi

# Copied unconditionally: bindgen has no way to fall back to gcc's copy.
for header in stddef.h stdarg.h stdbool.h stdint.h float.h limits.h; do
    if [ ! -f "$gcc_include/$header" ]; then
        echo "build-shim: $gcc_include/$header is missing; is gcc installed?" >&2
        exit 1
    fi
    cp "$gcc_include/$header" "$out/"
done

# `stdint.h` is special. GCC 15 rewrote it to `#include_next <stdint.h>` into
# the libc header, so a copy of gcc's file alone resolves nothing — clang's
# `include_next` lands back in its own (empty) resource dir. Prefer any newer
# gcc's version when one is installed; otherwise keep the copy above, which is
# self-contained on gcc <= 14 (it includes "stdint-gcc.h" from the same dir).
for gcc in gcc-15 gcc-14 gcc-13 gcc-12; do
    if command -v "$gcc" > /dev/null 2>&1; then
        candidate="$("$gcc" -print-file-name=include)/stdint.h"
        if [ -f "$candidate" ]; then
            cp "$candidate" "$out/stdint.h"
            break
        fi
    fi
done

echo "build-shim: wrote $(ls "$out" | tr '\n' ' ')to $out"
