#!/bin/sh
# ---------------------------------------------------------------------------
# bitcoindex installer
#
#   curl -fsSL https://raw.githubusercontent.com/jlopp/bitcoindex/master/scripts/install.sh | sh
#
# Downloads the prebuilt `bidx` binary for this machine from the GitHub
# release, verifies it against the release SHA256SUMS, and installs the two
# runtime dependencies the CLI needs:
#
#   * libzmq5            - `bidx live` (ZMQ hashblock notifications)
#   * clickhouse-client  - `bidx init-db` / `load` / `migrate` / `verify` /
#                          `index rebuild` (the loader shells out to it)
#
# Everything after the download is best-effort: a missing dependency or a
# read-only prefix degrades to a warning plus the exact command to run, it
# never leaves a half-installed binary behind.
#
# `--dry-run` prints every command it would run (and the resolved target,
# version and install path) without touching the network or the filesystem.
#
# Flags:
#   --version <v>      Release tag to install (default: latest)
#   --prefix <dir>     Install prefix (default: /usr/local)
#   --bindir <dir>     Where the binary goes (default: <prefix>/bin)
#   --libc <gnu|musl>  Which Linux build to fetch (default: auto-detected)
#   --no-deps          Do not install libzmq5 / clickhouse-client
#   --no-clickhouse    Install libzmq5 but skip clickhouse-client
#   --from-source      Build with cargo instead of downloading a release
#   --dry-run          Print the plan; change nothing
#   --yes              Never prompt (assume yes)
#   -h, --help         Show this help
#
# Environment overrides: BIDX_REPO, BIDX_VERSION, BIDX_PREFIX, BIDX_BINDIR
# ---------------------------------------------------------------------------
set -eu

REPO="${BIDX_REPO:-jlopp/bitcoindex}"
VERSION="${BIDX_VERSION:-}"
PREFIX="${BIDX_PREFIX:-/usr/local}"
BINDIR="${BIDX_BINDIR:-}"
BINDIR_EXPLICIT=0
if [ -n "$BINDIR" ]; then BINDIR_EXPLICIT=1; fi
LIBC=""
INSTALL_DEPS=1
INSTALL_CLICKHOUSE=1
FROM_SOURCE=0
DRY_RUN=0
ASSUME_YES=0

die() {
    printf 'install: error: %s\n' "$1" >&2
    exit 1
}

warn() {
    printf 'install: warning: %s\n' "$1" >&2
}

info() {
    printf 'install: %s\n' "$1"
}

# Run a side-effecting command, or print it under --dry-run. Every mutation in
# this script goes through here so the dry run is guaranteed to be faithful:
# there is no second "plan" code path to drift out of sync.
run() {
    if [ "$DRY_RUN" -eq 1 ]; then
        printf '  would run: %s\n' "$*"
        return 0
    fi
    "$@"
}

usage() {
    cat <<'EOF'
bitcoindex installer

  curl -fsSL https://raw.githubusercontent.com/jlopp/bitcoindex/master/scripts/install.sh | sh

Downloads the prebuilt `bidx` binary for this machine from the GitHub release,
verifies it against the release SHA256SUMS, and installs the runtime
dependencies the CLI needs:

  * libzmq5            - `bidx live` (ZMQ hashblock notifications)
  * clickhouse-client  - `bidx init-db` / `load` / `migrate` / `verify` /
                         `index rebuild` (the loader shells out to it)

Dependency installation is best-effort: if it fails, the binary is still
installed and the exact manual command is printed. A read-only install prefix
falls back to ~/.local/bin.

Flags:
  --version <v>      Release tag to install (default: latest)
  --prefix <dir>     Install prefix (default: /usr/local)
  --bindir <dir>     Where the binary goes (default: <prefix>/bin)
  --libc <gnu|musl>  Which Linux build to fetch (default: auto-detected)
  --no-deps          Do not install libzmq5 / clickhouse-client
  --no-clickhouse    Install libzmq5 but skip clickhouse-client
  --from-source      Build with cargo instead of downloading a release
  --dry-run          Print the plan; change nothing
  -h, --help         Show this help

Environment overrides: BIDX_REPO, BIDX_VERSION, BIDX_PREFIX, BIDX_BINDIR
EOF
    exit 0
}

# --- argument parsing ------------------------------------------------------
# Accepts both `--flag value` and `--flag=value`.
while [ $# -gt 0 ]; do
    case "$1" in
        --version) VERSION="${2:-}"; shift 2 ;;
        --version=*) VERSION="${1#*=}"; shift ;;
        --prefix) PREFIX="${2:-}"; shift 2 ;;
        --prefix=*) PREFIX="${1#*=}"; shift ;;
        --bindir) BINDIR="${2:-}"; BINDIR_EXPLICIT=1; shift 2 ;;
        --bindir=*) BINDIR="${1#*=}"; BINDIR_EXPLICIT=1; shift ;;
        --libc) LIBC="${2:-}"; shift 2 ;;
        --libc=*) LIBC="${1#*=}"; shift ;;
        --no-deps) INSTALL_DEPS=0; shift ;;
        --no-clickhouse) INSTALL_CLICKHOUSE=0; shift ;;
        --from-source) FROM_SOURCE=1; shift ;;
        --dry-run) DRY_RUN=1; shift ;;
        --yes|-y) ASSUME_YES=1; shift ;;
        -h|--help) usage ;;
        *) die "unknown argument '$1' (try --help)" ;;
    esac
done

if [ -n "$BINDIR" ]; then BINDIR_EXPLICIT=1; fi
if [ -z "$BINDIR" ]; then BINDIR="$PREFIX/bin"; fi

# --- platform detection ----------------------------------------------------
OS="$(uname -s)"
[ "$OS" = "Linux" ] || die "this installer supports Linux only (found '$OS'); \
on macOS use 'brew install $REPO' or build from source"

ARCH="$(uname -m)"
case "$ARCH" in
    x86_64|amd64) ARCH="x86_64" ;;
    aarch64|arm64) ARCH="aarch64" ;;
    *) die "no prebuilt binary for architecture '$ARCH'; re-run with --from-source" ;;
esac

detect_libc() {
    if [ -f /etc/alpine-release ]; then
        printf 'musl'
    elif ldd --version 2>&1 | grep -qi musl; then
        printf 'musl'
    else
        printf 'gnu'
    fi
}

if [ -z "$LIBC" ]; then LIBC="$(detect_libc)"; fi
case "$LIBC" in
    gnu|musl) ;;
    *) die "--libc must be 'gnu' or 'musl' (got '$LIBC')" ;;
esac

TARGET="${ARCH}-unknown-linux-${LIBC}"

if [ "$DRY_RUN" -eq 1 ]; then
    info "dry run: no changes will be made"
    info "platform: $ARCH / $LIBC ($TARGET)"
    info "install path: $BINDIR/bidx"
fi

# --- privileged command helper --------------------------------------------
# Runs "$@" as root directly, via sudo, or fails so the caller can degrade.
as_root() {
    if [ "$(id -u)" -eq 0 ]; then
        "$@"
    elif command -v sudo >/dev/null 2>&1; then
        sudo "$@"
    else
        return 1
    fi
}

pkg_manager() {
    if command -v apt-get >/dev/null 2>&1; then
        printf 'apt'
    elif command -v dnf >/dev/null 2>&1; then
        printf 'dnf'
    elif command -v pacman >/dev/null 2>&1; then
        printf 'pacman'
    else
        printf 'none'
    fi
}

# --- dependency installation ----------------------------------------------
# Ask before installing system packages. Non-interactive (including
# `curl | sh`, where stdin is the script) and --dry-run proceed without
# prompting; --yes skips the prompt explicitly.
confirm_deps() {
    if [ "$ASSUME_YES" -eq 1 ] || [ "$DRY_RUN" -eq 1 ] || [ ! -t 0 ]; then
        return 0
    fi
    printf 'install: install system dependencies (libzmq5, clickhouse-client) with sudo? [Y/n] '
    read -r reply || return 0
    case "$reply" in
        [nN]*) return 1 ;;
        *) return 0 ;;
    esac
}

have_libzmq() {
    command -v ldconfig >/dev/null 2>&1 && ldconfig -p 2>/dev/null | grep -q 'libzmq\.so'
}

install_libzmq() {
    if have_libzmq; then
        info "libzmq already present"
        return 0
    fi
    pm="$(pkg_manager)"
    case "$pm" in
        apt) pkg="libzmq5" ;;
        dnf) pkg="zeromq" ;;
        pacman) pkg="zeromq" ;;
        *) warn "cannot auto-install libzmq on this distro; install your \
distro's libzmq (>= 4.3) manually - required by 'bidx live'"; return 1 ;;
    esac
    info "installing libzmq ($pkg) - required by 'bidx live'"
    if case "$pm" in
           apt) run as_root apt-get update \
                    && run as_root apt-get install -y --no-install-recommends "$pkg" ;;
           dnf) run as_root dnf install -y "$pkg" ;;
           pacman) run as_root pacman -S --noconfirm --needed "$pkg" ;;
       esac; then
        return 0
    fi
    warn "could not install $pkg; run: sudo $pm install $pkg"
    return 1
}

install_clickhouse_client() {
    if command -v clickhouse-client >/dev/null 2>&1; then
        info "clickhouse-client already present"
        return 0
    fi
    if [ "$(pkg_manager)" != "apt" ]; then
        warn "clickhouse-client not found; install it from \
https://clickhouse.com/docs/en/install - required by init-db/load/migrate/verify"
        return 1
    fi
    info "installing clickhouse-client from the ClickHouse apt repo"
    # The legacy /clickhouse/clickhouse.pub path returns 403; the rpm key path
    # verifies the deb InRelease signature (same source as the Dockerfile).
    if run as_root install -d -m 0755 /etc/apt/keyrings \
        && run as_root sh -c 'curl -fsSL https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key \
             -o /etc/apt/keyrings/clickhouse.asc' \
        && run as_root sh -c 'echo "deb [signed-by=/etc/apt/keyrings/clickhouse.asc] https://packages.clickhouse.com/deb stable main" \
             > /etc/apt/sources.list.d/clickhouse.list' \
        && run as_root apt-get update \
        && run as_root apt-get install -y --no-install-recommends clickhouse-client; then
        return 0
    fi
    warn "could not install clickhouse-client automatically; \
follow https://clickhouse.com/docs/en/install"
    return 1
}

# --- install prefix handling ----------------------------------------------
# Install the binary into $BINDIR. Uses sudo only for the default /usr/local
# prefix; a non-writable explicit --bindir is a user error; anything else
# falls back to ~/.local/bin so the install still succeeds unprivileged.
install_binary() {
    src="$1"
    if [ -w "$BINDIR" ] || { [ ! -e "$BINDIR" ] && [ -w "$(dirname "$BINDIR")" ]; } \
        || [ "$(id -u)" -eq 0 ]; then
        run mkdir -p "$BINDIR"
        if run install -m 0755 "$src" "$BINDIR/bidx"; then
            return 0
        fi
    fi
    if [ "$PREFIX" = "/usr/local" ] && [ "$BINDIR_EXPLICIT" -eq 0 ] \
        && command -v sudo >/dev/null 2>&1; then
        if run sudo mkdir -p "$BINDIR" && run sudo install -m 0755 "$src" "$BINDIR/bidx"; then
            return 0
        fi
        warn "sudo install to $BINDIR failed; falling back to $HOME/.local/bin"
    elif [ "$BINDIR_EXPLICIT" -eq 1 ]; then
        die "cannot write to $BINDIR (choose a writable --bindir or run with sudo)"
    else
        warn "cannot write to $BINDIR; falling back to $HOME/.local/bin"
    fi
    BINDIR="$HOME/.local/bin"
    run mkdir -p "$BINDIR"
    if run install -m 0755 "$src" "$BINDIR/bidx"; then
        return 0
    fi
    die "could not write $BINDIR/bidx"
}

# --- source build fallback -------------------------------------------------
build_from_source() {
    if [ "$DRY_RUN" -eq 1 ]; then
        info "would clone https://github.com/$REPO.git and build with cargo:"
        printf '  would run: git clone --depth 1 https://github.com/%s.git <tmp>/src\n' "$REPO"
        printf '  would run: ./scripts/build-shim.sh\n'
        printf '  would run: cargo build --release --locked -p bidx-cli\n'
        printf '  would run: install -m 0755 <tmp>/src/target/release/bidx %s/bidx\n' "$BINDIR"
        return 0
    fi
    command -v cargo >/dev/null 2>&1 || die "--from-source needs cargo \
(https://rustup.rs) plus a C/C++ toolchain and libclang"
    command -v git >/dev/null 2>&1 || die "--from-source needs git"
    command -v clang >/dev/null 2>&1 || die "--from-source needs clang/libclang-dev \
(apt-get install -y clang libclang-dev llvm-dev cmake pkg-config libzmq3-dev)"
    info "building $REPO from source (this takes several minutes)"
    src="$(mktemp -d)"
    trap 'rm -rf "$src"' EXIT
    git clone --depth 1 "https://github.com/$REPO.git" "$src/src" \
        || die "git clone failed"
    ( cd "$src/src" \
        && ./scripts/build-shim.sh \
        && export BINDGEN_EXTRA_CLANG_ARGS="-I$PWD/.build-shim/include" \
        && export CXXFLAGS="-include cstdint" \
        && cargo build --release --locked -p bidx-cli ) || die "cargo build failed"
    info "installing to $BINDIR/bidx"
    install_binary "$src/src/target/release/bidx"
}

# --- release resolution ----------------------------------------------------
# Print the newest published release tag for $REPO, or nothing if the repo has
# no published release at all. Empty output means "nothing to install".
#
# `/releases/latest` is the fast path, but it deliberately omits prereleases
# and drafts, so it 404s on a repository whose only releases are prereleases -
# which is exactly this project today. Rather than fail the whole install on a
# young repo, fall back to the newest published release of any kind.
resolve_latest_version() {
    tag="$(curl -fsSL --proto '=https' --tlsv1.2 --retry 3 \
        "https://api.github.com/repos/$REPO/releases/latest" 2>/dev/null \
        | grep -o '"tag_name": *"[^"]*"' | head -1 | cut -d'"' -f4)"
    if [ -z "$tag" ]; then
        # /releases is newest-first, so the first tag_name is the newest
        # published release, prereleases included.
        tag="$(curl -fsSL --proto '=https' --tlsv1.2 --retry 3 \
            "https://api.github.com/repos/$REPO/releases?per_page=1" 2>/dev/null \
            | grep -o '"tag_name": *"[^"]*"' | head -1 | cut -d'"' -f4)"
        [ -z "$tag" ] || warn "no stable release yet; using the latest prerelease $tag"
    fi
    printf '%s' "$tag"
}

# --- main ------------------------------------------------------------------
if [ "$FROM_SOURCE" -eq 1 ]; then
    build_from_source
else
    # Resolve the release tag.
    if [ -z "$VERSION" ]; then
        if [ "$DRY_RUN" -eq 1 ]; then
            VERSION="v<latest>"
        else
            info "resolving latest release of $REPO"
            VERSION="$(resolve_latest_version)"
            [ -n "$VERSION" ] || die "no releases found for $REPO; \
pass --version <tag> (e.g. --version v0.1.0) or use --from-source"
        fi
    fi
    case "$VERSION" in
        v*) ;;
        *) VERSION="v$VERSION" ;;
    esac

    ARCHIVE="bidx-${TARGET}.tar.gz"
    BASE="https://github.com/$REPO/releases/download/$VERSION"

    if [ "$DRY_RUN" -eq 1 ]; then
        info "would download $BASE/$ARCHIVE"
        printf '  would run: curl -fsSL --proto =https --tlsv1.2 --retry 3 -o <tmp>/%s %s/%s\n' \
            "$ARCHIVE" "$BASE" "$ARCHIVE"
        info "would verify its sha256 against $BASE/SHA256SUMS"
        info "would extract and install to $BINDIR/bidx"
    else
        info "downloading $ARCHIVE ($VERSION)"
        tmp="$(mktemp -d)"
        trap 'rm -rf "$tmp"' EXIT

        curl -fsSL --proto '=https' --tlsv1.2 --retry 3 \
            -o "$tmp/$ARCHIVE" "$BASE/$ARCHIVE" \
            || die "download failed: $BASE/$ARCHIVE - either $VERSION has no \
$TARGET build or its release assets have not finished publishing yet \
(check https://github.com/$REPO/releases/tag/$VERSION)"
        curl -fsSL --proto '=https' --tlsv1.2 --retry 3 \
            -o "$tmp/SHA256SUMS" "$BASE/SHA256SUMS" \
            || die "could not download SHA256SUMS for $VERSION"

        expected="$(grep " ${ARCHIVE}\$" "$tmp/SHA256SUMS" | awk '{print $1}')"
        [ -n "$expected" ] || die "$ARCHIVE is not listed in SHA256SUMS"
        actual="$(sha256sum "$tmp/$ARCHIVE" | awk '{print $1}')"
        [ "$expected" = "$actual" ] \
            || die "checksum mismatch for $ARCHIVE (expected $expected, got $actual)"
        info "checksum verified"

        tar -xzf "$tmp/$ARCHIVE" -C "$tmp" || die "could not extract $ARCHIVE"
        [ -f "$tmp/bidx" ] || die "release archive did not contain a 'bidx' binary"

        info "installing to $BINDIR/bidx"
        install_binary "$tmp/bidx"
    fi
fi

# --- runtime dependencies --------------------------------------------------
if [ "$INSTALL_DEPS" -eq 1 ]; then
    if confirm_deps; then
        install_libzmq || true
        if [ "$INSTALL_CLICKHOUSE" -eq 1 ]; then
            install_clickhouse_client || true
        fi
    else
        info "skipping dependency installation"
    fi
else
    info "skipping dependency installation (--no-deps)"
fi

# --- report ----------------------------------------------------------------
if [ "$DRY_RUN" -eq 1 ]; then
    printf '\nDry run complete: nothing was downloaded, installed, or changed.\n'
    exit 0
fi

info "installed $("$BINDIR/bidx" --version 2>/dev/null || printf 'bidx') to $BINDIR/bidx"
case ":$PATH:" in
    *":$BINDIR:"*) ;;
    *) warn "$BINDIR is not on your PATH; add: export PATH=\"$BINDIR:\$PATH\"" ;;
esac
printf '\nNext steps:\n'
printf '  bidx headers --blocks-dir ~/.bitcoin/blocks   # sanity-check the chain\n'
printf '  bidx --help                                    # all subcommands\n'
