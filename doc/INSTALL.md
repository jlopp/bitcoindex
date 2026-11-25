# Installing bitcoindex

Every supported way to get the `bidx` binary onto a machine, what each one
installs, and what it still leaves you to do.

The authoritative sources are the code, not this file: the installer is
[`scripts/install.sh`](../scripts/install.sh), the package definition is
[`packaging/nfpm.yaml`](../packaging/nfpm.yaml), and the release pipeline that
produces every artifact is
[`.github/workflows/release.yml`](../.github/workflows/release.yml).

## Contents

- [Pick an option](#pick-an-option)
- [What you need before you start](#what-you-need-before-you-start)
- [1. Install script](#1-install-script)
- [2. Container image](#2-container-image)
- [3. Distro packages (.deb / .rpm / .apk)](#3-distro-packages-deb--rpm--apk)
- [4. Homebrew](#4-homebrew)
- [5. cargo install](#5-cargo-install)
- [6. Nix](#6-nix)
- [7. Build from source](#7-build-from-source)
- [Verifying a release](#verifying-a-release)
- [What you still need after installing](#what-you-still-need-after-installing)
- [Uninstalling](#uninstalling)
- [Troubleshooting](#troubleshooting)

---

## Pick an option

| # | Option | Command | Installs deps? | Needs root? | Platforms |
|---|--------|---------|:--------------:|:-----------:|-----------|
| 1 | Install script | `curl … \| sh` | yes (best-effort) | only for `/usr/local` | Linux x86_64 / aarch64 |
| 2 | Container | `docker run ghcr.io/jlopp/bitcoindex` | n/a | no | linux/amd64, linux/arm64 |
| 3 | Distro package | `apt install bitcoindex` | via package manager | yes | Debian/Ubuntu, Fedora/RHEL, Alpine |
| 4 | Homebrew | `brew install jlopp/tap/bitcoindex` | via Homebrew | no | Linux (Homebrew) |
| 5 | cargo | `cargo install --git …` | no | no | any with a C/C++ toolchain |
| 6 | Nix | `nix profile install` | via Nix | no | Linux |
| 7 | Source | `cargo build --release` | no | no | any with a C/C++ toolchain |

**In a hurry on Ubuntu?** Option 1, then option 3 once the apt repo is hosted.
**No Docker, no root, want it now?** Option 1 with `--bindir ~/.local/bin`.
**Already run containers?** Option 2 — nothing is installed on the host.
**Contributing?** Option 7, so you have the working tree.

The prebuilt artifacts are `bidx-<arch>-unknown-linux-<libc>.tar.gz` for
`x86_64`/`aarch64` × `gnu`/`musl`, attached to each `v*` release.

---

## What you need before you start

`bidx` is a CLI, but the *pipeline* it drives needs external services. Which
ones you need depends on what you are doing:

| Component | Needed for | Notes |
|-----------|-----------|-------|
| Bitcoin Core node (synced) | `headers`, `parse`, `live`, `verify` | `bidx` reads raw `blk*.dat` directly; no RPC in the hot path. |
| ClickHouse server | `init-db`, `load`, `migrate`, `serve`, `verify` | Compose stack ships one; `doc/deployment-ops.md` covers it. |
| `clickhouse-client` | `init-db`, `load`, `migrate`, `verify`, `index rebuild` | The loader **shells out** to it. Not packaged — see below. |
| `libzmq` (≥ 4.3) | `live` only | The prebuilt binaries link the system lib, not a vendored one. |

**`clickhouse-client` is deliberately not a package dependency.** It lives in
the ClickHouse apt/rpm repositories, not the distro ones, so declaring it would
make the package uninstallable on a clean system. Install it separately:

```bash
# Debian/Ubuntu
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://packages.clickhouse.com/rpm/lts/repodata/repomd.xml.key \
  -o /etc/apt/keyrings/clickhouse.asc
echo "deb [signed-by=/etc/apt/keyrings/clickhouse.asc] https://packages.clickhouse.com/deb stable main" \
  | sudo tee /etc/apt/sources.list.d/clickhouse.list
sudo apt-get update && sudo apt-get install -y clickhouse-client
```

The installer (option 1) does this for you on Debian/Ubuntu, and prints the
link on other distros. `bidx` reports a clear error if the client is missing
when you run a command that needs it.

---

## 1. Install script

```bash
curl -fsSL https://raw.githubusercontent.com/jlopp/bitcoindex/master/scripts/install.sh | sh
```

The script:

1. Detects the architecture (`x86_64`/`aarch64`) and libc (glibc/musl).
2. Resolves the release (`releases/latest`, or `--version`). If the repository
   has no stable release yet, it falls back to the newest published release of
   any kind — GitHub's `releases/latest` deliberately omits prereleases and
   drafts, so it 404s on a repository whose only releases are prereleases.
3. Downloads the matching tarball and **verifies its SHA-256** against the
   release `SHA256SUMS`, refusing to install on a mismatch.
4. Installs the binary — `/usr/local/bin` when writable, via `sudo` for the
   default prefix, falling back to `~/.local/bin`.
5. Installs `libzmq5` (or `zeromq`) and `clickhouse-client` on a best-effort
   basis. A failure warns and prints the manual command; it never rolls back
   the binary.

### Flags

| Flag | Default | Meaning |
|------|---------|---------|
| `--version <v>` | latest release | Tag to install; `0.1.0` and `v0.1.0` both work. |
| `--prefix <dir>` | `/usr/local` | Install prefix; the binary goes in `<prefix>/bin`. |
| `--bindir <dir>` | `<prefix>/bin` | Explicit destination. A non-writable explicit `--bindir` is an error, not a silent relocation. |
| `--libc <gnu\|musl>` | auto-detected | Override the libc detection. |
| `--no-deps` | off | Install nothing else; just the binary. |
| `--no-clickhouse` | off | Install `libzmq5` but skip `clickhouse-client`. |
| `--from-source` | off | Clone and `cargo build` instead of downloading. |
| `--dry-run` | off | Print the resolved plan and every command, change nothing. |
| `--yes`, `-y` | off | Do not prompt before installing system packages. |
| `-h`, `--help` | | Usage. |

Environment overrides: `BIDX_REPO`, `BIDX_VERSION`, `BIDX_PREFIX`,
`BIDX_BINDIR`.

### Examples

```bash
# Preview exactly what would happen, touching nothing:
curl -fsSL …/install.sh | sh -s -- --dry-run

# Pin a version and install without root:
curl -fsSL …/install.sh | sh -s -- --version v0.1.0 --bindir ~/.local/bin

# Binary only; you manage your own dependencies:
curl -fsSL …/install.sh | sh -s -- --no-deps

# No prebuilt for your distro/libc:
curl -fsSL …/install.sh | sh -s -- --from-source
```

### The dry run

`--dry-run` is faithful because every mutation in the script goes through a
single `run` wrapper: there is no second "plan" code path that could drift from
what the real run does. It prints the resolved platform, target triple, version
and install path, then each command it would execute. It makes no network
request and creates no files.

### Dependency prompt

If stdin is a terminal, the script asks before installing system packages. When
piped (`curl | sh`) or run non-interactively there is no prompt, and `--yes`
skips it explicitly. `--dry-run` never prompts.

---

## 2. Container image

The zero-install option — nothing is added to the host.

```bash
docker run --rm ghcr.io/jlopp/bitcoindex:latest --help
```

Tags: `latest` and `:1`, `:1.2`, `:1.2.3` on a release, plus `:edge` and
`:sha-<short>` from `master`. Multi-arch (`linux/amd64`, `linux/arm64`).

The image carries the `bidx` binary **and** `clickhouse-client`, so the loader
commands work out of the box. It exposes the serving API on `8432` and metrics
on `9101`:

```bash
docker run --rm -p 8432:8432 -p 9101:9101 \
  -v "$PWD/bidx.toml:/etc/bidx/bidx.toml:ro" \
  ghcr.io/jlopp/bitcoindex:latest serve --config /etc/bidx/bidx.toml
```

For the full stack (ClickHouse + `bidx serve`), use the Compose file — see
[deployment-ops.md](deployment-ops.md#the-compose-stack):

```bash
mkdir -p data/{utxo,out,clickhouse,blocks}
export BLOCKS_DIR=~/.bitcoin/blocks
docker compose up -d
docker compose run --rm bidx init-db
docker compose run --rm bidx load --input /data/out
```

The image is signed keyless with cosign and carries SLSA provenance plus an
SBOM attestation. Verify it with the identity regexp in
[`image.yml`](../.github/workflows/image.yml).

---

## 3. Distro packages (.deb / .rpm / .apk)

Every release attaches `bitcoindex` packages for amd64/arm64.

### Layout

| Path | Contents |
|------|----------|
| `/usr/bin/bidx` | The binary. |
| `/etc/bidx/bidx.toml` | Default serving config, marked as a config file so upgrades never clobber your edits. |
| `/usr/lib/systemd/system/bidx-serve.service` | Hardened unit for `bidx serve` (not present in the Alpine `.apk`, which has no systemd). |

Runtime dependency: `libzmq5` on deb, `zeromq` on rpm and apk.

### Install the .deb directly

```bash
sudo apt-get install -y ./bitcoindex_0.1.0_amd64.deb
```

### Install from the apt repository

The release ships `bidx-apt-repo.tar.gz`, a complete apt tree
(`dists/stable/main/binary-{amd64,arm64}` + `pool/`). Unpack it somewhere a
static host serves, then:

```bash
sudo mkdir -p /etc/apt/keyrings
curl -fsSL https://<host>/bitcoindex.asc | sudo tee /etc/apt/keyrings/bitcoindex.asc >/dev/null
echo "deb [signed-by=/etc/apt/keyrings/bitcoindex.asc] https://<host>/bitcoindex stable main" \
  | sudo tee /etc/apt/sources.list.d/bitcoindex.list
sudo apt-get update && sudo apt-get install -y bitcoindex
```

Reproduce the tree locally with
[`packaging/deb-repo.sh`](../packaging/deb-repo.sh):

```bash
packaging/deb-repo.sh dist/debs dist/apt-repo          # unsigned
packaging/deb-repo.sh dist/debs dist/apt-repo "$GPG_KEY_ID"  # signed (InRelease + Release.gpg)
```

A public repository should be signed; the script warns when it is not. Unsigned
trees still work with `[trusted=yes]` for local testing.

### Fedora / RHEL / Alpine

```bash
sudo dnf install ./bitcoindex-0.1.0-1.x86_64.rpm
sudo apk add --allow-untrusted ./bitcoindex_0.1.0_x86_64.apk
```

### Building the packages yourself

```bash
mkdir -p dist && cp target/release/bidx dist/
NFPM_ARCH=amd64 NFPM_VERSION=0.1.0 nfpm pkg --config packaging/nfpm.yaml \
  --packager deb --target bitcoindex_0.1.0_amd64.deb
```

`nfpm` is a single static binary. Note the per-packager architecture names:
deb wants `amd64`/`arm64`, rpm and apk want `x86_64`/`aarch64`. `src` paths in
`nfpm.yaml` resolve relative to the working directory, so run it from the repo
root with the binary staged at `dist/bidx`.

---

## 4. Homebrew

For Homebrew users on Linux. This is a **binary** formula: it downloads the
prebuilt tarball rather than compiling RocksDB and libzmq, which would take
minutes.

```bash
brew install jlopp/tap/bitcoindex
```

This needs the `jlopp/homebrew-tap` repository to exist (Homebrew requires a
repository literally named `homebrew-tap` under the owner). The release
pipeline keeps it current automatically — see [below](#how-the-tap-is-updated).

The formula declares `depends_on :linux` (there is no macOS build) and
`depends_on "zeromq"`, and its `test do` block runs `bidx --version`.

### How the tap is updated

On every `v*` release the pipeline renders the formula and pushes it into the
tap, so users never install a stale version. Two scripts do the work:

```bash
# 1. Render the formula from the release manifest (both sha256s come from here).
packaging/update-formula.sh 0.1.0 dist/SHA256SUMS
#    -> packaging/homebrew/bitcoindex.rb

# 2. Push it into the tap (no-op if the formula is unchanged).
TAP_TOKEN=<token> packaging/publish-tap.sh packaging/homebrew/bitcoindex.rb 0.1.0
```

`publish-tap.sh` reads `TAP_REPO` (default `jlopp/homebrew-tap`) and `TAP_BRANCH`
(default `main`), and refuses to push a formula that still contains `@@`
placeholders.

### One-time setup

The tap has to exist before the first release can populate it:

1. Create `jlopp/homebrew-tap` on GitHub (public, empty is fine).
2. Add a repository secret `TAP_GITHUB_TOKEN` to `jlopp/bitcoindex`: a
   fine-grained PAT with **Contents: read and write** on `jlopp/homebrew-tap`
   only. Without it the release's tap-publish step is skipped, not failed.
3. The seed contents for the repository — a `README.md` and an empty `Formula/`
   directory — are checked in at
   [`packaging/homebrew/tap/`](../packaging/homebrew/tap/) so the tap can be
   created by copying that directory into a fresh repo.

The main repository only ships the tap's *contents* under `packaging/homebrew/tap/`
(it is not itself the tap); `publish-tap.sh` writes `Formula/bitcoindex.rb`.

---

## 5. cargo install

Two variants, with very different tradeoffs.

### From git (works today)

```bash
cargo install --git https://github.com/jlopp/bitcoindex --locked --bin bidx
```

This compiles from source, so it needs the full native toolchain and takes
several minutes. Prefer it when you want a specific commit.

### From crates.io

```bash
cargo install bidx-cli     # once published
```

The workspace is configured for this — every crate carries `version`,
`license`, `repository`, `homepage`, `keywords` and `categories`, and the
internal `bidx-*` dependencies declare a `version` alongside their `path` (which
crates.io requires; a bare path dependency cannot be packaged).

Publishing is **ordered**: `cargo publish -p bidx-core` first, then the crates
that depend on it (`bidx-parser`, `bidx-store`, `bidx-loader`, `bidx-serve`,
`bidx-live`), then `bidx-udf` and `bidx-cli` last. A crate cannot be published
until its `bidx-*` dependencies are on crates.io.

Neither variant installs `libzmq` or `clickhouse-client`; see
[What you still need](#what-you-still-need-after-installing).

---

## 6. Nix

Flakes are the supported interface ([`flake.nix`](../flake.nix)):

```bash
# Run without installing:
nix run github:jlopp/bitcoindex -- --help

# Install into your profile:
nix profile install github:jlopp/bitcoindex

# Development shell with the full native toolchain and libzmq:
nix develop github:jlopp/bitcoindex
```

The package is built with `buildRustPackage` and reuses `Cargo.lock` for
reproducible dependency versions. It supplies `zeromq` so `zmq-sys` does not
build it from source, sets `LIBCLANG_PATH` for bindgen, and applies the same
`CXXFLAGS=-include cstdint` workaround as the Dockerfile. It also installs
`/etc/bidx/bidx.toml` and the systemd unit, matching the `.deb`/`.rpm` layout.

`doCheck = false`: the test suite needs a live ClickHouse and bitcoind, which
the Nix sandbox does not provide. Run `cargo test --release --workspace` in
`nix develop` for that.

---

## 7. Build from source

Needs a C/C++ toolchain, `libclang` for RocksDB's bindgen step, `cmake`,
`pkg-config`, and `libzmq` for live mode:

```bash
sudo apt-get install -y build-essential clang libclang-dev llvm-dev cmake pkg-config libzmq3-dev

source scripts/dev-env.sh   # sets the workarounds and finds libclang for you
cargo build --release
```

Or by hand:

```bash
./scripts/build-shim.sh                          # writes .build-shim/include
export LIBCLANG_PATH=/usr/lib/x86_64-linux-gnu  # wherever libclang lives
export BINDGEN_EXTRA_CLANG_ARGS="-I$PWD/.build-shim/include"
export CXXFLAGS="-include cstdint"               # rocksdb 8.x transitive <cstdint>
cargo build --release
```

`target/release/bidx` is the binary. The two exports are the workarounds the
`Dockerfile` also bakes in: `BINDGEN_EXTRA_CLANG_ARGS` points bindgen at the
shim so `stdbool.h` resolves (libclang's resource dir ships without it and
never falls back to gcc's copy), and `CXXFLAGS=-include cstdint` covers rocksdb
8.x's reliance on a transitive `<cstdint>` that GCC 15 no longer provides. If
you see `'stdbool.h' file not found` or a `<cstdint>` error, that is why.

The installer's `--from-source` flag runs exactly this sequence for you.

---

## Verifying a release

Each release attaches a CycloneDX SBOM and a `SHA256SUMS` covering the binary
tarballs. Verify a download before running it:

```bash
curl -fsSLO https://github.com/jlopp/bitcoindex/releases/download/v0.1.0/bidx-x86_64-unknown-linux-gnu.tar.gz
curl -fsSLO https://github.com/jlopp/bitcoindex/releases/download/v0.1.0/SHA256SUMS
sha256sum --check --ignore-missing SHA256SUMS
```

The install script does this automatically and refuses to install on a
mismatch. For the container image, verify the cosign signature and the
provenance/SBOM attestations as described in
[`image.yml`](../.github/workflows/image.yml).

Note that `SHA256SUMS` covers the three binary tarballs only; the apt-repo
archive (`bidx-apt-repo.tar.gz`) and the packages are published alongside it
but are not part of that manifest.

---

## What you still need after installing

Installing `bidx` is the easy part; the index is the work. After any option
above, the typical path is:

```bash
bidx headers --blocks-dir ~/.bitcoin/blocks          # sanity-check the chain
bidx parse --blocks-dir ~/.bitcoin/blocks \
           --out ./out --utxo ./utxo-db \
           --rpc-url http://127.0.0.1:8332           # enables the disk-space guard
bidx init-db --database bitcoin                       # needs clickhouse-client
bidx load --input ./out --database bitcoin
bidx verify --database bitcoin --rpc-url http://127.0.0.1:8332
```

Then serve:

```bash
bidx serve --config /etc/bidx/bidx.toml
curl -s http://127.0.0.1:8432/blocks/tip/height
```

The disk-space guard refuses to start a run that would fill the index
filesystem below 10 GiB free; `--skip-disk-check` bypasses it. Full flag
reference: [cli.md](cli.md). Live mode needs `libzmq` and
`zmqpubhashblock=tcp://127.0.0.1:28332` in `bitcoin.conf`.

---

## Uninstalling

```bash
# apt
sudo apt-get remove bitcoindex

# dnf
sudo dnf remove bitcoindex

# apk
sudo apk del bitcoindex

# Homebrew
brew uninstall bitcoindex

# cargo
cargo uninstall bidx-cli

# Nix
nix profile remove bitcoindex

# install script: just delete the binary
rm "$(command -v bidx)"
```

None of these touch your data. The index lives wherever you pointed
`--utxo`/`--out` (and `/var/lib/bidx` for the packaged service); remove those
directories yourself when you want the space back.

---

## Troubleshooting

| Symptom | Cause and fix |
|---------|---------------|
| `error while loading shared libraries: libzmq.so.5` | The binary links the system libzmq. Install `libzmq5` (deb) / `zeromq` (rpm, apk). Only `live` needs it. |
| `command not found: clickhouse-client` | Install it from the ClickHouse repo (see [above](#what-you-need-before-you-start)). Needed by `init-db`/`load`/`migrate`/`verify`/`index rebuild`. |
| `checksum mismatch for bidx-…` | The download was corrupted or tampered with. Re-run; if it persists, verify manually against `SHA256SUMS`. |
| `download failed … release assets have not finished publishing` | The release exists but its `Release` workflow has not attached the tarballs yet (or it has no build for your target). Wait for the workflow to finish, or re-run with `--version`/`--from-source`. |
| `no prebuilt binary for architecture` | Use `--from-source`, or the container image. |
| Installer fell back to `~/.local/bin` | The prefix was not writable. Add it to `PATH`, or re-run with `sudo`. |
| `'stdbool.h' file not found` on a source build | Source `scripts/dev-env.sh`, or run `./scripts/build-shim.sh` and export `BINDGEN_EXTRA_CLANG_ARGS` as in [option 7](#7-build-from-source). |
| `Unable to find libclang` on a source build | `libclang-dev` is missing or `LIBCLANG_PATH` is wrong. `scripts/dev-env.sh` locates it via `ldconfig`; usually `/usr/lib/x86_64-linux-gnu`, not `/usr/lib/llvm-<version>/lib`. |
| `<cstdint> not found` on a source build | Export `CXXFLAGS="-include cstdint"` (rocksdb 8.x, GCC 15+). |
| A musl/Alpine binary fails to run | The musl artifact may still link a glibc `libzmq`. Use the `.apk` package, or `--from-source`. |

See [deployment-ops.md](deployment-ops.md#troubleshooting) for runtime
troubleshooting (ClickHouse connectivity, the disk guard, reorgs, upgrades).
