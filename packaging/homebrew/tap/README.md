# bitcoindex Homebrew tap

```bash
brew install jlopp/tap/bitcoindex
```

This tap holds a single formula, `Formula/bitcoindex.rb`, for
[bitcoindex](https://github.com/jlopp/bitcoindex).

## It is generated, not hand-written

The formula is a **binary** formula: it downloads the prebuilt release tarball
for your architecture instead of compiling RocksDB and libzmq (a multi-minute
C++ build nobody should pay for on `brew install`). Its `version` and per-arch
`sha256` values come straight from the release's `SHA256SUMS`.

Every `v*` release of bitcoindex renders the formula with
`packaging/update-formula.sh` and pushes it here with
`packaging/publish-tap.sh`.

**Do not edit `Formula/bitcoindex.rb` by hand** — the next release overwrites it.
To change the formula, edit `packaging/homebrew/bitcoindex.rb.tmpl` in the main
repository instead.

## Installing

```bash
brew install jlopp/tap/bitcoindex
```

Homebrew on Linux only — the release matrix has no macOS target. `zeromq` is
pulled in as a dependency so `bidx live` works.

## Troubleshooting

- `Error: No available formula` — tap names are case-sensitive; use
  `jlopp/tap/bitcoindex`.
- `sha256 mismatch` — the release tarball was replaced without regenerating the
  formula. Open an issue on the main repository.
