#!/usr/bin/env bash
# Regenerate packaging/homebrew/bitcoindex.rb from a release's SHA256SUMS.
#
#   packaging/update-formula.sh <version> <SHA256SUMS> [template] [out]
#
# e.g. packaging/update-formula.sh 0.1.0 dist/SHA256SUMS
#
# The formula is a *binary* formula: Homebrew downloads the prebuilt tarball
# rather than building, because the RocksDB + libzmq build is a multi-minute
# C++ compile that no user should pay for on `brew install`.
#
# Homebrew on Linux uses the same tarballs as the installer script, so the
# per-arch URLs and sha256s are taken straight from the release manifest.
set -euo pipefail

version="${1:?usage: update-formula.sh <version> <SHA256SUMS> [template] [out]}"
sums="${2:?usage: update-formula.sh <version> <SHA256SUMS> [template] [out]}"
here="$(cd "$(dirname "$0")" && pwd)"
template="${3:-$here/homebrew/bitcoindex.rb.tmpl}"
out="${4:-$here/homebrew/bitcoindex.rb}"

[ -f "$sums" ] || { echo "update-formula: no such file: $sums" >&2; exit 1; }

# Strip a leading v so the formula version and the URLs use the bare semver.
bare="${version#v}"

# Look up one archive's sha256 in the manifest. SHA256SUMS lines are
# "<hash>  <filename>"; match on the filename exactly.
sha_for() {
    local file="$1" hash
    hash="$(awk -v f="$file" '$2 == f { print $1 }' "$sums")"
    [ -n "$hash" ] || { echo "update-formula: $file not found in $sums" >&2; exit 1; }
    printf '%s' "$hash"
}

amd64_tar="bidx-x86_64-unknown-linux-gnu.tar.gz"
arm64_tar="bidx-aarch64-unknown-linux-gnu.tar.gz"
sha_amd64="$(sha_for "$amd64_tar")"
sha_arm64="$(sha_for "$arm64_tar")"

# Render the template. Using awk (not sed) keeps the substitutions literal
# regardless of characters in the hashes.
awk \
    -v version="$bare" \
    -v sha_amd64="$sha_amd64" \
    -v sha_arm64="$sha_arm64" \
    -v tar_amd64="$amd64_tar" \
    -v tar_arm64="$arm64_tar" \
    '{
        gsub(/@@VERSION@@/, version);
        gsub(/@@SHA256_AMD64@@/, sha_amd64);
        gsub(/@@SHA256_ARM64@@/, sha_arm64);
        gsub(/@@TARBALL_AMD64@@/, tar_amd64);
        gsub(/@@TARBALL_ARM64@@/, tar_arm64);
        print
    }' "$template" > "$out"

echo "update-formula: wrote $out (version $bare)"
