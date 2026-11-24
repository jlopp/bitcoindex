#!/usr/bin/env bash
# Build the apt repository tree (Packages/Release/InRelease) from a directory
# of .deb files, for publishing to GitHub Pages / any static host.
#
#   packaging/deb-repo.sh dist/debs dist/apt-repo [gpg-key-id]
#
# Without a key id the repo is generated unsigned (fine for local testing and
# for `[trusted=yes]`; a public repo should be signed). With one, `Release` is
# signed into `InRelease` and `Release.gpg` with `gpg --batch`.
#
# The resulting tree is a valid apt root for an entry like:
#   deb [signed-by=/etc/apt/keyrings/bitcoindex.asc] \
#       https://<host>/bitcoindex stable main
set -euo pipefail

debs="${1:?usage: deb-repo.sh <debs-dir> <out-dir> [gpg-key-id]}"
out="${2:?usage: deb-repo.sh <debs-dir> <out-dir> [gpg-key-id]}"
key="${3:-}"

[ -d "$debs" ] || { echo "deb-repo: no such directory: $debs" >&2; exit 1; }

pool="$out/pool/main/b/bitcoindex"
mkdir -p "$pool" "$out/dists/stable/main/binary-amd64" "$out/dists/stable/main/binary-arm64"

# Copy the .debs into the pool, bucketed by architecture.
shopt -s nullglob
found=0
for deb in "$debs"/*.deb; do
    found=1
    cp "$deb" "$pool/"
done
[ "$found" -eq 1 ] || { echo "deb-repo: no .deb files in $debs" >&2; exit 1; }

# Per-architecture Packages indexes. apt-ftparchive derives the fields from
# each .deb's control file and must run from the repo root so its relative
# paths resolve.
for arch in amd64 arm64; do
    dir="$out/dists/stable/main/binary-$arch"
    : > "$dir/Packages"
    for deb in "$pool"/*.deb; do
        deb_arch="$(dpkg-deb -f "$deb" Architecture)"
        [ "$deb_arch" = "$arch" ] || continue
        rel="${deb#"$out"/}"
        ( cd "$out" && apt-ftparchive --arch "$arch" packages "$rel" ) >> "$dir/Packages"
    done
    gzip -kf "$dir/Packages"
done

# Top-level Release, then InRelease/Release.gpg when signing.
release_cfg="$(mktemp)"
trap 'rm -f "$release_cfg"' EXIT
cat > "$release_cfg" <<EOF
APT::FTPArchive::Release::Origin "bitcoindex";
APT::FTPArchive::Release::Label "bitcoindex";
APT::FTPArchive::Release::Suite "stable";
APT::FTPArchive::Release::Codename "stable";
APT::FTPArchive::Release::Architectures "amd64 arm64";
APT::FTPArchive::Release::Components "main";
EOF
( cd "$out" && apt-ftparchive -c "$release_cfg" release dists/stable > dists/stable/Release )

if [ -n "$key" ]; then
    gpg --batch --yes --default-key "$key" --armor --detach-sign \
        --output "$out/dists/stable/Release.gpg" "$out/dists/stable/Release"
    gpg --batch --yes --default-key "$key" --clearsign \
        --output "$out/dists/stable/InRelease" "$out/dists/stable/Release"
    echo "deb-repo: signed with key $key"
else
    echo "deb-repo: WARNING: generated unsigned (no key id given)"
fi

echo "deb-repo: wrote $out"
