#!/usr/bin/env bash
# Force all temporary files and tool/build caches into the repository directory
# so Cursor's agent shell and sandbox never write to /tmp (tmpfs).

_bidx_repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
_bidx_tmp="${_bidx_repo_root}/.tmp"
_bidx_cache="${_bidx_repo_root}/.sandbox-cache"

# `umask 077` (rather than `mkdir -m`) so the mode applies to *every* directory
# `-p` creates, not just the deepest one (shellcheck SC2174).
( umask 077 && mkdir -p "${_bidx_tmp}" "${_bidx_cache}" )

export TMPDIR="${_bidx_tmp}"
export TMP="${_bidx_tmp}"
export TEMP="${_bidx_tmp}"
export TMPPREFIX="${_bidx_tmp}/sh-"

export CARGO_TARGET_DIR="${_bidx_repo_root}/target"
export CCACHE_DIR="${_bidx_cache}/ccache"
export NPM_CONFIG_CACHE="${_bidx_cache}/npm"
export PNPM_STORE_PATH="${_bidx_cache}/pnpm-store"
export YARN_CACHE_FOLDER="${_bidx_cache}/yarn"
export BUN_INSTALL_CACHE_DIR="${_bidx_cache}/bun"
export npm_config_devdir="${_bidx_cache}/node-gyp"
export GOCACHE="${_bidx_cache}/go-build"
export GOMODCACHE="${_bidx_cache}/go-mod"
export PIP_CACHE_DIR="${_bidx_cache}/pip"
export UV_CACHE_DIR="${_bidx_cache}/uv"
export POETRY_CACHE_DIR="${_bidx_cache}/poetry"
export CONDA_PKGS_DIRS="${_bidx_cache}/conda"
export PLAYWRIGHT_BROWSERS_PATH="${_bidx_cache}/playwright"
export PUPPETEER_CACHE_DIR="${_bidx_cache}/puppeteer"
export TURBO_CACHE_DIR="${_bidx_cache}/turbo"
export NX_CACHE_DIRECTORY="${_bidx_cache}/nx"
export CYPRESS_CACHE_FOLDER="${_bidx_cache}/cypress"
export GRADLE_USER_HOME="${_bidx_cache}/gradle"
export GEM_SPEC_CACHE="${_bidx_cache}/gem-specs"
export BUNDLE_PATH="${_bidx_cache}/bundle"
export COMPOSER_HOME="${_bidx_cache}/composer"
export HOMEBREW_CACHE="${_bidx_cache}/homebrew"
export NUGET_PACKAGES="${_bidx_cache}/nuget"
export CP_HOME_DIR="${_bidx_cache}/cocoapods"

unset _bidx_repo_root _bidx_tmp _bidx_cache
