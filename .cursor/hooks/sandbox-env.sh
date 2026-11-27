#!/usr/bin/env bash
set -euo pipefail

input="$(cat)"
cmd="$(printf '%s' "$input" | jq -r '.tool_input.command // empty')"

if [[ -z "$cmd" ]]; then
  printf '{"permission":"allow"}\n'
  exit 0
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
env_script="${repo_root}/.cursor/hooks/env.sh"

mkdir -p -m 700 "${repo_root}/.tmp" "${repo_root}/.sandbox-cache"

# Idempotent guard so retried commands aren't double-prefixed.
prefix=". \"${env_script}\"; "
if [[ "$cmd" == "${prefix}"* ]]; then
  printf '{"permission":"allow"}\n'
  exit 0
fi

jq -cn \
  --arg new_cmd "${prefix}${cmd}" \
  --argjson orig "$(printf '%s' "$input" | jq '.tool_input')" \
  '{
    permission: "allow",
    updated_input: ($orig + { command: $new_cmd })
  }'
