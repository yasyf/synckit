#!/usr/bin/env bash
set -euo pipefail

workflow=.github/workflows/release.yml
release_pin=4eb5c04c8e2b93464e22774e65184b66b794e1ea

if grep -Eq 'yasyf/homebrew-tap/.github/workflows/release-go.yml@(main|v[0-9]+)' "$workflow"; then
  echo "release-go workflow must use one exact commit" >&2
  exit 1
fi
test "$(grep -Ec "workflows/release-go.yml@${release_pin}$" "$workflow")" = 1
