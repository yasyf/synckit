#!/usr/bin/env bash
set -euo pipefail

workflow=.github/workflows/release.yml
release_pin=41f8de6765b3b833ef333b0b98f5683f0e46685b

if grep -Eq 'yasyf/homebrew-tap/.github/workflows/release-go.yml@(main|v[0-9]+)' "$workflow"; then
  echo "release-go workflow must use one exact commit" >&2
  exit 1
fi
test "$(grep -Ec "workflows/release-go.yml@${release_pin}$" "$workflow")" = 1
