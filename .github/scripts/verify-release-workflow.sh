#!/usr/bin/env bash
set -euo pipefail

workflow=.github/workflows/release.yml
release_pin=7cc8a6c981cbec10fcb7f19bd75b36e9ee65ea7e

if grep -Eq 'yasyf/homebrew-tap/.github/workflows/release-go.yml@(main|v[0-9]+)' "$workflow"; then
  echo "release-go workflow must use one exact commit" >&2
  exit 1
fi
test "$(grep -Ec "workflows/release-go.yml@${release_pin}$" "$workflow")" = 1
