#!/usr/bin/env bash
# The body of every SDK repo's `CI` aggregator job, the one required status
# check: fail when any job it needs failed or was cancelled, and treat a
# skipped job (the PR-title check on a push, say) as passing.
#
# Usage, from the aggregator job:
#   env:
#     NEEDS: ${{ toJSON(needs) }}
#   run: scripts/ci/require-needs.sh
set -euo pipefail

: "${NEEDS:?set NEEDS to the toJSON(needs) of the aggregator job}"

jq -r 'to_entries[] | "\(.value.result)\t\(.key)"' <<<"$NEEDS"
bad="$(jq -r '[to_entries[] | select(.value.result == "failure" or .value.result == "cancelled") | .key] | join(", ")' <<<"$NEEDS")"
if [ -n "$bad" ]; then
  echo "::error::Jobs failed or were cancelled: $bad"
  exit 1
fi
echo "All jobs succeeded (or were intentionally skipped)."
