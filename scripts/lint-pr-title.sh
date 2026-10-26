#!/usr/bin/env bash
# Canonical PR-title (Conventional Commits) validator, shared by every WaveHouse
# SDK repo and by the server. Agents run it before `gh pr create`; the `PR title`
# job in shared-checks.yml runs it through scripts/ci/check-pr-title.sh, so a
# bad title is caught before the required check fails.
#
# Usage:  scripts/lint-pr-title.sh "<title>"     (or pipe the title on stdin)
# Exit:   0 = valid; 1 = invalid (human-readable reason on stderr).
# Env:    PR_TITLE_SKIP_LENGTH=1  exempt the length cap (Dependabot grouped updates).
#         PR_TITLE_MAX_LEN=N      override the 72-char cap.
#
# Rule:
#   <type>(optional-scope)(optional-!): <subject>
#   - type in {feat,fix,docs,refactor,test,chore,ci,deps,build,perf,revert,style}
#   - subject starts lowercase (house style) and has no trailing period
#   - title <= 72 chars (it becomes the squash-merge commit subject on main)
set -uo pipefail

MAX_LEN="${PR_TITLE_MAX_LEN:-72}"
# Type list — keep in sync with the org CONTRIBUTING.md (Wave-RF/.github).
PATTERN='^(feat|fix|docs|refactor|test|chore|ci|deps|build|perf|revert|style)(\([^)]+\))?!?: [^A-Z].+[^.]$'

# Use the argument if one was passed (even an empty one — never block on stdin
# for an explicit `""`); read stdin only when invoked with no arguments at all.
if [ "$#" -ge 1 ]; then
  title="$1"
else
  title="$(cat)"
fi

if [ -z "$title" ]; then
  echo "PR title is empty." >&2
  exit 1
fi

if [ "${PR_TITLE_SKIP_LENGTH:-}" != "1" ] && [ "${#title}" -gt "$MAX_LEN" ]; then
  echo "PR title is ${#title} chars (max ${MAX_LEN} — it becomes the squash-merge subject):" >&2
  echo "  ${title}" >&2
  exit 1
fi

if [[ "$title" =~ $PATTERN ]]; then
  exit 0
fi

cat >&2 <<EOF
PR title does not match Conventional Commits format:
  ${title}
Expected: <type>(optional-scope)(optional-!): <lowercase subject, no trailing period>   (<= ${MAX_LEN} chars)
Types: feat fix docs refactor test chore ci deps build perf revert style
e.g.  fix(ingest): drop duplicate events   ·   chore(deps): bump clickhouse-go to v2.30
EOF
exit 1
