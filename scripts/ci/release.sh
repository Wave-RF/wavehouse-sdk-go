#!/usr/bin/env bash
# Publish a release for a pushed tag: validate it, create the GitHub Release,
# then ask the Go module proxy for it so the version is fetchable at once.
#
# Usage: scripts/ci/release.sh vX.Y.Z[-rc.N]
# Env:   GH_TOKEN (for gh), GITHUB_REPOSITORY (default Wave-RF/wavehouse-sdk-go)
set -euo pipefail

tag="${1:?usage: $0 <tag>}"
module="github.com/Wave-RF/wavehouse-sdk-go"

# Exits non-zero on a tag that isn't semver, before anything is published.
channel="$(scripts/ci/release-channel.sh "$tag")"

args=(--verify-tag --generate-notes)
# A prerelease must not take "latest" from the last stable release.
[ "$channel" = "latest" ] || args+=(--prerelease)
gh release create "$tag" "${args[@]}"

# The proxy caches on first request; doing it here saves the first user a wait.
# Run outside the checkout: inside it Go refuses to query the main module at a version.
cd "$(mktemp -d)"
GOPROXY=https://proxy.golang.org go list -m "$module@$tag"
