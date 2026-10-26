# AGENTS.md — Wave-RF/wavehouse-sdk-go

The Go client for [WaveHouse](https://github.com/Wave-RF/WaveHouse). Module `github.com/Wave-RF/wavehouse-sdk-go`, package `wavehouse`, Go floor in `go.mod`. It was imported with history from the `go-sdk` branch of the server repository ([Wave-RF/WaveHouse#434](https://github.com/Wave-RF/WaveHouse/pull/434)) and is not released yet.

## Layout

- The package is flat at the repo root: `wavehouse.go` (client and config), `http.go` (REST transport, retries), `query_builder.go` and `table.go` (queries and inserts), `stream.go` and `live_query.go` (SSE and live queries), `pipes.go`, `policy.go`, `schema.go`, `dlq.go`, `sys.go`, `types.go`, `errors.go`.
- `testdata/wire_cases.json` holds the request conformance cases. The JS SDK is meant to replay the same cases.
- `e2e_test.go` holds the live tests, behind the `e2e` build tag.
- `scripts/` and `.github/` hold the files kept in step with `wavehouse-sdk-js` (see *Files kept in step*) plus this repo's `ci.yml`, `release.yml` and `scripts/ci/release.sh`.

## Commands

- `make ci` runs what CI runs: gofumpt check, `go vet`, golangci-lint (pinned in the Makefile, installed into `.bin/`), race tests with the coverage floor, shellcheck and actionlint.
- `make fix` applies formatting and lint fixes.
- `make test-e2e` runs the live tests against the server at `WAVEHOUSE_URL`. It is not part of `ci`.

## Rules

- **Standard library only.** The SDK has no third-party runtime dependencies, SSE framing included, and `go.mod` carries no `require` lines. Adding one needs a maintainer's agreement first. Dev tools run through `go run <module>@<version>` or `.bin/`, not `tool` directives, so they stay out of `go.mod`.
- The coverage floor is in the Makefile. Raise it when coverage rises; don't lower it to land a change.

## Tests today

- **Unit:** `*_test.go` against `httptest` servers.
- **Request conformance:** `conformance_test.go` replays `testdata/wire_cases.json` and checks each call's method, path, content type and body.
- **Live:** `e2e_test.go` behind the `e2e` build tag; not run in CI yet.
- **Server end-to-end:** none yet.

## Known gap

The SDK was written before several server protocol changes. Where it and the server disagree, the server wins; the catch-up is tracked in this repository's issues.

## Releasing

Tag `vX.Y.Z` (or `vX.Y.Z-rc.N`) on `main` and push the tag. `release.yml` validates the tag, creates the GitHub Release with generated notes (a prerelease unless the channel is `latest`) and asks the Go module proxy for the version. Go needs no publish step: the tag is the release.

## Rules shared by both WaveHouse SDKs

This section is the same in both SDK repositories, and so are the files listed under *Files kept in step* below. Change them in both, in matching PRs that link each other.

### The SDKs and the server

- The server is [Wave-RF/WaveHouse](https://github.com/Wave-RF/WaveHouse). It defines the HTTP and SSE protocol, and an SDK follows it. When an SDK and the server disagree, the server's code and its [API reference](https://wavehouse.dev/api) decide, not another SDK.
- The SDKs are [`wavehouse-sdk-js`](https://github.com/Wave-RF/wavehouse-sdk-js) (`@wavehouse/sdk` on npm) and [`wavehouse-sdk-go`](https://github.com/Wave-RF/wavehouse-sdk-go) (`github.com/Wave-RF/wavehouse-sdk-go`). They share no code, but they should behave the same. When one SDK's public behaviour changes, open an issue in the other unless one exists.
- The user documentation for every SDK lives on the [WaveHouse docs site](https://wavehouse.dev/sdk), in the server repository under `docs/src/content/docs/sdk/`, one page per topic with every language on it. A change to public behaviour needs a docs PR there; link it from this repo's PR.
- A server bug goes to the server's issue tracker, not here.

### Operating rules

1. **Run `make ci` before every push.** It runs what CI runs. Don't use CI as your first feedback loop.
2. **Record every user-visible change in `CHANGELOG.md`** under `## [Unreleased]`, in the same PR.
3. **Drafts only, and a valid title.** Agents open PRs with `gh pr create --draft` and never mark one ready, approve or merge it. The title becomes the squash-merge commit on `main`, so it must be a Conventional Commit of at most 72 characters with a lowercase subject and no trailing period. Check it with `scripts/lint-pr-title.sh "<title>"` before creating the PR; the `PR title` job runs the same script.
4. **Never force-push or rebase a PR branch.** To take in `main`, `git merge origin/main`.
5. **Address and resolve every review finding.** Fix it, or open an issue and link it in your reply, then resolve the thread. Reply to a bot with an @-mention (`@coderabbitai`), or it never sees the reply. Unresolved threads block the merge.
6. **Comment the why, not the what,** in one or two lines, at the density of the file around it.

### Merging and releasing

- `main` takes squash merges through a merge queue, after one maintainer approval and the `CI` check. `CI` is the aggregator job in `.github/workflows/ci.yml`; add every new job to its `needs`.
- A release is a tag, and the tag is the version: `vX.Y.Z`, or `vX.Y.Z-alpha.N`, `-beta.N` or `-rc.N` for a prerelease. `scripts/ci/release-channel.sh` maps a tag to its channel (`latest`, `alpha`, `beta`, `rc` or `next`) and rejects one that isn't semver. Only maintainers can push release tags.
- Before 1.0 a minor release may break the API. Mark a breaking PR's title with `!` and label it `breaking-change`, so the release notes lead with it.

### Tests

Every SDK works toward four layers. The repo's own section says which it has today.

| Layer | Runs in | Proves |
| --- | --- | --- |
| Unit | this repo | the SDK's own logic |
| Request conformance | this repo, from shared cases | for each SDK call in the cases, the exact request it sends: method, path, content type and body |
| Live | this repo, against a real server | that real responses decode and streams work end to end |
| Server end-to-end | the server repo, pinned to a released SDK | that the server still works with the released SDK |

A test that hand-writes a server response can't catch a decoding bug against the real server. That is the live layer's job, so don't count a mocked test as coverage of it.

### Files kept in step

These files are identical in both SDK repositories: `.claude/settings.json`, `.editorconfig`, `.github/copilot-instructions.md`, `.github/release.yml`, `.github/workflows/shared-checks.yml`, `CLAUDE.md`, `LICENSE`, `scripts/lint-pr-title.sh`, `scripts/ci/check-pr-title.sh`, `scripts/ci/release-channel.sh` and `scripts/ci/require-needs.sh`. Nothing syncs them, so a change to one goes into both repositories. Branch and tag protection come from organization rulesets that match every `wavehouse-sdk-*` repository.

### This repository is public

Everything here is world-readable: issues, PRs, commits, branch names and Actions logs. Write only what an outside contributor could see. That rules out internal hostnames, infrastructure, private repositories and their issue numbers, and credentials.
