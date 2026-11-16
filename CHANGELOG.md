# Changelog

## [Unreleased]

- **Breaking:** the client core and admin calls are rewritten. `New(baseURL, opts...)` with `WithToken`, `WithTokenSource`, `WithOperatorKey`, `WithTenant`, `WithRetry`, `WithHTTPClient`, `WithHeader` and `WithUserAgent` replaces `NewClient(Config)`; `Client.WithTenant` derives a client for another tenant. Health, version, schema, DLQ, pipe-definition and settings-reload calls are methods on `Client`. Failures are `*APIError` (with the server's `code`, `retryable` and `X-Request-Id`), matched with `errors.Is` against `ErrNotFound` and the other sentinels; retries depend on whether resending is safe and honour `Retry-After`. JSON numbers decode as `json.Number`. Go 1.26 is now the minimum. Ingest, query, pipe execution, raw SQL and streaming are being rewritten onto the new core and are absent until then.
- Imported, with history, from the `go-sdk` branch of WaveHouse ([Wave-RF/WaveHouse#434](https://github.com/Wave-RF/WaveHouse/pull/434)). Not released yet; the SDK still has to catch up with the current server protocol.
