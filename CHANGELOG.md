# Changelog

## [Unreleased]

Ready for the server behaviour of [Wave-RF/WaveHouse#882](https://github.com/Wave-RF/WaveHouse/pull/882), which validates ingest with ClickHouse's own parser and sends rows as ClickHouse renders them. Everything here also works against a 0.2.x server.

### Added

- `APIError.ExceptionCode` is ClickHouse's own error number when the server passes one on, as it does for an ingest body ClickHouse's parser refused (`117` for a column the table lacks or the role may not write). `APIError.Error` names it, since the server withholds the message when it would quote the table's definition.
- `Float`, a `float64` for struct fields, reads the strings `"nan"`, `"inf"` and `"-inf"` that the server sends for a NaN or an infinity (a `float64` field fails the whole `Scan` or `Event.Decode` on one), and writes them back on `Insert`, where `encoding/json` refuses to encode a `float64` NaN or infinity.

### Changed

- The docs say which struct field types read every server's rows, and the live tests now hold to them. A Decimal goes into a `json.Number`, which reads the number newer servers send as well as the string a 0.2.x server sends; the advice to use a `string` field or the `json:",string"` tag fit only 0.2.x. A Date or Date32 is `"2026-06-21"`, which a `time.Time` field refuses, so it goes into a `string` or is read with `Row.Time`. Times are compared as `time.Time`, never as strings, since a DateTime64 keeps its scale (`…:00.000Z`).
- `Query.Insert`, `InsertError` and `Event.Values` describe both servers. A record ClickHouse's parser refuses now refuses the whole call as a `400` `*APIError` (`clickhouse.rejected`, with the record named in the message), and nothing is published. Per-record rejections remain for a failed insert check, a missing dedupe id and a decline. A stream row holds an omitted column's evaluated `DEFAULT`, not `null`. No retry rule changed: a `400`, a `422` and a `500` with `retryable: false` are not resent, and a `503` waits out its `Retry-After`. Tests now pin each of these server answers.
- The live tests run ClickHouse 26.8.34.13, the server's own test pin, where they ran 26.6.3.62: chtypes publishes no 26.6 build, so a server that validates ingest with chtypes can't validate ingest on 26.6. The tests probe which kind of server they face and assert that server's answer.

## [0.1.0] - 2026-11-17

The first release. The SDK was imported from the `go-sdk` branch of WaveHouse ([Wave-RF/WaveHouse#434](https://github.com/Wave-RF/WaveHouse/pull/434)) and rewritten for the current server protocol; nothing of the pre-release API (`NewClient(Config)`, the namespace fields, the policy client, live queries) remains. Go 1.26 or newer; the runtime uses only the standard library.

### Client

- `New(baseURL, opts...)` with `WithToken`, `WithTokenSource`, `WithOperatorKey`, `WithHTTPClient`, `WithHeader`, `WithHeaderFunc`, `WithRetry` and `WithUserAgent`. Invalid options are all reported by `New` at once. A base URL with no scheme gets `http://` for a loopback host and `https://` for any other; a path, or a URL with a query or fragment, is refused.
- The token source and the header func run before every attempt, retries and stream connects included, so they must be cheap; caching is the implementer's job. Header precedence, lowest to highest: `WithHeader`, `WithHeaderFunc`, a call's own `Header`, then the SDK's (Content-Type, Accept, User-Agent, and Authorization when a token, token source result or operator key supplies one; with none, a caller's own `Authorization` header is sent).

### Queries, inserts, pipes and SQL

- `Client.From(table)` starts an immutable builder: `Select`, `SelectAll`, `Where(column, op, value)`, `OrderBy`, `GroupBy`, `Limit`, `TimeRange(column, from, to time.Time)`, `Last(column, d)`, `Count`, `Sum`, `Avg`, `Min`, `Max`, `CountDistinct`, `Aggregate` and `Header`, ending in `Scan(ctx, &rows)` (structs or maps) or `Fetch(ctx)` (`[]Row`, with `String`, `Int64`, `Uint64`, `Float64`, `Bool` and `Time` accessors). A misuse (an unknown operator or direction, an empty `In` list) is an error from `Scan`/`Fetch`, before anything is sent.
- `Select("*")` names a column called `*`; `SelectAll` reads every column the role may read. `GroupBy` sends only `group_by`, so select the key too to get it back ([Wave-RF/WaveHouse#295](https://github.com/Wave-RF/WaveHouse/issues/295)). No client-side default row limit: the server's `query.default_max_rows` applies.
- `Query.Insert` takes a struct, map or slice and always sends NDJSON, so every call returns a per-record `InsertResult`; rejected records also come back as an `*InsertError`. `InsertNDJSON` sends a pre-encoded body.
- `Client.Pipe(name).Param(...).Scan(ctx, &rows)` runs a pipe, `Exec` one that writes. `Client.SQL(query)` runs admin SQL with `Scan`, `Fetch` or `Exec`. 64-bit integers stay exact (`json.Number`).

### Streaming

- `Client.Subscribe(ctx, table, opts)` reads a table's live rows like `sql.Rows` (`Next`, `Event`, `Err`, `Close`) or as a channel (`Events`). It decodes the server's schema frames and positional rows into an `Event` (`Row()`, `Decode(dst)`), reconnects with backoff and `Last-Event-ID`, and replays from `Since` or a checkpointed `ResumeID`. Delivery is at-least-once.
- `OnState` reports the connection state; `OnDrop` reports a frame that was skipped after it failed to decode twice. A stream that drops before its first event resumes from the server's `Date`.

### Errors and retries

- Failures are `*APIError` with the server's `code`, `retryable` and `X-Request-Id`, matched with `errors.Is` (`ErrNotFound`, `ErrUnauthorized` and the other sentinels). `IsRetryable` tells you whether this call is safe to resend.
- Retries follow whether resending is safe: reads and ingest retry transport errors and retryable answers; a pipe call retries only the server's own `retryable:true`, a 429 or a 503 with `Retry-After`; raw SQL is never resent once its request may have reached the server. `Retry-After` is honoured, capped by `RetryPolicy.MaxBackoff`.
- `RetryPolicy.AttemptTimeout` (off by default) gives each attempt its own deadline, so a stalled attempt is retried instead of hanging the call; it matches `ErrAttemptTimeout`.
- A credentialed request never follows a redirect.

### Admin

- `Health`, `Version`, `Tables`, `TableSchema`, `RefreshSchema`, `DLQStats`, `ReloadSettings`, `Pipes` and `PipeDefinition`.

### Not in this release

- Live queries and pagination wait for server support ([Wave-RF/WaveHouse#951](https://github.com/Wave-RF/WaveHouse/issues/951), [Wave-RF/WaveHouse#452](https://github.com/Wave-RF/WaveHouse/issues/452)).
