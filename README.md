# WaveHouse Go SDK

Official Go client for [WaveHouse](https://github.com/Wave-RF/WaveHouse), a schema-aware real-time API gateway for ClickHouse. It has no third-party runtime dependencies, SSE parser included.

**Status: v0.1.0 is the first release.** The code was imported from the `go-sdk` branch of the WaveHouse repository and rewritten for the current server protocol. Before 1.0, a minor release may break the API.

## Install

```bash
go get github.com/Wave-RF/wavehouse-sdk-go
```

The import path is `github.com/Wave-RF/wavehouse-sdk-go` and the package name is `wavehouse`. It requires Go 1.26 or newer.

## Quick start

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"

    wavehouse "github.com/Wave-RF/wavehouse-sdk-go"
)

func main() {
    // Omit WithToken for unauthenticated access.
    wh, err := wavehouse.New("http://localhost:8080", wavehouse.WithToken(os.Getenv("WAVEHOUSE_TOKEN")))
    if err != nil {
        log.Fatal(err)
    }

    ctx := context.Background()

    // Write: a struct, a map, or a slice of either. Rejected records come back as an *InsertError.
    type click struct {
        Page  string `json:"page"`
        Score uint64 `json:"score"`
    }
    if _, err := wh.From("clicks").Insert(ctx, []click{{"/home", 3}, {"/about", 7}}); err != nil {
        log.Fatal(err)
    }

    // Read: a chain that ends in Scan, into structs or maps.
    var top []click
    err = wh.From("clicks").
        Select("page", "score").
        Where("score", wavehouse.Gt, 5).
        OrderBy("score", wavehouse.Desc).
        Limit(10).
        Scan(ctx, &top)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println(top)
}
```

`Fetch` returns `[]wavehouse.Row`, a map with typed accessors (`row.Uint64("score")`, `row.Time("ts")`, ...) that keep 64-bit integers exact and report a null or missing column by name. A window is `Last("ts", time.Hour)` (the server's clock) or `TimeRange("ts", from, to)`; `GroupBy` only sets `group_by`, so select the key too (`Select("page").GroupBy("page")`) to get it back; an aggregate with no alias is named as in the JS SDK (`count`, `sum_<column>`, `avg_<column>`, ...; `Aggregate` sends none), and `Select("*")` is a column name, so use `SelectAll` for every readable column. A bad operator or direction, an empty `In` list, or a `Where` on a query used for `Insert` is an error from the call, before anything is sent. `wh.Health` is a public liveness probe; the package examples show a real datasource health check, and a per-request token for a plugin that forwards its user's credential.

The same client runs pipes (`wh.Pipe("top_pages").Param("limit", 10).Scan(ctx, &rows)`), raw SQL as an admin (`wh.SQL("SELECT ...").Scan(ctx, &rows)`, or `.Exec(ctx)` for DDL) and the admin reads (`wh.Tables`, `wh.TableSchema`, `wh.Pipes`, `wh.DLQStats`).

## Authentication, headers and timeouts

`WithToken` sends a fixed JWT. For one that expires, `WithTokenSource(fn)` calls `fn` **before every attempt**: each retry, and each stream connect, runs it again, so a refreshed token is the one sent. `fn` must be cheap; caching and refreshing are yours to do. `WithHeaderFunc(fn)` is the same contract for headers that rotate.

Headers layer, lowest to highest: `WithHeader`, `WithHeaderFunc`, the call's own `Header(key, value)` (on a query, a pipe call or a SQL call; `SubscribeOptions.Header` for a stream), then the SDK's own (Content-Type, Accept, User-Agent, and Authorization when a token or operator key is set; with neither, your own `Authorization` header is sent as given). A later layer replaces a key, case-insensitively, and never joins values.

`RetryPolicy.AttemptTimeout` (off by default) gives each attempt its own deadline, so a stalled one is retried instead of holding the whole call. It covers a REST call, and each stream connect up to the response headers, but not an established stream. Your ctx still ends the call at once. A raw SQL or pipe call is not resent once its request headers were written, but an attempt that timed out while still connecting is.

## Streaming

`Subscribe` streams the rows ingested into a table, reconnecting and resuming on its own until you close it. A table the server doesn't know is not an error, only a stream that never delivers, so check the name first (`wh.TableSchema`, or a `Limit(1)` query):

```go
sub, err := wh.Subscribe(ctx, "clicks", nil)
if err != nil {
    log.Fatal(err)
}
defer sub.Close()
for sub.Next() {
    fmt.Println(sub.Event().Row())
}
if err := sub.Err(); err != nil {
    log.Fatal(err)
}
```

To select over the stream together with other channels, range over `sub.Events()` instead of calling `Next`; the channel closes when the subscription ends, and then `sub.Err()` says why. Use one or the other.

## Documentation

The SDK guides are on the WaveHouse docs site at [wavehouse.dev/sdk](https://wavehouse.dev/sdk). The package's doc comments and `example_test.go` are the API reference.

## Contributing

`make ci` runs everything CI runs. See [AGENTS.md](AGENTS.md) for the layout and conventions.

## License

[Apache-2.0](LICENSE)
