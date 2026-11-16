# WaveHouse Go SDK

Official Go client for [WaveHouse](https://github.com/Wave-RF/WaveHouse), a schema-aware real-time API gateway for ClickHouse. It has no third-party runtime dependencies, SSE parser included.

**Status: not released yet.** The code was imported from the `go-sdk` branch of the WaveHouse repository and predates several server protocol changes. Bringing it up to date with the current protocol is tracked in this repository's issues, and no version is tagged until that is done.

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

The same client runs pipes (`wh.Pipe("top_pages").Param("limit", 10).Scan(ctx, &rows)`), raw SQL as an admin (`wh.SQL("SELECT ...").Scan(ctx, &rows)`, or `.Exec(ctx)` for DDL) and the admin reads (`wh.Tables`, `wh.TableSchema`, `wh.Pipes`, `wh.DLQStats`). An admin call takes `wavehouse.ForTenant("acme")` to address another tenant (`wh.SQL(...).ForTenant("acme")` for SQL); it sends `?tenant=`, which the admin routes read in place of the `X-Tenant-ID` header.

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

The SDK guides are on the WaveHouse docs site at [wavehouse.dev/sdk](https://wavehouse.dev/sdk). They cover the JavaScript SDK today; Go examples join them with this SDK's first release. Until then, the package's doc comments and `example_test.go` are the reference.

## Contributing

`make ci` runs everything CI runs. See [AGENTS.md](AGENTS.md) for the layout and conventions.

## License

[Apache-2.0](LICENSE)
