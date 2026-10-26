# WaveHouse Go SDK

Official Go client for [WaveHouse](https://github.com/Wave-RF/WaveHouse), a schema-aware real-time API gateway for ClickHouse. It has no third-party runtime dependencies, SSE parser included.

**Status: not released yet.** The code was imported from the `go-sdk` branch of the WaveHouse repository and predates several server protocol changes. Bringing it up to date with the current protocol is tracked in this repository's issues, and no version is tagged until that is done.

## Install

```bash
go get github.com/Wave-RF/wavehouse-sdk-go
```

The import path is `github.com/Wave-RF/wavehouse-sdk-go` and the package name is `wavehouse`. It requires Go 1.24 or newer.

## Quick start

```go
package main

import (
    "context"
    "fmt"
    "log"

    wavehouse "github.com/Wave-RF/wavehouse-sdk-go"
)

func main() {
    wh := wavehouse.NewClient(wavehouse.Config{
        BaseURL: "http://localhost:8080",
        Auth:    wavehouse.StaticToken("your-jwt"), // omit for unauthenticated access
    })

    page, err := wh.From("clicks").
        Select("page", "button").
        Where("page", wavehouse.OpEq, "/home").
        Limit(10).
        FetchUntyped(context.Background())
    if err != nil {
        log.Fatal(err)
    }
    for _, row := range page.Data {
        fmt.Println(row["page"], row["button"])
    }
}
```

## Documentation

The SDK guides are on the WaveHouse docs site at [wavehouse.dev/sdk](https://wavehouse.dev/sdk). They cover the JavaScript SDK today; Go examples join them with this SDK's first release. Until then, the package's doc comments and `example_test.go` are the reference.

## Contributing

`make ci` runs everything CI runs. See [AGENTS.md](AGENTS.md) for the layout and conventions.

## License

[Apache-2.0](LICENSE)
