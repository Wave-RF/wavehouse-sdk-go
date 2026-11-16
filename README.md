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

    table, err := wh.Table(context.Background(), "clicks")
    if err != nil {
        log.Fatal(err)
    }
    for _, col := range table.Columns {
        fmt.Println(col.Name, col.Type)
    }
}
```

Ingest, queries, pipes and streaming are being rewritten onto this client and return before the first release.

## Documentation

The SDK guides are on the WaveHouse docs site at [wavehouse.dev/sdk](https://wavehouse.dev/sdk). They cover the JavaScript SDK today; Go examples join them with this SDK's first release. Until then, the package's doc comments and `example_test.go` are the reference.

## Contributing

`make ci` runs everything CI runs. See [AGENTS.md](AGENTS.md) for the layout and conventions.

## License

[Apache-2.0](LICENSE)
