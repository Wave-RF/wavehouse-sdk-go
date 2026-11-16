package wavehouse_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	wavehouse "github.com/Wave-RF/wavehouse-sdk-go"
)

// The examples need a running server, so they are compiled but not run.

func ExampleNew() {
	wh, err := wavehouse.New("http://localhost:8080", wavehouse.WithToken(os.Getenv("WAVEHOUSE_TOKEN")))
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = wh.Health(ctx)
	cancel()
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleWithTokenSource() {
	// fetchToken stands in for an identity provider client that caches its token.
	fetchToken := func(context.Context) (string, error) { return os.Getenv("WAVEHOUSE_TOKEN"), nil }

	wh, err := wavehouse.New("https://wavehouse.example.com", wavehouse.WithTokenSource(fetchToken))
	if err != nil {
		log.Fatal(err)
	}
	_ = wh
}

func ExampleWithOperatorKey() {
	wh, err := wavehouse.New("http://localhost:8080",
		wavehouse.WithOperatorKey(os.Getenv("WAVEHOUSE_OPERATOR_KEY")),
		wavehouse.WithRetry(wavehouse.RetryPolicy{MaxAttempts: 5, MaxBackoff: 10 * time.Second}),
		wavehouse.WithUserAgent("my-tool/1.0"),
	)
	if err != nil {
		log.Fatal(err)
	}
	_ = wh
}

func ExampleClient_WithTenant() {
	wh, err := wavehouse.New("http://localhost:8080", wavehouse.WithOperatorKey(os.Getenv("WAVEHOUSE_OPERATOR_KEY")))
	if err != nil {
		log.Fatal(err)
	}
	// acme shares wh's connections; wh still addresses the default tenant.
	acme := wh.WithTenant("acme")
	tables, err := acme.Tables(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	for _, t := range tables {
		fmt.Println(t.Name)
	}
}

func ExampleClient_Version() {
	wh, err := wavehouse.New("http://localhost:8080")
	if err != nil {
		log.Fatal(err)
	}
	v, err := wh.Version(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(v.Version, v.GitCommit)
}

func ExampleClient_Table() {
	wh, err := wavehouse.New("http://localhost:8080", wavehouse.WithToken(os.Getenv("WAVEHOUSE_ADMIN_TOKEN")))
	if err != nil {
		log.Fatal(err)
	}
	t, err := wh.Table(context.Background(), "clicks")
	if errors.Is(err, wavehouse.ErrNotFound) {
		fmt.Println("no such table")
		return
	}
	if err != nil {
		log.Fatal(err)
	}
	for _, col := range t.Columns {
		fmt.Println(col.Position, col.Name, col.Type)
	}
}

func ExampleClient_ReloadSettings() {
	wh, err := wavehouse.New("http://localhost:8080", wavehouse.WithOperatorKey(os.Getenv("WAVEHOUSE_OPERATOR_KEY")))
	if err != nil {
		log.Fatal(err)
	}
	// On a rejected directory both are set: the findings say why.
	res, err := wh.ReloadSettings(context.Background())
	if res != nil {
		for _, f := range res.Findings {
			fmt.Printf("%s: %s: %s\n", f.Severity, f.File, f.Message)
		}
	}
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleClient_PipeDefinition() {
	wh, err := wavehouse.New("http://localhost:8080", wavehouse.WithToken(os.Getenv("WAVEHOUSE_ADMIN_TOKEN")))
	if err != nil {
		log.Fatal(err)
	}
	p, err := wh.PipeDefinition(context.Background(), "top_pages")
	if err != nil {
		log.Fatal(err)
	}
	for _, param := range p.Parameters {
		fmt.Println(param.Name, param.Type, param.Required)
	}
}

func ExampleAPIError() {
	wh, err := wavehouse.New("http://localhost:8080")
	if err != nil {
		log.Fatal(err)
	}
	_, err = wh.DLQStats(context.Background())
	if apiErr, ok := errors.AsType[*wavehouse.APIError](err); ok {
		// Quote RequestID when reporting a failure: the server logged the request under it.
		fmt.Println(apiErr.StatusCode, apiErr.Code, apiErr.RequestID)
	}
	if wavehouse.IsRetryable(err) {
		fmt.Println("worth trying again later")
	}
}

func ExampleWithTenant() {
	wh, err := wavehouse.New("http://localhost:8080", wavehouse.WithTenant("acme"))
	if err != nil {
		log.Fatal(err)
	}
	// Sent as X-Tenant-ID: an unknown tenant is a 404.
	if err := wh.Health(context.Background()); errors.Is(err, wavehouse.ErrNotFound) {
		fmt.Println("tenant not served")
	}
}

func ExampleWithHTTPClient() {
	// The default client has no overall timeout; bound each call with its ctx instead.
	hc := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 32}}
	wh, err := wavehouse.New("http://localhost:8080",
		wavehouse.WithHTTPClient(hc),
		wavehouse.WithHeader("X-Request-Id", "deploy-check-1"),
	)
	if err != nil {
		log.Fatal(err)
	}
	_ = wh
}

func ExampleClient_Tables() {
	wh, err := wavehouse.New("http://localhost:8080", wavehouse.WithToken(os.Getenv("WAVEHOUSE_ADMIN_TOKEN")))
	if err != nil {
		log.Fatal(err)
	}
	tables, err := wh.Tables(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	for _, t := range tables {
		fmt.Println(t.Name, len(t.Columns))
	}
}

func ExampleClient_RefreshSchema() {
	wh, err := wavehouse.New("http://localhost:8080", wavehouse.WithToken(os.Getenv("WAVEHOUSE_ADMIN_TOKEN")))
	if err != nil {
		log.Fatal(err)
	}
	// After a CREATE TABLE, so the server sees the table without waiting for its interval.
	if _, err := wh.RefreshSchema(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func ExampleClient_DLQStats() {
	wh, err := wavehouse.New("http://localhost:8080", wavehouse.WithToken(os.Getenv("WAVEHOUSE_ADMIN_TOKEN")))
	if err != nil {
		log.Fatal(err)
	}
	s, err := wh.DLQStats(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	for table, n := range s.Tables {
		fmt.Println(table, n)
	}
}

func ExampleClient_Pipes() {
	wh, err := wavehouse.New("http://localhost:8080", wavehouse.WithToken(os.Getenv("WAVEHOUSE_ADMIN_TOKEN")))
	if err != nil {
		log.Fatal(err)
	}
	pipes, err := wh.Pipes(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	for _, p := range pipes {
		fmt.Println(p.Name, p.AllowedRoles)
	}
}

func ExampleClient_From() {
	wh, err := wavehouse.New("http://localhost:8080", wavehouse.WithToken(os.Getenv("WAVEHOUSE_TOKEN")))
	if err != nil {
		log.Fatal(err)
	}
	// One chain, ending in Scan: a query is a value you can keep and extend.
	recent := wh.From("clicks").Where("score", wavehouse.Gt, 10).OrderBy("received_timestamp", wavehouse.Desc).Limit(100)
	rows, err := recent.Select("page", "score").Fetch(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	for _, row := range rows {
		fmt.Println(row["page"], row["score"])
	}
}

func ExampleQuery_Insert() {
	wh, err := wavehouse.New("http://localhost:8080", wavehouse.WithToken(os.Getenv("WAVEHOUSE_TOKEN")))
	if err != nil {
		log.Fatal(err)
	}
	type click struct {
		EventID string `json:"event_id"` // the server's dedupe id field, when it has one
		Page    string `json:"page"`
	}
	res, err := wh.From("clicks").Insert(context.Background(), []click{{"e1", "/home"}, {"e2", "/about"}})
	// The server answers 200 even when it rejects records: the result is returned
	// with an *InsertError saying which.
	if ie, ok := errors.AsType[*wavehouse.InsertError](err); ok {
		for _, f := range ie.Failed {
			fmt.Printf("record %d rejected: %s\n", f.Index, f.Error)
		}
	} else if err != nil {
		log.Fatal(err)
	}
	fmt.Println(res.Succeeded, "published,", res.Duplicates, "duplicates")
}

func ExampleQuery_Scan() {
	wh, err := wavehouse.New("http://localhost:8080", wavehouse.WithToken(os.Getenv("WAVEHOUSE_TOKEN")))
	if err != nil {
		log.Fatal(err)
	}
	var top []struct {
		Page  string `json:"page"`
		Views uint64 `json:"views"` // exact, even above 2^53
	}
	err = wh.From("clicks").
		Count("*", "views").
		GroupBy("page").
		Select("page").
		OrderBy("views", wavehouse.Desc).
		Limit(10).
		TimeRange("received_timestamp", "24h", "").
		Scan(context.Background(), &top)
	if err != nil {
		log.Fatal(err)
	}
	for _, r := range top {
		fmt.Println(r.Page, r.Views)
	}
}

func ExampleClient_Pipe() {
	wh, err := wavehouse.New("http://localhost:8080", wavehouse.WithToken(os.Getenv("WAVEHOUSE_TOKEN")))
	if err != nil {
		log.Fatal(err)
	}
	var rows []struct {
		Page  string `json:"page"`
		Views int64  `json:"views"`
	}
	err = wh.Pipe("top_pages").Param("limit", 10).Param("pages", []string{"/home", "/about"}).Scan(context.Background(), &rows)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(len(rows))
}

func ExampleClient_SQL() {
	wh, err := wavehouse.New("http://localhost:8080", wavehouse.WithOperatorKey(os.Getenv("WAVEHOUSE_OPERATOR_KEY")))
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	if err := wh.SQL("CREATE TABLE IF NOT EXISTS t (a UInt64) ENGINE = Memory").Exec(ctx); err != nil {
		log.Fatal(err)
	}
	var out []struct {
		N uint64 `json:"n"`
	}
	if err := wh.SQL("SELECT count() AS n FROM t").Scan(ctx, &out); err != nil {
		log.Fatal(err)
	}
	fmt.Println(out[0].N)
}
