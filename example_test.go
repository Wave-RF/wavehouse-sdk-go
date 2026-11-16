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

// ExampleWithTokenSource_perRequest forwards each caller's own token, as a
// plugin that runs queries on behalf of signed-in users would: the token source
// receives the context of the call it authorises, so the caller puts the token
// in the context and the source reads it back.
func ExampleWithTokenSource_perRequest() {
	type tokenKey struct{}

	wh, err := wavehouse.New("https://wavehouse.example.com", wavehouse.WithTokenSource(
		func(ctx context.Context) (string, error) {
			tok, ok := ctx.Value(tokenKey{}).(string)
			if !ok {
				return "", errors.New("no user token on the request context")
			}
			return tok, nil
		}))
	if err != nil {
		log.Fatal(err)
	}

	// In a request handler: userToken is the OAuth token the user signed in with.
	userToken := "token-from-the-incoming-request"
	ctx := context.WithValue(context.Background(), tokenKey{}, userToken)
	rows, err := wh.From("clicks").Limit(10).Fetch(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(len(rows))
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

// ExampleClient_Health builds a datasource health check. Health alone is not
// one: the endpoint is public and checks neither the credential nor ClickHouse.
func ExampleClient_Health() {
	wh, err := wavehouse.New("http://localhost:8080", wavehouse.WithToken(os.Getenv("WAVEHOUSE_TOKEN")))
	if err != nil {
		log.Fatal(err)
	}
	check := func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := wh.Health(ctx); err != nil { // the server is up
			return fmt.Errorf("server: %w", err)
		}
		// An authenticated read proves the credential and the path to ClickHouse.
		// Use Tables instead for an admin credential, or From(table).Limit(1) with
		// a table the role may read.
		_, err := wh.From("clicks").Limit(1).Fetch(ctx)
		switch {
		case errors.Is(err, wavehouse.ErrUnauthorized):
			return errors.New("the credential is missing, expired or invalid")
		case errors.Is(err, wavehouse.ErrForbidden):
			return errors.New("the credential is valid but may not read this table")
		case err != nil:
			return fmt.Errorf("query: %w", err)
		}
		return nil
	}
	if err := check(); err != nil {
		log.Fatal(err)
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

func ExampleClient_TableSchema() {
	wh, err := wavehouse.New("http://localhost:8080", wavehouse.WithToken(os.Getenv("WAVEHOUSE_ADMIN_TOKEN")))
	if err != nil {
		log.Fatal(err)
	}
	t, err := wh.TableSchema(context.Background(), "clicks")
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

// ExampleWithHeader sends a header on every call and on the stream, for a
// proxy in front of the server, say. It is sent unchanged.
func ExampleWithHeader() {
	wh, err := wavehouse.New("http://localhost:8080", wavehouse.WithHeader("X-Trace-Id", "abc123"))
	if err != nil {
		log.Fatal(err)
	}
	if err := wh.Health(context.Background()); err != nil {
		log.Fatal(err)
	}
}

// ExampleWithHeaderFunc rotates a credential header. fn runs before every
// attempt, retries and stream connects included, so it must be cheap: the
// vault client caches the secret and refreshes it itself.
func ExampleWithHeaderFunc() {
	// secret stands in for a vault client that caches what it fetched.
	secret := func(context.Context) (string, error) { return os.Getenv("ACCESS_SECRET"), nil }

	wh, err := wavehouse.New("https://wavehouse.example.com", wavehouse.WithHeaderFunc(
		func(ctx context.Context) (http.Header, error) {
			s, err := secret(ctx)
			if err != nil {
				return nil, err
			}
			return http.Header{"X-Access-Secret": {s}}, nil
		}))
	if err != nil {
		log.Fatal(err)
	}
	_ = wh
}

// ExampleQuery_Header sends a header on one call only, over the client's own
// headers of the same name. It also works on a pipe call, a SQL call and
// SubscribeOptions.Header.
func ExampleQuery_Header() {
	wh, err := wavehouse.New("http://localhost:8080", wavehouse.WithHeader("X-Trace-Id", "client"))
	if err != nil {
		log.Fatal(err)
	}
	rows, err := wh.From("clicks").Header("X-Trace-Id", "abc123").
		Limit(10).Fetch(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(len(rows))
}

// ExampleRetryPolicy bounds each attempt, so a stalled one is retried instead
// of holding the call: the first attempt gets 5s, and the call's ctx still caps
// the whole of it.
func ExampleRetryPolicy() {
	wh, err := wavehouse.New("http://localhost:8080",
		wavehouse.WithRetry(wavehouse.RetryPolicy{MaxAttempts: 3, AttemptTimeout: 5 * time.Second}))
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := wh.From("clicks").Limit(10).Fetch(ctx); errors.Is(err, wavehouse.ErrAttemptTimeout) {
		fmt.Println("the last attempt timed out:", err)
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
		page, _ := row.String("page")
		score, err := row.Uint64("score") // exact, and an error rather than 0 for NULL
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(page, score)
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
		Select("page").
		GroupBy("page").
		OrderBy("views", wavehouse.Desc).
		Limit(10).
		Last("received_timestamp", 24*time.Hour).
		Scan(context.Background(), &top)
	if err != nil {
		log.Fatal(err)
	}
	for _, r := range top {
		fmt.Println(r.Page, r.Views)
	}
}

func ExampleQuery_Last() {
	wh, err := wavehouse.New("http://localhost:8080", wavehouse.WithToken(os.Getenv("WAVEHOUSE_TOKEN")))
	if err != nil {
		log.Fatal(err)
	}
	// "The last hour" is measured on the server's clock.
	rows, err := wh.From("clicks").Last("received_timestamp", time.Hour).Select("page").Count("*", "").GroupBy("page").Fetch(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	for _, r := range rows {
		n, _ := r.Uint64("count")
		fmt.Println(r["page"], n)
	}
}

func ExampleQuery_TimeRange() {
	wh, err := wavehouse.New("http://localhost:8080", wavehouse.WithToken(os.Getenv("WAVEHOUSE_TOKEN")))
	if err != nil {
		log.Fatal(err)
	}
	from := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// A zero end time leaves the window open until now.
	rows, err := wh.From("clicks").TimeRange("received_timestamp", from, time.Time{}).Limit(100).Fetch(context.Background())
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(len(rows))
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
