package wavehouse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAuthHeaders(t *testing.T) {
	cases := map[string]struct {
		opts []Option
		want []string // Authorization values received
	}{
		"bearer token":      {[]Option{WithToken("jwt")}, []string{"Bearer jwt"}},
		"operator key":      {[]Option{WithOperatorKey("k3y")}, []string{"Operator k3y"}},
		"none by default":   {nil, nil},
		"source empty":      {[]Option{WithTokenSource(func(context.Context) (string, error) { return "", nil })}, nil},
		"custom is dropped": {[]Option{WithHeader("Authorization", "Basic smuggled")}, nil},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rec := newRecorder(t, nil)
			_, err := rec.client(t, tc.opts...).DLQStats(t.Context())
			require.NoError(t, err)
			assert.Equal(t, tc.want, rec.last(t).header.Values("Authorization"))
		})
	}
}

func TestTokenSourcePerRequest(t *testing.T) {
	rec := newRecorder(t, nil)
	var calls atomic.Int32
	c := rec.client(t, WithTokenSource(func(context.Context) (string, error) {
		return fmt.Sprintf("tok-%d", calls.Add(1)), nil
	}))
	for i := 1; i <= 2; i++ {
		require.NoError(t, c.Health(t.Context()))
		assert.Equal(t, fmt.Sprintf("Bearer tok-%d", i), rec.last(t).header.Get("Authorization"))
	}
}

func TestTokenSourceError(t *testing.T) {
	rec := newRecorder(t, nil)
	boom := errors.New("idp down")
	err := rec.client(t, WithTokenSource(func(context.Context) (string, error) { return "", boom })).Health(t.Context())
	require.ErrorIs(t, err, boom)
	assert.Contains(t, err.Error(), "wavehouse: GET /v1/health: token source")
	assert.Empty(t, rec.requests(), "sent despite the token source failing")
	assert.False(t, IsRetryable(err))
}

func TestSDKHeadersWin(t *testing.T) {
	rec := newRecorder(t, nil)
	c := rec.client(t, WithToken("jwt"), WithUserAgent("grafana-plugin/1.0"),
		WithHeader("X-Custom", "a"), WithHeader("x-custom", "b"),
		WithHeader("Authorization", "Basic nope"), WithHeader("User-Agent", "nope"),
		WithHeader("X-Tenant-ID", "nope"), WithHeader("Content-Type", "text/nope"), WithHeader("Accept", "nope"))
	_, err := c.DLQStats(t.Context())
	require.NoError(t, err)
	h := rec.last(t).header
	assert.Equal(t, []string{"b"}, h.Values("X-Custom"))
	assert.Equal(t, "Bearer jwt", h.Get("Authorization"))
	assert.Equal(t, "grafana-plugin/1.0 wavehouse-sdk-go/devel", h.Get("User-Agent"))
	assert.Equal(t, "application/json", h.Get("Accept"))
	assert.Empty(t, h.Values("X-Tenant-ID"))
	assert.Empty(t, h.Values("Content-Type"))
}

func TestTenantPlacement(t *testing.T) {
	rec := newRecorder(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/health" {
			_, _ = io.WriteString(w, "{}")
		}
	})
	c := rec.client(t, WithTenant("acme"))

	cases := []struct {
		name               string
		call               func(*Client) error
		wantHeader, wantQS string
	}{
		{"data route", func(c *Client) error { return c.Health(t.Context()) }, "acme", ""},
		{"ops route", func(c *Client) error { _, err := c.DLQStats(t.Context()); return err }, "", "tenant=acme"},
		{"ops route with query", func(c *Client) error { _, err := c.Table(t.Context(), "clicks"); return err }, "", "table=clicks&tenant=acme"},
		{"tenant-exempt", func(c *Client) error { _, err := c.Version(t.Context()); return err }, "", ""},
	}
	for _, tc := range cases {
		require.NoError(t, tc.call(c), tc.name)
		got := rec.last(t)
		assert.Equal(t, tc.wantHeader, got.header.Get("X-Tenant-ID"), tc.name)
		assert.Equal(t, tc.wantQS, got.rawQuery, tc.name)
	}

	// Tenant 0 sends neither form.
	_, err := rec.client(t).DLQStats(t.Context())
	require.NoError(t, err)
	assert.Empty(t, rec.last(t).rawQuery)
	assert.Empty(t, rec.last(t).header.Values("X-Tenant-ID"))
}

func TestTenantDoesNotMutateQuery(t *testing.T) {
	c, err := New("http://h", WithTenant("acme"))
	require.NoError(t, err)
	q := url.Values{"table": {"t"}}
	assert.Equal(t, "http://h/v1/ops/schema?table=t&tenant=acme", c.url(&request{path: "/v1/ops/schema", query: q}))
	assert.False(t, q.Has("tenant"), "tenant written into the caller's query")
}

func TestRouteClassification(t *testing.T) {
	for path, want := range map[string][2]bool{
		"/v1/ops":          {true, false},
		"/v1/ops/schema":   {true, false},
		"/v1/ops/pipes/xy": {true, false},
		"/v1/opsx":         {false, true},
		"/v1/query":        {false, true},
		"/v1/stream":       {false, true},
		"/v1/pipes/a":      {false, true},
		"/v1/ingest":       {false, true},
		"/version":         {false, false},
		"/v1":              {false, false},
	} {
		assert.Equal(t, want, [2]bool{isOpsRoute(path), isDataRoute(path)}, path)
	}
}

var (
	errDial  = &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}
	errReset = &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset by peer")}
)

// TestRetryClassification checks the spec's table: which failure each call
// kind retries.
func TestRetryClassification(t *testing.T) {
	respond := func(status int, body string) roundTripFunc {
		return func(r *http.Request) (*http.Response, error) { return fakeResponse(r, status, body), nil }
	}
	failures := map[string]roundTripFunc{
		"dial error":          func(*http.Request) (*http.Response, error) { return nil, errDial },
		"transport error":     func(*http.Request) (*http.Response, error) { return nil, errReset },
		"plain 503":           respond(503, `{"error":"x"}`),
		"503 server says no":  respond(503, `{"error":"x","retryable":false}`),
		"plain 400":           respond(400, `{"error":"x"}`),
		"bare 429":            respond(429, ``),
		"500 server says yes": respond(500, `{"error":"x","retryable":true}`),
		"503 Retry-After": func(r *http.Request) (*http.Response, error) {
			res := fakeResponse(r, 503, `{"error":"x"}`)
			res.Header.Set("Retry-After", "5")
			return res, nil
		},
		"proxy 504": respond(504, `<html>gateway timeout</html>`),
	}
	// Whether each call kind makes a second attempt after each failure.
	retries := map[callKind]map[string]bool{
		callRead:   {"dial error": true, "transport error": true, "plain 503": true, "503 server says no": false, "plain 400": false, "bare 429": true, "500 server says yes": true, "503 Retry-After": true, "proxy 504": true},
		callIngest: {"dial error": true, "transport error": true, "plain 503": true, "503 server says no": false, "plain 400": false, "bare 429": true, "500 server says yes": true, "503 Retry-After": true, "proxy 504": true},
		// A write pipe may have run behind a bare 5xx, so only the server's own verdict, 429 or 503+Retry-After resend it.
		callPipe: {"dial error": true, "transport error": false, "plain 503": false, "503 server says no": false, "plain 400": false, "bare 429": true, "500 server says yes": true, "503 Retry-After": true, "proxy 504": false},
		callSQL:  {"dial error": true, "transport error": false, "plain 503": false, "503 server says no": false, "plain 400": false, "bare 429": false, "500 server says yes": false, "503 Retry-After": false, "proxy 504": false},
	}
	for kind, row := range retries {
		for name, fail := range failures {
			t.Run(fmt.Sprintf("kind%d/%s", kind, name), func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					attempts := 0
					c := fakeClient(t, func(r *http.Request) (*http.Response, error) {
						if attempts++; attempts == 1 {
							return fail(r)
						}
						return fakeResponse(r, 200, `{}`), nil
					})
					_, err := c.do(t.Context(), &request{kind: kind, method: http.MethodPost, path: "/v1/x", body: []byte("{}"), contentType: "application/json"})
					require.Equal(t, row[name], attempts > 1, "err %v", err)
					if attempts > 1 {
						require.NoError(t, err)
					}
				})
			})
		}
	}
}

func TestRetryResendsIdenticalBody(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var bodies []string
		c := fakeClient(t, func(r *http.Request) (*http.Response, error) {
			b, _ := io.ReadAll(r.Body)
			bodies = append(bodies, string(b))
			if len(bodies) < 3 {
				return nil, errReset
			}
			return fakeResponse(r, 200, `{}`), nil
		})
		_, err := c.do(t.Context(), &request{kind: callIngest, method: http.MethodPost, path: "/v1/ingest", body: []byte(`{"a":1}`), contentType: "application/x-ndjson"})
		require.NoError(t, err)
		assert.Equal(t, []string{`{"a":1}`, `{"a":1}`, `{"a":1}`}, bodies)
	})
}

func TestBackoffTiming(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var at []time.Time
		c := fakeClient(t, func(*http.Request) (*http.Response, error) {
			at = append(at, time.Now())
			return nil, errDial
		}, WithRetry(RetryPolicy{MaxAttempts: 5, MinBackoff: time.Second, MaxBackoff: 5 * time.Second}))
		err := c.Health(t.Context())
		require.ErrorIs(t, err, errDial)
		assert.True(t, IsRetryable(err))
		require.Len(t, at, 5)
		// 1s, 2s, 4s each ±20%, then 8s capped to 5s.
		for i, want := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second} {
			assert.InEpsilon(t, float64(want), float64(at[i+1].Sub(at[i])), 0.2, "wait %d", i+1)
		}
		assert.Equal(t, 5*time.Second, at[4].Sub(at[3]))
	})
}

func TestRetryAfter(t *testing.T) {
	cases := map[string]struct {
		header string // "" means an HTTP-date 10s ahead, built inside the bubble
		want   time.Duration
	}{
		"seconds":   {"7", 7 * time.Second},
		"http date": {"", 10 * time.Second},
		"capped":    {"3600", 30 * time.Second},
		"zero":      {"0", 0}, // falls back to the backoff
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				header := tc.header
				if header == "" {
					header = time.Now().Add(10 * time.Second).UTC().Format(http.TimeFormat)
				}
				var at []time.Time
				c := fakeClient(t, func(r *http.Request) (*http.Response, error) {
					if at = append(at, time.Now()); len(at) == 1 {
						return fakeResponse(r, 503, `{"error":"busy"}`, "Retry-After", header), nil
					}
					return fakeResponse(r, 200, `{}`), nil
				}, WithRetry(RetryPolicy{MinBackoff: 100 * time.Millisecond}))
				_, err := c.DLQStats(t.Context())
				require.NoError(t, err)
				if tc.want == 0 {
					assert.InEpsilon(t, float64(100*time.Millisecond), float64(at[1].Sub(at[0])), 0.2)
					return
				}
				assert.Equal(t, tc.want, at[1].Sub(at[0]))
			})
		})
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for in, want := range map[string]time.Duration{
		"":                              0,
		"abc":                           0,
		"-3":                            0,
		"5":                             5 * time.Second,
		"99999999999999999":             time.Duration(1<<63 - 1),
		"Thu, 01 Jan 2026 00:00:30 GMT": 30 * time.Second,
		"Wed, 31 Dec 2025 23:00:00 GMT": 0,
	} {
		assert.Equal(t, want, parseRetryAfter(in, now), in)
	}
}

func TestCancelStopsWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		attempts := 0
		c := fakeClient(t, func(r *http.Request) (*http.Response, error) {
			attempts++
			return fakeResponse(r, 503, `{"error":"busy"}`, "Retry-After", "20"), nil
		})
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		start := time.Now()
		err := c.Health(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Equal(t, time.Second, time.Since(start))
		assert.Equal(t, 1, attempts)
		assert.Contains(t, err.Error(), "busy", "does not say what it was waiting on")
		_, isAPI := errors.AsType[*APIError](err)
		assert.False(t, isAPI, "an abandoned call still matches its last APIError")
		assert.False(t, IsRetryable(err))
	})
}

func TestCancelledBeforeSend(t *testing.T) {
	rec := newRecorder(t, nil)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err := rec.client(t, WithRetry(RetryPolicy{MaxAttempts: 3})).Health(ctx)
	require.ErrorIs(t, err, context.Canceled)
	_, isURL := errors.AsType[*url.Error](err)
	assert.True(t, isURL, "does not wrap *url.Error")
	assert.True(t, strings.HasPrefix(err.Error(), "wavehouse: GET /v1/health: "), err.Error())
	assert.False(t, IsRetryable(err))
}

func TestRedirects(t *testing.T) {
	var elsewhere atomic.Int32
	target := newRecorder(t, func(http.ResponseWriter, *http.Request) { elsewhere.Add(1) })
	redirecting := newRecorder(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/v1/health", http.StatusFound)
	})

	for name, opt := range map[string]Option{
		"token":         WithToken("jwt"),
		"operator key":  WithOperatorKey("k"),
		"custom header": WithHeader("X-Operator-Key", "k"),
		"token source":  WithTokenSource(func(context.Context) (string, error) { return "jwt", nil }),
	} {
		err := redirecting.client(t, opt).Health(t.Context())
		require.ErrorIs(t, err, ErrRedirect, name)
		assert.Contains(t, err.Error(), "302 redirect to \""+target.URL+"/v1/health\"", name)
		assert.False(t, IsRetryable(err), name)
	}
	require.Zero(t, elsewhere.Load(), "credentialed requests reached the redirect target")

	// Without a credential the caller's redirect policy applies.
	require.NoError(t, redirecting.client(t).Health(t.Context()))
	assert.Equal(t, int32(1), elsewhere.Load())
}

func TestUseNumber(t *testing.T) {
	rec := newRecorder(t, reply(200, `{"big":9007199254740993,"f":1.5}`))
	var dst map[string]any
	require.NoError(t, rec.client(t).doJSON(t.Context(), &request{method: http.MethodGet, path: "/v1/x"}, &dst))
	assert.Equal(t, map[string]any{"big": json.Number("9007199254740993"), "f": json.Number("1.5")}, dst)
}

func TestDecodeFailure(t *testing.T) {
	rec := newRecorder(t, reply(200, `<html>`))
	_, err := rec.client(t).Version(t.Context())
	require.ErrorContains(t, err, "wavehouse: GET /version: decode response")
	assert.False(t, IsRetryable(err))
}

func TestBodyReadFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		attempts := 0
		c := fakeClient(t, func(r *http.Request) (*http.Response, error) {
			attempts++
			res := fakeResponse(r, 200, "")
			res.Body = io.NopCloser(io.MultiReader(strings.NewReader(`{"vers`), errReader{errReset}))
			return res, nil
		}, WithRetry(RetryPolicy{MaxAttempts: 2}))
		_, err := c.Version(t.Context())
		require.ErrorIs(t, err, errReset)
		assert.Equal(t, 2, attempts, "a read is retried after a broken body")
	})
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

func TestBadMethod(t *testing.T) {
	c, err := New("http://h")
	require.NoError(t, err)
	_, err = c.do(t.Context(), &request{method: "BAD METHOD", path: "/v1/x"})
	assert.Error(t, err)
}

// A refused connection from the real transport, not a hand-built error, so a
// change in how Go shapes it would fail here instead of silently disabling
// dial retries for pipes and SQL.
func TestRealRefusedConnectionIsADialError(t *testing.T) {
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())

	c, err := New("http://"+addr, WithRetry(RetryPolicy{MaxAttempts: 1}))
	require.NoError(t, err)
	err = c.SQL("SELECT 1").Exec(t.Context())
	require.Error(t, err)
	assert.True(t, IsRetryable(err), "a refused connection never reached the server: %v", err)
}
