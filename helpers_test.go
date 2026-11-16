package wavehouse

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// seen is one request a test server received.
type seen struct {
	method, path, rawPath, rawQuery string
	header                          http.Header
	body                            string
}

// recorder is an httptest server that records requests and answers with
// respond, which defaults to 200 "{}".
type recorder struct {
	*httptest.Server
	mu   sync.Mutex
	reqs []seen
}

func newRecorder(t *testing.T, respond http.HandlerFunc) *recorder {
	t.Helper()
	rec := &recorder{}
	rec.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		rec.mu.Lock()
		rec.reqs = append(rec.reqs, seen{
			method: r.Method, path: r.URL.Path, rawPath: r.URL.EscapedPath(), rawQuery: r.URL.RawQuery,
			header: r.Header.Clone(), body: string(body),
		})
		rec.mu.Unlock()
		if respond == nil {
			_, _ = io.WriteString(w, "{}")
			return
		}
		respond(w, r)
	}))
	t.Cleanup(rec.Close)
	return rec
}

func (rec *recorder) requests() []seen {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return append([]seen(nil), rec.reqs...)
}

func (rec *recorder) last(t *testing.T) seen {
	t.Helper()
	reqs := rec.requests()
	require.NotEmpty(t, reqs, "server received no request")
	return reqs[len(reqs)-1]
}

// client returns a client for rec that never retries unless opts say otherwise.
func (rec *recorder) client(t *testing.T, opts ...Option) *Client {
	t.Helper()
	c, err := New(rec.URL, append([]Option{WithRetry(RetryPolicy{MaxAttempts: 1})}, opts...)...)
	require.NoError(t, err)
	return c
}

// reply writes a JSON answer with status and headers given as key, value pairs.
func reply(status int, body string, kv ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		for i := 0; i+1 < len(kv); i += 2 {
			w.Header().Set(kv[i], kv[i+1])
		}
		if body != "" {
			w.Header().Set("Content-Type", "application/json")
		}
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}
}

// roundTripFunc fakes the network for synctest bubbles, where a real socket
// would never let fake time advance.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func fakeResponse(r *http.Request, status int, body string, kv ...string) *http.Response {
	h := http.Header{}
	for i := 0; i+1 < len(kv); i += 2 {
		h.Set(kv[i], kv[i+1])
	}
	return &http.Response{
		StatusCode: status, Status: http.StatusText(status), Header: h, Request: r,
		Body: io.NopCloser(strings.NewReader(body)), ProtoMajor: 1, ProtoMinor: 1,
	}
}

// fakeClient returns a client whose requests go to rt.
func fakeClient(t *testing.T, rt roundTripFunc, opts ...Option) *Client {
	t.Helper()
	c, err := New("http://wavehouse.test", append([]Option{WithHTTPClient(&http.Client{Transport: rt})}, opts...)...)
	require.NoError(t, err)
	return c
}
