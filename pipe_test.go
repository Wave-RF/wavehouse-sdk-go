package wavehouse

import (
	"net/http"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPipeRequest(t *testing.T) {
	rec := newRecorder(t, reply(200, `[{"n":3}]`))
	c := rec.client(t, WithToken("jwt"), WithHeader("X-Tenant-ID", "acme"))
	rows, err := c.Pipe("top pages").Param("limit", 10).Params(map[string]any{"tags": []string{"a", "b"}, "limit": 20}).Fetch(t.Context())
	require.NoError(t, err)
	assert.Equal(t, []Row{{"n": jsonNumber("3")}}, rows)

	got := rec.last(t)
	assert.Equal(t, "POST", got.method)
	assert.Equal(t, "/v1/pipes/top pages", got.path)
	assert.Equal(t, "/v1/pipes/top%20pages", got.rawPath)
	assert.Empty(t, got.rawQuery)
	assert.Equal(t, "application/json", got.header.Get("Content-Type"))
	assert.Equal(t, "acme", got.header.Get("X-Tenant-ID"))
	assert.JSONEq(t, `{"limit":20,"tags":["a","b"]}`, got.body)
}

func TestPipeNoParamsSendsEmptyObject(t *testing.T) {
	rec := newRecorder(t, reply(200, `[]`))
	_, err := rec.client(t).Pipe("p").Fetch(t.Context())
	require.NoError(t, err)
	assert.Equal(t, `{}`, rec.last(t).body)
}

func TestPipeImmutable(t *testing.T) {
	rec := newRecorder(t, reply(200, `[]`))
	base := rec.client(t).Pipe("p").Param("a", 1)
	other := base.Param("b", 2)
	_, err := other.Fetch(t.Context())
	require.NoError(t, err)
	assert.JSONEq(t, `{"a":1,"b":2}`, rec.last(t).body)
	_, err = base.Fetch(t.Context())
	require.NoError(t, err)
	assert.JSONEq(t, `{"a":1}`, rec.last(t).body)
}

func TestPipeScan(t *testing.T) {
	rec := newRecorder(t, reply(200, `[{"page":"/a","views":18446744073709551615}]`))
	var rows []struct {
		Page  string `json:"page"`
		Views uint64 `json:"views"`
	}
	require.NoError(t, rec.client(t).Pipe("p").Scan(t.Context(), &rows))
	require.Len(t, rows, 1)
	assert.Equal(t, "/a", rows[0].Page)
	assert.Equal(t, uint64(18446744073709551615), rows[0].Views)

	require.ErrorContains(t, rec.client(t).Pipe("p").Scan(t.Context(), rows), "dst must be")
}

func TestPipeErrors(t *testing.T) {
	rec := newRecorder(t, reply(404, `{"error":"pipe not found"}`))
	_, err := rec.client(t).Pipe("nope").Fetch(t.Context())
	require.ErrorIs(t, err, ErrNotFound)

	_, err = rec.client(t).Pipe("").Fetch(t.Context())
	require.ErrorContains(t, err, "empty pipe name")
	_, err = rec.client(t).Pipe("p").Param("c", make(chan int)).Fetch(t.Context())
	require.ErrorContains(t, err, "encode parameters")
	assert.Len(t, rec.requests(), 1)
}

// A pipe may write, so the SDK retries it only when it knows nothing was sent.
func TestPipeRetryClasses(t *testing.T) {
	cases := map[string]struct {
		fail     func(*http.Request) (*http.Response, error)
		attempts int
	}{
		"transport error after send is not retried": {func(*http.Request) (*http.Response, error) { return nil, errReset }, 1},
		"dial error is retried":                     {func(*http.Request) (*http.Response, error) { return nil, errDial }, 2},
		"retryable server error is retried": {func(r *http.Request) (*http.Response, error) {
			return fakeResponse(r, 503, `{"error":"x","retryable":true}`), nil
		}, 2},
		"write pipe's non-retryable error is not": {func(r *http.Request) (*http.Response, error) {
			return fakeResponse(r, 500, `{"error":"clickhouse exec: x","code":"clickhouse.unknown","retryable":false}`), nil
		}, 1},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				attempts := 0
				c := fakeClient(t, func(r *http.Request) (*http.Response, error) {
					if attempts++; attempts == 1 {
						return tc.fail(r)
					}
					return fakeResponse(r, 200, `[]`), nil
				})
				_, _ = c.Pipe("p").Fetch(t.Context())
				assert.Equal(t, tc.attempts, attempts)
			})
		})
	}
}

func TestPipeExec(t *testing.T) {
	rec := newRecorder(t, reply(200, `[]`))
	require.NoError(t, rec.client(t).Pipe("purge").Param("older_than", 7).Exec(t.Context()))
	got := rec.last(t)
	assert.Equal(t, "/v1/pipes/purge", got.path)
	assert.JSONEq(t, `{"older_than":7}`, got.body)

	require.ErrorContains(t, rec.client(t).Pipe("").Exec(t.Context()), "empty pipe name")
	bad := newRecorder(t, reply(404, `{"error":"pipe not found"}`))
	require.ErrorIs(t, bad.client(t).Pipe("nope").Exec(t.Context()), ErrNotFound)
}
