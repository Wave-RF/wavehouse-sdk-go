package wavehouse

import (
	"encoding/json"
	"math"
	"net/http"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func jsonNumber(s string) json.Number { return json.Number(s) }

func TestSQLRequest(t *testing.T) {
	rec := newRecorder(t, reply(200, `[{"n":1}]`))
	c := rec.client(t, WithOperatorKey("k"))
	rows, err := c.SQL("SELECT 1 AS n").Fetch(t.Context())
	require.NoError(t, err)
	assert.Equal(t, []Row{{"n": jsonNumber("1")}}, rows)

	got := rec.last(t)
	assert.Equal(t, "POST", got.method)
	assert.Equal(t, "/v1/ops/query", got.path)
	assert.Empty(t, got.rawQuery)
	assert.Equal(t, "Operator k", got.header.Get("Authorization"))
	assert.Equal(t, "application/json", got.header.Get("Content-Type"))
	assert.JSONEq(t, `{"sql":"SELECT 1 AS n"}`, got.body)
}

func TestSQLScanExactUInt64(t *testing.T) {
	rec := newRecorder(t, reply(200, `[{"u":18446744073709551615,"i":-9223372036854775808}]`))
	var rows []struct {
		U uint64 `json:"u"`
		I int64  `json:"i"`
	}
	require.NoError(t, rec.client(t).SQL("SELECT ...").Scan(t.Context(), &rows))
	require.Len(t, rows, 1)
	assert.Equal(t, uint64(math.MaxUint64), rows[0].U)
	assert.Equal(t, int64(math.MinInt64), rows[0].I)

	require.ErrorContains(t, rec.client(t).SQL("SELECT 1").Scan(t.Context(), &rows[0]), "dst must be")
}

func TestSQLExec(t *testing.T) {
	// DDL answers 200 with an empty array; a FORMAT clause answers with something that isn't JSON.
	for name, body := range map[string]string{"empty array": `[]`, "csv": "1,2\n"} {
		rec := newRecorder(t, reply(200, body))
		require.NoError(t, rec.client(t).SQL("CREATE TABLE x (a UInt8) ENGINE = Memory").Exec(t.Context()), name)
		assert.Len(t, rec.requests(), 1)
	}
}

func TestSQLNonJSONBody(t *testing.T) {
	rec := newRecorder(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/csv")
		_, _ = w.Write([]byte("1,2\n3,4\n"))
	})
	rows, err := rec.client(t).SQL("SELECT 1, 2 FORMAT CSV").Fetch(t.Context())
	require.Error(t, err)
	assert.Nil(t, rows)
	assert.ErrorContains(t, err, `"text/csv", not a JSON array of rows`)
	assert.ErrorContains(t, err, "FORMAT")
	assert.ErrorContains(t, err, "Exec")

	_, err = newRecorder(t, reply(200, `[{"a":`)).client(t).SQL("SELECT 1").Fetch(t.Context())
	require.ErrorContains(t, err, "decode response")
}

func TestSQLMisuse(t *testing.T) {
	rec := newRecorder(t, nil)
	require.ErrorContains(t, rec.client(t).SQL(" \n").Exec(t.Context()), "empty statement")
	_, err := rec.client(t).SQL("").Fetch(t.Context())
	require.ErrorContains(t, err, "empty statement")
	assert.Empty(t, rec.requests())
}

func TestSQLAPIError(t *testing.T) {
	rec := newRecorder(t, reply(403, `{"error":"forbidden"}`))
	require.ErrorIs(t, rec.client(t).SQL("SELECT 1").Exec(t.Context()), ErrForbidden)
}

// Raw SQL may mutate: a server error is never retried, and a transport error only when nothing was sent.
func TestSQLRetryClasses(t *testing.T) {
	cases := map[string]struct {
		fail     func(*http.Request) (*http.Response, error)
		attempts int
	}{
		"transport error after send": {func(r *http.Request) (*http.Response, error) { return afterSend(r, errReset) }, 1},
		"dial error":                 {func(*http.Request) (*http.Response, error) { return nil, errDial }, 2},
		"retryable APIError": {func(r *http.Request) (*http.Response, error) {
			return fakeResponse(r, 503, `{"error":"x","code":"clickhouse.unavailable","retryable":true}`, "Retry-After", "5"), nil
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
				_ = c.SQL("DROP TABLE x").Exec(t.Context())
				assert.Equal(t, tc.attempts, attempts)
			})
		})
	}
}
