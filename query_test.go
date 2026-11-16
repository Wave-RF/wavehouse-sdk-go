package wavehouse

import (
	"encoding/json"
	"math"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sentBody decodes the last request's JSON body.
func sentBody(t *testing.T, rec *recorder) map[string]any {
	t.Helper()
	var m map[string]any
	require.NoError(t, json.Unmarshal([]byte(rec.last(t).body), &m))
	return m
}

func TestQueryRequest(t *testing.T) {
	rec := newRecorder(t, reply(200, `[]`))
	c := rec.client(t)
	_, err := c.From("my table").Select("a").Fetch(t.Context())
	require.NoError(t, err)
	got := rec.last(t)
	assert.Equal(t, "POST", got.method)
	assert.Equal(t, "/v1/query", got.path)
	assert.Equal(t, "table=my+table", got.rawQuery)
	assert.Equal(t, "application/json", got.header.Get("Content-Type"))
}

func TestQueryProjection(t *testing.T) {
	cases := map[string]struct {
		q    func(*Client) *Query
		want map[string]any
	}{
		"default is select_all": {func(c *Client) *Query { return c.From("t") }, map[string]any{"select_all": true}},
		"empty Select is select_all": {
			func(c *Client) *Query { return c.From("t").Select() }, map[string]any{"select_all": true},
		},
		"columns accumulate": {
			func(c *Client) *Query { return c.From("t").Select("a").Select("b", "c") },
			map[string]any{"columns": []any{"a", "b", "c"}},
		},
		"aggregate alone has no select_all": {
			func(c *Client) *Query { return c.From("t").Count("*", "n") },
			map[string]any{"aggregations": []any{map[string]any{"fn": "count", "column": "*", "alias": "n"}}},
		},
		"explicit SelectAll": {func(c *Client) *Query { return c.From("t").SelectAll() }, map[string]any{"select_all": true}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rec := newRecorder(t, reply(200, `[]`))
			_, err := tc.q(rec.client(t)).Fetch(t.Context())
			require.NoError(t, err)
			assert.Equal(t, tc.want, sentBody(t, rec))
		})
	}
}

func TestQueryOpAndDirSpellings(t *testing.T) {
	for op, want := range map[Op]string{
		Eq: "eq", Neq: "neq", Gt: "gt", Gte: "gte", Lt: "lt", Lte: "lte", In: "in", Like: "like",
	} {
		rec := newRecorder(t, reply(200, `[]`))
		_, err := rec.client(t).From("t").Where("x", op, 1).Fetch(t.Context())
		require.NoError(t, err)
		assert.Equal(t, []any{map[string]any{"column": "x", "op": want, "value": float64(1)}}, sentBody(t, rec)["filters"], op)
	}
	for dir, want := range map[Dir]string{Asc: "asc", Desc: "desc", "": "asc"} {
		rec := newRecorder(t, reply(200, `[]`))
		_, err := rec.client(t).From("t").OrderBy("x", dir).Fetch(t.Context())
		require.NoError(t, err)
		assert.Equal(t, []any{map[string]any{"column": "x", "dir": want}}, sentBody(t, rec)["order_by"], dir)
	}
}

func TestQueryAggregates(t *testing.T) {
	rec := newRecorder(t, reply(200, `[]`))
	q := rec.client(t).From("t").
		Count("*", "").Sum("a", "").Avg("b", "").Min("c", "").Max("d", "").CountDistinct("e", "").
		Sum("a", "total").Aggregate("uniqExact", "u", "").GroupBy("g", "h")
	_, err := q.Fetch(t.Context())
	require.NoError(t, err)
	agg := func(fn, col, alias string) any { return map[string]any{"fn": fn, "column": col, "alias": alias} }
	body := sentBody(t, rec)
	assert.Equal(t, []any{
		agg("count", "*", "count"), agg("sum", "a", "sum_a"), agg("avg", "b", "avg_b"),
		agg("min", "c", "min_c"), agg("max", "d", "max_d"), agg("countDistinct", "e", "count_distinct_e"),
		agg("sum", "a", "total"), agg("uniqExact", "u", ""),
	}, body["aggregations"])
	assert.Equal(t, []any{"g", "h"}, body["group_by"])
}

func TestQueryTimeRangeLimitFilters(t *testing.T) {
	rec := newRecorder(t, reply(200, `[]`))
	ts := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	_, err := rec.client(t).From("t").
		Where("a", In, []string{"x", "y"}).Where("b", Gte, ts).Where("c", Like, "p%").
		TimeRange("ts", "1h", "").Limit(50).Fetch(t.Context())
	require.NoError(t, err)
	body := sentBody(t, rec)
	assert.Equal(t, []any{
		map[string]any{"column": "a", "op": "in", "value": []any{"x", "y"}},
		map[string]any{"column": "b", "op": "gte", "value": "2026-01-02T03:04:05Z"},
		map[string]any{"column": "c", "op": "like", "value": "p%"},
	}, body["filters"])
	assert.Equal(t, map[string]any{"column": "ts", "since": "1h"}, body["time_range"])
	assert.InDelta(t, 50, body["limit"], 0)

	_, err = rec.client(t).From("t").TimeRange("ts", "2026-01-01", "2026-02-01").Fetch(t.Context())
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"column": "ts", "since": "2026-01-01", "until": "2026-02-01"}, sentBody(t, rec)["time_range"])

	// 0 sends no limit, leaving the cap to the server.
	_, err = rec.client(t).From("t").Limit(0).Fetch(t.Context())
	require.NoError(t, err)
	assert.NotContains(t, sentBody(t, rec), "limit")
}

func TestQueryImmutable(t *testing.T) {
	rec := newRecorder(t, reply(200, `[]`))
	base := rec.client(t).From("t").Select("a")
	left := base.Select("b").Where("x", Eq, 1)
	right := base.Select("c")
	_, err := left.Fetch(t.Context())
	require.NoError(t, err)
	assert.Equal(t, []any{"a", "b"}, sentBody(t, rec)["columns"])
	_, err = right.Fetch(t.Context())
	require.NoError(t, err)
	body := sentBody(t, rec)
	assert.Equal(t, []any{"a", "c"}, body["columns"])
	assert.NotContains(t, body, "filters")
	_, err = base.Fetch(t.Context())
	require.NoError(t, err)
	assert.Equal(t, []any{"a"}, sentBody(t, rec)["columns"])
}

func TestQueryMisuse(t *testing.T) {
	rec := newRecorder(t, reply(200, `[]`))
	c := rec.client(t)
	cases := map[string]struct {
		q    *Query
		want string
	}{
		"select all with columns":    {c.From("t").SelectAll().Select("a"), "SelectAll can't be combined"},
		"select all with aggregates": {c.From("t").Count("*", "n").SelectAll(), "SelectAll can't be combined"},
		"empty select column":        {c.From("t").Select("a", ""), "Select: empty column name"},
		"empty where column":         {c.From("t").Where("", Eq, 1), "Where: empty column name"},
		"empty order column":         {c.From("t").OrderBy("", Asc), "OrderBy: empty column name"},
		"empty group column":         {c.From("t").GroupBy(""), "GroupBy: empty column name"},
		"empty time column":          {c.From("t").TimeRange("", "1h", ""), "TimeRange: empty column name"},
		"empty aggregate column":     {c.From("t").Sum("", ""), "Aggregate: empty column name"},
		"empty aggregate fn":         {c.From("t").Aggregate("", "a", ""), "empty function name"},
		"negative limit":             {c.From("t").Limit(-1), "negative limit"},
		"empty table":                {c.From(""), "empty table name"},
		"unencodable value":          {c.From("t").Where("a", Eq, math.NaN()), "encode filter values"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := tc.q.Fetch(t.Context())
			require.ErrorContains(t, err, tc.want)
			// The first misuse wins, and nothing was sent.
			assert.Empty(t, rec.requests())
		})
	}
	_, err := c.From("t").Select("").Limit(-1).Fetch(t.Context())
	require.ErrorContains(t, err, "Select: empty column name")
}

func TestScanDestination(t *testing.T) {
	rec := newRecorder(t, reply(200, `[]`))
	q := rec.client(t).From("t")
	var (
		structs []struct{ A int }
		ptrs    []*struct{ A int }
		maps    []map[string]any
		ints    []int
		notNil  *[]Row
	)
	for name, dst := range map[string]any{
		"nil": nil, "not a pointer": structs, "pointer to struct": &struct{}{}, "nil pointer": notNil,
	} {
		require.ErrorContains(t, q.Scan(t.Context(), dst), "dst must be", name)
	}
	require.ErrorContains(t, q.Scan(t.Context(), &ints), "elements must be structs or maps")
	assert.Empty(t, rec.requests(), "a bad dst is refused before sending")
	require.NoError(t, q.Scan(t.Context(), &structs))
	require.NoError(t, q.Scan(t.Context(), &ptrs))
	require.NoError(t, q.Scan(t.Context(), &maps))
}

func TestScanIntoStructs(t *testing.T) {
	rec := newRecorder(t, reply(200, `[
		{"id":18446744073709551615,"signed":-9223372036854775808,"price":"12345678901234567890.123456789","name":"a","ts":"2026-01-02T03:04:05.123Z","extra":1},
		{"id":9007199254740993,"signed":9007199254740993,"price":"0.5","name":"b","ts":"2026-01-02T03:04:06Z"}
	]`))
	type row struct {
		ID     uint64    `json:"id"`
		Signed int64     `json:"signed"`
		Price  string    `json:"price"`
		Name   string    `json:"name"`
		TS     time.Time `json:"ts"`
	}
	var rows []row
	require.NoError(t, rec.client(t).From("t").Scan(t.Context(), &rows))
	require.Len(t, rows, 2)
	assert.Equal(t, uint64(math.MaxUint64), rows[0].ID)
	assert.Equal(t, int64(math.MinInt64), rows[0].Signed)
	assert.Equal(t, "12345678901234567890.123456789", rows[0].Price)
	assert.Equal(t, time.Date(2026, 1, 2, 3, 4, 5, 123_000_000, time.UTC), rows[0].TS)
	assert.Equal(t, uint64(9007199254740993), rows[1].ID) // 2^53 + 1
	assert.Equal(t, int64(9007199254740993), rows[1].Signed)
}

func TestFetchKeepsNumbersExact(t *testing.T) {
	rec := newRecorder(t, reply(200, `[{"id":18446744073709551615,"f":1.5,"s":"x","n":null}]`))
	rows, err := rec.client(t).From("t").Fetch(t.Context())
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, json.Number("18446744073709551615"), rows[0]["id"])
	assert.Equal(t, json.Number("1.5"), rows[0]["f"])
	assert.Equal(t, "x", rows[0]["s"])
	assert.Nil(t, rows[0]["n"])
	_, err = rows[0]["id"].(json.Number).Int64()
	require.Error(t, err, "doesn't fit int64, so a float64 would have rounded it")
}

func TestFetchEmptyAndMalformed(t *testing.T) {
	rows, err := newRecorder(t, reply(200, `[]`)).client(t).From("t").Fetch(t.Context())
	require.NoError(t, err)
	assert.NotNil(t, rows)
	assert.Empty(t, rows)

	_, err = newRecorder(t, reply(200, `not json`)).client(t).From("t").Fetch(t.Context())
	require.ErrorContains(t, err, "POST /v1/query: decode response")
}

func TestQueryAPIError(t *testing.T) {
	rec := newRecorder(t, reply(400, `{"error":"unknown column \"zz\"","code":"clickhouse.rejected","retryable":false}`, "X-Request-Id", "req-1"))
	_, err := rec.client(t).From("t").Select("zz").Fetch(t.Context())
	require.ErrorIs(t, err, ErrBadRequest)
	assert.EqualError(t, err, `wavehouse: POST /v1/query: 400 clickhouse.rejected: unknown column "zz" (request req-1)`)
	assert.Len(t, rec.requests(), 1)
}

func TestQueryTenantAndAuth(t *testing.T) {
	rec := newRecorder(t, reply(200, `[]`))
	_, err := rec.client(t, WithToken("jwt")).WithTenant("acme").From("t").Fetch(t.Context())
	require.NoError(t, err)
	got := rec.last(t)
	assert.Equal(t, "acme", got.header.Get("X-Tenant-ID"))
	assert.Equal(t, "Bearer jwt", got.header.Get("Authorization"))
	assert.Equal(t, "table=t", got.rawQuery)
}

func TestQueryRetriesRead(t *testing.T) {
	calls := 0
	rec := newRecorder(t, func(w http.ResponseWriter, r *http.Request) {
		if calls++; calls == 1 {
			reply(503, `{"error":"busy"}`, "Retry-After", "0")(w, r)
			return
		}
		reply(200, `[{"a":1}]`)(w, r)
	})
	c, err := New(rec.URL, WithRetry(RetryPolicy{MinBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond}))
	require.NoError(t, err)
	rows, err := c.From("t").Fetch(t.Context())
	require.NoError(t, err)
	assert.Len(t, rows, 1)
	assert.Len(t, rec.requests(), 2)
}
