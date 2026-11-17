package e2e

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	wavehouse "github.com/Wave-RF/wavehouse-sdk-go"
)

// countRows is how many e2e_events rows carry name.
func countRows(t *testing.T, c *wavehouse.Client, name string) uint64 {
	t.Helper()
	var out []struct {
		N uint64 `json:"n"`
	}
	require.NoError(t, c.From("e2e_events").Where("name", wavehouse.Eq, name).Count("*", "n").Scan(t.Context(), &out))
	require.Len(t, out, 1)
	return out[0].N
}

// waitRows waits for the server's batch consumer to land n rows named name in ClickHouse.
func waitRows(t *testing.T, c *wavehouse.Client, name string, n uint64) {
	t.Helper()
	require.Eventually(t, func() bool { return countRows(t, c, name) == n }, 30*time.Second, 200*time.Millisecond,
		"rows named %q never reached %d", name, n)
}

func TestInsert(t *testing.T) {
	c := newClient(t)

	t.Run("one row", func(t *testing.T) {
		name := uniqueName(t)
		res, err := c.From("e2e_events").Insert(t.Context(), eventRow(name, 1.5))
		require.NoError(t, err)
		assert.Equal(t, 1, res.Total)
		assert.Equal(t, 1, res.Succeeded)
		assert.Equal(t, []wavehouse.RecordResult{{Index: 1, OK: true}}, res.Records)
		waitRows(t, c, name, 1)
	})

	t.Run("batch of structs", func(t *testing.T) {
		type event struct {
			EventID string  `json:"event_id"`
			Name    string  `json:"name"`
			Value   float64 `json:"value"`
		}
		name := uniqueName(t)
		rows := []event{{name + "-a", name, 1}, {name + "-b", name, 2}, {name + "-c", name, 3}}
		res, err := c.From("e2e_events").Insert(t.Context(), rows)
		require.NoError(t, err)
		assert.Equal(t, 3, res.Succeeded)
		waitRows(t, c, name, 3)
	})

	t.Run("duplicate dedupe id", func(t *testing.T) {
		name := uniqueName(t)
		row := eventRow(name, 1)
		first, err := c.From("e2e_events").Insert(t.Context(), row)
		require.NoError(t, err)
		assert.Equal(t, 1, first.Succeeded)

		again, err := c.From("e2e_events").Insert(t.Context(), []map[string]any{row, eventRow(name, 2)})
		require.NoError(t, err, "a duplicate is not a failure")
		assert.Equal(t, 1, again.Duplicates)
		assert.Equal(t, 1, again.Succeeded)
		assert.True(t, again.Records[0].Duplicate)
		assert.True(t, again.Records[1].OK)
		waitRows(t, c, name, 2)
	})

	t.Run("rejected record", func(t *testing.T) {
		name := uniqueName(t)
		bad := eventRow(name, 9)
		bad["no_such_column"] = 1
		res, err := c.From("e2e_events").Insert(t.Context(), []map[string]any{eventRow(name, 1), bad})
		require.Error(t, err)
		ie, ok := errors.AsType[*wavehouse.InsertError](err)
		require.True(t, ok, "err %v is not an *InsertError", err)
		require.NotNil(t, res, "the result comes back with the error")
		assert.Equal(t, 1, res.Succeeded)
		assert.Equal(t, 1, res.Failed)
		require.Len(t, ie.Failed, 1)
		assert.Equal(t, 2, ie.Failed[0].Index)
		assert.Contains(t, ie.Failed[0].Error, "unknown column")
		waitRows(t, c, name, 1)
	})

	t.Run("pre-encoded NDJSON", func(t *testing.T) {
		name := uniqueName(t)
		res, err := c.From("e2e_events").InsertNDJSON(t.Context(), strings.NewReader(
			`{"event_id":"`+name+`-1","name":"`+name+`","value":1}`+"\n"+`{"event_id":"`+name+`-2","name":"`+name+`","value":2}`+"\n"))
		require.NoError(t, err)
		assert.Equal(t, 2, res.Succeeded)
		waitRows(t, c, name, 2)
	})

	t.Run("unknown table", func(t *testing.T) {
		_, err := c.From("no_such_table").Insert(t.Context(), eventRow("x", 1))
		require.ErrorIs(t, err, wavehouse.ErrNotFound)
	})
}

// seedEvents inserts values 1..n under a fresh name and waits for them.
func seedEvents(t *testing.T, c *wavehouse.Client, n uint64) string {
	t.Helper()
	name := uniqueName(t)
	rows := make([]map[string]any, n)
	for i := range rows {
		rows[i] = eventRow(name, float64(i+1))
	}
	_, err := c.From("e2e_events").Insert(t.Context(), rows)
	require.NoError(t, err)
	waitRows(t, c, name, n)
	return name
}

func TestQuery(t *testing.T) {
	c := newClient(t)
	name := seedEvents(t, c, 5)
	events := func() *wavehouse.Query { return c.From("e2e_events").Where("name", wavehouse.Eq, name) }

	t.Run("projection and order", func(t *testing.T) {
		rows, err := events().Select("name", "value").OrderBy("value", wavehouse.Desc).Limit(3).Fetch(t.Context())
		require.NoError(t, err)
		require.Len(t, rows, 3)
		assert.Equal(t, []string{"name", "value"}, sortedKeys(rows[0]))
		assert.Equal(t, name, rows[0]["name"])
		assert.Equal(t, json.Number("5"), rows[0]["value"])
		assert.Equal(t, json.Number("3"), rows[2]["value"])
	})

	t.Run("typed scan of every column", func(t *testing.T) {
		var rows []struct {
			EventID string    `json:"event_id"`
			Name    string    `json:"name"`
			Value   float64   `json:"value"`
			TS      time.Time `json:"ts"`
		}
		require.NoError(t, events().OrderBy("value", wavehouse.Asc).Scan(t.Context(), &rows))
		require.Len(t, rows, 5)
		assert.Equal(t, name, rows[0].Name)
		assert.InDelta(t, 1, rows[0].Value, 0)
		assert.NotEmpty(t, rows[0].EventID)
		assert.WithinDuration(t, time.Now(), rows[0].TS, 5*time.Minute)
	})

	t.Run("aggregate", func(t *testing.T) {
		var out []struct {
			N     uint64  `json:"n"`
			Total float64 `json:"total"`
			Top   float64 `json:"max_value"`
		}
		require.NoError(t, events().Count("*", "n").Sum("value", "total").Max("value", "").Scan(t.Context(), &out))
		require.Len(t, out, 1)
		assert.Equal(t, uint64(5), out[0].N)
		assert.InDelta(t, 15, out[0].Total, 0)
		assert.InDelta(t, 5, out[0].Top, 0)
	})

	t.Run("group by returns the selected key", func(t *testing.T) {
		rows, err := events().Select("name").Count("*", "").GroupBy("name").Fetch(t.Context())
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, name, rows[0]["name"])
		n, err := rows[0].Uint64("count")
		require.NoError(t, err)
		assert.Equal(t, uint64(5), n)
	})

	t.Run("select all", func(t *testing.T) {
		rows, err := events().SelectAll().Limit(1).Fetch(t.Context())
		require.NoError(t, err)
		require.Len(t, rows, 1)
		assert.Equal(t, []string{"event_id", "name", "ts", "value"}, sortedKeys(rows[0]))
	})

	t.Run("op and dir spellings", func(t *testing.T) {
		rows, err := events().Where("value", wavehouse.In, []int{1, 2}).OrderBy("value", "DESC").Select("value").Fetch(t.Context())
		require.NoError(t, err)
		require.Len(t, rows, 2)
		v, err := rows[0].Float64("value")
		require.NoError(t, err)
		assert.InDelta(t, 2, v, 0)
	})

	t.Run("filters", func(t *testing.T) {
		count := func(q *wavehouse.Query) int {
			rows, err := q.Select("value").Fetch(t.Context())
			require.NoError(t, err)
			return len(rows)
		}
		assert.Equal(t, 2, count(events().Where("value", wavehouse.Gt, 3)))
		assert.Equal(t, 3, count(events().Where("value", wavehouse.Lte, 3)))
		assert.Equal(t, 2, count(events().Where("value", wavehouse.In, []float64{1, 5})))
		assert.Equal(t, 5, count(c.From("e2e_events").Where("name", wavehouse.Like, name[:len(name)-3]+"%")))
		assert.Equal(t, 0, count(events().Where("value", wavehouse.Neq, 1).Where("value", wavehouse.Lt, 2)))
	})

	t.Run("time range", func(t *testing.T) {
		now := time.Now()
		rows, err := events().TimeRange("ts", now.Add(-time.Hour), time.Time{}).Fetch(t.Context())
		require.NoError(t, err)
		assert.Len(t, rows, 5, "open-ended from an hour ago")
		rows, err = events().TimeRange("ts", now.Add(-2*time.Hour), now.Add(-time.Hour)).Fetch(t.Context())
		require.NoError(t, err)
		assert.Empty(t, rows, "a closed window before the rows")
		rows, err = events().TimeRange("ts", now.Add(-time.Hour), now.Add(time.Hour)).Fetch(t.Context())
		require.NoError(t, err)
		assert.Len(t, rows, 5, "a closed window around them")
	})

	t.Run("last", func(t *testing.T) {
		rows, err := events().Last("ts", time.Hour).Fetch(t.Context())
		require.NoError(t, err)
		assert.Len(t, rows, 5)
		rows, err = events().Last("ts", 36*time.Hour).Fetch(t.Context())
		require.NoError(t, err)
		assert.Len(t, rows, 5, "a duration over a day, which the server's relative form must take")
	})

	t.Run("limit", func(t *testing.T) {
		rows, err := events().Limit(2).Fetch(t.Context())
		require.NoError(t, err)
		assert.Len(t, rows, 2)
	})

	t.Run("unknown column", func(t *testing.T) {
		_, err := events().Select("nope").Fetch(t.Context())
		require.ErrorIs(t, err, wavehouse.ErrBadRequest)
	})

	t.Run("unknown table", func(t *testing.T) {
		_, err := c.From("nope").Fetch(t.Context())
		require.ErrorIs(t, err, wavehouse.ErrNotFound)
	})

	t.Run("misuse is refused before sending", func(t *testing.T) {
		_, err := events().Select("name").SelectAll().Fetch(t.Context())
		require.ErrorContains(t, err, "SelectAll")
	})
}

func TestQueryNumbers(t *testing.T) {
	c := newClient(t)
	id := uniqueName(t)
	const price = "12345678901234567.0123456789"
	res, err := c.From("e2e_numbers").Insert(t.Context(), map[string]any{
		"event_id": id, "big": uint64(math.MaxUint64), "signed": int64(math.MinInt64), "price": price,
	})
	require.NoError(t, err, "%+v", res)

	type number struct {
		Big    uint64 `json:"big"`
		Signed int64  `json:"signed"`
		Price  string `json:"price"` // a Decimal comes back as a JSON string
	}
	var rows []number
	require.Eventually(t, func() bool {
		rows = nil
		err := c.From("e2e_numbers").Where("event_id", wavehouse.Eq, id).Select("big", "signed", "price").Scan(t.Context(), &rows)
		return err == nil && len(rows) == 1
	}, 30*time.Second, 200*time.Millisecond)
	assert.Equal(t, number{Big: math.MaxUint64, Signed: math.MinInt64, Price: price}, rows[0])

	fetched, err := c.From("e2e_numbers").Where("event_id", wavehouse.Eq, id).Select("big").Fetch(t.Context())
	require.NoError(t, err)
	assert.Equal(t, json.Number("18446744073709551615"), fetched[0]["big"])
}

// TestRowAccessors reads a real UInt64, Int64, Decimal and DateTime64 through Row.
func TestRowAccessors(t *testing.T) {
	c := newClient(t)
	id := uniqueName(t)
	const price = "12345678901234567.0123456789"
	_, err := c.From("e2e_numbers").Insert(t.Context(), map[string]any{
		"event_id": id, "big": uint64(math.MaxUint64), "signed": int64(math.MinInt64), "price": price,
	})
	require.NoError(t, err)
	var row wavehouse.Row
	require.Eventually(t, func() bool {
		rows, err := c.From("e2e_numbers").Where("event_id", wavehouse.Eq, id).Select("big", "signed", "price").Fetch(t.Context())
		if err != nil || len(rows) != 1 {
			return false
		}
		row = rows[0]
		return true
	}, 30*time.Second, 200*time.Millisecond)

	big, err := row.Uint64("big")
	require.NoError(t, err)
	assert.Equal(t, uint64(math.MaxUint64), big)
	signed, err := row.Int64("signed")
	require.NoError(t, err)
	assert.Equal(t, int64(math.MinInt64), signed)
	dec, err := row.String("price")
	require.NoError(t, err)
	assert.Equal(t, price, dec)
	f, err := row.Float64("price")
	require.NoError(t, err)
	assert.InDelta(t, 1.2345678901234567e16, f, 4)
	_, err = row.Int64("big")
	require.ErrorContains(t, err, `"big"`, "UInt64 max does not fit an int64")
	_, err = row.Time("nope")
	require.ErrorContains(t, err, `"nope"`)

	name := seedEvents(t, c, 1)
	ev, err := c.From("e2e_events").Where("name", wavehouse.Eq, name).Select("ts", "value").Fetch(t.Context())
	require.NoError(t, err)
	require.Len(t, ev, 1)
	ts, err := ev[0].Time("ts") // a DateTime64(3)
	require.NoError(t, err, "wire value %#v", ev[0]["ts"])
	assert.WithinDuration(t, time.Now(), ts, 5*time.Minute)
}

func TestInsertRefusesReadState(t *testing.T) {
	c := newClient(t)
	_, err := c.From("e2e_events").Limit(1).Insert(t.Context(), eventRow("x", 1))
	require.ErrorContains(t, err, "read state")
}

func sortedKeys(r wavehouse.Row) []string {
	keys := make([]string, 0, len(r))
	for k := range r {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}

func TestSQL(t *testing.T) {
	c := newClient(t)

	t.Run("UInt64 stays exact", func(t *testing.T) {
		var rows []struct {
			U uint64 `json:"u"`
			I int64  `json:"i"`
		}
		require.NoError(t, c.SQL("SELECT toUInt64(18446744073709551615) AS u, toInt64(-9223372036854775808) AS i").Scan(t.Context(), &rows))
		require.Len(t, rows, 1)
		assert.Equal(t, uint64(math.MaxUint64), rows[0].U)
		assert.Equal(t, int64(math.MinInt64), rows[0].I)

		fetched, err := c.SQL("SELECT toUInt64(18446744073709551615) AS u").Fetch(t.Context())
		require.NoError(t, err)
		assert.Equal(t, json.Number("18446744073709551615"), fetched[0]["u"])
	})

	t.Run("exec DDL", func(t *testing.T) {
		table := "e2e_tmp_" + fmt.Sprintf("%d_%d", time.Now().UnixNano(), seq.Add(1))
		require.NoError(t, c.SQL("CREATE TABLE "+table+" (a UInt8) ENGINE = Memory").Exec(t.Context()))
		require.NoError(t, c.SQL("INSERT INTO "+table+" VALUES (1), (2)").Exec(t.Context()))
		rows, err := c.SQL("SELECT sum(a) AS s FROM " + table).Fetch(t.Context())
		require.NoError(t, err)
		assert.Equal(t, json.Number("3"), rows[0]["s"])
		require.NoError(t, c.SQL("DROP TABLE "+table).Exec(t.Context()))
	})

	t.Run("inline FORMAT is not JSON", func(t *testing.T) {
		_, err := c.SQL("SELECT 1 AS a FORMAT CSV").Fetch(t.Context())
		require.ErrorContains(t, err, "not a JSON array of rows")
		require.NoError(t, c.SQL("SELECT 1 AS a FORMAT CSV").Exec(t.Context()))
	})

	t.Run("ClickHouse error", func(t *testing.T) {
		err := c.SQL("SELECT * FROM no_such_table_here").Exec(t.Context())
		apiErr, ok := errors.AsType[*wavehouse.APIError](err)
		require.True(t, ok, "err %v is not an *APIError", err)
		assert.Equal(t, "clickhouse.rejected", apiErr.Code)
		assert.False(t, wavehouse.IsRetryable(err))
	})

	t.Run("needs the admin role", func(t *testing.T) {
		anon, err := wavehouse.New(stack.url)
		require.NoError(t, err)
		require.ErrorIs(t, anon.SQL("SELECT 1").Exec(t.Context()), wavehouse.ErrForbidden)
	})
}

func TestPipe(t *testing.T) {
	c := newClient(t)
	name := seedEvents(t, c, 4)

	var out []struct {
		N uint64 `json:"n"`
	}
	require.NoError(t, c.Pipe("e2e_count").Param("name", name).Scan(t.Context(), &out))
	require.Len(t, out, 1)
	assert.Equal(t, uint64(4), out[0].N)

	rows, err := c.Pipe("e2e_count").Params(map[string]any{"name": "nobody-" + name}).Fetch(t.Context())
	require.NoError(t, err)
	assert.Equal(t, json.Number("0"), rows[0]["n"])

	t.Run("missing parameter", func(t *testing.T) {
		_, err := c.Pipe("e2e_count").Fetch(t.Context())
		require.ErrorIs(t, err, wavehouse.ErrBadRequest)
		assert.ErrorContains(t, err, "missing required parameter")
	})

	t.Run("unknown pipe", func(t *testing.T) {
		_, err := c.Pipe("nope").Fetch(t.Context())
		require.ErrorIs(t, err, wavehouse.ErrNotFound)
	})
}
