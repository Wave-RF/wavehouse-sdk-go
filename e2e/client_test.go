package e2e

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	wavehouse "github.com/Wave-RF/wavehouse-sdk-go"
)

// newClient returns a client authenticated as the admin role, with one retry.
func newClient(t *testing.T) *wavehouse.Client {
	t.Helper()
	return wavehouse.NewClient(wavehouse.Config{
		BaseURL: stack.url,
		Auth:    wavehouse.StaticToken(stack.adminToken),
		Options: &wavehouse.ClientOptions{MaxRetries: wavehouse.Ptr(1)},
	})
}

var seq atomic.Int64

// uniqueName returns a value no other test or run writes, to find a test's own rows.
func uniqueName(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("%s-%d-%d", strings.ReplaceAll(t.Name(), "/", "_"), time.Now().UnixNano(), seq.Add(1))
}

// eventRow is one e2e_events row, with a unique dedupe id.
func eventRow(name string, value float64) map[string]any {
	return map[string]any{
		"event_id": fmt.Sprintf("%s-%d", name, seq.Add(1)),
		"name":     name,
		"value":    value,
	}
}

// rowsNamed waits until want rows named name are queryable: ingest writes to ClickHouse asynchronously.
func rowsNamed(t *testing.T, c *wavehouse.Client, name string, want int) []map[string]any {
	t.Helper()
	var rows []map[string]any
	require.EventuallyWithT(t, func(ct *assert.CollectT) {
		page, err := c.From("e2e_events").SelectAll().Where("name", wavehouse.OpEq, name).
			OrderBy("value", "asc").Limit(100).FetchUntyped(context.Background())
		require.NoError(ct, err)
		rows = page.Data
		assert.Len(ct, rows, want)
	}, 15*time.Second, 100*time.Millisecond)
	return rows
}

func TestHealth(t *testing.T) {
	require.NoError(t, newClient(t).Sys.Health(t.Context()))
}

func TestSchema(t *testing.T) {
	c := newClient(t)

	all, err := c.Schema.List(t.Context())
	require.NoError(t, err)
	require.Contains(t, all, "e2e_events")

	one, err := c.From("e2e_events").Schema(t.Context())
	require.NoError(t, err)
	names := make([]string, 0, len(one.Columns))
	for _, col := range one.Columns {
		names = append(names, col.Name)
	}
	assert.Equal(t, []string{"event_id", "name", "value", "ts"}, names)
}

func TestInsert(t *testing.T) {
	c := newClient(t)

	t.Run("one row", func(t *testing.T) {
		name := uniqueName(t)
		res, err := c.From("e2e_events").Insert(t.Context(), eventRow(name, 1))
		require.NoError(t, err)
		assert.True(t, res.OK)

		rows := rowsNamed(t, c, name, 1)
		assert.InDelta(t, 1.0, rows[0]["value"], 0)
	})

	t.Run("batch", func(t *testing.T) {
		name := uniqueName(t)
		batch := []map[string]any{eventRow(name, 1), eventRow(name, 2), eventRow(name, 3)}
		res, err := c.From("e2e_events").Insert(t.Context(), batch)
		require.NoError(t, err)
		assert.True(t, res.OK)
		require.NotNil(t, res.Succeeded)
		assert.Equal(t, 3, *res.Succeeded)

		rows := rowsNamed(t, c, name, 3)
		for i, row := range rows {
			assert.InDelta(t, float64(i+1), row["value"], 0)
		}
	})

	t.Run("duplicate dedupe id", func(t *testing.T) {
		row := eventRow(uniqueName(t), 1)
		first, err := c.From("e2e_events").Insert(t.Context(), row)
		require.NoError(t, err)
		assert.Nil(t, first.Duplicate)

		second, err := c.From("e2e_events").Insert(t.Context(), row)
		require.NoError(t, err)
		require.NotNil(t, second.Duplicate)
		assert.True(t, *second.Duplicate)
	})
}

func TestQuery(t *testing.T) {
	c := newClient(t)
	name := uniqueName(t)
	_, err := c.From("e2e_events").Insert(t.Context(), []map[string]any{eventRow(name, 10), eventRow(name, 20)})
	require.NoError(t, err)
	rowsNamed(t, c, name, 2)

	t.Run("projection and order", func(t *testing.T) {
		page, err := c.From("e2e_events").Select("name", "value").
			Where("name", wavehouse.OpEq, name).OrderBy("value", "desc").Limit(5).
			FetchUntyped(t.Context())
		require.NoError(t, err)
		require.Len(t, page.Data, 2)
		assert.Equal(t, map[string]any{"name": name, "value": 20.0}, page.Data[0])
	})

	t.Run("typed rows", func(t *testing.T) {
		type event struct {
			Name  string  `json:"name"`
			Value float64 `json:"value"`
		}
		q := c.From("e2e_events").Select("name", "value").Where("name", wavehouse.OpEq, name).OrderBy("value", "asc")
		page, err := wavehouse.FetchTyped[event](t.Context(), q)
		require.NoError(t, err)
		assert.Equal(t, []event{{name, 10}, {name, 20}}, page.Data)
	})

	t.Run("aggregate", func(t *testing.T) {
		page, err := c.From("e2e_events").Select().Sum("value", "total").Where("name", wavehouse.OpEq, name).
			FetchUntyped(t.Context())
		require.NoError(t, err)
		require.Len(t, page.Data, 1)
		assert.InDelta(t, 30.0, page.Data[0]["total"], 0)
	})
}

func TestSQL(t *testing.T) {
	rows, err := wavehouse.SQL[map[string]any](t.Context(), newClient(t), "SELECT 1 AS n")
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.InDelta(t, 1.0, rows[0]["n"], 0)
}

func TestPipes(t *testing.T) {
	c := newClient(t)

	list, err := c.Pipes.List(t.Context())
	require.NoError(t, err)
	names := make([]string, 0, len(list))
	for _, p := range list {
		names = append(names, p.Name)
	}
	assert.Contains(t, names, "e2e_count")

	pipe, err := c.Pipes.Get(t.Context(), "e2e_count")
	require.NoError(t, err)
	assert.Contains(t, pipe.SQL, "count()")

	name := uniqueName(t)
	_, err = c.From("e2e_events").Insert(t.Context(), []map[string]any{eventRow(name, 1), eventRow(name, 2)})
	require.NoError(t, err)
	rowsNamed(t, c, name, 2)

	rows, err := c.Pipe("e2e_count", map[string]any{"name": name}).FetchUntyped(t.Context())
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.InDelta(t, 2.0, rows[0]["n"], 0)
}
