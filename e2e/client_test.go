package e2e

import (
	"errors"
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
func newClient(t *testing.T, opts ...wavehouse.Option) *wavehouse.Client {
	t.Helper()
	opts = append([]wavehouse.Option{
		wavehouse.WithToken(stack.adminToken),
		wavehouse.WithRetry(wavehouse.RetryPolicy{MaxAttempts: 2}),
	}, opts...)
	c, err := wavehouse.New(stack.url, opts...)
	require.NoError(t, err)
	return c
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

func TestHealth(t *testing.T) {
	require.NoError(t, newClient(t).Health(t.Context()))
}

func TestVersion(t *testing.T) {
	v, err := newClient(t).Version(t.Context())
	require.NoError(t, err)
	assert.NotEmpty(t, v.Version)
	assert.True(t, strings.HasPrefix(v.GoVersion, "go"), "go_version %q", v.GoVersion)
}

// e2eEventsColumns is e2e_events as fixtureDDL declares it.
var e2eEventsColumns = []wavehouse.Column{
	{Name: "event_id", Type: "String", Position: 1},
	{Name: "name", Type: "String", Position: 2},
	{Name: "value", Type: "Float64", Position: 3},
	{Name: "ts", Type: "DateTime64(3)", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "now64(3)", Position: 4},
}

func TestSchema(t *testing.T) {
	c := newClient(t)

	t.Run("TableSchema", func(t *testing.T) {
		one, err := c.TableSchema(t.Context(), "e2e_events")
		require.NoError(t, err)
		assert.Equal(t, "e2e_events", one.Name)
		assert.Equal(t, e2eEventsColumns, one.Columns)
	})

	t.Run("Tables", func(t *testing.T) {
		all, err := c.Tables(t.Context())
		require.NoError(t, err)
		assert.Contains(t, tableNames(all), "e2e_events")
	})

	t.Run("RefreshSchema", func(t *testing.T) {
		all, err := c.RefreshSchema(t.Context())
		require.NoError(t, err)
		assert.Contains(t, tableNames(all), "e2e_events")
	})

	t.Run("unknown table", func(t *testing.T) {
		_, err := c.TableSchema(t.Context(), "nope")
		require.ErrorIs(t, err, wavehouse.ErrNotFound)
		apiErr, ok := errors.AsType[*wavehouse.APIError](err)
		require.True(t, ok, "err %v is not an *APIError", err)
		assert.Equal(t, 404, apiErr.StatusCode)
		assert.Equal(t, "table not found", apiErr.Message)
		assert.NotEmpty(t, apiErr.RequestID)
		assert.False(t, apiErr.Retryable)
		assert.False(t, wavehouse.IsRetryable(err))
	})
}

func tableNames(ts []wavehouse.TableSchema) []string {
	names := make([]string, 0, len(ts))
	for _, t := range ts {
		names = append(names, t.Name)
	}
	return names
}

func TestDLQStats(t *testing.T) {
	s, err := newClient(t).DLQStats(t.Context())
	require.NoError(t, err)
	assert.NotNil(t, s.Tables)
	assert.GreaterOrEqual(t, s.Total, int64(0))
}

func TestReloadSettings(t *testing.T) {
	res, err := newClient(t).ReloadSettings(t.Context())
	require.NoError(t, err)
	assert.True(t, res.Adopted)
	if !stack.managed {
		return
	}
	// The container copies the settings in as 0644 and config.json carries auth.jwt_secret.
	var found bool
	for _, f := range res.Findings {
		if f.Severity == "warning" && f.File == "config.json" && strings.Contains(f.Message, "world-readable") {
			found = true
		}
	}
	assert.True(t, found, "no world-readable warning in %+v", res.Findings)
}

func TestPipes(t *testing.T) {
	c := newClient(t)

	list, err := c.Pipes(t.Context())
	require.NoError(t, err)
	names := make([]string, 0, len(list))
	for _, p := range list {
		names = append(names, p.Name)
	}
	assert.Contains(t, names, "e2e_count")

	pipe, err := c.PipeDefinition(t.Context(), "e2e_count")
	require.NoError(t, err)
	assert.Equal(t, &wavehouse.PipeDefinition{
		Name:         "e2e_count",
		SQL:          "SELECT count() AS n FROM e2e_events WHERE name = {{name}}",
		Description:  "Rows in e2e_events with a given name",
		Parameters:   []wavehouse.PipeParameter{{Name: "name", Type: "string", Required: true}},
		AllowedRoles: []string{"public"},
	}, pipe)

	_, err = c.PipeDefinition(t.Context(), "nope")
	require.ErrorIs(t, err, wavehouse.ErrNotFound)
}

// X-Tenant-ID is the caller's to send, through WithHeader. Admin routes ignore it.
func TestTenantHeader(t *testing.T) {
	t.Run("unknown tenant on a data route", func(t *testing.T) {
		c := newClient(t, wavehouse.WithHeader("X-Tenant-ID", "nope"))
		require.ErrorIs(t, c.Health(t.Context()), wavehouse.ErrNotFound)
	})

	t.Run("default tenant 0", func(t *testing.T) {
		c := newClient(t, wavehouse.WithHeader("X-Tenant-ID", "0"))
		require.NoError(t, c.Health(t.Context()))
	})
}
