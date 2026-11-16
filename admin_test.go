package wavehouse

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Response bodies below are copied from the server's API reference.
const (
	schemaListBody = `[
  {
    "name": "clicks",
    "columns": [
      {"name": "page", "type": "String", "is_nullable": false, "has_default": false, "position": 1},
      {"name": "button", "type": "String", "is_nullable": false, "has_default": false, "position": 2},
      {"name": "score", "type": "Float64", "is_nullable": false, "has_default": false, "position": 3},
      {"name": "received_timestamp", "type": "DateTime64(3, 'UTC')", "is_nullable": false, "has_default": true, "default_kind": "DEFAULT", "default_expression": "now64(3, 'UTC')", "position": 4}
    ]
  }
]`
	tableBody = `{
  "name": "clicks",
  "columns": [
    {"name": "page", "type": "String", "is_nullable": false, "has_default": false, "position": 1},
    {"name": "button", "type": "String", "is_nullable": false, "has_default": false, "position": 2}
  ]
}`
	versionBody = `{"version": "1.2.3", "git_commit": "a1b2c3d", "build_time": "2026-06-02T12:00:00Z", "go_version": "go1.26.3"}`
	dlqBody     = `{"tables": {"clicks": 3, "page_views": 1}, "total": 4}`
	pipeBody    = `{
  "name": "top_pages",
  "sql": "SELECT page, count() as views FROM clicks WHERE received_timestamp >= {{start_date}} GROUP BY page LIMIT {{limit}}",
  "parameters": [
    {"name": "start_date", "type": "string", "required": true},
    {"name": "limit", "type": "number", "required": false, "default": 100}
  ],
  "description": "Top pages by view count",
  "allowed_roles": ["viewer"]
}`
	reloadBody = `{
  "adopted": true,
  "findings": [
    { "severity": "warning", "file": "policies.json", "message": "empty document — no policy; every request will be denied (fail closed)" }
  ]
}`
)

var (
	clicks = TableSchema{Name: "clicks", Columns: []Column{
		{Name: "page", Type: "String", Position: 1},
		{Name: "button", Type: "String", Position: 2},
		{Name: "score", Type: "Float64", Position: 3},
		{Name: "received_timestamp", Type: "DateTime64(3, 'UTC')", HasDefault: true, DefaultKind: "DEFAULT", DefaultExpression: "now64(3, 'UTC')", Position: 4},
	}}
	topPages = PipeDefinition{
		Name:        "top_pages",
		SQL:         "SELECT page, count() as views FROM clicks WHERE received_timestamp >= {{start_date}} GROUP BY page LIMIT {{limit}}",
		Description: "Top pages by view count",
		Parameters: []PipeParameter{
			{Name: "start_date", Type: "string", Required: true},
			{Name: "limit", Type: "number", Default: json.Number("100")},
		},
		AllowedRoles: []string{"viewer"},
	}
)

func TestAdminCalls(t *testing.T) {
	cases := []struct {
		name                string
		body                string
		call                func(*Client) (any, error)
		method, path, query string
		want                any
	}{
		{
			name: "Version", body: versionBody, method: "GET", path: "/version",
			call: func(c *Client) (any, error) { return c.Version(t.Context()) },
			want: &VersionInfo{Version: "1.2.3", GitCommit: "a1b2c3d", BuildTime: "2026-06-02T12:00:00Z", GoVersion: "go1.26.3"},
		},
		{
			name: "Tables", body: schemaListBody, method: "GET", path: "/v1/ops/schema",
			call: func(c *Client) (any, error) { return c.Tables(t.Context()) },
			want: []TableSchema{clicks},
		},
		{
			name: "Table", body: tableBody, method: "GET", path: "/v1/ops/schema", query: "table=clicks",
			call: func(c *Client) (any, error) { return c.Table(t.Context(), "clicks") },
			want: &TableSchema{Name: "clicks", Columns: clicks.Columns[:2]},
		},
		{
			name: "RefreshSchema", body: schemaListBody, method: "POST", path: "/v1/ops/schema/refresh",
			call: func(c *Client) (any, error) { return c.RefreshSchema(t.Context()) },
			want: []TableSchema{clicks},
		},
		{
			name: "DLQStats", body: dlqBody, method: "GET", path: "/v1/ops/dlq/stats",
			call: func(c *Client) (any, error) { return c.DLQStats(t.Context()) },
			want: &DLQStats{Tables: map[string]int64{"clicks": 3, "page_views": 1}, Total: 4},
		},
		{
			name: "ReloadSettings", body: reloadBody, method: "POST", path: "/v1/ops/settings/reload",
			call: func(c *Client) (any, error) { return c.ReloadSettings(t.Context()) },
			want: &ReloadResult{Adopted: true, Findings: []Finding{{
				Severity: "warning", File: "policies.json",
				Message: "empty document — no policy; every request will be denied (fail closed)",
			}}},
		},
		{
			name: "Pipes", body: "[" + pipeBody + "]", method: "GET", path: "/v1/ops/pipes",
			call: func(c *Client) (any, error) { return c.Pipes(t.Context()) },
			want: []PipeDefinition{topPages},
		},
		{
			name: "PipeDefinition", body: pipeBody, method: "GET", path: "/v1/ops/pipes/top_pages",
			call: func(c *Client) (any, error) { return c.PipeDefinition(t.Context(), "top_pages") },
			want: &topPages,
		},
		{
			name: "PipeDefinition escapes", body: pipeBody, method: "GET", path: "/v1/ops/pipes/a%2Fb%20c",
			call: func(c *Client) (any, error) { return c.PipeDefinition(t.Context(), "a/b c") },
			want: &topPages,
		},
		{
			name: "Health", method: "GET", path: "/v1/health",
			call: func(c *Client) (any, error) { return nil, c.Health(t.Context()) },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := newRecorder(t, reply(200, tc.body))
			got, err := tc.call(rec.client(t, WithOperatorKey("k")))
			require.NoError(t, err)
			req := rec.last(t)
			assert.Equal(t, [3]string{tc.method, tc.path, tc.query}, [3]string{req.method, req.rawPath, req.rawQuery})
			assert.Empty(t, req.body, "admin calls send no body")
			assert.Empty(t, req.header.Values("Content-Type"))
			if tc.want != nil {
				assert.Equal(t, tc.want, got)
			}
		})
	}
}

func TestAdminCallErrors(t *testing.T) {
	rec := newRecorder(t, reply(404, `{"error":"table not found"}`, "X-Request-Id", "r1"))
	c := rec.client(t)
	for name, call := range map[string]func() error{
		"Version":        func() error { _, err := c.Version(t.Context()); return err },
		"Tables":         func() error { _, err := c.Tables(t.Context()); return err },
		"Table":          func() error { _, err := c.Table(t.Context(), "nope"); return err },
		"RefreshSchema":  func() error { _, err := c.RefreshSchema(t.Context()); return err },
		"DLQStats":       func() error { _, err := c.DLQStats(t.Context()); return err },
		"ReloadSettings": func() error { _, err := c.ReloadSettings(t.Context()); return err },
		"Pipes":          func() error { _, err := c.Pipes(t.Context()); return err },
		"PipeDefinition": func() error { _, err := c.PipeDefinition(t.Context(), "nope"); return err },
		"Health":         func() error { return c.Health(t.Context()) },
	} {
		assert.ErrorIs(t, call(), ErrNotFound, name)
	}

	sent := len(rec.requests())
	_, err := c.Table(t.Context(), "")
	assert.Error(t, err)
	_, err = c.PipeDefinition(t.Context(), "")
	assert.Error(t, err)
	assert.Len(t, rec.requests(), sent, "an empty name was sent")
}

func TestReloadSettingsRejected(t *testing.T) {
	body := `{"adopted":false,"findings":[{"severity":"error","file":"config.json","path":"clickhouse.max_open_conns","message":"must be >= clickhouse.max_idle_conns (5), got 1"}]}`
	rec := newRecorder(t, reply(http.StatusUnprocessableEntity, body, "X-Request-Id", "abc"))
	res, err := rec.client(t).ReloadSettings(t.Context())

	ae, ok := errors.AsType[*APIError](err)
	require.True(t, ok, "err = %v", err)
	assert.Equal(t, 422, ae.StatusCode)
	assert.Equal(t, "abc", ae.RequestID)
	assert.Equal(t, "Unprocessable Entity", ae.Message)
	assert.Equal(t, &ReloadResult{Findings: []Finding{{
		Severity: "error", File: "config.json", Path: "clickhouse.max_open_conns",
		Message: "must be >= clickhouse.max_idle_conns (5), got 1",
	}}}, res)

	// A body that is not a reload result yields only the error, on a 422 or a 200.
	for _, status := range []int{http.StatusUnprocessableEntity, http.StatusOK} {
		rec = newRecorder(t, reply(status, `nope`))
		res, err := rec.client(t).ReloadSettings(t.Context())
		assert.Nil(t, res, status)
		assert.Error(t, err, status)
	}
}
