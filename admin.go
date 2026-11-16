package wavehouse

import (
	"context"
	"errors"
	"net/http"
	"net/url"
)

// Health pings GET /v1/health, which answers 200 with an empty body once the
// server is past boot and 503 while it is not; nil means 200. It resolves
// the client's tenant first, so an unknown tenant is a 404 ([ErrNotFound]).
//
// The endpoint is public: it checks neither the credential nor ClickHouse, so
// a nil result proves only that the server is up and the tenant exists. A
// health check for a datasource must go further: call Health, then an
// authenticated read such as From(table).Limit(1) or [Client.Tables], and map
// [ErrUnauthorized] and [ErrForbidden] to "bad credential" and "not allowed".
// See the example. A read: retried.
func (c *Client) Health(ctx context.Context) error {
	return c.doJSON(ctx, &request{kind: callRead, method: http.MethodGet, path: "/v1/health"}, nil)
}

// VersionInfo is the server's build, from GET /version.
type VersionInfo struct {
	Version   string `json:"version"`    // the release without its leading "v", or a pseudo-version
	GitCommit string `json:"git_commit"` // "unknown" when the build carried no VCS stamp
	BuildTime string `json:"build_time"` // as the build recorded it, normally RFC 3339
	GoVersion string `json:"go_version"` // the server's Go runtime
}

// Version returns the server's build metadata from GET /version, which needs
// no authentication. A read: retried.
func (c *Client) Version(ctx context.Context) (*VersionInfo, error) {
	var v VersionInfo
	if err := c.doJSON(ctx, &request{kind: callRead, method: http.MethodGet, path: "/version"}, &v); err != nil {
		return nil, err
	}
	return &v, nil
}

// TableSchema is one table as the server discovered it in ClickHouse.
type TableSchema struct {
	Name    string   `json:"name"`
	Columns []Column `json:"columns"` // in declaration order
}

// Column is one column of a [TableSchema].
type Column struct {
	Name     string `json:"name"`
	Type     string `json:"type"` // the ClickHouse type, e.g. "DateTime64(3, 'UTC')"
	Nullable bool   `json:"is_nullable"`
	// HasDefault says whether the column declares any default.
	HasDefault bool `json:"has_default"`
	// DefaultKind is "DEFAULT", "MATERIALIZED", "ALIAS" or "EPHEMERAL"; ""
	// when there is none. MATERIALIZED and ALIAS columns can't be inserted.
	DefaultKind       string `json:"default_kind,omitempty"`
	DefaultExpression string `json:"default_expression,omitempty"`
	// Position is the 1-based ordinal in the table's declaration.
	Position int `json:"position"`
}

// Tables lists every table schema of the client's tenant, from
// GET /v1/ops/schema. Admin role or operator key. A read: retried.
func (c *Client) Tables(ctx context.Context) ([]TableSchema, error) {
	var ts []TableSchema
	if err := c.doJSON(ctx, &request{kind: callRead, method: http.MethodGet, path: "/v1/ops/schema"}, &ts); err != nil {
		return nil, err
	}
	return ts, nil
}

// TableSchema returns one table's schema, from GET /v1/ops/schema?table=name;
// an unknown table is [ErrNotFound]. Admin role or operator key. A read:
// retried. To query a table, use [Client.From].
func (c *Client) TableSchema(ctx context.Context, name string) (*TableSchema, error) {
	if name == "" {
		return nil, errors.New("wavehouse: TableSchema: empty table name")
	}
	var t TableSchema
	r := &request{kind: callRead, method: http.MethodGet, path: "/v1/ops/schema", query: url.Values{"table": {name}}}
	if err := c.doJSON(ctx, r, &t); err != nil {
		return nil, err
	}
	return &t, nil
}

// RefreshSchema makes the server re-discover its tables in ClickHouse now,
// rather than at its next interval, and returns the result, via
// POST /v1/ops/schema/refresh. Admin role or operator key. Treated as a read:
// retried.
func (c *Client) RefreshSchema(ctx context.Context) ([]TableSchema, error) {
	var ts []TableSchema
	if err := c.doJSON(ctx, &request{kind: callRead, method: http.MethodPost, path: "/v1/ops/schema/refresh"}, &ts); err != nil {
		return nil, err
	}
	return ts, nil
}

// DLQStats counts the rows parked in a tenant's dead-letter queue.
type DLQStats struct {
	Tables map[string]int64 `json:"tables"` // per table; tables with none are absent
	Total  int64            `json:"total"`
}

// DLQStats returns the dead-letter queue counts of the client's tenant, from
// GET /v1/ops/dlq/stats. Admin role or operator key. A read: retried.
func (c *Client) DLQStats(ctx context.Context) (*DLQStats, error) {
	var s DLQStats
	if err := c.doJSON(ctx, &request{kind: callRead, method: http.MethodGet, path: "/v1/ops/dlq/stats"}, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// ReloadResult is the outcome of [Client.ReloadSettings].
type ReloadResult struct {
	// Adopted is false when an error finding made the server keep its
	// previous settings.
	Adopted  bool      `json:"adopted"`
	Findings []Finding `json:"findings"`
}

// Finding is one validation result from a settings reload.
type Finding struct {
	Severity string `json:"severity"`       // "error" or "warning"
	File     string `json:"file,omitempty"` // e.g. "policies.json"
	Path     string `json:"path,omitempty"` // the key within File, e.g. "tables.clicks.admin"
	Message  string `json:"message"`
}

// ReloadSettings makes the server re-validate its settings directory and
// adopt it if no finding is an error, via POST /v1/ops/settings/reload.
// When the server rejects the directory (422) it returns the result, whose
// Findings say why, together with an [*APIError]. Admin role or operator key;
// over a nested settings directory, the operator key only. Idempotent, so
// retried like a read.
func (c *Client) ReloadSettings(ctx context.Context) (*ReloadResult, error) {
	r := &request{kind: callRead, method: http.MethodPost, path: "/v1/ops/settings/reload"}
	res, err := c.do(ctx, r)
	if err != nil {
		if ae, ok := errors.AsType[*APIError](err); ok && ae.StatusCode == http.StatusUnprocessableEntity && res != nil {
			var rr ReloadResult
			if decodeJSON(res.body, &rr) == nil {
				return &rr, err
			}
		}
		return nil, err
	}
	var rr ReloadResult
	if err := decodeJSON(res.body, &rr); err != nil {
		return nil, decodeError(r, err)
	}
	return &rr, nil
}

// PipeDefinition is a named query pipe as the server's pipes.json defines it.
type PipeDefinition struct {
	Name        string          `json:"name"`
	SQL         string          `json:"sql"`
	Description string          `json:"description,omitempty"`
	Parameters  []PipeParameter `json:"parameters,omitempty"`
	// AllowedRoles may run the pipe; empty means the admin role only.
	AllowedRoles []string `json:"allowed_roles,omitempty"`
}

// PipeParameter is one parameter of a [PipeDefinition].
type PipeParameter struct {
	Name     string `json:"name"`
	Type     string `json:"type"` // advisory: "string", "number", "boolean" or "array"
	Required bool   `json:"required,omitempty"`
	// Default is used when the caller omits the parameter; numbers decode
	// as json.Number.
	Default any `json:"default,omitempty"`
}

// Pipes lists the client's tenant's named pipes, from GET /v1/ops/pipes.
// Admin role or operator key. A read: retried.
func (c *Client) Pipes(ctx context.Context) ([]PipeDefinition, error) {
	var ps []PipeDefinition
	if err := c.doJSON(ctx, &request{kind: callRead, method: http.MethodGet, path: "/v1/ops/pipes"}, &ps); err != nil {
		return nil, err
	}
	return ps, nil
}

// PipeDefinition returns one named pipe's definition, from
// GET /v1/ops/pipes/{name}; an unknown pipe is [ErrNotFound]. Admin role or
// operator key. A read: retried.
func (c *Client) PipeDefinition(ctx context.Context, name string) (*PipeDefinition, error) {
	if name == "" {
		return nil, errors.New("wavehouse: PipeDefinition: empty pipe name")
	}
	var p PipeDefinition
	r := &request{kind: callRead, method: http.MethodGet, path: "/v1/ops/pipes/" + url.PathEscape(name)}
	if err := c.doJSON(ctx, r, &p); err != nil {
		return nil, err
	}
	return &p, nil
}
