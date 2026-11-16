package wavehouse

import (
	_ "embed"
	"encoding/json"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// wireCasesJSON is the shared request-conformance fixture: the JS SDK
// (wavehouse-sdk-js) replays the same cases. It is embedded so the test binary
// is self-contained.
//
//go:embed testdata/wire_cases.json
var wireCasesJSON []byte

// wireCase is one SDK call and the exact request it must send.
type wireCase struct {
	Name       string            `json:"name"`
	SDKs       []string          `json:"sdks"` // which SDKs replay the case; empty means all
	Endpoint   string            `json:"endpoint"`
	Table      string            `json:"table"`
	Headers    map[string]string `json:"headers"` // request headers set through WithHeader
	Operations []wireOp          `json:"operations"`
	PipeName   string            `json:"pipe_name"`
	PipeParams map[string]any    `json:"pipe_params"`
	SQL        string            `json:"sql"`
	Tenant     string            `json:"tenant"` // the admin calls' ?tenant=, through ForTenant

	ExpectedPath        string            `json:"expected_path"`
	ExpectedMethod      string            `json:"expected_method"`
	ExpectedContentType string            `json:"expected_content_type"`
	ExpectedHeaders     map[string]string `json:"expected_headers"` // "" means the header must be absent
	ExpectedBody        json.RawMessage   `json:"expected_body"`
	ExpectedNDJSON      []json.RawMessage `json:"expected_ndjson"` // one object per body line
	ExpectedNoRequest   bool              `json:"expected_no_request"`
}

type wireOp struct {
	Method string `json:"method"`
	Args   []any  `json:"args"`
}

// symbolOps are the operator spellings the shared cases use, which the JS SDK
// also accepts; the Go SDK's own spellings are the wire ones.
var symbolOps = map[string]Op{"=": Eq, "!=": Neq, ">": Gt, ">=": Gte, "<": Lt, "<=": Lte}

// wireAnswer replies with a body valid for each route, so a call decodes its response.
func wireAnswer(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch p := r.URL.Path; {
	case p == "/v1/health":
	case p == "/version", p == "/v1/ops/settings/reload":
		_, _ = w.Write([]byte(`{"adopted":true}`))
	case p == "/v1/ingest":
		_, _ = w.Write([]byte(`{"total":0}`))
	case p == "/v1/ops/schema" && r.URL.Query().Has("table"):
		_, _ = w.Write([]byte(`{"name":"events","columns":[]}`))
	case p == "/v1/ops/dlq/stats":
		_, _ = w.Write([]byte(`{"tables":{},"total":0}`))
	case strings.HasPrefix(p, "/v1/ops/pipes/"):
		_, _ = w.Write([]byte(`{"name":"my_pipe","sql":"SELECT 1"}`))
	default:
		_, _ = w.Write([]byte(`[]`))
	}
}

func TestConformanceWireFormat(t *testing.T) {
	var cases []wireCase
	require.NoError(t, json.Unmarshal(wireCasesJSON, &cases))
	require.NotEmpty(t, cases)

	for _, tc := range cases {
		if len(tc.SDKs) > 0 && !slices.Contains(tc.SDKs, "go") {
			continue
		}
		t.Run(tc.Name, func(t *testing.T) {
			rec := newRecorder(t, wireAnswer)
			var opts []Option
			for k, v := range tc.Headers {
				opts = append(opts, WithHeader(k, v))
			}
			c := rec.client(t, opts...)
			require.NoError(t, runWireCase(t, c, tc))

			if tc.ExpectedNoRequest {
				assert.Empty(t, rec.requests())
				return
			}
			got := rec.last(t)
			assert.Equal(t, tc.ExpectedMethod, got.method)
			assert.Equal(t, normalizePath(tc.ExpectedPath), normalizePath(got.rawPath+"?"+got.rawQuery), "path")
			if tc.ExpectedContentType != "" {
				assert.Equal(t, tc.ExpectedContentType, got.header.Get("Content-Type"))
			}
			for k, v := range tc.ExpectedHeaders {
				assert.Equal(t, v, got.header.Get(k), "header %s", k)
			}
			switch {
			case tc.ExpectedNDJSON != nil:
				lines := strings.Split(got.body, "\n")
				require.Len(t, lines, len(tc.ExpectedNDJSON))
				for i, want := range tc.ExpectedNDJSON {
					assert.JSONEq(t, string(want), lines[i], "line %d", i+1)
				}
			case tc.ExpectedBody != nil:
				assert.JSONEq(t, string(tc.ExpectedBody), got.body)
			default:
				assert.Empty(t, got.body)
			}
		})
	}
}

func runWireCase(t *testing.T, c *Client, tc wireCase) error {
	t.Helper()
	ctx := t.Context()
	ft := ForTenant(tc.Tenant)
	switch tc.Endpoint {
	case "query":
		_, err := applyOps(t, c.From(tc.Table), tc.Operations).Fetch(ctx)
		return err
	case "ingest":
		require.Len(t, tc.Operations, 1)
		_, err := c.From(tc.Table).Insert(ctx, tc.Operations[0].Args[0])
		return err
	case "pipe":
		_, err := c.Pipe(tc.PipeName).Params(tc.PipeParams).Fetch(ctx)
		return err
	case "sql":
		_, err := c.SQL(tc.SQL).ForTenant(tc.Tenant).Fetch(ctx)
		return err
	case "sql_exec":
		return c.SQL(tc.SQL).ForTenant(tc.Tenant).Exec(ctx)
	case "health":
		return c.Health(ctx)
	case "version":
		_, err := c.Version(ctx)
		return err
	case "schema_list":
		_, err := c.Tables(ctx, ft)
		return err
	case "schema_table":
		_, err := c.TableSchema(ctx, tc.Table, ft)
		return err
	case "schema_refresh":
		_, err := c.RefreshSchema(ctx, ft)
		return err
	case "dlq_list":
		_, err := c.DLQStats(ctx, ft)
		return err
	case "settings_reload":
		_, err := c.ReloadSettings(ctx, ft)
		return err
	case "pipes_list":
		_, err := c.Pipes(ctx, ft)
		return err
	case "pipes_get":
		_, err := c.PipeDefinition(ctx, tc.PipeName, ft)
		return err
	}
	// A hard failure, so a case nobody wired up can't hide drift between the SDKs.
	t.Fatalf("unhandled endpoint %q: add it to runWireCase", tc.Endpoint)
	return nil
}

// applyOps replays a case's builder calls, in order.
func applyOps(t *testing.T, q *Query, ops []wireOp) *Query {
	t.Helper()
	str := func(op wireOp, i int) string {
		s, ok := op.Args[i].(string)
		require.True(t, ok, "%s arg %d: want a string, got %T", op.Method, i, op.Args[i])
		return s
	}
	strs := func(op wireOp) []string {
		out := make([]string, len(op.Args))
		for i := range op.Args {
			out[i] = str(op, i)
		}
		return out
	}
	for _, op := range ops {
		switch op.Method {
		case "select":
			q = q.Select(strs(op)...)
		case "selectAll":
			q = q.SelectAll()
		case "where":
			o := Op(str(op, 1))
			if sym, ok := symbolOps[string(o)]; ok {
				o = sym
			}
			q = q.Where(str(op, 0), o, op.Args[2])
		case "groupBy":
			q = q.GroupBy(strs(op)...)
		case "orderBy":
			q = q.OrderBy(str(op, 0), Dir(str(op, 1)))
		case "limit":
			q = q.Limit(int(op.Args[0].(float64)))
		case "timeRange":
			// RFC 3339 strings; an empty one is the zero time, an open end.
			parse := func(i int) time.Time {
				if str(op, i) == "" {
					return time.Time{}
				}
				ts, err := time.Parse(time.RFC3339Nano, str(op, i))
				require.NoError(t, err)
				return ts
			}
			q = q.TimeRange(str(op, 0), parse(1), parse(2))
		case "last":
			d, err := time.ParseDuration(str(op, 1))
			require.NoError(t, err)
			q = q.Last(str(op, 0), d)
		case "count":
			q = q.Count(str(op, 0), str(op, 1))
		case "sum":
			q = q.Sum(str(op, 0), str(op, 1))
		case "avg":
			q = q.Avg(str(op, 0), str(op, 1))
		case "min":
			q = q.Min(str(op, 0), str(op, 1))
		case "max":
			q = q.Max(str(op, 0), str(op, 1))
		case "countDistinct":
			q = q.CountDistinct(str(op, 0), str(op, 1))
		case "aggregate":
			q = q.Aggregate(str(op, 0), str(op, 1), str(op, 2))
		default:
			t.Fatalf("unhandled operation %q: add it to applyOps", op.Method)
		}
	}
	return q
}

// normalizePath compares request URIs by meaning: the same path and the same
// decoded query values, whatever the parameter order or the + vs %20 spelling.
func normalizePath(p string) string {
	u, err := url.ParseRequestURI(p)
	if err != nil {
		return p
	}
	return u.EscapedPath() + "?" + u.Query().Encode()
}
