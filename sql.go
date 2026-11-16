package wavehouse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// SQLCall is one raw SQL statement. Get one from [Client.SQL].
type SQLCall struct {
	c      *Client
	query  string
	tenant string
	header http.Header
	err    error
}

// ForTenant runs the statement against the ClickHouse of the tenant id, sent
// as ?tenant=, in place of the default tenant "0"; it applies to Scan, Fetch
// and Exec. An unknown id is [ErrNotFound], an empty id sends nothing. Unlike
// the X-Tenant-ID header of the data routes, which this route ignores, it is
// the only way to pick a tenant here. It returns a copy; s is unchanged.
func (s *SQLCall) ForTenant(id string) *SQLCall {
	n := *s
	n.tenant = id
	return &n
}

// Header sets a header on this statement's request, with the precedence and
// the copy semantics of [Query.Header]. It applies to Scan, Fetch and Exec.
func (s *SQLCall) Header(key, value string) *SQLCall {
	n := *s
	n.header = s.header.Clone()
	if err := checkHeader(key, value); err != nil {
		if n.err == nil {
			n.err = fmt.Errorf("wavehouse: SQL: Header: %w", err)
		}
		return &n
	}
	if n.header == nil {
		n.header = http.Header{}
	}
	n.header.Set(key, value)
	return &n
}

// SQL prepares a statement to run verbatim against ClickHouse, via
// POST /v1/ops/query. It needs the admin role or the operator key, and the
// statement can be anything ClickHouse accepts, DDL and mutations included.
// There is no parameter binding: build the statement yourself, and never from
// untrusted input.
func (c *Client) SQL(query string) *SQLCall {
	return &SQLCall{c: c, query: query}
}

func (s *SQLCall) request() (*request, error) {
	if s.err != nil {
		return nil, s.err
	}
	if strings.TrimSpace(s.query) == "" {
		return nil, errors.New("wavehouse: SQL: empty statement")
	}
	raw, err := json.Marshal(map[string]string{"sql": s.query})
	if err != nil {
		return nil, fmt.Errorf("wavehouse: SQL: %w", err)
	}
	// The statement may mutate: only a request that never left is retried, and an APIError never is.
	return &request{
		kind: callSQL, method: http.MethodPost, path: "/v1/ops/query",
		body: raw, contentType: "application/json", query: tenantQuery([]AdminOption{ForTenant(s.tenant)}),
		callHeader: s.header,
	}, nil
}

// Exec runs the statement and discards the body, for DDL and mutations; it is
// also the way to run one with an inline FORMAT clause. Nothing is retried
// after the server answered, and a transport error only when the request
// never left.
func (s *SQLCall) Exec(ctx context.Context) error {
	r, err := s.request()
	if err != nil {
		return err
	}
	_, err = s.c.do(ctx, r)
	return err
}

// Scan runs the statement and decodes the rows into dst, a non-nil pointer to
// a slice of structs or maps, with the same rules as [Query.Scan]. ClickHouse's
// UInt64 and Int64 arrive as JSON numbers, so they are exact in an int64 or
// uint64 field. A statement with an inline FORMAT clause answers in that
// format, not JSON, and Scan reports that rather than a decode error.
func (s *SQLCall) Scan(ctx context.Context, dst any) error {
	if err := checkScanDst(dst); err != nil {
		return err
	}
	r, err := s.request()
	if err != nil {
		return err
	}
	res, err := s.c.do(ctx, r)
	if err != nil {
		return err
	}
	if !bytes.HasPrefix(bytes.TrimSpace(res.body), []byte("[")) {
		return fmt.Errorf("wavehouse: %s %s: the response is %q, not a JSON array of rows; "+
			"an inline FORMAT clause replaces the JSON envelope, so remove it, or use Exec to discard the body",
			r.method, r.path, res.header.Get("Content-Type"))
	}
	if err := decodeJSON(res.body, dst); err != nil {
		return decodeError(r, err)
	}
	return nil
}

// Fetch runs the statement and returns its rows as maps; see [Query.Fetch].
func (s *SQLCall) Fetch(ctx context.Context) ([]Row, error) {
	var rows []Row
	if err := s.Scan(ctx, &rows); err != nil {
		return nil, err
	}
	return rows, nil
}
