package wavehouse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
)

// PipeCall runs one named pipe, a query the server's operator defined, with
// parameters. Like [Query] it is immutable: [PipeCall.Param] and
// [PipeCall.Params] return a copy. Get one from [Client.Pipe].
type PipeCall struct {
	c      *Client
	name   string
	params map[string]any
	header http.Header
	err    error
}

// Pipe starts a call to the named pipe, sent as POST /v1/pipes/{name} with
// the parameters as a JSON object. The pipe's allowed roles decide who may run
// it; [Client.PipeDefinition] lists its parameters.
func (c *Client) Pipe(name string) *PipeCall {
	return &PipeCall{c: c, name: name}
}

// Param sets one parameter; a later call for the same name wins. Values are
// JSON-encoded: a slice becomes an IN-style list on the server.
func (p *PipeCall) Param(name string, value any) *PipeCall {
	return p.Params(map[string]any{name: value})
}

// Params sets several parameters at once.
func (p *PipeCall) Params(m map[string]any) *PipeCall {
	cp := *p
	cp.params = make(map[string]any, len(p.params)+len(m))
	maps.Copy(cp.params, p.params)
	maps.Copy(cp.params, m)
	return &cp
}

// Header sets a header on this call's request, with the precedence and the
// copy semantics of [Query.Header]. It applies to Scan, Fetch and Exec.
func (p *PipeCall) Header(key, value string) *PipeCall {
	cp := *p
	cp.header = p.header.Clone()
	if err := checkHeader(key, value); err != nil {
		if cp.err == nil {
			cp.err = fmt.Errorf("wavehouse: Pipe %q: Header: %w", p.name, err)
		}
		return &cp
	}
	if cp.header == nil {
		cp.header = http.Header{}
	}
	cp.header.Set(key, value)
	return &cp
}

func (p *PipeCall) request() (*request, error) {
	if p.err != nil {
		return nil, p.err
	}
	if p.name == "" {
		return nil, errors.New("wavehouse: Pipe: empty pipe name")
	}
	params := p.params
	if params == nil {
		params = map[string]any{}
	}
	raw, err := json.Marshal(params)
	if err != nil {
		return nil, fmt.Errorf("wavehouse: Pipe %q: encode parameters: %w", p.name, err)
	}
	// A pipe may be one that writes, so only a request that never left is retried.
	return &request{
		kind: callPipe, method: http.MethodPost, path: "/v1/pipes/" + url.PathEscape(p.name),
		body: raw, contentType: "application/json", callHeader: p.header,
	}, nil
}

// Scan runs the pipe and decodes its rows into dst, a non-nil pointer to a
// slice of structs or maps, with the same rules as [Query.Scan]. A pipe that
// writes answers with no rows. Retries: a pipe may write, so a transport error
// is retried only when the request never left, and a failed answer only when
// the server itself marks it retryable, or it is a 429 or a 503 with
// Retry-After. A bare 5xx, which a proxy can send after the write ran, is not.
func (p *PipeCall) Scan(ctx context.Context, dst any) error {
	if err := checkScanDst(dst); err != nil {
		return err
	}
	r, err := p.request()
	if err != nil {
		return err
	}
	return p.c.doJSON(ctx, r, dst)
}

// Fetch runs the pipe and returns its rows as maps; see [Query.Fetch].
func (p *PipeCall) Fetch(ctx context.Context) ([]Row, error) {
	var rows []Row
	if err := p.Scan(ctx, &rows); err != nil {
		return nil, err
	}
	return rows, nil
}

// Exec runs the pipe and discards its rows, for a pipe that writes; like
// [SQLCall.Exec]. The retry rules are those of [PipeCall.Scan].
func (p *PipeCall) Exec(ctx context.Context) error {
	r, err := p.request()
	if err != nil {
		return err
	}
	_, err = p.c.do(ctx, r)
	return err
}
