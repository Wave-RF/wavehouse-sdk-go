package wavehouse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

// callKind decides which failures a call may retry: resending is only safe
// where the server can't have acted twice.
type callKind int

const (
	// callRead covers GETs, /v1/query and the schema refresh: retry any
	// transport error and any retryable APIError.
	callRead callKind = iota
	// callIngest is at-least-once, and the server's dedupe ids make it
	// exactly-once, so it retries like a read.
	callIngest
	// callPipe may run a write pipe: retry a transport error only when the
	// request never left; the server already marks a write pipe's errors
	// retryable:false.
	callPipe
	// callSQL may mutate: retry only a dial error, never an APIError.
	callSQL
)

// maxErrorBody bounds what is read of a non-2xx body: an error envelope or a
// reload's findings, never a result set.
const maxErrorBody = 1 << 20

// request describes one call. Its body is a byte slice so every attempt
// resends identical bytes.
type request struct {
	kind        callKind
	method      string
	path        string // escaped, starting with "/"
	query       url.Values
	body        []byte // nil sends no body
	contentType string // required with a body
	accept      string // "" means application/json
	// header is sent over WithHeader's headers and under the SDK's own.
	header http.Header
}

// response is a buffered answer: a 2xx, or the non-2xx behind an APIError.
type response struct {
	status int
	header http.Header
	body   []byte
}

// doJSON runs r and decodes a 2xx body into dst (UseNumber); a nil dst
// discards it.
func (c *Client) doJSON(ctx context.Context, r *request, dst any) error {
	res, err := c.do(ctx, r)
	if err != nil {
		return err
	}
	if dst == nil {
		return nil
	}
	if err := decodeJSON(res.body, dst); err != nil {
		return decodeError(r, err)
	}
	return nil
}

func decodeError(r *request, err error) error {
	return fmt.Errorf("wavehouse: %s %s: decode response: %w", r.method, r.path, err)
}

func decodeJSON(body []byte, dst any) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	return dec.Decode(dst)
}

// do runs r with retries and returns the buffered 2xx response. On an
// APIError the non-2xx response is returned with it, for callers that read a
// failure's body (ReloadSettings).
func (c *Client) do(ctx context.Context, r *request) (*response, error) {
	for attempt := 1; ; attempt++ {
		res, err := c.attempt(ctx, r)
		if err == nil || attempt >= c.retry.MaxAttempts || ctx.Err() != nil || !shouldRetry(r.kind, err) {
			return res, err
		}
		wait := c.retry.backoff(attempt)
		if ae, ok := errors.AsType[*APIError](err); ok && ae.RetryAfter > 0 {
			wait = min(ae.RetryAfter, c.retry.MaxBackoff)
		}
		if werr := sleep(ctx, wait); werr != nil {
			// Only ctx's error is wrapped: errors.As must not find the APIError of a call that was abandoned.
			return nil, fmt.Errorf("wavehouse: %s %s: %w while waiting to retry after: %s", r.method, r.path, werr, err.Error())
		}
	}
}

// attempt sends r once and buffers the answer.
func (c *Client) attempt(ctx context.Context, r *request) (*response, error) {
	res, err := c.send(ctx, r)
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode/100 == 2 {
		body, err := io.ReadAll(res.Body)
		if err != nil {
			return nil, c.transportError(ctx, r, err)
		}
		return &response{status: res.StatusCode, header: res.Header, body: body}, nil
	}
	body, _ := io.ReadAll(io.LimitReader(res.Body, maxErrorBody))
	return &response{status: res.StatusCode, header: res.Header, body: body}, newAPIError(r, res, body)
}

// send makes one HTTP request and returns any HTTP answer with its body
// unread, except a redirect while credentialed, which is an error. The
// caller closes the body.
func (c *Client) send(ctx context.Context, r *request) (*http.Response, error) {
	var body io.Reader
	if r.body != nil {
		body = bytes.NewReader(r.body)
	}
	req, err := http.NewRequestWithContext(ctx, r.method, c.url(r), body)
	if err != nil {
		return nil, fmt.Errorf("wavehouse: %s %s: %w", r.method, r.path, err)
	}

	// Custom headers first, so the SDK's own below win.
	for k, v := range c.headers {
		req.Header[k] = slices.Clone(v)
	}
	for k, v := range r.header {
		req.Header[k] = slices.Clone(v)
	}
	req.Header.Set("User-Agent", c.userAgent)
	accept := r.accept
	if accept == "" {
		accept = "application/json"
	}
	req.Header.Set("Accept", accept)
	if r.body != nil {
		req.Header.Set("Content-Type", r.contentType)
	} else {
		req.Header.Del("Content-Type")
	}
	req.Header.Del("X-Tenant-ID")
	if c.tenant != "" && isDataRoute(r.path) {
		req.Header.Set("X-Tenant-ID", c.tenant)
	}
	auth, err := c.authorization(ctx)
	if err != nil {
		return nil, fmt.Errorf("wavehouse: %s %s: token source: %w", r.method, r.path, err)
	}
	req.Header.Del("Authorization")
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}

	credentialed := auth != "" || len(c.headers) > 0
	hc := c.hc
	if credentialed {
		hc = c.noRedirect
	}
	res, err := hc.Do(req)
	if err != nil {
		return nil, c.transportError(ctx, r, err)
	}
	if credentialed && res.StatusCode/100 == 3 {
		_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, maxErrorBody))
		_ = res.Body.Close()
		return nil, &redirectError{method: r.method, path: r.path, status: res.StatusCode, location: res.Header.Get("Location")}
	}
	return res, nil
}

func (c *Client) url(r *request) string {
	q := r.query
	if c.tenant != "" && isOpsRoute(r.path) {
		q = cloneValues(q)
		q.Set("tenant", c.tenant)
	}
	u := c.baseURL + r.path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	return u
}

// cloneValues copies q so adding the tenant never writes to a caller's values.
func cloneValues(q url.Values) url.Values {
	cp := make(url.Values, len(q)+1)
	for k, v := range q {
		cp[k] = slices.Clone(v)
	}
	return cp
}

func (c *Client) authorization(ctx context.Context) (string, error) {
	switch {
	case c.operatorKey != "":
		return "Operator " + c.operatorKey, nil
	case c.token != "":
		return "Bearer " + c.token, nil
	case c.tokenSource != nil:
		tok, err := c.tokenSource(ctx)
		if err != nil || tok == "" {
			return "", err
		}
		return "Bearer " + tok, nil
	}
	return "", nil
}

func (c *Client) transportError(ctx context.Context, r *request, err error) error {
	retryable := ctx.Err() == nil
	if r.kind == callPipe || r.kind == callSQL {
		retryable = retryable && isDialError(err)
	}
	return &requestError{method: r.method, path: r.path, err: err, retryable: retryable}
}

// isDialError reports a failure to connect, after which the request
// certainly never reached the server.
func isDialError(err error) bool {
	op, ok := errors.AsType[*net.OpError](err)
	return ok && op.Op == "dial"
}

func shouldRetry(kind callKind, err error) bool {
	if ae, ok := errors.AsType[*APIError](err); ok {
		return kind != callSQL && ae.Retryable
	}
	if re, ok := errors.AsType[*requestError](err); ok {
		return re.retryable
	}
	return false
}

func isOpsRoute(path string) bool {
	return path == "/v1/ops" || strings.HasPrefix(path, "/v1/ops/")
}

// isDataRoute is every /v1 route that resolves a tenant from X-Tenant-ID.
func isDataRoute(path string) bool {
	return strings.HasPrefix(path, "/v1/") && !isOpsRoute(path)
}

func newAPIError(r *request, res *http.Response, body []byte) *APIError {
	e := &APIError{
		StatusCode: res.StatusCode,
		Method:     r.method,
		Path:       r.path,
		RequestID:  res.Header.Get("X-Request-Id"),
		RetryAfter: parseRetryAfter(res.Header.Get("Retry-After"), time.Now()),
		Retryable: res.StatusCode == http.StatusRequestTimeout ||
			res.StatusCode == http.StatusTooManyRequests || res.StatusCode >= 500,
	}
	var env struct {
		Error     string `json:"error"`
		Code      string `json:"code"`
		Retryable *bool  `json:"retryable"`
	}
	if json.Unmarshal(body, &env) == nil {
		e.Message, e.Code = env.Error, env.Code
		if env.Retryable != nil {
			e.Retryable = *env.Retryable
		}
	}
	if e.Message == "" {
		e.Message = http.StatusText(res.StatusCode)
	}
	return e
}

// parseRetryAfter reads delta-seconds or an HTTP-date; 0 when absent,
// unparseable or already past.
func parseRetryAfter(v string, now time.Time) time.Duration {
	if v == "" {
		return 0
	}
	if secs, err := strconv.ParseInt(v, 10, 64); err == nil {
		if secs <= 0 {
			return 0
		}
		// Clamp rather than overflow into a negative duration.
		if secs > math.MaxInt64/int64(time.Second) {
			return math.MaxInt64
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil && t.After(now) {
		return t.Sub(now)
	}
	return 0
}

// backoff is the wait before retry n (1-based): MinBackoff doubled per
// retry, ±20% jitter so clients that failed together don't retry together,
// capped at MaxBackoff after the jitter.
func (p RetryPolicy) backoff(n int) time.Duration {
	d := float64(p.MinBackoff) * math.Pow(2, float64(n-1))
	d *= 0.8 + 0.4*rand.Float64() //nolint:gosec // jitter, not cryptography
	return time.Duration(min(d, float64(p.MaxBackoff)))
}

// sleep waits d or until ctx is done, returning ctx.Err() in that case.
func sleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
