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
	// request never left, and an APIError only on the server's own
	// retryable:true, a 429, or a 503 with Retry-After — never a bare 5xx,
	// which a proxy can send after the write ran.
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
	// callHeader is the caller's per-call headers: sent over WithHeader and
	// WithHeaderFunc, under Authorization and the SDK's own.
	callHeader http.Header
	// header is the SDK's own protocol header (Last-Event-ID), sent over the
	// caller's but under Authorization.
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
			noteAttempts(err, attempt)
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

// attempt sends r once and buffers the answer. With an AttemptTimeout, the
// whole exchange, body included, has that long.
func (c *Client) attempt(ctx context.Context, r *request) (*response, error) {
	actx := ctx
	if d := c.retry.AttemptTimeout; d > 0 {
		var cancel context.CancelFunc
		actx, cancel = context.WithTimeoutCause(ctx, d, ErrAttemptTimeout)
		defer cancel()
	}
	res, err := c.send(ctx, actx, r)
	if err != nil {
		return nil, err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode/100 == 2 {
		body, err := io.ReadAll(res.Body)
		if err != nil {
			return nil, c.transportError(ctx, actx, r, err)
		}
		return &response{status: res.StatusCode, header: res.Header, body: body}, nil
	}
	body, _ := io.ReadAll(io.LimitReader(res.Body, maxErrorBody))
	return &response{status: res.StatusCode, header: res.Header, body: body}, newAPIError(r, res, body)
}

// send makes one HTTP request and returns any HTTP answer with its body
// unread, except a redirect while credentialed, which is an error. The
// caller closes the body.
//
// The request itself runs under actx, a child of the caller's ctx that may
// carry the attempt's deadline. The token source and the header func run under
// ctx: they are meant to be cheap, and the deadline bounds the HTTP exchange.
func (c *Client) send(ctx, actx context.Context, r *request) (*http.Response, error) {
	var body io.Reader
	if r.body != nil {
		body = bytes.NewReader(r.body)
	}
	req, err := http.NewRequestWithContext(actx, r.method, c.url(r), body)
	if err != nil {
		return nil, fmt.Errorf("wavehouse: %s %s: %w", r.method, r.path, err)
	}

	// Lowest to highest: WithHeader, WithHeaderFunc, the call's own headers;
	// then the SDK's own below win. A later layer replaces a key, never joins.
	custom := len(c.headers) > 0 || len(r.callHeader) > 0
	for k, v := range c.headers {
		req.Header[k] = slices.Clone(v)
	}
	if c.headerFunc != nil {
		h, err := c.headerFunc(ctx)
		if err == nil {
			err = checkHeaders(h)
		}
		if err != nil {
			return nil, fmt.Errorf("wavehouse: %s %s: %w: %w", r.method, r.path, errHeaderFunc, err)
		}
		custom = custom || len(h) > 0
		setHeaders(req.Header, h)
	}
	setHeaders(req.Header, r.callHeader)
	setHeaders(req.Header, r.header)
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
	auth, err := c.authorization(ctx)
	if err != nil {
		return nil, fmt.Errorf("wavehouse: %s %s: %w: %w", r.method, r.path, errTokenSource, err)
	}
	req.Header.Del("Authorization")
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}

	credentialed := auth != "" || custom
	hc := c.hc
	if credentialed {
		hc = c.noRedirect
	}
	res, err := hc.Do(req)
	if err != nil {
		return nil, c.transportError(ctx, actx, r, err)
	}
	if credentialed && res.StatusCode/100 == 3 {
		_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, maxErrorBody))
		_ = res.Body.Close()
		return nil, &redirectError{method: r.method, path: r.path, status: res.StatusCode, location: res.Header.Get("Location")}
	}
	return res, nil
}

// errTokenSource and errHeaderFunc mark a failure of the WithTokenSource or
// WithHeaderFunc function, which a stream retries once it has been live.
var (
	errTokenSource = errors.New("token source")
	errHeaderFunc  = errors.New("header func")
)

// setHeaders replaces each key of h in dst, canonicalising the key so
// "x-a" and "X-A" are one header.
func setHeaders(dst, h http.Header) {
	for k, v := range h {
		dst[http.CanonicalHeaderKey(k)] = slices.Clone(v)
	}
}

// checkHeaders reports a name or value that can't be sent.
func checkHeaders(h http.Header) error {
	for k, vs := range h {
		for _, v := range vs {
			if err := checkHeader(k, v); err != nil {
				return err
			}
		}
	}
	return nil
}

// checkHeader reports a header name or value that can't be sent.
func checkHeader(key, value string) error {
	if !validHeaderName(key) || strings.ContainsAny(value, "\r\n\x00") {
		return fmt.Errorf("invalid header name or value for %q", key)
	}
	return nil
}

func (c *Client) url(r *request) string {
	u := c.baseURL + r.path
	if len(r.query) > 0 {
		u += "?" + r.query.Encode()
	}
	return u
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

// transportError classifies a failed exchange. ctx is the caller's, actx the
// attempt's. An attempt that hit its own deadline while the caller's ctx is
// live is retryable like any transport error, with the same rule for calls
// that may write: only a dial that never connected is safe to resend. Its
// cause is flattened to text, so it doesn't match context.DeadlineExceeded,
// which is the caller's own deadline.
func (c *Client) transportError(ctx, actx context.Context, r *request, err error) error {
	retryable := ctx.Err() == nil
	if r.kind == callPipe || r.kind == callSQL {
		retryable = retryable && isDialError(err)
	}
	re := &requestError{method: r.method, path: r.path, err: err, retryable: retryable}
	if ctx.Err() == nil && errors.Is(context.Cause(actx), ErrAttemptTimeout) {
		re.timedOut = true
		re.timeout = c.retry.AttemptTimeout
		re.err = errors.New(err.Error())
	}
	return re
}

// noteAttempts records how many attempts a failed call made, for the message
// of an attempt timeout.
func noteAttempts(err error, n int) {
	if re, ok := errors.AsType[*requestError](err); ok && re.timedOut {
		re.attempts = n
	}
}

// isDialError reports a failure to connect, after which the request
// certainly never reached the server.
func isDialError(err error) bool {
	op, ok := errors.AsType[*net.OpError](err)
	return ok && op.Op == "dial"
}

func shouldRetry(kind callKind, err error) bool {
	if ae, ok := errors.AsType[*APIError](err); ok {
		switch kind {
		case callSQL:
			return false
		case callPipe:
			return ae.Retryable && (ae.fromServer || ae.StatusCode == http.StatusTooManyRequests ||
				(ae.StatusCode == http.StatusServiceUnavailable && ae.RetryAfter > 0))
		case callRead, callIngest:
		}
		return ae.Retryable
	}
	if re, ok := errors.AsType[*requestError](err); ok {
		return re.retryable
	}
	return false
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
			e.Retryable, e.fromServer = *env.Retryable, true
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
