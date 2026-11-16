package wavehouse

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// APIError is a non-2xx answer from the server. Match a status with errors.Is
// and the sentinels below, or read the fields with errors.As.
type APIError struct {
	// StatusCode is the HTTP status.
	StatusCode int
	// Code is the server's machine-readable "code", such as
	// "clickhouse.rejected" or "clickhouse.unavailable"; "" when the body
	// carries none.
	Code string
	// Message is the server's "error" text, or http.StatusText when the body
	// has none.
	Message string
	// Retryable is the server's "retryable" verdict when the body carries
	// one; otherwise true for 408, 429 and every 5xx.
	Retryable bool
	// fromServer is whether Retryable came from the body rather than the status.
	fromServer bool
	// RetryAfter is the parsed Retry-After header (seconds or an HTTP-date),
	// 0 when absent.
	RetryAfter time.Duration
	// RequestID is the X-Request-Id the server logged the request under.
	// Quote it when reporting a failure.
	RequestID string
	// Method and Path are the request's, for the message.
	Method, Path string
}

// Error renders "wavehouse: POST /v1/query: 400 clickhouse.rejected:
// <message> (request <id>)", leaving out a code or request id it lacks.
func (e *APIError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "wavehouse: %s %s: %d", e.Method, e.Path, e.StatusCode)
	if e.Code != "" {
		b.WriteString(" " + e.Code)
	}
	b.WriteString(": " + e.Message)
	if e.RequestID != "" {
		b.WriteString(" (request " + e.RequestID + ")")
	}
	return b.String()
}

// Is reports whether target is the sentinel for e's status, so
// errors.Is(err, ErrNotFound) matches any 404.
func (e *APIError) Is(target error) bool {
	status, ok := sentinelStatus[target]
	return ok && e.StatusCode == status
}

// Sentinels for the statuses a caller most often branches on, matched by
// [APIError.Is].
var (
	ErrBadRequest   = errors.New("wavehouse: bad request")  // 400
	ErrUnauthorized = errors.New("wavehouse: unauthorized") // 401
	ErrForbidden    = errors.New("wavehouse: forbidden")    // 403
	ErrNotFound     = errors.New("wavehouse: not found")    // 404
	ErrRateLimited  = errors.New("wavehouse: rate limited") // 429
	ErrUnavailable  = errors.New("wavehouse: unavailable")  // 503
)

// ErrRedirect is matched by the error a call returns when the server answers
// with a redirect while the request carries a credential (a token, the
// operator key or a custom header). The SDK never follows one, because the
// next hop would receive the credential. The error names the Location.
var ErrRedirect = errors.New("wavehouse: redirect refused")

// ErrAttemptTimeout is matched by the error of a call whose last attempt hit
// [RetryPolicy].AttemptTimeout. It is not the caller's own deadline: such an
// error doesn't match [context.DeadlineExceeded].
var ErrAttemptTimeout = errors.New("wavehouse: attempt timeout")

var sentinelStatus = map[error]int{
	ErrBadRequest:   http.StatusBadRequest,
	ErrUnauthorized: http.StatusUnauthorized,
	ErrForbidden:    http.StatusForbidden,
	ErrNotFound:     http.StatusNotFound,
	ErrRateLimited:  http.StatusTooManyRequests,
	ErrUnavailable:  http.StatusServiceUnavailable,
}

// IsRetryable reports whether err is worth retrying later: an [*APIError]
// whose Retryable is set, or a transport error the SDK itself would have
// retried for that call. A cancelled or expired context is never retryable.
func IsRetryable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if e, ok := errors.AsType[*APIError](err); ok {
		return e.Retryable
	}
	if e, ok := errors.AsType[*requestError](err); ok {
		return e.retryable
	}
	return false
}

// requestError is a request that got no HTTP answer: a dial failure, a reset
// connection, a cancelled ctx. It unwraps to the cause, so *url.Error and
// errors.Is(err, context.Canceled) work.
type requestError struct {
	method, path string
	err          error
	// retryable is whether resending was safe for this call, for IsRetryable.
	retryable bool
	// timedOut is an attempt that hit RetryPolicy.AttemptTimeout, timeout
	// long, after attempts attempts.
	timedOut bool
	timeout  time.Duration
	attempts int
}

func (e *requestError) Error() string {
	if e.timedOut {
		return fmt.Sprintf("wavehouse: %s %s: attempt timeout (%v each) after %d attempt(s): %s",
			e.method, e.path, e.timeout, max(e.attempts, 1), e.err)
	}
	return "wavehouse: " + e.method + " " + e.path + ": " + e.err.Error()
}

func (e *requestError) Unwrap() error { return e.err }

func (e *requestError) Is(target error) bool { return e.timedOut && target == ErrAttemptTimeout }

// redirectError is a refused redirect; it matches ErrRedirect.
type redirectError struct {
	method, path string
	status       int
	location     string
}

func (e *redirectError) Error() string {
	return fmt.Sprintf("wavehouse: %s %s: refused to follow a %d redirect to %q while the request carries a credential",
		e.method, e.path, e.status, e.location)
}

func (e *redirectError) Is(target error) bool { return target == ErrRedirect }
