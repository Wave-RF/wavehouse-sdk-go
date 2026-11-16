package wavehouse

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAPIErrorFromServerBody(t *testing.T) {
	rec := newRecorder(t, reply(http.StatusBadRequest,
		`{"error":"Code: 62. DB::Exception: Syntax error","code":"clickhouse.rejected","retryable":false}`,
		"X-Request-Id", "4f2a9c", "Retry-After", "5"))
	_, err := rec.client(t).Tables(t.Context())

	ae, ok := errors.AsType[*APIError](err)
	require.True(t, ok, "err = %v, want *APIError", err)
	assert.Equal(t, APIError{
		StatusCode: 400, Code: "clickhouse.rejected", Message: "Code: 62. DB::Exception: Syntax error",
		Retryable: false, fromServer: true, RetryAfter: 5 * time.Second, RequestID: "4f2a9c", Method: "GET", Path: "/v1/ops/schema",
	}, *ae)
	assert.EqualError(t, err, "wavehouse: GET /v1/ops/schema: 400 clickhouse.rejected: Code: 62. DB::Exception: Syntax error (request 4f2a9c)")
	assert.ErrorIs(t, err, ErrBadRequest)
	assert.NotErrorIs(t, err, ErrNotFound)
	assert.False(t, IsRetryable(err))
}

func TestAPIErrorFallbacks(t *testing.T) {
	cases := []struct {
		status    int
		body      string
		message   string
		retryable bool
		// fromServer: the verdict came from the body, which is what lets a pipe call trust it.
		fromServer bool
	}{
		{502, `<html>bad gateway</html>`, "Bad Gateway", true, false},
		{404, `{"error":"table not found"}`, "table not found", false, false},
		{408, ``, "Request Timeout", true, false},
		{429, `[]`, "Too Many Requests", true, false},
		{503, `{"error":"x","retryable":false}`, "x", false, true},
		{400, `{"error":"x","retryable":true}`, "x", true, true},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprint(tc.status), func(t *testing.T) {
			rec := newRecorder(t, func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			})
			_, err := rec.client(t).Tables(t.Context())
			ae, ok := errors.AsType[*APIError](err)
			require.True(t, ok, "err = %v", err)
			assert.Equal(t, APIError{StatusCode: tc.status, Message: tc.message, Retryable: tc.retryable, fromServer: tc.fromServer, Method: "GET", Path: "/v1/ops/schema"}, *ae)
			// httptest sends no X-Request-Id, so there is no "(request …)".
			assert.EqualError(t, err, fmt.Sprintf("wavehouse: GET /v1/ops/schema: %d: %s", tc.status, tc.message))
		})
	}
}

func TestSentinels(t *testing.T) {
	sentinels := []error{ErrBadRequest, ErrUnauthorized, ErrForbidden, ErrNotFound, ErrRateLimited, ErrUnavailable}
	for i, status := range []int{400, 401, 403, 404, 429, 503} {
		err := fmt.Errorf("wrapped: %w", &APIError{StatusCode: status})
		for j, s := range sentinels {
			assert.Equal(t, i == j, errors.Is(err, s), "errors.Is(%d, %v)", status, s)
		}
		assert.NotErrorIs(t, err, ErrRedirect)
	}
	for _, s := range sentinels {
		assert.NotErrorIs(t, &APIError{StatusCode: 500}, s)
	}
}

func TestIsRetryable(t *testing.T) {
	cases := map[string]struct {
		err  error
		want bool
	}{
		"nil":                {nil, false},
		"plain":              {errors.New("x"), false},
		"retryable api":      {fmt.Errorf("w: %w", &APIError{StatusCode: 503, Retryable: true}), true},
		"non-retryable api":  {&APIError{StatusCode: 400}, false},
		"retryable request":  {&requestError{method: "GET", path: "/", err: errReset, retryable: true}, true},
		"refused request":    {&requestError{method: "POST", path: "/", err: errReset}, false},
		"redirect":           {&redirectError{method: "GET", path: "/", status: 302, location: "x"}, false},
		"request, cancelled": {&requestError{err: fmt.Errorf("x: %w", context.Canceled), retryable: true}, false},
	}
	for name, tc := range cases {
		assert.Equal(t, tc.want, IsRetryable(tc.err), name)
	}
}
