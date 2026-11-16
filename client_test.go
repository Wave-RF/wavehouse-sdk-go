package wavehouse

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewBaseURL(t *testing.T) {
	for in, want := range map[string]string{
		"http://localhost:8080":       "http://localhost:8080",
		"https://wh.example.com/":     "https://wh.example.com",
		"https://example.com/prefix/": "https://example.com/prefix",
	} {
		c, err := New(in)
		if assert.NoError(t, err, in) {
			assert.Equal(t, want, c.baseURL, in)
		}
	}
	for _, bad := range []string{"", "localhost:8080", "ftp://host", "http://", "http://h/?a=1", "http://h/?", "http://h/#f", "http://h/%zz"} {
		_, err := New(bad)
		assert.Error(t, err, bad)
	}
}

func TestNewOptionErrors(t *testing.T) {
	src := func(context.Context) (string, error) { return "t", nil }
	cases := map[string][]Option{
		"empty token":        {WithToken("")},
		"nil token source":   {WithTokenSource(nil)},
		"empty operator key": {WithOperatorKey("")},
		"token and key":      {WithToken("t"), WithOperatorKey("k")},
		"source and key":     {WithTokenSource(src), WithOperatorKey("k")},
		"token and source":   {WithToken("t"), WithTokenSource(src)},
		"nil http client":    {WithHTTPClient(nil)},
		"bad header name":    {WithHeader("Bad Header", "v")},
		"empty header name":  {WithHeader("", "v")},
		"header value CRLF":  {WithHeader("X-A", "v\r\nX-B: w")},
		"negative attempts":  {WithRetry(RetryPolicy{MaxAttempts: -1})},
		"negative backoff":   {WithRetry(RetryPolicy{MinBackoff: -time.Second})},
		"min above max":      {WithRetry(RetryPolicy{MinBackoff: time.Minute, MaxBackoff: time.Second})},
		"min above default":  {WithRetry(RetryPolicy{MinBackoff: time.Hour})},
		"user agent newline": {WithUserAgent("app\n")},
	}
	for name, opts := range cases {
		_, err := New("http://localhost", opts...)
		require.Error(t, err, name)
		assert.Contains(t, err.Error(), "wavehouse: New: ", name)
	}
}

func TestNewReportsEveryProblem(t *testing.T) {
	_, err := New("http://localhost", WithToken(""), WithHTTPClient(nil))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "WithToken")
	assert.Contains(t, err.Error(), "WithHTTPClient")
}

func TestNewDefaults(t *testing.T) {
	c, err := New("http://localhost", nil) // a nil Option is skipped
	require.NoError(t, err)
	assert.Equal(t, RetryPolicy{MaxAttempts: 3, MinBackoff: 500 * time.Millisecond, MaxBackoff: 30 * time.Second}, c.retry)
	assert.Zero(t, c.hc.Timeout, "streams need a client with no overall timeout")
	assert.Equal(t, "wavehouse-sdk-go/devel", c.userAgent)

	c, err = New("http://localhost", WithRetry(RetryPolicy{MaxAttempts: 1, MaxBackoff: time.Second}))
	require.NoError(t, err)
	assert.Equal(t, RetryPolicy{MaxAttempts: 1, MinBackoff: 500 * time.Millisecond, MaxBackoff: time.Second}, c.retry)
}

func TestWithHTTPClientKeepsCallerRedirectPolicy(t *testing.T) {
	called := false
	hc := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { called = true; return nil }}
	c, err := New("http://localhost", WithHTTPClient(hc))
	require.NoError(t, err)
	require.Same(t, hc, c.hc)
	_ = hc.CheckRedirect(nil, nil)
	assert.True(t, called, "caller's CheckRedirect replaced")
	assert.ErrorIs(t, c.noRedirect.CheckRedirect(nil, nil), http.ErrUseLastResponse)
}

func TestVersionOrDevel(t *testing.T) {
	for in, want := range map[string]string{"": "devel", "(devel)": "devel", "v1.2.3": "v1.2.3"} {
		assert.Equal(t, want, versionOrDevel(in), in)
	}
}

func TestWithTenantCopyIsolation(t *testing.T) {
	rec := newRecorder(t, nil)
	base := rec.client(t, WithTenant("acme"))
	other := base.WithTenant("globex")
	tenant0 := other.WithTenant("")

	for _, tc := range []struct {
		c    *Client
		want string
	}{{base, "acme"}, {other, "globex"}, {tenant0, ""}, {base, "acme"}} {
		require.NoError(t, tc.c.Health(t.Context()))
		assert.Equal(t, tc.want, rec.last(t).header.Get("X-Tenant-ID"))
	}
	assert.Same(t, base.hc, other.hc, "WithTenant copy does not share the http client")
}
