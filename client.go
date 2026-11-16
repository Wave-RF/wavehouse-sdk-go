// Package wavehouse is the Go client for WaveHouse, a schema-aware HTTP and
// SSE gateway in front of ClickHouse.
//
// Create one [Client] with [New] and share it: it is safe for concurrent use.
// Every call that does I/O takes a context.Context first; bound or cancel the
// call with it. Failures are returned as errors to inspect with errors.Is and
// errors.As: an [*APIError] for an answer the server gave, a wrapped transport
// error otherwise.
//
// JSON responses are decoded with json.Decoder.UseNumber, so a number decoded
// into an any (a map[string]any row, say) is a [encoding/json.Number] that
// keeps 64-bit integers exact. Struct fields decode as usual.
package wavehouse

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"runtime/debug"
	"strings"
	"sync"
)

// Client talks to one WaveHouse server. It is safe for concurrent use, and a
// copy from [Client.WithTenant] shares its connections.
//
// Failed calls are retried per the [RetryPolicy], but only where resending is
// safe: a read is retried after a transport error or a retryable [*APIError];
// a call that may write retries a transport error only when the request never
// left (a dial failure). Each call's doc says which kind it is. A server's
// Retry-After is honoured, capped at RetryPolicy.MaxBackoff, and cancelling
// the ctx ends a wait at once.
type Client struct {
	baseURL string
	hc      *http.Client
	// noRedirect is hc with redirects refused, for requests that carry a credential.
	noRedirect  *http.Client
	token       string
	tokenSource func(context.Context) (string, error)
	operatorKey string
	headers     http.Header
	tenant      string
	retry       RetryPolicy
	userAgent   string
}

// New returns a client for the server at baseURL, an absolute http or https
// URL with no query or fragment; a path prefix is kept and a trailing slash
// trimmed. With no options the client is unauthenticated, retries reads up to
// 3 attempts, and uses an *http.Client with no overall timeout.
func New(baseURL string, opts ...Option) (*Client, error) {
	u, err := url.Parse(baseURL)
	switch {
	case err != nil:
		return nil, fmt.Errorf("wavehouse: New: base URL: %w", err)
	case u.Scheme != "http" && u.Scheme != "https":
		return nil, fmt.Errorf("wavehouse: New: base URL %q: scheme must be http or https", baseURL)
	case u.Host == "":
		return nil, fmt.Errorf("wavehouse: New: base URL %q: no host", baseURL)
	case u.RawQuery != "" || u.ForceQuery || u.Fragment != "":
		return nil, fmt.Errorf("wavehouse: New: base URL %q: must not carry a query or fragment", baseURL)
	}

	var o options
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if err := o.validate(); err != nil {
		return nil, fmt.Errorf("wavehouse: New: %w", err)
	}

	hc := o.httpClient
	if hc == nil {
		hc = &http.Client{}
	}
	// A copy, so the caller's client keeps its own CheckRedirect for uncredentialed requests.
	noRedirect := *hc
	noRedirect.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

	ua := "wavehouse-sdk-go/" + sdkVersion()
	if o.userAgent != "" {
		ua = o.userAgent + " " + ua
	}
	c := &Client{
		baseURL:     strings.TrimRight(u.String(), "/"),
		hc:          hc,
		noRedirect:  &noRedirect,
		tokenSource: o.tokenSource,
		headers:     o.headers,
		tenant:      o.tenant,
		retry:       o.retry,
		userAgent:   ua,
	}
	if o.token != nil {
		c.token = *o.token
	}
	if o.operatorKey != nil {
		c.operatorKey = *o.operatorKey
	}
	return c, nil
}

// WithTenant returns a copy of c scoped to tenant id, sharing c's connections
// and configuration, like [log/slog.Logger.With]. c itself is unchanged. An
// empty id addresses the default tenant 0. See [WithTenant] for how the
// tenant is sent.
func (c *Client) WithTenant(id string) *Client {
	cp := *c
	cp.tenant = id
	return &cp
}

const modulePath = "github.com/Wave-RF/wavehouse-sdk-go"

// sdkVersion is this module's version as the importing binary records it, so
// a release needs no constant bumped; "devel" inside this repository.
var sdkVersion = sync.OnceValue(func() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return "devel"
	}
	if bi.Main.Path == modulePath {
		return versionOrDevel(bi.Main.Version)
	}
	for _, dep := range bi.Deps {
		if dep.Path == modulePath {
			if dep.Replace != nil {
				return versionOrDevel(dep.Replace.Version)
			}
			return versionOrDevel(dep.Version)
		}
	}
	return "devel"
})

func versionOrDevel(v string) string {
	if v == "" || v == "(devel)" {
		return "devel"
	}
	return v
}
