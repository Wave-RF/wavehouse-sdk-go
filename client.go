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
	"regexp"
	"runtime/debug"
	"strings"
	"sync"
)

// Client talks to one WaveHouse server. It is safe for concurrent use.
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
	retry       RetryPolicy
	userAgent   string
}

// New returns a client for the server at baseURL, an http or https URL with no
// query or fragment; a path prefix is kept and a trailing slash trimmed. A
// baseURL with no scheme gets one: http for a loopback host (localhost,
// 127.0.0.0/8, ::1, *.localhost), https for any other, and an explicit scheme
// is never changed. With no options the client is unauthenticated, retries reads up to
// 3 attempts, and uses an *http.Client with no overall timeout.
func New(baseURL string, opts ...Option) (*Client, error) {
	u, err := url.Parse(withScheme(baseURL))
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

var (
	schemePrefix  = regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9+.-]*://`)
	loopbackIPv4  = regexp.MustCompile(`^127(\.[0-9]{1,3}){3}$`)
	authorityEnds = regexp.MustCompile(`[/?#]`)
	portSuffix    = regexp.MustCompile(`:[0-9]*$`)
)

// withScheme gives a scheme-less base URL one, working on the string rather
// than through url.URL, which would not tell "localhost:8080" from a scheme.
// The JS SDK applies the same rule, so one input resolves to one base in both.
func withScheme(in string) string {
	in = strings.TrimSpace(in)
	if schemePrefix.MatchString(in) {
		return in
	}
	rest := strings.TrimLeft(in, "/")
	if rest == "" {
		return rest
	}
	authority := rest
	if loc := authorityEnds.FindStringIndex(rest); loc != nil {
		authority = rest[:loc[0]]
	}
	if i := strings.LastIndex(authority, "@"); i >= 0 {
		authority = authority[i+1:]
	}
	if authority == "::1" {
		return "http://[::1]" + rest[len(authority):]
	}
	host := portSuffix.ReplaceAllString(authority, "")
	if strings.HasPrefix(authority, "[") {
		host = authority[:strings.Index(authority+"]", "]")+1]
	}
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	if host == "localhost" || strings.HasSuffix(host, ".localhost") || host == "[::1]" || loopbackIPv4.MatchString(host) {
		return "http://" + rest
	}
	return "https://" + rest
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
