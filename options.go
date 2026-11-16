package wavehouse

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// An Option configures a [Client] in [New]. Options are applied in order;
// New validates the result and returns an error for a value or a combination
// it cannot honour, rather than silently picking one.
type Option func(*options)

// options is what the Option functions write. New validates it once, so an
// Option never has to report an error itself.
type options struct {
	token       *string
	tokenSource func(context.Context) (string, error)
	sourceSet   bool
	operatorKey *string
	httpClient  *http.Client
	clientSet   bool
	headers     http.Header
	headerFunc  func(context.Context) (http.Header, error)
	funcSet     bool
	retry       RetryPolicy
	userAgent   string
	errs        []error
}

// WithToken authenticates every request with a fixed JWT, sent as
// "Authorization: Bearer <token>". An empty token is an error in [New], so an
// unset environment variable can't quietly turn into an unauthenticated
// client. For a token that expires, use [WithTokenSource].
func WithToken(token string) Option {
	return func(o *options) { o.token = &token }
}

// WithTokenSource authenticates with a token fetched by fn. The contract:
//
//   - fn runs before every attempt, so a retry and each stream (re)connect
//     call it again, and a token that changed in between is the one sent.
//   - fn must be cheap. It sits on the path of every request, so caching,
//     refreshing and de-duplicating concurrent refreshes are fn's job, not the
//     SDK's.
//   - A returned "" sends no Authorization header; a returned error fails the
//     attempt without sending it.
func WithTokenSource(fn func(ctx context.Context) (string, error)) Option {
	return func(o *options) { o.tokenSource, o.sourceSet = fn, true }
}

// WithOperatorKey authenticates as the server's platform operator, sent as
// "Authorization: Operator <key>". The operator key reaches every route,
// /v1/ops/* included, without a JWT. It can't be combined with [WithToken] or
// [WithTokenSource]: New returns an error.
func WithOperatorKey(key string) Option {
	return func(o *options) { o.operatorKey = &key }
}

// WithHTTPClient sets the *http.Client requests go through, for a custom
// transport, proxy or TLS setup. The default has no overall Timeout, because
// streams are long-lived: bound each call with its ctx instead. The client's
// own CheckRedirect still applies to requests that carry no credential; a
// credentialed request never follows a redirect (see [ErrRedirect]).
func WithHTTPClient(hc *http.Client) Option {
	return func(o *options) { o.httpClient, o.clientSet = hc, true }
}

// WithHeader adds a header to every request, REST calls and the stream
// connection alike. It is the lowest layer: [WithHeaderFunc] and a per-call
// header replace it, key by key, case-insensitively. The headers the SDK sets
// itself (Content-Type, Accept and User-Agent, and Authorization when the SDK
// has a token or operator key to send) win: a value given here for one of them
// is replaced, or dropped where the SDK sends none. With no SDK credential, an
// Authorization given here is sent as it is.
// Every other header is sent unchanged. A later WithHeader for the same key replaces the earlier one. Any
// custom header counts as a credential when deciding whether a redirect may be
// followed.
func WithHeader(key, value string) Option {
	return func(o *options) {
		if checkHeader(key, value) != nil {
			o.errs = append(o.errs, fmt.Errorf("WithHeader(%q): invalid header name or value", key))
			return
		}
		if o.headers == nil {
			o.headers = http.Header{}
		}
		o.headers.Set(key, value)
	}
}

// WithHeaderFunc adds headers computed by fn, for credentials that rotate. It
// follows the same contract as [WithTokenSource]: fn runs before every
// attempt, retries and each stream (re)connect included, must be cheap, and
// does its own caching. An error from fn fails the attempt without sending
// it, like a token source error: [Client.Subscribe] returns it from the first
// connect and retries it once the stream has been live.
//
// A header from fn replaces the same header from [WithHeader], and a per-call
// header ([Query.Header], [PipeCall.Header], [SQLCall.Header],
// SubscribeOptions.Header) replaces fn's. The SDK's own headers still win;
// see [WithHeader]. Like WithHeader, any header from fn counts as a
// credential when deciding whether a redirect may be followed. A later
// WithHeaderFunc replaces an earlier one.
func WithHeaderFunc(fn func(ctx context.Context) (http.Header, error)) Option {
	return func(o *options) { o.headerFunc, o.funcSet = fn, true }
}

// WithRetry replaces the retry policy. Zero fields keep their defaults; a
// negative field, or a MinBackoff above MaxBackoff, is an error in [New].
func WithRetry(p RetryPolicy) Option {
	return func(o *options) { o.retry = p }
}

// WithUserAgent prepends ua to the SDK's own User-Agent,
// "wavehouse-sdk-go/<version>", to identify the application in server logs.
func WithUserAgent(ua string) Option {
	return func(o *options) { o.userAgent = ua }
}

// RetryPolicy says how often and how patiently a failed call is retried.
// Which failures are retried at all depends on the call; see [Client].
type RetryPolicy struct {
	// MaxAttempts is the total number of attempts, the first included.
	// Default 3; 1 disables retries.
	MaxAttempts int
	// MinBackoff is the wait before the first retry. Each later wait doubles,
	// with ±20% jitter. Default 500ms.
	MinBackoff time.Duration
	// MaxBackoff caps every wait, a server's Retry-After included.
	// Default 30s.
	MaxBackoff time.Duration
	// AttemptTimeout bounds each attempt on its own, so one that stalls is
	// retried instead of holding the whole call. 0, the default, sets none.
	// It covers a REST call from sending to the end of the body, and each
	// stream (re)connect up to the server's response headers; an established
	// stream is not cut off. The caller's ctx is separate: cancelling it or
	// reaching its deadline ends the call at once and is never retried.
	//
	// An attempt that hits the limit is retried after a backoff, within
	// MaxAttempts, under the same rules as any transport error: a raw SQL
	// call or a pipe call is not resent once the request may have reached the
	// server, which is any timeout after its headers were written; one that
	// hit the limit while still connecting is resent. The final
	// error says "attempt timeout" with the attempt count and matches
	// [ErrAttemptTimeout]; it does not match [context.DeadlineExceeded].
	AttemptTimeout time.Duration
}

const (
	defaultMaxAttempts = 3
	defaultMinBackoff  = 500 * time.Millisecond
	defaultMaxBackoff  = 30 * time.Second
)

func (p RetryPolicy) withDefaults() (RetryPolicy, error) {
	if p.MaxAttempts < 0 || p.MinBackoff < 0 || p.MaxBackoff < 0 || p.AttemptTimeout < 0 {
		return p, fmt.Errorf("WithRetry: negative field in %+v", p)
	}
	if p.MaxAttempts == 0 {
		p.MaxAttempts = defaultMaxAttempts
	}
	if p.MinBackoff == 0 {
		p.MinBackoff = defaultMinBackoff
	}
	if p.MaxBackoff == 0 {
		p.MaxBackoff = defaultMaxBackoff
	}
	if p.MinBackoff > p.MaxBackoff {
		return p, fmt.Errorf("WithRetry: MinBackoff %v exceeds MaxBackoff %v", p.MinBackoff, p.MaxBackoff)
	}
	return p, nil
}

// validate reports every problem at once, so a misconfigured client is fixed
// in one round.
func (o *options) validate() error {
	errs := o.errs
	credentials := 0
	if o.token != nil {
		credentials++
		if *o.token == "" {
			errs = append(errs, errors.New("WithToken: empty token"))
		}
	}
	if o.sourceSet {
		credentials++
		if o.tokenSource == nil {
			errs = append(errs, errors.New("WithTokenSource: nil function"))
		}
	}
	if o.operatorKey != nil {
		credentials++
		if *o.operatorKey == "" {
			errs = append(errs, errors.New("WithOperatorKey: empty key"))
		}
	}
	if credentials > 1 {
		errs = append(errs, errors.New("WithToken, WithTokenSource and WithOperatorKey are mutually exclusive"))
	}
	if o.funcSet && o.headerFunc == nil {
		errs = append(errs, errors.New("WithHeaderFunc: nil function"))
	}
	if o.clientSet && o.httpClient == nil {
		errs = append(errs, errors.New("WithHTTPClient: nil client"))
	}
	if !validHeaderValue(o.userAgent) {
		errs = append(errs, errors.New("WithUserAgent: invalid characters"))
	}
	var err error
	if o.retry, err = o.retry.withDefaults(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// validHeaderName reports whether name is an RFC 9110 token.
func validHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for i := range len(name) {
		c := name[i]
		switch {
		case 'a' <= c && c <= 'z', 'A' <= c && c <= 'Z', '0' <= c && c <= '9':
		case strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0:
		default:
			return false
		}
	}
	return true
}
