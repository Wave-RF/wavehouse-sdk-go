package wavehouse

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// stall never answers until the attempt's context ends, as a request on a dead
// network path does.
func stall(r *http.Request) (*http.Response, error) {
	<-r.Context().Done()
	return nil, r.Context().Err()
}

// stallThen stalls the first n calls and then answers 200 with body.
func stallThen(n int, calls *atomic.Int32, body string) roundTripFunc {
	return func(r *http.Request) (*http.Response, error) {
		if int(calls.Add(1)) <= n {
			return stall(r)
		}
		return fakeResponse(r, 200, body), nil
	}
}

func TestAttemptTimeoutRetriesAStalledAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		c := fakeClient(t, stallThen(1, &calls, `[]`),
			WithRetry(RetryPolicy{AttemptTimeout: 2 * time.Second, MinBackoff: time.Second}))
		start := time.Now()
		_, err := c.From("t").Fetch(t.Context())
		require.NoError(t, err)
		assert.Equal(t, int32(2), calls.Load())
		// 2s to the deadline, then a backoff of 1s ±20%.
		assert.InDelta(t, float64(3*time.Second), float64(time.Since(start)), float64(300*time.Millisecond))
	})
}

func TestAttemptTimeoutOffByDefault(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		c := fakeClient(t, func(r *http.Request) (*http.Response, error) {
			calls.Add(1)
			time.Sleep(time.Hour)
			return fakeResponse(r, 200, `[]`), nil
		})
		_, err := c.From("t").Fetch(t.Context())
		require.NoError(t, err)
		assert.Equal(t, int32(1), calls.Load())
	})
}

func TestAttemptTimeoutAllAttemptsFail(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		c := fakeClient(t, stallThen(99, &calls, ``),
			WithRetry(RetryPolicy{MaxAttempts: 3, AttemptTimeout: time.Second}))
		err := c.Health(t.Context())
		require.ErrorIs(t, err, ErrAttemptTimeout)
		assert.Equal(t, int32(3), calls.Load())
		assert.Contains(t, err.Error(), "attempt timeout (1s each) after 3 attempt(s)")
		// Not the caller's own deadline.
		assert.NotErrorIs(t, err, context.DeadlineExceeded)
		assert.NotErrorIs(t, err, context.Canceled)
		assert.True(t, IsRetryable(err))
	})
}

func TestAttemptTimeoutCoversTheBody(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		c := fakeClient(t, func(r *http.Request) (*http.Response, error) {
			res := fakeResponse(r, 200, `[]`)
			if calls.Add(1) == 1 {
				res.Body = io.NopCloser(stalledBody{r.Context()})
			}
			return res, nil
		}, WithRetry(RetryPolicy{AttemptTimeout: time.Second, MinBackoff: time.Millisecond}))
		_, err := c.From("t").Fetch(t.Context())
		require.NoError(t, err)
		assert.Equal(t, int32(2), calls.Load())
	})
}

// stalledBody is a response whose body never arrives.
type stalledBody struct{ ctx context.Context }

func (b stalledBody) Read([]byte) (int, error) {
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}

func TestCallerCancelDuringRetryWaitStopsAtOnce(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		c := fakeClient(t, stallThen(99, &calls, ``),
			WithRetry(RetryPolicy{AttemptTimeout: time.Second, MinBackoff: time.Minute, MaxBackoff: 2 * time.Minute}))
		ctx, cancel := context.WithCancel(t.Context())
		start := time.Now()
		time.AfterFunc(5*time.Second, cancel) // inside the minute-long backoff
		err := c.Health(ctx)
		require.ErrorIs(t, err, context.Canceled)
		assert.NotErrorIs(t, err, ErrAttemptTimeout)
		assert.Equal(t, 5*time.Second, time.Since(start))
		assert.Equal(t, int32(1), calls.Load())
	})
}

func TestCallerDeadlineDuringAttemptIsNotAnAttemptTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		c := fakeClient(t, stallThen(99, &calls, ``), WithRetry(RetryPolicy{AttemptTimeout: time.Minute}))
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		err := c.Health(ctx)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.NotErrorIs(t, err, ErrAttemptTimeout)
		assert.False(t, IsRetryable(err))
		assert.Equal(t, int32(1), calls.Load())
	})
}

// listenTCP returns a listener and its base URL, closed with the test.
func listenTCP(t *testing.T) (net.Listener, string) {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	return ln, "http://" + ln.Addr().String()
}

// slowDialClient dials through a dialer that never connects until the attempt
// ends, as a blackholed address does, so the code under test sees what a real
// http.Transport returns for a dial cut short by the attempt's deadline.
func slowDialClient(t *testing.T, base string, dials *atomic.Int32) *Client {
	t.Helper()
	tr := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		dials.Add(1)
		<-ctx.Done()
		return nil, context.Cause(ctx)
	}}
	t.Cleanup(tr.CloseIdleConnections)
	c, err := New(base, WithHTTPClient(&http.Client{Transport: tr}),
		WithRetry(RetryPolicy{MaxAttempts: 3, AttemptTimeout: 30 * time.Millisecond, MinBackoff: time.Millisecond}))
	require.NoError(t, err)
	return c
}

// An attempt that times out while still connecting never sent anything, so a
// pipe or SQL call is resent.
func TestAttemptTimeoutWhileConnectingIsResent(t *testing.T) {
	calls := map[string]func(context.Context, *Client) error{
		"sql":  func(ctx context.Context, c *Client) error { return c.SQL("DROP TABLE x").Exec(ctx) },
		"pipe": func(ctx context.Context, c *Client) error { return c.Pipe("p").Exec(ctx) },
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			var dials atomic.Int32
			c := slowDialClient(t, "http://203.0.113.1:9", &dials)
			err := call(t.Context(), c)
			require.ErrorIs(t, err, ErrAttemptTimeout)
			assert.Equal(t, int32(3), dials.Load())
			assert.Contains(t, err.Error(), "after 3 attempt(s)")
		})
	}
}

// A listener that never accepts: the kernel completes the handshake, so the
// request is written into the socket buffer; that is "sent".
// A server that reads the headers and stalls must not see SQL or a pipe again.
func TestAttemptTimeoutAfterTheServerReadTheRequestIsNotResent(t *testing.T) {
	calls := map[string]struct {
		call     func(context.Context, *Client) error
		attempts int32
	}{
		"sql":  {func(ctx context.Context, c *Client) error { return c.SQL("DROP TABLE x").Exec(ctx) }, 1},
		"pipe": {func(ctx context.Context, c *Client) error { return c.Pipe("p").Exec(ctx) }, 1},
		"read": {func(ctx context.Context, c *Client) error { _, err := c.From("t").Fetch(ctx); return err }, 3},
		"insert": {func(ctx context.Context, c *Client) error {
			_, err := c.From("t").Insert(ctx, map[string]int{"a": 1})
			return err
		}, 3},
	}
	for name, tc := range calls {
		t.Run(name, func(t *testing.T) {
			var seenReqs atomic.Int32
			stop := make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				seenReqs.Add(1)
				select {
				case <-stop:
				case <-r.Context().Done():
				}
			}))
			t.Cleanup(func() { close(stop); srv.Close() })
			c, err := New(srv.URL, WithRetry(RetryPolicy{MaxAttempts: 3, AttemptTimeout: 100 * time.Millisecond, MinBackoff: time.Millisecond}))
			require.NoError(t, err)
			err = tc.call(t.Context(), c)
			require.ErrorIs(t, err, ErrAttemptTimeout)
			assert.Equal(t, tc.attempts, seenReqs.Load())
		})
	}
}

// A listener that accepts nothing and never reads: against a real transport
// the dial completes (the kernel backlog) and the headers are written, so
// the request counts as sent; with the backlog full the dial itself hangs.
// Either way the attempt must end at its timeout, never hang.
func TestAttemptTimeoutOnANeverAcceptingListenerEnds(t *testing.T) {
	_, base := listenTCP(t)
	c, err := New(base, WithRetry(RetryPolicy{MaxAttempts: 1, AttemptTimeout: 50 * time.Millisecond}))
	require.NoError(t, err)
	err = c.SQL("DROP TABLE x").Exec(t.Context())
	require.ErrorIs(t, err, ErrAttemptTimeout)
}

func TestAttemptTimeoutNegativeIsAnError(t *testing.T) {
	_, err := New("http://x", WithRetry(RetryPolicy{AttemptTimeout: -time.Second}))
	require.Error(t, err)
}

func TestSubscribeAttemptTimeoutRetriesAStalledConnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, f := newFakeStream(t, func(n int, r *http.Request) (*http.Response, error) {
			if n == 1 {
				return stall(r)
			}
			return sseOpen(r, schemaFrame("a"), rowFrame(1, 1)), nil
		}, WithRetry(RetryPolicy{AttemptTimeout: 3 * time.Second, MinBackoff: time.Second}))
		sub, err := c.Subscribe(t.Context(), "t", nil)
		require.NoError(t, err)
		defer func() { _ = sub.Close() }()
		assert.Equal(t, ts(1), next(t, sub).ID)
		assert.Len(t, f.requests(), 2)
	})
}

func TestSubscribeAttemptTimeoutGivesUpNamingTheCount(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, f := newFakeStream(t, func(_ int, r *http.Request) (*http.Response, error) { return stall(r) },
			WithRetry(RetryPolicy{MaxAttempts: 2, AttemptTimeout: time.Second}))
		_, err := c.Subscribe(t.Context(), "t", nil)
		require.ErrorIs(t, err, ErrAttemptTimeout)
		assert.Contains(t, err.Error(), "after 2 attempt(s)")
		assert.Len(t, f.requests(), 2)
	})
}

// An established stream outlives the attempt timeout: only the handshake is bounded.
func TestSubscribeAttemptTimeoutSparesAnEstablishedStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pr, pw := io.Pipe()
		c, f := newFakeStream(t, func(_ int, r *http.Request) (*http.Response, error) {
			return sseWith(r, pr), nil
		}, WithRetry(RetryPolicy{AttemptTimeout: time.Second}))
		sub, err := c.Subscribe(t.Context(), "t", nil)
		require.NoError(t, err)
		defer func() { _ = sub.Close() }()
		time.Sleep(time.Minute)
		go func() { _, _ = io.WriteString(pw, schemaFrame("a")+rowFrame(1, 1)) }()
		assert.Equal(t, ts(1), next(t, sub).ID)
		assert.Len(t, f.requests(), 1, "reconnected")
	})
}
