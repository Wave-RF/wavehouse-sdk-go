package wavehouse

import (
	"context"
	"io"
	"net/http"
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

// A timeout after the request may have left is never resent for a call that
// may write; one that never connected is.
func TestAttemptTimeoutRespectsSendSafety(t *testing.T) {
	stallDial := func(r *http.Request) (*http.Response, error) {
		<-r.Context().Done()
		return nil, errDial
	}
	cases := map[string]struct {
		call     func(context.Context, *Client) error
		rt       roundTripFunc
		attempts int32
	}{
		"sql after send": {func(ctx context.Context, c *Client) error { return c.SQL("DROP TABLE x").Exec(ctx) }, stall, 1},
		"sql dial":       {func(ctx context.Context, c *Client) error { return c.SQL("DROP TABLE x").Exec(ctx) }, stallDial, 3},
		"pipe after send": {func(ctx context.Context, c *Client) error {
			return c.Pipe("p").Exec(ctx)
		}, stall, 1},
		"pipe dial": {func(ctx context.Context, c *Client) error { return c.Pipe("p").Exec(ctx) }, stallDial, 3},
		"insert after send": {func(ctx context.Context, c *Client) error {
			_, err := c.From("t").Insert(ctx, map[string]int{"a": 1})
			return err
		}, stall, 3},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var calls atomic.Int32
				c := fakeClient(t, func(r *http.Request) (*http.Response, error) {
					calls.Add(1)
					return tc.rt(r)
				}, WithRetry(RetryPolicy{AttemptTimeout: time.Second, MinBackoff: time.Millisecond}))
				err := tc.call(t.Context(), c)
				require.ErrorIs(t, err, ErrAttemptTimeout)
				assert.Equal(t, tc.attempts, calls.Load())
				assert.Contains(t, err.Error(), "attempt(s)")
			})
		})
	}
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
