package wavehouse

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The token source runs before every attempt, so a token that changed between
// attempts is the one a retry sends.
func TestTokenSourceRunsPerAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		var sent []string
		c := fakeClient(t, func(r *http.Request) (*http.Response, error) {
			sent = append(sent, r.Header.Get("Authorization"))
			if len(sent) < 3 {
				return fakeResponse(r, 503, `{"error":"busy"}`), nil
			}
			return fakeResponse(r, 200, `[]`), nil
		}, WithTokenSource(func(context.Context) (string, error) {
			return fmt.Sprintf("tok-%d", calls.Add(1)), nil
		}))
		_, err := c.From("t").Fetch(t.Context())
		require.NoError(t, err)
		assert.Equal(t, []string{"Bearer tok-1", "Bearer tok-2", "Bearer tok-3"}, sent)
		assert.Equal(t, int32(3), calls.Load())
	})
}

func TestSubscribeTokenSourceRunsPerConnect(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		c, f := newFakeStream(t, func(n int, r *http.Request) (*http.Response, error) {
			if n < 3 {
				return sseEnds(r), nil
			}
			return sseOpen(r), nil
		}, WithTokenSource(func(context.Context) (string, error) { return fmt.Sprintf("tok-%d", calls.Add(1)), nil }))
		sub, err := c.Subscribe(t.Context(), "t", nil)
		require.NoError(t, err)
		defer func() { _ = sub.Close() }()
		time.Sleep(time.Minute)
		reqs := f.requests()
		require.Len(t, reqs, 3)
		for i, req := range reqs {
			assert.Equal(t, fmt.Sprintf("Bearer tok-%d", i+1), req.Header.Get("Authorization"))
		}
	})
}
