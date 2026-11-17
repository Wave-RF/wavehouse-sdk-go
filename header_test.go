package wavehouse

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHeaderPrecedence(t *testing.T) {
	rec := newRecorder(t, wireAnswer)
	fn := func(context.Context) (http.Header, error) {
		return http.Header{"X-A": {"func"}, "x-b": {"func"}, "X-Func": {"f"}, "Authorization": {"Basic func"}}, nil
	}
	c := rec.client(t, WithToken("jwt"), WithHeader("X-A", "client"), WithHeader("X-B", "client"),
		WithHeader("X-Client", "c"), WithHeaderFunc(fn))

	// Without a per-call header: the func beats the client.
	_, err := c.From("t").Fetch(t.Context())
	require.NoError(t, err)
	h := rec.last(t).header
	assert.Equal(t, []string{"func"}, h.Values("X-A"))
	assert.Equal(t, []string{"func"}, h.Values("X-B"))
	assert.Equal(t, []string{"c"}, h.Values("X-Client"))
	assert.Equal(t, []string{"f"}, h.Values("X-Func"))
	assert.Equal(t, []string{"Bearer jwt"}, h.Values("Authorization"))

	// The call beats both, in any letter case, and never joins; the SDK's own win.
	q := c.From("t").Header("x-a", "call-1").Header("X-A", "call-2").
		Header("Authorization", "Basic call").Header("Content-Type", "text/nope").
		Header("Accept", "nope").Header("User-Agent", "nope").Header("X-Call", "yes")
	_, err = q.Fetch(t.Context())
	require.NoError(t, err)
	h = rec.last(t).header
	assert.Equal(t, []string{"call-2"}, h.Values("X-A"))
	assert.Equal(t, []string{"func"}, h.Values("X-B"))
	assert.Equal(t, []string{"yes"}, h.Values("X-Call"))
	assert.Equal(t, []string{"Bearer jwt"}, h.Values("Authorization"))
	assert.Equal(t, []string{"application/json"}, h.Values("Content-Type"))
	assert.Equal(t, []string{"application/json"}, h.Values("Accept"))
	assert.Contains(t, h.Get("User-Agent"), "wavehouse-sdk-go/")
}

func TestPerCallHeaderEveryCall(t *testing.T) {
	rec := newRecorder(t, wireAnswer)
	c := rec.client(t)
	base := c.From("t")
	derived := base.Header("X-Call", "1")

	for name, call := range map[string]func() error{
		"fetch":  func() error { _, err := derived.Fetch(t.Context()); return err },
		"insert": func() error { _, err := derived.Insert(t.Context(), map[string]int{"a": 1}); return err },
		"ndjson": func() error {
			_, err := derived.InsertNDJSON(t.Context(), strings.NewReader("{\"a\":1}\n"))
			return err
		},
		"pipe": func() error { _, err := c.Pipe("p").Header("X-Call", "1").Fetch(t.Context()); return err },
		"pipe exec": func() error {
			return c.Pipe("p").Header("X-Call", "1").Exec(t.Context())
		},
		"sql":      func() error { _, err := c.SQL("SELECT 1").Header("X-Call", "1").Fetch(t.Context()); return err },
		"sql exec": func() error { return c.SQL("SELECT 1").Header("X-Call", "1").Exec(t.Context()) },
	} {
		require.NoError(t, call(), name)
		assert.Equal(t, []string{"1"}, rec.last(t).header.Values("X-Call"), name)
	}

	// Copy-on-write: the receiver is unchanged.
	_, err := base.Fetch(t.Context())
	require.NoError(t, err)
	assert.Empty(t, rec.last(t).header.Values("X-Call"))
	p := c.Pipe("p")
	_ = p.Header("X-Call", "1")
	require.NoError(t, p.Exec(t.Context()))
	assert.Empty(t, rec.last(t).header.Values("X-Call"))
	s := c.SQL("SELECT 1")
	_ = s.Header("X-Call", "1")
	require.NoError(t, s.Exec(t.Context()))
	assert.Empty(t, rec.last(t).header.Values("X-Call"))
}

func TestPerCallHeaderInvalid(t *testing.T) {
	rec := newRecorder(t, wireAnswer)
	c := rec.client(t)
	_, err := c.From("t").Header("Bad Name", "v").Fetch(t.Context())
	require.ErrorContains(t, err, "Header")
	_, err = c.From("t").Header("X-A", "a\r\nX-B: b").Insert(t.Context(), map[string]int{"a": 1})
	require.ErrorContains(t, err, "Header")
	require.ErrorContains(t, c.Pipe("p").Header("", "v").Exec(t.Context()), "Header")
	require.ErrorContains(t, c.SQL("SELECT 1").Header("X", "\x00").Exec(t.Context()), "Header")
	assert.Empty(t, rec.requests(), "sent despite an invalid header")
}

func TestHeaderFuncRunsPerAttempt(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		var sent []string
		c := fakeClient(t, func(r *http.Request) (*http.Response, error) {
			sent = append(sent, r.Header.Get("X-Rotating"))
			if len(sent) < 2 {
				return fakeResponse(r, 503, `{"error":"busy"}`), nil
			}
			return fakeResponse(r, 200, `[]`), nil
		}, WithHeaderFunc(func(context.Context) (http.Header, error) {
			return http.Header{"X-Rotating": {fmt.Sprintf("v%d", calls.Add(1))}}, nil
		}))
		_, err := c.From("t").Fetch(t.Context())
		require.NoError(t, err)
		assert.Equal(t, []string{"v1", "v2"}, sent)
	})
}

func TestHeaderFuncError(t *testing.T) {
	rec := newRecorder(t, nil)
	boom := errors.New("vault down")
	c := rec.client(t, WithHeaderFunc(func(context.Context) (http.Header, error) { return nil, boom }))
	err := c.Health(t.Context())
	require.ErrorIs(t, err, boom)
	assert.Contains(t, err.Error(), "wavehouse: GET /v1/health: header func")
	assert.Empty(t, rec.requests())
	assert.False(t, IsRetryable(err))

	bad := rec.client(t, WithHeaderFunc(func(context.Context) (http.Header, error) {
		return http.Header{"Bad Name": {"v"}}, nil
	}))
	require.ErrorContains(t, bad.Health(t.Context()), "header func")
	assert.Empty(t, rec.requests())
}

func TestHeaderFuncCountsAsCredentialForRedirects(t *testing.T) {
	rec := newRecorder(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/elsewhere", http.StatusFound)
	})
	c := rec.client(t, WithHeaderFunc(func(context.Context) (http.Header, error) {
		return http.Header{"X-Key": {"secret"}}, nil
	}))
	require.ErrorIs(t, c.Health(t.Context()), ErrRedirect)
	_, err := rec.client(t).From("t").Header("X-Key", "secret").Fetch(t.Context())
	require.ErrorIs(t, err, ErrRedirect)
}

func TestNewRejectsNilHeaderFunc(t *testing.T) {
	_, err := New("http://x", WithHeaderFunc(nil))
	require.ErrorContains(t, err, "WithHeaderFunc")
}

func TestSubscribeHeaderLayers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		c, f := newFakeStream(t, func(n int, r *http.Request) (*http.Response, error) {
			if n == 1 {
				return sseEnds(r), nil
			}
			return sseOpen(r), nil
		},
			WithHeader("X-A", "client"), WithHeader("X-B", "client"),
			WithHeaderFunc(func(context.Context) (http.Header, error) {
				return http.Header{"X-A": {"func"}, "X-N": {fmt.Sprintf("n%d", calls.Add(1))}}, nil
			}))
		sub, err := c.Subscribe(t.Context(), "t", &SubscribeOptions{
			ResumeID: "r1",
			Header:   http.Header{"x-a": {"sub"}, "Last-Event-Id": {"mine"}, "Accept": {"nope"}},
		})
		require.NoError(t, err)
		defer func() { _ = sub.Close() }()
		time.Sleep(time.Minute)
		reqs := f.requests()
		require.Len(t, reqs, 2)
		for i, req := range reqs {
			assert.Equal(t, []string{"sub"}, req.Header.Values("X-A"))
			assert.Equal(t, []string{"client"}, req.Header.Values("X-B"))
			assert.Equal(t, []string{fmt.Sprintf("n%d", i+1)}, req.Header.Values("X-N"), "func not run per connect")
			assert.Equal(t, []string{"r1"}, req.Header.Values("Last-Event-Id"))
			assert.Equal(t, []string{"text/event-stream"}, req.Header.Values("Accept"))
		}
	})
}

func TestSubscribeHeaderInvalid(t *testing.T) {
	rec := newRecorder(t, nil)
	_, err := rec.client(t).Subscribe(t.Context(), "t", &SubscribeOptions{Header: http.Header{"Bad Name": {"v"}}})
	require.ErrorContains(t, err, "SubscribeOptions.Header")
	assert.Empty(t, rec.requests())
}

func TestSubscribeHeaderFuncError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		boom := errors.New("vault down")
		c, f := newFakeStream(t, func(_ int, r *http.Request) (*http.Response, error) { return sseOpen(r), nil },
			WithHeaderFunc(func(context.Context) (http.Header, error) { return nil, boom }))
		_, err := c.Subscribe(t.Context(), "t", nil)
		require.ErrorIs(t, err, boom)
		assert.Empty(t, f.requests())
	})
}

func TestSubscribeHeaderFuncRetriedAfterLive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		fn := func(context.Context) (http.Header, error) {
			if n := calls.Add(1); n == 2 || n == 3 {
				return nil, errors.New("vault down")
			}
			return http.Header{"X-K": {"v"}}, nil
		}
		c, f := newFakeStream(t, func(n int, r *http.Request) (*http.Response, error) {
			if n == 1 {
				return sseEnds(r, schemaFrame("a"), rowFrame(1, 1)), nil
			}
			return sseOpen(r, schemaFrame("a"), rowFrame(2, 2)), nil
		}, WithHeaderFunc(fn))
		sub, err := c.Subscribe(t.Context(), "t", nil)
		require.NoError(t, err)
		defer func() { _ = sub.Close() }()
		assert.Equal(t, ts(1), next(t, sub).ID)
		assert.Equal(t, ts(2), next(t, sub).ID)
		assert.Len(t, f.requests(), 2)
		assert.Equal(t, int32(4), calls.Load())
	})
}

// Names must be tokens and values must pass net/http's field-value rule: every
// header source rejects the same bytes, before anything is sent, with an
// error that is not retryable.
func TestHeaderValidationMatchesTheWire(t *testing.T) {
	badValues := []string{"a\x01b", "a\x7fb", "a\vb", "a\fb", "a\rb", "a\nb", "a\x00b"}
	goodValues := []string{"a\tb", "café", "a b", ""}
	badNames := []string{"", "X A", "X:A", "X\x01", "café", "X(A)"}
	for _, v := range badValues {
		assert.Error(t, checkHeader("X-A", v), "%q", v)
	}
	for _, v := range goodValues {
		assert.NoError(t, checkHeader("X-A", v), "%q", v)
	}
	for _, n := range badNames {
		assert.Error(t, checkHeader(n, "v"), "%q", n)
	}

	rec := newRecorder(t, wireAnswer)
	for _, v := range []string{"a\x01b", "a\x7fb", "a\vb"} {
		t.Run(fmt.Sprintf("%q", v), func(t *testing.T) {
			_, err := New(rec.URL, WithHeader("X-A", v))
			require.Error(t, err)
			_, err = New(rec.URL, WithUserAgent(v))
			require.Error(t, err)

			c := rec.client(t, WithHeaderFunc(func(context.Context) (http.Header, error) {
				return http.Header{"X-A": {v}}, nil
			}))
			err = c.Health(t.Context())
			require.ErrorContains(t, err, "header func")
			assert.False(t, IsRetryable(err))

			c = rec.client(t)
			_, err = c.From("t").Header("X-A", v).Fetch(t.Context())
			require.Error(t, err)
			assert.False(t, IsRetryable(err))
			require.Error(t, c.Pipe("p").Header("X-A", v).Exec(t.Context()))
			require.Error(t, c.SQL("SELECT 1").Header("X-A", v).Exec(t.Context()))
			_, err = c.Subscribe(t.Context(), "t", &SubscribeOptions{Header: http.Header{"X-A": {v}}})
			require.Error(t, err)
			assert.False(t, IsRetryable(err))
		})
	}
	assert.Empty(t, rec.requests(), "sent despite an invalid header")
}
