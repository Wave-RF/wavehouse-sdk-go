package wavehouse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// fakeStream answers GET /v1/stream inside a synctest bubble, where a real
// socket would stop fake time. answer gets each connection's number, from 1.
type fakeStream struct {
	mu   sync.Mutex
	reqs []*http.Request
	at   []time.Time
}

func newFakeStream(t *testing.T, answer func(n int, r *http.Request) (*http.Response, error), opts ...Option) (*Client, *fakeStream) {
	t.Helper()
	f := &fakeStream{}
	c := fakeClient(t, func(r *http.Request) (*http.Response, error) {
		f.mu.Lock()
		f.reqs = append(f.reqs, r)
		f.at = append(f.at, time.Now())
		n := len(f.reqs)
		f.mu.Unlock()
		return answer(n, r)
	}, opts...)
	return c, f
}

func (f *fakeStream) requests() []*http.Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*http.Request(nil), f.reqs...)
}

// gap is the time between connection n and n+1, numbered from 1.
func (f *fakeStream) gap(n int) time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.at[n].Sub(f.at[n-1])
}

// testBody serves frames, then ends (hold 0), stays open until closed
// (hold < 0), or ends after hold.
type testBody struct {
	r      io.Reader
	hold   time.Duration
	once   sync.Once
	closed chan struct{}
}

var errBodyClosed = errors.New("read on closed body")

func (b *testBody) Read(p []byte) (int, error) {
	select {
	case <-b.closed:
		return 0, errBodyClosed
	default:
	}
	n, err := b.r.Read(p)
	if n > 0 || !errors.Is(err, io.EOF) || b.hold == 0 {
		return n, err
	}
	var end <-chan time.Time
	if b.hold > 0 {
		end = time.After(b.hold)
	}
	select {
	case <-b.closed:
		return 0, errBodyClosed
	case <-end:
		return 0, io.EOF
	}
}

func (b *testBody) Close() error {
	b.once.Do(func() { close(b.closed) })
	return nil
}

// gateReader blocks until ch is closed, then reads as the end.
type gateReader <-chan struct{}

func (g gateReader) Read([]byte) (int, error) {
	<-g
	return 0, io.EOF
}

func sseWith(r *http.Request, body io.ReadCloser) *http.Response {
	res := fakeResponse(r, http.StatusOK, "", "Content-Type", "text/event-stream")
	res.Body = body
	return res
}

func sseBody(frames string, hold time.Duration) *testBody {
	return &testBody{r: strings.NewReader(frames), hold: hold, closed: make(chan struct{})}
}

// sseEnds is a stream of frames that then ends.
func sseEnds(r *http.Request, frames ...string) *http.Response {
	return sseWith(r, sseBody(strings.Join(frames, ""), 0))
}

// sseOpen is a stream of frames that then stays open.
func sseOpen(r *http.Request, frames ...string) *http.Response {
	return sseWith(r, sseBody(strings.Join(frames, ""), -1))
}

func schemaFrame(cols ...string) string {
	b, _ := json.Marshal(cols)
	return `event: schema` + "\n" + `data: {"table_name":"t","columns":` + string(b) + "}\n\n"
}

// rowFrame is a data frame whose id and received_timestamp are ts(n).
func rowFrame(n int, values ...any) string {
	b, _ := json.Marshal(values)
	return fmt.Sprintf("id: %s\ndata: {\"table_name\":\"t\",\"received_timestamp\":%q,\"row\":%s}\n\n", ts(n), ts(n), b)
}

func ts(n int) string { return fmt.Sprintf("2026-11-16T16:00:%02d.000000001Z", n) }

// states records OnState calls, which arrive on the subscription's goroutine.
type states struct {
	mu  sync.Mutex
	got []State
}

func (s *states) on(st State) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, st)
}

func (s *states) list() []State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]State(nil), s.got...)
}

// next reads one event, failing the test if the stream ends first.
func next(t *testing.T, sub *Subscription) Event {
	t.Helper()
	require.True(t, sub.Next(), "stream ended: %v", sub.Err())
	return sub.Event()
}

func TestSubscribeZipsRows(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const id = "2026-11-16T16:03:24.977505914Z"
		c, f := newFakeStream(t, func(_ int, r *http.Request) (*http.Response, error) {
			return sseOpen(r,
				": connected\n\n",
				`event: schema`+"\n"+`data: {"table_name":"e2e_events","columns":["event_id","name","value","ts"]}`+"\n\n",
				`event: schema`+"\n"+`data: {"table_name":"e2e_events","columns":["event_id","name","value","ts"]}`+"\n\n",
				"id: "+id+"\n"+`data: {"table_name":"e2e_events","received_timestamp":"`+id+`","row":["a","x",9007199254740993,null]}`+"\n\n",
				":\n\n",
				"id: "+ts(2)+"\n"+`data: {"table_name":"e2e_events","received_timestamp":"`+ts(2)+`","row":["b","y",1.5,"2026-11-16T16:00:00Z"]}`+"\n\n",
			), nil
		})
		sub, err := c.Subscribe(t.Context(), "e2e_events", nil)
		require.NoError(t, err)
		defer func() { _ = sub.Close() }()

		ev := next(t, sub)
		assert.Equal(t, id, ev.ID)
		assert.Equal(t, "e2e_events", ev.Table)
		assert.Equal(t, time.Date(2026, 11, 16, 16, 3, 24, 977505914, time.UTC), ev.ReceivedAt)
		assert.Equal(t, []string{"event_id", "name", "value", "ts"}, ev.Columns)
		assert.Equal(t, []any{"a", "x", json.Number("9007199254740993"), nil}, ev.Values)
		assert.Equal(t, Row{"event_id": "a", "name": "x", "value": json.Number("9007199254740993"), "ts": nil}, ev.Row())

		var dst struct {
			ID    string  `json:"event_id"`
			Value uint64  `json:"value"`
			TS    *string `json:"ts"`
		}
		require.NoError(t, ev.Decode(&dst))
		assert.Equal(t, "a", dst.ID)
		assert.Equal(t, uint64(9007199254740993), dst.Value)
		assert.Nil(t, dst.TS)

		ev = next(t, sub)
		assert.Equal(t, ts(2), ev.ID)
		assert.Equal(t, json.Number("1.5"), ev.Values[2])

		require.Len(t, f.requests(), 1)
		req := f.requests()[0]
		assert.Equal(t, "/v1/stream", req.URL.Path)
		assert.Equal(t, "table=e2e_events", req.URL.RawQuery)
		assert.Equal(t, "text/event-stream", req.Header.Get("Accept"))
		assert.Empty(t, req.Header.Values("Last-Event-Id"))
	})
}

func TestEventDecode(t *testing.T) {
	ev := Event{
		Columns: []string{"big", "name", "f", "tags", "opt"},
		Values:  []any{json.Number("18446744073709551615"), "x", json.Number("-2.5e3"), []any{"a", "b"}, nil},
	}
	var dst struct {
		Big  uint64   `json:"big"`
		Name string   `json:"name"`
		F    float64  `json:"f"`
		Tags []string `json:"tags"`
		Opt  *int     `json:"opt"`
	}
	require.NoError(t, ev.Decode(&dst))
	assert.Equal(t, uint64(18446744073709551615), dst.Big)
	assert.Equal(t, "x", dst.Name)
	assert.InDelta(t, -2500.0, dst.F, 0)
	assert.Equal(t, []string{"a", "b"}, dst.Tags)
	assert.Nil(t, dst.Opt)

	var m map[string]any
	require.NoError(t, ev.Decode(&m))
	assert.Equal(t, json.Number("18446744073709551615"), m["big"], "a map keeps numbers exact")

	require.ErrorContains(t, ev.Decode(dst), "wavehouse: Event.Decode")
	bad := Event{Columns: []string{"f"}, Values: []any{func() {}}}
	require.ErrorContains(t, bad.Decode(&m), "wavehouse: Event.Decode")
}

func TestSubscribeReconnectsAfterEOF(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var st states
		c, f := newFakeStream(t, func(n int, r *http.Request) (*http.Response, error) {
			if n == 1 {
				return sseEnds(r, schemaFrame("a"), rowFrame(1, 1)), nil
			}
			return sseOpen(r, schemaFrame("a"), rowFrame(2, 2)), nil
		})
		sub, err := c.Subscribe(t.Context(), "t", &SubscribeOptions{OnState: st.on})
		require.NoError(t, err)
		defer func() { _ = sub.Close() }()

		assert.Equal(t, ts(1), next(t, sub).ID)
		assert.Equal(t, ts(2), next(t, sub).ID)
		assert.Equal(t, ts(1), f.requests()[1].Header.Get("Last-Event-ID"))
		// The first connection delivered, so the wait is the first backoff step: 500ms ±20%.
		assert.InDelta(t, float64(500*time.Millisecond), float64(f.gap(1)), float64(100*time.Millisecond))
		synctest.Wait() // OnState runs on a goroutine of its own
		assert.Equal(t, []State{StateConnecting, StateLive, StateReconnecting, StateLive}, st.list())
	})
}

func TestSubscribeSchemaKeepsResumeID(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, f := newFakeStream(t, func(n int, r *http.Request) (*http.Response, error) {
			if n == 1 {
				// A re-announcement after the row has no id: line and must not move the resume point.
				return sseEnds(r, schemaFrame("a"), rowFrame(1, 1), schemaFrame("a", "b")), nil
			}
			return sseOpen(r, schemaFrame("a", "b"), rowFrame(2, 2, 3)), nil
		})
		sub, err := c.Subscribe(t.Context(), "t", nil)
		require.NoError(t, err)
		defer func() { _ = sub.Close() }()

		assert.Equal(t, ts(1), next(t, sub).ID)
		ev := next(t, sub)
		assert.Equal(t, Row{"a": json.Number("2"), "b": json.Number("3")}, ev.Row())
		assert.Equal(t, ts(1), f.requests()[1].Header.Get("Last-Event-ID"))
	})
}

func TestSubscribeArityMismatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, f := newFakeStream(t, func(n int, r *http.Request) (*http.Response, error) {
			if n == 1 {
				// Row 2 is short: it and everything after it on this connection are dropped.
				return sseOpen(r, schemaFrame("a", "b"), rowFrame(1, 1, 2), rowFrame(2, 1), rowFrame(3, 3, 4)), nil
			}
			return sseOpen(r, schemaFrame("a", "b"), rowFrame(2, 1, 0), rowFrame(3, 3, 4)), nil
		})
		sub, err := c.Subscribe(t.Context(), "t", nil)
		require.NoError(t, err)
		defer func() { _ = sub.Close() }()

		var got []string
		for range 3 {
			ev := next(t, sub)
			got = append(got, ev.ID)
			assert.Len(t, ev.Values, 2)
		}
		assert.Equal(t, []string{ts(1), ts(2), ts(3)}, got)
		require.Len(t, f.requests(), 2)
		assert.Equal(t, ts(1), f.requests()[1].Header.Get("Last-Event-ID"))
	})
}

// A row before any schema frame can't be zipped, and a connection that
// delivered nothing resumes from Since, not from an id it never had.
func TestSubscribeRowWithoutSchema(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		since := time.Date(2026, 11, 16, 11, 0, 0, 5, time.FixedZone("x", 3600))
		c, f := newFakeStream(t, func(n int, r *http.Request) (*http.Response, error) {
			if n == 1 {
				return sseOpen(r, rowFrame(1, 1)), nil
			}
			return sseOpen(r, schemaFrame("a"), rowFrame(1, 1)), nil
		})
		sub, err := c.Subscribe(t.Context(), "t", &SubscribeOptions{Since: since})
		require.NoError(t, err)
		defer func() { _ = sub.Close() }()

		assert.Equal(t, ts(1), next(t, sub).ID)
		for _, req := range f.requests() {
			assert.Equal(t, "since=2026-11-16T10%3A00%3A00.000000005Z&table=t", req.URL.RawQuery)
			assert.Empty(t, req.Header.Values("Last-Event-Id"))
		}
	})
}

func TestSubscribeMalformedFrames(t *testing.T) {
	for name, frame := range map[string]string{
		"schema": "event: schema\ndata: {nope\n\n",
		"data":   "id: x\ndata: [1]\n\n",
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c, f := newFakeStream(t, func(n int, r *http.Request) (*http.Response, error) {
					if n == 1 {
						return sseOpen(r, schemaFrame("a"), rowFrame(1, 1), frame), nil
					}
					return sseOpen(r, schemaFrame("a"), rowFrame(2, 2)), nil
				})
				sub, err := c.Subscribe(t.Context(), "t", nil)
				require.NoError(t, err)
				defer func() { _ = sub.Close() }()
				assert.Equal(t, ts(1), next(t, sub).ID)
				assert.Equal(t, ts(2), next(t, sub).ID)
				assert.Equal(t, ts(1), f.requests()[1].Header.Get("Last-Event-ID"))
			})
		})
	}
}

func TestSubscribeOversizeLineReconnects(t *testing.T) {
	old := maxSSELine
	maxSSELine = 256
	t.Cleanup(func() { maxSSELine = old })
	synctest.Test(t, func(t *testing.T) {
		c, f := newFakeStream(t, func(n int, r *http.Request) (*http.Response, error) {
			if n == 1 {
				return sseOpen(r, schemaFrame("a"), rowFrame(1, strings.Repeat("x", 300))), nil
			}
			return sseOpen(r, schemaFrame("a"), rowFrame(2, "ok")), nil
		})
		sub, err := c.Subscribe(t.Context(), "t", nil)
		require.NoError(t, err)
		defer func() { _ = sub.Close() }()
		assert.Equal(t, []any{"ok"}, next(t, sub).Values)
		assert.Len(t, f.requests(), 2)
	})
}

func TestSubscribeTerminalStatus(t *testing.T) {
	for status, sentinel := range map[int]error{
		400: ErrBadRequest, 401: ErrUnauthorized, 403: ErrForbidden, 404: ErrNotFound,
	} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var st states
				c, f := newFakeStream(t, func(_ int, r *http.Request) (*http.Response, error) {
					return fakeResponse(r, status, `{"error":"no"}`, "X-Request-Id", "req-1"), nil
				})
				sub, err := c.Subscribe(t.Context(), "t", &SubscribeOptions{OnState: st.on})
				require.ErrorIs(t, err, sentinel)
				assert.Nil(t, sub)
				assert.Equal(t, fmt.Sprintf("wavehouse: GET /v1/stream: %d: no (request req-1)", status), err.Error())
				assert.Len(t, f.requests(), 1, "a terminal status was retried")
				synctest.Wait() // OnState runs on a goroutine of its own
				assert.Equal(t, []State{StateConnecting, StateClosed}, st.list())
			})
		})
	}
}

func TestSubscribeTerminalAfterLive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var st states
		c, _ := newFakeStream(t, func(n int, r *http.Request) (*http.Response, error) {
			if n == 1 {
				return sseEnds(r, schemaFrame("a"), rowFrame(1, 1)), nil
			}
			// The tenant was removed while the stream was open.
			return fakeResponse(r, 404, `{"error":"unknown tenant"}`), nil
		})
		sub, err := c.Subscribe(t.Context(), "t", &SubscribeOptions{OnState: st.on})
		require.NoError(t, err)
		defer func() { _ = sub.Close() }()

		assert.Equal(t, ts(1), next(t, sub).ID)
		assert.False(t, sub.Next())
		require.ErrorIs(t, sub.Err(), ErrNotFound)
		assert.False(t, sub.Next(), "Next after the end")
		synctest.Wait() // OnState runs on a goroutine of its own
		assert.Equal(t, []State{StateConnecting, StateLive, StateReconnecting, StateClosed}, st.list())
		require.NoError(t, sub.Close())
		require.ErrorIs(t, sub.Err(), ErrNotFound, "Close hides the error that ended the stream")
	})
}

func TestSubscribeNotAnEventStream(t *testing.T) {
	for name, answer := range map[string]func(r *http.Request) *http.Response{
		"json": func(r *http.Request) *http.Response {
			return fakeResponse(r, 200, `{}`, "Content-Type", "application/json")
		},
		"untyped": func(r *http.Request) *http.Response { return fakeResponse(r, 200, "") },
		"204": func(r *http.Request) *http.Response {
			return fakeResponse(r, 204, "", "Content-Type", "text/event-stream")
		},
		"param ok": nil,
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c, f := newFakeStream(t, func(_ int, r *http.Request) (*http.Response, error) {
					if answer == nil {
						res := sseOpen(r)
						res.Header.Set("Content-Type", "Text/Event-Stream; charset=utf-8")
						return res, nil
					}
					return answer(r), nil
				})
				sub, err := c.Subscribe(t.Context(), "t", nil)
				if answer == nil {
					require.NoError(t, err)
					require.NoError(t, sub.Close())
					return
				}
				require.ErrorContains(t, err, "not an event stream")
				assert.False(t, IsRetryable(err))
				assert.Len(t, f.requests(), 1)
			})
		})
	}
}

func TestSubscribeRetryAfter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, f := newFakeStream(t, func(n int, r *http.Request) (*http.Response, error) {
			switch n {
			case 1:
				return fakeResponse(r, 503, `{"error":"tenant rejected"}`, "Retry-After", "7"), nil
			case 2:
				return fakeResponse(r, 429, `{"error":"slow down"}`, "Retry-After", "3"), nil
			case 3:
				return nil, errDial
			}
			return sseOpen(r), nil
		}, WithRetry(RetryPolicy{MaxAttempts: 4}))
		sub, err := c.Subscribe(t.Context(), "t", nil)
		require.NoError(t, err)
		defer func() { _ = sub.Close() }()
		assert.Equal(t, 7*time.Second, f.gap(1))
		assert.Equal(t, 3*time.Second, f.gap(2))
		// No Retry-After: the third backoff step, 2s ±20%.
		assert.InDelta(t, float64(2*time.Second), float64(f.gap(3)), float64(400*time.Millisecond))
	})
}

func TestSubscribeFirstConnectGivesUp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var st states
		c, f := newFakeStream(t, func(_ int, r *http.Request) (*http.Response, error) {
			return fakeResponse(r, 503, `{"error":"busy"}`, "Retry-After", "1"), nil
		})
		_, err := c.Subscribe(t.Context(), "t", &SubscribeOptions{OnState: st.on})
		require.ErrorIs(t, err, ErrUnavailable)
		assert.Len(t, f.requests(), 3, "the default policy's three attempts")
		synctest.Wait() // OnState runs on a goroutine of its own
		assert.Equal(t, []State{StateConnecting, StateClosed}, st.list())
	})
}

// After the first connection, a subscription never gives up on a retryable
// failure, and its backoff starts over only after a healthy connection.
func TestSubscribeRetriesForever(t *testing.T) {
	for name, tc := range map[string]struct {
		up       time.Duration // how long connection 3 stays up, delivering nothing
		wantStep int           // the backoff step before connection 4
	}{
		"short connection": {up: time.Second, wantStep: 3},
		"healthy":          {up: healthyConnection, wantStep: 1},
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c, f := newFakeStream(t, func(n int, r *http.Request) (*http.Response, error) {
					switch {
					case n <= 2:
						return nil, errReset
					case n == 3:
						return sseWith(r, sseBody(": connected\n\n", tc.up)), nil
					case n < 10:
						return fakeResponse(r, 502, ""), nil
					}
					return sseOpen(r, schemaFrame("a"), rowFrame(1, 1)), nil
				}, WithRetry(RetryPolicy{MaxAttempts: 3, MaxBackoff: time.Hour}))
				sub, err := c.Subscribe(t.Context(), "t", nil)
				require.NoError(t, err)
				defer func() { _ = sub.Close() }()

				assert.Equal(t, ts(1), next(t, sub).ID)
				require.Len(t, f.requests(), 10)
				step := func(n int) time.Duration { return 500 * time.Millisecond << (n - 1) }
				gap := f.gap(3) - tc.up
				assert.InDelta(t, float64(step(tc.wantStep)), float64(gap), float64(step(tc.wantStep))/5)
				for i := 4; i < 10; i++ {
					want := step(tc.wantStep + i - 3)
					assert.InDelta(t, float64(want), float64(f.gap(i)), float64(want)/5, "gap %d", i)
				}
			})
		})
	}
}

func TestSubscribeRedirectRefused(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, f := newFakeStream(t, func(_ int, r *http.Request) (*http.Response, error) {
			return fakeResponse(r, 307, "", "Location", "http://elsewhere.test/v1/stream"), nil
		}, WithToken("jwt"))
		_, err := c.Subscribe(t.Context(), "t", nil)
		require.ErrorIs(t, err, ErrRedirect)
		assert.Contains(t, err.Error(), "http://elsewhere.test/v1/stream")
		assert.Len(t, f.requests(), 1)
	})
}

func TestSubscribeHeaders(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		c, f := newFakeStream(t, func(n int, r *http.Request) (*http.Response, error) {
			if n == 1 {
				return sseEnds(r), nil
			}
			return sseOpen(r), nil
		},
			WithTenant("acme"), WithHeader("Last-Event-ID", "custom-loses"),
			WithTokenSource(func(context.Context) (string, error) { return fmt.Sprintf("tok-%d", calls.Add(1)), nil }))
		sub, err := c.Subscribe(t.Context(), "clicks", &SubscribeOptions{
			ResumeID: "2026-11-16T00:00:00Z",
			Since:    time.Now(), // ResumeID wins
		})
		require.NoError(t, err)
		defer func() { _ = sub.Close() }()
		time.Sleep(time.Minute) // fake time: past the backoff before the second connection

		reqs := f.requests()
		require.Len(t, reqs, 2)
		for i, req := range reqs {
			assert.Equal(t, "acme", req.Header.Get("X-Tenant-ID"))
			assert.Equal(t, "table=clicks", req.URL.RawQuery)
			assert.Equal(t, []string{"2026-11-16T00:00:00Z"}, req.Header.Values("Last-Event-ID"))
			assert.Equal(t, fmt.Sprintf("Bearer tok-%d", i+1), req.Header.Get("Authorization"), "token source not re-read")
		}
	})
}

func TestSubscribeTokenSourceError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		boom := errors.New("idp down")
		c, f := newFakeStream(t, func(_ int, r *http.Request) (*http.Response, error) { return sseOpen(r), nil },
			WithTokenSource(func(context.Context) (string, error) { return "", boom }))
		_, err := c.Subscribe(t.Context(), "t", nil)
		require.ErrorIs(t, err, boom)
		assert.Empty(t, f.requests())
	})
}

func TestSubscribeEmptyTable(t *testing.T) {
	c, err := New("http://h")
	require.NoError(t, err)
	_, err = c.Subscribe(t.Context(), "", nil)
	require.ErrorContains(t, err, "empty table")
}

func TestSubscribeCancelWhileConnecting(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c, _ := newFakeStream(t, func(_ int, r *http.Request) (*http.Response, error) {
			return fakeResponse(r, 503, `{"error":"busy"}`, "Retry-After", "20"), nil
		})
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		_, err := c.Subscribe(ctx, "t", nil)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Contains(t, err.Error(), "busy")
		_, isAPI := errors.AsType[*APIError](err)
		assert.False(t, isAPI, "an abandoned connect still matches its last APIError")
	})
}

func TestSubscribeCloseUnblocksNext(t *testing.T) {
	for name, stop := range map[string]func(*testing.T, *Subscription, context.CancelFunc){
		"Close":      func(t *testing.T, s *Subscription, _ context.CancelFunc) { require.NoError(t, s.Close()) },
		"ctx cancel": func(_ *testing.T, _ *Subscription, cancel context.CancelFunc) { cancel() },
	} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var st states
				c, _ := newFakeStream(t, func(_ int, r *http.Request) (*http.Response, error) {
					return sseOpen(r, ": connected\n\n", schemaFrame("a")), nil
				})
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				sub, err := c.Subscribe(ctx, "t", &SubscribeOptions{OnState: st.on})
				require.NoError(t, err)

				got := make(chan bool)
				go func() { got <- sub.Next() }()
				synctest.Wait()
				stop(t, sub, cancel)
				assert.False(t, <-got)
				require.NoError(t, sub.Err())
				require.NoError(t, sub.Close())
				require.NoError(t, sub.Close(), "a second Close")
				assert.False(t, sub.Next())
				synctest.Wait() // OnState runs on a goroutine of its own
				assert.Equal(t, []State{StateConnecting, StateLive, StateClosed}, st.list())
			})
		})
	}
}

// Close is safe from inside OnState, which runs on the goroutine Close
// otherwise waits for.
func TestSubscribeCloseFromOnState(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var sub atomic.Pointer[Subscription]
		release := make(chan struct{})
		c, _ := newFakeStream(t, func(n int, r *http.Request) (*http.Response, error) {
			if n == 1 {
				// Ends only once the test has stored sub for the callback.
				frames := io.MultiReader(strings.NewReader(schemaFrame("a")+rowFrame(1, 1)), gateReader(release))
				return sseWith(r, &testBody{r: frames, closed: make(chan struct{})}), nil
			}
			return sseOpen(r), nil
		})
		s, err := c.Subscribe(t.Context(), "t", &SubscribeOptions{OnState: func(st State) {
			if st == StateReconnecting {
				_ = sub.Load().Close()
			}
		}})
		require.NoError(t, err)
		sub.Store(s)
		assert.Equal(t, ts(1), next(t, s).ID)
		close(release)
		assert.False(t, s.Next())
		require.NoError(t, s.Err())
	})
}

func TestSubscribeBackpressure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var written atomic.Int64
		c, _ := newFakeStream(t, func(_ int, r *http.Request) (*http.Response, error) {
			pr, pw := io.Pipe()
			go func() {
				if _, err := io.WriteString(pw, schemaFrame("a")); err != nil {
					return
				}
				for i := range 10_000 {
					if _, err := io.WriteString(pw, rowFrame(i%60, i)); err != nil {
						return
					}
					written.Add(1)
				}
			}()
			return sseWith(r, pr), nil
		})
		sub, err := c.Subscribe(t.Context(), "t", nil)
		require.NoError(t, err)
		defer func() { _ = sub.Close() }()

		synctest.Wait()
		// The buffer, one event blocked on it, and what the scanner holds: never the whole stream.
		assert.LessOrEqual(t, written.Load(), int64(eventBuffer+3))
		assert.Len(t, sub.events, eventBuffer)

		for i := range 10 {
			assert.Equal(t, []any{json.Number(fmt.Sprint(i))}, next(t, sub).Values)
		}
		synctest.Wait()
		assert.LessOrEqual(t, written.Load(), int64(eventBuffer+13))
		assert.GreaterOrEqual(t, written.Load(), int64(eventBuffer+10), "reading did not resume")
	})
}

func TestStateString(t *testing.T) {
	for st, want := range map[State]string{
		StateConnecting: "connecting", StateLive: "live", StateReconnecting: "reconnecting", StateClosed: "closed", State(9): "State(9)",
	} {
		assert.Equal(t, want, st.String())
	}
}

// Over a real connection: Close during a blocked Next leaves no goroutine
// behind, the transport's included.
func TestSubscribeNoLeak(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, ": connected\n\n"+schemaFrame("a")+rowFrame(1, 1))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	}))
	defer srv.Close()
	c, err := New(srv.URL, WithHTTPClient(&http.Client{Transport: &http.Transport{}}))
	require.NoError(t, err)

	sub, err := c.Subscribe(t.Context(), "t", nil)
	require.NoError(t, err)
	assert.Equal(t, ts(1), next(t, sub).ID)
	got := make(chan bool)
	go func() { got <- sub.Next() }()
	require.NoError(t, sub.Close())
	assert.False(t, <-got)
	require.NoError(t, sub.Err())
}

// Close never waits on OnState, even when the caller holds a lock OnState
// takes; the old design deadlocked here.
func TestSubscribeCloseUnderOnStateLock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		c, _ := newFakeStream(t, func(_ int, r *http.Request) (*http.Response, error) {
			return sseOpen(r, schemaFrame("a")), nil
		})
		sub, err := c.Subscribe(t.Context(), "t", &SubscribeOptions{OnState: func(State) {
			mu.Lock()
			defer mu.Unlock()
		}})
		require.NoError(t, err)
		mu.Lock()
		require.NoError(t, sub.Close())
		mu.Unlock()
		synctest.Wait()
	})
}

// Close means the connection and its reader are gone, even while an OnState
// call is blocked.
func TestSubscribeCloseWhileOnStateBlocked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var bodies []*testBody
		var mu sync.Mutex
		c, _ := newFakeStream(t, func(n int, r *http.Request) (*http.Response, error) {
			frames := schemaFrame("a")
			hold := time.Duration(-1)
			if n == 1 {
				frames, hold = frames+rowFrame(1, 1), 0
			}
			b := sseBody(frames, hold)
			mu.Lock()
			bodies = append(bodies, b)
			mu.Unlock()
			return sseWith(r, b), nil
		})
		lives := 0
		sub, err := c.Subscribe(t.Context(), "t", &SubscribeOptions{OnState: func(st State) {
			if st == StateLive {
				if lives++; lives == 2 {
					<-release // blocks while the second connection is open
				}
			}
		}})
		require.NoError(t, err)
		assert.Equal(t, ts(1), next(t, sub).ID)
		time.Sleep(time.Minute) // fake time: past the backoff to the second connection
		synctest.Wait()

		require.NoError(t, sub.Close())
		select {
		case <-sub.done:
		default:
			assert.Fail(t, "Close returned with the reader still running")
		}
		mu.Lock()
		require.Len(t, bodies, 2)
		select {
		case <-bodies[1].closed:
		default:
			assert.Fail(t, "Close returned with the connection open")
		}
		mu.Unlock()
		close(release)
	})
}

// A reconnect before any event was delivered replays from the first live
// connection's start, so rows ingested during the backoff aren't lost.
func TestSubscribeResumesFromConnectionStart(t *testing.T) {
	for name, date := range map[string]string{"server Date": "Mon, 16 Nov 2026 16:00:00 GMT", "no Date": ""} {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				c, f := newFakeStream(t, func(n int, r *http.Request) (*http.Response, error) {
					res := sseOpen(r, ": connected\n\n", schemaFrame("a"))
					if n == 1 {
						res = sseEnds(r, ": connected\n\n", schemaFrame("a"))
					}
					if date != "" {
						res.Header.Set("Date", date)
					}
					return res, nil
				})
				sub, err := c.Subscribe(t.Context(), "t", nil)
				require.NoError(t, err)
				defer func() { _ = sub.Close() }()
				time.Sleep(time.Minute)

				reqs := f.requests()
				require.GreaterOrEqual(t, len(reqs), 2)
				assert.Equal(t, "table=t", reqs[0].URL.RawQuery)
				want := "2026-11-16T16:00:00Z"
				if date == "" {
					f.mu.Lock()
					want = f.at[0].Add(-resumeSlack).UTC().Format(time.RFC3339Nano)
					f.mu.Unlock()
				}
				for _, req := range reqs[1:] {
					assert.Equal(t, want, req.URL.Query().Get("since"))
					assert.Empty(t, req.Header.Values("Last-Event-Id"))
				}
			})
		})
	}
}

// When Subscribe returns, OnState has seen StateLive, however slow it is.
func TestSubscribeReportsLiveBeforeReturning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var st states
		c, _ := newFakeStream(t, func(_ int, r *http.Request) (*http.Response, error) { return sseOpen(r), nil })
		sub, err := c.Subscribe(t.Context(), "t", &SubscribeOptions{OnState: func(s State) {
			if s == StateLive {
				time.Sleep(time.Second)
			}
			st.on(s)
		}})
		require.NoError(t, err)
		assert.Equal(t, []State{StateConnecting, StateLive}, st.list())
		require.NoError(t, sub.Close())
	})
}

// Once live, a failing token source is ridden out rather than ending the
// stream; at the first connect it is still returned (TestSubscribeTokenSourceError).
func TestSubscribeTokenSourceRetriedAfterLive(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		source := func(context.Context) (string, error) {
			if n := calls.Add(1); n == 2 || n == 3 {
				return "", errors.New("idp down")
			}
			return "tok", nil
		}
		c, f := newFakeStream(t, func(n int, r *http.Request) (*http.Response, error) {
			if n == 1 {
				return sseEnds(r, schemaFrame("a"), rowFrame(1, 1)), nil
			}
			return sseOpen(r, schemaFrame("a"), rowFrame(2, 2)), nil
		}, WithTokenSource(source))
		sub, err := c.Subscribe(t.Context(), "t", nil)
		require.NoError(t, err)
		defer func() { _ = sub.Close() }()
		assert.Equal(t, ts(1), next(t, sub).ID)
		assert.Equal(t, ts(2), next(t, sub).ID)
		assert.Len(t, f.requests(), 2)
		assert.Equal(t, int32(4), calls.Load())
	})
}
