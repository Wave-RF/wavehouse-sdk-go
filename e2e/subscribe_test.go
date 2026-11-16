package e2e

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	wavehouse "github.com/Wave-RF/wavehouse-sdk-go"
)

// ingest posts rows to e2e_events in one request, through the server directly
// so a broken SDK insert can't fail the stream tests.
func ingest(t *testing.T, rows ...map[string]any) {
	t.Helper()
	require.NoError(t, adminCall(t.Context(), http.MethodPost, "/v1/ingest?table=e2e_events", rows))
}

// pump reads sub on its own goroutine until it ends.
func pump(sub *wavehouse.Subscription) <-chan wavehouse.Event {
	ch := make(chan wavehouse.Event, 1024)
	go func() {
		defer close(ch)
		for sub.Next() {
			ch <- sub.Event()
		}
	}()
	return ch
}

// collect returns the next n events whose name column is name, skipping
// other tests' rows, and fails if they don't arrive within 20s.
func collect(t *testing.T, events <-chan wavehouse.Event, name string, n int) []wavehouse.Event {
	t.Helper()
	var got []wavehouse.Event
	timeout := time.After(20 * time.Second)
	for len(got) < n {
		select {
		case ev, ok := <-events:
			require.True(t, ok, "the stream ended")
			if ev.Row()["name"] == name {
				got = append(got, ev)
			}
		case <-timeout:
			require.FailNowf(t, "timed out", "%d of %d %q events arrived", len(got), n, name)
		}
	}
	return got
}

// subscribe opens a subscription closed with the test.
func subscribe(t *testing.T, c *wavehouse.Client, opts *wavehouse.SubscribeOptions) *wavehouse.Subscription {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	t.Cleanup(cancel)
	sub, err := c.Subscribe(ctx, "e2e_events", opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Close() })
	return sub
}

func TestSubscribeLive(t *testing.T) {
	live := make(chan struct{}, 1)
	sub := subscribe(t, newClient(t), &wavehouse.SubscribeOptions{OnState: func(s wavehouse.State) {
		if s == wavehouse.StateLive {
			select {
			case live <- struct{}{}:
			default:
			}
		}
	}})
	select {
	case <-live:
	default:
		require.FailNow(t, "Subscribe returned before reporting StateLive")
	}
	events := pump(sub)

	// The server answers before it registers the stream (WaveHouse#784), so rows sent at once can be
	// missed. Insert warm-up rows until one arrives, which proves the stream is registered.
	warm := uniqueName(t) + "-warm"
	tries := 0
	for arrived := false; !arrived; {
		require.Less(t, tries, 40, "no warm-up row arrived")
		tries++
		ingest(t, eventRow(warm, 0))
		wait := time.After(250 * time.Millisecond)
		for waiting := true; waiting && !arrived; {
			select {
			case ev := <-events:
				arrived = ev.Row()["name"] == warm
			case <-wait:
				waiting = false
			}
		}
	}
	t.Logf("first warm-up row arrived after %d inserts", tries)

	name := uniqueName(t)
	a, b := eventRow(name, 1.5), eventRow(name, 0)
	b["value"] = json.Number("9007199254740993") // above 2^53: a float64 would round it
	sent := time.Now()
	ingest(t, a, b)

	got := collect(t, events, name, 2)
	cols := []string{"event_id", "name", "value", "ts"}
	for i, want := range []map[string]any{a, b} {
		ev := got[i]
		assert.Equal(t, "e2e_events", ev.Table)
		assert.Equal(t, cols, ev.Columns)
		assert.Equal(t, want["event_id"], ev.Values[0])
		assert.Equal(t, name, ev.Values[1])
		assert.Nil(t, ev.Values[3], "the record omitted ts, so its slot is null")
		assert.Equal(t, ev.ID, ev.ReceivedAt.Format(time.RFC3339Nano))
		assert.WithinDuration(t, sent, ev.ReceivedAt, 10*time.Second)
	}
	assert.Equal(t, json.Number("1.5"), got[0].Values[2])
	// The stream forwards the number as the producer wrote it, not as the Float64 column stores it.
	assert.Equal(t, json.Number("9007199254740993"), got[1].Values[2])
	assert.Equal(t, got[0].ID, got[1].ID, "rows of one ingest request share a received_timestamp")

	var row struct {
		EventID string  `json:"event_id"`
		Name    string  `json:"name"`
		Value   float64 `json:"value"`
		TS      *string `json:"ts"`
	}
	require.NoError(t, got[0].Decode(&row))
	assert.Equal(t, a["event_id"], row.EventID)
	assert.InDelta(t, 1.5, row.Value, 0)
	assert.Nil(t, row.TS)
}

func TestSubscribeSince(t *testing.T) {
	name := uniqueName(t)
	start := time.Now().Add(-time.Second)
	ingest(t, eventRow(name, 1))
	ingest(t, eventRow(name, 2))

	sub := subscribe(t, newClient(t), &wavehouse.SubscribeOptions{Since: start})
	got := collect(t, pump(sub), name, 2)
	assert.Equal(t, json.Number("1"), got[0].Values[2])
	assert.Equal(t, json.Number("2"), got[1].Values[2])
}

func TestSubscribeResume(t *testing.T) {
	c := newClient(t)
	name := uniqueName(t)
	start := time.Now().Add(-time.Second)
	ingest(t, eventRow(name, 1))
	ingest(t, eventRow(name, 2))

	first := subscribe(t, c, &wavehouse.SubscribeOptions{Since: start})
	got := collect(t, pump(first), name, 2)
	checkpoint := got[1].ID
	require.NoError(t, first.Close())

	ingest(t, eventRow(name, 3))
	resumed := subscribe(t, c, &wavehouse.SubscribeOptions{ResumeID: checkpoint, Since: start})
	events := pump(resumed)
	var values []json.Number
	for v := json.Number(""); v != "3"; {
		v = collect(t, events, name, 1)[0].Values[2].(json.Number)
		values = append(values, v)
	}
	// Row 1 is before the checkpoint and must not come back. Row 2 is the checkpoint itself: the server
	// resumes by time, so it may come back (at-least-once), and the log says whether it did.
	assert.NotContains(t, values, json.Number("1"))
	t.Logf("resumed from row 2's id, the stream delivered values %v", values)
}

func TestSubscribeCloseEndsNext(t *testing.T) {
	sub := subscribe(t, newClient(t), nil)
	done := make(chan bool)
	go func() { done <- sub.Next() }()
	time.Sleep(100 * time.Millisecond) // let Next block on the quiet stream
	start := time.Now()
	require.NoError(t, sub.Close())
	select {
	case more := <-done:
		assert.False(t, more)
	case <-time.After(2 * time.Second):
		require.FailNow(t, "Next still blocked after Close")
	}
	assert.Less(t, time.Since(start), time.Second)
	require.NoError(t, sub.Err())
}

// dropFirst ends the first stream's body at once, as a proxy dropping the
// connection would.
type dropFirst struct {
	base http.RoundTripper
	n    atomic.Int32
}

func (d *dropFirst) RoundTrip(r *http.Request) (*http.Response, error) {
	res, err := d.base.RoundTrip(r)
	if err == nil && d.n.Add(1) == 1 {
		res.Body = endedBody{res.Body}
	}
	return res, err
}

type endedBody struct{ io.ReadCloser }

func (endedBody) Read([]byte) (int, error) { return 0, io.EOF }

// A row ingested while the stream reconnects, before it delivered anything,
// still arrives: the reconnect replays from the first connection's start.
func TestSubscribeReconnectBeforeAnyEvent(t *testing.T) {
	reconnecting := make(chan struct{}, 1)
	c := newClient(t,
		wavehouse.WithHTTPClient(&http.Client{Transport: &dropFirst{base: http.DefaultTransport}}),
		wavehouse.WithRetry(wavehouse.RetryPolicy{MaxAttempts: 2, MinBackoff: 2 * time.Second, MaxBackoff: 4 * time.Second}))
	sub := subscribe(t, c, &wavehouse.SubscribeOptions{OnState: func(s wavehouse.State) {
		if s == wavehouse.StateReconnecting {
			select {
			case reconnecting <- struct{}{}:
			default:
			}
		}
	}})
	events := pump(sub)
	select {
	case <-reconnecting:
	case <-time.After(10 * time.Second):
		require.FailNow(t, "the first connection never dropped")
	}
	name := uniqueName(t)
	ingest(t, eventRow(name, 7)) // inside the 1.6s-2.4s backoff
	got := collect(t, events, name, 1)
	assert.Equal(t, json.Number("7"), got[0].Values[2])
}

func TestSubscribeEvents(t *testing.T) {
	sub := subscribe(t, newClient(t), nil)
	events := sub.Events()

	// Warm up as TestSubscribeLive does, then check a named row through the channel.
	warm := uniqueName(t) + "-warm"
	for tries := 0; ; tries++ {
		require.Less(t, tries, 40, "no warm-up row arrived")
		ingest(t, eventRow(warm, 0))
		select {
		case ev := <-events:
			if ev.Row()["name"] != warm {
				continue
			}
		case <-time.After(250 * time.Millisecond):
			continue
		}
		break
	}
	name := uniqueName(t)
	ingest(t, eventRow(name, 3))
	got := collect(t, events, name, 1)
	v, err := got[0].Row().Float64("value")
	require.NoError(t, err)
	assert.InDelta(t, 3, v, 0)

	require.NoError(t, sub.Close())
	require.Eventually(t, func() bool {
		select {
		case _, ok := <-events:
			return !ok
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond, "Events was not closed by Close")
	require.NoError(t, sub.Err())
}

// A table the server doesn't have is no error: the stream is accepted and stays quiet.
func TestSubscribeUnknownTable(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	sub, err := newClient(t).Subscribe(ctx, "no_such_table", nil)
	require.NoError(t, err)
	select {
	case ev := <-sub.Events():
		require.FailNowf(t, "unexpected event", "%v", ev)
	case <-time.After(time.Second):
	}
	require.NoError(t, sub.Close())
	require.NoError(t, sub.Err())
}
