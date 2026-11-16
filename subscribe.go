package wavehouse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"sync"
	"time"
)

const streamPath = "/v1/stream"

// eventBuffer is how many received events a Subscription holds for a
// consumer that has fallen behind. Past it the SDK stops reading the
// connection, so memory stays bounded and the server sees the backpressure.
const eventBuffer = 64

// resumeSlack is how far before a connection's local start time a reconnect
// replays from when the server sent no Date header to take its clock from.
const resumeSlack = 5 * time.Second

// healthyConnection is how long a connection must stay up, delivering
// nothing, before a reconnect starts its backoff from MinBackoff again.
const healthyConnection = 30 * time.Second

// State is a [Subscription]'s connection state, reported to
// SubscribeOptions.OnState.
type State int

const (
	// StateConnecting is the first connection being made.
	StateConnecting State = iota
	// StateLive is an open connection: the server answered 200 with an event
	// stream. Events can still be lost while a connection opens; see
	// [Client.Subscribe].
	StateLive
	// StateReconnecting is a connection lost and being re-established.
	StateReconnecting
	// StateClosed is the end: Close, the ctx done, or a terminal error.
	StateClosed
)

// String returns "connecting", "live", "reconnecting" or "closed".
func (s State) String() string {
	switch s {
	case StateConnecting:
		return "connecting"
	case StateLive:
		return "live"
	case StateReconnecting:
		return "reconnecting"
	case StateClosed:
		return "closed"
	}
	return fmt.Sprintf("State(%d)", int(s))
}

// SubscribeOptions configures [Client.Subscribe]. A nil *SubscribeOptions
// streams live events only.
type SubscribeOptions struct {
	// Since replays the events the server received from this time on (the
	// server's ?since=, sent as RFC 3339 with nanoseconds) before the live
	// ones. Zero streams live events only.
	Since time.Time
	// ResumeID resumes after a checkpoint: a previous [Event.ID], sent as the
	// Last-Event-ID header. It wins over Since.
	ResumeID string
	// Header adds headers to every connection of this subscription, over the
	// client's [WithHeader] and [WithHeaderFunc] headers and under the SDK's
	// own, with the precedence of [Query.Header]; Last-Event-ID is the SDK's.
	// An invalid name or value is an error from Subscribe.
	Header http.Header
	// OnState, when set, is called with each change of connection state, in
	// order, one call at a time, on a goroutine of its own and never under a
	// lock of the SDK's. The calls up to the first StateLive complete before
	// Subscribe returns, so Subscribe must not be called holding a lock that
	// OnState takes. After that, a slow OnState delays only later state
	// reports, never events or Close. StateClosed comes last, exactly once,
	// and can come after Close has returned.
	OnState func(State)
}

// Event is one row streamed from a table.
type Event struct {
	// ID is the server's SSE id, the time it received the row. Pass it as
	// SubscribeOptions.ResumeID to resume after this event. Rows ingested in
	// one request share an ID.
	ID string
	// Table is the table the row belongs to.
	Table string
	// ReceivedAt is the server's received_timestamp parsed; zero if it is
	// missing or does not parse.
	ReceivedAt time.Time
	// Columns names the values, in order: the latest list the server
	// announced on this connection, already reduced to the columns the
	// caller's role may read. Events share the slice; don't modify it.
	Columns []string
	// Values holds one value per column, decoded with UseNumber: numbers are
	// [encoding/json.Number], so 64-bit integers stay exact; a column the
	// row did not carry is nil. Values are as the producer sent them, not as
	// the column stores them, except that DateTime values arrive as RFC 3339
	// UTC.
	Values []any
}

// Row returns the event's values by column name.
func (e Event) Row() Row {
	row := make(Row, len(e.Columns))
	for i, col := range e.Columns {
		row[col] = e.Values[i]
	}
	return row
}

// Decode stores the row in dst, a pointer to a struct or map, as
// encoding/json would decode a JSON object of the row: struct fields match
// columns by their json tags, and integer fields, uint64 included, are exact.
func (e Event) Decode(dst any) error {
	raw, err := json.Marshal(e.Row())
	if err != nil {
		return fmt.Errorf("wavehouse: Event.Decode: %w", err)
	}
	if err := decodeJSON(raw, dst); err != nil {
		return fmt.Errorf("wavehouse: Event.Decode: %w", err)
	}
	return nil
}

// Subscription is a stream of one table's rows, read like [database/sql.Rows]:
//
//	for sub.Next() {
//		ev := sub.Event()
//	}
//	if err := sub.Err(); err != nil { ... }
//
// Or range over [Subscription.Events]. Use Next and Event, or Events, not
// both. Next and Event belong to one goroutine; Close and Err may be called
// from any. A Subscription reconnects on its own and keeps doing so until it is
// closed, its ctx is done, or the server refuses it for good. Its goroutine
// and connection last until then, so always Close it.
type Subscription struct {
	c     *Client
	table string
	// since is where a connection with no event to resume from replays from:
	// the caller's Since, else the start of the first live connection.
	since time.Time
	// header is the caller's SubscribeOptions.Header, copied.
	header http.Header
	// notes delivers OnState calls; nil without OnState.
	notes *notifier

	ctx    context.Context
	cancel context.CancelFunc
	// events is closed by the reader goroutine as it exits, after err is set.
	events chan Event
	// done is closed when the reader goroutine has exited, its connection closed.
	done chan struct{}

	mu  sync.Mutex
	err error

	// The reader goroutine's own.
	lastID string
	state  State

	// The consumer's own.
	cur Event
}

// Subscribe streams the rows ingested into table, over GET /v1/stream
// (server-sent events).
//
// An unknown or misspelled table is not an error: the server accepts the
// stream and matches no events, so the subscription stays live and silent. To
// catch a typo, check the table first, with [Client.TableSchema] (admin) or a
// [Client.From] query with Limit(1), before subscribing.
//
// ctx bounds the whole subscription, not only the connect: cancelling it ends
// the stream as Close does. Subscribe returns once the server has answered
// 200 with an event stream. It returns an error, and no Subscription, when
// the first connection is refused for good (400, 401, 403, 404 or another
// status that is not retryable, a 200 that is not text/event-stream, a
// redirect while credentialed, or a token source error), or when every attempt the [RetryPolicy] allows has failed: a
// transport error, a 429 or a 5xx, honouring Retry-After.
//
// Once connected, a dropped connection, a retryable status or a malformed
// frame is retried without limit, waiting per the RetryPolicy's backoff (the
// count starts over after a connection that delivered an event or stayed up
// 30s), and resumes with Last-Event-ID set to the last event delivered. The
// token source and the header func ([WithTokenSource], [WithHeaderFunc]) are
// called on every connect, and once a connection has been live, their errors
// are retried like a transport error. RetryPolicy.AttemptTimeout bounds each
// connect up to the server's response headers, and never an established
// connection. A refusal for good, as above, ends the subscription with that
// error in Err.
//
// The server announces the column names in an "event: schema" frame and then
// sends each row as a positional array; Subscribe zips them into an [Event].
// A row whose length disagrees with the announced columns is dropped and the
// connection reopened from the last delivered event, which makes the server
// announce the columns again.
//
// Delivery is at-least-once across reconnects. A reconnect resumes from the
// last delivered event; when none has been delivered yet, it replays from
// Since or, without one, from the start of the first live connection (the
// server's Date header, else the local clock less 5s), so rows ingested
// while it reconnects are not lost. The server resumes by time, and every
// row of one ingest request carries the same ID, so events at the resume
// point can arrive twice. Dedupe on the row's own identity if that matters. Known server limitations: events published while a connection
// opens can be missed (WaveHouse#784), and a same-length column change after
// a gap-fill is not re-announced (WaveHouse#543). A consumer that falls far
// behind stops the SDK reading, and the server then drops events for this
// connection without telling it; keep Next moving.
func (c *Client) Subscribe(ctx context.Context, table string, opts *SubscribeOptions) (*Subscription, error) {
	if table == "" {
		return nil, errors.New("wavehouse: Subscribe: empty table name")
	}
	if opts == nil {
		opts = &SubscribeOptions{}
	}
	if err := checkHeaders(opts.Header); err != nil {
		return nil, fmt.Errorf("wavehouse: Subscribe: SubscribeOptions.Header: %w", err)
	}
	sctx, cancel := context.WithCancel(ctx)
	s := &Subscription{
		c:      c,
		table:  table,
		header: opts.Header.Clone(),
		since:  opts.Since,
		ctx:    sctx,
		cancel: cancel,
		events: make(chan Event, eventBuffer),
		done:   make(chan struct{}),
		lastID: opts.ResumeID,
		state:  -1, // none yet, so StateConnecting is reported
	}
	if opts.OnState != nil {
		s.notes = newNotifier(opts.OnState)
	}
	ready := make(chan error, 1)
	go s.run(ready)
	if err := <-ready; err != nil {
		<-s.done
		cancel()
		return nil, err
	}
	return s, nil
}

// Next waits for the next event and reports whether there is one. It
// returns false once the subscription is closed, its ctx is done, or it has
// ended with an error; then see Err.
func (s *Subscription) Next() bool {
	if s.ctx.Err() != nil {
		return false
	}
	select {
	case <-s.ctx.Done():
		return false
	case ev, ok := <-s.events:
		if !ok {
			return false
		}
		s.cur = ev
		return true
	}
}

// Event returns the event the last successful Next read.
func (s *Subscription) Event() Event { return s.cur }

// Events returns the subscription's events as a channel, for a consumer that
// selects over several sources. The channel is closed when the subscription
// ends, whether by Close, the ctx being done or a terminal error; then call
// [Subscription.Err] to tell them apart. After Close, events already buffered
// may still be read before the close.
//
// It is the same stream Next reads, not a copy: use Next and Event, or
// Events, not both on one Subscription. There is no goroutine behind it, so a
// consumer that stops reading leaks nothing; the SDK just stops reading the
// connection (see [Client.Subscribe] on a slow consumer) until Close or the
// ctx ends it. Always Close the Subscription.
func (s *Subscription) Events() <-chan Event { return s.events }

// Err returns the error that ended the subscription, or nil while it runs and
// after it was closed or its ctx was done.
func (s *Subscription) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Close ends the subscription and unblocks a waiting Next. It is safe to call
// more than once and from any goroutine, OnState included. It returns once
// the connection is closed and the goroutine reading it has exited, and it
// never waits on OnState. It always returns nil.
func (s *Subscription) Close() error {
	s.cancel()
	<-s.done
	return nil
}

// run is the subscription's goroutine. It reports the first connect's
// outcome on ready, and then keeps the stream connected until the end.
func (s *Subscription) run(ready chan<- error) {
	defer close(s.done)
	err := s.loop(ready)
	if s.ctx.Err() != nil {
		// Closed or cancelled: the end the caller asked for, not a failure.
		err = nil
	}
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
	close(s.events)
	s.setState(StateClosed)
}

// notifier calls OnState on a goroutine of its own, so the reader, and Close
// waiting on it, never wait on the caller's code. The queue is unbounded but
// grows by at most one state per connection attempt.
type notifier struct {
	fn    func(State)
	wake  chan struct{}
	mu    sync.Mutex
	queue []note
}

type note struct {
	state State
	ack   chan struct{} // closed once fn has returned for it; may be nil
}

func newNotifier(fn func(State)) *notifier {
	n := &notifier{fn: fn, wake: make(chan struct{}, 1)}
	go n.run()
	return n
}

func (n *notifier) push(st State, ack chan struct{}) {
	n.mu.Lock()
	n.queue = append(n.queue, note{st, ack})
	n.mu.Unlock()
	select {
	case n.wake <- struct{}{}:
	default:
	}
}

// run delivers notes until StateClosed, always the last one pushed.
func (n *notifier) run() {
	for range n.wake {
		for {
			n.mu.Lock()
			if len(n.queue) == 0 {
				n.mu.Unlock()
				break
			}
			nt := n.queue[0]
			n.queue = n.queue[1:]
			n.mu.Unlock()
			n.fn(nt.state)
			if nt.ack != nil {
				close(nt.ack)
			}
			if nt.state == StateClosed {
				return
			}
		}
	}
}

// loop connects and reconnects until the ctx is done or an error is terminal,
// and returns that error. ready is sent the first connect's outcome, once.
func (s *Subscription) loop(ready chan<- error) (end error) {
	defer func() {
		if ready != nil {
			ready <- end
		}
	}()
	s.setState(StateConnecting)
	failures := 0
	live := false
	for {
		res, err := s.connect() //nolint:bodyclose // consume closes it
		if err == nil {
			live = true
			if s.since.IsZero() && s.lastID == "" {
				s.since = connectionStart(res)
			}
			if ready != nil {
				// OnState has seen StateLive by the time Subscribe returns.
				s.setStateWait(StateLive)
				ready <- nil
				ready = nil
			}
			s.setState(StateLive)
			start := time.Now()
			var delivered bool
			delivered, err = s.consume(res)
			if s.ctx.Err() != nil {
				return s.ctxError(err)
			}
			if delivered || time.Since(start) >= healthyConnection {
				failures = 0
			}
			s.setState(StateReconnecting)
		} else {
			if s.ctx.Err() != nil {
				return s.ctxError(err)
			}
			// A token source or header func that fails after the stream was live is a blip to ride out, not a refusal.
			retryable := IsRetryable(err) || (live && (errors.Is(err, errTokenSource) || errors.Is(err, errHeaderFunc)))
			if !retryable || (ready != nil && failures+1 >= s.c.retry.MaxAttempts) {
				noteAttempts(err, failures+1)
				return err
			}
		}
		failures++
		wait := s.c.retry.backoff(failures)
		if ae, ok := errors.AsType[*APIError](err); ok && ae.RetryAfter > 0 {
			wait = min(ae.RetryAfter, s.c.retry.MaxBackoff)
		}
		if werr := sleep(s.ctx, wait); werr != nil {
			return fmt.Errorf("wavehouse: GET %s: %w while waiting to retry after: %s", streamPath, werr, err.Error())
		}
	}
}

// ctxError is what loop returns once the ctx is done: err when it already
// says so, else the ctx's error.
func (s *Subscription) ctxError(err error) error {
	if err != nil && errors.Is(err, s.ctx.Err()) {
		return err
	}
	return fmt.Errorf("wavehouse: GET %s: %w", streamPath, s.ctx.Err())
}

// connect opens one connection and returns it once the server has answered
// 200 with an event stream; any other answer is an error.
func (s *Subscription) connect() (*http.Response, error) {
	r := &request{
		kind:       callRead,
		method:     http.MethodGet,
		path:       streamPath,
		query:      url.Values{"table": {s.table}},
		accept:     "text/event-stream",
		callHeader: s.header,
	}
	switch {
	case s.lastID != "":
		r.header = http.Header{"Last-Event-Id": {s.lastID}}
	case !s.since.IsZero():
		r.query.Set("since", s.since.UTC().Format(time.RFC3339Nano))
	}
	// The attempt timeout runs until the response headers are in: the body of
	// an established stream is long-lived and has no deadline.
	actx, cancel := context.WithCancelCause(s.ctx)
	var timer *time.Timer
	if d := s.c.retry.AttemptTimeout; d > 0 {
		timer = time.AfterFunc(d, func() { cancel(ErrAttemptTimeout) })
	}
	fail := func() {
		if timer != nil {
			timer.Stop()
		}
		cancel(nil)
	}
	res, err := s.c.send(s.ctx, actx, r)
	if err != nil {
		fail()
		return nil, err
	}
	if res.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(res.Body, maxErrorBody))
		_ = res.Body.Close()
		fail()
		return nil, newAPIError(r, res, body)
	}
	if ct := res.Header.Get("Content-Type"); res.StatusCode != http.StatusOK || !isEventStream(ct) {
		_ = res.Body.Close()
		fail()
		return nil, fmt.Errorf("wavehouse: GET %s: the server answered %d with Content-Type %q, not an event stream",
			streamPath, res.StatusCode, ct)
	}
	if timer != nil && !timer.Stop() {
		// The deadline fired as the headers arrived, so the body is cancelled too.
		_ = res.Body.Close()
		return nil, s.c.transportError(s.ctx, actx, r, context.Cause(actx), true)
	}
	// The body lives under actx; releasing it with the body frees the context.
	res.Body = &releaseBody{ReadCloser: res.Body, release: func() { cancel(nil) }}
	return res, nil
}

// releaseBody runs release when the body is closed.
type releaseBody struct {
	io.ReadCloser
	release func()
}

func (b *releaseBody) Close() error {
	defer b.release()
	return b.ReadCloser.Close()
}

// connectionStart is when res's connection began by the server's clock,
// which is what it compares since against: its Date header (whole seconds,
// so never later than the start), else the local clock less resumeSlack.
func connectionStart(res *http.Response) time.Time {
	if t, err := http.ParseTime(res.Header.Get("Date")); err == nil {
		return t
	}
	return time.Now().Add(-resumeSlack)
}

func isEventStream(contentType string) bool {
	mt, _, err := mime.ParseMediaType(contentType)
	return err == nil && mt == "text/event-stream"
}

// errNoSchema is a row that arrives before any schema frame.
var errNoSchema = errors.New("row before the schema frame")

// errArity is a row that doesn't match the announced columns.
var errArity = errors.New("row length differs from the announced columns")

// consume reads one connection until it ends, handing each row to the
// consumer, and reports whether it delivered any. It always returns an error
// saying why the connection ended, io.EOF for a clean end.
func (s *Subscription) consume(res *http.Response) (delivered bool, err error) {
	// Close the body when the ctx ends, so a transport that ignores the ctx
	// mid-body can't hold Close up.
	stop := context.AfterFunc(s.ctx, func() { _ = res.Body.Close() })
	defer func() {
		stop()
		_ = res.Body.Close()
	}()

	rd := newSSEReader(res.Body, s.lastID)
	var columns []string
	haveSchema := false
	for {
		f, err := rd.next()
		if err != nil {
			return delivered, err
		}
		switch f.event {
		case "schema":
			var frame struct {
				Columns []string `json:"columns"`
			}
			if err := json.Unmarshal([]byte(f.data), &frame); err != nil {
				return delivered, fmt.Errorf("schema frame: %w", err)
			}
			columns = frame.Columns
			haveSchema = true
		case "message":
			if !haveSchema {
				// The server announces the columns before the first row of every connection.
				return delivered, fmt.Errorf("data frame %s: %w", f.id, errNoSchema)
			}
			ev, err := decodeEvent(f, columns, s.table)
			if err != nil {
				// Dropped, not delivered: the reconnect resumes from the last event that was.
				return delivered, err
			}
			select {
			case s.events <- ev:
			case <-s.ctx.Done():
				return delivered, s.ctx.Err()
			}
			delivered = true
			if ev.ID != "" {
				s.lastID = ev.ID
			}
		}
	}
}

// decodeEvent zips a data frame's positional row with columns.
func decodeEvent(f sseEvent, columns []string, table string) (Event, error) {
	var frame struct {
		TableName         string `json:"table_name"`
		ReceivedTimestamp string `json:"received_timestamp"`
		Row               []any  `json:"row"`
	}
	dec := json.NewDecoder(bytes.NewReader([]byte(f.data)))
	dec.UseNumber()
	if err := dec.Decode(&frame); err != nil {
		return Event{}, fmt.Errorf("data frame: %w", err)
	}
	if len(frame.Row) != len(columns) {
		return Event{}, fmt.Errorf("data frame %s: %w: %d values, %d columns", f.id, errArity, len(frame.Row), len(columns))
	}
	if frame.TableName == "" {
		frame.TableName = table
	}
	ev := Event{ID: f.id, Table: frame.TableName, Columns: columns, Values: frame.Row}
	if t, err := time.Parse(time.RFC3339Nano, frame.ReceivedTimestamp); err == nil {
		ev.ReceivedAt = t
	}
	return ev, nil
}

// setState records st and queues it for OnState, if it changed.
func (s *Subscription) setState(st State) {
	if st == s.state {
		return
	}
	s.state = st
	if s.notes != nil {
		s.notes.push(st, nil)
	}
}

// setStateWait is setState that also waits until OnState has returned for
// st, or the ctx is done.
func (s *Subscription) setStateWait(st State) {
	if st == s.state || s.notes == nil {
		s.state = st
		return
	}
	s.state = st
	ack := make(chan struct{})
	s.notes.push(st, ack)
	select {
	case <-ack:
	case <-s.ctx.Done():
	}
}
