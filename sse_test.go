package wavehouse

import (
	"bufio"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// readAllSSE parses body to the end and returns the events and the error that
// ended it (io.EOF for a clean end).
func readAllSSE(r io.Reader, lastID string) ([]sseEvent, error) {
	rd := newSSEReader(r, lastID)
	var out []sseEvent
	for {
		ev, err := rd.next()
		if err != nil {
			return out, err
		}
		out = append(out, ev)
	}
}

func TestSSEReader(t *testing.T) {
	msg := func(data, id string) sseEvent { return sseEvent{event: "message", data: data, id: id} }
	cases := map[string]struct {
		in     string
		lastID string
		want   []sseEvent
	}{
		"the server's frames": {
			in: ": connected\n\nevent: schema\ndata: {\"columns\":[\"a\"]}\n\nid: t1\ndata: {\"row\":[1]}\n\n:\n\n",
			want: []sseEvent{
				{event: "schema", data: `{"columns":["a"]}`},
				msg(`{"row":[1]}`, "t1"),
			},
		},
		"CRLF":                       {in: "data: a\r\n\r\ndata: b\r\n\r\n", want: []sseEvent{msg("a", ""), msg("b", "")}},
		"CR":                         {in: "data: a\r\rdata: b\r\r", want: []sseEvent{msg("a", ""), msg("b", "")}},
		"mixed endings":              {in: "data: a\r\ndata: b\rdata: c\n\r\n", want: []sseEvent{msg("a\nb\nc", "")}},
		"multi-line data":            {in: "data: one\ndata: two\n\n", want: []sseEvent{msg("one\ntwo", "")}},
		"no space after the colon":   {in: "data:x\n\n", want: []sseEvent{msg("x", "")}},
		"only one space is stripped": {in: "data:  x\n\n", want: []sseEvent{msg(" x", "")}},
		"a field with no colon":      {in: "data\ndata\n\n", want: []sseEvent{msg("\n", "")}},
		"an empty data line counts":  {in: "data:\n\n", want: []sseEvent{msg("", "")}},
		"a colon in the value":       {in: "data: a:b\n\n", want: []sseEvent{msg("a:b", "")}},
		"no data, no event, and the type resets": {
			in:   "event: schema\n\ndata: x\n\n",
			want: []sseEvent{msg("x", "")},
		},
		"comments and unknown fields are skipped": {
			in:   ":keepalive\nfoo: bar\nretry: 10\ndata: x\n: mid\n\n",
			want: []sseEvent{msg("x", "")},
		},
		"the id persists and an empty one resets it": {
			in:   "id: 1\ndata: a\n\ndata: b\n\nid\ndata: c\n\n",
			want: []sseEvent{msg("a", "1"), msg("b", "1"), msg("c", "")},
		},
		"an id with NUL is ignored": {in: "id: 1\x002\ndata: a\n\n", lastID: "seed", want: []sseEvent{msg("a", "seed")}},
		"the seed id carries over":  {in: "data: a\n\n", lastID: "seed", want: []sseEvent{msg("a", "seed")}},
		"an id alone dispatches nothing but sticks": {
			in:   "id: 9\n\ndata: a\n\n",
			want: []sseEvent{msg("a", "9")},
		},
		"a leading BOM is skipped": {in: "\xEF\xBB\xBFdata: a\n\n", want: []sseEvent{msg("a", "")}},
		"a trailing unterminated event is dropped": {
			in:   "data: a\n\ndata: b\n",
			want: []sseEvent{msg("a", "")},
		},
		"a trailing unterminated line is dropped": {in: "data: a\n\ndata: b", want: []sseEvent{msg("a", "")}},
		"a named event": {in: "event: other\ndata: a\n\n", want: []sseEvent{{event: "other", data: "a"}}},
		"empty body":    {in: "", want: nil},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			// Whole, and one byte per read, which splits every CRLF across reads.
			for _, r := range []io.Reader{strings.NewReader(tc.in), iotest.OneByteReader(strings.NewReader(tc.in))} {
				got, err := readAllSSE(r, tc.lastID)
				require.ErrorIs(t, err, io.EOF)
				assert.Equal(t, tc.want, got)
			}
		})
	}
}

// A CR at the end of what has arrived ends the line at once, without waiting
// to see whether an LF follows; in a bubble, waiting would be a deadlock.
func TestSSEReaderDoesNotWaitAfterCR(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pr, pw := io.Pipe()
		gotA := make(chan struct{})
		go func() {
			_, _ = io.WriteString(pw, "data: a\r\r")
			<-gotA
			_, _ = io.WriteString(pw, "\ndata: b\r\n\r\n") // the LF finishes the CR before it
			_ = pw.Close()
		}()
		rd := newSSEReader(pr, "")
		ev, err := rd.next()
		require.NoError(t, err)
		assert.Equal(t, "a", ev.data)
		close(gotA)
		ev, err = rd.next()
		require.NoError(t, err)
		assert.Equal(t, "b", ev.data)
		_, err = rd.next()
		require.ErrorIs(t, err, io.EOF)
	})
}

func TestSSEReaderBounds(t *testing.T) {
	old := maxSSELine
	maxSSELine = 64
	t.Cleanup(func() { maxSSELine = old })

	// An oversize event is stepped over: the reader is still usable.
	evs, err := readAllSSE(strings.NewReader("id: 1\ndata: "+strings.Repeat("x", 100)+"\ndata: more\n\n"), "")
	require.ErrorIs(t, err, errSSETooLarge)
	assert.Empty(t, evs)
	rd := newSSEReader(strings.NewReader("id: 7\ndata: "+strings.Repeat("x", 100)+"\n\nid: 8\ndata: ok\n\n"), "")
	ev, err := rd.next()
	require.ErrorIs(t, err, errSSETooLarge)
	assert.Equal(t, "7", ev.id)
	ev, err = rd.next()
	require.NoError(t, err)
	assert.Equal(t, sseEvent{event: "message", data: "ok", id: "8"}, ev)

	// A single line past the cap and its slack ends the stream.
	_, err = readAllSSE(strings.NewReader("data: "+strings.Repeat("x", 64+sseLineSlack+10)+"\n\n"), "")
	require.ErrorIs(t, err, bufio.ErrTooLong)

	many := strings.Repeat("data: "+strings.Repeat("x", 20)+"\n", 4) + "\n"
	_, err = readAllSSE(strings.NewReader(many), "")
	require.ErrorIs(t, err, errSSETooLarge)

	// The read error itself comes through.
	boom := errors.New("reset")
	_, err = readAllSSE(io.MultiReader(strings.NewReader("data: a\n"), iotest.ErrReader(boom)), "")
	require.ErrorIs(t, err, boom)
}

func TestSSEReaderRetryField(t *testing.T) {
	rd := newSSEReader(strings.NewReader("retry: 1500\n\ndata: a\n\nretry: abc\ndata: b\n\nretry: -5\ndata: c\n\n"), "")
	_, err := rd.next()
	require.NoError(t, err)
	assert.Equal(t, 1500*time.Millisecond, rd.retry)
	_, err = rd.next()
	require.NoError(t, err)
	assert.Equal(t, 1500*time.Millisecond, rd.retry, "a non-numeric retry is ignored")
	_, err = rd.next()
	require.NoError(t, err)
	assert.Equal(t, 1500*time.Millisecond, rd.retry, "a negative retry is ignored")
}
