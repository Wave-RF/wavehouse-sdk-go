package wavehouse

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strings"
)

// maxSSELine bounds one line, and one event's data, read from a stream. A row
// arrives as one data line, and the server accepts ingest bodies of up to
// 16 MiB, so twice that leaves room for any row it can publish.
var maxSSELine = 32 << 20 // a var so tests can lower it

// utf8BOM is skipped at the start of a stream, as the standard says.
var utf8BOM = []byte{0xEF, 0xBB, 0xBF}

// errSSETooLarge is an event whose data lines together exceed maxSSELine.
var errSSETooLarge = errors.New("server-sent event exceeds 32 MiB")

// sseEvent is one dispatched server-sent event.
type sseEvent struct {
	event string // "message" when the frame names none
	data  string
	id    string // the last event ID buffer at dispatch
}

// sseReader parses a text/event-stream body per the WHATWG HTML standard's
// "event stream interpretation": event, data, id and retry fields, comments,
// CRLF, LF or CR line endings, and an unterminated final event discarded.
type sseReader struct {
	sc      *bufio.Scanner
	lastID  string
	started bool
}

// newSSEReader reads r; lastID seeds the last event ID buffer, as a browser
// carries it across a reconnect.
func newSSEReader(r io.Reader, lastID string) *sseReader {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, min(4096, maxSSELine)), maxSSELine)
	sc.Split(splitSSELines())
	return &sseReader{sc: sc, lastID: lastID}
}

// next returns the next event, io.EOF at the end of the body, or the read
// error, bufio.ErrTooLong included.
func (r *sseReader) next() (sseEvent, error) {
	var event string
	var data strings.Builder
	for r.sc.Scan() {
		line := r.sc.Bytes()
		if !r.started {
			r.started = true
			line = bytes.TrimPrefix(line, utf8BOM)
		}
		if len(line) == 0 {
			if data.Len() == 0 {
				event = ""
				continue
			}
			if event == "" {
				event = "message"
			}
			return sseEvent{event: event, data: strings.TrimSuffix(data.String(), "\n"), id: r.lastID}, nil
		}
		if line[0] == ':' {
			continue // a comment: the server's ": connected" and keepalives
		}
		field, value, _ := bytes.Cut(line, []byte(":"))
		value = bytes.TrimPrefix(value, []byte(" "))
		switch string(field) {
		case "event":
			event = string(value)
		case "data":
			if data.Len()+len(value)+1 > maxSSELine {
				return sseEvent{}, errSSETooLarge
			}
			data.Write(value)
			data.WriteByte('\n')
		case "id":
			if bytes.IndexByte(value, 0) < 0 {
				r.lastID = string(value)
			}
		}
		// "retry" sets a browser's reconnection delay; the RetryPolicy decides ours, so it is ignored like
		// any unknown field.
	}
	if err := r.sc.Err(); err != nil {
		return sseEvent{}, err
	}
	return sseEvent{}, io.EOF
}

// splitSSELines splits on CRLF, LF or CR without waiting for the byte after a
// CR, which would hold back a CR-terminated event until more data arrived. An
// unterminated last line is dropped: the stream ended inside an event.
func splitSSELines() bufio.SplitFunc {
	skipLF := false // the previous token ended in a CR, so a leading LF finishes that line ending
	return func(data []byte, atEOF bool) (int, []byte, error) {
		if skipLF && len(data) > 0 {
			skipLF = false
			if data[0] == '\n' {
				return 1, nil, nil
			}
		}
		i := bytes.IndexAny(data, "\r\n")
		switch {
		case i < 0 && atEOF:
			return len(data), nil, nil
		case i < 0:
			return 0, nil, nil
		case data[i] == '\n':
			return i + 1, data[:i], nil
		case i+1 < len(data):
			if data[i+1] == '\n' {
				return i + 2, data[:i], nil
			}
			return i + 1, data[:i], nil
		default:
			skipLF = true
			return i + 1, data[:i], nil
		}
	}
}
