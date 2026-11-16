package wavehouse

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"
)

// maxSSELine bounds one line, and one event's data, read from a stream. A row
// arrives as one data line, and the server accepts ingest bodies of up to
// 16 MiB, so twice that leaves room for any row it can publish.
var maxSSELine = 32 << 20 // a var so tests can lower it

// sseLineSlack is how much longer than maxSSELine a line may be, for its field
// name.
const sseLineSlack = 4096

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
	// retry is the last "retry:" field the server sent, the reconnection time
	// it asks for; 0 until it sends one.
	retry time.Duration
}

// newSSEReader reads r; lastID seeds the last event ID buffer, as a browser
// carries it across a reconnect.
func newSSEReader(r io.Reader, lastID string) *sseReader {
	sc := bufio.NewScanner(r)
	// The slack lets a data line of exactly the cap in (its "data: " prefix
	// counts toward the line), so an oversize event surfaces as errSSETooLarge,
	// which the reader can step over, not as the scanner's fatal ErrTooLong.
	sc.Buffer(make([]byte, 0, min(4096, maxSSELine)), maxSSELine+sseLineSlack)
	sc.Split(splitSSELines())
	return &sseReader{sc: sc, lastID: lastID}
}

// next returns the next event, io.EOF at the end of the body, or the read
// error, bufio.ErrTooLong included. An event whose data exceeds the cap is
// read to its end and reported as errSSETooLarge with the id it carried (the
// reader is still usable); any other error ends the stream.
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
				return r.skipEvent()
			}
			data.Write(value)
			data.WriteByte('\n')
		case "id":
			if bytes.IndexByte(value, 0) < 0 {
				r.lastID = string(value)
			}
		case "retry":
			// Digits only, as the standard says; anything else is ignored.
			if ms, err := strconv.ParseUint(string(value), 10, 31); err == nil {
				r.retry = time.Duration(ms) * time.Millisecond
			}
		}
	}
	if err := r.sc.Err(); err != nil {
		return sseEvent{}, err
	}
	return sseEvent{}, io.EOF
}

// skipEvent reads on to the end of the current event, still honouring its id
// and retry fields, and reports it as errSSETooLarge.
func (r *sseReader) skipEvent() (sseEvent, error) {
	for r.sc.Scan() {
		line := r.sc.Bytes()
		if len(line) == 0 {
			break
		}
		field, value, _ := bytes.Cut(line, []byte(":"))
		value = bytes.TrimPrefix(value, []byte(" "))
		if string(field) == "id" && bytes.IndexByte(value, 0) < 0 {
			r.lastID = string(value)
		}
	}
	if err := r.sc.Err(); err != nil {
		return sseEvent{}, err
	}
	return sseEvent{id: r.lastID}, errSSETooLarge
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
