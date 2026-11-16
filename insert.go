package wavehouse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
)

// maxIngestBody is the server's request body cap for POST /v1/ingest; a larger
// body is refused here, before it is sent, with an error that says so.
const maxIngestBody = 16 << 20

// InsertResult is the server's per-record summary of one ingest call.
type InsertResult struct {
	Total      int `json:"total"`      // records read from the body
	Succeeded  int `json:"succeeded"`  // records validated and published
	Failed     int `json:"failed"`     // records rejected; see Records
	Duplicates int `json:"duplicates"` // records skipped by dedupe, when the server has it enabled
	// Records holds one entry per record, in order. The server truncates it
	// to the first 10,000 records of a very large batch; the counts above stay
	// authoritative.
	Records []RecordResult `json:"results"`
}

// RecordResult is the outcome of one record of a batch.
type RecordResult struct {
	Index     int    `json:"index"` // 1-based position in the body, as the server sends it
	OK        bool   `json:"ok"`
	Duplicate bool   `json:"duplicate"`
	Error     string `json:"error"` // why the record was rejected; "" otherwise
}

// InsertError is returned, together with the [*InsertResult], when the server
// accepted the request but rejected some records. Inspect it with errors.As.
// Records that were not rejected are published regardless.
type InsertError struct {
	// Count is how many records were rejected. It can exceed len(Failed) when
	// the server truncated its per-record list.
	Count int
	// Failed is the rejected records, in order, with the server's reason.
	Failed []RecordResult
}

// Error summarises the rejection and names the first few records.
func (e *InsertError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "wavehouse: ingest: %d records rejected", e.Count)
	for i, f := range e.Failed {
		if i == 3 {
			b.WriteString("; ...")
			break
		}
		fmt.Fprintf(&b, "; record %d: %s", f.Index, f.Error)
	}
	return b.String()
}

// From starts a query, or an insert, on table. The returned [*Query] is
// immutable: every builder method returns a modified copy, so a base query
// can be shared and extended safely.
func (c *Client) From(table string) *Query {
	return &Query{c: c, table: table}
}

// Insert sends rows to POST /v1/ingest?table= and returns the server's
// per-record summary. rows is one struct or map, or a slice or array of them;
// each is encoded with encoding/json (so use `json` tags, and time.Time is
// RFC 3339). Every call is sent as NDJSON, one line per row, so a single row
// gets the same summary as a batch. An empty slice sends nothing and returns a
// zero result.
//
// The server answers 200 even when records are rejected: those come back as
// an [*InsertError] together with the result, so check both. A body over
// 16 MiB, the server's limit, is refused before it is sent; split the batch.
// Insert is for a bare [Client.From]: a query that carries any read state
// (Select, Where, OrderBy, GroupBy, Limit, a time range or an aggregate) is an
// error, returned before anything is sent.
//
// Ingest is at-least-once: a failed attempt is retried, and where the server
// has dedupe configured an id field makes the retry exactly-once, reported in
// InsertResult.Duplicates.
func (q *Query) Insert(ctx context.Context, rows any) (*InsertResult, error) {
	if q.table == "" {
		return nil, errors.New("wavehouse: Insert: empty table name")
	}
	if err := q.checkInsert("Insert"); err != nil {
		return nil, err
	}
	body, n, err := encodeNDJSON(rows)
	if err != nil {
		return nil, fmt.Errorf("wavehouse: Insert: %w", err)
	}
	if n == 0 {
		return &InsertResult{}, nil
	}
	return q.ingest(ctx, body)
}

// InsertNDJSON sends a body that is already newline-delimited JSON, one
// object per line, as POST /v1/ingest?table= with the same result and errors
// as [Query.Insert], and the same refusal of a query with read state. The reader is read to its end and buffered, up to the
// server's 16 MiB limit, so a retry resends the same bytes. A body with no
// non-blank content sends nothing and returns a zero result.
func (q *Query) InsertNDJSON(ctx context.Context, r io.Reader) (*InsertResult, error) {
	if q.table == "" {
		return nil, errors.New("wavehouse: InsertNDJSON: empty table name")
	}
	if err := q.checkInsert("InsertNDJSON"); err != nil {
		return nil, err
	}
	body, err := io.ReadAll(io.LimitReader(r, maxIngestBody+1))
	if err != nil {
		return nil, fmt.Errorf("wavehouse: InsertNDJSON: read body: %w", err)
	}
	if len(body) > maxIngestBody {
		return nil, errBodyTooLarge("InsertNDJSON")
	}
	if len(bytes.TrimSpace(body)) == 0 {
		return &InsertResult{}, nil
	}
	return q.ingest(ctx, body)
}

// checkInsert refuses a query that carries read state: the server's ingest
// ignores it, so the caller's intent would be silently dropped.
func (q *Query) checkInsert(call string) error {
	if q.err != nil {
		return fmt.Errorf("wavehouse: %s on %q: %w", call, q.table, q.err)
	}
	if q.hasReadState() {
		return fmt.Errorf("wavehouse: %s on %q: the query carries read state (select, where, order, group, limit, time range or aggregates), which an insert ignores; call it on a bare From(table)", call, q.table)
	}
	return nil
}

func errBodyTooLarge(call string) error {
	return fmt.Errorf("wavehouse: %s: body exceeds the server's %d MiB limit; split the batch", call, maxIngestBody>>20)
}

func (q *Query) ingest(ctx context.Context, body []byte) (*InsertResult, error) {
	r := &request{
		kind: callIngest, method: http.MethodPost, path: "/v1/ingest",
		query: url.Values{"table": {q.table}}, body: body, contentType: "application/x-ndjson",
		callHeader: q.header,
	}
	res, err := q.c.do(ctx, r)
	if err != nil {
		return nil, err
	}
	var out InsertResult
	if err := decodeJSON(res.body, &out); err != nil {
		return nil, decodeError(r, err)
	}
	if out.Failed > 0 {
		ie := &InsertError{Count: out.Failed}
		for _, rec := range out.Records {
			if rec.Error != "" {
				ie.Failed = append(ie.Failed, rec)
			}
		}
		return &out, ie
	}
	return &out, nil
}

// encodeNDJSON marshals rows, one JSON object per line and no trailing
// newline, and returns the body and the row count.
func encodeNDJSON(rows any) ([]byte, int, error) {
	items, err := rowList(rows)
	if err != nil {
		return nil, 0, err
	}
	var buf bytes.Buffer
	for i, item := range items {
		line, err := json.Marshal(item)
		if err != nil {
			return nil, 0, fmt.Errorf("row %d: %w", i+1, err)
		}
		if len(line) == 0 || line[0] != '{' {
			return nil, 0, fmt.Errorf("row %d: %T encodes to %.20s, not a JSON object", i+1, item, line)
		}
		if i > 0 {
			buf.WriteByte('\n')
		}
		buf.Write(line)
		if buf.Len() > maxIngestBody {
			return nil, 0, errBodyTooLarge("Insert")
		}
	}
	return buf.Bytes(), len(items), nil
}

// rowList flattens rows into one element per record: a slice or array is
// spread, anything else is a single record.
func rowList(rows any) ([]any, error) {
	if raw, ok := rows.(json.RawMessage); ok {
		return []any{raw}, nil
	}
	v := reflect.ValueOf(rows)
	for v.Kind() == reflect.Pointer || v.Kind() == reflect.Interface {
		if v.IsNil() {
			return nil, errors.New("rows is nil")
		}
		v = v.Elem()
	}
	if !v.IsValid() {
		return nil, errors.New("rows is nil")
	}
	if k := v.Kind(); k != reflect.Slice && k != reflect.Array {
		return []any{v.Interface()}, nil
	}
	if v.Type().Elem().Kind() == reflect.Uint8 {
		return nil, errors.New("rows is a byte slice; pass pre-encoded NDJSON to InsertNDJSON")
	}
	items := make([]any, v.Len())
	for i := range items {
		items[i] = v.Index(i).Interface()
	}
	return items, nil
}
