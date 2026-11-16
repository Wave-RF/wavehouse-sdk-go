package wavehouse

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInsertSingleRowIsOneNDJSONLine(t *testing.T) {
	rec := newRecorder(t, reply(200, `{"total":1,"succeeded":1,"failed":0,"duplicates":0,"results":[{"index":1,"ok":true}]}`))
	res, err := rec.client(t).From("my table").Insert(t.Context(), map[string]any{"page": "/home", "n": 1})
	require.NoError(t, err)
	assert.Equal(t, &InsertResult{Total: 1, Succeeded: 1, Records: []RecordResult{{Index: 1, OK: true}}}, res)

	got := rec.last(t)
	assert.Equal(t, "POST", got.method)
	assert.Equal(t, "/v1/ingest", got.path)
	assert.Equal(t, "table=my+table", got.rawQuery)
	assert.Equal(t, "application/x-ndjson", got.header.Get("Content-Type"))
	assert.Equal(t, `{"n":1,"page":"/home"}`, got.body)
}

func TestInsertShapes(t *testing.T) {
	type click struct {
		Page string    `json:"page"`
		At   time.Time `json:"at"`
		Skip string    `json:"-"`
	}
	at := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	c := click{Page: "/a", At: at, Skip: "x"}
	cases := map[string]struct {
		rows any
		want string
	}{
		"struct":            {c, `{"page":"/a","at":"2026-01-02T03:04:05Z"}`},
		"pointer to struct": {&c, `{"page":"/a","at":"2026-01-02T03:04:05Z"}`},
		"struct slice":      {[]click{c, {Page: "/b"}}, "{\"page\":\"/a\",\"at\":\"2026-01-02T03:04:05Z\"}\n{\"page\":\"/b\",\"at\":\"0001-01-01T00:00:00Z\"}"},
		"pointer slice":     {[]*click{&c}, `{"page":"/a","at":"2026-01-02T03:04:05Z"}`},
		"map slice":         {[]map[string]any{{"a": 1}, {"b": 2}}, "{\"a\":1}\n{\"b\":2}"},
		"any slice":         {[]any{map[string]int{"a": 1}, c}, "{\"a\":1}\n{\"page\":\"/a\",\"at\":\"2026-01-02T03:04:05Z\"}"},
		"array":             {[1]map[string]int{{"a": 1}}, `{"a":1}`},
		"pointer to slice":  {&[]map[string]int{{"a": 1}}, `{"a":1}`},
		"raw message":       {json.RawMessage(`{"a":1}`), `{"a":1}`},
		"newlines escaped":  {map[string]string{"a": "x\ny"}, `{"a":"x\ny"}`},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rec := newRecorder(t, reply(200, `{"total":1}`))
			_, err := rec.client(t).From("t").Insert(t.Context(), tc.rows)
			require.NoError(t, err)
			assert.Equal(t, tc.want, rec.last(t).body)
		})
	}
}

func TestInsertEmptySendsNothing(t *testing.T) {
	rec := newRecorder(t, nil)
	q := rec.client(t).From("t")
	for name, rows := range map[string]any{"nil slice": []map[string]any(nil), "empty slice": []int{}, "empty array": [0]string{}} {
		res, err := q.Insert(t.Context(), rows)
		require.NoError(t, err, name)
		assert.Equal(t, &InsertResult{}, res, name)
	}
	assert.Empty(t, rec.requests())
}

func TestInsertRejectsBadRows(t *testing.T) {
	rec := newRecorder(t, nil)
	q := rec.client(t).From("t")
	var nilMap map[string]any
	var nilPtr *struct{}
	for name, tc := range map[string]struct {
		rows any
		want string
	}{
		"nil":             {nil, "rows is nil"},
		"nil pointer":     {nilPtr, "rows is nil"},
		"scalar":          {5, "not a JSON object"},
		"string":          {"x", "not a JSON object"},
		"slice of ints":   {[]int{1}, "row 1"},
		"nil map row":     {[]map[string]any{{"a": 1}, nilMap}, "row 2"},
		"bytes":           {[]byte(`{"a":1}`), "InsertNDJSON"},
		"unencodable":     {map[string]any{"c": make(chan int)}, "row 1"},
		"unencodable 2nd": {[]any{map[string]int{}, func() {}}, "row 2"},
	} {
		_, err := q.Insert(t.Context(), tc.rows)
		require.ErrorContains(t, err, tc.want, name)
	}
	_, err := (&Client{}).From("").Insert(t.Context(), map[string]int{})
	require.ErrorContains(t, err, "empty table name")
	assert.Empty(t, rec.requests())
}

func TestInsertBodyCap(t *testing.T) {
	rec := newRecorder(t, reply(200, `{"total":1}`))
	q := rec.client(t).From("t")
	big := strings.Repeat("x", 1<<20)
	rows := make([]map[string]string, 17) // 17 MiB
	for i := range rows {
		rows[i] = map[string]string{"s": big}
	}
	_, err := q.Insert(t.Context(), rows)
	require.ErrorContains(t, err, "16 MiB limit")
	assert.Empty(t, rec.requests())

	// Just under the cap goes through.
	_, err = q.Insert(t.Context(), rows[:15])
	require.NoError(t, err)
	assert.Len(t, rec.requests(), 1)

	_, err = q.InsertNDJSON(t.Context(), strings.NewReader(strings.Repeat("x", 16<<20+1)))
	require.ErrorContains(t, err, "16 MiB limit")
	assert.Len(t, rec.requests(), 1)
}

func TestInsertPartialFailure(t *testing.T) {
	rec := newRecorder(t, reply(200, `{"total":4,"succeeded":1,"failed":2,"duplicates":1,"results":[
		{"index":1,"duplicate":true},{"index":2,"ok":true},
		{"index":3,"error":"unknown column \"referrer\" for table \"clicks\""},{"index":4,"error":"invalid json"}]}`))
	res, err := rec.client(t).From("clicks").Insert(t.Context(), []map[string]int{{}, {}, {}, {}})
	require.Error(t, err)
	// The result still comes back: 1 record was published.
	require.NotNil(t, res)
	assert.Equal(t, 4, res.Total)
	assert.Equal(t, 1, res.Succeeded)
	assert.Equal(t, 1, res.Duplicates)
	assert.Equal(t, []RecordResult{
		{Index: 1, Duplicate: true},
		{Index: 2, OK: true},
		{Index: 3, Error: `unknown column "referrer" for table "clicks"`},
		{Index: 4, Error: "invalid json"},
	}, res.Records)

	ie, ok := errors.AsType[*InsertError](err)
	require.True(t, ok, "%v", err)
	assert.Equal(t, 2, ie.Count)
	assert.Equal(t, res.Records[2:], ie.Failed)
	assert.Equal(t, `wavehouse: ingest: 2 records rejected; record 3: unknown column "referrer" for table "clicks"; record 4: invalid json`, err.Error())
}

func TestInsertErrorTruncatesItsMessage(t *testing.T) {
	e := &InsertError{Count: 9, Failed: []RecordResult{{Index: 1, Error: "a"}, {Index: 2, Error: "b"}, {Index: 3, Error: "c"}, {Index: 4, Error: "d"}}}
	assert.Equal(t, "wavehouse: ingest: 9 records rejected; record 1: a; record 2: b; record 3: c; ...", e.Error())
}

func TestInsertDuplicates(t *testing.T) {
	rec := newRecorder(t, reply(200, `{"total":2,"succeeded":1,"failed":0,"duplicates":1,"results":[{"index":1,"ok":true},{"index":2,"duplicate":true}]}`))
	res, err := rec.client(t).From("t").Insert(t.Context(), []map[string]string{{"event_id": "e1"}, {"event_id": "e1"}})
	require.NoError(t, err, "a duplicate is not a failure")
	assert.Equal(t, 1, res.Duplicates)
	assert.True(t, res.Records[1].Duplicate)
}

func TestInsertNDJSON(t *testing.T) {
	rec := newRecorder(t, reply(200, `{"total":2,"succeeded":2}`))
	q := rec.client(t).From("t")
	body := "{\"a\":1}\n\n{\"b\":2}\n"
	res, err := q.InsertNDJSON(t.Context(), strings.NewReader(body))
	require.NoError(t, err)
	assert.Equal(t, 2, res.Succeeded)
	assert.Equal(t, body, rec.last(t).body, "sent as is")
	assert.Equal(t, "application/x-ndjson", rec.last(t).header.Get("Content-Type"))

	res, err = q.InsertNDJSON(t.Context(), strings.NewReader(" \n\n"))
	require.NoError(t, err)
	assert.Equal(t, &InsertResult{}, res)
	assert.Len(t, rec.requests(), 1, "a blank body sends nothing")

	_, err = q.InsertNDJSON(t.Context(), iotestErrReader{})
	require.ErrorContains(t, err, "read body")
	_, err = (&Client{}).From("").InsertNDJSON(t.Context(), strings.NewReader("{}"))
	require.ErrorContains(t, err, "empty table name")
}

type iotestErrReader struct{}

func (iotestErrReader) Read([]byte) (int, error) { return 0, errors.New("boom") }

func TestInsertAPIErrors(t *testing.T) {
	rec := newRecorder(t, reply(413, `{"error":"request body exceeded 16777216 bytes"}`))
	res, err := rec.client(t).From("t").Insert(t.Context(), map[string]int{"a": 1})
	require.ErrorContains(t, err, "413")
	assert.Nil(t, res)
	_, isInsert := errors.AsType[*InsertError](err)
	assert.False(t, isInsert)

	_, err = newRecorder(t, reply(200, `nope`)).client(t).From("t").Insert(t.Context(), map[string]int{"a": 1})
	require.ErrorContains(t, err, "decode response")
}

func TestInsertCustomHeader(t *testing.T) {
	rec := newRecorder(t, reply(200, `{"total":1}`))
	_, err := rec.client(t, WithHeader("X-Tenant-ID", "acme")).From("t").Insert(t.Context(), map[string]int{"a": 1})
	require.NoError(t, err)
	assert.Equal(t, "acme", rec.last(t).header.Get("X-Tenant-ID"))
	assert.Equal(t, "table=t", rec.last(t).rawQuery)
}

// An ingest is at-least-once, so a reset connection is retried with the same body.
func TestInsertRetriesTransportErrors(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var bodies []string
		c := fakeClient(t, func(r *http.Request) (*http.Response, error) {
			b, _ := io.ReadAll(r.Body)
			bodies = append(bodies, string(b))
			if len(bodies) == 1 {
				return nil, errReset
			}
			return fakeResponse(r, 200, `{"total":1,"succeeded":1,"duplicates":0}`), nil
		})
		res, err := c.From("t").Insert(t.Context(), map[string]int{"a": 1})
		require.NoError(t, err)
		assert.Equal(t, 1, res.Succeeded)
		assert.Equal(t, []string{`{"a":1}`, `{"a":1}`}, bodies)
	})
}

func TestInsertRefusesReadState(t *testing.T) {
	rec := newRecorder(t, reply(200, `{"total":1,"succeeded":1}`))
	c := rec.client(t)
	for name, q := range map[string]*Query{
		"select":     c.From("t").Select("a"),
		"select all": c.From("t").SelectAll(),
		"where":      c.From("t").Where("a", Eq, 1),
		"order":      c.From("t").OrderBy("a", Asc),
		"group":      c.From("t").GroupBy("a"),
		"limit":      c.From("t").Limit(5),
		"time range": c.From("t").Last("ts", time.Hour),
		"aggregate":  c.From("t").Count("*", ""),
	} {
		_, err := q.Insert(t.Context(), map[string]any{"a": 1})
		require.ErrorContains(t, err, "read state", name)
		_, err = q.InsertNDJSON(t.Context(), strings.NewReader(`{"a":1}`))
		require.ErrorContains(t, err, "read state", name)
	}
	assert.Empty(t, rec.requests())

	// An empty Select() is no state.
	_, err := c.From("t").Select().Insert(t.Context(), map[string]any{"a": 1})
	require.NoError(t, err)
}
