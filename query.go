package wavehouse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"slices"
	"strings"
	"time"
)

// Op is a filter operator, spelled as the server expects it.
type Op string

// The filter operators of [Query.Where].
const (
	Eq   Op = "eq"
	Neq  Op = "neq"
	Gt   Op = "gt"
	Gte  Op = "gte"
	Lt   Op = "lt"
	Lte  Op = "lte"
	In   Op = "in"   // value is a slice
	Like Op = "like" // SQL LIKE pattern
)

var allOps = []Op{Eq, Neq, Gt, Gte, Lt, Lte, In, Like}

// Dir is a sort direction.
type Dir string

// The sort directions of [Query.OrderBy].
const (
	Asc  Dir = "asc"
	Desc Dir = "desc"
)

// Query builds one structured query against a table, sent as
// POST /v1/query?table=. It is immutable: every method returns a modified
// copy and leaves its receiver alone, so a partly built query can be shared by
// goroutines and extended in several directions. Get one from [Client.From].
//
// A misuse of the builder (an empty column name, SelectAll together with
// columns or aggregates, a negative limit) does not panic; the first one is
// returned by Scan or Fetch, before anything is sent.
type Query struct {
	c         *Client
	table     string
	columns   []string
	selectAll bool
	aggs      []aggregation
	filters   []filter
	groupBy   []string
	orderBy   []orderClause
	limit     int
	timeRange *timeRange
	header    http.Header
	err       error
}

type aggregation struct {
	Fn     string `json:"fn"`
	Column string `json:"column"`
	Alias  string `json:"alias,omitempty"`
}

type filter struct {
	Column string `json:"column"`
	Op     Op     `json:"op"`
	Value  any    `json:"value"`
}

type orderClause struct {
	Column string `json:"column"`
	Dir    Dir    `json:"dir"`
}

type timeRange struct {
	Column string `json:"column"`
	Since  string `json:"since"`
	Until  string `json:"until,omitempty"`
}

// queryBody is the wire form of a Query.
type queryBody struct {
	Columns      []string      `json:"columns,omitempty"`
	SelectAll    bool          `json:"select_all,omitempty"`
	Aggregations []aggregation `json:"aggregations,omitempty"`
	Filters      []filter      `json:"filters,omitempty"`
	GroupBy      []string      `json:"group_by,omitempty"`
	OrderBy      []orderClause `json:"order_by,omitempty"`
	Limit        int           `json:"limit,omitempty"`
	TimeRange    *timeRange    `json:"time_range,omitempty"`
}

// with returns a copy of q, with its slices copied, after applying f.
func (q *Query) with(f func(*Query)) *Query {
	cp := *q
	cp.columns = slices.Clone(q.columns)
	cp.aggs = slices.Clone(q.aggs)
	cp.filters = slices.Clone(q.filters)
	cp.groupBy = slices.Clone(q.groupBy)
	cp.orderBy = slices.Clone(q.orderBy)
	cp.header = q.header.Clone()
	f(&cp)
	return &cp
}

// fail records the first misuse.
func (q *Query) fail(format string, args ...any) {
	if q.err == nil {
		q.err = fmt.Errorf(format, args...)
	}
}

// checkNames records an error for an empty name.
func (q *Query) checkNames(call string, names ...string) {
	if slices.Contains(names, "") {
		q.fail("%s: empty column name", call)
	}
}

// Header sets a header on this query's request, over the client's
// [WithHeader] and [WithHeaderFunc] headers and under the SDK's own
// (Authorization, Content-Type, Accept, User-Agent). A later Header for the
// same key, in any letter case, replaces the earlier one; values are never
// joined. It applies to Scan, Fetch and Insert, and an invalid name or value
// is an error from them, before anything is sent. It returns a copy.
func (q *Query) Header(key, value string) *Query {
	return q.with(func(q *Query) {
		if err := checkHeader(key, value); err != nil {
			q.fail("Header: %v", err)
			return
		}
		if q.header == nil {
			q.header = http.Header{}
		}
		q.header.Set(key, value)
	})
}

// Select adds columns to the projection; calls accumulate. With no Select,
// SelectAll or aggregate, the query selects every column. "*" is a column
// name, not a wildcard; use [Query.SelectAll] for every readable column.
func (q *Query) Select(columns ...string) *Query {
	return q.with(func(q *Query) {
		q.checkNames("Select", columns...)
		q.columns = append(q.columns, columns...)
	})
}

// SelectAll selects every column the caller's role may read (select_all). It
// can't be combined with [Query.Select] columns or aggregates; Scan and Fetch
// report that. It is also the default when nothing else is selected.
func (q *Query) SelectAll() *Query {
	return q.with(func(q *Query) { q.selectAll = true })
}

// Where adds a filter. Filters accumulate and are ANDed. op must be one of the
// eight [Op] constants, and for [In] value must be a non-empty slice; Scan and
// Fetch report anything else. Values are JSON-encoded, so a time.Time is
// RFC 3339.
func (q *Query) Where(column string, op Op, value any) *Query {
	return q.with(func(q *Query) {
		q.checkNames("Where", column)
		if !slices.Contains(allOps, op) {
			q.fail("Where: unknown operator %q on column %q; use one of the Op constants (Eq, Neq, Gt, Gte, Lt, Lte, In, Like)", op, column)
		} else if op == In {
			if v := reflect.ValueOf(value); v.Kind() != reflect.Slice || v.Len() == 0 {
				q.fail("Where: In on column %q needs a non-empty slice, got %T", column, value)
			}
		}
		q.filters = append(q.filters, filter{Column: column, Op: op, Value: value})
	})
}

// OrderBy adds a sort key. column may be a table column or an aggregate's
// alias. An empty dir sorts ascending; "DESC" and "Asc" are accepted and
// normalised, and any other dir is an error from Scan or Fetch.
func (q *Query) OrderBy(column string, dir Dir) *Query {
	return q.with(func(q *Query) {
		q.checkNames("OrderBy", column)
		switch d := Dir(strings.ToLower(string(dir))); d {
		case "":
			dir = Asc
		case Asc, Desc:
			dir = d
		default:
			q.fail("OrderBy: unknown direction %q on column %q; use Asc or Desc", dir, column)
		}
		q.orderBy = append(q.orderBy, orderClause{Column: column, Dir: dir})
	})
}

// GroupBy adds GROUP BY columns; calls accumulate. It sends group_by and
// nothing else: the server returns only the selected columns and aggregates,
// so a caller who wants the key back selects it too, as in
// Select("page").GroupBy("page"). How the server should treat grouping keys
// is tracked in https://github.com/Wave-RF/WaveHouse/issues/295.
func (q *Query) GroupBy(columns ...string) *Query {
	return q.with(func(q *Query) {
		q.checkNames("GroupBy", columns...)
		q.groupBy = append(q.groupBy, columns...)
	})
}

// Limit caps the rows returned. 0, the default, sends no limit and leaves the
// cap to the server, which applies its own maximum (10,000 rows by default)
// silently; a negative n is an error from Scan or Fetch.
func (q *Query) Limit(n int) *Query {
	return q.with(func(q *Query) {
		if n < 0 {
			q.fail("Limit: negative limit %d", n)
		}
		q.limit = n
	})
}

// TimeRange restricts column to the window from, until to, in UTC with
// nanosecond precision. A zero to leaves the window open at the end, meaning
// now, and no until is sent; a zero from is an error from Scan or Fetch (use
// [Query.Last] for a window ending now). A later TimeRange or Last replaces
// this one.
func (q *Query) TimeRange(column string, from, to time.Time) *Query {
	return q.with(func(q *Query) {
		q.checkNames("TimeRange", column)
		if from.IsZero() {
			q.fail("TimeRange: zero from time; use Last for a window ending now")
		}
		tr := &timeRange{Column: column, Since: from.UTC().Format(time.RFC3339Nano)}
		if !to.IsZero() {
			tr.Until = to.UTC().Format(time.RFC3339Nano)
		}
		q.timeRange = tr
	})
}

// Last restricts column to the last d, up to now on the server's clock, so a
// client with a skewed clock still gets the right window. d must be positive.
// It is sent as the server's relative form (d as a Go duration such as
// "1h30m0s", meaning that long ago). A later Last or TimeRange replaces it.
func (q *Query) Last(column string, d time.Duration) *Query {
	return q.with(func(q *Query) {
		q.checkNames("Last", column)
		if d <= 0 {
			q.fail("Last: duration %v must be positive", d)
		}
		q.timeRange = &timeRange{Column: column, Since: d.String()}
	})
}

// Count adds count(column) AS alias; column "*" counts rows. An empty alias is
// "count", whatever the column.
func (q *Query) Count(column, alias string) *Query {
	return q.Aggregate("count", column, orDefault(alias, "count"))
}

// Sum adds sum(column) AS alias; an empty alias is "sum_<column>".
func (q *Query) Sum(column, alias string) *Query {
	return q.Aggregate("sum", column, orDefault(alias, "sum_"+column))
}

// Avg adds avg(column) AS alias; an empty alias is "avg_<column>".
func (q *Query) Avg(column, alias string) *Query {
	return q.Aggregate("avg", column, orDefault(alias, "avg_"+column))
}

// Min adds min(column) AS alias; an empty alias is "min_<column>".
func (q *Query) Min(column, alias string) *Query {
	return q.Aggregate("min", column, orDefault(alias, "min_"+column))
}

// Max adds max(column) AS alias; an empty alias is "max_<column>".
func (q *Query) Max(column, alias string) *Query {
	return q.Aggregate("max", column, orDefault(alias, "max_"+column))
}

// CountDistinct adds countDistinct(column) AS alias; an empty alias is
// "count_distinct_<column>".
func (q *Query) CountDistinct(column, alias string) *Query {
	return q.Aggregate("countDistinct", column, orDefault(alias, "count_distinct_"+column))
}

func orDefault(alias, def string) string {
	if alias == "" {
		return def
	}
	return alias
}

// Aggregate adds fn(column) AS alias for any aggregate function the server
// allows, such as "uniqExact". An empty alias sends none, and the server names
// the result column.
func (q *Query) Aggregate(fn, column, alias string) *Query {
	return q.with(func(q *Query) {
		if fn == "" {
			q.fail("Aggregate: empty function name")
		}
		q.checkNames("Aggregate", column)
		q.aggs = append(q.aggs, aggregation{Fn: fn, Column: column, Alias: alias})
	})
}

// hasReadState reports whether q carries anything a read would send, which an
// insert can't use.
func (q *Query) hasReadState() bool {
	return len(q.columns) > 0 || q.selectAll || len(q.aggs) > 0 || len(q.filters) > 0 ||
		len(q.groupBy) > 0 || len(q.orderBy) > 0 || q.limit != 0 || q.timeRange != nil
}

// body validates q and returns its wire form.
func (q *Query) body() (*queryBody, error) {
	if q.err != nil {
		return nil, fmt.Errorf("wavehouse: query on %q: %w", q.table, q.err)
	}
	columns, all := q.columns, q.selectAll
	if all && (len(columns) > 0 || len(q.aggs) > 0) {
		return nil, fmt.Errorf("wavehouse: query on %q: SelectAll can't be combined with Select columns or aggregates", q.table)
	}
	return &queryBody{
		Columns:      columns,
		SelectAll:    all || (len(columns) == 0 && len(q.aggs) == 0),
		Aggregations: q.aggs,
		Filters:      q.filters,
		GroupBy:      q.groupBy,
		OrderBy:      q.orderBy,
		Limit:        q.limit,
		TimeRange:    q.timeRange,
	}, nil
}

func (q *Query) request() (*request, error) {
	if q.table == "" {
		return nil, errors.New("wavehouse: query: empty table name")
	}
	b, err := q.body()
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(b)
	if err != nil {
		return nil, fmt.Errorf("wavehouse: query on %q: encode filter values: %w", q.table, err)
	}
	return &request{
		kind: callRead, method: http.MethodPost, path: "/v1/query",
		query: url.Values{"table": {q.table}}, body: raw, contentType: "application/json",
		callHeader: q.header,
	}, nil
}

// Scan runs the query and decodes the rows into dst, a non-nil pointer to a
// slice of structs (matched by `json` tag, else case-insensitively by field
// name) or of maps. The server renders UInt64 and Int64 as JSON numbers, so
// an int64 or uint64 field is exact, and a Decimal as a JSON string, so give
// it a string field (or the `json:",string"` tag for a numeric one). Into a
// map or any, numbers are [encoding/json.Number].
//
// Scan is a read: a transport error or a retryable [*APIError] is retried. A
// result over the server's row cap is silently truncated, so set [Query.Limit].
func (q *Query) Scan(ctx context.Context, dst any) error {
	if err := checkScanDst(dst); err != nil {
		return err
	}
	r, err := q.request()
	if err != nil {
		return err
	}
	return q.c.doJSON(ctx, r, dst)
}

// Fetch runs the query and returns the rows as [Row]s, whose accessors read
// the typed values. Use [Query.Scan] to decode into structs.
func (q *Query) Fetch(ctx context.Context) ([]Row, error) {
	var rows []Row
	if err := q.Scan(ctx, &rows); err != nil {
		return nil, err
	}
	return rows, nil
}

// checkScanDst reports a dst that Scan can't fill, before any request is made.
func checkScanDst(dst any) error {
	v := reflect.ValueOf(dst)
	if v.Kind() != reflect.Pointer || v.IsNil() || v.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("wavehouse: Scan: dst must be a non-nil pointer to a slice of structs or maps, got %T", dst)
	}
	elem := v.Elem().Type().Elem()
	for elem.Kind() == reflect.Pointer {
		elem = elem.Elem()
	}
	if k := elem.Kind(); k == reflect.Struct || k == reflect.Map || k == reflect.Interface {
		return nil
	}
	return fmt.Errorf("wavehouse: Scan: dst's elements must be structs or maps, got %s", v.Elem().Type().Elem())
}
