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
)

// Row is one result row with its columns by name. Numbers are
// [encoding/json.Number], so 64-bit integers stay exact; a Decimal column is
// a string, as the server renders it.
type Row = map[string]any

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
	err       error
}

type aggregation struct {
	Fn     string `json:"fn"`
	Column string `json:"column"`
	Alias  string `json:"alias"`
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

// Select adds columns to the projection; calls accumulate. With no Select,
// SelectAll or aggregate, the query selects every column.
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

// Where adds a filter. Filters accumulate and are ANDed. For [In], value is
// a slice; values are JSON-encoded, so a time.Time is RFC 3339.
func (q *Query) Where(column string, op Op, value any) *Query {
	return q.with(func(q *Query) {
		q.checkNames("Where", column)
		q.filters = append(q.filters, filter{Column: column, Op: op, Value: value})
	})
}

// OrderBy adds a sort key. column may be a table column or an aggregate's
// alias. An empty dir sorts ascending.
func (q *Query) OrderBy(column string, dir Dir) *Query {
	return q.with(func(q *Query) {
		q.checkNames("OrderBy", column)
		if dir == "" {
			dir = Asc
		}
		q.orderBy = append(q.orderBy, orderClause{Column: column, Dir: dir})
	})
}

// GroupBy adds GROUP BY columns; calls accumulate.
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

// TimeRange restricts column to a window. since and until are RFC 3339
// timestamps or relative durations meaning that long ago ("1h", "30m", "7d",
// "2w"); an empty until means now. The server applies the window only when
// since is set.
func (q *Query) TimeRange(column, since, until string) *Query {
	return q.with(func(q *Query) {
		q.checkNames("TimeRange", column)
		q.timeRange = &timeRange{Column: column, Since: since, Until: until}
	})
}

// Count adds count(column) AS alias; column "*" counts rows. An empty alias
// is "count".
func (q *Query) Count(column, alias string) *Query {
	return q.aggregate("count", "count", column, alias)
}

// Sum adds sum(column) AS alias; an empty alias is "sum_<column>".
func (q *Query) Sum(column, alias string) *Query {
	return q.aggregate("sum", "sum_"+column, column, alias)
}

// Avg adds avg(column) AS alias; an empty alias is "avg_<column>".
func (q *Query) Avg(column, alias string) *Query {
	return q.aggregate("avg", "avg_"+column, column, alias)
}

// Min adds min(column) AS alias; an empty alias is "min_<column>".
func (q *Query) Min(column, alias string) *Query {
	return q.aggregate("min", "min_"+column, column, alias)
}

// Max adds max(column) AS alias; an empty alias is "max_<column>".
func (q *Query) Max(column, alias string) *Query {
	return q.aggregate("max", "max_"+column, column, alias)
}

// CountDistinct adds countDistinct(column) AS alias; an empty alias is
// "count_distinct_<column>".
func (q *Query) CountDistinct(column, alias string) *Query {
	return q.aggregate("countDistinct", "count_distinct_"+column, column, alias)
}

// Aggregate adds fn(column) AS alias for any aggregate function the server
// allows, such as "uniqExact". An empty alias leaves the result column named
// by ClickHouse.
func (q *Query) Aggregate(fn, column, alias string) *Query {
	return q.with(func(q *Query) {
		if fn == "" {
			q.fail("Aggregate: empty function name")
		}
		q.checkNames("Aggregate", column)
		q.aggs = append(q.aggs, aggregation{Fn: fn, Column: column, Alias: alias})
	})
}

func (q *Query) aggregate(fn, defaultAlias, column, alias string) *Query {
	if alias == "" {
		alias = defaultAlias
	}
	return q.Aggregate(fn, column, alias)
}

// body validates q and returns its wire form.
func (q *Query) body() (*queryBody, error) {
	if q.err != nil {
		return nil, fmt.Errorf("wavehouse: query on %q: %w", q.table, q.err)
	}
	if q.selectAll && (len(q.columns) > 0 || len(q.aggs) > 0) {
		return nil, fmt.Errorf("wavehouse: query on %q: SelectAll can't be combined with Select columns or aggregates", q.table)
	}
	return &queryBody{
		Columns:      q.columns,
		SelectAll:    q.selectAll || (len(q.columns) == 0 && len(q.aggs) == 0),
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

// Fetch runs the query and returns the rows as maps; numbers are
// [encoding/json.Number]. Use [Query.Scan] for typed rows.
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
