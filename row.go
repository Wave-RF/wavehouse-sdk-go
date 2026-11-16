package wavehouse

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"
)

// Row is one result row, its columns by name. Values come off the wire with
// these shapes:
//
//   - A number is an [encoding/json.Number], never a float64, so a UInt64 or
//     Int64 past 2^53 stays exact. Read it with [Row.Int64], [Row.Uint64] or
//     [Row.Float64], or type-assert it and call its methods.
//   - A Decimal, and a 64-bit integer the server chose to quote, is a string;
//     the numeric accessors parse it.
//   - A Date, DateTime or DateTime64 is a string. [Row.Time] parses it;
//     RFC 3339 (with fractional seconds) is what the server sends.
//   - NULL is a nil value; the accessors report it as an error that names the
//     column, so a missing value is never read as zero.
//
// The accessors return an error naming the column when it is absent, null or
// of a shape that can't hold the wanted type. Indexing the map directly stays
// available for anything else.
type Row map[string]any

// get returns the column's value, or an error for a missing or null column.
func (r Row) get(col string) (any, error) {
	v, ok := r[col]
	if !ok {
		return nil, fmt.Errorf("wavehouse: row has no column %q", col)
	}
	if v == nil {
		return nil, fmt.Errorf("wavehouse: row column %q is null", col)
	}
	return v, nil
}

func badShape(col string, v any, want string) error {
	return fmt.Errorf("wavehouse: row column %q: can't read %v (%T) as %s", col, v, v, want)
}

// String returns a string column, or a number's decimal text.
func (r Row) String(col string) (string, error) {
	v, err := r.get(col)
	if err != nil {
		return "", err
	}
	switch x := v.(type) {
	case string:
		return x, nil
	case json.Number:
		return x.String(), nil
	}
	return "", badShape(col, v, "a string")
}

// Int64 returns an integer column. A string is parsed, so a quoted Int64
// works; a number with a fraction or beyond int64 is an error.
func (r Row) Int64(col string) (int64, error) {
	v, err := r.get(col)
	if err != nil {
		return 0, err
	}
	var n int64
	switch x := v.(type) {
	case json.Number:
		n, err = x.Int64()
	case string:
		n, err = strconv.ParseInt(x, 10, 64)
	case int:
		n = int64(x)
	case int64:
		n = x
	case uint64:
		if x > math.MaxInt64 {
			return 0, badShape(col, v, "int64")
		}
		n = int64(x)
	case float64:
		if x != math.Trunc(x) || x < -(1<<63) || x >= 1<<63 {
			return 0, badShape(col, v, "int64")
		}
		n = int64(x)
	default:
		return 0, badShape(col, v, "int64")
	}
	if err != nil {
		return 0, badShape(col, v, "int64")
	}
	return n, nil
}

// Uint64 returns an unsigned integer column; see [Row.Int64].
func (r Row) Uint64(col string) (uint64, error) {
	v, err := r.get(col)
	if err != nil {
		return 0, err
	}
	var n uint64
	switch x := v.(type) {
	case json.Number:
		n, err = strconv.ParseUint(x.String(), 10, 64)
	case string:
		n, err = strconv.ParseUint(x, 10, 64)
	case int:
		if x < 0 {
			return 0, badShape(col, v, "uint64")
		}
		n = uint64(x)
	case int64:
		if x < 0 {
			return 0, badShape(col, v, "uint64")
		}
		n = uint64(x)
	case uint64:
		n = x
	case float64:
		if x != math.Trunc(x) || x < 0 || x >= 1<<64 {
			return 0, badShape(col, v, "uint64")
		}
		n = uint64(x)
	default:
		return 0, badShape(col, v, "uint64")
	}
	if err != nil {
		return 0, badShape(col, v, "uint64")
	}
	return n, nil
}

// Float64 returns a floating-point column. A string is parsed, which is how a
// Decimal is read; converting a Decimal to float64 can lose digits, so keep
// it as a string ([Row.String]) when it must stay exact.
func (r Row) Float64(col string) (float64, error) {
	v, err := r.get(col)
	if err != nil {
		return 0, err
	}
	var f float64
	switch x := v.(type) {
	case json.Number:
		f, err = x.Float64()
	case string:
		f, err = strconv.ParseFloat(x, 64)
	case float64:
		f = x
	case int:
		f = float64(x)
	case int64:
		f = float64(x)
	case uint64:
		f = float64(x)
	default:
		return 0, badShape(col, v, "float64")
	}
	if err != nil {
		return 0, badShape(col, v, "float64")
	}
	return f, nil
}

// Bool returns a Bool column.
func (r Row) Bool(col string) (bool, error) {
	v, err := r.get(col)
	if err != nil {
		return false, err
	}
	if b, ok := v.(bool); ok {
		return b, nil
	}
	return false, badShape(col, v, "bool")
}

// timeLayouts are the spellings of a ClickHouse time the server may send: RFC
// 3339 first, then ClickHouse's own text form and a bare Date, both UTC.
var timeLayouts = []string{time.RFC3339Nano, "2006-01-02 15:04:05.999999999", "2006-01-02"}

// Time returns a Date, DateTime or DateTime64 column, in the zone the server
// sent it (a form without a zone is UTC).
func (r Row) Time(col string) (time.Time, error) {
	v, err := r.get(col)
	if err != nil {
		return time.Time{}, err
	}
	switch x := v.(type) {
	case time.Time:
		return x, nil
	case string:
		for _, layout := range timeLayouts {
			if t, err := time.Parse(layout, x); err == nil {
				return t, nil
			}
		}
	}
	return time.Time{}, badShape(col, v, "a time")
}
