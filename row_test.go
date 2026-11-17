package wavehouse

import (
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRowAccessors(t *testing.T) {
	rec := newRecorder(t, reply(200, `[{
		"u":18446744073709551615,"i":-9223372036854775808,"f":1.5,"dec":"12345678901234567.0123456789",
		"qi":"42","s":"hi","b":true,"ts":"2026-01-02T03:04:05.123456789Z","ch":"2026-01-02 03:04:05.5","d":"2026-01-02","n":null}]`))
	rows, err := rec.client(t).From("t").Fetch(t.Context())
	require.NoError(t, err)
	r := rows[0]

	u, err := r.Uint64("u")
	require.NoError(t, err)
	assert.Equal(t, uint64(math.MaxUint64), u)
	i, err := r.Int64("i")
	require.NoError(t, err)
	assert.Equal(t, int64(math.MinInt64), i)
	qi, err := r.Int64("qi")
	require.NoError(t, err)
	assert.Equal(t, int64(42), qi)
	f, err := r.Float64("f")
	require.NoError(t, err)
	assert.InDelta(t, 1.5, f, 0)
	dec, err := r.String("dec")
	require.NoError(t, err)
	assert.Equal(t, "12345678901234567.0123456789", dec)
	df, err := r.Float64("dec")
	require.NoError(t, err)
	assert.InDelta(t, 1.2345678901234567e16, df, 4)
	num, err := r.String("u")
	require.NoError(t, err)
	assert.Equal(t, "18446744073709551615", num)
	b, err := r.Bool("b")
	require.NoError(t, err)
	assert.True(t, b)
	ts, err := r.Time("ts")
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 1, 2, 3, 4, 5, 123456789, time.UTC), ts.UTC())
	ch, err := r.Time("ch")
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 1, 2, 3, 4, 5, 500_000_000, time.UTC), ch)
	d, err := r.Time("d")
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), d)
}

func TestRowAccessorErrors(t *testing.T) {
	r := Row{
		"n": nil, "s": "abc", "frac": json.Number("1.5"), "neg": json.Number("-1"), "big": json.Number("18446744073709551615"),
		"b": true, "u": uint64(math.MaxUint64), "fl": 1.5,
	}
	_, err := r.Int64("missing")
	require.ErrorContains(t, err, `no column "missing"`)
	for name, call := range map[string]func() error{
		"String":  func() error { _, err := r.String("n"); return err },
		"Int64":   func() error { _, err := r.Int64("n"); return err },
		"Uint64":  func() error { _, err := r.Uint64("n"); return err },
		"Float64": func() error { _, err := r.Float64("n"); return err },
		"Bool":    func() error { _, err := r.Bool("n"); return err },
		"Time":    func() error { _, err := r.Time("n"); return err },
	} {
		require.ErrorContains(t, call(), `column "n" is null`, name)
	}
	for name, call := range map[string]func() error{
		"string as int":     func() error { _, err := r.Int64("s"); return err },
		"fraction as int":   func() error { _, err := r.Int64("frac"); return err },
		"overflow as int":   func() error { _, err := r.Int64("big"); return err },
		"uint overflow int": func() error { _, err := r.Int64("u"); return err },
		"float as int":      func() error { _, err := r.Int64("fl"); return err },
		"negative as uint":  func() error { _, err := r.Uint64("neg"); return err },
		"fraction as uint":  func() error { _, err := r.Uint64("frac"); return err },
		"bool as uint":      func() error { _, err := r.Uint64("b"); return err },
		"string as float":   func() error { _, err := r.Float64("s"); return err },
		"bool as float":     func() error { _, err := r.Float64("b"); return err },
		"string as bool":    func() error { _, err := r.Bool("s"); return err },
		"bool as string":    func() error { _, err := r.String("b"); return err },
		"garbage as time":   func() error { _, err := r.Time("s"); return err },
		"number as time":    func() error { _, err := r.Time("frac"); return err },
		"bool as int":       func() error { _, err := r.Int64("b"); return err },
	} {
		require.ErrorContains(t, call(), "can't read", name)
	}
}

// Rows built by hand, not decoded, hold Go numbers; the accessors take them too.
func TestRowAccessorsGoValues(t *testing.T) {
	now := time.Now()
	r := Row{"i": 7, "i64": int64(-7), "u": uint64(9), "f": 2.0, "t": now, "neg": -1}
	for col, want := range map[string]int64{"i": 7, "i64": -7, "u": 9, "f": 2} {
		got, err := r.Int64(col)
		require.NoError(t, err, col)
		assert.Equal(t, want, got, col)
	}
	for col, want := range map[string]uint64{"i": 7, "i64": 0, "u": 9, "f": 2} {
		got, err := r.Uint64(col)
		if col == "i64" {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err, col)
		assert.Equal(t, want, got, col)
	}
	_, err := r.Uint64("neg")
	require.Error(t, err)
	for _, col := range []string{"i", "i64", "u", "f"} {
		_, err := r.Float64(col)
		require.NoError(t, err, col)
	}
	got, err := r.Time("t")
	require.NoError(t, err)
	assert.True(t, now.Equal(got))
}

// A row as a server that renders with ClickHouse (Wave-RF/WaveHouse#882)
// sends it, its bytes as measured: a Decimal as a number, a NaN or infinity
// as a string, a Date bare and a DateTime64(3) at its scale.
func TestRowAccessorsClickHouseRendered(t *testing.T) {
	rec := newRecorder(t, reply(200, `[{"price":12345678901234567.0123456789,"nan":"nan","inf":"inf","ninf":"-inf",`+
		`"d":"2026-06-21","whole":"2026-06-21T04:00:00.000Z","frac":"2026-06-21T04:00:00.120Z"}]`))
	rows, err := rec.client(t).From("t").Fetch(t.Context())
	require.NoError(t, err)
	r := rows[0]

	dec, err := r.String("price")
	require.NoError(t, err)
	assert.Equal(t, "12345678901234567.0123456789", dec, "every digit, as the 0.2.x string kept them")
	df, err := r.Float64("price")
	require.NoError(t, err)
	assert.InDelta(t, 1.2345678901234567e16, df, 4)

	for col, want := range map[string]float64{"inf": math.Inf(1), "ninf": math.Inf(-1)} {
		f, err := r.Float64(col)
		require.NoError(t, err, col)
		assert.Equal(t, want, f, col)
	}
	nan, err := r.Float64("nan")
	require.NoError(t, err)
	assert.True(t, math.IsNaN(nan))

	d, err := r.Time("d")
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 6, 21, 0, 0, 0, 0, time.UTC), d)
	whole, err := r.Time("whole")
	require.NoError(t, err)
	frac, err := r.Time("frac")
	require.NoError(t, err)
	// As strings ".000Z" < ".120Z" but "00Z" > ".120Z"; as times the scale doesn't matter.
	trimmed, err := Row{"t": "2026-06-21T04:00:00Z"}.Time("t")
	require.NoError(t, err)
	assert.True(t, whole.Equal(trimmed), "0.2.x's trimmed spelling is the same instant")
	assert.True(t, trimmed.Before(frac))
	assert.Greater(t, "2026-06-21T04:00:00Z", "2026-06-21T04:00:00.120Z", "which string order gets wrong")
}

func TestFloatUnmarshal(t *testing.T) {
	for in, want := range map[string]float64{
		`1.5`: 1.5, `-2`: -2, `1e3`: 1000, `"1.5"`: 1.5,
		`"inf"`: math.Inf(1), `"-inf"`: math.Inf(-1), `"+inf"`: math.Inf(1),
	} {
		var f Float
		require.NoError(t, json.Unmarshal([]byte(in), &f), in)
		assert.Equal(t, Float(want), f, in)
	}
	var nan Float
	require.NoError(t, json.Unmarshal([]byte(`"nan"`), &nan))
	assert.True(t, math.IsNaN(float64(nan)))

	kept := Float(7)
	require.NoError(t, json.Unmarshal([]byte(`null`), &kept))
	assert.Equal(t, Float(7), kept, "null leaves it unchanged")
	var p *Float
	require.NoError(t, json.Unmarshal([]byte(`null`), &p))
	assert.Nil(t, p, "a *Float tells a NULL apart")

	for _, in := range []string{`"abc"`, `true`, `{}`, `""`, `"1e400"`} {
		var f Float
		require.ErrorContains(t, json.Unmarshal([]byte(in), &f), "wavehouse: Float", in)
	}
	// encoding/json passes only valid JSON; a direct call may not.
	require.ErrorContains(t, new(Float).UnmarshalJSON([]byte(`"nan`)), "wavehouse: Float")
}

func TestFloatMarshal(t *testing.T) {
	for want, f := range map[string]float64{
		`"nan"`: math.NaN(), `"inf"`: math.Inf(1), `"-inf"`: math.Inf(-1), `1.5`: 1.5, `0`: 0, `1e+21`: 1e21,
	} {
		got, err := json.Marshal(Float(f))
		require.NoError(t, err)
		assert.Equal(t, want, string(got))
		back := Float(0)
		require.NoError(t, json.Unmarshal(got, &back))
		if !math.IsNaN(f) {
			assert.Equal(t, Float(f), back, "round trip of %s", want)
		}
	}
}
