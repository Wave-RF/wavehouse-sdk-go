package wavehouse

// Row is one result row with its columns by name. Numbers are
// [encoding/json.Number], so 64-bit integers stay exact; a Decimal column is
// a string, as the server renders it.
type Row = map[string]any
