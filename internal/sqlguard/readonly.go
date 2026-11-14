//go:build duckdb_arrow

package sqlguard

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// CheckReadOnly decides whether query may run in a read-only session. It
// returns nil when it may, a [*Violation] when the policy refuses it, and a
// [*ParseError] when DuckDB cannot parse it at all.
//
// The policy, in the order it is applied:
//
//  1. Statement type. Only what DuckDB parses as a SELECT node is allowed:
//     SELECT, WITH … SELECT, VALUES, FROM-first `FROM tbl`, set operations,
//     DESCRIBE, SHOW, SUMMARIZE (DuckDB rewrites all three to a SELECT over a
//     SHOW_REF). json_serialize_sql refuses every other statement type, which
//     is what makes this an allowlist decided by the parser rather than by
//     keyword matching: `WITH x AS (…) INSERT …` is an INSERT to DuckDB and is
//     refused. A script passes only if every statement in it does.
//     A plain `EXPLAIN <select>` is allowed as a single statement; EXPLAIN
//     ANALYZE is not, because it executes the statement it explains.
//
//  2. Table functions, by allowlist ([allowedTableFunctions]). A SELECT can
//     read files or URLs through read_csv/read_text/read_parquet/glob/…, and
//     run SQL hidden in a string literal through query()/query_table() — a
//     string this check cannot see into. The allowlist keeps the generators
//     and catalog introspection an analyst needs and refuses the rest.
//
//  3. Scalar functions, by denylist ([deniedFunctions]): the few that read
//     the process environment or mutate state from inside a SELECT.
//
//  4. Replacement scans. `FROM 's3://bucket/x.parquet'` or `FROM 'x.csv'` is a
//     table reference DuckDB turns into a file read at bind time, so a base
//     table whose name looks like a path or URL is refused.
//
// This is the parser half of the read-only boundary. The engine half — a
// separate DuckDB instance with external access off, the lake attached
// READ_ONLY and configuration locked — is what still holds if something here
// is missed; see the README's security section.
func (g *Guard) CheckReadOnly(ctx context.Context, query string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed {
		return errClosed
	}

	body, explain, err := stripExplain(query)
	if err != nil {
		return err
	}
	res, err := g.serialize(ctx, body)
	if err != nil {
		return err
	}
	if res.Error {
		if res.ErrorType == "parser" {
			return &ParseError{Msg: res.ErrorMessage}
		}
		// json_serialize_sql's "Only SELECT statements can be serialized"
		// is the statement-type refusal. Other error types are refused too
		// (fail closed) but keep DuckDB's wording.
		if strings.Contains(res.ErrorMessage, "Only SELECT statements") {
			return &Violation{Reason: "only SELECT, EXPLAIN, DESCRIBE, SHOW and SUMMARIZE statements are allowed"}
		}
		return &Violation{Reason: res.ErrorMessage}
	}
	switch {
	case len(res.Statements) == 0:
		return &Violation{Reason: "empty query"}
	case explain && len(res.Statements) != 1:
		return &Violation{Reason: "EXPLAIN must be the only statement"}
	}
	for _, raw := range res.Statements {
		var node any
		if err := json.Unmarshal(raw, &node); err != nil {
			return fmt.Errorf("sqlguard: decode statement: %w", err)
		}
		if err := checkNode(node); err != nil {
			return err
		}
	}
	return nil
}

// allowedTableFunctions are the table functions a read-only session may call
// in a FROM clause. Everything else is refused, including every function that
// reads a path or URL (read_*, glob, parquet_*, sniff_csv, iceberg_scan, …)
// and the two that execute SQL from a string (query, query_table).
//
// iceberg_snapshots/iceberg_metadata take a table *or* a path; a path is
// still confined by the engine's allowed_directories.
var allowedTableFunctions = map[string]bool{
	"range":               true,
	"generate_series":     true,
	"unnest":              true,
	"repeat":              true,
	"json_each":           true,
	"json_tree":           true,
	"duckdb_columns":      true,
	"duckdb_constraints":  true,
	"duckdb_databases":    true,
	"duckdb_dependencies": true,
	"duckdb_functions":    true,
	"duckdb_indexes":      true,
	"duckdb_keywords":     true,
	"duckdb_schemas":      true,
	"duckdb_sequences":    true,
	"duckdb_tables":       true,
	"duckdb_types":        true,
	"duckdb_views":        true,
	"pragma_table_info":   true,
	"pragma_show":         true,
	"pragma_version":      true,
	"iceberg_snapshots":   true,
	"iceberg_metadata":    true,
}

// deniedFunctions are scalar functions refused anywhere in a read-only
// statement. getenv exists only in the DuckDB CLI today, but it is the
// canonical credential leak, so it is named here regardless of build.
var deniedFunctions = map[string]string{
	"getenv":       "reads the server's environment",
	"which_secret": "reveals secret configuration",
	"nextval":      "mutates a sequence",
	"currval":      "reads sequence state",
	"setseed":      "mutates connection state",
}

// checkNode walks one serialized statement. The AST is walked generically
// rather than through typed structs: an unfamiliar node shape is still
// descended into, so a table function nested somewhere this code did not
// anticipate (a CTE, a lateral join, a subquery in a projection) is still
// found.
func checkNode(n any) error {
	switch v := n.(type) {
	case []any:
		for _, c := range v {
			if err := checkNode(c); err != nil {
				return err
			}
		}
	case map[string]any:
		if typ, _ := v["type"].(string); typ != "" {
			switch typ {
			case "TABLE_FUNCTION":
				fn, _ := v["function"].(map[string]any)
				name, _ := fn["function_name"].(string)
				if !allowedTableFunctions[strings.ToLower(name)] {
					return &Violation{Reason: fmt.Sprintf("table function %q is not allowed", name)}
				}
			case "BASE_TABLE":
				for _, k := range []string{"table_name", "schema_name", "catalog_name"} {
					if s, _ := v[k].(string); looksLikePath(s) {
						return &Violation{Reason: "reading files or URLs through a table reference is not allowed"}
					}
				}
			}
		}
		if name, ok := v["function_name"].(string); ok {
			if why, denied := deniedFunctions[strings.ToLower(name)]; denied {
				return &Violation{Reason: fmt.Sprintf("function %q is not allowed (%s)", name, why)}
			}
		}
		for _, c := range v {
			if err := checkNode(c); err != nil {
				return err
			}
		}
	}
	return nil
}

// looksLikePath reports whether a table name would be taken by DuckDB as a
// file or URL for a replacement scan. Quoted identifiers containing these
// characters are refused too; that costs nothing real.
func looksLikePath(s string) bool {
	return strings.ContainsAny(s, `/\:.*?`)
}

// stripExplain recognizes a leading EXPLAIN and returns the statement it
// explains. The text is only ever narrowed here, never widened: what is
// returned must still pass the parser-based check on its own, so a mistake in
// this lexing can only refuse a statement, not admit one.
func stripExplain(query string) (body string, explain bool, err error) {
	rest := skipSpaceAndComments(query)
	kw, after := leadingWord(rest)
	if !strings.EqualFold(kw, "EXPLAIN") {
		return query, false, nil
	}
	after = skipSpaceAndComments(after)
	next, _ := leadingWord(after)
	switch {
	case strings.EqualFold(next, "ANALYZE"), strings.EqualFold(next, "ANALYSE"): //nolint:misspell // DuckDB accepts the British spelling
		return "", true, &Violation{Reason: "EXPLAIN ANALYZE executes the statement and is not allowed"}
	case strings.HasPrefix(after, "("):
		return "", true, &Violation{Reason: "EXPLAIN options are not allowed"}
	}
	return after, true, nil
}

func leadingWord(s string) (word, rest string) {
	i := 0
	for i < len(s) && (s[i] == '_' || s[i] >= 'a' && s[i] <= 'z' || s[i] >= 'A' && s[i] <= 'Z') {
		i++
	}
	return s[:i], s[i:]
}

// skipSpaceAndComments drops leading whitespace, `--` line comments and
// (nested) `/* */` block comments, the way DuckDB's Postgres-derived scanner
// treats them.
func skipSpaceAndComments(s string) string {
	for {
		s = strings.TrimLeft(s, " \t\r\n\f\v")
		switch {
		case strings.HasPrefix(s, "--"):
			if i := strings.IndexByte(s, '\n'); i >= 0 {
				s = s[i+1:]
			} else {
				return ""
			}
		case strings.HasPrefix(s, "/*"):
			depth, i := 0, 0
			for i < len(s) {
				switch {
				case strings.HasPrefix(s[i:], "/*"):
					depth++
					i += 2
				case strings.HasPrefix(s[i:], "*/"):
					depth--
					i += 2
				default:
					i++
				}
				if depth == 0 {
					break
				}
			}
			if depth != 0 {
				return s // unterminated: leave it for the parser to reject
			}
			s = s[i:]
		default:
			return s
		}
	}
}
