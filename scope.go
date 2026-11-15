package diagnose

import (
	"fmt"
	"strings"
)

// Scope limits table-oriented checks at the ClickHouse server. Table may only
// be used together with Database. Values are always bound query parameters and
// are never interpolated into SQL.
type Scope struct {
	Database string `json:"database,omitempty"`
	Table    string `json:"table,omitempty"`
}

func validateScope(scope Scope) error {
	if scope.Table != "" && scope.Database == "" {
		return inputError("table scope requires database")
	}
	if len(scope.Database) > 256 || len(scope.Table) > 256 {
		return inputError("scope field exceeds 256 bytes")
	}
	return nil
}

// scopedQuery inserts one of two fixed predicates before anchor. The caller
// supplies only package-owned SQL and anchors; user values remain parameters.
func scopedQuery(query, anchor string, hasWhere bool, scope Scope) (string, []any, error) {
	if err := validateScope(scope); err != nil {
		return "", nil, err
	}
	if scope.Database == "" {
		return query, nil, nil
	}
	idx := strings.Index(query, anchor)
	if idx < 0 {
		return "", nil, fmt.Errorf("diagnose: scoped query anchor %q missing", anchor)
	}
	joiner := "WHERE"
	if hasWhere {
		joiner = "AND"
	}
	predicate := joiner + " database = ?\n"
	args := []any{scope.Database}
	if scope.Table != "" {
		predicate = joiner + " database = ? AND table = ?\n"
		args = append(args, scope.Table)
	}
	return query[:idx] + predicate + query[idx:], args, nil
}
