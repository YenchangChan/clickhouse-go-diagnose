package diagnose

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestTableScopeIsBoundAndPushedIntoQuery(t *testing.T) {
	tgt, q := partsTarget("n1", "s1", nil)
	_, err := RunCheck(context.Background(), tooManyPartsCheckID, []Target{tgt}, CheckOptions{
		Scope: Scope{Database: "sales", Table: "events"},
	})
	if err != nil {
		t.Fatalf("RunCheck: %v", err)
	}
	if !strings.Contains(q.query, "AND database = ? AND table = ?") {
		t.Fatalf("scope predicate missing from query: %s", q.query)
	}
	if got, want := len(q.args), 2; got != want {
		t.Fatalf("args len = %d, want %d", got, want)
	}
	if q.args[0] != "sales" || q.args[1] != "events" {
		t.Fatalf("args = %#v", q.args)
	}
	if strings.Contains(q.query, "sales") || strings.Contains(q.query, "events") {
		t.Fatalf("scope values must not be interpolated: %s", q.query)
	}
}

func TestTableScopeRequiresDatabaseBeforeQuery(t *testing.T) {
	tgt, q := partsTarget("n1", "s1", nil)
	_, err := RunCheck(context.Background(), tooManyPartsCheckID, []Target{tgt}, CheckOptions{
		Scope: Scope{Table: "events"},
	})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("err = %v, want ErrInvalidInput", err)
	}
	if q.queries != 0 {
		t.Fatalf("invalid scope issued %d queries", q.queries)
	}
}

func TestScopeQueryShapesForTableChecks(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		anchor   string
		hasWhere bool
	}{
		{"replication failures", replicationQueueSQL, "ORDER BY", true},
		{"too many parts", tooManyPartsSQL, "GROUP BY", true},
		{"replica status", replicaStatusSQL, "ORDER BY", false},
		{"mutations", mutationsStuckSQL, "ORDER BY", true},
		{"queue backlog", queueBacklogSQL, "GROUP BY", false},
		{"detached parts", detachedPartsSQL, "ORDER BY", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, args, err := scopedQuery(tt.query, tt.anchor, tt.hasWhere, Scope{Database: "db", Table: "t"})
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(query, "database = ? AND table = ?") || len(args) != 2 {
				t.Fatalf("query=%q args=%#v", query, args)
			}
		})
	}
}
