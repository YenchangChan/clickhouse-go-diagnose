package diagnose

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func scenarioTarget(rows [][]any) (Target, *fakeQuerier) {
	q := &fakeQuerier{rows: &fakeRows{rows: rows}}
	return Target{NodeID: "node-1", ShardID: "shard-1", Querier: q}, q
}

func TestResourcePressureReportsSnapshotAndThresholdMatch(t *testing.T) {
	target, _ := scenarioTarget([][]any{{"0.91", "900", "1000"}})
	report, err := CheckResourcePressure(context.Background(), []Target{target}, ResourcePressureOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if report.Outcome != OutcomeMatched || len(report.Snapshots) != 1 || len(report.Matches) != 1 {
		t.Fatalf("unexpected report: %+v", report)
	}
	if report.Matches[0].CPUPercent != 91 || report.Matches[0].MemoryPercent != 90 {
		t.Fatalf("unexpected percentages: %+v", report.Matches[0])
	}
}

func TestSlowQueryUsesBoundQueryID(t *testing.T) {
	target, q := scenarioTarget([][]any{{"q-1", "1200", "100", "2048", "1", "4096", "800000", "SELECT 1"}})
	report, err := CheckSlowQuery(context.Background(), []Target{target}, SlowQueryOptions{QueryID: "q-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(q.args) != 1 || q.args[0] != "q-1" || strings.Contains(q.query, "q-1") {
		t.Fatalf("query must bind query_id: query=%q args=%#v", q.query, q.args)
	}
	if len(report.Matches) != 1 || report.Matches[0].DurationMS != 1200 || report.Matches[0].CPUTimeUS != 800000 {
		t.Fatalf("unexpected report: %+v", report)
	}
}

func TestDistributedDDLUsesBoundEntry(t *testing.T) {
	target, q := scenarioTarget([][]any{{"entry-1", "host-1", "Inactive", "999", "boom", "300", "ALTER TABLE t"}})
	report, err := CheckDistributedDDL(context.Background(), []Target{target}, DistributedDDLOptions{Entry: "entry-1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(q.args) != 1 || q.args[0] != "entry-1" || strings.Contains(q.query, "entry-1") {
		t.Fatalf("query must bind entry: query=%q args=%#v", q.query, q.args)
	}
	if len(report.Matches) != 1 || report.Matches[0].ExceptionCode != 999 || report.Matches[0].AgeSeconds != 300 {
		t.Fatalf("unexpected report: %+v", report)
	}
}

func TestNativeScenarioSelectorsRequiredBeforeQuery(t *testing.T) {
	target, q := scenarioTarget(nil)
	if _, err := CheckSlowQuery(context.Background(), []Target{target}, SlowQueryOptions{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("slow query err = %v", err)
	}
	if _, err := CheckDistributedDDL(context.Background(), []Target{target}, DistributedDDLOptions{}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("DDL err = %v", err)
	}
	if q.queries != 0 {
		t.Fatalf("invalid selectors issued %d queries", q.queries)
	}
}
