package diagnose

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// ---- 测试辅助 ----

func partsRow(db, table, partition, parts, bytes string) []any {
	return []any{db, table, partition, parts, bytes}
}

func partsTarget(node, shard string, rows [][]any) (Target, *fakeQuerier) {
	q := &fakeQuerier{rows: &fakeRows{rows: rows}}
	return Target{NodeID: node, ShardID: shard, Querier: q}, q
}

// ---- 检查级测试 ----

func TestPartsThresholdFiltering(t *testing.T) {
	// 阈值判定：超过保留，等于或低于跳过；跳过行计入 RowsRead。
	tgt, _ := partsTarget("n1", "s1", [][]any{
		partsRow("db1", "t1", "2026-01", "11", "1048576"),
		partsRow("db1", "t1", "2026-02", "10", "1048576"), // 等于阈值：不保留
		partsRow("db1", "t1", "2026-03", "3", "2048"),
	})
	report, err := CheckTooManyParts(context.Background(), []Target{tgt},
		TooManyPartsOptions{PartsThreshold: 10})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != ExecutionComplete || report.Outcome != OutcomeMatched {
		t.Errorf("got %s/%s, want complete/matched", report.Execution, report.Outcome)
	}
	nr := report.Targets[0]
	if nr.RowsRead != 3 || nr.RowsKept != 1 {
		t.Errorf("计数错误: %+v", nr)
	}
	if len(report.Matches) != 1 || report.Matches[0].Partition != "2026-01" || report.Matches[0].Parts != 11 {
		t.Errorf("匹配错误: %+v", report.Matches)
	}
	if report.Matches[0].Observations[0].DiskBytes != "1048576" {
		t.Errorf("字节数未保留: %+v", report.Matches[0].Observations[0])
	}
}

func TestPartsDefaultThreshold(t *testing.T) {
	// 默认阈值 1000：1000 不报，1001 报。
	tgt, _ := partsTarget("n1", "s1", [][]any{
		partsRow("db1", "t1", "2026-01", "1000", "0"),
		partsRow("db1", "t1", "2026-02", "1001", "0"),
	})
	report, err := CheckTooManyParts(context.Background(), []Target{tgt}, TooManyPartsOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if len(report.Matches) != 1 || report.Matches[0].Partition != "2026-02" {
		t.Errorf("默认阈值判定错误: %+v", report.Matches)
	}
}

func TestPartsNoMatchComplete(t *testing.T) {
	tgt, _ := partsTarget("n1", "s1", [][]any{partsRow("db1", "t1", "2026-01", "5", "1024")})
	report, err := CheckTooManyParts(context.Background(), []Target{tgt}, TooManyPartsOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != ExecutionComplete || report.Outcome != OutcomeNoMatch {
		t.Errorf("got %s/%s, want complete/no_match", report.Execution, report.Outcome)
	}
	if len(report.NextSteps) != 0 {
		t.Errorf("complete + no_match 不应有建议: %+v", report.NextSteps)
	}
	if report.CheckID != tooManyPartsCheckID || report.RuleVersion != tooManyPartsRuleVersion {
		t.Errorf("检查身份错误: %s/%s", report.CheckID, report.RuleVersion)
	}
}

func TestPartsMergesSameShardReplicasKeepsMax(t *testing.T) {
	// 同分片两副本报同一分区：合并保留两个来源，Parts 取最大值。
	a, _ := partsTarget("n-a", "s1", [][]any{partsRow("db1", "t1", "2026-01", "1200", "100")})
	b, _ := partsTarget("n-b", "s1", [][]any{partsRow("db1", "t1", "2026-01", "3000", "200")})
	report, err := CheckTooManyParts(context.Background(), []Target{a, b}, TooManyPartsOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if len(report.Matches) != 1 {
		t.Fatalf("应归并为 1 条，got %d: %+v", len(report.Matches), report.Matches)
	}
	m := report.Matches[0]
	if len(m.Observations) != 2 || m.Parts != 3000 {
		t.Errorf("应保留两个来源且取最大: %+v", m)
	}
	if !hasAdviceCode(report.NextSteps, AdviceInspectMergeThroughput) {
		t.Errorf("匹配应给合并吞吐建议: %+v", report.NextSteps)
	}
	if !hasAdviceCode(report.NextSteps, AdviceReviewInsertBatchSize) ||
		!hasAdviceCode(report.NextSteps, AdviceReviewPartitionKey) {
		t.Errorf("匹配应给写入批次与分区键建议: %+v", report.NextSteps)
	}
}

func TestPartsSeparatesShards(t *testing.T) {
	a, _ := partsTarget("n1", "s1", [][]any{partsRow("db1", "t1", "2026-01", "2000", "0")})
	b, _ := partsTarget("n3", "s2", [][]any{partsRow("db1", "t1", "2026-01", "2000", "0")})
	report, _ := CheckTooManyParts(context.Background(), []Target{a, b}, TooManyPartsOptions{})
	if len(report.Matches) != 2 {
		t.Fatalf("不同分片应为 2 条，got %d", len(report.Matches))
	}
}

func TestPartsPartialFailureKeepsEvidence(t *testing.T) {
	down := Target{NodeID: "down", ShardID: "s1", Querier: &fakeQuerier{err: errors.New("connect refused")}}
	up, _ := partsTarget("up", "s2", [][]any{partsRow("db1", "t1", "2026-01", "2000", "0")})
	report, err := CheckTooManyParts(context.Background(), []Target{down, up}, TooManyPartsOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != ExecutionPartial || report.Outcome != OutcomeMatched {
		t.Errorf("got %s/%s, want partial/matched", report.Execution, report.Outcome)
	}
	if len(report.Matches) != 1 {
		t.Errorf("证据丢失: %+v", report.Matches)
	}
	if !hasAdviceCode(report.NextSteps, AdviceCompleteNodeCoverage) {
		t.Errorf("部分失败应提示补查覆盖: %+v", report.NextSteps)
	}
}

func TestPartsPartialNoMatchIsUnknown(t *testing.T) {
	down := Target{NodeID: "down", ShardID: "s1", Querier: &fakeQuerier{err: errors.New("connect refused")}}
	up, _ := partsTarget("up", "s2", [][]any{partsRow("db1", "t1", "2026-01", "5", "0")})
	report, err := CheckTooManyParts(context.Background(), []Target{down, up}, TooManyPartsOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != ExecutionPartial || report.Outcome != OutcomeUnknown {
		t.Errorf("got %s/%s, want partial/unknown", report.Execution, report.Outcome)
	}
}

func TestPartsAllFailedReturnsNoUsableObservation(t *testing.T) {
	a := Target{NodeID: "a", ShardID: "s1", Querier: &fakeQuerier{err: errors.New("boom")}}
	b := Target{NodeID: "b", ShardID: "s2", Querier: &fakeQuerier{err: errors.New("boom")}}
	report, err := CheckTooManyParts(context.Background(), []Target{a, b}, TooManyPartsOptions{})
	if !errors.Is(err, ErrNoUsableObservation) {
		t.Fatalf("err = %v, want ErrNoUsableObservation", err)
	}
	if report.Execution != ExecutionFailed || report.Outcome != OutcomeUnknown {
		t.Errorf("got %s/%s, want failed/unknown", report.Execution, report.Outcome)
	}
}

func TestPartsTruncationMarksIncomplete(t *testing.T) {
	// 超阈值行达到 N+1 条即截断：保留前 N 条，但 RowsRead 含全部已读行。
	rows := [][]any{
		partsRow("db1", "t1", "2026-01", "2000", "0"),
		partsRow("db1", "t1", "2026-02", "2000", "0"),
		partsRow("db1", "t1", "2026-03", "3000", "0"),
	}
	tgt, _ := partsTarget("n1", "s1", rows)
	report, err := CheckTooManyParts(context.Background(), []Target{tgt},
		TooManyPartsOptions{MaxRowsPerNode: 2, PartsThreshold: 10})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != ExecutionPartial || report.Outcome != OutcomeMatched {
		t.Errorf("got %s/%s, want partial/matched（截断不能输出 complete）", report.Execution, report.Outcome)
	}
	nr := report.Targets[0]
	if nr.RowsRead != 3 || nr.RowsKept != 2 || !nr.Truncated {
		t.Errorf("截断计数错误: %+v", nr)
	}
	if !hasAdviceCode(report.NextSteps, AdviceRerunWithHigherRowLimit) {
		t.Errorf("截断应提示复跑: %+v", report.NextSteps)
	}
}

func TestPartsScanErrorKeepsPriorRows(t *testing.T) {
	rows := [][]any{
		partsRow("db1", "t1", "2026-01", "2000", "0"),
		partsRow("db1", "t1", "2026-02", "2000", "0"),
		partsRow("db1", "t1", "2026-03", "3000", "0"),
	}
	fr := &fakeRows{rows: rows, scanErrAt: 2}
	tgt := Target{NodeID: "n1", ShardID: "s1", Querier: &fakeQuerier{rows: fr}}
	report, err := CheckTooManyParts(context.Background(), []Target{tgt}, TooManyPartsOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != ExecutionPartial || report.Outcome != OutcomeMatched {
		t.Errorf("got %s/%s, want partial/matched", report.Execution, report.Outcome)
	}
	nr := report.Targets[0]
	if nr.Status != NodeFailed || nr.ErrCategory != ErrCatScanFailed {
		t.Errorf("节点状态错误: %+v", nr)
	}
	if nr.RowsRead != 1 || nr.RowsKept != 1 || len(report.Matches) != 1 {
		t.Errorf("有效证据未保留: %+v / %+v", nr, report.Matches)
	}
}

func TestPartsUnparseableCountIsScanFailure(t *testing.T) {
	// parts 列解析失败按 scan 失败处理，不做静默猜测；
	// 先读到的有效证据仍然保留。
	rows := [][]any{
		partsRow("db1", "t1", "2026-01", "2000", "0"),
		partsRow("db1", "t1", "2026-02", "not-a-number", "0"),
		partsRow("db1", "t1", "2026-03", "300", "0"),
	}
	tgt, _ := partsTarget("n1", "s1", rows)
	report, err := CheckTooManyParts(context.Background(), []Target{tgt}, TooManyPartsOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil（已有有效证据，不应作为顶层错误）", err)
	}
	if report.Execution != ExecutionPartial || report.Outcome != OutcomeMatched {
		t.Errorf("got %s/%s, want partial/matched", report.Execution, report.Outcome)
	}
	nr := report.Targets[0]
	if nr.Status != NodeFailed || nr.ErrCategory != ErrCatScanFailed {
		t.Errorf("got %+v, want scan_failed", nr)
	}
	// 第 2 行解析失败终止采集：RowsRead 只含成功 Scan 的第 1 行。
	if nr.RowsRead != 1 || nr.RowsKept != 1 || len(report.Matches) != 1 {
		t.Errorf("证据保留错误: %+v / %+v", nr, report.Matches)
	}
}

func TestPartsTimeoutMarksNotStarted(t *testing.T) {
	slow := Target{NodeID: "slow", ShardID: "s1", Querier: &fakeQuerier{delay: 150 * time.Millisecond}}
	b1, _ := partsTarget("b1", "s2", [][]any{partsRow("db1", "t1", "2026-01", "2000", "0")})
	report, err := CheckTooManyParts(context.Background(), []Target{slow, b1},
		TooManyPartsOptions{Timeout: 80 * time.Millisecond, NodeTimeout: 500 * time.Millisecond, Concurrency: 1})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if report.Targets[0].Status != NodeFailed || report.Targets[0].ErrCategory != ErrCatTimeout {
		t.Errorf("慢节点分类错误: %+v", report.Targets[0])
	}
	if report.Targets[1].Status != NodeNotStarted || report.Targets[1].ErrCategory != ErrCatNotStarted {
		t.Errorf("未启动节点记录错误: %+v", report.Targets[1])
	}
}

func TestPartsInvalidInput(t *testing.T) {
	q := &fakeQuerier{}
	cases := []struct {
		name    string
		targets []Target
		opts    TooManyPartsOptions
	}{
		{"empty targets", nil, TooManyPartsOptions{}},
		{"empty node id", []Target{{ShardID: "s1", Querier: q}}, TooManyPartsOptions{}},
		{"nil querier", []Target{{NodeID: "n", ShardID: "s"}}, TooManyPartsOptions{}},
		{"dup node id", []Target{{NodeID: "n", ShardID: "s1", Querier: q}, {NodeID: "n", ShardID: "s2", Querier: q}}, TooManyPartsOptions{}},
		{"negative threshold", []Target{{NodeID: "n", ShardID: "s", Querier: q}}, TooManyPartsOptions{PartsThreshold: -1}},
		{"negative max rows", []Target{{NodeID: "n", ShardID: "s", Querier: q}}, TooManyPartsOptions{MaxRowsPerNode: -1}},
	}
	for _, c := range cases {
		report, err := CheckTooManyParts(context.Background(), c.targets, c.opts)
		if !errors.Is(err, ErrInvalidInput) {
			t.Errorf("%s: err = %v, want ErrInvalidInput", c.name, err)
		}
		if report.Execution != "" || report.Outcome != "" {
			t.Errorf("%s: 校验失败应返回零值报告: %+v", c.name, report)
		}
		if q.queries != 0 {
			t.Errorf("%s: 校验失败不应执行查询", c.name)
		}
	}
}

func TestPartsFixedQueryContract(t *testing.T) {
	// 固定查询的契约：system.parts、active 过滤、排除系统库、聚合分组、
	// toString 下发字符串列、无 SELECT *。
	for _, want := range []string{
		"system.parts", "active", "GROUP BY database, table, partition",
		"toString(count())", "toString(sum(bytes_on_disk))",
		"NOT IN ('system'",
	} {
		if !strings.Contains(tooManyPartsSQL, want) {
			t.Errorf("固定查询缺少 %q:\n%s", want, tooManyPartsSQL)
		}
	}
	if strings.Contains(tooManyPartsSQL, "SELECT *") {
		t.Error("固定查询不允许 SELECT *")
	}
}

func TestPartsMatchOrdering(t *testing.T) {
	// 同表多分区按字符串逆序（ISO 日期下新分区在前）；不同表按字典序。
	a, _ := partsTarget("n1", "s1", [][]any{
		partsRow("db1", "t1", "2026-01", "2000", "0"),
		partsRow("db1", "t1", "2026-02", "2000", "0"),
		partsRow("db1", "t2", "2026-01", "100", "0"),
	})
	report, _ := CheckTooManyParts(context.Background(), []Target{a}, TooManyPartsOptions{PartsThreshold: 10})
	if len(report.Matches) != 3 {
		t.Fatalf("应有 3 条，got %d", len(report.Matches))
	}
	want := [][2]string{{"t1", "2026-02"}, {"t1", "2026-01"}, {"t2", "2026-01"}}
	for i, w := range want {
		if report.Matches[i].Table != w[0] || report.Matches[i].Partition != w[1] {
			t.Errorf("Matches[%d] = %s/%s, want %s/%s", i,
				report.Matches[i].Table, report.Matches[i].Partition, w[0], w[1])
		}
	}
}
