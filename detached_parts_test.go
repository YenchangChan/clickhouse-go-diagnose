package diagnose

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// ---- 测试辅助 ----

// detachedRow 形状对齐 detachedPartsSQL 的列：
// database, table, name, reason, disk, bytes_on_disk（字符串）。
func detachedRow(db, table, name, reason, disk, bytes string) []any {
	return []any{db, table, name, reason, disk, bytes}
}

func detachedTarget(node, shard string, rows [][]any) (Target, *fakeQuerier) {
	q := &fakeQuerier{rows: &fakeRows{rows: rows}}
	return Target{NodeID: node, ShardID: shard, Querier: q}, q
}

// ---- 检查级测试 ----

func TestDetachedUserDetachOnlyCountRule(t *testing.T) {
	// 空 reason 的用户 detach：默认阈值 10 下不构成匹配；
	// 阈值 1 时通过数量规则命中，匹配级 Reasons 带 count_exceeded。
	tgt, _ := detachedTarget("n1", "s1", [][]any{
		detachedRow("app", "t1", "all_1_1_0", "", "default", "1024"),
	})
	report, err := CheckDetachedParts(context.Background(), []Target{tgt}, DetachedPartsOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != ExecutionComplete || report.Outcome != OutcomeNoMatch {
		t.Errorf("got %s/%s, want complete/no_match", report.Execution, report.Outcome)
	}
	if report.Targets[0].RowsRead != 1 {
		t.Errorf("所有行都应保留为证据: %+v", report.Targets[0])
	}

	tgt2, _ := detachedTarget("n1", "s1", [][]any{
		detachedRow("app", "t1", "all_1_1_0", "", "default", "1024"),
	})
	report2, _ := CheckDetachedParts(context.Background(), []Target{tgt2}, DetachedPartsOptions{CountThreshold: 1})
	if report2.Outcome != OutcomeMatched {
		t.Fatalf("got %s, want matched", report2.Outcome)
	}
	m := report2.Matches[0]
	if m.Database != "app" || m.Table != "t1" || m.Parts != 1 {
		t.Errorf("匹配对象错误: %+v", m)
	}
	if len(m.Reasons) != 1 || m.Reasons[0] != ReasonDetachedCountExceeded {
		t.Errorf("匹配级理由码错误: %+v", m.Reasons)
	}
	if len(m.Observations[0].Reasons) != 0 {
		t.Errorf("空 reason 的用户 detach 不应有逐 part 理由: %+v", m.Observations[0])
	}
}

func TestDetachedBrokenReason(t *testing.T) {
	// broken 前缀（含 broken-on-start）→ detached_broken。
	tgt, _ := detachedTarget("n1", "s1", [][]any{
		detachedRow("app", "t1", "broken_1_1_0", "broken", "default", "512"),
		detachedRow("app", "t1", "broken-on-start_2_2_0", "broken-on-start", "default", "0"),
	})
	report, _ := CheckDetachedParts(context.Background(), []Target{tgt}, DetachedPartsOptions{})
	if report.Outcome != OutcomeMatched {
		t.Fatalf("got %s, want matched", report.Outcome)
	}
	m := report.Matches[0]
	if len(m.Reasons) != 0 { // 默认阈值 10，单表只有 2 条，不触发数量规则
		t.Errorf("不应触发数量规则: %+v", m.Reasons)
	}
	for i, want := range []string{ReasonDetachedBroken, ReasonDetachedBroken} {
		if len(m.Observations[i].Reasons) != 1 || m.Observations[i].Reasons[0] != want {
			t.Errorf("Observations[%d] 理由码错误: %+v", i, m.Observations[i].Reasons)
		}
	}
	if !hasAdviceCode(report.NextSteps, AdviceInvestigateBrokenPart) {
		t.Errorf("损坏 part 应触发专项核查建议: %+v", report.NextSteps)
	}
	if !hasAdviceCode(report.NextSteps, AdviceInspectDetachedParts) {
		t.Errorf("缺少 detached 目录核查建议: %+v", report.NextSteps)
	}
}

func TestDetachedServerInitiatedReason(t *testing.T) {
	// 非空非 broken 前缀（如 merge 残留、clone）→ detached_server_initiated。
	tgt, _ := detachedTarget("n1", "s1", [][]any{
		detachedRow("app", "t1", "merge_1_2_0", "merge", "default", "2048"),
	})
	report, _ := CheckDetachedParts(context.Background(), []Target{tgt}, DetachedPartsOptions{})
	if report.Outcome != OutcomeMatched {
		t.Fatalf("got %s, want matched", report.Outcome)
	}
	o := report.Matches[0].Observations[0]
	if len(o.Reasons) != 1 || o.Reasons[0] != ReasonDetachedServerInitiated {
		t.Errorf("理由码错误: %+v", o.Reasons)
	}
	if hasAdviceCode(report.NextSteps, AdviceInvestigateBrokenPart) {
		t.Errorf("非 broken 原因不应触发损坏专项建议: %+v", report.NextSteps)
	}
}

func TestDetachedNullReasonEqualsEmpty(t *testing.T) {
	// reason 为 SQL NULL（fakeRows 传 nil）：与空字符串同语义，不触发逐 part 规则。
	tgt, _ := detachedTarget("n1", "s1", [][]any{
		[]any{"app", "t1", "all_1_1_0", nil, "default", "1024"},
	})
	report, err := CheckDetachedParts(context.Background(), []Target{tgt}, DetachedPartsOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != ExecutionComplete || report.Outcome != OutcomeNoMatch {
		t.Errorf("got %s/%s, want complete/no_match", report.Execution, report.Outcome)
	}
}

func TestDetachedCountBoundary(t *testing.T) {
	// 默认阈值 10（>= 判定）：恰好 10 条命中，9 条不命中。
	rows := func(n int) [][]any {
		var out [][]any
		for i := 0; i < n; i++ {
			out = append(out, detachedRow("app", "t1", "all_1_1_0_"+string(rune('a'+i)), "", "default", "1"))
		}
		return out
	}
	tgt9, _ := detachedTarget("n1", "s1", rows(9))
	report, _ := CheckDetachedParts(context.Background(), []Target{tgt9}, DetachedPartsOptions{})
	if len(report.Matches) != 0 {
		t.Fatalf("9 条不应命中: %+v", report.Matches)
	}
	tgt10, _ := detachedTarget("n1", "s1", rows(10))
	report, _ = CheckDetachedParts(context.Background(), []Target{tgt10}, DetachedPartsOptions{})
	if len(report.Matches) != 1 || report.Matches[0].Parts != 10 ||
		len(report.Matches[0].Reasons) != 1 || report.Matches[0].Reasons[0] != ReasonDetachedCountExceeded {
		t.Fatalf("10 条应命中且 Parts=10: %+v", report.Matches)
	}
}

func TestDetachedMergeSameShardMaxPerNode(t *testing.T) {
	// 同分片两节点的同一张表：Parts 取单节点最大值（2），不跨节点累加（4）。
	a, _ := detachedTarget("n-a", "s1", [][]any{
		detachedRow("app", "t1", "all_1_1_0", "", "default", "1"),
		detachedRow("app", "t1", "all_2_2_0", "", "default", "1"),
	})
	b, _ := detachedTarget("n-b", "s1", [][]any{
		detachedRow("app", "t1", "all_1_1_0", "", "default", "1"),
		detachedRow("app", "t1", "all_2_2_0", "", "default", "1"),
	})
	report, err := CheckDetachedParts(context.Background(), []Target{a, b}, DetachedPartsOptions{CountThreshold: 2})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if len(report.Matches) != 1 {
		t.Fatalf("应归并为 1 条，got %d", len(report.Matches))
	}
	m := report.Matches[0]
	if m.Parts != 2 {
		t.Errorf("Parts 应取单节点最大值 2 而非累加 4: %+v", m)
	}
	if len(m.Observations) != 4 {
		t.Errorf("来源观测应全部保留: %+v", m.Observations)
	}
	if len(m.Reasons) != 1 || m.Reasons[0] != ReasonDetachedCountExceeded {
		t.Errorf("阈值 2 应触发数量规则: %+v", m.Reasons)
	}
}

func TestDetachedSeparatesShardsAndTables(t *testing.T) {
	a, _ := detachedTarget("n1", "s1", [][]any{detachedRow("app", "t1", "p1", "", "d", "1")})
	b, _ := detachedTarget("n2", "s2", [][]any{detachedRow("app", "t1", "p1", "", "d", "1")})
	c, _ := detachedTarget("n3", "s1", [][]any{detachedRow("app", "t2", "p1", "", "d", "1")})
	report, _ := CheckDetachedParts(context.Background(), []Target{a, b, c},
		DetachedPartsOptions{CountThreshold: 1})
	if len(report.Matches) != 3 {
		t.Fatalf("不同分片/表应为 3 条，got %d: %+v", len(report.Matches), report.Matches)
	}
}

func TestDetachedEmptyNoMatch(t *testing.T) {
	tgt, _ := detachedTarget("n1", "s1", nil)
	report, err := CheckDetachedParts(context.Background(), []Target{tgt}, DetachedPartsOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != ExecutionComplete || report.Outcome != OutcomeNoMatch {
		t.Errorf("got %s/%s, want complete/no_match", report.Execution, report.Outcome)
	}
	if len(report.Matches) != 0 || len(report.NextSteps) != 0 {
		t.Errorf("空结果不应有匹配或建议: %+v / %+v", report.Matches, report.NextSteps)
	}
}

func TestDetachedUnparseableBytesIsScanFailure(t *testing.T) {
	tgt, _ := detachedTarget("n1", "s1", [][]any{
		detachedRow("app", "t1", "all_1_1_0", "", "default", "1024"),
		detachedRow("app", "t1", "bad_1_1_0", "", "default", "not-a-number"),
	})
	report, _ := CheckDetachedParts(context.Background(), []Target{tgt}, DetachedPartsOptions{})
	nr := report.Targets[0]
	if nr.Status != NodeFailed || nr.ErrCategory != ErrCatScanFailed {
		t.Errorf("got %+v, want scan_failed", nr)
	}
	if nr.RowsRead != 1 || nr.RowsKept != 1 || len(report.Matches) != 0 {
		// 空 reason 的单条行保留为证据但不构成匹配（低于默认阈值且无理由）。
		t.Errorf("证据保留错误: %+v / %+v", nr, report.Matches)
	}
}

func TestDetachedPartialFailureKeepsEvidence(t *testing.T) {
	down := Target{NodeID: "down", ShardID: "s1", Querier: &fakeQuerier{err: errors.New("connect refused")}}
	up, _ := detachedTarget("up", "s2", [][]any{detachedRow("app", "t1", "broken_1_1_0", "broken", "d", "1")})
	report, err := CheckDetachedParts(context.Background(), []Target{down, up}, DetachedPartsOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != ExecutionPartial || report.Outcome != OutcomeMatched {
		t.Errorf("got %s/%s, want partial/matched", report.Execution, report.Outcome)
	}
	if !hasAdviceCode(report.NextSteps, AdviceCompleteNodeCoverage) {
		t.Errorf("部分失败应提示补查覆盖: %+v", report.NextSteps)
	}
}

func TestDetachedTruncationMarksIncomplete(t *testing.T) {
	tgt, _ := detachedTarget("n1", "s1", [][]any{
		detachedRow("app", "t1", "p1", "", "d", "1"),
		detachedRow("app", "t1", "p2", "", "d", "1"),
		detachedRow("app", "t1", "p3", "", "d", "1"),
	})
	report, err := CheckDetachedParts(context.Background(), []Target{tgt},
		DetachedPartsOptions{MaxRowsPerNode: 2, CountThreshold: 1})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != ExecutionPartial || report.Outcome != OutcomeMatched {
		t.Errorf("got %s/%s, want partial/matched（截断不能输出 complete）", report.Execution, report.Outcome)
	}
	if !report.Targets[0].Truncated || !hasAdviceCode(report.NextSteps, AdviceRerunWithHigherRowLimit) {
		t.Errorf("截断标记/建议缺失: %+v / %+v", report.Targets[0], report.NextSteps)
	}
}

func TestDetachedAllFailedReturnsNoUsableObservation(t *testing.T) {
	a := Target{NodeID: "a", ShardID: "s1", Querier: &fakeQuerier{err: errors.New("boom")}}
	report, err := CheckDetachedParts(context.Background(), []Target{a}, DetachedPartsOptions{})
	if !errors.Is(err, ErrNoUsableObservation) {
		t.Fatalf("err = %v, want ErrNoUsableObservation", err)
	}
	if report.Execution != ExecutionFailed || report.Outcome != OutcomeUnknown {
		t.Errorf("got %s/%s, want failed/unknown", report.Execution, report.Outcome)
	}
}

func TestDetachedTimeoutMarksNotStarted(t *testing.T) {
	slow := Target{NodeID: "slow", ShardID: "s1", Querier: &fakeQuerier{delay: 150 * time.Millisecond}}
	b1, _ := detachedTarget("b1", "s2", [][]any{detachedRow("app", "t1", "p1", "", "d", "1")})
	report, err := CheckDetachedParts(context.Background(), []Target{slow, b1},
		DetachedPartsOptions{Timeout: 80 * time.Millisecond, NodeTimeout: 500 * time.Millisecond, Concurrency: 1})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if report.Targets[0].Status != NodeFailed || report.Targets[0].ErrCategory != ErrCatTimeout {
		t.Errorf("慢节点分类错误: %+v", report.Targets[0])
	}
	if report.Targets[1].Status != NodeNotStarted {
		t.Errorf("未启动节点记录错误: %+v", report.Targets[1])
	}
}

func TestDetachedInvalidInput(t *testing.T) {
	q := &fakeQuerier{}
	cases := []struct {
		name    string
		targets []Target
		opts    DetachedPartsOptions
	}{
		{"empty targets", nil, DetachedPartsOptions{}},
		{"empty node id", []Target{{ShardID: "s1", Querier: q}}, DetachedPartsOptions{}},
		{"nil querier", []Target{{NodeID: "n", ShardID: "s"}}, DetachedPartsOptions{}},
		{"dup node id", []Target{{NodeID: "n", ShardID: "s1", Querier: q}, {NodeID: "n", ShardID: "s2", Querier: q}}, DetachedPartsOptions{}},
		{"negative threshold", []Target{{NodeID: "n", ShardID: "s", Querier: q}}, DetachedPartsOptions{CountThreshold: -1}},
		{"negative max rows", []Target{{NodeID: "n", ShardID: "s", Querier: q}}, DetachedPartsOptions{MaxRowsPerNode: -1}},
		{"negative timeout", []Target{{NodeID: "n", ShardID: "s", Querier: q}}, DetachedPartsOptions{Timeout: -1}},
	}
	for _, c := range cases {
		report, err := CheckDetachedParts(context.Background(), c.targets, c.opts)
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

func TestDetachedFixedQueryContract(t *testing.T) {
	for _, want := range []string{
		"FROM system.detached_parts", "coalesce(reason, '')", "toString(bytes_on_disk)",
		"ORDER BY database, table, name",
	} {
		if !strings.Contains(detachedPartsSQL, want) {
			t.Errorf("固定查询缺少 %q:\n%s", want, detachedPartsSQL)
		}
	}
	if strings.Contains(detachedPartsSQL, "SELECT *") {
		t.Error("固定查询不允许 SELECT *")
	}
	// system 库与 information_schema 必须在查询内排除（26.10 真机上
	// 服务日志表的 broken-on-start 残留属于环境噪音）。
	for _, want := range []string{"'system'", "'information_schema'"} {
		if !strings.Contains(detachedPartsSQL, want) {
			t.Errorf("固定查询缺少系统库排除 %q:\n%s", want, detachedPartsSQL)
		}
	}
}

func TestDetachedMatchOrdering(t *testing.T) {
	tgt, _ := detachedTarget("n1", "s1", [][]any{
		detachedRow("app", "t2", "p1", "broken", "d", "1"),
		detachedRow("app", "t1", "p2", "merge", "d", "1"),
		detachedRow("app", "t1", "p1", "", "d", "1"),
		detachedRow("beta", "t1", "p1", "", "d", "1"),
	})
	report, _ := CheckDetachedParts(context.Background(), []Target{tgt}, DetachedPartsOptions{CountThreshold: 1})
	want := []struct{ db, table string }{{"app", "t1"}, {"app", "t2"}, {"beta", "t1"}}
	if len(report.Matches) != len(want) {
		t.Fatalf("应有 %d 条，got %d: %+v", len(want), len(report.Matches), report.Matches)
	}
	for i, w := range want {
		m := report.Matches[i]
		if m.Database != w.db || m.Table != w.table {
			t.Errorf("Matches[%d] = %s.%s, want %s.%s", i, m.Database, m.Table, w.db, w.table)
		}
	}
}
