package diagnose

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// ---- 测试辅助 ----

// mutationRow 形状对齐 mutationsStuckSQL 的列：
// db, table, mutation_id, command, is_done, age_seconds, latest_fail_reason。
func mutationRow(db, table, id, cmd, done, age, failReason string) []any {
	return []any{db, table, id, cmd, done, age, failReason}
}

func failingRow(db, table, id string, age string) []any {
	return mutationRow(db, table, id, "ALTER UPDATE b = intDiv(a, 0) WHERE 1", "0", age,
		"Code: 153. DB::Exception: Division by zero: while executing 'FUNCTION intDiv(a : 0, 0_UInt8 :: 3)'. (ILLEGAL_DIVISION)")
}

func pendingRow(db, table, id string, age string) []any {
	return mutationRow(db, table, id, "ALTER DELETE WHERE 1", "0", age, "")
}

func mutationTarget(node, shard string, rows [][]any) (Target, *fakeQuerier) {
	q := &fakeQuerier{rows: &fakeRows{rows: rows}}
	return Target{NodeID: node, ShardID: shard, Querier: q}, q
}

// ---- 检查级测试 ----

func TestMutationsFailingMatched(t *testing.T) {
	tgt, _ := mutationTarget("n1", "s1", [][]any{
		failingRow("db1", "t1", "mutation_3.txt", "5"),
	})
	report, err := CheckMutationsStuck(context.Background(), []Target{tgt}, MutationsStuckOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != ExecutionComplete || report.Outcome != OutcomeMatched {
		t.Errorf("got %s/%s, want complete/matched", report.Execution, report.Outcome)
	}
	if report.CheckID != mutationsStuckCheckID || report.RuleVersion != mutationsStuckRuleVersion {
		t.Errorf("检查身份错误: %s/%s", report.CheckID, report.RuleVersion)
	}
	m := report.Matches[0]
	if m.MutationID != "mutation_3.txt" || m.Table != "t1" {
		t.Errorf("匹配对象错误: %+v", m)
	}
	o := m.Observations[0]
	if !strings.Contains(o.LatestFailReason, "ILLEGAL_DIVISION") {
		t.Errorf("失败文本未保留: %.160s", o.LatestFailReason)
	}
	if o.Command == "" || o.AgeSeconds != 5 {
		t.Errorf("command/年龄未采集: %+v", o)
	}
	if len(o.Reasons) != 1 || o.Reasons[0] != ReasonMutationFailing {
		t.Errorf("理由码错误: %+v", o.Reasons)
	}
	if !hasAdviceCode(report.NextSteps, AdviceInspectMutationFailure) {
		t.Errorf("失败 mutation 应给核查建议: %+v", report.NextSteps)
	}
}

func TestMutationsLongRunningMatched(t *testing.T) {
	// 无失败文本但年龄超阈值：长任务未完成，需人工确认是否在推进。
	tgt, _ := mutationTarget("n1", "s1", [][]any{
		pendingRow("db1", "t1", "mutation_9.txt", "90000"),
	})
	report, err := CheckMutationsStuck(context.Background(), []Target{tgt}, MutationsStuckOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Outcome != OutcomeMatched {
		t.Fatalf("got %s, want matched", report.Outcome)
	}
	o := report.Matches[0].Observations[0]
	if o.LatestFailReason != "" {
		t.Errorf("不应有失败文本: %+v", o)
	}
	if len(o.Reasons) != 1 || o.Reasons[0] != ReasonMutationLongRunning {
		t.Errorf("理由码错误: %+v", o.Reasons)
	}
	if !hasAdviceCode(report.NextSteps, AdviceCheckMutationProgress) {
		t.Errorf("长任务应给进度核查建议: %+v", report.NextSteps)
	}
}

func TestMutationsAgeThresholdBoundary(t *testing.T) {
	// 等于阈值命中（>= 语义）；年轻且无失败文本的条目不命中（只读不报）。
	tgt, _ := mutationTarget("n1", "s1", [][]any{
		pendingRow("db1", "t1", "mutation_1.txt", "86400"),
		pendingRow("db1", "t1", "mutation_2.txt", "86399"),
		pendingRow("db1", "t1", "mutation_3.txt", "10"),
	})
	report, err := CheckMutationsStuck(context.Background(), []Target{tgt}, MutationsStuckOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Targets[0].RowsRead != 3 || report.Targets[0].RowsKept != 1 {
		t.Fatalf("阈值边界判定错误: %+v", report.Targets[0])
	}
	if len(report.Matches) != 1 || report.Matches[0].MutationID != "mutation_1.txt" {
		t.Errorf("匹配错误: %+v", report.Matches)
	}
}

func TestMutationsFailingYoungKeptRegardlessOfAge(t *testing.T) {
	// 失败重试的 mutation 无论年龄多小都命中；同时超龄时双理由按声明顺序。
	tgt, _ := mutationTarget("n1", "s1", [][]any{
		failingRow("db1", "t1", "mutation_1.txt", "1"),
		failingRow("db1", "t1", "mutation_2.txt", "90000"),
	})
	report, _ := CheckMutationsStuck(context.Background(), []Target{tgt}, MutationsStuckOptions{})
	if len(report.Matches) != 2 {
		t.Fatalf("应匹配 2 条，got %d: %+v", len(report.Matches), report.Matches)
	}
	young := report.Matches[0].Observations[0]
	if len(young.Reasons) != 1 || young.Reasons[0] != ReasonMutationFailing {
		t.Errorf("年轻失败条目理由错误: %+v", young.Reasons)
	}
	old := report.Matches[1].Observations[0]
	want := []string{ReasonMutationFailing, ReasonMutationLongRunning}
	if len(old.Reasons) != len(want) {
		t.Fatalf("理由数 = %d, want %d: %+v", len(old.Reasons), len(want), old.Reasons)
	}
	for i := range want {
		if old.Reasons[i] != want[i] {
			t.Errorf("Reasons[%d] = %s, want %s", i, old.Reasons[i], want[i])
		}
	}
}

func TestMutationsDefaultAgeThreshold(t *testing.T) {
	// 默认阈值 86400：86400 报，86399 不报。
	tgt, _ := mutationTarget("n1", "s1", [][]any{
		pendingRow("db1", "t1", "mutation_1.txt", "86399"),
		pendingRow("db1", "t1", "mutation_2.txt", "86400"),
	})
	report, _ := CheckMutationsStuck(context.Background(), []Target{tgt}, MutationsStuckOptions{})
	if len(report.Matches) != 1 || report.Matches[0].MutationID != "mutation_2.txt" {
		t.Errorf("默认阈值判定错误: %+v", report.Matches)
	}
}

func TestMutationsDoneRowBreaksContract(t *testing.T) {
	// 固定查询服务端已过滤 is_done=1；若客户端仍读到已完成条目，
	// 视为契约破坏（scan 失败），不静默跳过。已读证据保留。
	rows := [][]any{
		failingRow("db1", "t1", "mutation_1.txt", "5"),
		mutationRow("db1", "t2", "mutation_2.txt", "ALTER UPDATE b = 'x' WHERE 1", "1", "3", ""),
		pendingRow("db1", "t3", "mutation_3.txt", "90000"),
	}
	tgt, _ := mutationTarget("n1", "s1", rows)
	report, err := CheckMutationsStuck(context.Background(), []Target{tgt}, MutationsStuckOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil（已有有效证据，不应作为顶层错误）", err)
	}
	nr := report.Targets[0]
	if nr.Status != NodeFailed || nr.ErrCategory != ErrCatScanFailed {
		t.Errorf("got %+v, want scan_failed", nr)
	}
	if nr.RowsRead != 1 || nr.RowsKept != 1 || len(report.Matches) != 1 {
		t.Errorf("证据保留错误: %+v / %+v", nr, report.Matches)
	}
}

func TestMutationsUnparseableCountIsScanFailure(t *testing.T) {
	rows := [][]any{
		failingRow("db1", "t1", "mutation_1.txt", "5"),
		mutationRow("db1", "t2", "mutation_2.txt", "ALTER UPDATE b = 'x' WHERE 1", "0", "not-a-number", ""),
		pendingRow("db1", "t3", "mutation_3.txt", "90000"),
	}
	tgt, _ := mutationTarget("n1", "s1", rows)
	report, _ := CheckMutationsStuck(context.Background(), []Target{tgt}, MutationsStuckOptions{})
	nr := report.Targets[0]
	if nr.Status != NodeFailed || nr.ErrCategory != ErrCatScanFailed {
		t.Errorf("got %+v, want scan_failed", nr)
	}
	if nr.RowsRead != 1 || nr.RowsKept != 1 || len(report.Matches) != 1 {
		t.Errorf("证据保留错误: %+v / %+v", nr, report.Matches)
	}
}

func TestMutationsMergesSameShardKeepsBothViews(t *testing.T) {
	// 同分片两副本报告同一 mutation：一个副本失败重试、另一个已推进
	// 到无失败状态但超龄，两个视角都保留。
	a, _ := mutationTarget("n-a", "s1", [][]any{
		failingRow("db1", "t1", "mutation_5.txt", "10"),
	})
	b, _ := mutationTarget("n-b", "s1", [][]any{
		pendingRow("db1", "t1", "mutation_5.txt", "90000"),
	})
	report, err := CheckMutationsStuck(context.Background(), []Target{a, b}, MutationsStuckOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if len(report.Matches) != 1 {
		t.Fatalf("应归并为 1 条，got %d: %+v", len(report.Matches), report.Matches)
	}
	m := report.Matches[0]
	if len(m.Observations) != 2 || m.Observations[0].NodeID != "n-a" || m.Observations[1].NodeID != "n-b" {
		t.Errorf("来源观测错误: %+v", m.Observations)
	}
	if m.Observations[0].Reasons[0] != ReasonMutationFailing ||
		m.Observations[1].Reasons[0] != ReasonMutationLongRunning {
		t.Errorf("各节点视角未保留: %+v", m.Observations)
	}
}

func TestMutationsSeparatesShardsAndTables(t *testing.T) {
	a, _ := mutationTarget("n1", "s1", [][]any{failingRow("db1", "t1", "mutation_1.txt", "1")})
	b, _ := mutationTarget("n3", "s2", [][]any{failingRow("db1", "t1", "mutation_1.txt", "1")})
	c, _ := mutationTarget("n4", "s1", [][]any{failingRow("db1", "t2", "mutation_1.txt", "1")})
	report, _ := CheckMutationsStuck(context.Background(), []Target{a, b, c}, MutationsStuckOptions{})
	if len(report.Matches) != 3 {
		t.Fatalf("不同分片/表应为 3 条，got %d", len(report.Matches))
	}
}

func TestMutationsNoMatchComplete(t *testing.T) {
	// 全部条目年轻且无失败文本：complete/no_match，无建议。
	tgt, _ := mutationTarget("n1", "s1", [][]any{
		pendingRow("db1", "t1", "mutation_1.txt", "30"),
	})
	report, err := CheckMutationsStuck(context.Background(), []Target{tgt}, MutationsStuckOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != ExecutionComplete || report.Outcome != OutcomeNoMatch {
		t.Errorf("got %s/%s, want complete/no_match", report.Execution, report.Outcome)
	}
	if len(report.Matches) != 0 || len(report.NextSteps) != 0 {
		t.Errorf("无匹配不应有建议: %+v / %+v", report.Matches, report.NextSteps)
	}
	if report.Targets[0].RowsRead != 1 || report.Targets[0].RowsKept != 0 {
		t.Errorf("只读不报的条目应计入 RowsRead: %+v", report.Targets[0])
	}
}

func TestMutationsPartialFailureKeepsEvidence(t *testing.T) {
	down := Target{NodeID: "down", ShardID: "s1", Querier: &fakeQuerier{err: errors.New("connect refused")}}
	up, _ := mutationTarget("up", "s2", [][]any{failingRow("db1", "t1", "mutation_1.txt", "5")})
	report, err := CheckMutationsStuck(context.Background(), []Target{down, up}, MutationsStuckOptions{})
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

func TestMutationsTruncationMarksIncomplete(t *testing.T) {
	rows := [][]any{
		failingRow("db1", "t1", "mutation_1.txt", "1"),
		failingRow("db1", "t1", "mutation_2.txt", "1"),
		failingRow("db1", "t1", "mutation_3.txt", "1"),
	}
	tgt, _ := mutationTarget("n1", "s1", rows)
	report, err := CheckMutationsStuck(context.Background(), []Target{tgt},
		MutationsStuckOptions{MaxRowsPerNode: 2})
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

func TestMutationsAllFailedReturnsNoUsableObservation(t *testing.T) {
	a := Target{NodeID: "a", ShardID: "s1", Querier: &fakeQuerier{err: errors.New("boom")}}
	b := Target{NodeID: "b", ShardID: "s2", Querier: &fakeQuerier{err: errors.New("boom")}}
	report, err := CheckMutationsStuck(context.Background(), []Target{a, b}, MutationsStuckOptions{})
	if !errors.Is(err, ErrNoUsableObservation) {
		t.Fatalf("err = %v, want ErrNoUsableObservation", err)
	}
	if report.Execution != ExecutionFailed || report.Outcome != OutcomeUnknown {
		t.Errorf("got %s/%s, want failed/unknown", report.Execution, report.Outcome)
	}
}

func TestMutationsTimeoutMarksNotStarted(t *testing.T) {
	slow := Target{NodeID: "slow", ShardID: "s1", Querier: &fakeQuerier{delay: 150 * time.Millisecond}}
	b1, _ := mutationTarget("b1", "s2", [][]any{failingRow("db1", "t1", "mutation_1.txt", "1")})
	report, err := CheckMutationsStuck(context.Background(), []Target{slow, b1},
		MutationsStuckOptions{Timeout: 80 * time.Millisecond, NodeTimeout: 500 * time.Millisecond, Concurrency: 1})
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

func TestMutationsInvalidInput(t *testing.T) {
	q := &fakeQuerier{}
	cases := []struct {
		name    string
		targets []Target
		opts    MutationsStuckOptions
	}{
		{"empty targets", nil, MutationsStuckOptions{}},
		{"empty node id", []Target{{ShardID: "s1", Querier: q}}, MutationsStuckOptions{}},
		{"nil querier", []Target{{NodeID: "n", ShardID: "s"}}, MutationsStuckOptions{}},
		{"dup node id", []Target{{NodeID: "n", ShardID: "s1", Querier: q}, {NodeID: "n", ShardID: "s2", Querier: q}}, MutationsStuckOptions{}},
		{"negative age threshold", []Target{{NodeID: "n", ShardID: "s", Querier: q}}, MutationsStuckOptions{AgeThresholdSeconds: -1}},
		{"negative timeout", []Target{{NodeID: "n", ShardID: "s", Querier: q}}, MutationsStuckOptions{Timeout: -1}},
	}
	for _, c := range cases {
		report, err := CheckMutationsStuck(context.Background(), c.targets, c.opts)
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

func TestMutationsFixedQueryContract(t *testing.T) {
	// 固定查询的契约：system.mutations、is_done 过滤、服务端算年龄、
	// toString 下发字符串列、无 SELECT *。
	for _, want := range []string{
		"FROM system.mutations", "WHERE NOT is_done",
		"toString(is_done)", "dateDiff('second', create_time, now())",
		"latest_fail_reason",
	} {
		if !strings.Contains(mutationsStuckSQL, want) {
			t.Errorf("固定查询缺少 %q:\n%s", want, mutationsStuckSQL)
		}
	}
	if strings.Contains(mutationsStuckSQL, "SELECT *") {
		t.Error("固定查询不允许 SELECT *")
	}
}

func TestMutationsMatchOrdering(t *testing.T) {
	// 按分片/库/表/mutation_id 字典序。
	a, _ := mutationTarget("n1", "s1", [][]any{
		failingRow("db2", "t1", "mutation_1.txt", "1"),
		failingRow("db1", "t2", "mutation_1.txt", "1"),
		failingRow("db1", "t1", "mutation_2.txt", "1"),
		failingRow("db1", "t1", "mutation_1.txt", "1"),
	})
	report, _ := CheckMutationsStuck(context.Background(), []Target{a}, MutationsStuckOptions{})
	want := [][3]string{{"db1", "t1", "mutation_1.txt"}, {"db1", "t1", "mutation_2.txt"}, {"db1", "t2", "mutation_1.txt"}, {"db2", "t1", "mutation_1.txt"}}
	if len(report.Matches) != 4 {
		t.Fatalf("应有 4 条，got %d", len(report.Matches))
	}
	for i, w := range want {
		m := report.Matches[i]
		if m.Database != w[0] || m.Table != w[1] || m.MutationID != w[2] {
			t.Errorf("Matches[%d] = %s/%s/%s, want %s/%s/%s", i,
				m.Database, m.Table, m.MutationID, w[0], w[1], w[2])
		}
	}
}
