package diagnose

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// ---- 测试辅助 ----

// replicaRow 形状对齐 replicaStatusSQL 的列：
// db, table, is_readonly, is_session_expired, absolute_delay,
// total_replicas, active_replicas, lost_part_count。
func replicaRow(db, table, ro, se, delay, total, active, lost string) []any {
	return []any{db, table, ro, se, delay, total, active, lost}
}

func healthyRow(db, table string) []any {
	return replicaRow(db, table, "0", "0", "0", "2", "2", "0")
}

func replicaTarget(node, shard string, rows [][]any) (Target, *fakeQuerier) {
	q := &fakeQuerier{rows: &fakeRows{rows: rows}}
	return Target{NodeID: node, ShardID: shard, Querier: q}, q
}

// ---- 检查级测试 ----

func TestReplicaHealthyIsNoMatch(t *testing.T) {
	tgt, _ := replicaTarget("n1", "s1", [][]any{
		healthyRow("db1", "t1"),
		replicaRow("db1", "t2", "0", "0", "299", "2", "2", "0"), // 延迟低于默认阈值
	})
	report, err := CheckReplicaStatus(context.Background(), []Target{tgt}, ReplicaStatusOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != ExecutionComplete || report.Outcome != OutcomeNoMatch {
		t.Errorf("got %s/%s, want complete/no_match", report.Execution, report.Outcome)
	}
	if len(report.Matches) != 0 || len(report.NextSteps) != 0 {
		t.Errorf("健康表不应有匹配或建议: %+v / %+v", report.Matches, report.NextSteps)
	}
	if report.CheckID != replicaStatusCheckID || report.RuleVersion != replicaStatusRuleVersion {
		t.Errorf("检查身份错误: %s/%s", report.CheckID, report.RuleVersion)
	}
	if report.Targets[0].RowsRead != 2 || report.Targets[0].RowsKept != 0 {
		t.Errorf("计数错误: %+v", report.Targets[0])
	}
}

func TestReplicaReadonlyMatched(t *testing.T) {
	// is_readonly=2（显式只读模式）也判只读：非 0 即命中。
	tgt, _ := replicaTarget("n1", "s1", [][]any{
		replicaRow("db1", "t1", "2", "0", "0", "2", "2", "0"),
	})
	report, err := CheckReplicaStatus(context.Background(), []Target{tgt}, ReplicaStatusOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Outcome != OutcomeMatched {
		t.Fatalf("got %s, want matched", report.Outcome)
	}
	o := report.Matches[0].Observations[0]
	if !o.IsReadonly || o.IsSessionExpired {
		t.Errorf("观测字段错误: %+v", o)
	}
	if len(o.Reasons) != 1 || o.Reasons[0] != ReasonReplicaReadonly {
		t.Errorf("理由码错误: %+v", o.Reasons)
	}
	if !hasAdviceCode(report.NextSteps, AdviceVerifyKeeperConnectivity) {
		t.Errorf("只读应给 Keeper 连通性建议: %+v", report.NextSteps)
	}
}

func TestReplicaSessionExpiredMatched(t *testing.T) {
	tgt, _ := replicaTarget("n1", "s1", [][]any{
		replicaRow("db1", "t1", "0", "1", "0", "2", "2", "0"),
	})
	report, _ := CheckReplicaStatus(context.Background(), []Target{tgt}, ReplicaStatusOptions{})
	o := report.Matches[0].Observations[0]
	if !o.IsSessionExpired || o.IsReadonly {
		t.Errorf("观测字段错误: %+v", o)
	}
	if len(o.Reasons) != 1 || o.Reasons[0] != ReasonSessionExpired {
		t.Errorf("理由码错误: %+v", o.Reasons)
	}
}

func TestReplicaActiveDeficitMatched(t *testing.T) {
	tgt, _ := replicaTarget("n1", "s1", [][]any{
		replicaRow("db1", "t1", "0", "0", "0", "3", "1", "0"),
	})
	report, _ := CheckReplicaStatus(context.Background(), []Target{tgt}, ReplicaStatusOptions{})
	o := report.Matches[0].Observations[0]
	if o.TotalReplicas != 3 || o.ActiveReplicas != 1 {
		t.Errorf("观测计数错误: %+v", o)
	}
	if len(o.Reasons) != 1 || o.Reasons[0] != ReasonActiveReplicaDeficit {
		t.Errorf("理由码错误: %+v", o.Reasons)
	}
	if !hasAdviceCode(report.NextSteps, AdviceLocateInactiveReplica) {
		t.Errorf("缺口应给定位建议: %+v", report.NextSteps)
	}
}

func TestReplicaDelayThresholdBoundary(t *testing.T) {
	// 等于阈值命中（>= 语义），低于阈值不命中。
	tgt, _ := replicaTarget("n1", "s1", [][]any{
		replicaRow("db1", "t1", "0", "0", "600", "2", "2", "0"),
		replicaRow("db1", "t2", "0", "0", "301", "2", "2", "0"),
		replicaRow("db1", "t3", "0", "0", "299", "2", "2", "0"),
	})
	report, err := CheckReplicaStatus(context.Background(), []Target{tgt},
		ReplicaStatusOptions{DelayThresholdSeconds: 300})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if len(report.Matches) != 2 || report.Targets[0].RowsKept != 2 {
		t.Fatalf("阈值边界判定错误: %+v / %+v", report.Matches, report.Targets[0])
	}
	for _, m := range report.Matches {
		if m.Observations[0].Reasons[0] != ReasonReplicaDelayExceeded {
			t.Errorf("理由码错误: %+v", m.Observations[0].Reasons)
		}
	}
	if !hasAdviceCode(report.NextSteps, AdviceInspectReplicaDelay) {
		t.Errorf("延迟应给延迟核查建议: %+v", report.NextSteps)
	}
}

func TestReplicaLostPartsMatched(t *testing.T) {
	tgt, _ := replicaTarget("n1", "s1", [][]any{
		replicaRow("db1", "t1", "0", "0", "0", "2", "2", "3"),
	})
	report, _ := CheckReplicaStatus(context.Background(), []Target{tgt}, ReplicaStatusOptions{})
	o := report.Matches[0].Observations[0]
	if o.LostPartCount != 3 {
		t.Errorf("丢件计数错误: %+v", o)
	}
	if len(o.Reasons) != 1 || o.Reasons[0] != ReasonLostParts {
		t.Errorf("理由码错误: %+v", o.Reasons)
	}
	if !hasAdviceCode(report.NextSteps, AdviceInvestigateLostParts) {
		t.Errorf("丢件应给核查建议: %+v", report.NextSteps)
	}
}

func TestReplicaMultipleReasonsInFixedOrder(t *testing.T) {
	// 一行命中多条规则：理由码按规则声明顺序排列。
	tgt, _ := replicaTarget("n1", "s1", [][]any{
		replicaRow("db1", "t1", "1", "1", "999", "2", "0", "5"),
	})
	report, _ := CheckReplicaStatus(context.Background(), []Target{tgt}, ReplicaStatusOptions{})
	want := []string{
		ReasonReplicaReadonly, ReasonSessionExpired, ReasonActiveReplicaDeficit,
		ReasonReplicaDelayExceeded, ReasonLostParts,
	}
	got := report.Matches[0].Observations[0].Reasons
	if len(got) != len(want) {
		t.Fatalf("理由数 = %d, want %d: %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Reasons[%d] = %s, want %s", i, got[i], want[i])
		}
	}
}

func TestReplicaMergesSameShardKeepsBothViews(t *testing.T) {
	// 同分片两副本报告同一表：故障副本自身报只读，健康副本报缺口，
	// 两个视角都保留——这正是复制组故障的典型证据形态。
	a, _ := replicaTarget("n-a", "s1", [][]any{
		replicaRow("db1", "t1", "1", "0", "0", "2", "1", "0"),
	})
	b, _ := replicaTarget("n-b", "s1", [][]any{
		replicaRow("db1", "t1", "0", "0", "0", "2", "1", "0"),
	})
	report, err := CheckReplicaStatus(context.Background(), []Target{a, b}, ReplicaStatusOptions{})
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
	if m.Observations[0].Reasons[0] != ReasonReplicaReadonly ||
		m.Observations[1].Reasons[0] != ReasonActiveReplicaDeficit {
		t.Errorf("各节点视角未保留: %+v", m.Observations)
	}
}

func TestReplicaSeparatesShards(t *testing.T) {
	a, _ := replicaTarget("n1", "s1", [][]any{replicaRow("db1", "t1", "1", "0", "0", "2", "2", "0")})
	b, _ := replicaTarget("n3", "s2", [][]any{replicaRow("db1", "t1", "1", "0", "0", "2", "2", "0")})
	report, _ := CheckReplicaStatus(context.Background(), []Target{a, b}, ReplicaStatusOptions{})
	if len(report.Matches) != 2 {
		t.Fatalf("不同分片应为 2 条，got %d", len(report.Matches))
	}
}

func TestReplicaPartialFailureKeepsEvidence(t *testing.T) {
	down := Target{NodeID: "down", ShardID: "s1", Querier: &fakeQuerier{err: errors.New("connect refused")}}
	up, _ := replicaTarget("up", "s2", [][]any{replicaRow("db1", "t1", "1", "0", "0", "2", "2", "0")})
	report, err := CheckReplicaStatus(context.Background(), []Target{down, up}, ReplicaStatusOptions{})
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

func TestReplicaTruncationMarksIncomplete(t *testing.T) {
	rows := [][]any{
		replicaRow("db1", "t1", "1", "0", "0", "2", "2", "0"),
		replicaRow("db1", "t2", "1", "0", "0", "2", "2", "0"),
		replicaRow("db1", "t3", "1", "0", "0", "2", "2", "0"),
	}
	tgt, _ := replicaTarget("n1", "s1", rows)
	report, err := CheckReplicaStatus(context.Background(), []Target{tgt},
		ReplicaStatusOptions{MaxRowsPerNode: 2})
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

func TestReplicaUnparseableCountIsScanFailure(t *testing.T) {
	rows := [][]any{
		replicaRow("db1", "t1", "1", "0", "0", "2", "2", "0"),
		replicaRow("db1", "t2", "1", "0", "oops", "2", "2", "0"),
		replicaRow("db1", "t3", "1", "0", "0", "2", "2", "0"),
	}
	tgt, _ := replicaTarget("n1", "s1", rows)
	report, err := CheckReplicaStatus(context.Background(), []Target{tgt}, ReplicaStatusOptions{})
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

func TestReplicaAllFailedReturnsNoUsableObservation(t *testing.T) {
	a := Target{NodeID: "a", ShardID: "s1", Querier: &fakeQuerier{err: errors.New("boom")}}
	b := Target{NodeID: "b", ShardID: "s2", Querier: &fakeQuerier{err: errors.New("boom")}}
	report, err := CheckReplicaStatus(context.Background(), []Target{a, b}, ReplicaStatusOptions{})
	if !errors.Is(err, ErrNoUsableObservation) {
		t.Fatalf("err = %v, want ErrNoUsableObservation", err)
	}
	if report.Execution != ExecutionFailed || report.Outcome != OutcomeUnknown {
		t.Errorf("got %s/%s, want failed/unknown", report.Execution, report.Outcome)
	}
}

func TestReplicaTimeoutMarksNotStarted(t *testing.T) {
	slow := Target{NodeID: "slow", ShardID: "s1", Querier: &fakeQuerier{delay: 150 * time.Millisecond}}
	b1, _ := replicaTarget("b1", "s2", [][]any{replicaRow("db1", "t1", "1", "0", "0", "2", "2", "0")})
	report, err := CheckReplicaStatus(context.Background(), []Target{slow, b1},
		ReplicaStatusOptions{Timeout: 80 * time.Millisecond, NodeTimeout: 500 * time.Millisecond, Concurrency: 1})
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

func TestReplicaInvalidInput(t *testing.T) {
	q := &fakeQuerier{}
	cases := []struct {
		name    string
		targets []Target
		opts    ReplicaStatusOptions
	}{
		{"empty targets", nil, ReplicaStatusOptions{}},
		{"empty node id", []Target{{ShardID: "s1", Querier: q}}, ReplicaStatusOptions{}},
		{"nil querier", []Target{{NodeID: "n", ShardID: "s"}}, ReplicaStatusOptions{}},
		{"dup node id", []Target{{NodeID: "n", ShardID: "s1", Querier: q}, {NodeID: "n", ShardID: "s2", Querier: q}}, ReplicaStatusOptions{}},
		{"negative delay threshold", []Target{{NodeID: "n", ShardID: "s", Querier: q}}, ReplicaStatusOptions{DelayThresholdSeconds: -1}},
		{"negative concurrency", []Target{{NodeID: "n", ShardID: "s", Querier: q}}, ReplicaStatusOptions{Concurrency: -1}},
	}
	for _, c := range cases {
		report, err := CheckReplicaStatus(context.Background(), c.targets, c.opts)
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

func TestReplicaFixedQueryContract(t *testing.T) {
	// 固定查询的契约：system.replicas、toString 下发字符串列、按表排序。
	for _, want := range []string{
		"FROM system.replicas",
		"toString(is_readonly)", "toString(is_session_expired)",
		"toString(absolute_delay)", "toString(total_replicas)",
		"toString(active_replicas)", "toString(lost_part_count)",
	} {
		if !strings.Contains(replicaStatusSQL, want) {
			t.Errorf("固定查询缺少 %q:\n%s", want, replicaStatusSQL)
		}
	}
	if strings.Contains(replicaStatusSQL, "SELECT *") {
		t.Error("固定查询不允许 SELECT *")
	}
}

func TestReplicaMatchOrdering(t *testing.T) {
	// 不同表按字典序；同表不同库按库字典序。
	a, _ := replicaTarget("n1", "s1", [][]any{
		replicaRow("db2", "t1", "1", "0", "0", "2", "2", "0"),
		replicaRow("db1", "t2", "1", "0", "0", "2", "2", "0"),
		replicaRow("db1", "t1", "1", "0", "0", "2", "2", "0"),
	})
	report, _ := CheckReplicaStatus(context.Background(), []Target{a}, ReplicaStatusOptions{})
	want := [][2]string{{"db1", "t1"}, {"db1", "t2"}, {"db2", "t1"}}
	if len(report.Matches) != 3 {
		t.Fatalf("应有 3 条，got %d", len(report.Matches))
	}
	for i, w := range want {
		if report.Matches[i].Database != w[0] || report.Matches[i].Table != w[1] {
			t.Errorf("Matches[%d] = %s/%s, want %s/%s", i,
				report.Matches[i].Database, report.Matches[i].Table, w[0], w[1])
		}
	}
}
