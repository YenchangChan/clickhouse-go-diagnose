package diagnose

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// ---- 测试辅助 ----

// backlogRow 形状对齐 queueBacklogSQL 的列：
// db, table, queue_depth, oldest_age, max_tries, failing_entries。
func backlogRow(db, table, depth, age, tries, failing string) []any {
	return []any{db, table, depth, age, tries, failing}
}

func backlogTarget(node, shard string, rows [][]any) (Target, *fakeQuerier) {
	q := &fakeQuerier{rows: &fakeRows{rows: rows}}
	return Target{NodeID: node, ShardID: shard, Querier: q}, q
}

// ---- 检查级测试 ----

func TestBacklogDepthMatched(t *testing.T) {
	tgt, _ := backlogTarget("n1", "s1", [][]any{
		backlogRow("db1", "t1", "150", "30", "1", "0"),
	})
	report, err := CheckReplicationQueueBacklog(context.Background(), []Target{tgt}, QueueBacklogOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != ExecutionComplete || report.Outcome != OutcomeMatched {
		t.Errorf("got %s/%s, want complete/matched", report.Execution, report.Outcome)
	}
	if report.CheckID != queueBacklogCheckID || report.RuleVersion != queueBacklogRuleVersion {
		t.Errorf("检查身份错误: %s/%s", report.CheckID, report.RuleVersion)
	}
	m := report.Matches[0]
	if m.Database != "db1" || m.Table != "t1" || m.Depth != 150 {
		t.Errorf("匹配对象错误: %+v", m)
	}
	o := m.Observations[0]
	if len(o.Reasons) != 1 || o.Reasons[0] != ReasonQueueDepth {
		t.Errorf("理由码错误: %+v", o.Reasons)
	}
	if o.FailingEntries != 0 {
		t.Errorf("背景证据未保留: %+v", o)
	}
	for _, want := range []string{AdviceInspectQueueOnSourceNodes, AdviceVerifyKeeperConnectivity, AdviceInspectReplicaDelay} {
		if !hasAdviceCode(report.NextSteps, want) {
			t.Errorf("缺少积压核查建议 %s: %+v", want, report.NextSteps)
		}
	}
}

func TestBacklogOldestAgeMatched(t *testing.T) {
	// 深度小但最老条目超龄：消费受阻而非瞬时写入。
	tgt, _ := backlogTarget("n1", "s1", [][]any{
		backlogRow("db1", "t1", "2", "7200", "0", "0"),
	})
	report, _ := CheckReplicationQueueBacklog(context.Background(), []Target{tgt}, QueueBacklogOptions{})
	if report.Outcome != OutcomeMatched {
		t.Fatalf("got %s, want matched", report.Outcome)
	}
	o := report.Matches[0].Observations[0]
	if len(o.Reasons) != 1 || o.Reasons[0] != ReasonQueueOldestEntry {
		t.Errorf("理由码错误: %+v", o.Reasons)
	}
	if report.Matches[0].OldestAgeSeconds != 7200 {
		t.Errorf("年龄未保留: %+v", report.Matches[0])
	}
}

func TestBacklogRetriesMatched(t *testing.T) {
	// 重试次数超阈值：条目反复失败。
	tgt, _ := backlogTarget("n1", "s1", [][]any{
		backlogRow("db1", "t1", "3", "600", "15", "1"),
	})
	report, _ := CheckReplicationQueueBacklog(context.Background(), []Target{tgt}, QueueBacklogOptions{})
	if report.Outcome != OutcomeMatched {
		t.Fatalf("got %s, want matched", report.Outcome)
	}
	o := report.Matches[0].Observations[0]
	if len(o.Reasons) != 1 || o.Reasons[0] != ReasonQueueMaxRetries {
		t.Errorf("理由码错误: %+v", o.Reasons)
	}
	if o.MaxRetries != 15 || o.FailingEntries != 1 {
		t.Errorf("观测字段错误: %+v", o)
	}
}

func TestBacklogDefaultThresholdsBoundary(t *testing.T) {
	// 默认阈值边界（>= 语义）：depth 100 / age 3600 / tries 10 命中，
	// 99 / 3599 / 9 不命中。
	thresholds := [][]any{
		backlogRow("db1", "t_at_all", "100", "3600", "10", "0"),
		backlogRow("db1", "t_below", "99", "3599", "9", "0"),
	}
	tgt, _ := backlogTarget("n1", "s1", thresholds)
	report, _ := CheckReplicationQueueBacklog(context.Background(), []Target{tgt}, QueueBacklogOptions{})
	if len(report.Matches) != 1 || report.Matches[0].Table != "t_at_all" {
		t.Fatalf("默认阈值边界判定错误: %+v", report.Matches)
	}
	o := report.Matches[0].Observations[0]
	want := []string{ReasonQueueDepth, ReasonQueueOldestEntry, ReasonQueueMaxRetries}
	if len(o.Reasons) != len(want) {
		t.Fatalf("理由数 = %d, want %d: %+v", len(o.Reasons), len(want), o.Reasons)
	}
	for i := range want {
		if o.Reasons[i] != want[i] {
			t.Errorf("Reasons[%d] = %s, want %s（理由码按声明顺序）", i, o.Reasons[i], want[i])
		}
	}
}

func TestBacklogCustomThresholds(t *testing.T) {
	// 自定义阈值收紧判定：深度 5、阈值 3 即命中。
	tgt, _ := backlogTarget("n1", "s1", [][]any{
		backlogRow("db1", "t1", "5", "10", "0", "0"),
	})
	report, _ := CheckReplicationQueueBacklog(context.Background(), []Target{tgt},
		QueueBacklogOptions{DepthThreshold: 3, AgeThresholdSeconds: 100, RetriesThreshold: 5})
	if report.Outcome != OutcomeMatched || report.Matches[0].Depth != 5 {
		t.Fatalf("自定义阈值判定错误: %+v", report.Matches)
	}
	o := report.Matches[0].Observations[0]
	if len(o.Reasons) != 1 || o.Reasons[0] != ReasonQueueDepth {
		t.Errorf("理由码错误: %+v", o.Reasons)
	}
}

func TestBacklogBelowAllThresholdsNoMatch(t *testing.T) {
	// 全部低于阈值：complete/no_match，无建议。
	tgt, _ := backlogTarget("n1", "s1", [][]any{
		backlogRow("db1", "t1", "3", "120", "1", "0"),
		backlogRow("db1", "t2", "1", "5", "0", "0"),
	})
	report, err := CheckReplicationQueueBacklog(context.Background(), []Target{tgt}, QueueBacklogOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != ExecutionComplete || report.Outcome != OutcomeNoMatch {
		t.Errorf("got %s/%s, want complete/no_match", report.Execution, report.Outcome)
	}
	if len(report.Matches) != 0 || len(report.NextSteps) != 0 {
		t.Errorf("无匹配不应有建议: %+v / %+v", report.Matches, report.NextSteps)
	}
	if report.Targets[0].RowsRead != 2 || report.Targets[0].RowsKept != 0 {
		t.Errorf("低于阈值的聚合行应计入 RowsRead: %+v", report.Targets[0])
	}
}

func TestBacklogEmptyQueueNoMatch(t *testing.T) {
	// 空队列（无聚合行）：complete/no_match。
	tgt, _ := backlogTarget("n1", "s1", nil)
	report, err := CheckReplicationQueueBacklog(context.Background(), []Target{tgt}, QueueBacklogOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != ExecutionComplete || report.Outcome != OutcomeNoMatch {
		t.Errorf("got %s/%s, want complete/no_match", report.Execution, report.Outcome)
	}
	if report.Targets[0].RowsRead != 0 {
		t.Errorf("空队列 RowsRead 应为 0: %+v", report.Targets[0])
	}
}

func TestBacklogMergeTakesMaxAcrossReplicas(t *testing.T) {
	// 同分片两副本同一复制表：各副本队列独立，观测都保留；
	// 深度/年龄/重试取各节点最大值。
	a, _ := backlogTarget("n-a", "s1", [][]any{
		backlogRow("db1", "t1", "120", "100", "2", "0"),
	})
	b, _ := backlogTarget("n-b", "s1", [][]any{
		backlogRow("db1", "t1", "5", "5000", "12", "3"),
	})
	report, err := CheckReplicationQueueBacklog(context.Background(), []Target{a, b}, QueueBacklogOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if len(report.Matches) != 1 {
		t.Fatalf("应归并为 1 条，got %d: %+v", len(report.Matches), report.Matches)
	}
	m := report.Matches[0]
	if m.Depth != 120 || m.OldestAgeSeconds != 5000 || m.MaxRetries != 12 {
		t.Errorf("跨副本最大值错误: %+v", m)
	}
	if len(m.Observations) != 2 || m.Observations[0].NodeID != "n-a" || m.Observations[1].NodeID != "n-b" {
		t.Errorf("来源观测错误: %+v", m.Observations)
	}
}

func TestBacklogSeparatesShardsAndTables(t *testing.T) {
	a, _ := backlogTarget("n1", "s1", [][]any{backlogRow("db1", "t1", "200", "1", "0", "0")})
	b, _ := backlogTarget("n3", "s2", [][]any{backlogRow("db1", "t1", "200", "1", "0", "0")})
	c, _ := backlogTarget("n4", "s1", [][]any{backlogRow("db1", "t2", "200", "1", "0", "0")})
	report, _ := CheckReplicationQueueBacklog(context.Background(), []Target{a, b, c}, QueueBacklogOptions{})
	if len(report.Matches) != 3 {
		t.Fatalf("不同分片/表应为 3 条，got %d", len(report.Matches))
	}
}

func TestBacklogUnparseableCountIsScanFailure(t *testing.T) {
	rows := [][]any{
		backlogRow("db1", "t1", "200", "10", "0", "0"),
		backlogRow("db1", "t2", "1", "not-a-number", "0", "0"),
	}
	tgt, _ := backlogTarget("n1", "s1", rows)
	report, _ := CheckReplicationQueueBacklog(context.Background(), []Target{tgt}, QueueBacklogOptions{})
	nr := report.Targets[0]
	if nr.Status != NodeFailed || nr.ErrCategory != ErrCatScanFailed {
		t.Errorf("got %+v, want scan_failed", nr)
	}
	if nr.RowsRead != 1 || nr.RowsKept != 1 || len(report.Matches) != 1 {
		t.Errorf("证据保留错误: %+v / %+v", nr, report.Matches)
	}
}

func TestBacklogPartialFailureKeepsEvidence(t *testing.T) {
	down := Target{NodeID: "down", ShardID: "s1", Querier: &fakeQuerier{err: errors.New("connect refused")}}
	up, _ := backlogTarget("up", "s2", [][]any{backlogRow("db1", "t1", "500", "10", "0", "0")})
	report, err := CheckReplicationQueueBacklog(context.Background(), []Target{down, up}, QueueBacklogOptions{})
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

func TestBacklogTruncationMarksIncomplete(t *testing.T) {
	rows := [][]any{
		backlogRow("db1", "t1", "200", "10", "0", "0"),
		backlogRow("db1", "t2", "200", "10", "0", "0"),
		backlogRow("db1", "t3", "200", "10", "0", "0"),
	}
	tgt, _ := backlogTarget("n1", "s1", rows)
	report, err := CheckReplicationQueueBacklog(context.Background(), []Target{tgt},
		QueueBacklogOptions{MaxRowsPerNode: 2})
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

func TestBacklogAllFailedReturnsNoUsableObservation(t *testing.T) {
	a := Target{NodeID: "a", ShardID: "s1", Querier: &fakeQuerier{err: errors.New("boom")}}
	b := Target{NodeID: "b", ShardID: "s2", Querier: &fakeQuerier{err: errors.New("boom")}}
	report, err := CheckReplicationQueueBacklog(context.Background(), []Target{a, b}, QueueBacklogOptions{})
	if !errors.Is(err, ErrNoUsableObservation) {
		t.Fatalf("err = %v, want ErrNoUsableObservation", err)
	}
	if report.Execution != ExecutionFailed || report.Outcome != OutcomeUnknown {
		t.Errorf("got %s/%s, want failed/unknown", report.Execution, report.Outcome)
	}
}

func TestBacklogTimeoutMarksNotStarted(t *testing.T) {
	slow := Target{NodeID: "slow", ShardID: "s1", Querier: &fakeQuerier{delay: 150 * time.Millisecond}}
	b1, _ := backlogTarget("b1", "s2", [][]any{backlogRow("db1", "t1", "200", "10", "0", "0")})
	report, err := CheckReplicationQueueBacklog(context.Background(), []Target{slow, b1},
		QueueBacklogOptions{Timeout: 80 * time.Millisecond, NodeTimeout: 500 * time.Millisecond, Concurrency: 1})
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

func TestBacklogInvalidInput(t *testing.T) {
	q := &fakeQuerier{}
	cases := []struct {
		name    string
		targets []Target
		opts    QueueBacklogOptions
	}{
		{"empty targets", nil, QueueBacklogOptions{}},
		{"empty node id", []Target{{ShardID: "s1", Querier: q}}, QueueBacklogOptions{}},
		{"nil querier", []Target{{NodeID: "n", ShardID: "s"}}, QueueBacklogOptions{}},
		{"dup node id", []Target{{NodeID: "n", ShardID: "s1", Querier: q}, {NodeID: "n", ShardID: "s2", Querier: q}}, QueueBacklogOptions{}},
		{"negative depth threshold", []Target{{NodeID: "n", ShardID: "s", Querier: q}}, QueueBacklogOptions{DepthThreshold: -1}},
		{"negative age threshold", []Target{{NodeID: "n", ShardID: "s", Querier: q}}, QueueBacklogOptions{AgeThresholdSeconds: -1}},
		{"negative retries threshold", []Target{{NodeID: "n", ShardID: "s", Querier: q}}, QueueBacklogOptions{RetriesThreshold: -1}},
		{"negative timeout", []Target{{NodeID: "n", ShardID: "s", Querier: q}}, QueueBacklogOptions{Timeout: -1}},
	}
	for _, c := range cases {
		report, err := CheckReplicationQueueBacklog(context.Background(), c.targets, c.opts)
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

func TestBacklogFixedQueryContract(t *testing.T) {
	// 固定查询的契约：system.replication_queue、按 (库,表) 服务端聚合、
	// 服务端算最老条目年龄、toString 下发字符串列、countIf 背景证据、
	// 无 SELECT *。
	for _, want := range []string{
		"FROM system.replication_queue",
		"GROUP BY database, table",
		"toString(count())", "dateDiff('second', min(create_time), now())",
		"max(num_tries)", "countIf(last_exception != '')",
	} {
		if !strings.Contains(queueBacklogSQL, want) {
			t.Errorf("固定查询缺少 %q:\n%s", want, queueBacklogSQL)
		}
	}
	if strings.Contains(queueBacklogSQL, "SELECT *") {
		t.Error("固定查询不允许 SELECT *")
	}
	if !strings.Contains(queueBacklogSQL, "ORDER BY database, table") {
		t.Error("固定查询必须按 (库,表) 排序以保证截断保留确定前缀")
	}
}

func TestBacklogMatchOrdering(t *testing.T) {
	// 按 分片/库/表 字典序。
	a, _ := backlogTarget("n1", "s1", [][]any{
		backlogRow("db2", "t1", "200", "10", "0", "0"),
		backlogRow("db1", "t2", "200", "10", "0", "0"),
		backlogRow("db1", "t1", "200", "10", "0", "0"),
	})
	report, _ := CheckReplicationQueueBacklog(context.Background(), []Target{a}, QueueBacklogOptions{})
	want := [][2]string{{"db1", "t1"}, {"db1", "t2"}, {"db2", "t1"}}
	if len(report.Matches) != 3 {
		t.Fatalf("应有 3 条，got %d", len(report.Matches))
	}
	for i, w := range want {
		m := report.Matches[i]
		if m.Database != w[0] || m.Table != w[1] {
			t.Errorf("Matches[%d] = %s/%s, want %s/%s", i, m.Database, m.Table, w[0], w[1])
		}
	}
}
