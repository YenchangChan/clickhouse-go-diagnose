package diagnose

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

// ---- 测试适配器 ----

// fakeRows 按 plan 中的约定模拟 Rows：Scan/Err/Close 可注入失败。
type fakeRows struct {
	rows      [][]any
	i         int
	scanErrAt int   // 1-based：消费到该行时 Scan 失败
	err       error // Err() 返回值
	closeErr  error // Close() 返回值
}

func (r *fakeRows) Next() bool { return r.i < len(r.rows) }

func (r *fakeRows) Scan(dest ...any) error {
	if r.scanErrAt > 0 && r.i+1 == r.scanErrAt {
		r.i++
		return errors.New("scan boom")
	}
	if r.i >= len(r.rows) {
		return errors.New("scan after end")
	}
	row := r.rows[r.i]
	r.i++
	for j, d := range dest {
		// 与核心库一致：Scan 目标是 *sql.NullString。
		p, ok := d.(*sql.NullString)
		if !ok {
			return fmt.Errorf("fakeRows: unsupported dest %T", d)
		}
		if row[j] == nil {
			*p = sql.NullString{}
		} else {
			*p = sql.NullString{Valid: true, String: row[j].(string)}
		}
	}
	return nil
}

func (r *fakeRows) Err() error   { return r.err }
func (r *fakeRows) Close() error { return r.closeErr }

// fakeQuerier 可注入延迟（遵守 context）与 Query 失败。
type fakeQuerier struct {
	rows    Rows
	err     error
	delay   time.Duration
	queries int
}

func (q *fakeQuerier) Query(ctx context.Context, _ string, _ ...any) (Rows, error) {
	q.queries++
	if q.delay > 0 {
		select {
		case <-time.After(q.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return q.rows, q.err
}

// matchRow 构造一条命中 NO_REPLICA_HAS_PART 的队列记录。
func matchRow(db, table, part string) []any {
	return []any{db, table, part, "Code: 262. DB::Exception: No replica has part covering " + part + " (NO_REPLICA_HAS_PART)"}
}

func plainRow(db, table, part string) []any {
	return []any{db, table, part, "Table is in readonly mode"}
}

func target(node, shard string, rows [][]any) (Target, *fakeQuerier) {
	q := &fakeQuerier{rows: &fakeRows{rows: rows}}
	return Target{NodeID: node, ShardID: shard, Querier: q}, q
}

// ---- 检查级测试（对应 implementation-plan.md 7.2 验收集合）----

func TestCheckCompleteNoMatch(t *testing.T) {
	tgt, _ := target("n1", "s1", [][]any{plainRow("db1", "t1", "p1")})
	report, err := CheckReplicationQueueFailures(context.Background(), []Target{tgt}, ReplicationQueueOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != ExecutionComplete || report.Outcome != OutcomeNoMatch {
		t.Errorf("got %s/%s, want complete/no_match", report.Execution, report.Outcome)
	}
	if report.Targets[0].Status != NodeOK || report.Targets[0].RowsRead != 1 {
		t.Errorf("节点结果错误: %+v", report.Targets[0])
	}
	if len(report.NextSteps) != 0 {
		t.Errorf("complete + no_match 不应有建议: %+v", report.NextSteps)
	}
	if report.CheckID != replicationQueueCheckID || report.RuleVersion != replicationQueueRuleVersion {
		t.Errorf("检查身份错误: %s/%s", report.CheckID, report.RuleVersion)
	}
}

func TestCheckMergesSameShardReplicas(t *testing.T) {
	// 同分片两个副本报同一 part：合并为一条，保留两个来源。
	a, _ := target("n-a", "s1", [][]any{matchRow("db1", "t1", "p3")})
	b, _ := target("n-b", "s1", [][]any{matchRow("db1", "t1", "p3")})
	report, err := CheckReplicationQueueFailures(context.Background(), []Target{a, b}, ReplicationQueueOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != ExecutionComplete || report.Outcome != OutcomeMatched {
		t.Errorf("got %s/%s, want complete/matched", report.Execution, report.Outcome)
	}
	if len(report.Matches) != 1 {
		t.Fatalf("应归并为 1 条，got %d: %+v", len(report.Matches), report.Matches)
	}
	if len(report.Matches[0].Observations) != 2 {
		t.Errorf("应保留两个来源观测: %+v", report.Matches[0].Observations)
	}
}

func TestCheckSeparatesShards(t *testing.T) {
	// 不同分片同名 part 不能合并。
	a, _ := target("n1", "s1", [][]any{matchRow("db1", "t1", "p3")})
	b, _ := target("n3", "s2", [][]any{matchRow("db1", "t1", "p3")})
	report, _ := CheckReplicationQueueFailures(context.Background(), []Target{a, b}, ReplicationQueueOptions{})
	if len(report.Matches) != 2 {
		t.Fatalf("不同分片应为 2 条，got %d", len(report.Matches))
	}
	if report.Matches[0].ShardID == report.Matches[1].ShardID {
		t.Errorf("未按分片区分: %+v", report.Matches)
	}
}

func TestCheckPartialFailureKeepsEvidence(t *testing.T) {
	// 部分节点失败：可达节点的证据必须保留，且明确不完整。
	down := Target{NodeID: "down", ShardID: "s1", Querier: &fakeQuerier{err: errors.New("connect refused")}}
	up, _ := target("up", "s2", [][]any{matchRow("db1", "t1", "p3")})
	report, err := CheckReplicationQueueFailures(context.Background(), []Target{down, up}, ReplicationQueueOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil（部分失败不应作为顶层错误丢弃证据）", err)
	}
	if report.Execution != ExecutionPartial || report.Outcome != OutcomeMatched {
		t.Errorf("got %s/%s, want partial/matched", report.Execution, report.Outcome)
	}
	if len(report.Matches) != 1 || len(report.Matches[0].Observations) != 1 {
		t.Errorf("证据丢失: %+v", report.Matches)
	}
	for _, r := range report.Targets {
		if r.NodeID == "down" && (r.Status != NodeFailed || r.ErrCategory != ErrCatQueryFailed) {
			t.Errorf("失败节点分类错误: %+v", r)
		}
	}
	if !hasAdviceCode(report.NextSteps, AdviceCompleteNodeCoverage) {
		t.Errorf("部分失败应提示补查覆盖: %+v", report.NextSteps)
	}
}

func TestCheckPartialNoMatchIsUnknown(t *testing.T) {
	down := Target{NodeID: "down", ShardID: "s1", Querier: &fakeQuerier{err: errors.New("connect refused")}}
	up, _ := target("up", "s2", [][]any{plainRow("db1", "t1", "p1")})
	report, err := CheckReplicationQueueFailures(context.Background(), []Target{down, up}, ReplicationQueueOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != ExecutionPartial || report.Outcome != OutcomeUnknown {
		t.Errorf("got %s/%s, want partial/unknown（部分覆盖不能输出 no_match）", report.Execution, report.Outcome)
	}
}

func TestCheckAllFailedReturnsNoUsableObservation(t *testing.T) {
	a := Target{NodeID: "a", ShardID: "s1", Querier: &fakeQuerier{err: errors.New("boom")}}
	b := Target{NodeID: "b", ShardID: "s2", Querier: &fakeQuerier{err: errors.New("boom")}}
	report, err := CheckReplicationQueueFailures(context.Background(), []Target{a, b}, ReplicationQueueOptions{})
	if !errors.Is(err, ErrNoUsableObservation) {
		t.Fatalf("err = %v, want ErrNoUsableObservation", err)
	}
	if report.Execution != ExecutionFailed || report.Outcome != OutcomeUnknown {
		t.Errorf("got %s/%s, want failed/unknown", report.Execution, report.Outcome)
	}
}

func TestCheckTruncationMarksIncomplete(t *testing.T) {
	// 读到第 N+1 条即停止并标记 Truncated，保留前 N 条。
	rows := [][]any{matchRow("db1", "t1", "p1"), matchRow("db1", "t1", "p2"), matchRow("db1", "t1", "p3")}
	tgt, _ := target("n1", "s1", rows)
	report, err := CheckReplicationQueueFailures(context.Background(), []Target{tgt},
		ReplicationQueueOptions{MaxRowsPerNode: 2})
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
	if len(report.Matches) != 2 {
		t.Errorf("应只保留前 2 条: %+v", report.Matches)
	}
	if !hasAdviceCode(report.NextSteps, AdviceRerunWithHigherRowLimit) {
		t.Errorf("截断应提示复跑: %+v", report.NextSteps)
	}
}

func TestCheckScanErrorKeepsPriorRows(t *testing.T) {
	// 先读到有效行再 Scan 失败：证据保留，节点不算成功。
	rows := [][]any{matchRow("db1", "t1", "p1"), matchRow("db1", "t1", "p2"), matchRow("db1", "t1", "p3")}
	fr := &fakeRows{rows: rows, scanErrAt: 2}
	tgt := Target{NodeID: "n1", ShardID: "s1", Querier: &fakeQuerier{rows: fr}}
	report, err := CheckReplicationQueueFailures(context.Background(), []Target{tgt}, ReplicationQueueOptions{})
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

func TestCheckIterationError(t *testing.T) {
	// 迭代结束后 Rows.Err 非空：不能算完整成功。
	fr := &fakeRows{rows: [][]any{matchRow("db1", "t1", "p1")}, err: errors.New("connection lost mid-iteration")}
	tgt := Target{NodeID: "n1", ShardID: "s1", Querier: &fakeQuerier{rows: fr}}
	report, _ := CheckReplicationQueueFailures(context.Background(), []Target{tgt}, ReplicationQueueOptions{})
	nr := report.Targets[0]
	if nr.Status != NodeFailed || nr.ErrCategory != ErrCatIterationFailed {
		t.Errorf("got %+v, want iteration_failed", nr)
	}
	if nr.RowsKept != 1 || len(report.Matches) != 1 {
		t.Errorf("有效证据未保留: %+v", nr)
	}
	if report.Execution != ExecutionPartial {
		t.Errorf("execution = %s, want partial", report.Execution)
	}
}

func TestCheckCloseError(t *testing.T) {
	// 关闭错误不能静默丢弃。
	fr := &fakeRows{rows: [][]any{matchRow("db1", "t1", "p1")}, closeErr: errors.New("close boom")}
	tgt := Target{NodeID: "n1", ShardID: "s1", Querier: &fakeQuerier{rows: fr}}
	report, _ := CheckReplicationQueueFailures(context.Background(), []Target{tgt}, ReplicationQueueOptions{})
	nr := report.Targets[0]
	if nr.Status != NodeFailed || nr.ErrCategory != ErrCatCloseFailed {
		t.Errorf("got %+v, want close_failed", nr)
	}
}

func TestCheckTimeoutMarksNotStarted(t *testing.T) {
	// 总预算耗尽：已调度节点标 timeout，未调度节点保留 not_started，
	// 顶层返回 context 错误，但不产生 ErrNoUsableObservation 以外的正常结果。
	slow := Target{NodeID: "slow", ShardID: "s1", Querier: &fakeQuerier{delay: 150 * time.Millisecond}}
	b1, _ := target("b1", "s2", [][]any{matchRow("db1", "t1", "p1")})
	b2, _ := target("b2", "s2", [][]any{matchRow("db1", "t1", "p2")})
	report, err := CheckReplicationQueueFailures(context.Background(), []Target{slow, b1, b2},
		ReplicationQueueOptions{Timeout: 80 * time.Millisecond, NodeTimeout: 500 * time.Millisecond, Concurrency: 1})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if report.Execution != ExecutionFailed {
		t.Errorf("execution = %s, want failed（无任何可用观测）", report.Execution)
	}
	if report.Targets[0].Status != NodeFailed || report.Targets[0].ErrCategory != ErrCatTimeout {
		t.Errorf("慢节点分类错误: %+v", report.Targets[0])
	}
	for _, r := range report.Targets[1:] {
		if r.Status != NodeNotStarted || r.ErrCategory != ErrCatNotStarted {
			t.Errorf("未启动节点记录错误: %+v", r)
		}
		if !r.StartedAt.IsZero() {
			t.Errorf("未启动节点不应有时间戳: %+v", r)
		}
	}
}

func TestCheckPreservesTargetOrder(t *testing.T) {
	// 报告中目标顺序与输入一致，不受并发调度影响。
	a, _ := target("alpha", "s1", [][]any{matchRow("db1", "t1", "p1")})
	b, _ := target("beta", "s2", [][]any{matchRow("db1", "t1", "p2")})
	c, _ := target("gamma", "s3", [][]any{matchRow("db1", "t1", "p3")})
	report, _ := CheckReplicationQueueFailures(context.Background(), []Target{a, b, c}, ReplicationQueueOptions{})
	want := []string{"alpha", "beta", "gamma"}
	for i, w := range want {
		if report.Targets[i].NodeID != w {
			t.Errorf("Targets[%d] = %s, want %s", i, report.Targets[i].NodeID, w)
		}
	}
}

func TestCheckInvalidInput(t *testing.T) {
	q := &fakeQuerier{}
	cases := []struct {
		name    string
		targets []Target
		opts    ReplicationQueueOptions
	}{
		{"empty targets", nil, ReplicationQueueOptions{}},
		{"empty node id", []Target{{ShardID: "s1", Querier: q}}, ReplicationQueueOptions{}},
		{"empty shard id", []Target{{NodeID: "n", Querier: q}}, ReplicationQueueOptions{}},
		{"nil querier", []Target{{NodeID: "n", ShardID: "s"}}, ReplicationQueueOptions{}},
		{"dup node id", []Target{{NodeID: "n", ShardID: "s1", Querier: q}, {NodeID: "n", ShardID: "s2", Querier: q}}, ReplicationQueueOptions{}},
		{"negative timeout", []Target{{NodeID: "n", ShardID: "s", Querier: q}}, ReplicationQueueOptions{Timeout: -1}},
	}
	for _, c := range cases {
		report, err := CheckReplicationQueueFailures(context.Background(), c.targets, c.opts)
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

func TestFixedQueryContract(t *testing.T) {
	// 固定查询的契约：目标表、四个必需列、异常特征、无 SELECT *。
	for _, want := range []string{"system.replication_queue", "database", "table", "new_part_name", "last_exception", exceptionMarker} {
		if !strings.Contains(replicationQueueSQL, want) {
			t.Errorf("固定查询缺少 %q:\n%s", want, replicationQueueSQL)
		}
	}
	if strings.Contains(replicationQueueSQL, "SELECT *") {
		t.Error("固定查询不允许 SELECT *")
	}
}

func hasAdviceCode(steps []FollowUp, code string) bool {
	for _, s := range steps {
		if s.Code == code {
			return true
		}
	}
	return false
}
