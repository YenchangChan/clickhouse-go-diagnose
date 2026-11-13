package diagnose

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// ---- 测试辅助 ----

// diskRow 形状对齐 diskSpaceSQL 的列：
// name, path, free_bytes, total_bytes, keep_free_bytes（后三列是十进制字符串）。
func diskRow(name, path, free, total, keep string) []any {
	return []any{name, path, free, total, keep}
}

func diskTarget(node, shard string, rows [][]any) (Target, *fakeQuerier) {
	q := &fakeQuerier{rows: &fakeRows{rows: rows}}
	return Target{NodeID: node, ShardID: shard, Querier: q}, q
}

// ---- 检查级测试 ----

func TestDiskSpaceLowMatched(t *testing.T) {
	// 100GB 中剩 5GB（5%）→ 水位理由命中。
	tgt, _ := diskTarget("n1", "s1", [][]any{
		diskRow("default", "/var/lib/clickhouse/", "5000000000", "100000000000", "0"),
	})
	report, err := CheckDiskSpaceLow(context.Background(), []Target{tgt}, DiskSpaceOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != ExecutionComplete || report.Outcome != OutcomeMatched {
		t.Errorf("got %s/%s, want complete/matched", report.Execution, report.Outcome)
	}
	if report.CheckID != diskSpaceCheckID || report.RuleVersion != diskSpaceRuleVersion {
		t.Errorf("检查身份错误: %s/%s", report.CheckID, report.RuleVersion)
	}
	m := report.Matches[0]
	if m.DiskName != "default" || m.Path != "/var/lib/clickhouse/" || m.FreePercent != 5 {
		t.Errorf("匹配对象错误: %+v", m)
	}
	o := m.Observations[0]
	if len(o.Reasons) != 1 || o.Reasons[0] != ReasonDiskSpaceLow {
		t.Errorf("理由码错误: %+v", o.Reasons)
	}
	if o.FreeBytes != 5000000000 || o.TotalBytes != 100000000000 {
		t.Errorf("容量未保留: %+v", o)
	}
	for _, want := range []string{AdviceInspectDiskUsage, AdviceReviewTTLRetention} {
		if !hasAdviceCode(report.NextSteps, want) {
			t.Errorf("缺少水位核查建议 %s: %+v", want, report.NextSteps)
		}
	}
}

func TestDiskReserveBreached(t *testing.T) {
	// 占比尚有 50%，但保留量 60GB 已被击穿：只命中保留量理由。
	tgt, _ := diskTarget("n1", "s1", [][]any{
		diskRow("default", "/data/", "50000000000", "100000000000", "60000000000"),
	})
	report, _ := CheckDiskSpaceLow(context.Background(), []Target{tgt}, DiskSpaceOptions{})
	if report.Outcome != OutcomeMatched {
		t.Fatalf("got %s, want matched", report.Outcome)
	}
	o := report.Matches[0].Observations[0]
	if len(o.Reasons) != 1 || o.Reasons[0] != ReasonDiskReserveBreached {
		t.Errorf("理由码错误: %+v", o.Reasons)
	}
	if o.KeepFreeBytes != 60000000000 {
		t.Errorf("保留量未保留: %+v", o)
	}
}

func TestDiskBothReasonsOrder(t *testing.T) {
	// 占比 3% 且保留量击穿：两条理由按声明顺序。
	tgt, _ := diskTarget("n1", "s1", [][]any{
		diskRow("default", "/data/", "3000000000", "100000000000", "5000000000"),
	})
	report, _ := CheckDiskSpaceLow(context.Background(), []Target{tgt}, DiskSpaceOptions{})
	o := report.Matches[0].Observations[0]
	want := []string{ReasonDiskSpaceLow, ReasonDiskReserveBreached}
	if len(o.Reasons) != len(want) {
		t.Fatalf("理由数 = %d, want %d: %+v", len(o.Reasons), len(want), o.Reasons)
	}
	for i := range want {
		if o.Reasons[i] != want[i] {
			t.Errorf("Reasons[%d] = %s, want %s", i, o.Reasons[i], want[i])
		}
	}
}

func TestDiskPercentBoundary(t *testing.T) {
	// 默认阈值 10：恰好 10% 命中（<= 语义），11% 不命中。
	tgt, _ := diskTarget("n1", "s1", [][]any{
		diskRow("at", "/p1", "10000000000", "100000000000", "0"),    // 10%
		diskRow("above", "/p2", "11000000000", "100000000000", "0"), // 11%
	})
	report, _ := CheckDiskSpaceLow(context.Background(), []Target{tgt}, DiskSpaceOptions{})
	if len(report.Matches) != 1 || report.Matches[0].DiskName != "at" {
		t.Fatalf("阈值边界判定错误: %+v", report.Matches)
	}
}

func TestDiskZeroTotalSkipsPercentRule(t *testing.T) {
	// total_space=0：占比不可计算（-1），不命中水位规则；保留量击穿仍可命中。
	tgt, _ := diskTarget("n1", "s1", [][]any{
		diskRow("zero-no-keep", "/z1", "1000", "0", "0"),
		diskRow("zero-keep", "/z2", "100", "0", "500"),
	})
	report, _ := CheckDiskSpaceLow(context.Background(), []Target{tgt}, DiskSpaceOptions{})
	if len(report.Matches) != 1 || report.Matches[0].DiskName != "zero-keep" {
		t.Fatalf("total=0 判定错误: %+v", report.Matches)
	}
	m := report.Matches[0]
	if m.FreePercent != -1 {
		t.Errorf("不可计算占比应为 -1: %+v", m)
	}
	o := m.Observations[0]
	if len(o.Reasons) != 1 || o.Reasons[0] != ReasonDiskReserveBreached {
		t.Errorf("理由码错误: %+v", o.Reasons)
	}
}

func TestDiskNoMatch(t *testing.T) {
	tgt, _ := diskTarget("n1", "s1", [][]any{
		diskRow("default", "/data/", "50000000000", "100000000000", "0"),
		diskRow("backup", "/backup/", "90000000000", "100000000000", "0"),
	})
	report, err := CheckDiskSpaceLow(context.Background(), []Target{tgt}, DiskSpaceOptions{})
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
		t.Errorf("健康磁盘应计入 RowsRead: %+v", report.Targets[0])
	}
}

func TestDiskMergeSameShardTakesWorst(t *testing.T) {
	// 同分片两节点的同名同路径磁盘：水位取最差（最小），来源都保留。
	// 只有命中理由的观测会进入报告：健康节点不产生观测（与 ReplicaStatus
	// 一致），因此两个节点都构造为低水位。
	a, _ := diskTarget("n-a", "s1", [][]any{
		diskRow("default", "/data/", "9000000000", "100000000000", "0"), // 9%
	})
	b, _ := diskTarget("n-b", "s1", [][]any{
		diskRow("default", "/data/", "6000000000", "100000000000", "0"), // 6%
	})
	report, err := CheckDiskSpaceLow(context.Background(), []Target{a, b}, DiskSpaceOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if len(report.Matches) != 1 {
		t.Fatalf("应归并为 1 条，got %d: %+v", len(report.Matches), report.Matches)
	}
	m := report.Matches[0]
	if m.FreePercent != 6 {
		t.Errorf("跨节点应取最差水位: %+v", m)
	}
	if len(m.Observations) != 2 || m.Observations[0].NodeID != "n-a" || m.Observations[1].NodeID != "n-b" {
		t.Errorf("来源观测错误: %+v", m.Observations)
	}
	if m.Observations[0].FreePercent != 9 || m.Observations[1].FreePercent != 6 {
		t.Errorf("各节点水位未保留: %+v", m.Observations)
	}
}

func TestDiskSeparatesShardsAndPaths(t *testing.T) {
	a, _ := diskTarget("n1", "s1", [][]any{diskRow("default", "/data/", "100", "10000", "0")})
	b, _ := diskTarget("n2", "s2", [][]any{diskRow("default", "/data/", "100", "10000", "0")})
	c, _ := diskTarget("n3", "s1", [][]any{diskRow("default", "/data2/", "100", "10000", "0")})
	report, _ := CheckDiskSpaceLow(context.Background(), []Target{a, b, c}, DiskSpaceOptions{})
	if len(report.Matches) != 3 {
		t.Fatalf("不同分片/路径应为 3 条，got %d: %+v", len(report.Matches), report.Matches)
	}
}

func TestDiskCustomThreshold100(t *testing.T) {
	// 阈值 100：任何未完全满的磁盘都命中（验证判定链路时使用）。
	tgt, _ := diskTarget("n1", "s1", [][]any{
		diskRow("default", "/data/", "87000000000", "100000000000", "0"),
	})
	report, _ := CheckDiskSpaceLow(context.Background(), []Target{tgt},
		DiskSpaceOptions{FreePercentThreshold: 100})
	if report.Outcome != OutcomeMatched || report.Matches[0].FreePercent != 87 {
		t.Fatalf("阈值 100 判定错误: %+v", report.Matches)
	}
}

func TestDiskUnparseableBytesIsScanFailure(t *testing.T) {
	tgt, _ := diskTarget("n1", "s1", [][]any{
		diskRow("default", "/data/", "100", "10000", "0"),
		diskRow("bad", "/bad/", "not-a-number", "100", "0"),
	})
	report, _ := CheckDiskSpaceLow(context.Background(), []Target{tgt}, DiskSpaceOptions{})
	nr := report.Targets[0]
	if nr.Status != NodeFailed || nr.ErrCategory != ErrCatScanFailed {
		t.Errorf("got %+v, want scan_failed", nr)
	}
	if nr.RowsRead != 1 || nr.RowsKept != 1 || len(report.Matches) != 1 {
		t.Errorf("证据保留错误: %+v / %+v", nr, report.Matches)
	}
}

func TestDiskPartialFailureKeepsEvidence(t *testing.T) {
	down := Target{NodeID: "down", ShardID: "s1", Querier: &fakeQuerier{err: errors.New("connect refused")}}
	up, _ := diskTarget("up", "s2", [][]any{diskRow("default", "/data/", "100", "10000", "0")})
	report, err := CheckDiskSpaceLow(context.Background(), []Target{down, up}, DiskSpaceOptions{})
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

func TestDiskTruncationMarksIncomplete(t *testing.T) {
	tgt, _ := diskTarget("n1", "s1", [][]any{
		diskRow("d1", "/p1", "100", "10000", "0"),
		diskRow("d2", "/p2", "100", "10000", "0"),
		diskRow("d3", "/p3", "100", "10000", "0"),
	})
	report, err := CheckDiskSpaceLow(context.Background(), []Target{tgt},
		DiskSpaceOptions{MaxRowsPerNode: 2})
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

func TestDiskAllFailedReturnsNoUsableObservation(t *testing.T) {
	a := Target{NodeID: "a", ShardID: "s1", Querier: &fakeQuerier{err: errors.New("boom")}}
	b := Target{NodeID: "b", ShardID: "s2", Querier: &fakeQuerier{err: errors.New("boom")}}
	report, err := CheckDiskSpaceLow(context.Background(), []Target{a, b}, DiskSpaceOptions{})
	if !errors.Is(err, ErrNoUsableObservation) {
		t.Fatalf("err = %v, want ErrNoUsableObservation", err)
	}
	if report.Execution != ExecutionFailed || report.Outcome != OutcomeUnknown {
		t.Errorf("got %s/%s, want failed/unknown", report.Execution, report.Outcome)
	}
}

func TestDiskTimeoutMarksNotStarted(t *testing.T) {
	slow := Target{NodeID: "slow", ShardID: "s1", Querier: &fakeQuerier{delay: 150 * time.Millisecond}}
	b1, _ := diskTarget("b1", "s2", [][]any{diskRow("default", "/data/", "100", "100", "0")})
	report, err := CheckDiskSpaceLow(context.Background(), []Target{slow, b1},
		DiskSpaceOptions{Timeout: 80 * time.Millisecond, NodeTimeout: 500 * time.Millisecond, Concurrency: 1})
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

func TestDiskInvalidInput(t *testing.T) {
	q := &fakeQuerier{}
	cases := []struct {
		name    string
		targets []Target
		opts    DiskSpaceOptions
	}{
		{"empty targets", nil, DiskSpaceOptions{}},
		{"empty node id", []Target{{ShardID: "s1", Querier: q}}, DiskSpaceOptions{}},
		{"nil querier", []Target{{NodeID: "n", ShardID: "s"}}, DiskSpaceOptions{}},
		{"dup node id", []Target{{NodeID: "n", ShardID: "s1", Querier: q}, {NodeID: "n", ShardID: "s2", Querier: q}}, DiskSpaceOptions{}},
		{"negative threshold", []Target{{NodeID: "n", ShardID: "s", Querier: q}}, DiskSpaceOptions{FreePercentThreshold: -1}},
		{"threshold over 100", []Target{{NodeID: "n", ShardID: "s", Querier: q}}, DiskSpaceOptions{FreePercentThreshold: 101}},
		{"negative timeout", []Target{{NodeID: "n", ShardID: "s", Querier: q}}, DiskSpaceOptions{Timeout: -1}},
	}
	for _, c := range cases {
		report, err := CheckDiskSpaceLow(context.Background(), c.targets, c.opts)
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

func TestDiskFixedQueryContract(t *testing.T) {
	for _, want := range []string{
		"FROM system.disks", "toString(free_space)", "toString(total_space)",
		"toString(keep_free_space)", "ORDER BY name, path",
	} {
		if !strings.Contains(diskSpaceSQL, want) {
			t.Errorf("固定查询缺少 %q:\n%s", want, diskSpaceSQL)
		}
	}
	if strings.Contains(diskSpaceSQL, "SELECT *") {
		t.Error("固定查询不允许 SELECT *")
	}
}

func TestDiskMatchOrdering(t *testing.T) {
	tgt, _ := diskTarget("n1", "s1", [][]any{
		diskRow("s3", "/p1", "100", "10000", "0"),
		diskRow("backup", "/p9", "100", "10000", "0"),
		diskRow("backup", "/p1", "100", "10000", "0"),
		diskRow("backup", "/p2", "100", "10000", "0"),
	})
	report, _ := CheckDiskSpaceLow(context.Background(), []Target{tgt}, DiskSpaceOptions{})
	want := [][2]string{{"backup", "/p1"}, {"backup", "/p2"}, {"backup", "/p9"}, {"s3", "/p1"}}
	if len(report.Matches) != 4 {
		t.Fatalf("应有 4 条，got %d", len(report.Matches))
	}
	for i, w := range want {
		m := report.Matches[i]
		if m.DiskName != w[0] || m.Path != w[1] {
			t.Errorf("Matches[%d] = %s%s, want %s%s", i, m.DiskName, m.Path, w[0], w[1])
		}
	}
}
