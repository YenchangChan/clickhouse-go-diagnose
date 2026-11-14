package diagnose

import (
	"context"
	"errors"
	"testing"
)

// wantCheckIDs 与全量检查清单对齐；新增检查时同步更新此处与 checkRegistry。
var wantCheckIDs = []string{
	"replication_queue_failures",
	"too_many_parts",
	"replica_status",
	"mutations_stuck",
	"replication_queue_backlog",
	"disk_space_low",
	"detached_parts",
}

func TestListChecks(t *testing.T) {
	metas := ListChecks()
	if len(metas) != len(wantCheckIDs) {
		t.Fatalf("ListChecks 返回 %d 项, want %d", len(metas), len(wantCheckIDs))
	}
	seen := map[string]bool{}
	for i, m := range metas {
		if m.ID != wantCheckIDs[i] {
			t.Errorf("metas[%d].ID = %q, want %q（顺序应稳定）", i, m.ID, wantCheckIDs[i])
		}
		if seen[m.ID] {
			t.Errorf("检查 ID %q 重复", m.ID)
		}
		seen[m.ID] = true
		if m.Description == "" {
			t.Errorf("检查 %q 缺少描述", m.ID)
		}
		if m.RuleVersion == "" {
			t.Errorf("检查 %q 缺少 RuleVersion", m.ID)
		}
		for _, th := range m.Thresholds {
			if th.Key == "" || th.Description == "" {
				t.Errorf("检查 %q 阈值槽位声明不完整: %+v", m.ID, th)
			}
			if th.Default <= 0 {
				t.Errorf("检查 %q 阈值 %q 默认值应大于 0: %d", m.ID, th.Key, th.Default)
			}
		}
	}
	// 阈值槽位与库内默认常量一致（防止文档漂移）。
	byID := map[string]CheckMeta{}
	for _, m := range metas {
		byID[m.ID] = m
	}
	cases := []struct {
		id   string
		keys []string
		defs []int
	}{
		{"replication_queue_failures", nil, nil},
		{"too_many_parts", []string{"parts_count"}, []int{defaultPartsThreshold}},
		{"replica_status", []string{"delay_seconds"}, []int{defaultDelayThreshold}},
		{"mutations_stuck", []string{"age_seconds"}, []int{defaultAgeThreshold}},
		{"replication_queue_backlog", []string{"depth", "age_seconds", "retries"}, []int{defaultDepthThreshold, defaultAgeThresholdSeconds, defaultRetriesThreshold}},
		{"disk_space_low", []string{"free_percent"}, []int{defaultDiskFreePercentThreshold}},
		{"detached_parts", []string{"count"}, []int{defaultDetachedCountThreshold}},
	}
	for _, c := range cases {
		m := byID[c.id]
		if len(m.Thresholds) != len(c.keys) {
			t.Fatalf("%s 阈值槽位数 = %d, want %d", c.id, len(m.Thresholds), len(c.keys))
		}
		for i, k := range c.keys {
			if m.Thresholds[i].Key != k {
				t.Errorf("%s 槽位 %d key = %q, want %q", c.id, i, m.Thresholds[i].Key, k)
			}
			if m.Thresholds[i].Default != c.defs[i] {
				t.Errorf("%s 槽位 %q 默认值 = %d, want %d", c.id, k, m.Thresholds[i].Default, c.defs[i])
			}
		}
	}
}

func TestRunCheckRoutesByID(t *testing.T) {
	// 空结果集对所有检查都是合法输入：complete + no_match + nil。
	for _, id := range wantCheckIDs {
		rep, err := RunCheck(context.Background(), id, []Target{
			{NodeID: "n1", ShardID: "s1", Querier: &fakeQuerier{rows: &fakeRows{}}},
		}, CheckOptions{})
		if err != nil {
			t.Fatalf("%s: err = %v, want nil", id, err)
		}
		h := rep.Header()
		if h.ID != id {
			t.Errorf("%s: Header.ID = %q", id, h.ID)
		}
		if h.Execution != ExecutionComplete || h.Outcome != OutcomeNoMatch {
			t.Errorf("%s: got %s/%s, want complete/no_match", id, h.Execution, h.Outcome)
		}
		if len(h.Targets) != 1 || h.Targets[0].Status != NodeOK {
			t.Errorf("%s: targets 异常: %+v", id, h.Targets)
		}
	}
}

func TestRunCheckUnknownID(t *testing.T) {
	rep, err := RunCheck(context.Background(), "no_such_check", []Target{
		{NodeID: "n1", ShardID: "s1", Querier: &fakeQuerier{}},
	}, CheckOptions{})
	if !errors.Is(err, ErrInvalidInput) {
		t.Errorf("err = %v, want ErrInvalidInput", err)
	}
	if rep != nil {
		t.Errorf("未知 ID 应返回 nil 报告: %+v", rep)
	}
}

func TestRunCheckThresholdSlots(t *testing.T) {
	// Thresholds 按槽位透传：replica_status 槽位 0 是 delay_seconds，
	// 阈值 1 时 delay=5 的健康行应命中；默认阈值 300 时不应命中。
	rows := [][]any{replicaRow("db1", "t1", "0", "0", "5", "2", "2", "0")}
	tgts := func() []Target {
		tgt, _ := replicaTarget("n1", "s1", rows)
		return []Target{tgt}
	}
	rep, err := RunCheck(context.Background(), "replica_status", tgts(), CheckOptions{Thresholds: []int{1}})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if rep.Header().Outcome != OutcomeMatched {
		t.Errorf("阈值 1 应命中 delay=5, got %s", rep.Header().Outcome)
	}
	rep2, err := RunCheck(context.Background(), "replica_status", tgts(), CheckOptions{})
	if err != nil {
		t.Fatalf("err = %v", err)
	}
	if rep2.Header().Outcome != OutcomeNoMatch {
		t.Errorf("默认阈值不应命中 delay=5, got %s", rep2.Header().Outcome)
	}
}

func TestRunCheckValidatesInput(t *testing.T) {
	q := &fakeQuerier{}
	cases := []struct {
		name    string
		id      string
		targets []Target
		opts    CheckOptions
	}{
		{"empty targets", "replica_status", nil, CheckOptions{}},
		{"negative budget", "replica_status", []Target{{NodeID: "n", ShardID: "s", Querier: q}}, CheckOptions{Timeout: -1}},
		{"threshold slot negative", "replica_status", []Target{{NodeID: "n", ShardID: "s", Querier: q}}, CheckOptions{Thresholds: []int{-1}}},
		{"threshold slot overflow", "replica_status", []Target{{NodeID: "n", ShardID: "s", Querier: q}}, CheckOptions{Thresholds: []int{1, 2}}},
	}
	for _, c := range cases {
		rep, err := RunCheck(context.Background(), c.id, c.targets, c.opts)
		if !errors.Is(err, ErrInvalidInput) {
			t.Errorf("%s: err = %v, want ErrInvalidInput", c.name, err)
		}
		if rep != nil {
			t.Errorf("%s: 校验失败应返回 nil 报告", c.name)
		}
		if q.queries != 0 {
			t.Errorf("%s: 校验失败不应执行查询", c.name)
		}
	}
}
