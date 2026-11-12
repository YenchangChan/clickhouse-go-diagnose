package diagnose

import (
	"reflect"
	"testing"
)

func TestMatchesException(t *testing.T) {
	cases := map[string]bool{
		"Code: 262. DB::Exception: No replica has part 20261102_10_20_3 (NO_REPLICA_HAS_PART)": true,
		"no_replica_has_part covering something":                                               true, // 与 SQL 端 positionCaseInsensitive 一致
		"NO_REPLICA_HAS_PART":                                                                  true,
		"Table is in readonly mode":                                                            false,
		"":                                                                                     false,
	}
	for in, want := range cases {
		if got := matchesException(in); got != want {
			t.Errorf("matchesException(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestReduceMatchesMergesSameShard(t *testing.T) {
	// 同分片两个副本报出同一 part 是这类故障的定义特征，必须合并展示
	// 但保留各来源节点；同节点的多条队列记录保留为多个观测。
	obs := []nodeObservation{
		{"shard-1", "db1", "t1", "p3", "node-b", "NO_REPLICA_HAS_PART"},
		{"shard-1", "db1", "t1", "p3", "node-a", "NO_REPLICA_HAS_PART v1"},
		{"shard-1", "db1", "t1", "p3", "node-a", "NO_REPLICA_HAS_PART v2"},
	}
	got := reduceMatches(obs)
	if len(got) != 1 {
		t.Fatalf("同分片同 part 应归并为 1 条，got %d: %+v", len(got), got)
	}
	m := got[0]
	if m.ShardID != "shard-1" || m.Database != "db1" || m.Table != "t1" || m.PartName != "p3" {
		t.Errorf("归并键错误: %+v", m)
	}
	if len(m.Observations) != 3 {
		t.Fatalf("三条队列记录应保留为 3 个观测，got %d", len(m.Observations))
	}
	// 观测按 NodeID 排序，同节点内部保持稳定。
	if m.Observations[0].NodeID != "node-a" || m.Observations[0].LastException != "NO_REPLICA_HAS_PART v1" {
		t.Errorf("观测排序错误: %+v", m.Observations)
	}
	if m.Observations[2].NodeID != "node-b" {
		t.Errorf("观测排序错误: %+v", m.Observations)
	}
}

func TestReduceMatchesSeparatesShards(t *testing.T) {
	// 不同分片是独立的复制组，同名表同名 part 不能合并。
	obs := []nodeObservation{
		{"shard-1", "db1", "t1", "p3", "n1", "NO_REPLICA_HAS_PART"},
		{"shard-2", "db1", "t1", "p3", "n3", "NO_REPLICA_HAS_PART"},
	}
	got := reduceMatches(obs)
	if len(got) != 2 {
		t.Fatalf("不同分片应为 2 条，got %d", len(got))
	}
	if got[0].ShardID == got[1].ShardID {
		t.Errorf("归并未按分片区分: %+v", got)
	}
}

func TestReduceMatchesStableOrder(t *testing.T) {
	// 输入顺序不能影响输出顺序：并发调度不改变报告。
	obs := []nodeObservation{
		{"s2", "db", "t", "b", "n1", "NO_REPLICA_HAS_PART"},
		{"s1", "db", "t", "z", "n1", "NO_REPLICA_HAS_PART"},
		{"s1", "db", "t", "a", "n1", "NO_REPLICA_HAS_PART"},
	}
	want := reduceMatches(obs)
	// 反转输入重放，结果必须相同。
	reversed := make([]nodeObservation, len(obs))
	for i, o := range obs {
		reversed[len(obs)-1-i] = o
	}
	got := reduceMatches(reversed)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("归并结果受输入顺序影响:\n want %+v\n got  %+v", want, got)
	}
}

func TestSummarizeExecutionMatrix(t *testing.T) {
	cases := []struct {
		name    string
		results []NodeResult
		want    Execution
	}{
		{"all ok", []NodeResult{{Status: NodeOK}, {Status: NodeOK}}, ExecutionComplete},
		{"ok with truncated", []NodeResult{{Status: NodeOK, Truncated: true}, {Status: NodeOK}}, ExecutionPartial},
		{"ok with failed", []NodeResult{{Status: NodeOK}, {Status: NodeFailed}}, ExecutionPartial},
		{"ok with not started", []NodeResult{{Status: NodeOK}, {Status: NodeNotStarted}}, ExecutionPartial},
		{"failed with kept evidence", []NodeResult{{Status: NodeFailed}, {Status: NodeFailed}}, ExecutionFailed},
	}
	for _, c := range cases {
		if got := summarizeExecution(c.results); got != c.want {
			t.Errorf("%s: summarizeExecution = %s, want %s", c.name, got, c.want)
		}
	}
}

func TestSummarizeOutcomeMatrix(t *testing.T) {
	cases := []struct {
		name    string
		exe     Execution
		matches []QueueMatch
		want    Outcome
	}{
		{"complete no match", ExecutionComplete, nil, OutcomeNoMatch},
		{"complete matched", ExecutionComplete, []QueueMatch{{ShardID: "s1"}}, OutcomeMatched},
		{"partial matched", ExecutionPartial, []QueueMatch{{ShardID: "s1"}}, OutcomeMatched},
		{"partial no match is unknown", ExecutionPartial, nil, OutcomeUnknown},
		{"failed no match is unknown", ExecutionFailed, nil, OutcomeUnknown},
	}
	for _, c := range cases {
		if got := summarizeOutcome(c.exe, c.matches); got != c.want {
			t.Errorf("%s: summarizeOutcome = %s, want %s", c.name, got, c.want)
		}
	}
}

func TestBuildFollowUps(t *testing.T) {
	ok := NodeResult{Status: NodeOK}
	trunc := NodeResult{Status: NodeOK, Truncated: true}

	got := buildFollowUps(ExecutionComplete, []NodeResult{ok}, nil)
	if len(got) != 0 {
		t.Errorf("complete + no_match 不应产生建议: %+v", got)
	}

	got = buildFollowUps(ExecutionPartial, []NodeResult{{Status: NodeFailed}, trunc}, []QueueMatch{{ShardID: "s1"}})
	codes := map[string]bool{}
	for _, s := range got {
		codes[s.Code] = true
	}
	for _, want := range []string{AdviceVerifyReplicasForPart, AdviceInspectQueueOnSourceNodes, AdviceCompleteNodeCoverage, AdviceRerunWithHigherRowLimit} {
		if !codes[want] {
			t.Errorf("缺少建议 %s: %+v", want, got)
		}
	}
}
