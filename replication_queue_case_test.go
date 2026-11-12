package diagnose

import (
	"encoding/json"
	"errors"
	"os"
	"testing"
)

var errQueryFailed = errors.New("connect refused")

// syntheticCase 是 testdata/replication_queue/*.json 的回放格式。
type syntheticCase struct {
	Name        string `json:"name"`
	Source      string `json:"source"`
	Description string `json:"description"`
	Expect      struct {
		Execution string `json:"execution"`
		Outcome   string `json:"outcome"`
		Matches   []struct {
			ShardID     string   `json:"shardID"`
			Database    string   `json:"database"`
			Table       string   `json:"table"`
			PartName    string   `json:"partName"`
			SourceNodes []string `json:"sourceNodes"`
		} `json:"matches"`
	} `json:"expect"`
	Nodes []struct {
		NodeID    string     `json:"nodeID"`
		ShardID   string     `json:"shardID"`
		Rows      [][]string `json:"rows"`
		FailQuery bool       `json:"failQuery"`
	} `json:"nodes"`
}

// TestSyntheticCase01 从 testdata 回放合成案例：无需数据库即可由输入快照
// 确定报告。真实案例验收保持未完成，不能由本测试冒充（M0/M3）。
func TestSyntheticCase01(t *testing.T) {
	data, err := os.ReadFile("testdata/replication_queue/synthetic-case-01.json")
	if err != nil {
		t.Fatalf("读取案例: %v", err)
	}
	var c syntheticCase
	if err := json.Unmarshal(data, &c); err != nil {
		t.Fatalf("解析案例: %v", err)
	}

	targets := make([]Target, 0, len(c.Nodes))
	for _, n := range c.Nodes {
		if n.FailQuery {
			targets = append(targets, Target{
				NodeID:  n.NodeID,
				ShardID: n.ShardID,
				Querier: &fakeQuerier{err: errQueryFailed},
			})
			continue
		}
		rows := make([][]any, 0, len(n.Rows))
		for _, r := range n.Rows {
			row := make([]any, len(r))
			for i, v := range r {
				row[i] = v
			}
			rows = append(rows, row)
		}
		targets = append(targets, Target{
			NodeID:  n.NodeID,
			ShardID: n.ShardID,
			Querier: &fakeQuerier{rows: &fakeRows{rows: rows}},
		})
	}

	report, err := CheckReplicationQueueFailures(t.Context(), targets, ReplicationQueueOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil（部分节点失败仍应给出匹配结果）", err)
	}
	if string(report.Execution) != c.Expect.Execution || string(report.Outcome) != c.Expect.Outcome {
		t.Fatalf("got %s/%s, want %s/%s", report.Execution, report.Outcome, c.Expect.Execution, c.Expect.Outcome)
	}
	if len(report.Matches) != len(c.Expect.Matches) {
		t.Fatalf("匹配数 = %d, want %d:\n%+v", len(report.Matches), len(c.Expect.Matches), report.Matches)
	}
	for i, want := range c.Expect.Matches {
		got := report.Matches[i]
		if got.ShardID != want.ShardID || got.Database != want.Database || got.Table != want.Table || got.PartName != want.PartName {
			t.Errorf("matches[%d] = (%s,%s,%s,%s), want (%s,%s,%s,%s)",
				i, got.ShardID, got.Database, got.Table, got.PartName, want.ShardID, want.Database, want.Table, want.PartName)
		}
		if len(got.Observations) != len(want.SourceNodes) {
			t.Errorf("matches[%d] 来源数 = %d, want %d", i, len(got.Observations), len(want.SourceNodes))
			continue
		}
		for j, node := range want.SourceNodes {
			if got.Observations[j].NodeID != node {
				t.Errorf("matches[%d] 来源[%d] = %s, want %s", i, j, got.Observations[j].NodeID, node)
			}
		}
	}
	// 覆盖信息：node-down 必须以 failed 记录，且报告提示补查覆盖。
	foundFailed := false
	for _, r := range report.Targets {
		if r.NodeID == "node-down" && r.Status == NodeFailed {
			foundFailed = true
		}
	}
	if !foundFailed {
		t.Errorf("node-down 未被记录为失败: %+v", report.Targets)
	}
	if !hasAdviceCode(report.NextSteps, AdviceCompleteNodeCoverage) {
		t.Errorf("应提示补查覆盖: %+v", report.NextSteps)
	}
}
