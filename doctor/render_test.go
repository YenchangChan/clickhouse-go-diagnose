package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestParseChecks(t *testing.T) {
	got, err := parseChecks("all")
	if err != nil || len(got) != 7 {
		t.Fatalf("all: %v, len=%d", err, len(got))
	}
	got, err = parseChecks("replica_status, disk_space_low ,replica_status")
	if err != nil {
		t.Fatalf("逗号列表: %v", err)
	}
	if len(got) != 2 || got[0] != "replica_status" || got[1] != "disk_space_low" {
		t.Fatalf("逗号列表去重错误: %v", got)
	}
	if _, err := parseChecks("nope"); err == nil {
		t.Fatal("未知检查名应报错")
	}
	if _, err := parseChecks("  "); err != nil {
		t.Fatalf("空白应视为 all: %v", err)
	}
}

func TestExitCode(t *testing.T) {
	cases := []struct {
		name string
		in   []checkView
		want int
	}{
		{"全部干净", []checkView{{Execution: "complete", Outcome: "no_match"}}, exitOK},
		{"有匹配", []checkView{{Execution: "complete", Outcome: "matched"}}, exitMatched},
		{"partial 但有匹配仍报匹配", []checkView{{Execution: "partial", Outcome: "matched"}}, exitMatched},
		{"partial 无匹配为覆盖不完整", []checkView{{Execution: "partial", Outcome: "unknown"}}, exitUncovSeg},
		{"failed", []checkView{{Execution: "failed", Outcome: "unknown"}}, exitUncovSeg},
		{"mixed", []checkView{
			{Execution: "complete", Outcome: "no_match"},
			{Execution: "failed", Outcome: "unknown"},
		}, exitUncovSeg},
		{"mixed matched", []checkView{
			{Execution: "failed", Outcome: "unknown"},
			{Execution: "complete", Outcome: "matched"},
		}, exitMatched},
	}
	for _, c := range cases {
		if got := exitCode(c.in); got != c.want {
			t.Errorf("%s: got %d want %d", c.name, got, c.want)
		}
	}
}

func TestRenderJSON(t *testing.T) {
	replicaObs := replicaObsView{
		NodeID: "node-a", IsReadonly: true, AbsoluteDelaySecond: 42,
		TotalReplicas: 2, ActiveReplicas: 1, Reasons: []string{"replica_readonly"},
	}
	in := []checkView{{
		Check:     "replica_status",
		Execution: "complete",
		Outcome:   "matched",
		CheckID:   "replica_status",
		Targets:   []targetView{{NodeID: "node-a", ShardID: "s1", Status: "ok", RowsRead: 5, RowsKept: 1}},
		Matches: []any{replicaMatchView{
			ShardID: "s1", Database: "default", Table: "t1",
			Observations: []replicaObsView{replicaObs},
		}},
		NextSteps: []followUpView{{Code: "locate_inactive_replica", Detail: "定位未激活副本"}},
	}}
	meta := runMeta{Version: version, NodeID: "node-a", ShardID: "s1", Protocol: "native",
		StartedAt: time.Unix(1700000000, 0), FinishedAt: time.Unix(1700000001, 0)}

	var buf bytes.Buffer
	if err := renderJSON(&buf, meta, in); err != nil {
		t.Fatalf("renderJSON: %v", err)
	}

	var doc struct {
		Run    runMeta `json:"run"`
		Checks []struct {
			Check     string `json:"check"`
			Execution string `json:"execution"`
			Outcome   string `json:"outcome"`
			Targets   []struct {
				NodeID string `json:"node_id"`
			} `json:"targets"`
			Matches []replicaMatchView `json:"matches"`
		} `json:"checks"`
	}
	if err := json.Unmarshal(buf.Bytes(), &doc); err != nil {
		t.Fatalf("unmarshal: %v\n%s", err, buf.String())
	}
	if len(doc.Checks) != 1 || doc.Checks[0].Check != "replica_status" {
		t.Fatalf("checks 解析错误: %+v", doc)
	}
	if doc.Run.Protocol != "native" || doc.Checks[0].Execution != "complete" {
		t.Fatalf("meta/execution 错误: %+v", doc)
	}
	if len(doc.Checks[0].Matches) != 1 || len(doc.Checks[0].Matches[0].Observations) != 1 {
		t.Fatalf("matches 解析错误: %+v", doc.Checks[0].Matches)
	}
	if doc.Checks[0].Matches[0].Observations[0].Reasons[0] != "replica_readonly" {
		t.Fatalf("reasons 错误")
	}
	// 顶层 error 为空时不得出现 error 键
	if strings.Contains(buf.String(), `"error":`) {
		t.Fatalf("空 error 不应出现在 JSON:\n%s", buf.String())
	}
}

func TestRenderTextMatchedAndPartial(t *testing.T) {
	meta := runMeta{Version: version, NodeID: "node-a", ShardID: "s1", Protocol: "native",
		StartedAt: time.Unix(1700000000, 0), FinishedAt: time.Unix(1700000001, 0)}

	results := []checkView{
		{
			Check: "replica_status", Execution: "complete", Outcome: "matched",
			Targets: []targetView{{NodeID: "node-a", ShardID: "s1", Status: "ok", RowsRead: 5, RowsKept: 1}},
			Matches: []any{replicaMatchView{
				ShardID: "s1", Database: "default", Table: "t1",
				Observations: []replicaObsView{{
					NodeID: "node-a", IsReadonly: true, TotalReplicas: 2, ActiveReplicas: 1,
					Reasons: []string{"replica_readonly"},
				}},
			}},
			NextSteps: []followUpView{{Code: "locate_inactive_replica"}},
		},
		{
			Check: "too_many_parts", Execution: "partial", Outcome: "unknown",
			Err: "diagnose: no usable observation from any target",
			Targets: []targetView{{
				NodeID: "node-a", ShardID: "s1", Status: "failed",
				ErrCategory: "timeout", ErrMessage: "something sensitive from server",
			}},
		},
	}

	var buf bytes.Buffer
	if err := renderText(&buf, meta, results); err != nil {
		t.Fatalf("renderText: %v", err)
	}
	out := buf.String()

	// matched 三态显式呈现（!! 标记）
	if !strings.Contains(out, "[!!] replica_status complete/matched") {
		t.Errorf("matched 行缺失:\n%s", out)
	}
	// 覆盖缺口必须出现，不得渲染成健康
	if !strings.Contains(out, "partial") || !strings.Contains(out, "无法做出匹配判断") {
		t.Errorf("partial/unknown 语义缺失:\n%s", out)
	}
	if !strings.Contains(out, "timeout") {
		t.Errorf("错误类别缺失:\n%s", out)
	}
	// 顶层 error 在报告之后呈现（先证据后错误）
	if !strings.Contains(out, "no usable observation") {
		t.Errorf("顶层 error 缺失:\n%s", out)
	}
	// 建议代码出现
	if !strings.Contains(out, "locate_inactive_replica") {
		t.Errorf("建议代码缺失:\n%s", out)
	}
}

func TestRenderTextNoMatchNote(t *testing.T) {
	meta := runMeta{Version: version, Protocol: "native"}
	var buf bytes.Buffer
	err := renderText(&buf, meta, []checkView{{
		Check: "disk_space_low", Execution: "complete", Outcome: "no_match",
		Targets: []targetView{{NodeID: "node-a", Status: "ok", RowsRead: 1, RowsKept: 0}},
		Matches: []any{},
	}})
	if err != nil {
		t.Fatalf("renderText: %v", err)
	}
	if !strings.Contains(buf.String(), "未发现匹配（不等于集群健康）") {
		t.Errorf("no_match 提示缺失:\n%s", buf.String())
	}
}

func TestClip(t *testing.T) {
	long := strings.Repeat("字", maxTextLen+10)
	got := clip(long)
	suffix := "...(截断,完整内容见 JSON)"
	if len([]rune(got)) != maxTextLen+len([]rune(suffix)) {
		t.Errorf("clip 长度错误: %d", len([]rune(got)))
	}
	if clip("短") != "短" {
		t.Errorf("短文本不应截断")
	}
}
