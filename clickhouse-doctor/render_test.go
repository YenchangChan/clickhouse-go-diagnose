package main

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/YenchangChan/clickhouse-go-diagnose/clickhouse-doctor/targets"
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
	meta := runMeta{Version: version, Protocol: "native", Targets: []targetSummary{
		{NodeID: "node-a", ShardID: "s1", Addr: "host-a:9000", Protocol: "native"},
	},
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
	meta := runMeta{Version: version, Protocol: "native", Targets: []targetSummary{
		{NodeID: "node-a", ShardID: "s1", Addr: "host-a:9000", Protocol: "native"},
	},
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
	meta := runMeta{Version: version, Protocol: "native", Targets: []targetSummary{
		{NodeID: "node-a", ShardID: "shard-1", Addr: "host-a:9000", Protocol: "native"},
	}}
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

func TestBuildNodeSpecs(t *testing.T) {
	// 多节点：shard 前缀 + 无前缀（用默认分片）+ HTTP 地址
	specs, err := buildNodeSpecs(options{
		nodes:   targets.FlagList{"shard-1@host-a:9000", "host-b:9000", "shard-2@http://host-c:8123"},
		shardID: "shard-1",
	})
	if err != nil {
		t.Fatalf("多节点解析: %v", err)
	}
	if len(specs) != 3 {
		t.Fatalf("应有 3 个节点，got %d", len(specs))
	}
	if specs[0].ShardID != "shard-1" || specs[0].NodeID != "host-a:9000" {
		t.Errorf("specs[0] 错误: %+v", specs[0])
	}
	if specs[1].ShardID != "shard-1" || specs[1].NodeID != "host-b:9000" {
		t.Errorf("specs[1] 应使用默认分片: %+v", specs[1])
	}
	if !targets.IsHTTP(specs[2].Addr) {
		t.Errorf("specs[2] 应识别为 HTTP: %+v", specs[2])
	}

	// 单节点兼容形式：--addr + --node-id
	specs, err = buildNodeSpecs(options{addr: "host-a:9000", nodeID: "node-a", shardID: "shard-9"})
	if err != nil {
		t.Fatalf("单节点兼容: %v", err)
	}
	if len(specs) != 1 || specs[0].NodeID != "node-a" || specs[0].ShardID != "shard-9" {
		t.Fatalf("单节点兼容错误: %+v", specs)
	}

	// 单节点兼容形式：--http 自动加 http:// 前缀
	specs, err = buildNodeSpecs(options{httpAddr: "host-c:8123", shardID: "shard-1"})
	if err != nil {
		t.Fatalf("--http 兼容: %v", err)
	}
	if !targets.IsHTTP(specs[0].Addr) {
		t.Errorf("--http 应产生 http:// 前缀地址: %+v", specs[0])
	}

	// 空：必须报错
	if _, err := buildNodeSpecs(options{}); err == nil {
		t.Error("无任何节点参数应报错")
	}

	// 重复 NodeID：必须报错
	if _, err := buildNodeSpecs(options{
		nodes: targets.FlagList{"shard-1@host-a:9000", "shard-2@host-a:9000"},
	}); err == nil {
		t.Error("重复 node-id 应报错")
	}

	// 残缺 shard 前缀：必须报错
	if _, err := buildNodeSpecs(options{nodes: targets.FlagList{"@host-a:9000"}}); err == nil {
		t.Error("空 shard 前缀应报错")
	}
}

func TestBuildNodeSpecsThreeParts(t *testing.T) {
	specs, err := buildNodeSpecs(options{
		nodes:   targets.FlagList{"shard-1@r1@192.168.56.101:9000", "shard-2@r2@192.168.56.101:9000"},
		shardID: "shard-1",
	})
	if err != nil {
		t.Fatalf("三段式解析: %v", err)
	}
	if specs[0].NodeID != "r1" || specs[1].NodeID != "r2" {
		t.Errorf("三段式 node-id 错误: %+v", specs)
	}
	if specs[0].Addr != "192.168.56.101:9000" {
		t.Errorf("三段式 addr 错误: %+v", specs[0])
	}
	// 四段（地址含 @ 之类）应报错
	if _, err := buildNodeSpecs(options{nodes: targets.FlagList{"a@b@c@d"}}); err == nil {
		t.Error("超过三段应报错")
	}
}

func TestValidateThresholds(t *testing.T) {
	// 全 0（全用库默认）合法
	if err := validateThresholds(thresholds{}); err != nil {
		t.Fatalf("全零阈值应合法: %v", err)
	}
	// 正值合法
	if err := validateThresholds(thresholds{parts: 5, diskFreePercent: 20}); err != nil {
		t.Fatalf("正值阈值应合法: %v", err)
	}
	// 任一负值必须拒绝
	neg := []thresholds{
		{replicaDelaySeconds: -1}, {parts: -1}, {queueDepth: -1},
		{queueAgeSeconds: -1}, {queueRetries: -1}, {mutationAgeSeconds: -1},
		{diskFreePercent: -1}, {detachedCount: -1},
	}
	for i, th := range neg {
		if err := validateThresholds(th); err == nil {
			t.Errorf("负值阈值 case %d 应拒绝: %+v", i, th)
		}
	}
}
