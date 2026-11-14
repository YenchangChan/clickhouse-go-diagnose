package main

// 真实结构体的裁剪回归测试：Keeper 会话丢失窗口的 replica_status 报告
// 会让全表命中（matches 数量爆炸），detached_parts 按表归并会让单条
// match 的 observations 数量爆炸——两种形态都必须被压回上限内。
// 曾经的 bug：核心库结构没有 json tag，键名是 "Matches"，trimPayload
// 取 "matches" 永远为空，裁剪全程空转（真机 1.3MB 直进上下文）。

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/AutumnsGrove/nova/tool"

	diagnose "github.com/YenchangChan/clickhouse-go-diagnose"
)

func TestRunCheckBigReportTrimmed(t *testing.T) {
	rows := make([][]any, 0, 668)
	for i := 0; i < 668; i++ {
		rows = append(rows, []any{fmt.Sprintf("db%d", i/100), fmt.Sprintf("t%d", i), "1", "1", "0", "0", "0", "0"})
	}
	env := checkEnv{
		targets: []diagnose.Target{
			{NodeID: "n1", ShardID: "s1", Querier: &fakeQuerier{rows: &fakeRows{rows: rows}}},
			{NodeID: "n2", ShardID: "s1", Querier: &fakeQuerier{rows: &fakeRows{rows: rows}}},
		},
	}
	tl := runCheckTool(env)
	res, err := tl.Run(context.Background(), json.RawMessage(`{"check":"replica_status"}`), &tool.Context{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Content) > toolResultMaxBytes*3/2 {
		t.Fatalf("裁剪失效: %d bytes", len(res.Content))
	}
	var payload struct {
		Report struct {
			MatchesTotal int    `json:"matches_total"`
			Truncated    string `json:"truncated"`
		} `json:"report"`
	}
	if err := json.Unmarshal([]byte(res.Content), &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Report.Truncated == "" {
		t.Error("应注入截断说明，模型才能知道清单不完整")
	}
}
