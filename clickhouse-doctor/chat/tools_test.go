package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/AutumnsGrove/nova/tool"

	diagnose "github.com/YenchangChan/clickhouse-go-diagnose"
)

func execCommand(name string, args ...string) (string, error) {
	out, err := exec.Command(name, args...).Output()
	return string(out), err
}

// ---- 极简 fake querier：实现 diagnose.Querier/Rows ----

type fakeRows struct {
	rows [][]any
	i    int
}

func (r *fakeRows) Next() bool { return r.i < len(r.rows) }

func (r *fakeRows) Scan(dest ...any) error {
	if r.i >= len(r.rows) {
		return errors.New("scan after EOF")
	}
	row := r.rows[r.i]
	r.i++
	if len(row) != len(dest) {
		return fmt.Errorf("列数 %d != %d", len(dest), len(row))
	}
	for j, d := range dest {
		ns, ok := d.(*sql.NullString)
		if !ok {
			return fmt.Errorf("dest[%d] 不是 *sql.NullString", j)
		}
		if s, ok := row[j].(string); ok {
			ns.String, ns.Valid = s, true
		} else {
			ns.Valid = false
		}
	}
	return nil
}

func (r *fakeRows) Err() error   { return nil }
func (r *fakeRows) Close() error { return nil }

type fakeQuerier struct{ rows *fakeRows }

func (q *fakeQuerier) Query(ctx context.Context, query string, args ...any) (diagnose.Rows, error) {
	return q.rows, nil
}

func fakeEnv(rows ...[]any) checkEnv {
	return checkEnv{
		targets: []diagnose.Target{
			{NodeID: "n1", ShardID: "s1", Querier: &fakeQuerier{rows: &fakeRows{rows: rows}}},
		},
		timeout: time.Second, nodeTO: time.Second,
	}
}

func callTool(t *testing.T, tl tool.Tool, args string) tool.Result {
	t.Helper()
	res, err := tl.Run(context.Background(), json.RawMessage(args), &tool.Context{})
	if err != nil {
		t.Fatalf("Run err = %v（工具错误应走 Result.IsError 而不是 error）", err)
	}
	return res
}

// ---- list_checks ----

func TestListChecksTool(t *testing.T) {
	res := callTool(t, listChecksTool(), `{}`)
	if res.IsError {
		t.Fatalf("list_checks 不应失败: %s", res.Content)
	}
	var metas []diagnose.CheckMeta
	if err := json.Unmarshal([]byte(res.Content), &metas); err != nil {
		t.Fatalf("输出应为 CheckMeta JSON: %v", err)
	}
	if len(metas) != 7 {
		t.Fatalf("应列出 7 个检查, got %d", len(metas))
	}
	ids := map[string]bool{}
	for _, m := range metas {
		ids[m.ID] = true
	}
	if !ids["replica_status"] || !ids["detached_parts"] {
		t.Errorf("清单缺少关键检查: %v", ids)
	}
}

// ---- run_check 参数校验（错误走 Result.IsError，不是 error）----

func TestRunCheckUnknownID(t *testing.T) {
	res := callTool(t, runCheckTool(fakeEnv()), `{"check":"no_such"}`)
	if !res.IsError || !strings.Contains(res.Content, "未知检查 ID") {
		t.Errorf("未知 ID 应报 IsError 且提示 list_checks: %s", res.Content)
	}
}

func TestRunCheckUnknownThresholdKey(t *testing.T) {
	res := callTool(t, runCheckTool(fakeEnv()), `{"check":"replica_status","thresholds":{"nope":1}}`)
	if !res.IsError || !strings.Contains(res.Content, "没有阈值槽位") {
		t.Errorf("未知槽位应报 IsError: %s", res.Content)
	}
	if strings.Contains(res.Content, "delay_seconds") == false {
		t.Errorf("错误信息应列出可用槽位: %s", res.Content)
	}
}

func TestRunCheckNegativeThreshold(t *testing.T) {
	res := callTool(t, runCheckTool(fakeEnv()), `{"check":"disk_space_low","thresholds":{"free_percent":-1}}`)
	if !res.IsError {
		t.Errorf("负值阈值应报 IsError: %s", res.Content)
	}
}

// ---- run_check 正常路径：阈值透传 + 报告 JSON ----

func TestRunCheckHappyPath(t *testing.T) {
	env := fakeEnv([]any{"db1", "t1", "1", "0", "0", "2", "2", "0"}) // readonly 行
	res := callTool(t, runCheckTool(env), `{"check":"replica_status","thresholds":{"delay_seconds":1}}`)
	if res.IsError {
		t.Fatalf("run_check 不应失败: %s", res.Content)
	}
	var payload struct {
		Meta   diagnose.CheckMeta           `json:"meta"`
		Report diagnose.ReplicaStatusReport `json:"report"`
	}
	if err := json.Unmarshal([]byte(res.Content), &payload); err != nil {
		t.Fatalf("payload 应可解析: %v", err)
	}
	if payload.Meta.ID != "replica_status" {
		t.Errorf("meta.ID = %q", payload.Meta.ID)
	}
	if payload.Report.Outcome != diagnose.OutcomeMatched {
		t.Fatalf("outcome = %s, want matched", payload.Report.Outcome)
	}
	if len(payload.Report.Matches) != 1 {
		t.Fatalf("应有 1 条匹配, got %d", len(payload.Report.Matches))
	}
	if o := payload.Report.Matches[0].Observations[0]; !o.IsReadonly {
		t.Errorf("观测应命中 readonly: %+v", o)
	}
}

func TestRunCheckEmptyTargetsIsToolError(t *testing.T) {
	// 执行器内部校验失败（targets 为空）→ nil 报告 → 工具层转 IsError，
	// 让模型看到可修正的错误而不是 Go error。
	res := callTool(t, runCheckTool(checkEnv{}), `{"check":"list_checks"}`)
	_ = res
	res = callTool(t, runCheckTool(checkEnv{}), `{"check":"replica_status"}`)
	if !res.IsError || !strings.Contains(res.Content, "检查未执行") {
		t.Errorf("空 targets 应报 IsError: %s", res.Content)
	}
}

// ---- 分层断言：轻量 CLI 依赖闭包不得出现 nova ----

func execGoList(t *testing.T, modulePath string) (string, error) {
	t.Helper()
	out, err := execCommand("go", "list", "-deps", modulePath)
	if err != nil {
		return "", err
	}
	return out, nil
}

func TestLightCLIDepsExcludeNova(t *testing.T) {
	if testing.Short() {
		t.Skip("short 模式跳过 go list")
	}
	out, err := execGoList(t, "github.com/YenchangChan/clickhouse-go-diagnose/clickhouse-doctor")
	if err != nil {
		t.Skipf("go list 不可用: %v", err)
	}
	for _, line := range strings.Split(out, "\n") {
		if strings.Contains(line, "AutumnsGrove/nova") {
			t.Errorf("轻量 CLI 依赖闭包混入 Agent 框架: %s", line)
		}
	}
}
