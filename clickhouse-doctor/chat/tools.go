// clickhouse-doctor-chat 的工具注册。边界纪律（与架构约定一致）：
//   - 判定全部来自核心库（RunCheck），Agent 只做消费与交互；
//   - 工具面只注册两个只读检查工具，模型无法触达 SQL 执行或修复操作；
//   - targets 在会话级固定，工具参数不暴露连接地址，Agent 不能连到别的主机。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/AutumnsGrove/nova/tool"

	diagnose "github.com/YenchangChan/clickhouse-go-diagnose"
)

// checkEnv 是工具共享的执行上下文：会话级固定的目标与预算。
type checkEnv struct {
	targets []diagnose.Target
	timeout time.Duration
	nodeTO  time.Duration
	maxRows int
}

// newTools 注册 chat 的全部工具。只读检查工具自动放行依赖这一事实：
// 所有工具 ReadOnly 且无其他工具注册。
func newTools(env checkEnv) *tool.Registry {
	reg := tool.NewRegistry()
	reg.MustRegister(listChecksTool())
	reg.MustRegister(runCheckTool(env))
	return reg
}

func listChecksTool() tool.Tool {
	return tool.New(tool.Info{
		Name: "list_checks",
		Description: "列出全部可用的只读诊断检查：稳定 ID、描述、规则版本与阈值槽位" +
			"（Key/默认值）。执行检查前先看这里有哪些 ID 和可调阈值。",
		Parameters: map[string]any{
			"type":       "object",
			"properties": map[string]any{},
		},
		Parallel: true,
		ReadOnly: true,
	}, func(ctx context.Context, _ json.RawMessage, _ *tool.Context) (tool.Result, error) {
		out, err := json.MarshalIndent(diagnose.ListChecks(), "", "  ")
		if err != nil {
			return tool.Errorf("检查清单序列化失败: %v", err), nil
		}
		return tool.Text("%s", out), nil
	})
}

func runCheckTool(env checkEnv) tool.Tool {
	return tool.New(tool.Info{
		Name:        "run_check",
		Description: "对会话固定的目标节点执行一个只读诊断检查，返回结构化 JSON 报告。报告是诊断结论的唯一事实源。",
		Parameters: map[string]any{
			"type": "object",
			"properties": map[string]any{
				"check": map[string]any{
					"type":        "string",
					"description": "检查 ID，必须来自 list_checks 的返回",
				},
				"thresholds": map[string]any{
					"type":        "object",
					"description": "可选的阈值覆盖：key 为该检查声明的阈值槽位 Key，value 为非负整数；缺省槽位用库默认值",
					"additionalProperties": map[string]any{
						"type": "integer",
					},
				},
			},
			"required": []string{"check"},
		},
		Parallel: true,
		ReadOnly: true,
	}, func(ctx context.Context, args json.RawMessage, _ *tool.Context) (tool.Result, error) {
		var req struct {
			Check      string         `json:"check"`
			Thresholds map[string]int `json:"thresholds"`
		}
		if err := json.Unmarshal(args, &req); err != nil {
			return tool.Errorf("参数解析失败: %v", err), nil
		}
		metas := diagnose.ListChecks()
		var meta *diagnose.CheckMeta
		for i := range metas {
			if metas[i].ID == req.Check {
				meta = &metas[i]
				break
			}
		}
		if meta == nil {
			return tool.Errorf("未知检查 ID %q；先调用 list_checks 获取可用 ID", req.Check), nil
		}

		var slots []int
		for key, val := range req.Thresholds {
			idx := -1
			for i, th := range meta.Thresholds {
				if th.Key == key {
					idx = i
					break
				}
			}
			if idx < 0 {
				return tool.Errorf("检查 %q 没有阈值槽位 %q；可用槽位: %s", req.Check, key, slotKeys(meta)), nil
			}
			if val < 0 {
				return tool.Errorf("阈值槽位 %q 不允许为负值", key), nil
			}
			for len(slots) <= idx {
				slots = append(slots, 0)
			}
			slots[idx] = val
		}

		opts := diagnose.CheckOptions{
			Timeout:        env.timeout,
			NodeTimeout:    env.nodeTO,
			MaxRowsPerNode: env.maxRows,
			Thresholds:     slots,
		}
		rep, err := diagnose.RunCheck(ctx, req.Check, env.targets, opts)
		if rep == nil {
			// 校验类失败：无报告可给，把错误交给模型修正参数。
			return tool.Errorf("检查未执行: %v", err), nil
		}
		payload := map[string]any{
			"meta":   rep.Header().CheckMeta,
			"report": rep,
		}
		if err != nil {
			// 非校验错误（超时、预算耗尽等）：报告中的证据仍然成立，
			// 按契约先报告后错误，把两者都交给模型。
			payload["execution_error"] = err.Error()
		}
		out, merr := json.MarshalIndent(trimPayload(payload), "", "  ")
		if merr != nil {
			return tool.Errorf("报告序列化失败: %v", merr), nil
		}
		return tool.Text("%s", out), nil
	})
}

// toolResultMaxBytes 是单个工具结果进入模型上下文的大小上限。
// detached_parts 等海量报告（四节点实测单次 1.3MB——单条 match 的
// observations 可达数千个）若原样进入 history，会在后续每轮请求中
// 重复携带，迅速撑爆端点的 prompt 上限。
const toolResultMaxBytes = 32 * 1024

// trimPayload 把超过上限的报告做结构化裁剪，逐级降配直到达标：
// 20 条匹配 × 4 观测 → 10×2 → 5×1 → 仅保留统计。逐级是因为海量报告
// 有两种形态：matches 多（too_many_parts），或单条 match 的
// observations 极多（detached_parts 按表归并），一级裁剪覆盖不全。
// 裁剪是通用的 JSON 操作（归并键都是 matches/observations），不依赖具体类型。
func trimPayload(payload map[string]any) map[string]any {
	if underLimit(payload) {
		return payload
	}
	reportJSON, err := json.Marshal(payload["report"])
	if err != nil {
		return payload
	}
	var report map[string]any
	if err := json.Unmarshal(reportJSON, &report); err != nil {
		return payload
	}
	matches, _ := report["matches"].([]any)
	total := len(matches)

	for _, step := range []struct{ keepM, keepO int }{{20, 4}, {10, 2}, {5, 1}} {
		kept := matches
		if len(matches) > step.keepM {
			kept = matches[:step.keepM]
		}
		for _, m := range kept {
			mo, ok := m.(map[string]any)
			if !ok {
				continue
			}
			if obs, ok := mo["observations"].([]any); ok && len(obs) > step.keepO {
				mo["observations"] = obs[:step.keepO]
			}
		}
		report["matches"] = kept
		report["matches_total"] = total
		report["truncated"] = fmt.Sprintf("报告过大已裁剪：保留前 %d 条匹配（每条 %d 个观测），共 %d 条。需要其余内容请缩小问题范围（如指定库/表）后重跑", len(kept), step.keepO, total)
		payload["report"] = report
		payload["truncated"] = true
		if underLimit(payload) {
			return payload
		}
	}

	// 最终降级：细节全部省略，只留统计与引导。
	report["matches"] = []any{}
	report["truncated"] = fmt.Sprintf("报告过大（共 %d 条匹配），已省略全部细节。请缩小问题范围（如指定库/表）后重跑", total)
	payload["report"] = report
	return payload
}

func underLimit(payload map[string]any) bool {
	out, err := json.Marshal(payload)
	return err == nil && len(out) <= toolResultMaxBytes
}

func slotKeys(meta *diagnose.CheckMeta) string {
	keys := ""
	for i, th := range meta.Thresholds {
		if i > 0 {
			keys += ", "
		}
		keys += th.Key
	}
	if keys == "" {
		return "（无）"
	}
	return keys
}
