package main

import (
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// renderJSON 输出稳定 JSON 契约（缩进两格）。matches 内元素按检查类型不同，
// 字段见 view.go 中各 MatchView；消费方按 check 字段区分。
func renderJSON(w io.Writer, meta runMeta, results []checkView) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(struct {
		RunMeta  runMeta     `json:"run"`
		CheckRun []checkView `json:"checks"`
	}{RunMeta: meta, CheckRun: results})
}

// renderText 输出人类可读报告。核心契约的展示纪律：
//   - matched / no_match / unknown 三态显式呈现，unknown 与 partial 不渲染成健康；
//   - 覆盖缺口（节点失败、未启动、截断）必须与结论一起出现；
//   - 先渲染报告证据，顶层 error 最后呈现。

func renderText(w io.Writer, meta runMeta, results []checkView) error {
	fmt.Fprintf(w, "clickhouse-doctor %s  protocol=%s  targets=%d\n",
		meta.Version, meta.Protocol, len(meta.Targets))
	for _, t := range meta.Targets {
		fmt.Fprintf(w, "  目标 %s (%s) [%s]\n", t.NodeID, t.ShardID, t.Protocol)
	}
	fmt.Fprintf(w, "时间: %s ~ %s\n\n", meta.StartedAt.Format(time.RFC3339), meta.FinishedAt.Format(time.RFC3339))

	for _, r := range results {
		if err := renderCheckText(w, r); err != nil {
			return err
		}
		fmt.Fprintln(w)
	}
	return nil
}

func renderCheckText(w io.Writer, r checkView) error {
	mark := "??"
	switch {
	case r.Outcome == "matched":
		mark = "!!"
	case r.Outcome == "no_match" && r.Execution == "complete":
		mark = "ok"
	case r.Execution == "failed":
		mark = "xx"
	}

	fmt.Fprintf(w, "[%s] %s %s/%s", mark, r.Check, r.Execution, r.Outcome)
	if r.Err != "" {
		fmt.Fprintf(w, "  (error: %s)", r.Err)
	}
	fmt.Fprintln(w)

	for _, t := range r.Targets {
		line := fmt.Sprintf("  目标 %s [%s]", t.NodeID, t.Status)
		if t.Status != "not_started" {
			line += fmt.Sprintf(" rows_read=%d rows_kept=%d", t.RowsRead, t.RowsKept)
		}
		if t.Truncated {
			line += " TRUNCATED(结果不完整)"
		}
		fmt.Fprintln(w, line)
		if t.ErrCategory != "" {
			fmt.Fprintf(w, "    错误类别=%s", t.ErrCategory)
			if t.ErrMessage != "" {
				fmt.Fprintf(w, " 信息=%q", t.ErrMessage)
			}
			fmt.Fprintln(w)
		}
	}

	if len(r.Matches) == 0 {
		switch {
		case r.Outcome == "matched":
			// 不应出现，防御
			fmt.Fprintln(w, "  匹配列表为空但 outcome=matched（内部不一致）")
		case r.Execution != "complete":
			fmt.Fprintln(w, "  覆盖不完整，无法做出匹配判断；不代表无异常")
		default:
			fmt.Fprintln(w, "  已覆盖范围内未发现匹配（不等于集群健康）")
		}
	} else {
		fmt.Fprintf(w, "  匹配 %d 条:\n", len(r.Matches))
		for i, m := range r.Matches {
			if err := renderMatchText(w, i+1, m); err != nil {
				return err
			}
		}
	}

	if len(r.NextSteps) > 0 {
		fmt.Fprintln(w, "  后续核查建议（稳定代码，人工执行，工具不修复）:")
		for _, f := range r.NextSteps {
			if f.Detail != "" {
				fmt.Fprintf(w, "    - %s: %s\n", f.Code, f.Detail)
			} else {
				fmt.Fprintf(w, "    - %s\n", f.Code)
			}
		}
	}
	return nil
}

// maxTextLen 限制文本输出中的服务端文本长度；完整内容以 JSON 输出为准。
const maxTextLen = 160

func clip(s string) string {
	r := []rune(s)
	if len(r) <= maxTextLen {
		return s
	}
	return string(r[:maxTextLen]) + "...(截断,完整内容见 JSON)"
}

func renderMatchText(w io.Writer, idx int, m any) error {
	switch mv := m.(type) {
	case queueFailureMatchView:
		fmt.Fprintf(w, "    %d. %s.%s part=%s\n", idx, mv.Database, mv.Table, mv.PartName)
		for _, o := range mv.Observations {
			fmt.Fprintf(w, "       来源 %s  异常: %s\n", o.NodeID, clip(o.LastException))
		}
	case partsMatchView:
		fmt.Fprintf(w, "    %d. %s.%s 分区=%s 最大 part 数=%d\n", idx, mv.Database, mv.Table, mv.Partition, mv.Parts)
		for _, o := range mv.Observations {
			fmt.Fprintf(w, "       来源 %s  parts=%d 磁盘占用=%s\n", o.NodeID, o.Parts, o.DiskBytes)
		}
	case replicaMatchView:
		fmt.Fprintf(w, "    %d. %s.%s\n", idx, mv.Database, mv.Table)
		for _, o := range mv.Observations {
			fmt.Fprintf(w, "       来源 %s  readonly=%v session_expired=%v delay=%ds active=%d/%d lost_part=%d 理由=%v\n",
				o.NodeID, o.IsReadonly, o.IsSessionExpired, o.AbsoluteDelaySecond,
				o.ActiveReplicas, o.TotalReplicas, o.LostPartCount, o.Reasons)
		}
	case mutationMatchView:
		fmt.Fprintf(w, "    %d. %s.%s mutation=%s\n", idx, mv.Database, mv.Table, mv.MutationID)
		for _, o := range mv.Observations {
			fmt.Fprintf(w, "       来源 %s  未完成时长=%ds 理由=%v\n", o.NodeID, o.AgeSeconds, o.Reasons)
			if o.LatestFailReason != "" {
				fmt.Fprintf(w, "         失败原因: %s\n", clip(o.LatestFailReason))
			}
			if o.Command != "" {
				fmt.Fprintf(w, "         命令: %s\n", clip(o.Command))
			}
		}
	case backlogMatchView:
		fmt.Fprintf(w, "    %d. %s.%s 队列积压 depth=%d 最老=%ds 最大重试=%d\n",
			idx, mv.Database, mv.Table, mv.Depth, mv.OldestAgeSeconds, mv.MaxRetries)
		for _, o := range mv.Observations {
			fmt.Fprintf(w, "       来源 %s  depth=%d oldest=%ds retries=%d failing=%d 理由=%v\n",
				o.NodeID, o.Depth, o.OldestAgeSeconds, o.MaxRetries, o.FailingEntries, o.Reasons)
		}
	case diskMatchView:
		fmt.Fprintf(w, "    %d. 磁盘=%s 路径=%s 最差水位=%d%%\n", idx, mv.DiskName, mv.Path, mv.FreePercent)
		for _, o := range mv.Observations {
			fmt.Fprintf(w, "       来源 %s  free=%d/%dB keep_free=%dB 理由=%v\n",
				o.NodeID, o.FreeBytes, o.TotalBytes, o.KeepFreeBytes, o.Reasons)
		}
	case detachedMatchView:
		fmt.Fprintf(w, "    %d. %s.%s 单节点 detached 总数=%d 理由=%v\n", idx, mv.Database, mv.Table, mv.Parts, mv.Reasons)
		for _, o := range mv.Observations {
			fmt.Fprintf(w, "       来源 %s  part=%s 磁盘=%s 字节=%d reason=%q 逐part理由=%v\n",
				o.NodeID, o.PartName, o.Disk, o.BytesOnDisk, o.Reason, o.Reasons)
		}
	default:
		fmt.Fprintf(w, "    %d. (未知匹配类型 %T)\n", idx, m)
	}
	return nil
}
