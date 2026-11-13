package diagnose

import "sort"

// mutationObservation 是归并前的单节点 mutation 观测。
type mutationObservation struct {
	ShardID          string
	NodeID           string
	Database         string
	Table            string
	MutationID       string
	Command          string
	AgeSeconds       int    // 服务端 dateDiff 计算的未完成时长（秒）
	LatestFailReason string // 原始异常文本，可能包含敏感信息；空表示从未失败
	Reasons          []string
}

// reduceMutationObservations 按 (ShardID, Database, Table, MutationID) 归并观测。
//
// 复制组按分片独立：不同分片绝不因同名表或同 ID 合并。复制表的同一个
// mutation 会出现在每个副本节点的 system.mutations 里，多个节点报告时
// 合并展示但保留每个来源节点——各副本的失败状态可能不同（部分副本
// 已重试成功），这正是证据。归并结果按键稳定排序，保证并发调度顺序
// 不影响输出。
func reduceMutationObservations(obs []mutationObservation) []MutationMatch {
	type key struct{ shard, db, table, mutationID string }
	byKey := map[key]*MutationMatch{}
	var order []key
	for _, o := range obs {
		k := key{o.ShardID, o.Database, o.Table, o.MutationID}
		m, ok := byKey[k]
		if !ok {
			m = &MutationMatch{
				ShardID:    o.ShardID,
				Database:   o.Database,
				Table:      o.Table,
				MutationID: o.MutationID,
			}
			byKey[k] = m
			order = append(order, k)
		}
		m.Observations = append(m.Observations, MutationObservation{
			NodeID:           o.NodeID,
			Command:          o.Command,
			AgeSeconds:       o.AgeSeconds,
			LatestFailReason: o.LatestFailReason,
			Reasons:          append([]string(nil), o.Reasons...),
		})
	}

	matches := make([]MutationMatch, 0, len(order))
	for _, k := range order {
		m := byKey[k]
		sort.SliceStable(m.Observations, func(i, j int) bool {
			return m.Observations[i].NodeID < m.Observations[j].NodeID
		})
		matches = append(matches, *m)
	}
	sort.SliceStable(matches, func(i, j int) bool {
		a, b := matches[i], matches[j]
		if a.ShardID != b.ShardID {
			return a.ShardID < b.ShardID
		}
		if a.Database != b.Database {
			return a.Database < b.Database
		}
		if a.Table != b.Table {
			return a.Table < b.Table
		}
		return a.MutationID < b.MutationID
	})
	return matches
}

// buildMutationFollowUps 依据命中理由生成后续核查建议，每个命中的理由
// 至多生成一条；建议只引导核查，不构成修复方案。
func buildMutationFollowUps(exe Execution, results []NodeResult, matches []MutationMatch) []FollowUp {
	hits := map[string]bool{}
	for _, m := range matches {
		for _, o := range m.Observations {
			for _, r := range o.Reasons {
				hits[r] = true
			}
		}
	}
	var steps []FollowUp
	// 按固定顺序输出，保证同输入下建议列表稳定。
	add := func(reason, code, detail string) {
		if hits[reason] {
			steps = append(steps, FollowUp{Code: code, Detail: detail})
		}
	}
	add(ReasonMutationFailing, AdviceInspectMutationFailure,
		"mutation 在服务端持续失败并重试；核对 latest_fail_reason 与数据/模式冲突，确认修复手段（调整命令、修复数据或 KILL MUTATION）")
	add(ReasonMutationLongRunning, AdviceCheckMutationProgress,
		"mutation 未完成且已超过年龄阈值；大表慢 mutation 可能是正常现象，结合 parts_to_do 与磁盘吞吐确认是否真的在推进")
	return append(steps, coverageFollowUps(exe, results)...)
}
