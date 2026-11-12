package diagnose

import "sort"

// nodeObservation 是归并前的单节点原始观测。
type nodeObservation struct {
	ShardID       string
	Database      string
	Table         string
	PartName      string
	NodeID        string
	LastException string
}

// reduceMatches 按 (ShardID, Database, Table, PartName) 归并观测。
//
// 不同分片绝不因同名表或同名 part 合并——它们是独立的复制组。
// 同一节点的多条队列记录保留为多个观测，不声称合并后的数量就是队列任务数。
// 归并结果按键稳定排序，保证并发调度顺序不影响输出。
func reduceMatches(obs []nodeObservation) []QueueMatch {
	type key struct{ shard, db, table, part string }
	byKey := map[key]*QueueMatch{}
	var order []key
	for _, o := range obs {
		k := key{o.ShardID, o.Database, o.Table, o.PartName}
		m, ok := byKey[k]
		if !ok {
			m = &QueueMatch{
				ShardID:  o.ShardID,
				Database: o.Database,
				Table:    o.Table,
				PartName: o.PartName,
			}
			byKey[k] = m
			order = append(order, k)
		}
		m.Observations = append(m.Observations, QueueObservation{
			NodeID:        o.NodeID,
			LastException: o.LastException,
		})
	}

	matches := make([]QueueMatch, 0, len(order))
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
		return a.PartName < b.PartName
	})
	return matches
}

// summarizeExecution 汇总执行状态。截断的节点即使读取成功也计入不完整：
// 任何不完整路径都不能产出 complete。有节点产出过行（即使中途失败）说明
// 存在可用观测，算 partial；完全没有可用观测才是 failed。
func summarizeExecution(results []NodeResult) Execution {
	anyOK := false
	anyEvidence := false
	incomplete := false
	for _, r := range results {
		switch r.Status {
		case NodeOK:
			anyOK = true
			if r.Truncated {
				incomplete = true
			}
		case NodeFailed:
			if r.RowsRead > 0 {
				anyEvidence = true
			}
			incomplete = true
		case NodeNotStarted:
			incomplete = true
		}
	}
	switch {
	case anyOK && !incomplete:
		return ExecutionComplete
	case anyOK || anyEvidence:
		return ExecutionPartial
	default:
		return ExecutionFailed
	}
}

// summarizeOutcome 依据执行状态与匹配计算检查结果。
// 覆盖不完整且没有匹配时是 unknown：不能把"只查了一部分没发现"说成"没有"。
func summarizeOutcome(exe Execution, matches []QueueMatch) Outcome {
	if len(matches) > 0 {
		return OutcomeMatched
	}
	if exe == ExecutionComplete {
		return OutcomeNoMatch
	}
	return OutcomeUnknown
}

// buildFollowUps 依据结果生成后续核查建议。建议只引导核查，不构成修复方案。
func buildFollowUps(exe Execution, results []NodeResult, matches []QueueMatch) []FollowUp {
	var steps []FollowUp
	if len(matches) > 0 {
		steps = append(steps,
			FollowUp{
				Code:   AdviceVerifyReplicasForPart,
				Detail: "在匹配分片的各副本上核对 part 是否存在及覆盖情况；匹配基于队列历史异常，异常可能已是历史状态",
			},
			FollowUp{
				Code:   AdviceInspectQueueOnSourceNodes,
				Detail: "在来源节点核对对应队列条目的创建时间与重试情况，确认是否仍然卡住",
			},
		)
	}
	if exe != ExecutionComplete {
		steps = append(steps, FollowUp{
			Code:   AdviceCompleteNodeCoverage,
			Detail: "本次检查未完整覆盖全部目标节点；未覆盖范围不影响已有证据，但异常清单可能不完整",
		})
	}
	for _, r := range results {
		if r.Truncated {
			steps = append(steps, FollowUp{
				Code:   AdviceRerunWithHigherRowLimit,
				Detail: "部分节点结果达到保留上限被截断，复跑并提高 MaxRowsPerNode 才能保证清单完整",
			})
			break
		}
	}
	return steps
}
