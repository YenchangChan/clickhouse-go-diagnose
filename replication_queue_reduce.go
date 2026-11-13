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
	return outcomeFor(exe, len(matches) > 0)
}

// outcomeFor 是检查无关的结果判定：有匹配即 matched；完整覆盖且无匹配
// 才是 no_match；其余（覆盖不完整或失败）一律 unknown。
func outcomeFor(exe Execution, hasMatches bool) Outcome {
	if hasMatches {
		return OutcomeMatched
	}
	if exe == ExecutionComplete {
		return OutcomeNoMatch
	}
	return OutcomeUnknown
}
