package diagnose

import "sort"

// detachedObservation 是归并前的单节点 detached part 观测。
//
// 与其他检查"只有命中理由的观测才保留"不同，这里所有行都保留：
// 空 reason 的用户 detach 也是单表 detached 总数的组成部分，
// 是数量阈值规则的证据。是否构成匹配由归并层决定。
type detachedObservation struct {
	ShardID  string
	NodeID   string
	Database string
	Table    string
	PartName string
	Reason   string // 服务端 reason 原始值；空表示用户通过 ALTER 手工 detach
	Disk     string

	BytesOnDisk uint64

	Reasons []string // 逐 part 判定理由（ReasonDetachedBroken / ReasonDetachedServerInitiated）
}

// reduceDetachedObservations 按 (ShardID, Database, Table) 归并观测。
//
// 同一复制表的多个副本节点各自维护独立的 detached 目录，观测保留每个
// 来源节点；Parts 取各节点观测中的最大值（单节点视角的 detached 总数），
// 跨节点不累加——副本间是同一份数据的不同视图，累加会重复计数。
// 匹配条件：任一观测带逐 part 理由，或任一节点单表 detached 总数达到阈值。
// 归并结果按键稳定排序，保证并发调度顺序不影响输出。
func reduceDetachedObservations(obs []detachedObservation, countThreshold int) []DetachedMatch {
	type key struct{ shard, db, table string }
	byKey := map[key]*DetachedMatch{}
	var order []key
	for _, o := range obs {
		k := key{o.ShardID, o.Database, o.Table}
		m, ok := byKey[k]
		if !ok {
			m = &DetachedMatch{
				ShardID:  o.ShardID,
				Database: o.Database,
				Table:    o.Table,
			}
			byKey[k] = m
			order = append(order, k)
		}
		m.Observations = append(m.Observations, DetachedObservation{
			NodeID:      o.NodeID,
			PartName:    o.PartName,
			Reason:      o.Reason,
			Disk:        o.Disk,
			BytesOnDisk: o.BytesOnDisk,
			Reasons:     o.Reasons,
		})
		perNode := perNodeCount(m, o.NodeID)
		if perNode > m.Parts {
			m.Parts = perNode
		}
	}

	matches := make([]DetachedMatch, 0, len(order))
	for _, k := range order {
		m := byKey[k]
		hasPartReason := false
		for _, o := range m.Observations {
			if len(o.Reasons) > 0 {
				hasPartReason = true
				break
			}
		}
		if !hasPartReason && m.Parts < countThreshold {
			continue
		}
		if m.Parts >= countThreshold {
			m.Reasons = append(m.Reasons, ReasonDetachedCountExceeded)
		}
		sort.SliceStable(m.Observations, func(i, j int) bool {
			return m.Observations[i].PartName < m.Observations[j].PartName
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
		return a.Table < b.Table
	})
	return matches
}

// perNodeCount 统计匹配内来自单个节点的观测数。
func perNodeCount(m *DetachedMatch, nodeID string) int {
	n := 0
	for _, o := range m.Observations {
		if o.NodeID == nodeID {
			n++
		}
	}
	return n
}

// buildDetachedFollowUps 依据结果生成后续核查建议。建议只引导核查，不构成修复方案。
func buildDetachedFollowUps(exe Execution, results []NodeResult, matches []DetachedMatch) []FollowUp {
	var steps []FollowUp
	if len(matches) > 0 {
		broken := false
		for _, m := range matches {
			for _, o := range m.Observations {
				for _, r := range o.Reasons {
					if r == ReasonDetachedBroken {
						broken = true
					}
				}
			}
		}
		if broken {
			steps = append(steps, FollowUp{
				Code:   AdviceInvestigateBrokenPart,
				Detail: "匹配中存在服务端判定损坏的 part：核对 part 来源与其他副本的对应数据，评估 ATTACH 恢复或重新同步副本",
			})
		}
		steps = append(steps, FollowUp{
			Code:   AdviceInspectDetachedParts,
			Detail: "在匹配节点上核对 detached 目录内容：确认这些 part 是等待 ATTACH、待清理还是故障残留，并据此规划处理",
		})
	}
	return append(steps, coverageFollowUps(exe, results)...)
}
