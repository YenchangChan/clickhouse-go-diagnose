package diagnose

import "sort"

// backlogObservation 是归并前的单节点队列积压聚合观测。
type backlogObservation struct {
	ShardID  string
	NodeID   string
	Database string
	Table    string

	Depth          int
	OldestAge      int // 秒，服务端 dateDiff 计算
	MaxRetries     int
	FailingEntries int

	Reasons []string
}

// reduceBacklogObservations 按 (ShardID, Database, Table) 归并观测。
//
// 不同分片绝不因同名表合并——它们是独立的复制组。同一复制表的多个
// 副本节点各自维护独立队列，观测保留每个来源节点；深度/最老年龄/最大
// 重试次数取各节点中的最大值——任一副本积压本身就构成证据，不做跨副本
// 平均或求和（各副本队列独立，总数没有共同语义）。
// 归并结果按键稳定排序，保证并发调度顺序不影响输出。
func reduceBacklogObservations(obs []backlogObservation) []QueueBacklogMatch {
	type key struct{ shard, db, table string }
	byKey := map[key]*QueueBacklogMatch{}
	var order []key
	for _, o := range obs {
		k := key{o.ShardID, o.Database, o.Table}
		m, ok := byKey[k]
		if !ok {
			m = &QueueBacklogMatch{
				ShardID:  o.ShardID,
				Database: o.Database,
				Table:    o.Table,
			}
			byKey[k] = m
			order = append(order, k)
		}
		m.Observations = append(m.Observations, QueueBacklogObservation{
			NodeID:           o.NodeID,
			Depth:            o.Depth,
			OldestAgeSeconds: o.OldestAge,
			MaxRetries:       o.MaxRetries,
			FailingEntries:   o.FailingEntries,
			Reasons:          o.Reasons,
		})
		if o.Depth > m.Depth {
			m.Depth = o.Depth
		}
		if o.OldestAge > m.OldestAgeSeconds {
			m.OldestAgeSeconds = o.OldestAge
		}
		if o.MaxRetries > m.MaxRetries {
			m.MaxRetries = o.MaxRetries
		}
	}

	matches := make([]QueueBacklogMatch, 0, len(order))
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
		return a.Table < b.Table
	})
	return matches
}

// buildBacklogFollowUps 依据结果生成后续核查建议。建议只引导核查，不构成修复方案。
func buildBacklogFollowUps(exe Execution, results []NodeResult, matches []QueueBacklogMatch) []FollowUp {
	var steps []FollowUp
	if len(matches) > 0 {
		steps = append(steps,
			FollowUp{
				Code:   AdviceInspectQueueOnSourceNodes,
				Detail: "在来源节点核对队列条目的类型、创建时间与重试次数分布，确认积压是否仍在增长",
			},
			FollowUp{
				Code:   AdviceVerifyKeeperConnectivity,
				Detail: "队列长期不消化通常与副本会话中断或 Keeper 可用性相关，核对各节点与 Keeper 的连通性",
			},
			FollowUp{
				Code:   AdviceInspectReplicaDelay,
				Detail: "比较各副本的 absolute_delay，确认积压是否已造成副本落后扩大",
			},
		)
	}
	return append(steps, coverageFollowUps(exe, results)...)
}
