package diagnose

import "sort"

// partsObservation 是归并前的单节点分区统计。
type partsObservation struct {
	ShardID   string
	NodeID    string
	Database  string
	Table     string
	Partition string
	Parts     int    // 该节点上该分区的活跃 part 数
	DiskBytes string // 该节点上该分区的磁盘占用（服务端 toString 后的十进制字符串）
}

// reducePartsObservations 按 (ShardID, Database, Table, Partition) 归并观测。
//
// 不同分片绝不因同名表或同名分区合并——它们是独立的复制组。同一分区
// 多个节点报告时保留每个来源节点；Parts 取各节点中的最大值——单副本
// 分区碎片化本身就是问题，跨副本取最大而不是平均。
// 归并结果按键稳定排序，保证并发调度顺序不影响输出。
func reducePartsObservations(obs []partsObservation) []PartsMatch {
	type key struct{ shard, db, table, partition string }
	byKey := map[key]*PartsMatch{}
	var order []key
	for _, o := range obs {
		k := key{o.ShardID, o.Database, o.Table, o.Partition}
		m, ok := byKey[k]
		if !ok {
			m = &PartsMatch{
				ShardID:   o.ShardID,
				Database:  o.Database,
				Table:     o.Table,
				Partition: o.Partition,
			}
			byKey[k] = m
			order = append(order, k)
		}
		m.Observations = append(m.Observations, PartsObservation{
			NodeID:    o.NodeID,
			Parts:     o.Parts,
			DiskBytes: o.DiskBytes,
		})
	}

	matches := make([]PartsMatch, 0, len(order))
	for _, k := range order {
		m := byKey[k]
		sort.SliceStable(m.Observations, func(i, j int) bool {
			return m.Observations[i].NodeID < m.Observations[j].NodeID
		})
		// Parts 取观测中的最大值。
		for _, o := range m.Observations {
			if o.Parts > m.Parts {
				m.Parts = o.Parts
			}
		}
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
		// 分区按字符串逆序：ISO 日期等按时间递增的格式下，更新的分区在前。
		return a.Partition > b.Partition
	})
	return matches
}

// buildPartsFollowUps 依据结果生成后续核查建议。建议只引导核查，不构成修复方案。
func buildPartsFollowUps(exe Execution, results []NodeResult, matches []PartsMatch) []FollowUp {
	var steps []FollowUp
	if len(matches) > 0 {
		steps = append(steps,
			FollowUp{
				Code:   AdviceInspectMergeThroughput,
				Detail: "在匹配节点上核对 system.merges 中同表是否有进行中的合并；合并长期积压说明后台合并吞吐不足",
			},
			FollowUp{
				Code:   AdviceReviewInsertBatchSize,
				Detail: "高频小批量 INSERT 会产生大量小 part；核对写入批次大小或考虑服务端 async_insert",
			},
			FollowUp{
				Code:   AdviceReviewPartitionKey,
				Detail: "分区基数远超数据保留粒度说明分区键过细；调整 PARTITION BY 需要重建表",
			},
		)
	}
	return append(steps, coverageFollowUps(exe, results)...)
}
