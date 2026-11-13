package diagnose

import "sort"

// diskObservation 是归并前的单节点磁盘水位观测。
type diskObservation struct {
	ShardID  string
	NodeID   string
	DiskName string
	Path     string

	FreeBytes     uint64
	TotalBytes    uint64
	KeepFreeBytes uint64
	FreePercent   int // 0-100；total_space=0 时为 -1（不可计算）

	Reasons []string
}

// reduceDiskObservations 按 (ShardID, DiskName, Path) 归并观测。
//
// 同一分片内不同节点的同名同路径磁盘属于同一类卷，观测保留每个来源节点；
// 匹配级 FreePercent 取观测中最小值（最差水位）——任一副本磁盘快满本身
// 就是证据，不做跨节点平均。不同分片绝不合并。
// 归并结果按键稳定排序，保证并发调度顺序不影响输出。
func reduceDiskObservations(obs []diskObservation) []DiskSpaceMatch {
	type key struct{ shard, name, path string }
	byKey := map[key]*DiskSpaceMatch{}
	var order []key
	for _, o := range obs {
		k := key{o.ShardID, o.DiskName, o.Path}
		m, ok := byKey[k]
		if !ok {
			m = &DiskSpaceMatch{
				ShardID:     o.ShardID,
				DiskName:    o.DiskName,
				Path:        o.Path,
				FreePercent: -1,
			}
			byKey[k] = m
			order = append(order, k)
		}
		m.Observations = append(m.Observations, DiskObservation{
			NodeID:        o.NodeID,
			FreeBytes:     o.FreeBytes,
			TotalBytes:    o.TotalBytes,
			KeepFreeBytes: o.KeepFreeBytes,
			FreePercent:   o.FreePercent,
			Reasons:       o.Reasons,
		})
		if o.FreePercent >= 0 && (m.FreePercent < 0 || o.FreePercent < m.FreePercent) {
			m.FreePercent = o.FreePercent
		}
	}

	matches := make([]DiskSpaceMatch, 0, len(order))
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
		if a.DiskName != b.DiskName {
			return a.DiskName < b.DiskName
		}
		return a.Path < b.Path
	})
	return matches
}

// buildDiskFollowUps 依据结果生成后续核查建议。建议只引导核查，不构成修复方案。
func buildDiskFollowUps(exe Execution, results []NodeResult, matches []DiskSpaceMatch) []FollowUp {
	var steps []FollowUp
	if len(matches) > 0 {
		steps = append(steps,
			FollowUp{
				Code:   AdviceInspectDiskUsage,
				Detail: "在匹配节点上按表/分区核对磁盘占用（system.parts 聚合），定位增长最快的表与分区",
			},
			FollowUp{
				Code:   AdviceReviewTTLRetention,
				Detail: "核对表的 TTL 与保留策略是否生效，以及是否有可清理的历史分区或 detached 数据",
			},
		)
	}
	return append(steps, coverageFollowUps(exe, results)...)
}
