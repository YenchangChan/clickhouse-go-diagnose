package diagnose

import "sort"

// replicaObservation 是归并前的单节点副本状态观测。
type replicaObservation struct {
	ShardID  string
	NodeID   string
	Database string
	Table    string

	IsReadonly       bool
	IsSessionExpired bool
	AbsoluteDelay    int // 秒
	TotalReplicas    int
	ActiveReplicas   int
	LostPartCount    int

	// Reasons 是该观测命中的稳定理由码（见 Reason* 常量），
	// 顺序固定为规则声明顺序，不随执行路径变化。
	Reasons []string
}

// reduceReplicaObservations 按 (ShardID, Database, Table) 归并观测。
//
// 复制组按分片独立：不同分片绝不因同名表合并。同一表多个副本节点
// 报告时保留每个来源节点——不同副本对同一复制组的观测可能不同
// （例如故障副本自身报 readonly，健康副本报 active 缺口），这正是证据。
// 归并结果按键稳定排序，保证并发调度顺序不影响输出。
func reduceReplicaObservations(obs []replicaObservation) []ReplicaMatch {
	type key struct{ shard, db, table string }
	byKey := map[key]*ReplicaMatch{}
	var order []key
	for _, o := range obs {
		k := key{o.ShardID, o.Database, o.Table}
		m, ok := byKey[k]
		if !ok {
			m = &ReplicaMatch{
				ShardID:  o.ShardID,
				Database: o.Database,
				Table:    o.Table,
			}
			byKey[k] = m
			order = append(order, k)
		}
		m.Observations = append(m.Observations, ReplicaObservation{
			NodeID:           o.NodeID,
			IsReadonly:       o.IsReadonly,
			IsSessionExpired: o.IsSessionExpired,
			AbsoluteDelay:    o.AbsoluteDelay,
			TotalReplicas:    o.TotalReplicas,
			ActiveReplicas:   o.ActiveReplicas,
			LostPartCount:    o.LostPartCount,
			Reasons:          append([]string(nil), o.Reasons...),
		})
	}

	matches := make([]ReplicaMatch, 0, len(order))
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

// buildReplicaFollowUps 依据命中理由生成后续核查建议，每个命中的理由
// 至多生成一条；建议只引导核查，不构成修复方案。
func buildReplicaFollowUps(exe Execution, results []NodeResult, matches []ReplicaMatch) []FollowUp {
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
	// 多个理由映射同一建议代码时（如 keeper_unreachable/readonly/expired 都
	// 指向 verify_keeper_connectivity）只保留最先命中的一条，建议按 code 去重。
	seen := map[string]bool{}
	add := func(reason, code, detail string) {
		if hits[reason] && !seen[code] {
			seen[code] = true
			steps = append(steps, FollowUp{Code: code, Detail: detail})
		}
	}
	add(ReasonKeeperUnreachable, AdviceVerifyKeeperConnectivity,
		"节点读不到 Keeper 上的副本清单（total_replicas=0）；先核对 Keeper 服务与网络连通性，恢复后逐表状态会自动刷新，无需逐表处置")
	add(ReasonReplicaReadonly, AdviceVerifyKeeperConnectivity,
		"副本只读通常源于 Keeper 连接或会话异常；在匹配节点上核对与 Keeper 的连通性与会话状态")
	add(ReasonSessionExpired, AdviceVerifyKeeperConnectivity,
		"副本会话已过期；确认 Keeper 服务状态与网络可达性，会话恢复后副本会自动退出只读")
	add(ReasonActiveReplicaDeficit, AdviceLocateInactiveReplica,
		"复制组存在未激活副本；对照各副本节点确认是哪个副本掉线及其原因（进程、网络、会话）")
	add(ReasonReplicaDelayExceeded, AdviceInspectReplicaDelay,
		"副本落后超过阈值；比较各副本的 absolute_delay 并核对 system.merges / 拉取活动，确认落后是否还在扩大")
	add(ReasonLostParts, AdviceInvestigateLostParts,
		"副本报告存在永久性丢件记录；在该副本上核对 system.replication_queue 与其他副本的数据完整性，评估恢复手段")
	return append(steps, coverageFollowUps(exe, results)...)
}
