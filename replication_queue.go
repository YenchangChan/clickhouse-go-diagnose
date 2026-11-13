package diagnose

import (
	"context"
	"database/sql"
	"time"
)

// ReplicationQueueOptions 控制检查的执行预算。零值字段使用默认值，负值报错。
// 默认值是初始配置，需集成测试验证（见 docs/implementation-plan.md M3）。
type ReplicationQueueOptions struct {
	// Timeout 是整次检查的总预算，包含调度等待。默认 10s。
	Timeout time.Duration
	// NodeTimeout 是单个节点的查询与读取预算。默认 2s。
	NodeTimeout time.Duration
	// Concurrency 是并发 worker 数上限。默认 4。
	Concurrency int
	// MaxRowsPerNode 是单节点保留的匹配行上限；读取到第 N+1 条即停止并标记截断。
	// 默认 1000。
	MaxRowsPerNode int
}

// CheckReplicationQueueFailures 检查指定节点复制队列中记录的
// NO_REPLICA_HAS_PART 异常，返回受影响对象、证据来源、采集时间与检查覆盖情况。
//
// 结果语义（见 ReplicationQueueReport）：
//   - 匹配异常记录不等于确认数据永久丢失，也不确定故障起因；
//   - 无匹配不等于集群健康；
//   - 部分节点失败时保留有效证据并标记不完整，不声称已列出全部异常。
//
// 输入不合法时返回包装 ErrInvalidInput 的错误，不执行任何查询，报告为零值。
// 检查失败（所有目标无有效观测）时返回报告（Execution=failed）与
// ErrNoUsableObservation。取消或总预算耗尽时返回报告与对应 context 错误；
// 已完整获得的证据保留在报告中。
func CheckReplicationQueueFailures(
	ctx context.Context,
	targets []Target,
	opts ReplicationQueueOptions,
) (ReplicationQueueReport, error) {
	if err := validateTargets(targets); err != nil {
		return ReplicationQueueReport{}, err
	}
	if opts.Timeout < 0 || opts.NodeTimeout < 0 || opts.Concurrency < 0 || opts.MaxRowsPerNode < 0 {
		return ReplicationQueueReport{}, inputError("预算项不允许为负值")
	}

	b := budget{
		timeout:     opts.Timeout,
		nodeTimeout: opts.NodeTimeout,
		concurrency: opts.Concurrency,
		maxRows:     opts.MaxRowsPerNode,
	}
	fillBudget(&b, defaultTimeout, defaultNodeTimeout, defaultConcurrency, defaultMaxRowsPerNode)

	c := collector[nodeObservation]{
		sql: replicationQueueSQL,
		scan: func(t Target, rows Rows) (nodeObservation, bool, error) {
			var db, table, part, lastExcp sql.NullString
			if err := rows.Scan(&db, &table, &part, &lastExcp); err != nil {
				return nodeObservation{}, false, err
			}
			excp := lastExcp.String
			if !matchesException(excp) {
				return nodeObservation{}, false, nil
			}
			return nodeObservation{
				ShardID:       t.ShardID,
				NodeID:        t.NodeID,
				Database:      db.String,
				Table:         table.String,
				PartName:      part.String,
				LastException: excp,
			}, true, nil
		},
	}

	start := time.Now()
	results, observations, cerr := collect(ctx, targets, b, c)

	matches := reduceMatches(observations)
	exe := summarizeExecution(results)
	outcome := summarizeOutcome(exe, matches)
	report := ReplicationQueueReport{
		CheckID:     checkID,
		RuleVersion: ruleVersion,
		StartedAt:   start,
		FinishedAt:  time.Now(),
		Execution:   exe,
		Outcome:     outcome,
		Targets:     results,
		Matches:     matches,
		NextSteps:   buildFollowUps(exe, results, matches),
	}

	// 取消或总预算耗尽优先返回 context 错误；报告中的证据仍然成立。
	if cerr != nil {
		return report, cerr
	}
	if exe == ExecutionFailed && len(observations) == 0 {
		return report, ErrNoUsableObservation
	}
	return report, nil
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
	return append(steps, coverageFollowUps(exe, results)...)
}
