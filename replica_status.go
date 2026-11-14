package diagnose

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"
)

const (
	replicaStatusCheckID     = "replica_status"
	replicaStatusRuleVersion = "2"
	// defaultDelayThreshold 副本 absolute_delay 的判定阈值（秒）。
	// 取常规监控的告警档位：超过 5 分钟的持续落后通常意味着拉取/合并
	// 吞吐不足或副本半死；短时抖动由复跑确认，不在此猜规则。
	defaultDelayThreshold = 300
)

// ReplicaStatusOptions 控制检查的执行预算与判定阈值。零值字段使用默认值，负值报错。
type ReplicaStatusOptions struct {
	// Timeout 是整次检查的总预算，包含调度等待。默认 10s。
	Timeout time.Duration
	// NodeTimeout 是单个节点的查询与读取预算。默认 2s。
	NodeTimeout time.Duration
	// Concurrency 是并发 worker 数上限。默认 4。
	Concurrency int
	// MaxRowsPerNode 是单节点保留的异常条目上限；读取到第 N+1 条即停止并标记截断。
	// 默认 1000。
	MaxRowsPerNode int
	// DelayThresholdSeconds 是副本 absolute_delay 的判定阈值（秒）。默认 300。
	DelayThresholdSeconds int
}

// CheckReplicaStatus 检查指定节点上复制表的副本状态，判定规则：
//
//   - total_replicas = 0（节点读不到 Keeper 上的副本清单；物理不可能值，
//     Keeper 恢复后逐表理由可能全部消失，本理由是环境级证据）；
//   - is_readonly 非 0（副本只读，通常源于 Keeper 连接/会话异常）；
//   - is_session_expired 非 0（会话过期）；
//   - active_replicas < total_replicas（复制组存在未激活副本）；
//   - absolute_delay >= DelayThresholdSeconds（副本持续落后）；
//   - lost_part_count > 0（副本记录了永久性丢件）。
//
// 每条观测附带命中的稳定理由码（Reason* 常量）。返回匹配的复制表、
// 各节点证据、采集时间与检查覆盖情况。
//
// 结果语义（见 ReplicaStatusReport）：
//   - 匹配说明复制状态值得人工核查，不等于集群不可用；例如复制组有
//     未激活副本时，健康副本仍可服务读写；
//   - absolute_delay 是相对复制组内最先进副本的落后程度；极长时间
//     无写入的表在个别服务器版本上可能出现虚高值，判定结果应结合
//     写入频率人工确认；
//   - 单副本表的 total/active 恒为 1，不产生缺口类匹配，这是诚实结果
//     而非遗漏；
//   - 无匹配不等于集群健康；部分节点失败时保留有效证据并标记不完整。
//
// 输入不合法时返回包装 ErrInvalidInput 的错误，不执行任何查询，报告为零值。
// 检查失败（所有目标无有效观测）时返回报告（Execution=failed）与
// ErrNoUsableObservation。取消或总预算耗尽时返回报告与对应 context 错误；
// 已完整获得的证据保留在报告中。
func CheckReplicaStatus(
	ctx context.Context,
	targets []Target,
	opts ReplicaStatusOptions,
) (ReplicaStatusReport, error) {
	if err := validateTargets(targets); err != nil {
		return ReplicaStatusReport{}, err
	}
	if opts.Timeout < 0 || opts.NodeTimeout < 0 || opts.Concurrency < 0 || opts.MaxRowsPerNode < 0 || opts.DelayThresholdSeconds < 0 {
		return ReplicaStatusReport{}, inputError("预算与阈值不允许为负值")
	}
	if opts.DelayThresholdSeconds == 0 {
		opts.DelayThresholdSeconds = defaultDelayThreshold
	}

	b := budget{
		timeout:     opts.Timeout,
		nodeTimeout: opts.NodeTimeout,
		concurrency: opts.Concurrency,
		maxRows:     opts.MaxRowsPerNode,
	}
	fillBudget(&b, defaultTimeout, defaultNodeTimeout, defaultConcurrency, defaultMaxRowsPerNode)

	c := collector[replicaObservation]{
		sql: replicaStatusSQL,
		scan: func(t Target, rows Rows) (replicaObservation, bool, error) {
			var (
				db, table                  sql.NullString
				readonly, sessionExpired   sql.NullString
				delay, total, active, lost sql.NullString
			)
			if err := rows.Scan(&db, &table, &readonly, &sessionExpired, &delay, &total, &active, &lost); err != nil {
				return replicaObservation{}, false, err
			}

			parse := func(v string) (int, error) {
				n, err := strconv.ParseUint(v, 10, 64)
				return int(n), err
			}
			// 计数列由服务端 toString 下发，理论上恒为十进制数字；
			// 解析失败按 scan 失败处理，不做静默猜测。
			ro, errRo := parse(readonly.String)
			se, errSe := parse(sessionExpired.String)
			d, errDelay := parse(delay.String)
			tot, errTotal := parse(total.String)
			act, errActive := parse(active.String)
			lp, errLost := parse(lost.String)
			for _, e := range []error{errRo, errSe, errDelay, errTotal, errActive, errLost} {
				if e != nil {
					return replicaObservation{}, false, fmt.Errorf("diagnose: system.replicas 计数列解析失败 (%s.%s): %w", db.String, table.String, e)
				}
			}

			// 判定在客户端完成：规则按声明顺序生成理由码。
			var reasons []string
			if tot == 0 {
				reasons = append(reasons, ReasonKeeperUnreachable)
			}
			if ro != 0 {
				reasons = append(reasons, ReasonReplicaReadonly)
			}
			if se != 0 {
				reasons = append(reasons, ReasonSessionExpired)
			}
			if act < tot {
				reasons = append(reasons, ReasonActiveReplicaDeficit)
			}
			if d >= opts.DelayThresholdSeconds {
				reasons = append(reasons, ReasonReplicaDelayExceeded)
			}
			if lp > 0 {
				reasons = append(reasons, ReasonLostParts)
			}
			if len(reasons) == 0 {
				return replicaObservation{}, false, nil
			}

			return replicaObservation{
				ShardID:          t.ShardID,
				NodeID:           t.NodeID,
				Database:         db.String,
				Table:            table.String,
				IsReadonly:       ro != 0,
				IsSessionExpired: se != 0,
				AbsoluteDelay:    d,
				TotalReplicas:    tot,
				ActiveReplicas:   act,
				LostPartCount:    lp,
				Reasons:          reasons,
			}, true, nil
		},
	}

	start := time.Now()
	results, observations, cerr := collect(ctx, targets, b, c)

	matches := reduceReplicaObservations(observations)
	exe := summarizeExecution(results)
	outcome := outcomeFor(exe, len(matches) > 0)
	report := ReplicaStatusReport{
		CheckID:     replicaStatusCheckID,
		RuleVersion: replicaStatusRuleVersion,
		StartedAt:   start,
		FinishedAt:  time.Now(),
		Execution:   exe,
		Outcome:     outcome,
		Targets:     results,
		Matches:     matches,
		NextSteps:   buildReplicaFollowUps(exe, results, matches),
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
