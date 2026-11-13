package diagnose

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"
)

const (
	mutationsStuckCheckID     = "mutations_stuck"
	mutationsStuckRuleVersion = "1"
	// defaultAgeThreshold 未完成 mutation 的年龄判定阈值（秒）。
	// 大表的慢 mutation 属正常现象，阈值只表示"超过一天仍未完成值得
	// 人工核查是否在推进"，不是服务端限制，也过高的默认值刻意避免把
	// 正常的长任务误报为卡住。
	defaultAgeThreshold = 86400
)

// MutationsStuckOptions 控制检查的执行预算与判定阈值。零值字段使用默认值，负值报错。
type MutationsStuckOptions struct {
	// Timeout 是整次检查的总预算，包含调度等待。默认 10s。
	Timeout time.Duration
	// NodeTimeout 是单个节点的查询与读取预算。默认 2s。
	NodeTimeout time.Duration
	// Concurrency 是并发 worker 数上限。默认 4。
	Concurrency int
	// MaxRowsPerNode 是单节点保留的异常条目上限；读取到第 N+1 条即停止并标记截断。
	// 默认 1000。
	MaxRowsPerNode int
	// AgeThresholdSeconds 是未完成 mutation 的年龄判定阈值（秒）。默认 86400（一天）。
	AgeThresholdSeconds int
}

// CheckMutationsStuck 检查指定节点上卡住的 mutation（system.mutations 中
// 未完成条目），判定规则：
//
//   - latest_fail_reason 非空（服务端失败且仍在队列重试）；
//   - 未完成时长 >= AgeThresholdSeconds（长时间未完成，需人工确认是否
//     真的在推进——大表的慢 mutation 可能是正常现象）。
//
// 每条观测附带命中的稳定理由码（ReasonMutation* 常量）。已完成条目
// （is_done=1）由固定查询在服务端过滤——检查报告的是"当前卡住"，
// 不是"曾经失败"；失败后重试成功的 mutation 不在结果里。
//
// 结果语义（见 MutationsStuckReport）：
//   - 匹配说明 mutation 状态值得人工核查，不等于集群不可用；
//   - 无匹配不等于集群健康；部分节点失败时保留有效证据并标记不完整。
//
// 输入不合法时返回包装 ErrInvalidInput 的错误，不执行任何查询，报告为零值。
// 检查失败（所有目标无有效观测）时返回报告（Execution=failed）与
// ErrNoUsableObservation。取消或总预算耗尽时返回报告与对应 context 错误；
// 已完整获得的证据保留在报告中。
func CheckMutationsStuck(
	ctx context.Context,
	targets []Target,
	opts MutationsStuckOptions,
) (MutationsStuckReport, error) {
	if err := validateTargets(targets); err != nil {
		return MutationsStuckReport{}, err
	}
	if opts.Timeout < 0 || opts.NodeTimeout < 0 || opts.Concurrency < 0 || opts.MaxRowsPerNode < 0 || opts.AgeThresholdSeconds < 0 {
		return MutationsStuckReport{}, inputError("预算与阈值不允许为负值")
	}
	if opts.AgeThresholdSeconds == 0 {
		opts.AgeThresholdSeconds = defaultAgeThreshold
	}

	b := budget{
		timeout:     opts.Timeout,
		nodeTimeout: opts.NodeTimeout,
		concurrency: opts.Concurrency,
		maxRows:     opts.MaxRowsPerNode,
	}
	fillBudget(&b, defaultTimeout, defaultNodeTimeout, defaultConcurrency, defaultMaxRowsPerNode)

	c := collector[mutationObservation]{
		sql: mutationsStuckSQL,
		scan: func(t Target, rows Rows) (mutationObservation, bool, error) {
			var (
				db, table, id, cmd    sql.NullString
				done, age, failReason sql.NullString
			)
			if err := rows.Scan(&db, &table, &id, &cmd, &done, &age, &failReason); err != nil {
				return mutationObservation{}, false, err
			}

			// is_done 与年龄列由服务端 toString 下发，理论上恒为十进制数字；
			// 解析失败按 scan 失败处理，不做静默猜测。
			doneN, err := strconv.ParseUint(done.String, 10, 64)
			if err != nil {
				return mutationObservation{}, false, fmt.Errorf("diagnose: system.mutations is_done 解析失败 (%s.%s): %w", db.String, table.String, err)
			}
			ageN, err := strconv.ParseUint(age.String, 10, 64)
			if err != nil {
				return mutationObservation{}, false, fmt.Errorf("diagnose: system.mutations age_seconds 解析失败 (%s.%s): %w", db.String, table.String, err)
			}

			// 固定查询已在服务端过滤 is_done=1；此处按服务端事实复核，
			// 若仍出现已完成条目则视为查询契约被破坏，不静默跳过。
			if doneN != 0 {
				return mutationObservation{}, false, fmt.Errorf("diagnose: system.mutations 返回了已完成条目 (%s.%s mutation %s)，固定查询契约被破坏", db.String, table.String, id.String)
			}

			// 判定在客户端完成：两条规则按声明顺序生成理由码。
			var reasons []string
			if failReason.String != "" {
				reasons = append(reasons, ReasonMutationFailing)
			}
			if int(ageN) >= opts.AgeThresholdSeconds {
				reasons = append(reasons, ReasonMutationLongRunning)
			}
			if len(reasons) == 0 {
				return mutationObservation{}, false, nil
			}

			return mutationObservation{
				ShardID:          t.ShardID,
				NodeID:           t.NodeID,
				Database:         db.String,
				Table:            table.String,
				MutationID:       id.String,
				Command:          cmd.String,
				AgeSeconds:       int(ageN),
				LatestFailReason: failReason.String,
				Reasons:          reasons,
			}, true, nil
		},
	}

	start := time.Now()
	results, observations, cerr := collect(ctx, targets, b, c)

	matches := reduceMutationObservations(observations)
	exe := summarizeExecution(results)
	outcome := outcomeFor(exe, len(matches) > 0)
	report := MutationsStuckReport{
		CheckID:     mutationsStuckCheckID,
		RuleVersion: mutationsStuckRuleVersion,
		StartedAt:   start,
		FinishedAt:  time.Now(),
		Execution:   exe,
		Outcome:     outcome,
		Targets:     results,
		Matches:     matches,
		NextSteps:   buildMutationFollowUps(exe, results, matches),
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
