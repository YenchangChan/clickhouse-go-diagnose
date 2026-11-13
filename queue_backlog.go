package diagnose

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"
)

const (
	// defaultDepthThreshold 在队条目数的积压判定阈值。健康复制表的队列
	// 一般只有个位数在途条目；持续上百说明消化速率跟不上产生速率。
	defaultDepthThreshold = 100
	// defaultAgeThresholdSeconds 最老条目年龄的判定阈值（秒）。在途条目
	// 存在超过一小时，通常意味着队列消费受阻而非瞬时写入。
	defaultAgeThresholdSeconds = 3600
	// defaultRetriesThreshold 最大重试次数的判定阈值。num_tries 持续增长
	// 说明条目反复失败；10 次以上不太可能是瞬时抖动。
	defaultRetriesThreshold = 10
)

// QueueBacklogOptions 控制检查的执行预算与判定阈值。零值字段使用默认值，负值报错。
type QueueBacklogOptions struct {
	// Timeout 是整次检查的总预算，包含调度等待。默认 10s。
	Timeout time.Duration
	// NodeTimeout 是单个节点的查询与读取预算。默认 2s。
	NodeTimeout time.Duration
	// Concurrency 是并发 worker 数上限。默认 4。
	Concurrency int
	// MaxRowsPerNode 是单节点保留的聚合条目上限；读取到第 N+1 条即停止并标记截断。
	// 默认 1000。
	MaxRowsPerNode int
	// DepthThreshold 是单表在队条目数的判定阈值（该节点队列内）。
	// 判定条件为 >=。默认 100。
	DepthThreshold int
	// AgeThresholdSeconds 是队列最老条目年龄的判定阈值（秒），判定条件
	// 为 >=。默认 3600（一小时）。
	AgeThresholdSeconds int
	// RetriesThreshold 是队列条目最大重试次数的判定阈值，判定条件为 >=。
	// 默认 10。
	RetriesThreshold int
}

// CheckReplicationQueueBacklog 检查指定节点复制队列的积压特征（在队条目
// 数、最老条目年龄、重试次数），不限于单一异常类型，返回积压表、各节点
// 证据、采集时间与检查覆盖情况。
//
// 数据源是各节点 system.replication_queue 按表的服务端聚合，客户端做三条
// 阈值判定（各条观测附带稳定理由码 ReasonQueue*）：
//   - 在队条目数 >= DepthThreshold（队列消化速率跟不上）；
//   - 最老条目年龄 >= AgeThresholdSeconds（消费受阻而非瞬时写入）；
//   - 最大重试次数 >= RetriesThreshold（条目反复失败）。
//
// 结果语义（见 QueueBacklogReport）：
//   - 队列覆盖全部条目类型，健康的瞬时在途条目也会计入深度，阈值存在
//     的意义就是消化这类噪音；积压不等于数据不可用，需人工确认；
//   - no_match 不等于集群健康；
//   - 部分节点失败时保留有效证据并标记不完整，不声称已检查全部表。
//
// 输入不合法时返回包装 ErrInvalidInput 的错误，不执行任何查询，报告为零值。
// 检查失败（所有目标无有效观测）时返回报告（Execution=failed）与
// ErrNoUsableObservation。取消或总预算耗尽时返回报告与对应 context 错误；
// 已完整获得的证据保留在报告中。
func CheckReplicationQueueBacklog(
	ctx context.Context,
	targets []Target,
	opts QueueBacklogOptions,
) (QueueBacklogReport, error) {
	if err := validateTargets(targets); err != nil {
		return QueueBacklogReport{}, err
	}
	if opts.Timeout < 0 || opts.NodeTimeout < 0 || opts.Concurrency < 0 || opts.MaxRowsPerNode < 0 ||
		opts.DepthThreshold < 0 || opts.AgeThresholdSeconds < 0 || opts.RetriesThreshold < 0 {
		return QueueBacklogReport{}, inputError("预算与阈值不允许为负值")
	}
	if opts.DepthThreshold == 0 {
		opts.DepthThreshold = defaultDepthThreshold
	}
	if opts.AgeThresholdSeconds == 0 {
		opts.AgeThresholdSeconds = defaultAgeThresholdSeconds
	}
	if opts.RetriesThreshold == 0 {
		opts.RetriesThreshold = defaultRetriesThreshold
	}

	b := budget{
		timeout:     opts.Timeout,
		nodeTimeout: opts.NodeTimeout,
		concurrency: opts.Concurrency,
		maxRows:     opts.MaxRowsPerNode,
	}
	fillBudget(&b, defaultTimeout, defaultNodeTimeout, defaultConcurrency, defaultMaxRowsPerNode)

	c := collector[backlogObservation]{
		sql: queueBacklogSQL,
		scan: func(t Target, rows Rows) (backlogObservation, bool, error) {
			var db, table, depth, oldestAge, maxTries, failing sql.NullString
			if err := rows.Scan(&db, &table, &depth, &oldestAge, &maxTries, &failing); err != nil {
				return backlogObservation{}, false, err
			}
			// 计数列由服务端 toString 下发，理论上恒为十进制数字；
			// 解析失败按 scan 失败处理，不做静默猜测。这不是调用方
			// 输入问题，不包装 ErrInvalidInput。
			parse := func(field, v string) (int, error) {
				n, err := strconv.ParseUint(v, 10, 64)
				if err != nil {
					return 0, fmt.Errorf("system.replication_queue 聚合列 %s 包含非数字值 %q: %w", field, v, err)
				}
				return int(n), nil
			}
			d, err := parse("queue_depth", depth.String)
			if err != nil {
				return backlogObservation{}, false, err
			}
			age, err := parse("oldest_age", oldestAge.String)
			if err != nil {
				return backlogObservation{}, false, err
			}
			tries, err := parse("max_tries", maxTries.String)
			if err != nil {
				return backlogObservation{}, false, err
			}
			fe, err := parse("failing_entries", failing.String)
			if err != nil {
				return backlogObservation{}, false, err
			}

			var reasons []string
			if d >= opts.DepthThreshold {
				reasons = append(reasons, ReasonQueueDepth)
			}
			if age >= opts.AgeThresholdSeconds {
				reasons = append(reasons, ReasonQueueOldestEntry)
			}
			if tries >= opts.RetriesThreshold {
				reasons = append(reasons, ReasonQueueMaxRetries)
			}
			if len(reasons) == 0 {
				return backlogObservation{}, false, nil
			}
			return backlogObservation{
				ShardID:        t.ShardID,
				NodeID:         t.NodeID,
				Database:       db.String,
				Table:          table.String,
				Depth:          d,
				OldestAge:      age,
				MaxRetries:     tries,
				FailingEntries: fe,
				Reasons:        reasons,
			}, true, nil
		},
	}

	start := time.Now()
	results, observations, cerr := collect(ctx, targets, b, c)

	matches := reduceBacklogObservations(observations)
	exe := summarizeExecution(results)
	outcome := outcomeFor(exe, len(matches) > 0)
	report := QueueBacklogReport{
		CheckID:     queueBacklogCheckID,
		RuleVersion: queueBacklogRuleVersion,
		StartedAt:   start,
		FinishedAt:  time.Now(),
		Execution:   exe,
		Outcome:     outcome,
		Targets:     results,
		Matches:     matches,
		NextSteps:   buildBacklogFollowUps(exe, results, matches),
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
