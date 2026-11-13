package diagnose

import (
	"context"
	"database/sql"
	"strconv"
	"time"
)

const (
	tooManyPartsCheckID     = "too_many_parts"
	tooManyPartsRuleVersion = "1"
	// defaultPartsThreshold 单分区活跃 part 数的判定阈值。
	// 与既有运维经验对齐（warning 档 1000）；阈值不是服务端硬限制，
	// 只表示"该分区的写入碎片化值得人工核查"。
	defaultPartsThreshold = 1000
)

// TooManyPartsOptions 控制检查的执行预算与判定阈值。零值字段使用默认值，负值报错。
type TooManyPartsOptions struct {
	// Timeout 是整次检查的总预算，包含调度等待。默认 10s。
	Timeout time.Duration
	// NodeTimeout 是单个节点的查询与读取预算。默认 2s。
	NodeTimeout time.Duration
	// Concurrency 是并发 worker 数上限。默认 4。
	Concurrency int
	// MaxRowsPerNode 是单节点保留的分区条目上限；读取到第 N+1 条即停止并标记截断。
	// 默认 1000。
	MaxRowsPerNode int
	// PartsThreshold 是单分区活跃 part 数的判定阈值。默认 1000。
	PartsThreshold int
}

// CheckTooManyParts 检查指定节点上活跃 part 数超过阈值的表分区，
// 返回超阈值分区、各节点证据、采集时间与检查覆盖情况。
//
// 结果语义（见 TooManyPartsReport）：
//   - 超阈值表示写入碎片化值得人工核查，不等于需要立即修复；
//   - 无匹配不等于集群健康；
//   - 部分节点失败时保留有效证据并标记不完整，不声称已检查全部表。
//
// 输入不合法时返回包装 ErrInvalidInput 的错误，不执行任何查询，报告为零值。
// 检查失败（所有目标无有效观测）时返回报告（Execution=failed）与
// ErrNoUsableObservation。取消或总预算耗尽时返回报告与对应 context 错误；
// 已完整获得的证据保留在报告中。
func CheckTooManyParts(
	ctx context.Context,
	targets []Target,
	opts TooManyPartsOptions,
) (TooManyPartsReport, error) {
	if err := validateTargets(targets); err != nil {
		return TooManyPartsReport{}, err
	}
	if opts.Timeout < 0 || opts.NodeTimeout < 0 || opts.Concurrency < 0 || opts.MaxRowsPerNode < 0 || opts.PartsThreshold < 0 {
		return TooManyPartsReport{}, inputError("预算与阈值不允许为负值")
	}
	if opts.PartsThreshold == 0 {
		opts.PartsThreshold = defaultPartsThreshold
	}

	b := budget{
		timeout:     opts.Timeout,
		nodeTimeout: opts.NodeTimeout,
		concurrency: opts.Concurrency,
		maxRows:     opts.MaxRowsPerNode,
	}
	fillBudget(&b, defaultTimeout, defaultNodeTimeout, defaultConcurrency, defaultMaxRowsPerNode)

	c := collector[partsObservation]{
		sql: tooManyPartsSQL,
		scan: func(t Target, rows Rows) (partsObservation, bool, error) {
			var db, table, partition, parts, bytes sql.NullString
			if err := rows.Scan(&db, &table, &partition, &parts, &bytes); err != nil {
				return partsObservation{}, false, err
			}
			// 计数列由服务端 toString 下发，理论上恒为十进制数字；
			// 解析失败按 scan 失败处理，不做静默猜测。
			n, err := strconv.ParseUint(parts.String, 10, 64)
			if err != nil {
				return partsObservation{}, false, err
			}
			if int(n) <= opts.PartsThreshold {
				return partsObservation{}, false, nil
			}
			return partsObservation{
				ShardID:   t.ShardID,
				NodeID:    t.NodeID,
				Database:  db.String,
				Table:     table.String,
				Partition: partition.String,
				Parts:     int(n),
				DiskBytes: bytes.String,
			}, true, nil
		},
	}

	start := time.Now()
	results, observations, cerr := collect(ctx, targets, b, c)

	matches := reducePartsObservations(observations)
	exe := summarizeExecution(results)
	outcome := outcomeFor(exe, len(matches) > 0)
	report := TooManyPartsReport{
		CheckID:     tooManyPartsCheckID,
		RuleVersion: tooManyPartsRuleVersion,
		StartedAt:   start,
		FinishedAt:  time.Now(),
		Execution:   exe,
		Outcome:     outcome,
		Targets:     results,
		Matches:     matches,
		NextSteps:   buildPartsFollowUps(exe, results, matches),
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
