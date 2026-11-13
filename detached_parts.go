package diagnose

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// DetachedPartsOptions 控制检查的执行预算与判定阈值。零值字段使用默认值，负值报错。
type DetachedPartsOptions struct {
	// Timeout 是整次检查的总预算，包含调度等待。默认 10s。
	Timeout time.Duration
	// NodeTimeout 是单个节点的查询与读取预算。默认 2s。
	NodeTimeout time.Duration
	// Concurrency 是并发 worker 数上限。默认 4。
	Concurrency int
	// MaxRowsPerNode 是单节点保留的 detached part 条目上限；读取到第 N+1 条
	// 即停止并标记截断。默认 1000。
	MaxRowsPerNode int
	// CountThreshold 是单表 detached part 总数的判定阈值（>= 判定）。
	// 默认 10；1 表示"存在任何 detached part 都命中"，只建议用于验证判定链路。
	CountThreshold int
}

// CheckDetachedParts 检查指定节点上用户库表的 detached part，
// 返回涉及表、各 part 证据、采集时间与检查覆盖情况。
//
// 数据源 system.detached_parts（本地视图，不含 system/information_schema）。
// 客户端判定规则（稳定理由码见 ReasonDetached* 常量）：
//   - reason 前缀 "broken"（如 broken、broken-on-start）：服务端判定该
//     part 损坏（ReasonDetachedBroken）；
//   - reason 非空且非 broken 前缀：服务端自行下线该 part（merge 失败
//     残留、clone 等，ReasonDetachedServerInitiated）——用户通过 ALTER
//     TABLE ... DETACH 手工 detach 的 part reason 为空，不触发此规则；
//   - 单节点上同一张表的 detached part 总数 >= CountThreshold
//     （ReasonDetachedCountExceeded，附加在匹配级 Reasons 上）。
//
// 结果语义（见 DetachedPartsReport）：
//   - 所有 detached part 行都作为证据保留（含空 reason 的用户 detach），
//     是否构成匹配由上述规则决定；
//   - detached 目录有内容说明该表状态值得人工核查，不等于数据丢失；
//   - no_match 不等于集群健康：只覆盖被检查节点的本地视图；
//   - 部分节点失败时保留有效证据并标记不完整，不声称已检查全部表。
//
// 输入不合法时返回包装 ErrInvalidInput 的错误，不执行任何查询，报告为零值。
// 检查失败（所有目标无有效观测）时返回报告（Execution=failed）与
// ErrNoUsableObservation。取消或总预算耗尽时返回报告与对应 context 错误；
// 已完整获得的证据保留在报告中。
func CheckDetachedParts(
	ctx context.Context,
	targets []Target,
	opts DetachedPartsOptions,
) (DetachedPartsReport, error) {
	if err := validateTargets(targets); err != nil {
		return DetachedPartsReport{}, err
	}
	if opts.Timeout < 0 || opts.NodeTimeout < 0 || opts.Concurrency < 0 || opts.MaxRowsPerNode < 0 ||
		opts.CountThreshold < 0 {
		return DetachedPartsReport{}, inputError("预算与阈值不允许为负值")
	}
	if opts.CountThreshold == 0 {
		opts.CountThreshold = defaultDetachedCountThreshold
	}

	b := budget{
		timeout:     opts.Timeout,
		nodeTimeout: opts.NodeTimeout,
		concurrency: opts.Concurrency,
		maxRows:     opts.MaxRowsPerNode,
	}
	fillBudget(&b, defaultTimeout, defaultNodeTimeout, defaultConcurrency, defaultMaxRowsPerNode)

	c := collector[detachedObservation]{
		sql: detachedPartsSQL,
		scan: func(t Target, rows Rows) (detachedObservation, bool, error) {
			var db, table, name, reason, disk, bytes sql.NullString
			if err := rows.Scan(&db, &table, &name, &reason, &disk, &bytes); err != nil {
				return detachedObservation{}, false, err
			}
			// 容量由服务端 toString 下发，理论上恒为十进制数字；解析失败
			// 按 scan 失败处理，不做静默猜测。这不是调用方输入问题，
			// 不包装 ErrInvalidInput。
			byteN, err := strconv.ParseUint(bytes.String, 10, 64)
			if err != nil {
				return detachedObservation{}, false, fmt.Errorf(
					"system.detached_parts 列 bytes_on_disk 包含非数字值 %q: %w", bytes.String, err)
			}

			var reasons []string
			switch {
			case strings.HasPrefix(reason.String, "broken"):
				reasons = append(reasons, ReasonDetachedBroken)
			case reason.String != "":
				reasons = append(reasons, ReasonDetachedServerInitiated)
			}

			// 所有行都保留：空 reason 的用户 detach 是数量阈值规则的证据，
			// 是否构成匹配由归并层按规则判定。
			return detachedObservation{
				ShardID:     t.ShardID,
				NodeID:      t.NodeID,
				Database:    db.String,
				Table:       table.String,
				PartName:    name.String,
				Reason:      reason.String,
				Disk:        disk.String,
				BytesOnDisk: byteN,
				Reasons:     reasons,
			}, true, nil
		},
	}

	start := time.Now()
	results, observations, cerr := collect(ctx, targets, b, c)

	matches := reduceDetachedObservations(observations, opts.CountThreshold)
	exe := summarizeExecution(results)
	outcome := outcomeFor(exe, len(matches) > 0)
	report := DetachedPartsReport{
		CheckID:     detachedPartsCheckID,
		RuleVersion: detachedPartsRuleVersion,
		StartedAt:   start,
		FinishedAt:  time.Now(),
		Execution:   exe,
		Outcome:     outcome,
		Targets:     results,
		Matches:     matches,
		NextSteps:   buildDetachedFollowUps(exe, results, matches),
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
