package diagnose

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"
)

// defaultDiskFreePercentThreshold 剩余空间占比的判定阈值（百分比）。
// 10% 与常见运维告警水位对齐；阈值不是服务端限制，只表示"该卷容量
// 水位值得人工核查"。
const defaultDiskFreePercentThreshold = 10

// DiskSpaceOptions 控制检查的执行预算与判定阈值。零值字段使用默认值，负值报错。
type DiskSpaceOptions struct {
	// Timeout 是整次检查的总预算，包含调度等待。默认 10s。
	Timeout time.Duration
	// NodeTimeout 是单个节点的查询与读取预算。默认 2s。
	NodeTimeout time.Duration
	// Concurrency 是并发 worker 数上限。默认 4。
	Concurrency int
	// MaxRowsPerNode 是单节点保留的磁盘条目上限；读取到第 N+1 条即停止并标记截断。
	// 默认 1000（磁盘数量通常极少，该上限只作防御）。
	MaxRowsPerNode int
	// FreePercentThreshold 是剩余空间占比的判定阈值（百分比，0-100，
	// 判定条件为 free/total*100 <= 阈值）。默认 10。100 表示"任何未完全
	// 空的磁盘都命中"，只建议用于验证判定链路。
	FreePercentThreshold int
}

// CheckDiskSpaceLow 检查指定节点上剩余空间低于水位的磁盘，返回磁盘水位、
// 各节点证据、采集时间与检查覆盖情况。
//
// 数据源 system.disks，客户端两条判定规则（各条观测附带稳定理由码 ReasonDisk*）：
//   - 剩余占比 free/total*100（向下取整）<= FreePercentThreshold；
//   - keep_free_space > 0 且 free_space < keep_free_space：保留量已被击穿，
//     服务端会拒绝继续写入（这条与占比无关，即使占比尚可也可能命中）。
//
// 结果语义（见 DiskSpaceReport）：
//   - 水位低说明容量值得人工核查，不等于已发生写入失败（保留量击穿除外）；
//   - total_space=0 的磁盘无法计算占比（FreePercent=-1），只参与保留量判定；
//   - no_match 不等于集群健康：只覆盖被检查节点上的本地磁盘视图；
//   - 部分节点失败时保留有效证据并标记不完整，不声称已检查全部磁盘。
//
// 输入不合法时返回包装 ErrInvalidInput 的错误，不执行任何查询，报告为零值。
// 检查失败（所有目标无有效观测）时返回报告（Execution=failed）与
// ErrNoUsableObservation。取消或总预算耗尽时返回报告与对应 context 错误；
// 已完整获得的证据保留在报告中。
func CheckDiskSpaceLow(
	ctx context.Context,
	targets []Target,
	opts DiskSpaceOptions,
) (DiskSpaceReport, error) {
	if err := validateTargets(targets); err != nil {
		return DiskSpaceReport{}, err
	}
	if opts.Timeout < 0 || opts.NodeTimeout < 0 || opts.Concurrency < 0 || opts.MaxRowsPerNode < 0 ||
		opts.FreePercentThreshold < 0 {
		return DiskSpaceReport{}, inputError("预算与阈值不允许为负值")
	}
	if opts.FreePercentThreshold > 100 {
		return DiskSpaceReport{}, inputError("FreePercentThreshold=%d 超出百分比范围（0-100）", opts.FreePercentThreshold)
	}
	if opts.FreePercentThreshold == 0 {
		opts.FreePercentThreshold = defaultDiskFreePercentThreshold
	}

	b := budget{
		timeout:     opts.Timeout,
		nodeTimeout: opts.NodeTimeout,
		concurrency: opts.Concurrency,
		maxRows:     opts.MaxRowsPerNode,
	}
	fillBudget(&b, defaultTimeout, defaultNodeTimeout, defaultConcurrency, defaultMaxRowsPerNode)

	c := collector[diskObservation]{
		sql: diskSpaceSQL,
		scan: func(t Target, rows Rows) (diskObservation, bool, error) {
			var name, path, free, total, keepFree sql.NullString
			if err := rows.Scan(&name, &path, &free, &total, &keepFree); err != nil {
				return diskObservation{}, false, err
			}
			// 容量由服务端 toString 下发，理论上恒为十进制数字；解析失败
			// 按 scan 失败处理，不做静默猜测。这不是调用方输入问题，
			// 不包装 ErrInvalidInput。
			parse := func(field, v string) (uint64, error) {
				n, err := strconv.ParseUint(v, 10, 64)
				if err != nil {
					return 0, fmt.Errorf("system.disks 列 %s 包含非数字值 %q: %w", field, v, err)
				}
				return n, nil
			}
			freeN, err := parse("free_space", free.String)
			if err != nil {
				return diskObservation{}, false, err
			}
			totalN, err := parse("total_space", total.String)
			if err != nil {
				return diskObservation{}, false, err
			}
			keepN, err := parse("keep_free_space", keepFree.String)
			if err != nil {
				return diskObservation{}, false, err
			}

			percent := -1
			if totalN > 0 {
				percent = int(freeN * 100 / totalN)
			}
			var reasons []string
			if percent >= 0 && percent <= opts.FreePercentThreshold {
				reasons = append(reasons, ReasonDiskSpaceLow)
			}
			if keepN > 0 && freeN < keepN {
				reasons = append(reasons, ReasonDiskReserveBreached)
			}
			if len(reasons) == 0 {
				return diskObservation{}, false, nil
			}
			return diskObservation{
				ShardID:       t.ShardID,
				NodeID:        t.NodeID,
				DiskName:      name.String,
				Path:          path.String,
				FreeBytes:     freeN,
				TotalBytes:    totalN,
				KeepFreeBytes: keepN,
				FreePercent:   percent,
				Reasons:       reasons,
			}, true, nil
		},
	}

	start := time.Now()
	results, observations, cerr := collect(ctx, targets, b, c)

	matches := reduceDiskObservations(observations)
	exe := summarizeExecution(results)
	outcome := outcomeFor(exe, len(matches) > 0)
	report := DiskSpaceReport{
		CheckID:     diskSpaceCheckID,
		RuleVersion: diskSpaceRuleVersion,
		StartedAt:   start,
		FinishedAt:  time.Now(),
		Execution:   exe,
		Outcome:     outcome,
		Targets:     results,
		Matches:     matches,
		NextSteps:   buildDiskFollowUps(exe, results, matches),
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
