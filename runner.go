package diagnose

import (
	"context"
	"errors"
	"sync"
	"time"
)

const (
	defaultTimeout        = 10 * time.Second
	defaultNodeTimeout    = 2 * time.Second
	defaultConcurrency    = 4
	defaultMaxRowsPerNode = 1000
)

// Target 是一个被检查的目标节点。NodeID 在一次调用内必须全局唯一
// （同一主机上的多个实例须使用不同 NodeID）；ShardID 在本次调用中稳定，
// 不同分片绝不因同名表或 part 合并。Querier 由调用方构造，库不管理连接。
type Target struct {
	NodeID  string
	ShardID string
	Querier Querier
}

// budget 是检查共享的执行预算。由各检查的 Options 映射而来，
// 零值字段取默认值，负值由检查入口自行校验。
type budget struct {
	timeout     time.Duration
	nodeTimeout time.Duration
	concurrency int
	maxRows     int
}

// fillBudget 用指定默认值填充零值字段。
func fillBudget(b *budget, defTimeout, defNodeTimeout time.Duration, defConcurrency, defMaxRows int) {
	if b.timeout == 0 {
		b.timeout = defTimeout
	}
	if b.nodeTimeout == 0 {
		b.nodeTimeout = defNodeTimeout
	}
	if b.concurrency == 0 {
		b.concurrency = defConcurrency
	}
	if b.maxRows == 0 {
		b.maxRows = defMaxRows
	}
}

// collector 是检查对共享调度暴露的采集逻辑：每个节点执行同一条固定 SQL，
// 并用 scan 把结果集的每一行转成一个候选观测。保留的观测进入归并。
type collector[T any] struct {
	// sql 是在每个目标节点上执行的固定查询。
	sql string
	// scan 处理当前行：返回 (观测, 是否保留)。返回错误表示 Scan 或
	// 解析失败，采集即终止并分类为 scan_failed，已保留的证据仍然有效。
	// 实现应在观测中标注 t.NodeID / t.ShardID；不得在内部读取 rows 的
	// 其他方法或缓存 rows 供之后使用。
	scan func(t Target, rows Rows) (obs T, keep bool, err error)
}

// validateTargets 校验检查输入的目标列表。
func validateTargets(targets []Target) error {
	if len(targets) == 0 {
		return inputError("targets 为空，无法开展检查")
	}
	seen := make(map[string]struct{}, len(targets))
	for i, t := range targets {
		if t.NodeID == "" {
			return inputError("targets[%d].NodeID 为空", i)
		}
		if t.ShardID == "" {
			return inputError("targets[%d](%s).ShardID 为空", i, t.NodeID)
		}
		if t.Querier == nil {
			return inputError("targets[%d](%s).Querier 为 nil", i, t.NodeID)
		}
		if _, dup := seen[t.NodeID]; dup {
			return inputError("NodeID %q 重复", t.NodeID)
		}
		seen[t.NodeID] = struct{}{}
	}
	return nil
}

// collect 对全部目标并发采集，返回各节点结果、全部保留观测，以及总预算
// 状态：非 nil 表示取消或总预算耗尽（context.DeadlineExceeded / Canceled）。
//
// 调度语义（两个检查共用）：
//   - 全部目标先记为 not_started，被调度并产出结果的槽位被覆盖；
//   - 固定 worker 数，按 targets 顺序派发，总预算耗尽后停止派发；
//   - 单节点读取到第 maxRows+1 条保留观测即停止并标记截断（N+1 检测）；
//   - 节点上的有效证据在任何失败路径下都保留；先读到有效行再发生
//     迭代错误的节点不能计入完整成功。
func collect[T any](ctx context.Context, targets []Target, b budget, c collector[T]) ([]NodeResult, []T, error) {
	totalCtx, cancelTotal := context.WithTimeout(ctx, b.timeout)
	defer cancelTotal()

	// 全部目标先记为 not_started；被调度并产出结果的槽位被覆盖。
	results := make([]NodeResult, len(targets))
	for i, t := range targets {
		results[i] = NodeResult{
			NodeID:      t.NodeID,
			ShardID:     t.ShardID,
			Status:      NodeNotStarted,
			ErrCategory: ErrCatNotStarted,
		}
	}

	var (
		mu  sync.Mutex
		obs []T
	)

	jobs := make(chan int)
	go func() {
		defer close(jobs)
		for i := range targets {
			select {
			case jobs <- i:
			case <-totalCtx.Done():
				return
			}
		}
	}()

	workers := b.concurrency
	if workers > len(targets) {
		workers = len(targets)
	}
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				res, kept := runNode(totalCtx, targets[i], b.nodeTimeout, b.maxRows, c)
				results[i] = res
				if len(kept) > 0 {
					mu.Lock()
					obs = append(obs, kept...)
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()

	// 在 defer cancelTotal 执行前求值：cancel 后 Err 恒为 Canceled，
	// 会把"预算耗尽/取消"与"正常完成"混为一谈。
	return results, obs, totalCtx.Err()
}

// runNode 采集单个目标节点。节点上的有效证据在任何失败路径下都保留；
// 先读到有效行再发生迭代错误的节点不能计入完整成功。
func runNode[T any](total context.Context, t Target, nodeTimeout time.Duration, maxRows int, c collector[T]) (NodeResult, []T) {
	res := NodeResult{
		NodeID:      t.NodeID,
		ShardID:     t.ShardID,
		Status:      NodeNotStarted,
		ErrCategory: ErrCatNotStarted,
	}
	if err := total.Err(); err != nil {
		res.ErrCategory = classifyContextError(err)
		res.ErrMessage = err.Error()
		return res, nil
	}

	nodeCtx, cancelNode := context.WithTimeout(total, nodeTimeout)
	defer cancelNode()

	// 节点已实际开始采集：清除 not_started 的初始分类，改由各失败路径填写。
	res.ErrCategory = ""
	res.ErrMessage = ""
	res.Status = NodeFailed
	res.StartedAt = time.Now()

	rows, err := t.Querier.Query(nodeCtx, c.sql)
	if err != nil {
		res.ErrCategory = classifyQueryError(nodeCtx, err)
		res.ErrMessage = err.Error()
		res.FinishedAt = time.Now()
		return res, nil
	}

	var (
		kept      []T
		read      int
		truncated bool
		failMsg   string
	)

readLoop:
	for rows.Next() {
		// scan 内部使用 sql.NullString 而不是 *any：clickhouse-go v2 的
		// ScanRow 不支持 *any 目标（真机验证发现），NullString 在
		// clickhouse-go v2 与 database/sql 两条路径上都受支持，且天然处理 NULL。
		o, keep, serr := c.scan(t, rows)
		if serr != nil {
			res.ErrCategory = ErrCatScanFailed
			failMsg = serr.Error()
			break readLoop
		}
		read++
		if !keep {
			continue
		}
		if len(kept) >= maxRows {
			// 第 N+1 条只用于截断检测，不保留。
			truncated = true
			break readLoop
		}
		kept = append(kept, o)
	}

	// 退出循环后按优先级分类失败原因；context 取消优先，避免被适配器的
	// 迭代错误文本掩盖真实原因。
	if res.ErrCategory == "" {
		if nerr := nodeCtx.Err(); nerr != nil {
			res.ErrCategory = classifyContextError(nerr)
			failMsg = nerr.Error()
		} else if rerr := rows.Err(); rerr != nil {
			res.ErrCategory = ErrCatIterationFailed
			failMsg = rerr.Error()
		}
	}
	if cerr := rows.Close(); cerr != nil && res.ErrCategory == "" {
		res.ErrCategory = ErrCatCloseFailed
		failMsg = cerr.Error()
	}

	res.RowsRead = read
	res.RowsKept = len(kept)
	res.Truncated = truncated
	res.FinishedAt = time.Now()

	if res.ErrCategory != "" {
		res.ErrMessage = failMsg
		res.Status = NodeFailed
		return res, kept
	}
	res.Status = NodeOK
	return res, kept
}

// classifyContextError 区分超时与取消。
func classifyContextError(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrCatTimeout
	}
	return ErrCatCanceled
}

// classifyQueryError 对 Query 阶段的错误分类：context 类错误优先于通用查询失败。
func classifyQueryError(nodeCtx context.Context, err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return ErrCatTimeout
	}
	if errors.Is(err, context.Canceled) {
		return ErrCatCanceled
	}
	if nerr := nodeCtx.Err(); nerr != nil {
		return classifyContextError(nerr)
	}
	return ErrCatQueryFailed
}

// coverageFollowUps 生成与检查无关的覆盖类建议：覆盖不完整、结果截断。
func coverageFollowUps(exe Execution, results []NodeResult) []FollowUp {
	var steps []FollowUp
	if exe != ExecutionComplete {
		steps = append(steps, FollowUp{
			Code:   AdviceCompleteNodeCoverage,
			Detail: "本次检查未完整覆盖全部目标节点；未覆盖范围不影响已有证据，但异常清单可能不完整",
		})
	}
	for _, r := range results {
		if r.Truncated {
			steps = append(steps, FollowUp{
				Code:   AdviceRerunWithHigherRowLimit,
				Detail: "部分节点结果达到保留上限被截断，复跑并提高行数上限才能保证清单完整",
			})
			break
		}
	}
	return steps
}
