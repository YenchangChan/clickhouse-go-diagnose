package diagnose

import (
	"context"
	"database/sql"
	"errors"
	"sync"
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
	if len(targets) == 0 {
		return ReplicationQueueReport{}, inputError("targets 为空，无法开展检查")
	}
	seen := make(map[string]struct{}, len(targets))
	for i, t := range targets {
		if t.NodeID == "" {
			return ReplicationQueueReport{}, inputError("targets[%d].NodeID 为空", i)
		}
		if t.ShardID == "" {
			return ReplicationQueueReport{}, inputError("targets[%d](%s).ShardID 为空", i, t.NodeID)
		}
		if t.Querier == nil {
			return ReplicationQueueReport{}, inputError("targets[%d](%s).Querier 为 nil", i, t.NodeID)
		}
		if _, dup := seen[t.NodeID]; dup {
			return ReplicationQueueReport{}, inputError("NodeID %q 重复", t.NodeID)
		}
		seen[t.NodeID] = struct{}{}
	}
	if opts.Timeout < 0 || opts.NodeTimeout < 0 || opts.Concurrency < 0 || opts.MaxRowsPerNode < 0 {
		return ReplicationQueueReport{}, inputError("预算项不允许为负值")
	}
	if opts.Timeout == 0 {
		opts.Timeout = defaultTimeout
	}
	if opts.NodeTimeout == 0 {
		opts.NodeTimeout = defaultNodeTimeout
	}
	if opts.Concurrency == 0 {
		opts.Concurrency = defaultConcurrency
	}
	if opts.MaxRowsPerNode == 0 {
		opts.MaxRowsPerNode = defaultMaxRowsPerNode
	}

	totalCtx, cancelTotal := context.WithTimeout(ctx, opts.Timeout)
	defer cancelTotal()

	start := time.Now()
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
		mu           sync.Mutex
		observations []nodeObservation
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

	workers := opts.Concurrency
	if workers > len(targets) {
		workers = len(targets)
	}
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				res, obs := runTarget(totalCtx, targets[i], opts.NodeTimeout, opts.MaxRowsPerNode)
				results[i] = res
				if len(obs) > 0 {
					mu.Lock()
					observations = append(observations, obs...)
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	finish := time.Now()

	matches := reduceMatches(observations)
	exe := summarizeExecution(results)
	outcome := summarizeOutcome(exe, matches)
	report := ReplicationQueueReport{
		CheckID:     checkID,
		RuleVersion: ruleVersion,
		StartedAt:   start,
		FinishedAt:  finish,
		Execution:   exe,
		Outcome:     outcome,
		Targets:     results,
		Matches:     matches,
		NextSteps:   buildFollowUps(exe, results, matches),
	}

	// 取消或总预算耗尽优先返回 context 错误；报告中的证据仍然成立。
	if cerr := totalCtx.Err(); cerr != nil {
		return report, cerr
	}
	if exe == ExecutionFailed && len(observations) == 0 {
		return report, ErrNoUsableObservation
	}
	return report, nil
}

// runTarget 采集单个目标节点。节点上的有效证据在任何失败路径下都保留；
// 先读到有效行再发生迭代错误的节点不能计入完整成功。
func runTarget(total context.Context, t Target, nodeTimeout time.Duration, maxRows int) (NodeResult, []nodeObservation) {
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

	rows, err := t.Querier.Query(nodeCtx, replicationQueueSQL)
	if err != nil {
		res.ErrCategory = classifyQueryError(nodeCtx, err)
		res.ErrMessage = err.Error()
		res.FinishedAt = time.Now()
		return res, nil
	}

	var (
		obs       []nodeObservation
		read      int
		truncated bool
		failMsg   string
	)

readLoop:
	for rows.Next() {
		// 用 sql.NullString 而不是 *any：clickhouse-go v2 的 ScanRow 不支持
		// *any 目标（真机验证发现），NullString 在 clickhouse-go v2 与
		// database/sql 两条路径上都受支持，且天然处理 NULL。
		var db, table, part, lastExcp sql.NullString
		if err := rows.Scan(&db, &table, &part, &lastExcp); err != nil {
			res.ErrCategory = ErrCatScanFailed
			failMsg = err.Error()
			break readLoop
		}
		read++
		excpS := lastExcp.String
		if !matchesException(excpS) {
			continue
		}
		if len(obs) >= maxRows {
			// 第 N+1 条只用于截断检测，不保留。
			truncated = true
			break readLoop
		}
		obs = append(obs, nodeObservation{
			ShardID:       t.ShardID,
			Database:      db.String,
			Table:         table.String,
			PartName:      part.String,
			NodeID:        t.NodeID,
			LastException: excpS,
		})
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
	res.RowsKept = len(obs)
	res.Truncated = truncated
	res.FinishedAt = time.Now()

	if res.ErrCategory != "" {
		res.ErrMessage = failMsg
		res.Status = NodeFailed
		return res, obs
	}
	res.Status = NodeOK
	return res, obs
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
