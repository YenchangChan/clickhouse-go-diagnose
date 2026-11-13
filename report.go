package diagnose

import (
	"errors"
	"fmt"
	"time"
)

// Execution 表示采集是否完整，与检查结果分离。
type Execution string

const (
	// ExecutionComplete 所有目标成功读取且未截断。
	ExecutionComplete Execution = "complete"
	// ExecutionPartial 部分目标失败、未启动或结果被截断。
	ExecutionPartial Execution = "partial"
	// ExecutionFailed 没有任何目标产出有效观测。
	ExecutionFailed Execution = "failed"
)

// Outcome 只表示该检查的匹配结果，不表示集群整体健康。
type Outcome string

const (
	// OutcomeMatched 发现异常记录；范围是已观测范围，可能不全。
	OutcomeMatched Outcome = "matched"
	// OutcomeNoMatch 已完整覆盖的范围内未见匹配；不等于集群健康。
	OutcomeNoMatch Outcome = "no_match"
	// OutcomeUnknown 覆盖不完整或检查失败，无法做出匹配判断。
	OutcomeUnknown Outcome = "unknown"
)

// NodeStatus 是单个目标节点的采集状态。
type NodeStatus string

const (
	// NodeOK 读取完成且未截断。
	NodeOK NodeStatus = "ok"
	// NodeFailed 查询、扫描、迭代或关闭失败；已读到的有效证据仍保留。
	NodeFailed NodeStatus = "failed"
	// NodeNotStarted 总预算耗尽或取消后未开始。
	NodeNotStarted NodeStatus = "not_started"
)

// 错误类别。权限不足、版本不兼容等细分只有在适配器能可靠分类时才补充；
// 不用不稳定的字符串猜测覆盖通用错误。
const (
	ErrCatQueryFailed     = "query_failed"
	ErrCatScanFailed      = "scan_failed"
	ErrCatIterationFailed = "iteration_failed"
	ErrCatCloseFailed     = "close_failed"
	ErrCatTimeout         = "timeout"
	ErrCatCanceled        = "canceled"
	ErrCatNotStarted      = "not_started"
)

// 后续核查建议的稳定代码。展示文案由消费端决定，机器消费不应解析文案。
const (
	// AdviceVerifyReplicasForPart 逐副本核对 part 是否存在与队列当前状态。
	AdviceVerifyReplicasForPart = "verify_replicas_for_part"
	// AdviceInspectQueueOnSourceNodes 在来源节点核对队列条目时间与重试情况。
	AdviceInspectQueueOnSourceNodes = "inspect_queue_on_source_nodes"
	// AdviceCompleteNodeCoverage 失败或未启动的节点需补查才能得到完整范围。
	AdviceCompleteNodeCoverage = "complete_node_coverage"
	// AdviceRerunWithHigherRowLimit 结果被截断，需提高上限后复跑。
	AdviceRerunWithHigherRowLimit = "rerun_with_higher_row_limit"
	// AdviceInspectMergeThroughput 核对同表是否有进行中的合并与合并积压。
	AdviceInspectMergeThroughput = "inspect_merge_throughput"
	// AdviceReviewInsertBatchSize 核对写入批次大小与 async_insert。
	AdviceReviewInsertBatchSize = "review_insert_batch_size"
	// AdviceReviewPartitionKey 核对分区键粒度是否过细。
	AdviceReviewPartitionKey = "review_partition_key"
	// AdviceVerifyKeeperConnectivity 核对节点与 Keeper 的连通性与会话状态。
	AdviceVerifyKeeperConnectivity = "verify_keeper_connectivity"
	// AdviceLocateInactiveReplica 定位复制组中未激活的副本及其掉线原因。
	AdviceLocateInactiveReplica = "locate_inactive_replica"
	// AdviceInspectReplicaDelay 比较各副本延迟并确认落后是否扩大。
	AdviceInspectReplicaDelay = "inspect_replica_delay"
	// AdviceInvestigateLostParts 核对丢件副本的队列与数据完整性，评估恢复。
	AdviceInvestigateLostParts = "investigate_lost_parts"
)

// 副本状态判定理由的稳定代码。机器消费不应解析展示文案，只匹配这些代码。
const (
	// ReasonReplicaReadonly 副本处于只读状态（is_readonly 非 0）。
	ReasonReplicaReadonly = "replica_readonly"
	// ReasonSessionExpired 副本与 Keeper 的会话已过期（is_session_expired 非 0）。
	ReasonSessionExpired = "session_expired"
	// ReasonActiveReplicaDeficit 复制组存在未激活副本（active < total）。
	ReasonActiveReplicaDeficit = "active_replica_deficit"
	// ReasonReplicaDelayExceeded 副本延迟超过阈值（absolute_delay >= 阈值）。
	ReasonReplicaDelayExceeded = "replica_delay_exceeded"
	// ReasonLostParts 副本记录了永久性丢件（lost_part_count > 0）。
	ReasonLostParts = "lost_parts"
)

var (
	// ErrNoUsableObservation 表示所有目标都失败或没有产出任何可用观测。
	// 报告仍会返回（Execution=failed），消费端必须先处理报告再展示该错误，
	// 不能因错误丢弃报告中的信息。
	ErrNoUsableObservation = errors.New("diagnose: no usable observation from any target")

	// ErrInvalidInput 表示调用方输入不合法。校验失败时不执行任何查询，
	// 返回的报告为零值。
	ErrInvalidInput = errors.New("diagnose: invalid input")
)

// NodeResult 是单个目标节点的采集结果与覆盖信息。
type NodeResult struct {
	NodeID  string
	ShardID string
	Status  NodeStatus

	RowsRead  int  // 本节点实际读取的行数（含未匹配行）
	RowsKept  int  // 匹配且保留的异常行数
	Truncated bool // 达到保留上限后停止读取；本节点清单不完整

	ErrCategory string // Status 为 failed 或 not_started 时非空
	ErrMessage  string // 可能包含服务端异常文本，UI 必须按文本转义展示

	StartedAt  time.Time // 未启动时为零值
	FinishedAt time.Time // 未启动时为零值
}

// QueueObservation 是一条匹配证据：哪个节点的队列记录了该异常。
type QueueObservation struct {
	NodeID        string
	LastException string // 原始异常文本，可能包含敏感信息
}

// QueueMatch 是按 (ShardID, Database, Table, PartName) 归并后的异常对象。
// 同一分片多个副本报告同一 part 时合并展示，但保留每个来源节点；
// 不同分片绝不因同名表或同名 part 合并。
// 匹配条数不是队列任务数，也不代表需要修复的次数。
type QueueMatch struct {
	ShardID      string
	Database     string
	Table        string
	PartName     string
	Observations []QueueObservation
}

// FollowUp 是一条后续核查建议。建议只引导人工核查，不构成修复方案；
// 库不自动执行任何修复操作。
type FollowUp struct {
	Code   string
	Detail string
}

// ReplicationQueueReport 是检查的结构化结果。
//
// 语义约定：
//   - Execution 与 Outcome 分离；partial + matched 表示异常成立但范围可能不全；
//   - 匹配不等于确认数据永久丢失；no_match 不等于集群健康；
//   - 时间戳是各节点各自的采集时刻，不是跨节点原子快照；
//   - 即使顶层返回 error（取消、超时、ErrNoUsableObservation），报告中已保留的
//     有效证据仍然成立，消费端应先处理报告再处理错误。
type ReplicationQueueReport struct {
	CheckID     string
	RuleVersion string

	StartedAt  time.Time
	FinishedAt time.Time

	Execution Execution
	Outcome   Outcome

	Targets   []NodeResult
	Matches   []QueueMatch
	NextSteps []FollowUp
}

// PartsObservation 是一条分区统计证据：哪个节点报告了该分区的 part 数。
type PartsObservation struct {
	NodeID    string
	Parts     int    // 该节点上该分区的活跃 part 数
	DiskBytes string // 磁盘占用，服务端 toString 后的十进制字符串
}

// PartsMatch 是按 (ShardID, Database, Table, Partition) 归并后的分区统计。
// 同一分区多个节点报告时保留每个来源节点；Parts 取各节点中的最大值。
// 超过阈值说明该分区在对应节点上存在写入碎片化风险，不构成修复指令。
type PartsMatch struct {
	ShardID      string
	Database     string
	Table        string
	Partition    string
	Parts        int // 观测中的最大 part 数
	Observations []PartsObservation
}

// TooManyPartsReport 是 TooManyParts 检查的结构化结果。
//
// 语义约定与 ReplicationQueueReport 一致：Execution 与 Outcome 分离；
// 超阈值不等于需要立即修复，no_match 不等于集群健康；时间戳是各节点
// 各自的采集时刻；即使顶层返回 error，报告中已保留的有效证据仍然成立。
type TooManyPartsReport struct {
	CheckID     string
	RuleVersion string

	StartedAt  time.Time
	FinishedAt time.Time

	Execution Execution
	Outcome   Outcome

	Targets   []NodeResult
	Matches   []PartsMatch
	NextSteps []FollowUp
}

// ReplicaObservation 是一条副本状态证据：哪个节点报告了该复制表的
// 副本状态，以及该观测命中的稳定理由码（Reason* 常量）。
type ReplicaObservation struct {
	NodeID string

	IsReadonly       bool
	IsSessionExpired bool
	AbsoluteDelay    int // 秒
	TotalReplicas    int
	ActiveReplicas   int
	LostPartCount    int

	Reasons []string
}

// ReplicaMatch 是按 (ShardID, Database, Table) 归并后的副本异常对象。
// 同一分片的多个副本节点报告同一表时合并展示，但保留每个来源节点——
// 不同副本对同一复制组的观测可能不同，这正是证据。
// 匹配条数不等于故障副本数，也不构成修复指令。
type ReplicaMatch struct {
	ShardID      string
	Database     string
	Table        string
	Observations []ReplicaObservation
}

// ReplicaStatusReport 是 ReplicaStatus 检查的结构化结果。
//
// 语义约定与 TooManyPartsReport 一致：Execution 与 Outcome 分离；
// 匹配不等于集群不可用，no_match 不等于集群健康；时间戳是各节点
// 各自的采集时刻；即使顶层返回 error，报告中已保留的有效证据仍然成立。
type ReplicaStatusReport struct {
	CheckID     string
	RuleVersion string

	StartedAt  time.Time
	FinishedAt time.Time

	Execution Execution
	Outcome   Outcome

	Targets   []NodeResult
	Matches   []ReplicaMatch
	NextSteps []FollowUp
}

func inputError(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrInvalidInput, fmt.Sprintf(format, args...))
}
