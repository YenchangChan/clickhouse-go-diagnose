package diagnose

import (
	"context"
	"errors"
	"time"
)

// ThresholdSpec 声明检查的一个判定阈值槽位。Key 是稳定名，供消费端
// （CLI flag、配置、Agent 工具参数）按名映射；Default 是库默认值。
type ThresholdSpec struct {
	Key         string `json:"key"`
	Default     int    `json:"default"`
	Description string `json:"description"`
}

// CheckMeta 描述一个可枚举的检查：稳定 ID、人类可读描述、当前规则版本
// 与阈值槽位清单。ID 与 RuleVersion 是机器消费的稳定契约。
type CheckMeta struct {
	ID          string          `json:"id"`
	Description string          `json:"description"`
	RuleVersion string          `json:"rule_version"`
	Thresholds  []ThresholdSpec `json:"thresholds"`
}

// CheckOptions 是 RunCheck 的执行预算与判定阈值。预算字段零值语义与
// 各 CheckXxx Options 一致（0 取默认，负值报错）；Thresholds 按
// CheckMeta.Thresholds 的槽位顺序提供，缺省槽位或槽位值为 0 时用库默认值。
type CheckOptions struct {
	Timeout        time.Duration `json:"timeout"`
	NodeTimeout    time.Duration `json:"node_timeout"`
	Concurrency    int           `json:"concurrency"`
	MaxRowsPerNode int           `json:"max_rows_per_node"`

	Thresholds []int `json:"thresholds"`
}

// ReportHeader 是各检查报告的公共视图：执行完整性、匹配结果、逐节点
// 采集状态与后续建议。类型各异的 Matches 证据仍在具体报告类型上，
// 类型断言回具体类型取用。
type ReportHeader struct {
	CheckMeta

	StartedAt  time.Time `json:"started_at"`
	FinishedAt time.Time `json:"finished_at"`
	Execution  Execution `json:"execution"`
	Outcome    Outcome   `json:"outcome"`

	Targets   []NodeResult `json:"targets"`
	NextSteps []FollowUp   `json:"next_steps"`
}

// Report 是各检查结构化报告的公共接口。库只产出证据与稳定理由码，
// 不判定严重级别；档位/告警分级由消费端基于 Header 与 Matches 自行决定。
type Report interface {
	Header() ReportHeader
}

// checkEntry 是检查注册表条目：元数据 + 统一执行的适配闭包。
// 适配闭包把 CheckOptions 翻译成该检查自己的 Options 并调用 CheckXxx。
type checkEntry struct {
	meta CheckMeta
	run  func(ctx context.Context, targets []Target, opts CheckOptions) (Report, error)
}

// checkRegistry 保持声明顺序；新增检查时在此追加，并同步
// check_test.go 的 wantCheckIDs。
var checkRegistry = []checkEntry{
	{
		meta: CheckMeta{
			ID:          replicationQueueCheckID,
			RuleVersion: replicationQueueRuleVersion,
			Description: "复制队列（system.replication_queue）中的 NO_REPLICA_HAS_PART 异常",
		},
		run: runReplicationQueueFailures,
	},
	{
		meta: CheckMeta{
			ID:          tooManyPartsCheckID,
			RuleVersion: tooManyPartsRuleVersion,
			Description: "单分区活跃 part 数超过阈值（写入碎片化风险）",
			Thresholds: []ThresholdSpec{{
				Key:         "parts_count",
				Default:     defaultPartsThreshold,
				Description: "单分区活跃 part 数",
			}},
		},
		run: runTooManyParts,
	},
	{
		meta: CheckMeta{
			ID:          replicaStatusCheckID,
			RuleVersion: replicaStatusRuleVersion,
			Description: "副本状态：Keeper 不可达、只读、会话过期、active 缺口、延迟超阈值、丢件",
			Thresholds: []ThresholdSpec{{
				Key:         "delay_seconds",
				Default:     defaultDelayThreshold,
				Description: "副本 absolute_delay 判定阈值（秒）",
			}},
		},
		run: runReplicaStatus,
	},
	{
		meta: CheckMeta{
			ID:          mutationsStuckCheckID,
			RuleVersion: mutationsStuckRuleVersion,
			Description: "卡住的 mutation：失败重试、未完成超龄",
			Thresholds: []ThresholdSpec{{
				Key:         "age_seconds",
				Default:     defaultAgeThreshold,
				Description: "未完成 mutation 的年龄阈值（秒）",
			}},
		},
		run: runMutationsStuck,
	},
	{
		meta: CheckMeta{
			ID:          queueBacklogCheckID,
			RuleVersion: queueBacklogRuleVersion,
			Description: "复制队列积压：在队条目数、最老条目年龄、重试次数",
			Thresholds: []ThresholdSpec{
				{Key: "depth", Default: defaultDepthThreshold, Description: "单表在队条目数"},
				{Key: "age_seconds", Default: defaultAgeThresholdSeconds, Description: "最老条目年龄（秒）"},
				{Key: "retries", Default: defaultRetriesThreshold, Description: "最大重试次数"},
			},
		},
		run: runQueueBacklog,
	},
	{
		meta: CheckMeta{
			ID:          diskSpaceCheckID,
			RuleVersion: diskSpaceRuleVersion,
			Description: "磁盘水位：剩余占比低于阈值、keep_free_space 保留量击穿",
			Thresholds: []ThresholdSpec{{
				Key:         "free_percent",
				Default:     defaultDiskFreePercentThreshold,
				Description: "剩余空间占比阈值（%）",
			}},
		},
		run: runDiskSpaceLow,
	},
	{
		meta: CheckMeta{
			ID:          detachedPartsCheckID,
			RuleVersion: detachedPartsRuleVersion,
			Description: "detached part：服务端判定损坏、自行下线、单表数量超阈值",
			Thresholds: []ThresholdSpec{{
				Key:         "count",
				Default:     defaultDetachedCountThreshold,
				Description: "单表 detached part 总数阈值",
			}},
		},
		run: runDetachedParts,
	},
}

// ListChecks 返回全部检查的元数据，顺序稳定。
func ListChecks() []CheckMeta {
	metas := make([]CheckMeta, 0, len(checkRegistry))
	for _, e := range checkRegistry {
		metas = append(metas, e.meta)
	}
	return metas
}

// RunCheck 按检查 ID 统一执行。输入不合法（未知 ID、目标校验失败、
// 预算或阈值槽位非法）时返回包装 ErrInvalidInput 的错误与 nil 报告，
// 不执行任何查询；预算负值与目标校验由各 CheckXxx 复用原有契约。
//
// 顶层错误与诊断发现的分离语义与各 CheckXxx 一致：报告先于错误，
// 取消/预算耗尽时返回报告与 context 错误。
func RunCheck(ctx context.Context, id string, targets []Target, opts CheckOptions) (Report, error) {
	var entry *checkEntry
	for i := range checkRegistry {
		if checkRegistry[i].meta.ID == id {
			entry = &checkRegistry[i]
			break
		}
	}
	if entry == nil {
		return nil, inputError("未知检查 ID %q（ListChecks 查看全部）", id)
	}
	if len(opts.Thresholds) > len(entry.meta.Thresholds) {
		return nil, inputError("检查 %q 只有 %d 个阈值槽位，收到 %d 个", id, len(entry.meta.Thresholds), len(opts.Thresholds))
	}
	for i, v := range opts.Thresholds {
		if v < 0 {
			return nil, inputError("检查 %q 阈值槽位 %d 不允许为负值", id, i)
		}
	}
	if err := validateTargets(targets); err != nil {
		return nil, err
	}
	rep, err := entry.run(ctx, targets, opts)
	// 校验失败路径（ErrInvalidInput）的报告是零值、无信息量，统一收口为
	// nil；其余错误（取消、预算耗尽、ErrNoUsableObservation）保留报告，
	// 遵循"先报告后错误"契约。
	if err != nil && IsInvalidInput(err) {
		return nil, err
	}
	return rep, err
}

// IsInvalidInput 判断 err 是否为输入校验类错误。
func IsInvalidInput(err error) bool {
	return errors.Is(err, ErrInvalidInput)
}

// headerFor 组装公共视图。ID/RuleVersion 以报告自身携带的值为准
// （真值源），Description/Thresholds 从注册表补全。
func headerFor(checkID, ruleVersion string, started, finished time.Time,
	exe Execution, outcome Outcome, targets []NodeResult, next []FollowUp) ReportHeader {

	h := ReportHeader{
		CheckMeta: CheckMeta{ID: checkID, RuleVersion: ruleVersion},
		StartedAt: started, FinishedAt: finished,
		Execution: exe, Outcome: outcome,
		Targets: targets, NextSteps: next,
	}
	for i := range checkRegistry {
		if checkRegistry[i].meta.ID == checkID {
			h.Description = checkRegistry[i].meta.Description
			h.Thresholds = checkRegistry[i].meta.Thresholds
			break
		}
	}
	return h
}

// ---- 各检查的统一执行适配 ----

func runReplicationQueueFailures(ctx context.Context, targets []Target, opts CheckOptions) (Report, error) {
	rep, err := CheckReplicationQueueFailures(ctx, targets, ReplicationQueueOptions{
		Timeout:        opts.Timeout,
		NodeTimeout:    opts.NodeTimeout,
		Concurrency:    opts.Concurrency,
		MaxRowsPerNode: opts.MaxRowsPerNode,
	})
	return rep, err
}

func runTooManyParts(ctx context.Context, targets []Target, opts CheckOptions) (Report, error) {
	o := TooManyPartsOptions{
		Timeout:        opts.Timeout,
		NodeTimeout:    opts.NodeTimeout,
		Concurrency:    opts.Concurrency,
		MaxRowsPerNode: opts.MaxRowsPerNode,
	}
	if len(opts.Thresholds) > 0 {
		o.PartsThreshold = opts.Thresholds[0]
	}
	rep, err := CheckTooManyParts(ctx, targets, o)
	return rep, err
}

func runReplicaStatus(ctx context.Context, targets []Target, opts CheckOptions) (Report, error) {
	o := ReplicaStatusOptions{
		Timeout:        opts.Timeout,
		NodeTimeout:    opts.NodeTimeout,
		Concurrency:    opts.Concurrency,
		MaxRowsPerNode: opts.MaxRowsPerNode,
	}
	if len(opts.Thresholds) > 0 {
		o.DelayThresholdSeconds = opts.Thresholds[0]
	}
	rep, err := CheckReplicaStatus(ctx, targets, o)
	return rep, err
}

func runMutationsStuck(ctx context.Context, targets []Target, opts CheckOptions) (Report, error) {
	o := MutationsStuckOptions{
		Timeout:        opts.Timeout,
		NodeTimeout:    opts.NodeTimeout,
		Concurrency:    opts.Concurrency,
		MaxRowsPerNode: opts.MaxRowsPerNode,
	}
	if len(opts.Thresholds) > 0 {
		o.AgeThresholdSeconds = opts.Thresholds[0]
	}
	rep, err := CheckMutationsStuck(ctx, targets, o)
	return rep, err
}

func runQueueBacklog(ctx context.Context, targets []Target, opts CheckOptions) (Report, error) {
	o := QueueBacklogOptions{
		Timeout:        opts.Timeout,
		NodeTimeout:    opts.NodeTimeout,
		Concurrency:    opts.Concurrency,
		MaxRowsPerNode: opts.MaxRowsPerNode,
	}
	if len(opts.Thresholds) > 0 {
		o.DepthThreshold = opts.Thresholds[0]
	}
	if len(opts.Thresholds) > 1 {
		o.AgeThresholdSeconds = opts.Thresholds[1]
	}
	if len(opts.Thresholds) > 2 {
		o.RetriesThreshold = opts.Thresholds[2]
	}
	rep, err := CheckReplicationQueueBacklog(ctx, targets, o)
	return rep, err
}

func runDiskSpaceLow(ctx context.Context, targets []Target, opts CheckOptions) (Report, error) {
	o := DiskSpaceOptions{
		Timeout:        opts.Timeout,
		NodeTimeout:    opts.NodeTimeout,
		Concurrency:    opts.Concurrency,
		MaxRowsPerNode: opts.MaxRowsPerNode,
	}
	if len(opts.Thresholds) > 0 {
		o.FreePercentThreshold = opts.Thresholds[0]
	}
	rep, err := CheckDiskSpaceLow(ctx, targets, o)
	return rep, err
}

func runDetachedParts(ctx context.Context, targets []Target, opts CheckOptions) (Report, error) {
	o := DetachedPartsOptions{
		Timeout:        opts.Timeout,
		NodeTimeout:    opts.NodeTimeout,
		Concurrency:    opts.Concurrency,
		MaxRowsPerNode: opts.MaxRowsPerNode,
	}
	if len(opts.Thresholds) > 0 {
		o.CountThreshold = opts.Thresholds[0]
	}
	rep, err := CheckDetachedParts(ctx, targets, o)
	return rep, err
}

// ---- 各报告的公共视图实现 ----

func (r ReplicationQueueReport) Header() ReportHeader {
	return headerFor(r.CheckID, r.RuleVersion, r.StartedAt, r.FinishedAt, r.Execution, r.Outcome, r.Targets, r.NextSteps)
}

func (r TooManyPartsReport) Header() ReportHeader {
	return headerFor(r.CheckID, r.RuleVersion, r.StartedAt, r.FinishedAt, r.Execution, r.Outcome, r.Targets, r.NextSteps)
}

func (r ReplicaStatusReport) Header() ReportHeader {
	return headerFor(r.CheckID, r.RuleVersion, r.StartedAt, r.FinishedAt, r.Execution, r.Outcome, r.Targets, r.NextSteps)
}

func (r MutationsStuckReport) Header() ReportHeader {
	return headerFor(r.CheckID, r.RuleVersion, r.StartedAt, r.FinishedAt, r.Execution, r.Outcome, r.Targets, r.NextSteps)
}

func (r QueueBacklogReport) Header() ReportHeader {
	return headerFor(r.CheckID, r.RuleVersion, r.StartedAt, r.FinishedAt, r.Execution, r.Outcome, r.Targets, r.NextSteps)
}

func (r DiskSpaceReport) Header() ReportHeader {
	return headerFor(r.CheckID, r.RuleVersion, r.StartedAt, r.FinishedAt, r.Execution, r.Outcome, r.Targets, r.NextSteps)
}

func (r DetachedPartsReport) Header() ReportHeader {
	return headerFor(r.CheckID, r.RuleVersion, r.StartedAt, r.FinishedAt, r.Execution, r.Outcome, r.Targets, r.NextSteps)
}
