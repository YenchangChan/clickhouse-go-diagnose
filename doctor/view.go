package main

import (
	"context"
	"time"

	diagnose "github.com/YenchangChan/clickhouse-go-diagnose"
)

// 本文件的视图类型是 clickhouse-doctor 拥有的稳定输出契约（JSON）：
// 字段名与取值不随核心库内部类型变动；新增字段只做加法。
// 渲染语义遵循核心库契约：partial/unknown 不得渲染成健康，
// error 文本可能含服务端敏感信息，展示方需按文本处理。

type runMeta struct {
	Version    string          `json:"version"`
	Protocol   string          `json:"protocol"`
	Targets    []targetSummary `json:"targets"`
	StartedAt  time.Time       `json:"started_at"`
	FinishedAt time.Time       `json:"finished_at"`
}

// targetSummary 记录本次运行的目标身份清单；NodeID 与 ShardID 与
// 各检查报告 targets 字段中的取值一致，便于关联。
type targetSummary struct {
	NodeID   string `json:"node_id"`
	ShardID  string `json:"shard_id"`
	Addr     string `json:"addr"`
	Protocol string `json:"protocol"`
}

type targetView struct {
	NodeID      string `json:"node_id"`
	ShardID     string `json:"shard_id"`
	Status      string `json:"status"`
	RowsRead    int    `json:"rows_read"`
	RowsKept    int    `json:"rows_kept"`
	Truncated   bool   `json:"truncated"`
	ErrCategory string `json:"error_category,omitempty"`
	ErrMessage  string `json:"error_message,omitempty"`
}

type followUpView struct {
	Code   string `json:"code"`
	Detail string `json:"detail,omitempty"`
}

type checkView struct {
	Check       string         `json:"check"`
	Execution   string         `json:"execution"`
	Outcome     string         `json:"outcome"`
	Err         string         `json:"error,omitempty"`
	CheckID     string         `json:"check_id,omitempty"`
	RuleVersion string         `json:"rule_version,omitempty"`
	StartedAt   time.Time      `json:"started_at,omitempty"`
	FinishedAt  time.Time      `json:"finished_at,omitempty"`
	Targets     []targetView   `json:"targets"`
	Matches     []any          `json:"matches"`
	NextSteps   []followUpView `json:"next_steps"`
}

// 各检查匹配视图。object 字段与核心库归并键一一对应。

type queueFailureMatchView struct {
	ShardID      string                `json:"shard_id"`
	Database     string                `json:"database"`
	Table        string                `json:"table"`
	PartName     string                `json:"part_name"`
	Observations []queueFailureObsView `json:"observations"`
}

type queueFailureObsView struct {
	NodeID        string `json:"node_id"`
	LastException string `json:"last_exception,omitempty"`
}

type partsMatchView struct {
	ShardID      string         `json:"shard_id"`
	Database     string         `json:"database"`
	Table        string         `json:"table"`
	Partition    string         `json:"partition"`
	Parts        int            `json:"parts"`
	Observations []partsObsView `json:"observations"`
}

type partsObsView struct {
	NodeID    string `json:"node_id"`
	Parts     int    `json:"parts"`
	DiskBytes string `json:"disk_bytes"`
}

type replicaMatchView struct {
	ShardID      string           `json:"shard_id"`
	Database     string           `json:"database"`
	Table        string           `json:"table"`
	Observations []replicaObsView `json:"observations"`
}

type replicaObsView struct {
	NodeID              string   `json:"node_id"`
	IsReadonly          bool     `json:"is_readonly"`
	IsSessionExpired    bool     `json:"is_session_expired"`
	AbsoluteDelaySecond int      `json:"absolute_delay_seconds"`
	TotalReplicas       int      `json:"total_replicas"`
	ActiveReplicas      int      `json:"active_replicas"`
	LostPartCount       int      `json:"lost_part_count"`
	Reasons             []string `json:"reasons"`
}

type mutationMatchView struct {
	ShardID      string            `json:"shard_id"`
	Database     string            `json:"database"`
	Table        string            `json:"table"`
	MutationID   string            `json:"mutation_id"`
	Observations []mutationObsView `json:"observations"`
}

type mutationObsView struct {
	NodeID           string   `json:"node_id"`
	Command          string   `json:"command,omitempty"`
	AgeSeconds       int      `json:"age_seconds"`
	LatestFailReason string   `json:"latest_fail_reason,omitempty"`
	Reasons          []string `json:"reasons"`
}

type backlogMatchView struct {
	ShardID          string           `json:"shard_id"`
	Database         string           `json:"database"`
	Table            string           `json:"table"`
	Depth            int              `json:"depth"`
	OldestAgeSeconds int              `json:"oldest_age_seconds"`
	MaxRetries       int              `json:"max_retries"`
	Observations     []backlogObsView `json:"observations"`
}

type backlogObsView struct {
	NodeID           string   `json:"node_id"`
	Depth            int      `json:"depth"`
	OldestAgeSeconds int      `json:"oldest_age_seconds"`
	MaxRetries       int      `json:"max_retries"`
	FailingEntries   int      `json:"failing_entries"`
	Reasons          []string `json:"reasons"`
}

type diskMatchView struct {
	ShardID      string        `json:"shard_id"`
	DiskName     string        `json:"disk_name"`
	Path         string        `json:"path"`
	FreePercent  int           `json:"free_percent"`
	Observations []diskObsView `json:"observations"`
}

type diskObsView struct {
	NodeID        string   `json:"node_id"`
	FreeBytes     uint64   `json:"free_bytes"`
	TotalBytes    uint64   `json:"total_bytes"`
	KeepFreeBytes uint64   `json:"keep_free_bytes"`
	FreePercent   int      `json:"free_percent"`
	Reasons       []string `json:"reasons"`
}

type detachedMatchView struct {
	ShardID      string            `json:"shard_id"`
	Database     string            `json:"database"`
	Table        string            `json:"table"`
	Parts        int               `json:"parts"`
	Reasons      []string          `json:"reasons,omitempty"`
	Observations []detachedObsView `json:"observations"`
}

type detachedObsView struct {
	NodeID      string   `json:"node_id"`
	PartName    string   `json:"part_name"`
	Reason      string   `json:"reason,omitempty"`
	Disk        string   `json:"disk,omitempty"`
	BytesOnDisk uint64   `json:"bytes_on_disk"`
	Reasons     []string `json:"reasons"`
}

// viewTargets 转换节点结果。时间戳不进视图（单节点 CLI 意义有限，JSON 保持精简）。
func viewTargets(ns []diagnose.NodeResult) []targetView {
	out := make([]targetView, 0, len(ns))
	for _, n := range ns {
		out = append(out, targetView{
			NodeID:      n.NodeID,
			ShardID:     n.ShardID,
			Status:      string(n.Status),
			RowsRead:    n.RowsRead,
			RowsKept:    n.RowsKept,
			Truncated:   n.Truncated,
			ErrCategory: n.ErrCategory,
			ErrMessage:  n.ErrMessage,
		})
	}
	return out
}

func viewFollowUps(fs []diagnose.FollowUp) []followUpView {
	out := make([]followUpView, 0, len(fs))
	for _, f := range fs {
		out = append(out, followUpView{Code: f.Code, Detail: f.Detail})
	}
	return out
}

// runCheck 按名称调用核心库检查，并把报告转换为视图。
// 顶层 error 始终记入视图 Err 字段，报告本身照常渲染（契约：先报告后错误）。
func runCheck(ctx context.Context, name string, targets []diagnose.Target, total, node time.Duration) checkView {
	v := checkView{Check: name, Targets: []targetView{}, Matches: []any{}, NextSteps: []followUpView{}}
	switch name {
	case checkQueueFailures:
		rep, err := diagnose.CheckReplicationQueueFailures(ctx, targets, diagnose.ReplicationQueueOptions{
			Timeout: total, NodeTimeout: node,
		})
		fillCommon(&v, rep.CheckID, rep.RuleVersion, rep.StartedAt, rep.FinishedAt, rep.Execution, rep.Outcome, rep.Targets, rep.NextSteps, err)
		for _, m := range rep.Matches {
			obs := make([]queueFailureObsView, 0, len(m.Observations))
			for _, o := range m.Observations {
				obs = append(obs, queueFailureObsView{NodeID: o.NodeID, LastException: o.LastException})
			}
			v.Matches = append(v.Matches, queueFailureMatchView{
				ShardID: m.ShardID, Database: m.Database, Table: m.Table, PartName: m.PartName,
				Observations: obs,
			})
		}

	case checkTooManyParts:
		rep, err := diagnose.CheckTooManyParts(ctx, targets, diagnose.TooManyPartsOptions{
			Timeout: total, NodeTimeout: node,
		})
		fillCommon(&v, rep.CheckID, rep.RuleVersion, rep.StartedAt, rep.FinishedAt, rep.Execution, rep.Outcome, rep.Targets, rep.NextSteps, err)
		for _, m := range rep.Matches {
			obs := make([]partsObsView, 0, len(m.Observations))
			for _, o := range m.Observations {
				obs = append(obs, partsObsView{NodeID: o.NodeID, Parts: o.Parts, DiskBytes: o.DiskBytes})
			}
			v.Matches = append(v.Matches, partsMatchView{
				ShardID: m.ShardID, Database: m.Database, Table: m.Table, Partition: m.Partition,
				Parts: m.Parts, Observations: obs,
			})
		}

	case checkReplicaStatus:
		rep, err := diagnose.CheckReplicaStatus(ctx, targets, diagnose.ReplicaStatusOptions{
			Timeout: total, NodeTimeout: node,
		})
		fillCommon(&v, rep.CheckID, rep.RuleVersion, rep.StartedAt, rep.FinishedAt, rep.Execution, rep.Outcome, rep.Targets, rep.NextSteps, err)
		for _, m := range rep.Matches {
			obs := make([]replicaObsView, 0, len(m.Observations))
			for _, o := range m.Observations {
				obs = append(obs, replicaObsView{
					NodeID: o.NodeID, IsReadonly: o.IsReadonly, IsSessionExpired: o.IsSessionExpired,
					AbsoluteDelaySecond: o.AbsoluteDelay, TotalReplicas: o.TotalReplicas,
					ActiveReplicas: o.ActiveReplicas, LostPartCount: o.LostPartCount, Reasons: o.Reasons,
				})
			}
			v.Matches = append(v.Matches, replicaMatchView{
				ShardID: m.ShardID, Database: m.Database, Table: m.Table, Observations: obs,
			})
		}

	case checkMutationsStuck:
		rep, err := diagnose.CheckMutationsStuck(ctx, targets, diagnose.MutationsStuckOptions{
			Timeout: total, NodeTimeout: node,
		})
		fillCommon(&v, rep.CheckID, rep.RuleVersion, rep.StartedAt, rep.FinishedAt, rep.Execution, rep.Outcome, rep.Targets, rep.NextSteps, err)
		for _, m := range rep.Matches {
			obs := make([]mutationObsView, 0, len(m.Observations))
			for _, o := range m.Observations {
				obs = append(obs, mutationObsView{
					NodeID: o.NodeID, Command: o.Command, AgeSeconds: o.AgeSeconds,
					LatestFailReason: o.LatestFailReason, Reasons: o.Reasons,
				})
			}
			v.Matches = append(v.Matches, mutationMatchView{
				ShardID: m.ShardID, Database: m.Database, Table: m.Table, MutationID: m.MutationID,
				Observations: obs,
			})
		}

	case checkQueueBacklog:
		rep, err := diagnose.CheckReplicationQueueBacklog(ctx, targets, diagnose.QueueBacklogOptions{
			Timeout: total, NodeTimeout: node,
		})
		fillCommon(&v, rep.CheckID, rep.RuleVersion, rep.StartedAt, rep.FinishedAt, rep.Execution, rep.Outcome, rep.Targets, rep.NextSteps, err)
		for _, m := range rep.Matches {
			obs := make([]backlogObsView, 0, len(m.Observations))
			for _, o := range m.Observations {
				obs = append(obs, backlogObsView{
					NodeID: o.NodeID, Depth: o.Depth, OldestAgeSeconds: o.OldestAgeSeconds,
					MaxRetries: o.MaxRetries, FailingEntries: o.FailingEntries, Reasons: o.Reasons,
				})
			}
			v.Matches = append(v.Matches, backlogMatchView{
				ShardID: m.ShardID, Database: m.Database, Table: m.Table,
				Depth: m.Depth, OldestAgeSeconds: m.OldestAgeSeconds, MaxRetries: m.MaxRetries,
				Observations: obs,
			})
		}

	case checkDiskSpaceLow:
		rep, err := diagnose.CheckDiskSpaceLow(ctx, targets, diagnose.DiskSpaceOptions{
			Timeout: total, NodeTimeout: node,
		})
		fillCommon(&v, rep.CheckID, rep.RuleVersion, rep.StartedAt, rep.FinishedAt, rep.Execution, rep.Outcome, rep.Targets, rep.NextSteps, err)
		for _, m := range rep.Matches {
			obs := make([]diskObsView, 0, len(m.Observations))
			for _, o := range m.Observations {
				obs = append(obs, diskObsView{
					NodeID: o.NodeID, FreeBytes: o.FreeBytes, TotalBytes: o.TotalBytes,
					KeepFreeBytes: o.KeepFreeBytes, FreePercent: o.FreePercent, Reasons: o.Reasons,
				})
			}
			v.Matches = append(v.Matches, diskMatchView{
				ShardID: m.ShardID, DiskName: m.DiskName, Path: m.Path,
				FreePercent: m.FreePercent, Observations: obs,
			})
		}

	case checkDetachedParts:
		rep, err := diagnose.CheckDetachedParts(ctx, targets, diagnose.DetachedPartsOptions{
			Timeout: total, NodeTimeout: node,
		})
		fillCommon(&v, rep.CheckID, rep.RuleVersion, rep.StartedAt, rep.FinishedAt, rep.Execution, rep.Outcome, rep.Targets, rep.NextSteps, err)
		for _, m := range rep.Matches {
			obs := make([]detachedObsView, 0, len(m.Observations))
			for _, o := range m.Observations {
				obs = append(obs, detachedObsView{
					NodeID: o.NodeID, PartName: o.PartName, Reason: o.Reason,
					Disk: o.Disk, BytesOnDisk: o.BytesOnDisk, Reasons: o.Reasons,
				})
			}
			v.Matches = append(v.Matches, detachedMatchView{
				ShardID: m.ShardID, Database: m.Database, Table: m.Table,
				Parts: m.Parts, Reasons: m.Reasons, Observations: obs,
			})
		}
	}
	return v
}

func fillCommon(v *checkView, checkID, ruleVersion string, started, finished time.Time,
	exec diagnose.Execution, outcome diagnose.Outcome, targets []diagnose.NodeResult,
	next []diagnose.FollowUp, err error) {

	v.CheckID = checkID
	v.RuleVersion = ruleVersion
	v.StartedAt = started
	v.FinishedAt = finished
	v.Execution = string(exec)
	v.Outcome = string(outcome)
	v.Targets = viewTargets(targets)
	v.NextSteps = viewFollowUps(next)
	if err != nil {
		v.Err = err.Error()
	}
}

// exitCode 汇总退出码：matched 优先，其次覆盖不完整，最后全部干净为 0。
func exitCode(results []checkView) int {
	matched, uncovered := false, false
	for _, r := range results {
		switch {
		case r.Outcome == string(diagnose.OutcomeMatched):
			matched = true
		case r.Outcome == string(diagnose.OutcomeUnknown),
			r.Execution != string(diagnose.ExecutionComplete):
			uncovered = true
		}
	}
	switch {
	case matched:
		return exitMatched
	case uncovered:
		return exitUncovSeg
	default:
		return exitOK
	}
}
