package diagnose

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"
)

const (
	resourcePressureCheckID       = "resource_pressure"
	resourcePressureRuleVersion   = "1"
	defaultCPUPercentThreshold    = 80
	defaultMemoryPercentThreshold = 85
)

const resourcePressureSQL = `SELECT
    toString(maxIf(value, metric = 'OSUserTimeNormalized') + maxIf(value, metric = 'OSSystemTimeNormalized')) AS cpu_normalized,
    toString(maxIf(value, metric = 'MemoryResident')) AS memory_resident,
    toString(maxIf(value, metric = 'OSMemoryTotal')) AS memory_total
FROM system.asynchronous_metrics
HAVING countIf(metric IN ('OSUserTimeNormalized', 'OSSystemTimeNormalized', 'MemoryResident', 'OSMemoryTotal')) > 0`

type ResourcePressureOptions struct {
	Timeout, NodeTimeout                        time.Duration
	Concurrency, MaxRowsPerNode                 int
	CPUPercentThreshold, MemoryPercentThreshold int
}

type resourcePressureObservation struct {
	ShardID, NodeID             string
	CPUPercent, MemoryPercent   float64
	MemoryResident, MemoryTotal uint64
	Reasons                     []string
}

func CheckResourcePressure(ctx context.Context, targets []Target, opts ResourcePressureOptions) (ResourcePressureReport, error) {
	if err := validateTargets(targets); err != nil {
		return ResourcePressureReport{}, err
	}
	if opts.Timeout < 0 || opts.NodeTimeout < 0 || opts.Concurrency < 0 || opts.MaxRowsPerNode < 0 || opts.CPUPercentThreshold < 0 || opts.MemoryPercentThreshold < 0 {
		return ResourcePressureReport{}, inputError("预算与阈值不允许为负值")
	}
	if opts.CPUPercentThreshold == 0 {
		opts.CPUPercentThreshold = defaultCPUPercentThreshold
	}
	if opts.MemoryPercentThreshold == 0 {
		opts.MemoryPercentThreshold = defaultMemoryPercentThreshold
	}
	if opts.CPUPercentThreshold > 100 || opts.MemoryPercentThreshold > 100 {
		return ResourcePressureReport{}, inputError("资源百分比阈值不得超过 100")
	}
	b := budget{timeout: opts.Timeout, nodeTimeout: opts.NodeTimeout, concurrency: opts.Concurrency, maxRows: opts.MaxRowsPerNode}
	fillBudget(&b, defaultTimeout, defaultNodeTimeout, defaultConcurrency, defaultMaxRowsPerNode)
	c := collector[resourcePressureObservation]{sql: resourcePressureSQL, scan: func(t Target, rows Rows) (resourcePressureObservation, bool, error) {
		var cpu, resident, total sql.NullString
		if err := rows.Scan(&cpu, &resident, &total); err != nil {
			return resourcePressureObservation{}, false, err
		}
		cpuN, err := strconv.ParseFloat(cpu.String, 64)
		if err != nil {
			return resourcePressureObservation{}, false, fmt.Errorf("cpu metric: %w", err)
		}
		residentN, err := strconv.ParseUint(resident.String, 10, 64)
		if err != nil {
			return resourcePressureObservation{}, false, fmt.Errorf("memory resident: %w", err)
		}
		totalN, err := strconv.ParseUint(total.String, 10, 64)
		if err != nil {
			return resourcePressureObservation{}, false, fmt.Errorf("memory total: %w", err)
		}
		o := resourcePressureObservation{ShardID: t.ShardID, NodeID: t.NodeID, CPUPercent: cpuN * 100, MemoryResident: residentN, MemoryTotal: totalN}
		if totalN > 0 {
			o.MemoryPercent = float64(residentN) * 100 / float64(totalN)
		}
		if o.CPUPercent >= float64(opts.CPUPercentThreshold) {
			o.Reasons = append(o.Reasons, ReasonCPUUsageHigh)
		}
		if totalN > 0 && o.MemoryPercent >= float64(opts.MemoryPercentThreshold) {
			o.Reasons = append(o.Reasons, ReasonMemoryUsageHigh)
		}
		return o, true, nil
	}}
	started := time.Now()
	results, observations, cerr := collect(ctx, targets, b, c)
	snapshots := make([]ResourcePressureMatch, 0, len(observations))
	matches := make([]ResourcePressureMatch, 0)
	for _, o := range observations {
		m := ResourcePressureMatch{ShardID: o.ShardID, NodeID: o.NodeID, CPUPercent: o.CPUPercent, MemoryPercent: o.MemoryPercent, MemoryResident: o.MemoryResident, MemoryTotal: o.MemoryTotal, Reasons: o.Reasons}
		snapshots = append(snapshots, m)
		if len(o.Reasons) > 0 {
			matches = append(matches, m)
		}
	}
	exe := summarizeExecution(results)
	outcome := outcomeFor(exe, len(matches) > 0)
	next := coverageFollowUps(exe, results)
	if len(matches) > 0 {
		next = append([]FollowUp{{Code: AdviceInspectResourceConsumers, Detail: "核对同一时间窗口内的查询、合并、mutation 与后台任务资源消耗"}}, next...)
	}
	report := ResourcePressureReport{CheckID: resourcePressureCheckID, RuleVersion: resourcePressureRuleVersion, StartedAt: started, FinishedAt: time.Now(), Execution: exe, Outcome: outcome, Targets: results, Snapshots: snapshots, Matches: matches, NextSteps: next}
	if cerr != nil {
		return report, cerr
	}
	if exe == ExecutionFailed && len(observations) == 0 {
		return report, ErrNoUsableObservation
	}
	return report, nil
}
