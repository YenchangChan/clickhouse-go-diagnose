package diagnose

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const slowQueryCheckID = "slow_query"
const slowQueryRuleVersion = "1"

const slowQuerySQL = `SELECT
    query_id,
    toString(query_duration_ms),
    toString(read_rows),
    toString(read_bytes),
    toString(result_rows),
    toString(memory_usage),
    toString(ProfileEvents['UserTimeMicroseconds'] + ProfileEvents['SystemTimeMicroseconds']),
    substring(query, 1, 4096)
FROM system.query_log
WHERE type = 2 AND query_id = ?
ORDER BY event_time DESC
LIMIT 1`

type SlowQueryOptions struct {
	Timeout, NodeTimeout        time.Duration
	Concurrency, MaxRowsPerNode int
	QueryID                     string
}

func CheckSlowQuery(ctx context.Context, targets []Target, opts SlowQueryOptions) (SlowQueryReport, error) {
	if err := validateTargets(targets); err != nil {
		return SlowQueryReport{}, err
	}
	if opts.Timeout < 0 || opts.NodeTimeout < 0 || opts.Concurrency < 0 || opts.MaxRowsPerNode < 0 {
		return SlowQueryReport{}, inputError("预算项不允许为负值")
	}
	if strings.TrimSpace(opts.QueryID) == "" || len(opts.QueryID) > 256 {
		return SlowQueryReport{}, inputError("query_id 为空或过长")
	}
	b := budget{timeout: opts.Timeout, nodeTimeout: opts.NodeTimeout, concurrency: opts.Concurrency, maxRows: opts.MaxRowsPerNode}
	fillBudget(&b, defaultTimeout, defaultNodeTimeout, defaultConcurrency, defaultMaxRowsPerNode)
	c := collector[SlowQueryMatch]{sql: slowQuerySQL, args: []any{opts.QueryID}, scan: func(t Target, rows Rows) (SlowQueryMatch, bool, error) {
		var id, duration, readRows, readBytes, resultRows, memory, cpu, query sql.NullString
		if err := rows.Scan(&id, &duration, &readRows, &readBytes, &resultRows, &memory, &cpu, &query); err != nil {
			return SlowQueryMatch{}, false, err
		}
		parse := func(name, value string) (uint64, error) {
			n, err := strconv.ParseUint(value, 10, 64)
			if err != nil {
				return 0, fmt.Errorf("%s: %w", name, err)
			}
			return n, nil
		}
		d, err := parse("query_duration_ms", duration.String)
		if err != nil {
			return SlowQueryMatch{}, false, err
		}
		rr, err := parse("read_rows", readRows.String)
		if err != nil {
			return SlowQueryMatch{}, false, err
		}
		rb, err := parse("read_bytes", readBytes.String)
		if err != nil {
			return SlowQueryMatch{}, false, err
		}
		out, err := parse("result_rows", resultRows.String)
		if err != nil {
			return SlowQueryMatch{}, false, err
		}
		mem, err := parse("memory_usage", memory.String)
		if err != nil {
			return SlowQueryMatch{}, false, err
		}
		cpuN, err := parse("cpu_time_us", cpu.String)
		if err != nil {
			return SlowQueryMatch{}, false, err
		}
		return SlowQueryMatch{ShardID: t.ShardID, NodeID: t.NodeID, QueryID: id.String, DurationMS: d, ReadRows: rr, ReadBytes: rb, ResultRows: out, MemoryUsage: mem, CPUTimeUS: cpuN, Query: query.String, Reasons: []string{ReasonSlowQuerySelected}}, true, nil
	}}
	started := time.Now()
	results, matches, cerr := collect(ctx, targets, b, c)
	exe := summarizeExecution(results)
	outcome := outcomeFor(exe, len(matches) > 0)
	report := SlowQueryReport{CheckID: slowQueryCheckID, RuleVersion: slowQueryRuleVersion, StartedAt: started, FinishedAt: time.Now(), Execution: exe, Outcome: outcome, Targets: results, Matches: matches, NextSteps: coverageFollowUps(exe, results)}
	if len(matches) > 0 {
		report.NextSteps = append([]FollowUp{{Code: AdviceExplainSlowQuery, Detail: "结合 EXPLAIN、读取行数、内存与 CPU 时间核对过滤、连接和聚合代价"}}, report.NextSteps...)
	}
	if cerr != nil {
		return report, cerr
	}
	if exe == ExecutionFailed {
		return report, ErrNoUsableObservation
	}
	return report, nil
}
