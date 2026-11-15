package diagnose

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const distributedDDLCheckID = "distributed_ddl"
const distributedDDLRuleVersion = "1"

const distributedDDLSQL = `SELECT
    entry,
    host,
    status,
    toString(exception_code),
    substring(exception_text, 1, 4096),
    toString(dateDiff('second', query_create_time, now())),
    substring(query, 1, 4096)
FROM system.distributed_ddl_queue
WHERE entry = ? AND status != 'Finished'
ORDER BY host, status`

type DistributedDDLOptions struct {
	Timeout, NodeTimeout        time.Duration
	Concurrency, MaxRowsPerNode int
	Entry                       string
}

func CheckDistributedDDL(ctx context.Context, targets []Target, opts DistributedDDLOptions) (DistributedDDLReport, error) {
	if err := validateTargets(targets); err != nil {
		return DistributedDDLReport{}, err
	}
	if opts.Timeout < 0 || opts.NodeTimeout < 0 || opts.Concurrency < 0 || opts.MaxRowsPerNode < 0 {
		return DistributedDDLReport{}, inputError("预算项不允许为负值")
	}
	if strings.TrimSpace(opts.Entry) == "" || len(opts.Entry) > 512 {
		return DistributedDDLReport{}, inputError("DDL entry 为空或过长")
	}
	b := budget{timeout: opts.Timeout, nodeTimeout: opts.NodeTimeout, concurrency: opts.Concurrency, maxRows: opts.MaxRowsPerNode}
	fillBudget(&b, defaultTimeout, defaultNodeTimeout, defaultConcurrency, defaultMaxRowsPerNode)
	c := collector[DistributedDDLMatch]{sql: distributedDDLSQL, args: []any{opts.Entry}, scan: func(t Target, rows Rows) (DistributedDDLMatch, bool, error) {
		var entry, host, status, code, exception, age, query sql.NullString
		if err := rows.Scan(&entry, &host, &status, &code, &exception, &age, &query); err != nil {
			return DistributedDDLMatch{}, false, err
		}
		codeN, err := strconv.Atoi(code.String)
		if err != nil {
			return DistributedDDLMatch{}, false, fmt.Errorf("exception_code: %w", err)
		}
		ageN, err := strconv.Atoi(age.String)
		if err != nil {
			return DistributedDDLMatch{}, false, fmt.Errorf("age_seconds: %w", err)
		}
		return DistributedDDLMatch{ShardID: t.ShardID, NodeID: t.NodeID, Entry: entry.String, Host: host.String, Status: status.String, ExceptionCode: codeN, ExceptionText: exception.String, AgeSeconds: ageN, Query: query.String, Reasons: []string{ReasonDistributedDDLStuck}}, true, nil
	}}
	started := time.Now()
	results, matches, cerr := collect(ctx, targets, b, c)
	exe := summarizeExecution(results)
	outcome := outcomeFor(exe, len(matches) > 0)
	report := DistributedDDLReport{CheckID: distributedDDLCheckID, RuleVersion: distributedDDLRuleVersion, StartedAt: started, FinishedAt: time.Now(), Execution: exe, Outcome: outcome, Targets: results, Matches: matches, NextSteps: coverageFollowUps(exe, results)}
	if len(matches) > 0 {
		report.NextSteps = append([]FollowUp{{Code: AdviceInspectDDLHosts, Detail: "按 host 核对未完成状态、异常文本以及目标节点可用性"}}, report.NextSteps...)
	}
	if cerr != nil {
		return report, cerr
	}
	if exe == ExecutionFailed {
		return report, ErrNoUsableObservation
	}
	return report, nil
}
