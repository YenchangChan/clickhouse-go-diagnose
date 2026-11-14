// clickhouse-doctor 是 clickhouse-go-diagnose 库的命令行消费端。
//
// 职责边界（对应 ai-diagnostics.md 的三层架构）：
//   - 生成 Target（节点/分片身份）与查询适配器；
//   - 调用核心库的 Check* 函数并渲染报告；
//   - 不包含任何判定逻辑（判定全部在库中），不执行修复，
//     不引入 LLM 依赖。
//
// 结果语义遵循核心库契约：Execution 与 Outcome 分离；即使顶层返回 error，
// 报告中已保留的证据仍然成立——本工具总是先渲染报告再报告错误。
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	diagnose "github.com/YenchangChan/clickhouse-go-diagnose"
	"github.com/YenchangChan/clickhouse-go-diagnose/clickhouse-doctor/internal/buildinfo"
	"github.com/YenchangChan/clickhouse-go-diagnose/clickhouse-doctor/targets"
)

const (
	defaultTimeout = 10 * time.Second
	defaultNodeTO  = 2 * time.Second
)

// exitMismatched 等退出码：0 全部 complete+no_match；2 有匹配；
// 3 覆盖不完整（unknown/failed/partial+unknown）；1 调用或连接错误。
const (
	exitOK       = 0
	exitUsage    = 1
	exitMatched  = 2
	exitUncovSeg = 3
)

type options struct {
	addr       string
	httpAddr   string
	user       string
	password   string
	nodeID     string
	shardID    string
	nodes      targets.FlagList
	checks     string
	timeout    time.Duration
	nodeTO     time.Duration
	maxRows    int
	parallel   bool
	thresholds thresholds
	jsonOutput bool
	showVer    bool
}

// thresholds 是各检查的判定阈值。约定：0 表示使用核心库默认值（与库的
// 零值语义一致），负数在解析层拒绝。核心库负责判定，这里只透传。
type thresholds struct {
	replicaDelaySeconds int // replica_status：absolute_delay 秒数（库默认 300）
	parts               int // too_many_parts：单分区 part 数（库默认 1000）
	queueDepth          int // queue_backlog：单表在队条目数（库默认 100）
	queueAgeSeconds     int // queue_backlog：最老条目年龄秒数（库默认 3600）
	queueRetries        int // queue_backlog：最大重试次数（库默认 10）
	mutationAgeSeconds  int // mutations_stuck：mutation 运行时长秒数（库默认 86400）
	diskFreePercent     int // disk_space_low：磁盘剩余空间百分比（库默认 10）
	detachedCount       int // detached_parts：单表 detached part 数（库默认 10）
}

func validateThresholds(t thresholds) error {
	neg := func(name string, v int) error {
		if v < 0 {
			return fmt.Errorf("%s 不能为负", name)
		}
		return nil
	}
	for _, e := range []error{
		neg("--replica-delay-seconds", t.replicaDelaySeconds),
		neg("--parts-threshold", t.parts),
		neg("--queue-depth-threshold", t.queueDepth),
		neg("--queue-age-seconds", t.queueAgeSeconds),
		neg("--queue-retries-threshold", t.queueRetries),
		neg("--mutation-age-seconds", t.mutationAgeSeconds),
		neg("--disk-free-percent", t.diskFreePercent),
		neg("--detached-count-threshold", t.detachedCount),
	} {
		if e != nil {
			return e
		}
	}
	return nil
}

// buildNodeSpecs 解析多节点参数。优先 --node 重复传参（targets.FromNodes）；
// 未提供时退回单节点兼容形式（--addr / --http + --node-id / --shard-id）。
func buildNodeSpecs(o options) ([]targets.Spec, error) {
	if len(o.nodes) > 0 {
		return targets.FromNodes(o.nodes, o.shardID)
	}
	addr := o.addr
	if addr == "" && o.httpAddr != "" {
		addr = "http://" + o.httpAddr
	}
	if addr == "" {
		return nil, fmt.Errorf("必须提供 --node 或 --addr / --http")
	}
	nodeID := addr
	if o.nodeID != "" {
		nodeID = o.nodeID
	}
	return []targets.Spec{{ShardID: o.shardID, NodeID: nodeID, Addr: addr}}, nil
}

func main() {
	code, err := run(os.Args[1:], os.Stdout, os.Stderr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "clickhouse-doctor:", err)
	}
	os.Exit(code)
}

func run(args []string, stdout, stderr io.Writer) (int, error) {
	opts := options{}
	fs := flag.NewFlagSet("clickhouse-doctor", flag.ContinueOnError)
	fs.StringVar(&opts.addr, "addr", "", "native 协议地址 host:9000（必填，除非只用 --http）")
	fs.StringVar(&opts.httpAddr, "http", "", "HTTP 协议地址 host:8123（设置后走 database/sql 路径）")
	fs.StringVar(&opts.user, "user", "default", "用户名")
	fs.StringVar(&opts.password, "password", "", "密码；留空则读环境变量 CLICKHOUSE_DOCTOR_PASSWORD")
	fs.StringVar(&opts.nodeID, "node-id", "", "节点标识（仅单节点兼容形式；默认取 addr）")
	fs.StringVar(&opts.shardID, "shard-id", "shard-1", "默认分片标识（--node 未写 shard 前缀时使用）")
	fs.Var(&opts.nodes, "node", "目标节点，可重复：--node shard-1@host:9000 或 shard-1@node-别名@host:9000（同主机多实例）；地址写 http://host:8123 则走 HTTP 路径")
	fs.StringVar(&opts.checks, "check", "all", "检查项：all 或逗号分隔的名称（见 --list-checks）")
	fs.DurationVar(&opts.timeout, "timeout", defaultTimeout, "单次检查总预算")
	fs.DurationVar(&opts.nodeTO, "node-timeout", defaultNodeTO, "单节点预算；慢集群（弱磁盘虚拟机等）system.replicas / system.detached_parts 常需 60s 以上")
	fs.IntVar(&opts.maxRows, "max-rows-per-node", 0, "单节点保留条目上限（读 N+1 条即截断），0 用库默认 1000；detached_parts 等海量场景需调大")
	fs.BoolVar(&opts.parallel, "parallel-checks", false, "并发运行多个检查（默认串行，对慢集群更温和；开启后单检查的 worker 会叠加，注意服务端压力）")
	fs.IntVar(&opts.thresholds.replicaDelaySeconds, "replica-delay-seconds", 0, "replica_status 副本延迟阈值（秒），0 用库默认 300")
	fs.IntVar(&opts.thresholds.parts, "parts-threshold", 0, "too_many_parts 单分区 part 数阈值，0 用库默认 1000")
	fs.IntVar(&opts.thresholds.queueDepth, "queue-depth-threshold", 0, "queue_backlog 单表在队条目数阈值，0 用库默认 100")
	fs.IntVar(&opts.thresholds.queueAgeSeconds, "queue-age-seconds", 0, "queue_backlog 最老条目年龄阈值（秒），0 用库默认 3600")
	fs.IntVar(&opts.thresholds.queueRetries, "queue-retries-threshold", 0, "queue_backlog 最大重试次数阈值，0 用库默认 10")
	fs.IntVar(&opts.thresholds.mutationAgeSeconds, "mutation-age-seconds", 0, "mutations_stuck 运行时长阈值（秒），0 用库默认 86400")
	fs.IntVar(&opts.thresholds.diskFreePercent, "disk-free-percent", 0, "disk_space_low 剩余空间百分比阈值，0 用库默认 10")
	fs.IntVar(&opts.thresholds.detachedCount, "detached-count-threshold", 0, "detached_parts 单表 part 数阈值，0 用库默认 10")
	fs.BoolVar(&opts.jsonOutput, "json", false, "以 JSON 输出（稳定契约，供机器消费）")
	fs.BoolVar(&opts.showVer, "version", false, "打印版本")
	fs.BoolVar(&listChecks, "list-checks", false, "列出全部检查名后退出")
	if err := fs.Parse(args); err != nil {
		return exitUsage, err
	}

	if listChecks {
		printChecks(stdout)
		return exitOK, nil
	}
	if opts.showVer {
		fmt.Fprintf(stdout, "clickhouse-doctor %s (clickhouse-go-diagnose %s)\n", buildinfo.Version(), buildinfo.CoreVersion())
		return exitOK, nil
	}
	if opts.password == "" {
		opts.password = os.Getenv("CLICKHOUSE_DOCTOR_PASSWORD")
	}
	if opts.timeout < 0 || opts.nodeTO < 0 {
		return exitUsage, fmt.Errorf("timeout 与 node-timeout 不能为负")
	}
	if opts.maxRows < 0 {
		return exitUsage, fmt.Errorf("max-rows-per-node 不能为负")
	}
	if err := validateThresholds(opts.thresholds); err != nil {
		return exitUsage, err
	}

	specs, err := buildNodeSpecs(opts)
	if err != nil {
		return exitUsage, err
	}
	names, err := parseChecks(opts.checks)
	if err != nil {
		return exitUsage, err
	}

	targets, closers, err := targets.Open(specs, opts.user, opts.password)
	if err != nil {
		for _, c := range closers {
			c()
		}
		return exitUsage, err
	}
	defer func() {
		for _, c := range closers {
			c()
		}
	}()

	started := time.Now()
	results := make([]checkView, len(names))
	if opts.parallel && len(names) > 1 {
		// 并发跑检查，结果按 index 写回槽位，输出顺序与串行一致。
		// targets 的底层连接（database/sql 池、clickhouse-go 连接池）
		// 支持并发查询；并发度 = 检查数 × 单检查 worker 数，慢集群慎用。
		var wg sync.WaitGroup
		for i, name := range names {
			wg.Add(1)
			go func(i int, name string) {
				defer wg.Done()
				results[i] = runCheck(context.Background(), name, targets, opts.timeout, opts.nodeTO, opts.thresholds, opts.maxRows)
			}(i, name)
		}
		wg.Wait()
	} else {
		for i, name := range names {
			results[i] = runCheck(context.Background(), name, targets, opts.timeout, opts.nodeTO, opts.thresholds, opts.maxRows)
		}
	}
	meta := runMeta{
		Version:    buildinfo.Version(),
		Protocol:   protocolName(specs),
		Targets:    targetSummaries(specs),
		StartedAt:  started,
		FinishedAt: time.Now(),
	}

	if opts.jsonOutput {
		err = renderJSON(stdout, meta, results)
	} else {
		err = renderText(stdout, meta, results)
	}
	if err != nil {
		return exitUsage, err
	}
	return exitCode(results), nil
}

func protocolName(specs []targets.Spec) string {
	hasNative, hasHTTP := false, false
	for _, s := range specs {
		if targets.IsHTTP(s.Addr) {
			hasHTTP = true
		} else {
			hasNative = true
		}
	}
	switch {
	case hasNative && hasHTTP:
		return "mixed"
	case hasHTTP:
		return "http/database-sql"
	default:
		return "native"
	}
}

func targetSummaries(specs []targets.Spec) []targetSummary {
	out := make([]targetSummary, 0, len(specs))
	for _, s := range specs {
		proto := "native"
		if targets.IsHTTP(s.Addr) {
			proto = "http/database-sql"
		}
		out = append(out, targetSummary{NodeID: s.NodeID, ShardID: s.ShardID, Addr: s.Addr, Protocol: proto})
	}
	return out
}

// 检查名清单。名称稳定，供 --check 与 JSON 输出的 check 字段使用。
const (
	checkQueueFailures  = "replication_queue_failures"
	checkTooManyParts   = "too_many_parts"
	checkReplicaStatus  = "replica_status"
	checkMutationsStuck = "mutations_stuck"
	checkQueueBacklog   = "replication_queue_backlog"
	checkDiskSpaceLow   = "disk_space_low"
	checkDetachedParts  = "detached_parts"
)

var listChecks bool

// allCheckNames 从核心库 ListChecks() 取全部检查 ID：doctor 不维护
// 检查清单，主仓新增检查后 doctor 自动跟随（阈值与渲染转换需各自补注册）。
func allCheckNames() []string {
	metas := diagnose.ListChecks()
	names := make([]string, 0, len(metas))
	for _, m := range metas {
		names = append(names, m.ID)
	}
	return names
}

func printChecks(w io.Writer) {
	for _, m := range diagnose.ListChecks() {
		fmt.Fprintf(w, "%s\t%s\n", m.ID, m.Description)
	}
}

func parseChecks(spec string) ([]string, error) {
	spec = strings.TrimSpace(spec)
	if spec == "" || spec == "all" {
		return allCheckNames(), nil
	}
	seen := map[string]bool{}
	var names []string
	for _, part := range strings.Split(spec, ",") {
		name := strings.TrimSpace(part)
		if name == "" {
			continue
		}
		if name == "all" {
			names = append(names, allCheckNames()...)
			continue
		}
		valid := false
		for _, n := range allCheckNames() {
			if n == name {
				valid = true
				break
			}
		}
		if !valid {
			return nil, fmt.Errorf("未知检查名 %q（--list-checks 查看全部）", name)
		}
		if !seen[name] {
			seen[name] = true
			names = append(names, name)
		}
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("--check 未解析出任何检查名")
	}
	return names, nil
}
