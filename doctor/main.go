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
	"database/sql"
	"flag"
	"fmt"
	"io"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	_ "github.com/ClickHouse/clickhouse-go/v2" // database/sql 驱动注册
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	diagnose "github.com/YenchangChan/clickhouse-go-diagnose"
)

const (
	version        = "0.1.0"
	coreVersion    = "v0.2.0"
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
	nodes      nodeFlags
	checks     string
	timeout    time.Duration
	nodeTO     time.Duration
	jsonOutput bool
	showVer    bool
}

// nodeFlags 支持重复传参：--node shard-1@host:9000 --node shard-2@host:9000。
// 地址写 http:// 前缀则该节点走 HTTP/database/sql 路径，混合协议按节点生效。
type nodeFlags []string

func (n *nodeFlags) String() string { return strings.Join(*n, ",") }

func (n *nodeFlags) Set(s string) error {
	if s == "" {
		return fmt.Errorf("--node 不能为空")
	}
	*n = append(*n, s)
	return nil
}

// nodeSpec 是一个目标的完整身份：分片、节点标识与连接地址。
type nodeSpec struct {
	shardID string
	nodeID  string
	addr    string // 原样保留；http:// 前缀表示 HTTP/database/sql 路径
}

func isHTTPAddr(addr string) bool { return strings.HasPrefix(addr, "http://") }

// buildNodeSpecs 解析多节点参数。优先 --node 重复传参；未提供时退回
// 单节点兼容形式（--addr / --http + --node-id / --shard-id）。
// NodeID 默认取地址本身；重复 NodeID 在此拒绝，而不是让库报 ErrInvalidInput。
func buildNodeSpecs(o options) ([]nodeSpec, error) {
	var specs []nodeSpec
	if len(o.nodes) > 0 {
		for _, raw := range o.nodes {
			shard, nodeID := o.shardID, ""
			addr := raw
			parts := strings.SplitN(raw, "@", 3)
			switch len(parts) {
			case 2: // shard-id@addr
				shard, addr = strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
			case 3: // shard-id@node-id@addr（同一主机多实例必须显式区分 node-id）
				shard, nodeID, addr = strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), strings.TrimSpace(parts[2])
			}
			if shard == "" || addr == "" || (len(parts) == 3 && nodeID == "") || strings.Contains(addr, "@") {
				return nil, fmt.Errorf("--node %q 格式错误：应为 shard-id@addr 或 shard-id@node-id@addr", raw)
			}
			if nodeID == "" {
				nodeID = addr
			}
			specs = append(specs, nodeSpec{shardID: shard, nodeID: nodeID, addr: addr})
		}
	} else {
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
		specs = []nodeSpec{{shardID: o.shardID, nodeID: nodeID, addr: addr}}
	}

	seen := map[string]string{}
	for _, s := range specs {
		if prev, dup := seen[s.nodeID]; dup {
			return nil, fmt.Errorf("节点标识 %q 重复（出现在 %s 与 %s）；同一主机的多个实例必须用不同标识", s.nodeID, prev, s.addr)
		}
		seen[s.nodeID] = s.addr
	}
	return specs, nil
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
	fs.DurationVar(&opts.nodeTO, "node-timeout", defaultNodeTO, "单节点预算")
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
		fmt.Fprintf(stdout, "clickhouse-doctor %s (clickhouse-go-diagnose %s)\n", version, coreVersion)
		return exitOK, nil
	}
	if opts.password == "" {
		opts.password = os.Getenv("CLICKHOUSE_DOCTOR_PASSWORD")
	}
	if opts.timeout < 0 || opts.nodeTO < 0 {
		return exitUsage, fmt.Errorf("timeout 与 node-timeout 不能为负")
	}

	specs, err := buildNodeSpecs(opts)
	if err != nil {
		return exitUsage, err
	}
	names, err := parseChecks(opts.checks)
	if err != nil {
		return exitUsage, err
	}

	targets, closers, err := openTargets(specs, opts.user, opts.password)
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
	results := make([]checkView, 0, len(names))
	for _, name := range names {
		v := runCheck(context.Background(), name, targets, opts.timeout, opts.nodeTO)
		results = append(results, v)
	}
	meta := runMeta{
		Version:    version,
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

func protocolName(specs []nodeSpec) string {
	hasNative, hasHTTP := false, false
	for _, s := range specs {
		if isHTTPAddr(s.addr) {
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

func targetSummaries(specs []nodeSpec) []targetSummary {
	out := make([]targetSummary, 0, len(specs))
	for _, s := range specs {
		proto := "native"
		if isHTTPAddr(s.addr) {
			proto = "http/database-sql"
		}
		out = append(out, targetSummary{NodeID: s.nodeID, ShardID: s.shardID, Addr: s.addr, Protocol: proto})
	}
	return out
}

// openTargets 逐个建连并组装 diagnose.Target。任一节点连接失败立即返回错误
// （已打开的连接由调用方关闭）；建连与 Ping 不计入检查预算，单节点上限 5s。
func openTargets(specs []nodeSpec, user, password string) ([]diagnose.Target, []func(), error) {
	var (
		targets []diagnose.Target
		closers []func()
	)
	for _, s := range specs {
		q, closeFn, err := openQuerier(s.addr, user, password)
		if err != nil {
			return nil, closers, fmt.Errorf("连接 %s (%s): %w", s.nodeID, s.shardID, err)
		}
		closers = append(closers, closeFn)
		targets = append(targets, diagnose.Target{NodeID: s.nodeID, ShardID: s.shardID, Querier: q})
	}
	return targets, closers, nil
}

// openQuerier 按地址建连。http:// 前缀走 database/sql（HTTP DSN），否则 native。
func openQuerier(addr, user, password string) (diagnose.Querier, func(), error) {
	if isHTTPAddr(addr) {
		host := strings.TrimPrefix(addr, "http://")
		u := url.URL{
			Scheme: "http",
			User:   url.UserPassword(user, password),
			Host:   host,
			Path:   "default",
		}
		db, err := sql.Open("clickhouse", u.String())
		if err != nil {
			return nil, nil, err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := db.PingContext(ctx); err != nil {
			_ = db.Close()
			return nil, nil, err
		}
		return &sqlQuerier{db: db}, func() { _ = db.Close() }, nil
	}

	opt := &clickhouse.Options{
		Addr:        []string{addr},
		Auth:        clickhouse.Auth{Database: "default", Username: user, Password: password},
		DialTimeout: 5 * time.Second,
	}
	conn, err := clickhouse.Open(opt)
	if err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	return &nativeQuerier{conn: conn}, func() { _ = conn.Close() }, nil
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

func allCheckNames() []string {
	return []string{
		checkQueueFailures,
		checkTooManyParts,
		checkReplicaStatus,
		checkMutationsStuck,
		checkQueueBacklog,
		checkDiskSpaceLow,
		checkDetachedParts,
	}
}

func printChecks(w io.Writer) {
	for _, n := range allCheckNames() {
		fmt.Fprintln(w, n)
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

// nativeQuerier 把 clickhouse-go native 连接适配为 diagnose.Querier。
type nativeQuerier struct{ conn driver.Conn }

func (n *nativeQuerier) Query(ctx context.Context, query string, args ...any) (diagnose.Rows, error) {
	rows, err := n.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return driverRows{rows}, nil
}

// driverRows 直接内嵌 driver.Rows：Next/Scan/Err/Close 签名一致。
type driverRows struct{ driver.Rows }

// sqlQuerier 走 database/sql（含 HTTP DSN）。sql.Rows 的 Next/Scan/Err/Close
// 与 diagnose.Rows 签名一致，可直接返回。
type sqlQuerier struct{ db *sql.DB }

func (s *sqlQuerier) Query(ctx context.Context, query string, args ...any) (diagnose.Rows, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return rows, nil
}
