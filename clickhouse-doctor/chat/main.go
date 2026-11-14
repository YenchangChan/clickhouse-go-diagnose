// clickhouse-doctor-chat 是 clickhouse-doctor 的可选 Agent 入口：
// 对话式诊断解释与按需触发检查。判定全部来自核心库，本工具不修复、
// 不生成 SQL；工具面只有两个只读检查工具。
//
// 与轻量 CLI（clickhouse-doctor）的边界：只有本入口 import nova/agent，
// 轻量 CLI 的依赖闭包不包含任何 Agent 框架（有测试断言）。
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/AutumnsGrove/nova/agent"
	"github.com/AutumnsGrove/nova/message"
	"github.com/AutumnsGrove/nova/provider/openaicompat"

	diagnose "github.com/YenchangChan/clickhouse-go-diagnose"
	"github.com/YenchangChan/clickhouse-go-diagnose/clickhouse-doctor/targets"
)

const (
	version     = "0.1.0"
	coreVersion = "v0.2.0"

	defaultMaxTurns   = 12
	defaultLLMTimeout = 120 * time.Second
)

type options struct {
	nodes    targets.FlagList
	shardID  string
	addr     string
	httpAddr string
	user     string
	password string

	baseURL  string
	apiKey   string
	model    string
	maxTurns int

	timeout time.Duration
	nodeTO  time.Duration
	maxRows int
	showVer bool
}

func main() {
	code, err := run(os.Args[1:], os.Stdout, os.Stderr)
	if err != nil {
		fmt.Fprintln(os.Stderr, "clickhouse-doctor-chat:", err)
	}
	os.Exit(code)
}

func run(args []string, stdout, stderr io.Writer) (int, error) {
	opts := options{}
	fs := flag.NewFlagSet("clickhouse-doctor-chat", flag.ContinueOnError)
	fs.StringVar(&opts.addr, "addr", "", "native 协议地址 host:9000（与 --node 二选一）")
	fs.StringVar(&opts.httpAddr, "http", "", "HTTP 协议地址 host:8123（设置后走 database/sql 路径）")
	fs.StringVar(&opts.user, "user", "default", "ClickHouse 用户名")
	fs.StringVar(&opts.password, "password", "", "密码；留空则读环境变量 CLICKHOUSE_DOCTOR_PASSWORD")
	fs.StringVar(&opts.shardID, "shard-id", "shard-1", "默认分片标识")
	fs.Var(&opts.nodes, "node", "目标节点，可重复：--node shard-1@host:9000 或 shard-1@node-别名@host:9000")
	fs.StringVar(&opts.baseURL, "base-url", "", "OpenAI 兼容 API 根地址（如 https://host/v1），必填")
	fs.StringVar(&opts.apiKey, "api-key", "", "API Key；留空则读环境变量 CLICKHOUSE_CHAT_API_KEY")
	fs.StringVar(&opts.model, "model", "", "模型名，必填")
	fs.IntVar(&opts.maxTurns, "max-turns", defaultMaxTurns, "单次提问的最大工具调用轮数")
	fs.DurationVar(&opts.timeout, "timeout", 60*time.Second, "单次检查总预算")
	fs.DurationVar(&opts.nodeTO, "node-timeout", 30*time.Second, "单节点预算（Agent 场景默认放宽，适配慢集群）")
	fs.IntVar(&opts.maxRows, "max-rows-per-node", 0, "单节点保留上限，0 用库默认 1000")
	fs.BoolVar(&opts.showVer, "version", false, "打印版本")
	if err := fs.Parse(args); err != nil {
		return exitUsage, err
	}
	if opts.showVer {
		fmt.Fprintf(stdout, "clickhouse-doctor-chat %s (clickhouse-go-diagnose %s)\n", version, coreVersion)
		return exitOK, nil
	}
	if opts.password == "" {
		opts.password = os.Getenv("CLICKHOUSE_DOCTOR_PASSWORD")
	}
	if opts.apiKey == "" {
		opts.apiKey = os.Getenv("CLICKHOUSE_CHAT_API_KEY")
	}
	if opts.baseURL == "" || opts.model == "" {
		return exitUsage, fmt.Errorf("--base-url 与 --model 必填（API Key 可用环境变量 CLICKHOUSE_CHAT_API_KEY）")
	}

	specs, err := nodeSpecs(opts)
	if err != nil {
		return exitUsage, err
	}
	tgts, closers, err := targets.Open(specs, opts.user, opts.password)
	if err != nil {
		return exitUsage, err
	}
	defer func() {
		for _, c := range closers {
			c()
		}
	}()

	env := checkEnv{targets: tgts, timeout: opts.timeout, nodeTO: opts.nodeTO, maxRows: opts.maxRows}
	provider := openaicompat.New(openaicompat.Config{
		Name:    "openai-compat",
		BaseURL: opts.baseURL,
		APIKey:  opts.apiKey,
	})
	a := agent.New(agent.Config{
		Provider:       provider,
		Model:          opts.model,
		System:         systemPrompt,
		Tools:          newTools(env),
		MaxTurns:       opts.maxTurns,
		RequestTimeout: defaultLLMTimeout,
	})

	fmt.Fprintf(stderr, "clickhouse-doctor-chat %s 目标 %d 个节点，模型 %s\n", version, len(tgts), opts.model)
	fmt.Fprintln(stderr, "注意：以下回复均为 AI 解释；诊断结论以工具返回的结构化报告为准。输入 exit 退出。")

	return repl(context.Background(), a, stdout, stderr)
}

func nodeSpecs(o options) ([]targets.Spec, error) {
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
	return []targets.Spec{{ShardID: o.shardID, NodeID: nodeID, Addr: addr}}, nil
}

const (
	exitOK    = 0
	exitUsage = 1
)

// repl 是交互循环：每行输入触发一次 agent.Run，history 全量累积。
func repl(ctx context.Context, a *agent.Agent, stdout, stderr io.Writer) (int, error) {
	ctx, cancel := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer cancel()

	sc := bufio.NewScanner(os.Stdin)
	var history []message.Message
	for {
		fmt.Fprint(stderr, "> ")
		if !sc.Scan() {
			break
		}
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if line == "exit" || line == "quit" {
			break
		}
		fmt.Fprintln(stderr, "（执行中，可能包含多次工具调用…）")
		res, err := a.Run(ctx, history, message.User(line))
		if err != nil {
			// 中断（Ctrl+C）按正常退出处理；其余错误继续会话，
			// 让用户可以换一种问法——会话状态不受单轮失败影响。
			if ctx.Err() != nil {
				return exitOK, nil
			}
			fmt.Fprintln(stderr, "本轮失败:", err)
			continue
		}
		history = append(history, res.New...)
		fmt.Fprintln(stdout, res.Answer)
		fmt.Fprintf(stderr, "（AI 解释，共 %d 轮工具调用；以上述报告为准）\n", res.Turns)
	}
	return exitOK, nil
}

var _ = diagnose.ListChecks // 编译期锚定核心库版本
