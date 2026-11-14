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
	"strings"
	"time"

	"github.com/AutumnsGrove/nova/agent"
	"github.com/AutumnsGrove/nova/compaction"
	"github.com/AutumnsGrove/nova/message"
	"github.com/AutumnsGrove/nova/provider/openaicompat"

	diagnose "github.com/YenchangChan/clickhouse-go-diagnose"
	"github.com/YenchangChan/clickhouse-go-diagnose/clickhouse-doctor/internal/buildinfo"
	"github.com/YenchangChan/clickhouse-go-diagnose/clickhouse-doctor/targets"
)

const (
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
		fmt.Fprintf(stdout, "clickhouse-doctor-chat %s (clickhouse-go-diagnose %s)\n", buildinfo.Version(), buildinfo.CoreVersion())
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
	// 输出分离：模型回复（markdown 渲染后）走 stdout，banner/提示符/工具
	// 进度/spinner 走 stderr——管道消费两侧互不干扰。stdout 非 TTY 时
	// 颜色与动画自动降级为纯文本。
	tty := stdoutIsTTY()
	uiTTY = tty
	a := agent.New(agent.Config{
		Provider:       provider,
		Model:          opts.model,
		System:         systemPrompt,
		Tools:          newTools(env),
		MaxTurns:       opts.maxTurns,
		RequestTimeout: defaultLLMTimeout,
		Emit:           makeEmitter(newSpinner(stderr, tty), stderr),
	})

	targetSummary := fmt.Sprintf("%d 个节点", len(tgts))
	if len(specs) <= 3 {
		var addrs []string
		for _, s := range specs {
			addrs = append(addrs, s.Addr)
		}
		targetSummary = strings.Join(addrs, ", ")
	}
	fmt.Fprintln(stderr, banner(buildinfo.Version(), opts.model, targetSummary, tty))

	// 上下文压缩器：上限取端点实际限制（硅基流动 163840），留 24K 余量。
	compactor := &compaction.Compactor{
		Provider: provider,
		Model:    opts.model,
		Settings: compaction.Settings{
			Enabled:          true,
			ContextLimit:     163840,
			Reserve:          24000,
			KeepRecent:       24000,
			SummaryMaxTokens: 2000,
		},
	}

	return repl(a, stdout, stderr, tty, compactor)
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
// 退出方式：exit / quit / Ctrl+D（EOF）/ Ctrl+C（默认信号行为，直接终止）。
// 不拦截信号——之前拦截后 Ctrl+C 只取消了 context，而循环阻塞在
// Scanner 上感知不到，进程像"无法退出"。
//
// 上下文预算：每轮结束后估算 token，逼近端点上限（硅基流动 160K）时
// 用 nova 的 compaction 把较早轮次摘要替换，避免 334K 超限崩溃。
func repl(a *agent.Agent, stdout, stderr io.Writer, tty bool, compactor *compaction.Compactor) (int, error) {
	renderer := newAnswerRenderer(tty)
	prompt := "> "
	if tty {
		prompt = stylePrompt.Render("❯ ")
	}

	sc := bufio.NewScanner(os.Stdin)
	var history []message.Message
	for {
		if tty {
			fmt.Fprint(stderr, prompt)
		}
		if !sc.Scan() {
			// Ctrl+D / EOF：换行收尾，干净退出。
			if tty {
				fmt.Fprintln(stderr)
			}
			break
		}
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if line == "exit" || line == "quit" {
			break
		}
		res, err := a.Run(context.Background(), history, message.User(line))
		if err != nil {
			// 单轮失败不结束会话（Ctrl+C 直接杀进程，不存在 ctx 分支），
			// 用户可以换一种问法继续。
			fmt.Fprintln(stderr, paint(styleFail, "✗ 本轮失败:"), err)
			continue
		}
		history = append(history, res.New...)
		fmt.Fprintln(stdout, renderer.render(res.Answer))
		fmt.Fprintln(stderr, paint(styleNote, fmt.Sprintf("✦ AI 解释 · %d 轮工具调用 · 诊断结论以结构化报告为准", res.Turns)))

		if compaction.ShouldCompact(compactor.Settings, history, nil) {
			cr, err := compactor.Compact(context.Background(), history, "")
			if err != nil {
				fmt.Fprintln(stderr, paint(styleFail, "✗ 上下文压缩失败，长会话可能触发端点 token 上限，建议 exit 后重开"), err)
				continue
			}
			nh := []message.Message{message.User("<summary>\n" + cr.Summary + "\n</summary>")}
			nh = append(nh, history[cr.FirstKept:]...)
			history = nh
			fmt.Fprintln(stderr, paint(styleNote, fmt.Sprintf("✦ 上下文已自动压缩：%d → 约 %d tokens，较早轮次已替换为摘要", cr.TokensBefore, cr.TokensAfter)))
		}
	}
	return exitOK, nil
}

var _ = diagnose.ListChecks // 编译期锚定核心库版本
