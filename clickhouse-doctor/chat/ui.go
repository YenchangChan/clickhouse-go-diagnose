package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"charm.land/glamour/v2"
	"charm.land/lipgloss/v2"

	"github.com/AutumnsGrove/nova/proto"
)

// ---- 终端能力检测 ----

// stdoutIsTTY 决定交互增强是否启用：管道消费（测试脚本）时自动降级为
// 纯文本输出，行为与现代化改造前一致。
func stdoutIsTTY() bool {
	fi, err := os.Stdout.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// uiTTY 由 run() 按 stdout 的 TTY 状态设置。lipgloss v2 的自动降色跟随
// TERM 环境变量而非实际输出流，stdout 重定向到文件时仍会注入 ANSI 码，
// 因此进度/提示行统一走 paint 显式控制。
var uiTTY bool

func paint(style lipgloss.Style, s string) string {
	if uiTTY {
		return style.Render(s)
	}
	return s
}

// ---- 样式集（lipgloss 按 termenv 自动降色/无色）----

var (
	styleTool   = lipgloss.NewStyle().Foreground(lipgloss.Color("6")) // 青
	styleOK     = lipgloss.NewStyle().Foreground(lipgloss.Color("2")) // 绿
	styleFail   = lipgloss.NewStyle().Foreground(lipgloss.Color("1")) // 红
	styleDim    = lipgloss.NewStyle().Foreground(lipgloss.Color("8")) // 灰
	stylePrompt = lipgloss.NewStyle().Foreground(lipgloss.Color("6")).Bold(true)
	styleNote   = lipgloss.NewStyle().Foreground(lipgloss.Color("8"))
	styleKey    = lipgloss.NewStyle().Foreground(lipgloss.Color("6")).Bold(true)
	stylePanel  = lipgloss.NewStyle().
			Border(lipgloss.RoundedBorder()).
			BorderForeground(lipgloss.Color("6")).
			Padding(0, 1)
)

// banner 渲染启动面板。非 TTY 输出无边框的紧凑版，避免污染管道。
func banner(version, model, targetSummary string, tty bool) string {
	if !tty {
		return fmt.Sprintf("clickhouse-doctor-chat %s 目标 %s 模型 %s\n退出：exit / Ctrl+D / Ctrl+C。回复均为 AI 解释，诊断结论以结构化报告为准。",
			version, targetSummary, model)
	}
	body := strings.Join([]string{
		styleKey.Render("◆ clickhouse-doctor-chat") + styleDim.Render(" "+version),
		styleKey.Render("目标") + "  " + targetSummary,
		styleKey.Render("模型") + "  " + model,
		styleKey.Render("纪律") + "  AI 解释仅供参考 · 诊断结论以结构化报告为准",
		styleDim.Render("退出  exit / Ctrl+D / Ctrl+C"),
	}, "\n")
	return stylePanel.Render(body)
}

// ---- spinner：工具执行期间的等待动画 ----

var spinnerFrames = []string{"⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"}

// spinner 是引用计数的单实例动画：任一工具执行期间转动，全部结束即停。
// 非 TTY 下 begin/end 直接返回，不产生任何输出。
type spinner struct {
	mu      sync.Mutex
	active  int
	stop    chan struct{}
	stopped chan struct{}
	w       io.Writer
	tty     bool
}

func newSpinner(w io.Writer, tty bool) *spinner { return &spinner{w: w, tty: tty} }

func (s *spinner) begin() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active++
	if s.active == 1 && s.tty {
		s.stop = make(chan struct{})
		s.stopped = make(chan struct{})
		go s.spin(s.stop, s.stopped)
	}
}

func (s *spinner) end() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active > 0 {
		s.active--
	}
	if s.active == 0 && s.stop != nil {
		close(s.stop)
		<-s.stopped // 等清行完成再打印结果行，避免交错
		s.stop, s.stopped = nil, nil
	}
}

func (s *spinner) spin(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	i := 0
	t := time.NewTicker(80 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-stop:
			fmt.Fprint(s.w, "\r\033[K")
			return
		case <-t.C:
			fmt.Fprintf(s.w, "\r\033[K%s 运行中…", spinnerFrames[i%len(spinnerFrames)])
			i++
		}
	}
}

// ---- 事件流 → 终端反馈 ----

// makeEmitter 把 nova 事件流转成图标化进度行。进度一律走 stderr，
// stdout 只保留模型回复（markdown 渲染后的），管道消费两侧互不干扰。
func makeEmitter(sp *spinner, stderr io.Writer) func(proto.Event) {
	return func(ev proto.Event) {
		switch ev.Type {
		case proto.EventToolCall:
			// 工具名与参数在 tool_call 事件（tool_start 只带 CallID）。
			if ev.Call == nil {
				return
			}
			fmt.Fprintf(stderr, "%s %s\n", paint(styleTool, "● "+ev.Call.Name), paint(styleDim, argSummary(ev.Call.Arguments)))
			sp.begin()
		case proto.EventToolEnd:
			sp.end()
			if ev.Result == nil {
				return
			}
			if ev.Result.IsError {
				fmt.Fprintf(stderr, "  %s %s\n", paint(styleFail, "✗ 失败"), paint(styleDim, clipLine(ev.Result.Content, 120)))
			} else {
				fmt.Fprintf(stderr, "  %s %s\n", paint(styleOK, "✓ 完成"), paint(styleDim, fmt.Sprintf("%d 字节", len(ev.Result.Content))))
			}
		case proto.EventError:
			if ev.Error != nil {
				fmt.Fprintf(stderr, "%s %s\n", paint(styleFail, "✗ 事件错误"), paint(styleDim, clipLine(ev.Error.Message, 200)))
			}
		}
	}
}

func argSummary(args json.RawMessage) string {
	if len(args) == 0 {
		return ""
	}
	var m map[string]any
	if err := json.Unmarshal(args, &m); err != nil {
		return ""
	}
	if c, ok := m["check"].(string); ok {
		return "check=" + c
	}
	return ""
}

func clipLine(s string, max int) string {
	r := []rune(strings.TrimSpace(s))
	if len(r) <= max {
		return string(r)
	}
	return string(r[:max]) + "…"
}

// ---- markdown 渲染 ----

// answerRenderer 用 glamour 渲染模型回复。glamour v2 没有 auto style，
// 按 stdout 能力显式选择：TTY 深色主题全彩；管道 / NO_COLOR 降级为
// 无色但保留结构缩进。
type answerRenderer struct {
	r *glamour.TermRenderer
}

func newAnswerRenderer(tty bool) *answerRenderer {
	style := "notty"
	if tty && os.Getenv("NO_COLOR") == "" {
		style = "dark"
	}
	r, err := glamour.NewTermRenderer(glamour.WithStylePath(style), glamour.WithWordWrap(100))
	if err != nil {
		return &answerRenderer{}
	}
	return &answerRenderer{r: r}
}

// render 渲染 markdown；渲染不可用时原样返回（不吞内容）。
func (a *answerRenderer) render(md string) string {
	if a.r == nil || strings.TrimSpace(md) == "" {
		return md
	}
	out, err := a.r.Render(md)
	if err != nil {
		return md
	}
	return strings.TrimRight(out, "\n")
}
