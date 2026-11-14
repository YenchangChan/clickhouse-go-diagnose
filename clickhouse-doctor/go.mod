module github.com/YenchangChan/clickhouse-go-diagnose/clickhouse-doctor

go 1.26.7

require (
	charm.land/glamour/v2 v2.0.1
	charm.land/lipgloss/v2 v2.1.0
	github.com/AutumnsGrove/nova v0.4.1
	github.com/ClickHouse/clickhouse-go/v2 v2.15.0
	github.com/YenchangChan/clickhouse-go-diagnose v0.3.0
)

require (
	github.com/ClickHouse/ch-go v0.58.2 // indirect
	github.com/alecthomas/chroma/v2 v2.27.0 // indirect
	github.com/andybalholm/brotli v1.0.6 // indirect
	github.com/aymerick/douceur v0.2.0 // indirect
	github.com/charmbracelet/colorprofile v0.4.3 // indirect
	github.com/charmbracelet/ultraviolet v0.0.0-20261008173134-6b8d4baf91b4 // indirect
	github.com/charmbracelet/x/ansi v0.11.9 // indirect
	github.com/charmbracelet/x/exp/slice v0.0.0-20250327172914-2fdc97757edf // indirect
	github.com/charmbracelet/x/term v0.2.2 // indirect
	github.com/charmbracelet/x/termios v0.1.1 // indirect
	github.com/charmbracelet/x/windows v0.2.2 // indirect
	github.com/clipperhouse/displaywidth v0.11.0 // indirect
	github.com/clipperhouse/uax29/v2 v2.7.0 // indirect
	github.com/dlclark/regexp2/v2 v2.2.1 // indirect
	github.com/go-faster/city v1.0.1 // indirect
	github.com/go-faster/errors v0.6.1 // indirect
	github.com/google/uuid v1.3.1 // indirect
	github.com/gorilla/css v1.0.1 // indirect
	github.com/klauspost/compress v1.16.7 // indirect
	github.com/lucasb-eyer/go-colorful v1.4.1 // indirect
	github.com/mattn/go-runewidth v0.0.30 // indirect
	github.com/microcosm-cc/bluemonday v1.0.27 // indirect
	github.com/muesli/cancelreader v0.2.2 // indirect
	github.com/paulmach/orb v0.10.0 // indirect
	github.com/pierrec/lz4/v4 v4.1.18 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/segmentio/asm v1.2.0 // indirect
	github.com/shopspring/decimal v1.3.1 // indirect
	github.com/xo/terminfo v1.0.0 // indirect
	github.com/yuin/goldmark v1.7.8 // indirect
	github.com/yuin/goldmark-emoji v1.0.5 // indirect
	go.opentelemetry.io/otel v1.19.0 // indirect
	go.opentelemetry.io/otel/trace v1.19.0 // indirect
	golang.org/x/net v0.61.0 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
	golang.org/x/text v0.43.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// 本地开发时核心库由仓库根的 go.work（use .）解析到工作区；GOWORK=off
// 单独构建使用 require 的核心库发布版本。不得使用 replace 指令：go install
// module@version 禁止目标模块 go.mod 含 replace。
// 核心库保持只依赖标准库，clickhouse-go 只出现在本模块。
