// Package buildinfo 从构建信息提取版本号。go install 安装的二进制自动
// 带真实模块版本（如 v0.1.2）；源码构建为 (devel)，回落到 fallback 常量，
// 避免每次发布手工同步版本字符串。
package buildinfo

import (
	"runtime/debug"
)

// fallback 是 BuildInfo 不可用或版本为 (devel)（源码构建）时的显示值。
// coreFallback 与本模块 go.mod require 的核心库版本保持一致。
const (
	fallbackVersion = "0.1.0-dev"
	coreFallback    = "v0.3.0"
)

const coreModule = "github.com/YenchangChan/clickhouse-go-diagnose"

// Version 返回本模块（clickhouse-doctor）的构建版本。
func Version() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return fallbackVersion
	}
	if v := bi.Main.Version; v != "" && v != "(devel)" {
		return v
	}
	return fallbackVersion
}

// CoreVersion 返回依赖的核心库 clickhouse-go-diagnose 版本。
func CoreVersion() string {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return coreFallback
	}
	for _, d := range bi.Deps {
		if d.Path == coreModule {
			if v := d.Version; v != "" && v != "(devel)" {
				return v
			}
			break
		}
	}
	return coreFallback
}
