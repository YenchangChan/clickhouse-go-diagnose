// Package integration 是独立测试模块：验证 clickhouse-go-diagnose 核心库
// 在真实 ClickHouse 服务器上的行为（implementation-plan M3）。
//
// 默认不访问外部集群：所有真实服务器测试都需要显式设置环境变量，
// 未设置时自动跳过。当前验证环境由调用方提供（本地隔离集群）。
//
//	DIAGNOSE_IT_ADDR=192.168.56.101:9000   native 协议地址
//	DIAGNOSE_IT_HTTP_ADDR=192.168.56.101:8123   HTTP 协议地址（可选）
//	DIAGNOSE_IT_USER=default                用户名（可选，默认 default）
//	DIAGNOSE_IT_PASSWORD=123456             密码（可选）
package integration

import (
	"context"
	"database/sql"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	diagnose "example.org/clickhouse-go-diagnose"
)

type config struct {
	addr     string // native
	httpAddr string
	user     string
	password string
}

func loadConfig(t *testing.T) config {
	t.Helper()
	addr := os.Getenv("DIAGNOSE_IT_ADDR")
	if addr == "" {
		t.Skip("DIAGNOSE_IT_ADDR 未设置，跳过真实服务器测试（默认不访问外部集群）")
	}
	return config{
		addr:     addr,
		httpAddr: os.Getenv("DIAGNOSE_IT_HTTP_ADDR"),
		user:     envOr("DIAGNOSE_IT_USER", "default"),
		password: envOr("DIAGNOSE_IT_PASSWORD", "123456"),
	}
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
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

func openNative(t *testing.T, cfg config, user, password string, protocol clickhouse.Protocol) driver.Conn {
	t.Helper()
	opt := &clickhouse.Options{
		Addr:        []string{cfg.addr},
		Auth:        clickhouse.Auth{Database: "default", Username: user, Password: password},
		DialTimeout: 5 * time.Second,
		Protocol:    protocol,
	}
	if protocol == clickhouse.HTTP && cfg.httpAddr != "" {
		opt.Addr = []string{cfg.httpAddr}
	}
	conn, err := clickhouse.Open(opt)
	if err != nil {
		t.Fatalf("clickhouse.Open: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := conn.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	return conn
}

// rewriteQuerier 把固定查询的目标表改写到隔离的仿表，用于在真实服务器上
// 验证库的采集机制（截断、NULL 处理、取消），不污染 system 表。
type rewriteQuerier struct {
	inner diagnose.Querier
	from  string
	to    string
}

func (r *rewriteQuerier) Query(ctx context.Context, query string, args ...any) (diagnose.Rows, error) {
	return r.inner.Query(ctx, strings.ReplaceAll(query, r.from, r.to), args...)
}
