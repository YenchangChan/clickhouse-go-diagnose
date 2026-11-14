// Package targets 组装诊断目标：解析 --node 参数与建连适配。
// 轻量 CLI 与 chat 入口共用；本包不含任何判定逻辑。
package targets

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	diagnose "github.com/YenchangChan/clickhouse-go-diagnose"
)

// FlagList 实现 flag.Value，支持重复 --node 传参。
type FlagList []string

func (n *FlagList) String() string { return strings.Join(*n, ",") }

func (n *FlagList) Set(s string) error {
	if s == "" {
		return fmt.Errorf("--node 不能为空")
	}
	*n = append(*n, s)
	return nil
}

// Spec 是一个目标的完整身份：分片、节点标识与连接地址。
type Spec struct {
	ShardID string
	NodeID  string
	Addr    string // 原样保留；http:// 前缀表示 HTTP/database/sql 路径
}

func IsHTTP(addr string) bool { return strings.HasPrefix(addr, "http://") }

// FromNodes 解析重复 --node 参数：shard-id@addr 或 shard-id@node-id@addr
// （同一主机多实例必须显式区分 node-id）。NodeID 默认取地址本身；
// 重复 NodeID 在此拒绝，而不是让库报 ErrInvalidInput。
func FromNodes(nodes []string, defaultShard string) ([]Spec, error) {
	var specs []Spec
	for _, raw := range nodes {
		shard, nodeID := defaultShard, ""
		addr := raw
		parts := strings.SplitN(raw, "@", 3)
		switch len(parts) {
		case 2: // shard-id@addr
			shard, addr = strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		case 3: // shard-id@node-id@addr
			shard, nodeID, addr = strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1]), strings.TrimSpace(parts[2])
		}
		if shard == "" || addr == "" || (len(parts) == 3 && nodeID == "") || strings.Contains(addr, "@") {
			return nil, fmt.Errorf("--node %q 格式错误：应为 shard-id@addr 或 shard-id@node-id@addr", raw)
		}
		if nodeID == "" {
			nodeID = addr
		}
		specs = append(specs, Spec{ShardID: shard, NodeID: nodeID, Addr: addr})
	}

	seen := map[string]string{}
	for _, s := range specs {
		if prev, dup := seen[s.NodeID]; dup {
			return nil, fmt.Errorf("节点标识 %q 重复（出现在 %s 与 %s）；同一主机的多个实例必须用不同标识", s.NodeID, prev, s.Addr)
		}
		seen[s.NodeID] = s.Addr
	}
	return specs, nil
}

// Open 逐个建连并组装 diagnose.Target。任一节点连接失败立即返回错误
// （已打开的连接由调用方关闭）；建连与 Ping 不计入检查预算，单节点上限 5s。
func Open(specs []Spec, user, password string) ([]diagnose.Target, []func(), error) {
	var (
		targets []diagnose.Target
		closers []func()
	)
	for _, s := range specs {
		q, closeFn, err := openQuerier(s.Addr, user, password)
		if err != nil {
			return nil, closers, fmt.Errorf("连接 %s (%s): %w", s.NodeID, s.ShardID, err)
		}
		closers = append(closers, closeFn)
		targets = append(targets, diagnose.Target{NodeID: s.NodeID, ShardID: s.ShardID, Querier: q})
	}
	return targets, closers, nil
}

// openQuerier 按地址建连。http:// 前缀走 database/sql（HTTP DSN），否则 native。
func openQuerier(addr, user, password string) (diagnose.Querier, func(), error) {
	if IsHTTP(addr) {
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
