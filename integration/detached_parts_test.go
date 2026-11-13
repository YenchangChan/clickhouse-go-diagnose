package integration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	diagnose "github.com/YenchangChan/clickhouse-go-diagnose"
)

const detachedDB = "__diagnose_it_detached"

// setupDetachedFixture 建单副本复制表（Keeper 路径按运行时刻唯一，规避
// DROP 异步收尾竞态），插入 3 个分区各一个 part，并 DETACH 其中一个
// （用户手工 detach，reason 为空）。返回 detaching 的 part 名。
func setupDetachedFixture(t *testing.T, conn driver.Conn) (db, table, zkBase string) {
	t.Helper()
	ctx := context.Background()
	uniq := time.Now().Unix()
	db = detachedDB
	table = fmt.Sprintf("dp_t_%d", uniq)
	zkBase = fmt.Sprintf("/clickhouse/tables/diagnose-it/dp_it_%d", uniq)

	mustExec(t, conn, ctx, "CREATE DATABASE IF NOT EXISTS "+db)
	mustExec(t, conn, ctx,
		"CREATE TABLE "+db+"."+table+
			" (a Int64, s String) ENGINE = ReplicatedMergeTree('"+zkBase+"', 'it-r1') "+
			"PARTITION BY a % 3 ORDER BY a")
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dcancel()
		// 先把 detached part 挂回来再删库，避免残留 detached 目录。
		var n uint64
		if err := conn.QueryRow(dctx,
			"SELECT count() FROM system.detached_parts WHERE database=? AND table=?", db, table).
			Scan(&n); err == nil && n > 0 {
			_ = conn.Exec(dctx, "ALTER TABLE "+db+"."+table+" ATTACH DETACHED")
		}
		_ = conn.Exec(dctx, "DROP DATABASE IF EXISTS "+db)
	})
	// async_insert=0：该环境的 async insert 偶发 120s 等不到 flush
	//（真机实测，见 docs/compatibility.md），fixture 要求确定性落盘。
	for p := 0; p < 3; p++ {
		mustExec(t, conn, ctx, fmt.Sprintf(
			"INSERT INTO %s.%s SELECT number, 'x' FROM numbers(30) WHERE number %% 3 = %d "+
				"SETTINGS async_insert = 0, wait_for_async_insert = 0", db, table, p))
	}

	// 等 part 落盘后 DETACH 一个。
	deadline := time.Now().Add(15 * time.Second)
	for {
		var active uint64
		if err := conn.QueryRow(ctx,
			"SELECT count() FROM system.parts WHERE database=? AND table=? AND active", db, table).
			Scan(&active); err != nil {
			t.Fatalf("读取 system.parts: %v", err)
		}
		if active >= 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fixture part 15s 内未落盘: active=%d", active)
		}
		time.Sleep(300 * time.Millisecond)
	}

	var name string
	if err := conn.QueryRow(ctx,
		"SELECT name FROM system.parts WHERE database=? AND table=? AND active ORDER BY name LIMIT 1",
		db, table).Scan(&name); err != nil {
		t.Fatalf("选取待 DETACH part: %v", err)
	}
	mustExec(t, conn, ctx, "ALTER TABLE "+db+"."+table+" DETACH PART '"+name+"'")

	// 等服务端把 part 登记进 system.detached_parts。
	deadline = time.Now().Add(10 * time.Second)
	for {
		var n uint64
		if err := conn.QueryRow(ctx,
			"SELECT count() FROM system.detached_parts WHERE database=? AND table=?", db, table).
			Scan(&n); err != nil {
			t.Fatalf("读取 system.detached_parts: %v", err)
		}
		if n >= 1 {
			return db, table, zkBase
		}
		if time.Now().After(deadline) {
			t.Fatalf("DETACH 后 10s 未登记进 system.detached_parts")
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func TestCheckDetachedPartsReal(t *testing.T) {
	cfg := loadConfig(t)
	conn := openNative(t, cfg, cfg.user, cfg.password, clickhouse.Native)
	db, table, _ := setupDetachedFixture(t, conn)

	tgts := []diagnose.Target{{NodeID: "node-1", ShardID: "shard-1", Querier: &nativeQuerier{conn: conn}}}

	// 默认阈值：单条空 reason 的用户 detach 不构成匹配，但行保留为证据。
	report, err := diagnose.CheckDetachedParts(context.Background(), tgts, diagnose.DetachedPartsOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != diagnose.ExecutionComplete || report.Outcome != diagnose.OutcomeNoMatch {
		t.Fatalf("got %s/%s, want complete/no_match", report.Execution, report.Outcome)
	}
	if report.Targets[0].RowsRead < 1 {
		t.Errorf("detached part 应保留为证据: %+v", report.Targets[0])
	}

	// 阈值 1：命中，且证据与服务端事实一致（非同源核对）。
	report, err = diagnose.CheckDetachedParts(context.Background(), tgts,
		diagnose.DetachedPartsOptions{CountThreshold: 1})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != diagnose.ExecutionComplete || report.Outcome != diagnose.OutcomeMatched {
		t.Fatalf("got %s/%s, want complete/matched", report.Execution, report.Outcome)
	}
	if len(report.Matches) != 1 {
		t.Fatalf("应有 1 条匹配，got %d: %+v", len(report.Matches), report.Matches)
	}
	m := report.Matches[0]
	if m.Database != db || m.Table != table {
		t.Errorf("匹配对象错误: %+v", m)
	}
	if len(m.Reasons) != 1 || m.Reasons[0] != diagnose.ReasonDetachedCountExceeded {
		t.Errorf("匹配级理由码错误: %+v", m.Reasons)
	}
	if m.Parts != len(m.Observations) {
		t.Errorf("单节点视角 Parts=%d 应等于观测数 %d", m.Parts, len(m.Observations))
	}
	for _, o := range m.Observations {
		if len(o.Reasons) != 0 {
			t.Errorf("用户 detach 不应有逐 part 理由: %+v", o)
		}
		if o.PartName == "" {
			t.Errorf("part 名不应为空: %+v", o)
		}
	}

	var serverParts uint64
	if err := conn.QueryRow(context.Background(),
		"SELECT count() FROM system.detached_parts WHERE database=? AND table=?", db, table).
		Scan(&serverParts); err != nil {
		t.Fatalf("读取服务端事实: %v", err)
	}
	if len(m.Observations) != int(serverParts) {
		t.Errorf("观测数 %d != 服务端 detached 数 %d", len(m.Observations), serverParts)
	}
}

// TestCheckDetachedPartsHTTP 确认 database/sql（HTTP DSN）路径同样可用。
func TestCheckDetachedPartsHTTP(t *testing.T) {
	cfg := loadConfig(t)
	if cfg.httpAddr == "" {
		t.Skip("DIAGNOSE_IT_HTTP_ADDR 未设置，跳过 HTTP 协议验证")
	}
	conn := openNative(t, cfg, cfg.user, cfg.password, clickhouse.Native)
	db, table, _ := setupDetachedFixture(t, conn)

	dsn := "http://" + cfg.user + ":" + cfg.password + "@" + cfg.httpAddr + "/default"
	sqlDB, err := sql.Open("clickhouse", dsn)
	if err != nil {
		t.Fatalf("open http: %v", err)
	}
	defer sqlDB.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := sqlDB.PingContext(ctx); err != nil {
		t.Fatalf("http ping: %v", err)
	}

	report, err := diagnose.CheckDetachedParts(ctx,
		[]diagnose.Target{{NodeID: "it-http", ShardID: "shard-1", Querier: &sqlQuerier{db: sqlDB}}},
		diagnose.DetachedPartsOptions{CountThreshold: 1})
	if err != nil {
		for _, nr := range report.Targets {
			t.Logf("HTTP 目标状态: %+v", nr)
		}
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != diagnose.ExecutionComplete || report.Outcome != diagnose.OutcomeMatched {
		t.Fatalf("got %s/%s, want complete/matched", report.Execution, report.Outcome)
	}
	if len(report.Matches) != 1 || report.Matches[0].Database != db || report.Matches[0].Table != table {
		t.Fatalf("HTTP 路径匹配错误: %+v", report.Matches)
	}
}
