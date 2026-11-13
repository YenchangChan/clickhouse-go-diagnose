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

const partsDB = "__diagnose_it_parts"
const partsTable = "frag"

// setupPartsFixture 建非复制表并制造碎片化 part：
// 停掉该表的合并，逐行 INSERT 产生 n 个独立 part。
func setupPartsFixture(t *testing.T, conn driver.Conn, n int) {
	t.Helper()
	ctx := context.Background()
	mustExec(t, conn, ctx, "CREATE DATABASE IF NOT EXISTS "+partsDB)
	mustExec(t, conn, ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", partsDB, partsTable))
	mustExec(t, conn, ctx, fmt.Sprintf(
		"CREATE TABLE %s.%s (a UInt32) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 8192",
		partsDB, partsTable))
	// 停合并保证逐条 INSERT 各自成 part。
	mustExec(t, conn, ctx, fmt.Sprintf("SYSTEM STOP MERGES %s.%s", partsDB, partsTable))
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dcancel()
		_ = conn.Exec(dctx, "DROP DATABASE IF EXISTS "+partsDB)
	})
	for i := 0; i < n; i++ {
		mustExec(t, conn, ctx, fmt.Sprintf("INSERT INTO %s.%s VALUES (%d)", partsDB, partsTable, i))
	}
}

// TestPartsColumnsOnRealServer 验证 system.parts 的必需列在真实服务器上存在。
func TestPartsColumnsOnRealServer(t *testing.T) {
	cfg := loadConfig(t)
	conn := openNative(t, cfg, cfg.user, cfg.password, clickhouse.Native)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := conn.Query(ctx,
		"SELECT name FROM system.columns WHERE database='system' AND table='parts'")
	if err != nil {
		t.Fatalf("query columns: %v", err)
	}
	defer rows.Close()
	names := map[string]bool{}
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan column name: %v", err)
		}
		names[name] = true
	}
	if rows.Err() != nil {
		t.Fatalf("describe rows: %v", rows.Err())
	}
	for _, col := range []string{"database", "table", "partition", "bytes_on_disk", "active"} {
		if !names[col] {
			t.Errorf("system.parts 缺少列 %q", col)
		}
	}
}

// TestCheckTooManyPartsReal 真实场景：制造超过阈值的碎片化分区，
// 验证库返回 matched 与正确证据、建议齐全；高阈值场景返回 complete/no_match。
func TestCheckTooManyPartsReal(t *testing.T) {
	cfg := loadConfig(t)
	conn := openNative(t, cfg, cfg.user, cfg.password, clickhouse.Native)

	const fixtureParts = 12
	const threshold = 5
	setupPartsFixture(t, conn, fixtureParts)

	run := func(threshold int) diagnose.TooManyPartsReport {
		t.Helper()
		report, err := diagnose.CheckTooManyParts(context.Background(),
			[]diagnose.Target{{NodeID: "it-node", ShardID: "shard-1", Querier: &nativeQuerier{conn: conn}}},
			diagnose.TooManyPartsOptions{PartsThreshold: threshold, Timeout: 15 * time.Second, NodeTimeout: 10 * time.Second})
		if err != nil {
			t.Fatalf("err = %v, want nil", err)
		}
		return report
	}

	report := run(threshold)
	if report.Execution != diagnose.ExecutionComplete || report.Outcome != diagnose.OutcomeMatched {
		t.Fatalf("got %s/%s, want complete/matched", report.Execution, report.Outcome)
	}
	var match *diagnose.PartsMatch
	for i := range report.Matches {
		if report.Matches[i].Database == partsDB {
			if match != nil {
				t.Fatalf("fixture 表出现多条匹配: %+v", report.Matches)
			}
			match = &report.Matches[i]
		}
	}
	if match == nil {
		t.Fatalf("fixture 分区未匹配: %+v", report.Matches)
	}
	if match.Table != partsTable || match.Partition == "" {
		t.Errorf("匹配对象错误: %+v", match)
	}
	if match.Parts < fixtureParts {
		t.Errorf("parts = %d, want >= %d（合并已开始或插入未产生独立 part）", match.Parts, fixtureParts)
	}
	if len(match.Observations) != 1 || match.Observations[0].NodeID != "it-node" {
		t.Errorf("来源观测错误: %+v", match.Observations)
	}
	if match.Observations[0].DiskBytes == "" {
		t.Errorf("DiskBytes 未采集: %+v", match.Observations[0])
	}
	for _, code := range []string{"inspect_merge_throughput", "review_insert_batch_size", "review_partition_key"} {
		found := false
		for _, s := range report.NextSteps {
			if s.Code == code {
				found = true
			}
		}
		if !found {
			t.Errorf("缺少建议 %q: %+v", code, report.NextSteps)
		}
	}

	// 低于阈值：同一集群用极高阈值应为 complete/no_match。
	high := run(1 << 30)
	if high.Execution != diagnose.ExecutionComplete || high.Outcome != diagnose.OutcomeNoMatch {
		t.Errorf("got %s/%s, want complete/no_match", high.Execution, high.Outcome)
	}
	if len(high.Matches) != 0 {
		t.Errorf("不应有匹配: %+v", high.Matches)
	}
}

// TestCheckTooManyPartsHTTP 确认 database/sql（HTTP DSN）路径同样可用。
func TestCheckTooManyPartsHTTP(t *testing.T) {
	cfg := loadConfig(t)
	if cfg.httpAddr == "" {
		t.Skip("DIAGNOSE_IT_HTTP_ADDR 未设置，跳过 HTTP 协议验证")
	}
	dsn := "http://" + cfg.user + ":" + cfg.password + "@" + cfg.httpAddr + "/default"
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("http ping: %v", err)
	}

	report, err := diagnose.CheckTooManyParts(context.Background(),
		[]diagnose.Target{{NodeID: "it-http", ShardID: "shard-1", Querier: &sqlQuerier{db: db}}},
		diagnose.TooManyPartsOptions{Timeout: 15 * time.Second, NodeTimeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != diagnose.ExecutionComplete || report.Outcome != diagnose.OutcomeNoMatch {
		t.Errorf("got %s/%s, want complete/no_match", report.Execution, report.Outcome)
	}
}
