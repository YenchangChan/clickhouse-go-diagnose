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

const backlogDB = "__diagnose_it_qb"

// backlogFixture 建单副本复制表并写入若干行。Keeper 路径按运行时刻唯一：
// DROP TABLE 对副本 znode 的清理是异步收尾，固定路径重跑会撞
// REPLICA_ALREADY_EXISTS 或吃到上一轮残留的 log 条目；路径唯一从根上
// 规避（留下的空 znode 目录无害）。
func backlogFixture(t *testing.T, conn driver.Conn) (db, table string) {
	t.Helper()
	ctx := context.Background()
	uniq := time.Now().Unix()
	db = backlogDB
	table = fmt.Sprintf("backlog_it_%d", uniq)
	zkBase := fmt.Sprintf("/clickhouse/tables/diagnose-it/qb_it_%d", uniq)
	replicaName := fmt.Sprintf("it-r1-%d", uniq)

	mustExec(t, conn, ctx, "CREATE DATABASE IF NOT EXISTS "+db)
	mustExec(t, conn, ctx,
		"CREATE TABLE "+db+"."+table+
			" (a Int64) ENGINE = ReplicatedMergeTree('"+zkBase+"', '"+replicaName+"') ORDER BY a")
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dcancel()
		_ = conn.Exec(dctx, "DROP TABLE IF EXISTS "+db+"."+table)
	})
	mustExec(t, conn, ctx, "INSERT INTO "+db+"."+table+" SELECT number FROM numbers(5)")
	return db, table
}

// waitForQueueDrain 等待该表在当前节点的复制队列清空。
func waitForQueueDrain(t *testing.T, conn driver.Conn, db, table string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		var pending uint64
		if err := conn.QueryRow(ctx0(),
			"SELECT count() FROM system.replication_queue WHERE database=? AND table=?", db, table).
			Scan(&pending); err != nil {
			t.Fatalf("读取队列深度: %v", err)
		}
		if pending == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("队列 15s 内未清空: %d 条", pending)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func ctx0() context.Context { return context.Background() }

// TestQueueBacklogColumnsOnRealServer 验证固定查询依赖的列在真实服务器上存在。
func TestQueueBacklogColumnsOnRealServer(t *testing.T) {
	cfg := loadConfig(t)
	conn := openNative(t, cfg, cfg.user, cfg.password, clickhouse.Native)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := conn.Query(ctx,
		"SELECT name FROM system.columns WHERE database='system' AND table='replication_queue'")
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
	for _, col := range []string{"database", "table", "create_time", "num_tries", "last_exception"} {
		if !names[col] {
			t.Errorf("system.replication_queue 缺少列 %q", col)
		}
	}
}

// TestCheckQueueBacklogHealthy 健康场景：单副本复制表队列清空后，
// 默认阈值下不应被报出。
func TestCheckQueueBacklogHealthy(t *testing.T) {
	cfg := loadConfig(t)
	conn := openNative(t, cfg, cfg.user, cfg.password, clickhouse.Native)
	ctx := context.Background()

	db, table := backlogFixture(t, conn)
	waitForQueueDrain(t, conn, db, table)

	targets := []diagnose.Target{
		{NodeID: "backlog-healthy", ShardID: "shard-1", Querier: &nativeQuerier{conn: conn}},
	}
	report, err := diagnose.CheckReplicationQueueBacklog(ctx, targets, diagnose.QueueBacklogOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != diagnose.ExecutionComplete {
		t.Fatalf("execution = %s, want complete", report.Execution)
	}
	// 自建表不允许出现在匹配中。全局 Outcome 可能因外部注入的故障为
	// matched，那不是本检查的失败。
	for _, m := range report.Matches {
		if m.Database == db && m.Table == table {
			t.Errorf("健康表被误报: %+v", m)
		}
	}
	if report.Outcome == diagnose.OutcomeNoMatch && len(report.NextSteps) != 0 {
		t.Errorf("无匹配不应有建议: %+v", report.NextSteps)
	}
	t.Logf("outcome=%s matches=%d", report.Outcome, len(report.Matches))
}

// TestCheckQueueBacklogFault 故障场景：检测到队列中存在带异常文本的条目
// （由 scripts/inject_no_replica_has_part.py 注入）时，收紧阈值复跑：
//   - 故障表必须被报出且深度理由命中；
//   - 报出的匹配集合必须等于服务端聚合事实，不多不少。
func TestCheckQueueBacklogFault(t *testing.T) {
	cfg := loadConfig(t)
	conn := openNative(t, cfg, cfg.user, cfg.password, clickhouse.Native)
	ctx := context.Background()

	var faultEntries uint64
	if err := conn.QueryRow(ctx,
		"SELECT count() FROM system.replication_queue WHERE last_exception != ''").Scan(&faultEntries); err != nil {
		t.Fatalf("预检查 queue 失败: %v", err)
	}
	if faultEntries == 0 {
		t.Skip("当前队列无异常条目；先运行 scripts/inject_no_replica_has_part.py 注入故障")
	}

	// 服务端事实：按 (库,表) 聚合的深度，作为期望匹配集合。
	rows, err := conn.Query(ctx,
		"SELECT database, table, count() FROM system.replication_queue GROUP BY database, table")
	if err != nil {
		t.Fatalf("读取服务端聚合事实: %v", err)
	}
	defer rows.Close()
	want := map[string]int{}
	for rows.Next() {
		var db, table string
		var depth uint64
		if err := rows.Scan(&db, &table, &depth); err != nil {
			t.Fatalf("scan: %v", err)
		}
		want[db+"."+table] = int(depth)
	}
	if rows.Err() != nil {
		t.Fatalf("rows err: %v", rows.Err())
	}

	targets := []diagnose.Target{
		{NodeID: "backlog-fault", ShardID: "shard-1", Querier: &nativeQuerier{conn: conn}},
	}
	report, err := diagnose.CheckReplicationQueueBacklog(ctx, targets, diagnose.QueueBacklogOptions{
		DepthThreshold:      1, // >= 语义：单条目即命中
		AgeThresholdSeconds: 1,
		RetriesThreshold:    1000000, // 关闭重试规则（注入初期重试次数可能为 0）
	})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != diagnose.ExecutionComplete || report.Outcome != diagnose.OutcomeMatched {
		t.Fatalf("got %s/%s, want complete/matched", report.Execution, report.Outcome)
	}

	got := map[string]bool{}
	for _, m := range report.Matches {
		got[m.Database+"."+m.Table] = true
		o := m.Observations[0]
		if o.Depth < 1 {
			t.Errorf("深度错误: %+v", o)
		}
		if !hasReasonCode(o.Reasons, diagnose.ReasonQueueDepth) {
			t.Errorf("深度规则未命中: %+v", o.Reasons)
		}
	}
	for k := range want {
		if !got[k] {
			t.Errorf("漏报积压表 %s（服务端深度 %d）", k, want[k])
		}
	}
	if len(got) != len(want) {
		t.Errorf("匹配数 = %d, want %d（不应多报）", len(got), len(want))
	}
}

// TestCheckQueueBacklogHTTP 确认 database/sql（HTTP DSN）路径同样可用。
func TestCheckQueueBacklogHTTP(t *testing.T) {
	cfg := loadConfig(t)
	if cfg.httpAddr == "" {
		t.Skip("DIAGNOSE_IT_HTTP_ADDR 未设置，跳过 HTTP 协议验证")
	}
	dsn := "http://" + cfg.user + ":" + cfg.password + "@" + cfg.httpAddr + "/default"
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		t.Fatalf("open http: %v", err)
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("http ping: %v", err)
	}

	report, err := diagnose.CheckReplicationQueueBacklog(ctx,
		[]diagnose.Target{{NodeID: "it-http", ShardID: "shard-1", Querier: &sqlQuerier{db: db}}},
		diagnose.QueueBacklogOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != diagnose.ExecutionComplete {
		t.Fatalf("execution = %s, want complete", report.Execution)
	}
	t.Logf("HTTP 路径 outcome=%s matches=%d", report.Outcome, len(report.Matches))
}

func hasReasonCode(reasons []string, code string) bool {
	for _, r := range reasons {
		if r == code {
			return true
		}
	}
	return false
}
