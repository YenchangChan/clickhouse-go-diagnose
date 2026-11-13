package integration

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	diagnose "github.com/YenchangChan/clickhouse-go-diagnose"
)

const replicaDB = "__diagnose_it_rs"

// 幽灵副本注入载体（scripts/inject_ghost_replica.py 建的表）。
const ghostTable = "rep_t"

// 测试自建 fixture（脚本未注入时使用），与脚本表名、Keeper 路径互不干扰。
const ownTable = "rep_it"
const ownZKBase = "/clickhouse/tables/diagnose-it/rs_it"

// fixtureTable 确定本次运行的被检表：
//   - 脚本已注入（ghostTable 存在）→ 直接使用它，不碰脚本资产；
//   - 脚本未注入 → 自建单副本表 ownTable（副本名按运行时刻唯一，
//     避免历史孤儿 znode 引发的 REPLICA_ALREADY_EXISTS），插入一行，
//     测试结束只删自己的表，不删库（可能装有脚本资产）。
//
// 返回 (库名, 表名, 是否预期 active 缺口)。
func fixtureTable(t *testing.T, conn driver.Conn) (db, table string, expectDeficit bool) {
	t.Helper()
	ctx := context.Background()

	var exists uint8 // EXISTS TABLE 返回 UInt8
	if err := conn.QueryRow(ctx,
		"EXISTS TABLE "+replicaDB+"."+ghostTable).Scan(&exists); err != nil {
		t.Fatalf("EXISTS TABLE: %v", err)
	}
	if exists == 1 {
		t.Logf("检测到幽灵副本注入载体 %s.%s，直接使用", replicaDB, ghostTable)
		return replicaDB, ghostTable, true
	}

	replicaName := fmt.Sprintf("it-r1-%d", time.Now().Unix())
	mustExec(t, conn, ctx, "CREATE DATABASE IF NOT EXISTS "+replicaDB)
	mustExec(t, conn, ctx, "DROP TABLE IF EXISTS "+replicaDB+"."+ownTable)
	mustExec(t, conn, ctx,
		"CREATE TABLE "+replicaDB+"."+ownTable+
			" (a Int64) ENGINE = ReplicatedMergeTree('"+ownZKBase+"', '"+replicaName+"') ORDER BY a")
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dcancel()
		_ = conn.Exec(dctx, "DROP TABLE IF EXISTS "+replicaDB+"."+ownTable)
	})
	mustExec(t, conn, ctx, "INSERT INTO "+replicaDB+"."+ownTable+" SELECT toInt64(rand64())")
	return replicaDB, ownTable, false
}

// TestReplicaColumnsOnRealServer 验证 system.replicas 的必需列在真实服务器上存在。
func TestReplicaColumnsOnRealServer(t *testing.T) {
	cfg := loadConfig(t)
	conn := openNative(t, cfg, cfg.user, cfg.password, clickhouse.Native)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := conn.Query(ctx,
		"SELECT name FROM system.columns WHERE database='system' AND table='replicas'")
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
	for _, col := range []string{
		"database", "table", "is_readonly", "is_session_expired",
		"absolute_delay", "total_replicas", "active_replicas", "lost_part_count",
	} {
		if !names[col] {
			t.Errorf("system.replicas 缺少列 %q", col)
		}
	}
}

// TestCheckReplicaStatusReal 真实场景：
//   - 健康的单副本 fixture（total=active=1）不应被报出；
//   - 幽灵副本注入后（total > active）应报出且理由码为 active 缺口；
//   - 集群里真实存在的病变表（如残留的 readonly 表）应被如实报出。
//
// 幽灵副本注入是外部动作（scripts/inject_ghost_replica.py）：未注入时按
// 健康分支断言，已注入时按故障分支断言，测试不自己造 znode。
func TestCheckReplicaStatusReal(t *testing.T) {
	cfg := loadConfig(t)
	conn := openNative(t, cfg, cfg.user, cfg.password, clickhouse.Native)
	ctx := context.Background()
	db, table, expectDeficit := fixtureTable(t, conn)

	// 等本副本激活（system.replicas 出现该表且 active >= 1）。
	deadline := time.Now().Add(15 * time.Second)
	for {
		var total, active uint32 // system.replicas 计数列是 UInt32
		if err := conn.QueryRow(ctx,
			"SELECT total_replicas, active_replicas FROM system.replicas "+
				"WHERE database=? AND table=?", db, table).
			Scan(&total, &active); err != nil {
			t.Fatalf("读取 fixture 副本状态: %v", err)
		}
		if active >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fixture 副本 15s 内未激活: total=%d active=%d", total, active)
		}
		time.Sleep(500 * time.Millisecond)
	}

	// 服务端事实：fixture 是否带 active 缺口；集群是否有真实 readonly 表。
	if err := conn.QueryRow(ctx,
		"SELECT total_replicas > active_replicas FROM system.replicas "+
			"WHERE database=? AND table=?", db, table).Scan(&expectDeficit); err != nil {
		t.Fatalf("预检查幽灵副本: %v", err)
	}
	var sickLeftover uint64
	if err := conn.QueryRow(ctx,
		"SELECT count() FROM system.replicas WHERE is_readonly = 1").Scan(&sickLeftover); err != nil {
		t.Fatalf("预检查 readonly 表: %v", err)
	}
	if sickLeftover == 0 {
		t.Log("集群当前没有 readonly 残留表，跳过真实 readonly 断言")
	}

	report, err := diagnose.CheckReplicaStatus(ctx,
		[]diagnose.Target{{NodeID: "it-node", ShardID: "shard-1", Querier: &nativeQuerier{conn: conn}}},
		diagnose.ReplicaStatusOptions{Timeout: 15 * time.Second, NodeTimeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != diagnose.ExecutionComplete {
		t.Fatalf("execution = %s, want complete", report.Execution)
	}
	if report.CheckID != "replica_status" || report.RuleVersion != "1" {
		t.Errorf("检查身份错误: %s/%s", report.CheckID, report.RuleVersion)
	}

	// 找 fixture 表的匹配（若有）。
	var fixtureMatch *diagnose.ReplicaMatch
	for i := range report.Matches {
		m := &report.Matches[i]
		if m.Database == db && m.Table == table {
			if fixtureMatch != nil {
				t.Fatalf("fixture 表出现多条匹配: %+v", report.Matches)
			}
			fixtureMatch = m
		}
	}

	if expectDeficit {
		t.Logf("故障分支：fixture %s.%s 存在 active 缺口（total>active）", db, table)
		if fixtureMatch == nil {
			t.Fatalf("active 缺口已注入但 fixture 表未匹配: %+v", report.Matches)
		}
		if report.Outcome != diagnose.OutcomeMatched {
			t.Errorf("outcome = %s, want matched", report.Outcome)
		}
		o := fixtureMatch.Observations[0]
		if o.TotalReplicas <= o.ActiveReplicas {
			t.Errorf("缺口计数错误: %+v", o)
		}
		if !slices.Contains(o.Reasons, diagnose.ReasonActiveReplicaDeficit) {
			t.Errorf("缺少 active 缺口理由码: %+v", o.Reasons)
		}
		if !hasCode(report.NextSteps, diagnose.AdviceLocateInactiveReplica) {
			t.Errorf("缺口应给定位建议: %+v", report.NextSteps)
		}
	} else {
		t.Logf("健康分支：fixture %s.%s 无缺口，期望不被报出", db, table)
		if fixtureMatch != nil {
			t.Errorf("健康的 fixture 表不应匹配: %+v", fixtureMatch)
		}
	}

	// 真实病变表：集群里确实 readonly 的表必须被报出且理由码正确。
	for _, m := range report.Matches {
		if m.Database == db && m.Table == table {
			continue // fixture 已单独断言
		}
		o := m.Observations[0]
		if o.IsReadonly && !slices.Contains(o.Reasons, diagnose.ReasonReplicaReadonly) {
			t.Errorf("readonly 表 %s.%s 缺少 readonly 理由码: %+v", m.Database, m.Table, o.Reasons)
		}
		if o.IsReadonly {
			t.Logf("真实 readonly 残留表被如实报出: %s.%s total=%d active=%d delay=%ds",
				m.Database, m.Table, o.TotalReplicas, o.ActiveReplicas, o.AbsoluteDelay)
		}
	}
	if sickLeftover > 0 {
		found := false
		for _, m := range report.Matches {
			if m.Observations[0].IsReadonly {
				found = true
			}
		}
		if !found {
			t.Errorf("集群存在 %d 张 readonly 表但检查未报出", sickLeftover)
		}
	}
}

// TestCheckReplicaStatusHTTP 确认 database/sql（HTTP DSN）路径同样可用。
func TestCheckReplicaStatusHTTP(t *testing.T) {
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

	report, err := diagnose.CheckReplicaStatus(context.Background(),
		[]diagnose.Target{{NodeID: "it-http", ShardID: "shard-1", Querier: &sqlQuerier{db: db}}},
		diagnose.ReplicaStatusOptions{Timeout: 15 * time.Second, NodeTimeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != diagnose.ExecutionComplete {
		t.Errorf("execution = %s, want complete", report.Execution)
	}
	// 只验证协议通道与读取完整性；匹配与否取决于集群当前状态（
	// 例如残留的 readonly 表），与服务端事实对账。
	for _, m := range report.Matches {
		if len(m.Observations) == 0 || len(m.Observations[0].Reasons) == 0 {
			t.Errorf("匹配缺少观测或理由码: %+v", m)
		}
	}
}
