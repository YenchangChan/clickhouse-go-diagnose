package integration

import (
	"context"
	"database/sql"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	diagnose "github.com/YenchangChan/clickhouse-go-diagnose"
)

const mutDB = "__diagnose_it_mut"
const mutTable = "mut_t"

// setupMutationFixture 建非复制表并插入数据。
// 必然失败的 mutation 用 intDiv 除零构造：`a / 0` 是浮点除法得到 inf
// 不会失败（真机实测），整数除法 intDiv 才抛 ILLEGAL_DIVISION。
// 另注意 ORDER BY 键列不允许 UPDATE（CANNOT_UPDATE_COLUMN，提交期报错），
// 所以命令必须作用于非键列。
func setupMutationFixture(t *testing.T, conn driver.Conn) {
	t.Helper()
	ctx := context.Background()
	mustExec(t, conn, ctx, "CREATE DATABASE IF NOT EXISTS "+mutDB)
	mustExec(t, conn, ctx, "DROP TABLE IF EXISTS "+mutDB+"."+mutTable)
	mustExec(t, conn, ctx,
		"CREATE TABLE "+mutDB+"."+mutTable+" (a Int64, b String) ENGINE = MergeTree ORDER BY a")
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dcancel()
		_ = conn.Exec(dctx, "DROP DATABASE IF EXISTS "+mutDB)
	})
	mustExec(t, conn, ctx,
		"INSERT INTO "+mutDB+"."+mutTable+" SELECT number, toString(number) FROM numbers(1000)")
}

// TestMutationsColumnsOnRealServer 验证 system.mutations 的必需列在真实服务器上存在。
func TestMutationsColumnsOnRealServer(t *testing.T) {
	cfg := loadConfig(t)
	conn := openNative(t, cfg, cfg.user, cfg.password, clickhouse.Native)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := conn.Query(ctx,
		"SELECT name FROM system.columns WHERE database='system' AND table='mutations'")
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
		"database", "table", "mutation_id", "command",
		"create_time", "is_done", "latest_fail_reason",
	} {
		if !names[col] {
			t.Errorf("system.mutations 缺少列 %q", col)
		}
	}
}

// TestCheckMutationsStuckReal 真实场景：
//  1. 注入必然失败的 mutation（intDiv 除零，作用于非键列）→ 等待
//     latest_fail_reason 出现 → 检查报出 mutation_failing；
//  2. 再注入一个正常命令的 mutation（async，未完成窗口内年龄极小）→
//     默认阈值下不应被报出（年轻且无失败文本的条目只读不报）；
//  3. --cleanup（DROP DATABASE）后所有 mutation 消失，检查回到
//     complete/no_match。
func TestCheckMutationsStuckReal(t *testing.T) {
	cfg := loadConfig(t)
	conn := openNative(t, cfg, cfg.user, cfg.password, clickhouse.Native)
	ctx := context.Background()
	setupMutationFixture(t, conn)

	// 1. 必然失败的 mutation。
	mustExec(t, conn, ctx,
		"ALTER TABLE "+mutDB+"."+mutTable+" UPDATE b = toString(intDiv(a, 0)) WHERE 1")

	// 等 latest_fail_reason 落地（真机实测 1 秒内出现，留足余量）。
	deadline := time.Now().Add(30 * time.Second)
	for {
		var stuck uint8
		if err := conn.QueryRow(ctx,
			"SELECT count() > 0 FROM system.mutations "+
				"WHERE database=? AND table=? AND NOT is_done AND latest_fail_reason != ''",
			mutDB, mutTable).Scan(&stuck); err != nil {
			t.Fatalf("读取失败 mutation 状态: %v", err)
		}
		if stuck == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("失败 mutation 30s 内未出现")
		}
		time.Sleep(500 * time.Millisecond)
	}

	// 2. 年轻的正常 mutation：真机实测 async ALTER 立即返回、执行秒级完成，
	//    无法可靠捕捉"未完成且无失败"的窗口；不构造，其"只读不报"语义
	//    由服务端 WHERE + 单测覆盖。
	report, err := diagnose.CheckMutationsStuck(ctx,
		[]diagnose.Target{{NodeID: "it-node", ShardID: "shard-1", Querier: &nativeQuerier{conn: conn}}},
		diagnose.MutationsStuckOptions{Timeout: 15 * time.Second, NodeTimeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != diagnose.ExecutionComplete || report.Outcome != diagnose.OutcomeMatched {
		t.Fatalf("got %s/%s, want complete/matched", report.Execution, report.Outcome)
	}
	if report.CheckID != "mutations_stuck" || report.RuleVersion != "1" {
		t.Errorf("检查身份错误: %s/%s", report.CheckID, report.RuleVersion)
	}
	var match *diagnose.MutationMatch
	for i := range report.Matches {
		m := &report.Matches[i]
		if m.Database == mutDB && m.Table == mutTable {
			if match != nil {
				t.Fatalf("fixture 表出现多条匹配: %+v", report.Matches)
			}
			match = m
		}
	}
	if match == nil {
		t.Fatalf("fixture 的失败 mutation 未匹配: %+v", report.Matches)
	}
	o := match.Observations[0]
	if !slices.Contains(o.Reasons, diagnose.ReasonMutationFailing) {
		t.Errorf("缺少 mutation_failing 理由码: %+v", o.Reasons)
	}
	if !strings.Contains(o.LatestFailReason, "ILLEGAL_DIVISION") {
		t.Errorf("失败文本与预期不符: %.200s", o.LatestFailReason)
	}
	if !strings.Contains(o.Command, "intDiv") {
		t.Errorf("原始命令未采集: %q", o.Command)
	}
	if !hasCode(report.NextSteps, diagnose.AdviceInspectMutationFailure) {
		t.Errorf("匹配应给失败核查建议: %+v", report.NextSteps)
	}

	// 3. 清理 fixture（t.Cleanup 的 DROP DATABASE 会在测试结束时执行），
	//    这里手动提前验证：清掉失败 mutation 后回到 complete/no_match。
	//
	//    注意不能 DROP DATABASE——那是 t.Cleanup 的职责；改用 KILL
	//    MUTATION 把失败条目标记 killed 并同步等待移出队列。
	mustExec(t, conn, ctx, "KILL MUTATION WHERE database='"+mutDB+"' AND table='"+mutTable+"' SYNC")
	dctx, dcancel := context.WithTimeout(ctx, 10*time.Second)
	defer dcancel()
	deadline = time.Now().Add(30 * time.Second)
	for {
		var pending uint64
		if err := conn.QueryRow(dctx,
			"SELECT count() FROM system.mutations WHERE database=? AND table=? AND NOT is_done",
			mutDB, mutTable).Scan(&pending); err != nil {
			t.Fatalf("读取清理后状态: %v", err)
		}
		if pending == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("KILL MUTATION 后 30s 仍有未完成条目")
		}
		time.Sleep(500 * time.Millisecond)
	}

	clean, err := diagnose.CheckMutationsStuck(ctx,
		[]diagnose.Target{{NodeID: "it-node", ShardID: "shard-1", Querier: &nativeQuerier{conn: conn}}},
		diagnose.MutationsStuckOptions{Timeout: 15 * time.Second, NodeTimeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("清理后 err = %v, want nil", err)
	}
	if clean.Execution != diagnose.ExecutionComplete || clean.Outcome != diagnose.OutcomeNoMatch {
		t.Errorf("清理后 got %s/%s, want complete/no_match", clean.Execution, clean.Outcome)
	}
	if len(clean.Matches) != 0 {
		t.Errorf("清理后不应有匹配: %+v", clean.Matches)
	}
}

// TestCheckMutationsStuckHTTP 确认 database/sql（HTTP DSN）路径同样可用。
func TestCheckMutationsStuckHTTP(t *testing.T) {
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

	report, err := diagnose.CheckMutationsStuck(context.Background(),
		[]diagnose.Target{{NodeID: "it-http", ShardID: "shard-1", Querier: &sqlQuerier{db: db}}},
		diagnose.MutationsStuckOptions{Timeout: 15 * time.Second, NodeTimeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != diagnose.ExecutionComplete {
		t.Errorf("execution = %s, want complete", report.Execution)
	}
	// 匹配与否取决于集群当前状态，与服务端事实对账。
	for _, m := range report.Matches {
		if len(m.Observations) == 0 || len(m.Observations[0].Reasons) == 0 {
			t.Errorf("匹配缺少观测或理由码: %+v", m)
		}
	}
}
