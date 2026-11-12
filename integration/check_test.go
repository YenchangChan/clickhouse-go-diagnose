package integration

import (
	"context"
	"database/sql"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	diagnose "example.org/clickhouse-go-diagnose"
)

const (
	mimicDB    = "diagnose_it_verify"
	mimicTable = mimicDB + ".mimic_replication_queue"
	fromTable  = "system.replication_queue"
)

// setupMimic 建立隔离仿表，镜像 system.replication_queue 的四个投影列
// （last_exception 为 String；另用 Nullable 副表验证 NULL 处理）。
// 返回清理函数。
func setupMimic(t *testing.T, conn driver.Conn, rows int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mustExec(t, conn, ctx, "DROP DATABASE IF EXISTS "+mimicDB)
	mustExec(t, conn, ctx, "CREATE DATABASE "+mimicDB)
	// last_exception 用 Nullable(String)：比真实列更严格，顺带验证 NULL 行
	// 不会导致检查失败。
	mustExec(t, conn, ctx, `CREATE TABLE `+mimicTable+` (
		database String,
		`+"`table`"+` String,
		new_part_name String,
		last_exception Nullable(String)
	) ENGINE = MergeTree ORDER BY tuple()`)
	if rows > 0 {
		mustExec(t, conn, ctx,
			`INSERT INTO `+mimicTable+`
			SELECT 'db1', 't1', concat('p', toString(number)), 'Code: 262. DB::Exception: No replica has part (NO_REPLICA_HAS_PART) x'
			FROM numbers(`+itoa(rows)+`)`)
	}
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dcancel()
		_ = conn.Exec(dctx, "DROP DATABASE IF EXISTS "+mimicDB)
	})
}

func mustExec(t *testing.T, conn driver.Conn, ctx context.Context, sql string) {
	t.Helper()
	if err := conn.Exec(ctx, sql); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

func rewriteAdapter(conn driver.Conn) *rewriteQuerier {
	return &rewriteQuerier{
		inner: &nativeQuerier{conn: conn},
		from:  fromTable,
		to:    mimicTable,
	}
}

// TestCheckNativeEmptyQueue 真实服务器上运行检查，期望值与服务端真实状态一致：
// 队列为空时 complete + no_match；集群上存在真实故障条目时 complete + matched。
// （注入脚本 scripts/inject_no_replica_has_part.py 会让队列非空。）
func TestCheckNativeEmptyQueue(t *testing.T) {
	cfg := loadConfig(t)
	conn := openNative(t, cfg, cfg.user, cfg.password, clickhouse.Native)
	ctx := context.Background()

	var pending uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM "+fromTable+
		" WHERE positionCaseInsensitive(last_exception, 'NO_REPLICA_HAS_PART') > 0").Scan(&pending); err != nil {
		t.Fatalf("计数失败: %v", err)
	}
	wantOutcome := diagnose.OutcomeNoMatch
	if pending > 0 {
		wantOutcome = diagnose.OutcomeMatched
	}

	report, err := diagnose.CheckReplicationQueueFailures(ctx,
		[]diagnose.Target{
			{NodeID: "it-node-a", ShardID: "shard-1", Querier: &nativeQuerier{conn: conn}},
		},
		diagnose.ReplicationQueueOptions{Timeout: 10 * time.Second, NodeTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != diagnose.ExecutionComplete || report.Outcome != wantOutcome {
		t.Errorf("got %s/%s, want complete/%s（服务端匹配行数 %d）",
			report.Execution, report.Outcome, wantOutcome, pending)
	}
	nr := report.Targets[0]
	if nr.Status != diagnose.NodeOK || nr.ErrCategory != "" {
		t.Errorf("目标状态错误: %+v", nr)
	}
}

// TestCheckHTTPProtocol 验证 HTTP 协议路径。
//
// 真机验证发现：clickhouse-go v2.15.0 的 Options.Protocol 字段在 Open()
// 路径下不被消费（dial 只走 native 握手），HTTP 仅在 DSN scheme 或
// database/sql 路径生效。因此本测试走 database/sql + http DSN。
func TestCheckHTTPProtocol(t *testing.T) {
	cfg := loadConfig(t)
	if cfg.httpAddr == "" {
		t.Skip("DIAGNOSE_IT_HTTP_ADDR 未设置，跳过 HTTP 协议验证")
	}
	dsn := "http://" + cfg.user + ":" + cfg.password + "@" + cfg.httpAddr + "/default"
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		t.Fatalf("sql open http dsn: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("http ping: %v", err)
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	var pending uint64
	if err := db.QueryRowContext(ctx2, "SELECT count() FROM "+fromTable+
		" WHERE positionCaseInsensitive(last_exception, 'NO_REPLICA_HAS_PART') > 0").Scan(&pending); err != nil {
		t.Fatalf("http 计数失败: %v", err)
	}
	wantOutcome := diagnose.OutcomeNoMatch
	if pending > 0 {
		wantOutcome = diagnose.OutcomeMatched
	}

	report, err := diagnose.CheckReplicationQueueFailures(context.Background(),
		[]diagnose.Target{{NodeID: "it-http", ShardID: "shard-1", Querier: &sqlQuerier{db: db}}},
		diagnose.ReplicationQueueOptions{Timeout: 10 * time.Second, NodeTimeout: 5 * time.Second})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != diagnose.ExecutionComplete || report.Outcome != wantOutcome {
		t.Errorf("got %s/%s, want complete/%s", report.Execution, report.Outcome, wantOutcome)
	}
}

// TestCheckMimicTable 真实服务器上验证采集机制：跨目标归并、NULL 行、
// 非匹配行、N+1 截断。
func TestCheckMimicTable(t *testing.T) {
	cfg := loadConfig(t)
	conn := openNative(t, cfg, cfg.user, cfg.password, clickhouse.Native)
	setupMimic(t, conn, 0)

	ctx := context.Background()
	// 每个"节点"5 行：3 条命中 + 1 条 NULL + 1 条不相关。
	insert := `INSERT INTO ` + mimicTable + ` VALUES
		('db1','t1','20261102_10_20_3','Code: 262 (NO_REPLICA_HAS_PART) a'),
		('db1','t1','20261102_0_2229_425','Code: 262 (NO_REPLICA_HAS_PART) b'),
		('db1','t1','p3b','NO_REPLICA_HAS_PART c'),
		('db1','t2','p_null',NULL),
		('db2','t1','p_plain','Table is in readonly mode')`
	mustExec(t, conn, ctx, insert)

	a := diagnose.Target{NodeID: "node-a", ShardID: "shard-1", Querier: rewriteAdapter(conn)}
	b := diagnose.Target{NodeID: "node-b", ShardID: "shard-1", Querier: rewriteAdapter(conn)}
	report, err := diagnose.CheckReplicationQueueFailures(ctx, []diagnose.Target{a, b},
		diagnose.ReplicationQueueOptions{Timeout: 10 * time.Second, NodeTimeout: 5 * time.Second})
	if err != nil {
		for _, nr := range report.Targets {
			t.Logf("目标 %s 失败信息: %s", nr.NodeID, nr.ErrMessage)
		}
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != diagnose.ExecutionComplete || report.Outcome != diagnose.OutcomeMatched {
		t.Fatalf("got %s/%s, want complete/matched", report.Execution, report.Outcome)
	}
	// 同分片两节点报同样 3 个 part：归并为 3 条，每条 2 个来源观测。
	if len(report.Matches) != 3 {
		t.Fatalf("匹配数 = %d, want 3: %+v", len(report.Matches), report.Matches)
	}
	for _, m := range report.Matches {
		if m.ShardID != "shard-1" || m.Database != "db1" || m.Table != "t1" {
			t.Errorf("归并键错误: %+v", m)
		}
		if len(m.Observations) != 2 {
			t.Errorf("part %s 来源观测 = %d, want 2（同分片两副本应合并）", m.PartName, len(m.Observations))
		}
	}
	parts := []string{}
	for _, m := range report.Matches {
		parts = append(parts, m.PartName)
	}
	for _, want := range []string{"20261102_0_2229_425", "20261102_10_20_3", "p3b"} {
		if !slices.Contains(parts, want) {
			t.Errorf("缺少 part %s: %v", want, parts)
		}
	}
	if slices.Contains(parts, "p_null") || slices.Contains(parts, "p_plain") {
		t.Errorf("NULL 行与不相关行不应匹配: %v", parts)
	}
	for _, nr := range report.Targets {
		// 注意：服务端 WHERE 已过滤 NULL 与不相关行，客户端只读到 3 条匹配行；
		// RowsRead==RowsKept 是预期行为，NULL 行由服务端过滤而非客户端跳过。
		if nr.RowsRead != 3 || nr.RowsKept != 3 || nr.Truncated {
			t.Errorf("节点计数错误: %+v", nr)
		}
	}

	// N+1 截断：MaxRowsPerNode=2，读到第 3 条匹配即停止。
	report, err = diagnose.CheckReplicationQueueFailures(ctx, []diagnose.Target{a},
		diagnose.ReplicationQueueOptions{Timeout: 10 * time.Second, NodeTimeout: 5 * time.Second, MaxRowsPerNode: 2})
	if err != nil {
		t.Fatalf("truncation err = %v, want nil", err)
	}
	if report.Execution != diagnose.ExecutionPartial || report.Outcome != diagnose.OutcomeMatched {
		t.Errorf("got %s/%s, want partial/matched（截断不能输出 complete）", report.Execution, report.Outcome)
	}
	nr := report.Targets[0]
	if nr.RowsRead != 3 || nr.RowsKept != 2 || !nr.Truncated {
		t.Errorf("截断计数错误: %+v", nr)
	}
	if !hasCode(report.NextSteps, diagnose.AdviceRerunWithHigherRowLimit) {
		t.Errorf("截断应提示复跑: %+v", report.NextSteps)
	}
}

// TestCheckCancelTinyBudget 极小预算下的取消路径：无论取消发生在 Query
// 还是迭代中，语义必须一致（不得输出 complete）。
func TestCheckCancelTinyBudget(t *testing.T) {
	cfg := loadConfig(t)
	conn := openNative(t, cfg, cfg.user, cfg.password, clickhouse.Native)
	setupMimic(t, conn, 50000)

	report, err := diagnose.CheckReplicationQueueFailures(context.Background(),
		[]diagnose.Target{{NodeID: "node-big", ShardID: "s1", Querier: rewriteAdapter(conn)}},
		diagnose.ReplicationQueueOptions{Timeout: 200 * time.Millisecond, NodeTimeout: 100 * time.Millisecond, MaxRowsPerNode: 60000})
	if err == nil {
		// 在预算内完成：必须完整且匹配。
		if report.Execution != diagnose.ExecutionComplete || report.Outcome != diagnose.OutcomeMatched {
			t.Errorf("提前完成但语义错误: %s/%s", report.Execution, report.Outcome)
		}
		t.Logf("50000 行在 200ms 内完成（本地服务器性能足够）")
		return
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if report.Execution == diagnose.ExecutionComplete {
		t.Errorf("超时返回了 complete")
	}
	for _, nr := range report.Targets {
		switch nr.ErrCategory {
		case diagnose.ErrCatTimeout, diagnose.ErrCatCanceled, diagnose.ErrCatQueryFailed, diagnose.ErrCatIterationFailed:
			// 允许的分类：取消可能发生在 Query 或迭代中。
		default:
			t.Errorf("超时下出现异常分类 %q: %+v", nr.ErrCategory, nr)
		}
	}
	t.Logf("取消路径验证通过: execution=%s, target_err=%s", report.Execution, report.Targets[0].ErrCategory)
}

// TestCheckPermissionDenied 无权限用户：必须得到可识别的失败，
// 不能当作空结果（no_match）。
func TestCheckPermissionDenied(t *testing.T) {
	cfg := loadConfig(t)
	conn := openNative(t, cfg, cfg.user, cfg.password, clickhouse.Native)
	ctx := context.Background()

	mustExec(t, conn, ctx, "DROP USER IF EXISTS diagnose_it_none")
	mustExec(t, conn, ctx, "CREATE USER diagnose_it_none IDENTIFIED WITH plaintext_password BY 'it_none_pw'")
	t.Cleanup(func() {
		dctx, dcancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dcancel()
		_ = conn.Exec(dctx, "DROP USER IF EXISTS diagnose_it_none")
	})

	noneConn := openNative(t, cfg, "diagnose_it_none", "it_none_pw", clickhouse.Native)
	report, err := diagnose.CheckReplicationQueueFailures(context.Background(),
		[]diagnose.Target{{NodeID: "no-grants", ShardID: "s1", Querier: &nativeQuerier{conn: noneConn}}},
		diagnose.ReplicationQueueOptions{Timeout: 10 * time.Second, NodeTimeout: 5 * time.Second})
	if !errors.Is(err, diagnose.ErrNoUsableObservation) {
		t.Fatalf("err = %v, want ErrNoUsableObservation", err)
	}
	if report.Execution != diagnose.ExecutionFailed || report.Outcome != diagnose.OutcomeUnknown {
		t.Errorf("got %s/%s, want failed/unknown（权限不足不能当作空结果）", report.Execution, report.Outcome)
	}
	nr := report.Targets[0]
	if nr.Status != diagnose.NodeFailed || nr.ErrCategory != diagnose.ErrCatQueryFailed {
		t.Errorf("权限错误分类: %+v", nr)
	}
	t.Logf("权限不足错误信息（截断）: %.120s", nr.ErrMessage)
}

func hasCode(steps []diagnose.FollowUp, code string) bool {
	for _, s := range steps {
		if s.Code == code {
			return true
		}
	}
	return false
}
