package integration

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
)

// sqlForVerification 是核心库固定查询的验证副本（replication_queue_sql.go）。
// 核心库的 SQL 未导出；此副本用于直接验证服务器行为，改动 SQL 时需同步。
const sqlForVerification = `SELECT database, table, new_part_name, last_exception
FROM system.replication_queue
WHERE positionCaseInsensitive(last_exception, 'NO_REPLICA_HAS_PART') > 0
ORDER BY database, table, new_part_name, last_exception`

// TestServerVersionAndQueryContract 验证：服务器可达、固定查询语法有效、
// 四个必需列存在且投影顺序正确。
func TestServerVersionAndQueryContract(t *testing.T) {
	cfg := loadConfig(t)
	conn := openNative(t, cfg, cfg.user, cfg.password, clickhouse.Native)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var version string
	if err := conn.QueryRow(ctx, "SELECT version()").Scan(&version); err != nil {
		t.Fatalf("SELECT version(): %v", err)
	}
	t.Logf("服务器版本: %s", version)

	rows, err := conn.Query(ctx, "DESCRIBE TABLE system.replication_queue")
	if err != nil {
		t.Fatalf("DESCRIBE system.replication_queue: %v", err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		// DESCRIBE 返回 7 列：name, type, default_type, default_expression,
		// comment, codec_expression, ttl_expression。
		var name, typ, dt, de, comment, codec, ttl string
		if err := rows.Scan(&name, &typ, &dt, &de, &comment, &codec, &ttl); err != nil {
			t.Fatalf("scan describe: %v", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("describe err: %v", err)
	}
	for _, col := range []string{"database", "table", "new_part_name", "last_exception"} {
		if !slices.Contains(names, col) {
			t.Errorf("system.replication_queue 缺少列 %q（版本 %s）", col, version)
		}
	}

	// 固定查询语法有效；空队列也应正常返回且列序确定。
	qrows, err := conn.Query(ctx, sqlForVerification)
	if err != nil {
		t.Fatalf("固定查询执行失败: %v", err)
	}
	defer qrows.Close()
	gotCols := qrows.Columns()
	wantCols := []string{"database", "table", "new_part_name", "last_exception"}
	if !slices.Equal(gotCols, wantCols) {
		t.Errorf("投影列 = %v, want %v", gotCols, wantCols)
	}
	n := 0
	for qrows.Next() {
		n++
		var db, table, part, excp string
		if err := qrows.Scan(&db, &table, &part, &excp); err != nil {
			t.Fatalf("scan 队列行: %v", err)
		}
	}
	if err := qrows.Err(); err != nil {
		t.Fatalf("queue rows err: %v", err)
	}
	t.Logf("system.replication_queue 当前 %d 行（验证集群通常为空）", n)
}
