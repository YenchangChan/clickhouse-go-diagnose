package integration

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	diagnose "github.com/YenchangChan/clickhouse-go-diagnose"
)

// TestDiskSpaceColumnsOnRealServer 验证固定查询依赖的列在真实服务器上存在。
func TestDiskSpaceColumnsOnRealServer(t *testing.T) {
	cfg := loadConfig(t)
	conn := openNative(t, cfg, cfg.user, cfg.password, clickhouse.Native)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rows, err := conn.Query(ctx,
		"SELECT name FROM system.columns WHERE database='system' AND table='disks'")
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
	for _, col := range []string{"name", "path", "free_space", "total_space", "keep_free_space"} {
		if !names[col] {
			t.Errorf("system.disks 缺少列 %q", col)
		}
	}
}

// TestCheckDiskSpaceReal 真实场景：
//   - 默认阈值（10%）下真实集群磁盘水位通常不命中（本集群约 87% 空闲）；
//   - 阈值提到 100% 时每块 total>0 的磁盘都应命中，且客户端算出的占比与
//     服务端事实一致——验证判定链路而不依赖构造磁盘满。
func TestCheckDiskSpaceReal(t *testing.T) {
	cfg := loadConfig(t)
	conn := openNative(t, cfg, cfg.user, cfg.password, clickhouse.Native)
	ctx := context.Background()

	targets := []diagnose.Target{
		{NodeID: "disk-real", ShardID: "shard-1", Querier: &nativeQuerier{conn: conn}},
	}

	// 默认阈值：允许 matched（真实低水位应被报出），但不得误报健康磁盘。
	report, err := diagnose.CheckDiskSpaceLow(ctx, targets, diagnose.DiskSpaceOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != diagnose.ExecutionComplete {
		t.Fatalf("execution = %s, want complete", report.Execution)
	}
	for _, m := range report.Matches {
		for _, o := range m.Observations {
			if o.TotalBytes == 0 {
				continue
			}
			want := int(o.FreeBytes * 100 / o.TotalBytes)
			if o.FreePercent != want {
				t.Errorf("占比计算错误: got %d, want %d (%+v)", o.FreePercent, want, o)
			}
			if o.FreePercent > 10 && !(o.KeepFreeBytes > 0 && o.FreeBytes < o.KeepFreeBytes) {
				t.Errorf("默认阈值下不应命中健康磁盘: %+v", o)
			}
		}
	}
	t.Logf("默认阈值 outcome=%s matches=%d", report.Outcome, len(report.Matches))

	// 阈值 100%：每块 total>0 的磁盘都应命中。
	reportAll, err := diagnose.CheckDiskSpaceLow(ctx, targets,
		diagnose.DiskSpaceOptions{FreePercentThreshold: 100})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if reportAll.Execution != diagnose.ExecutionComplete || reportAll.Outcome != diagnose.OutcomeMatched {
		t.Fatalf("got %s/%s, want complete/matched", reportAll.Execution, reportAll.Outcome)
	}

	rows, err := conn.Query(ctx, "SELECT name, path, free_space, total_space FROM system.disks")
	if err != nil {
		t.Fatalf("读取磁盘事实: %v", err)
	}
	defer rows.Close()
	want := map[string]int{} // name|path -> 占比
	for rows.Next() {
		var name, path string
		var free, total uint64
		if err := rows.Scan(&name, &path, &free, &total); err != nil {
			t.Fatalf("scan: %v", err)
		}
		if total == 0 {
			continue
		}
		want[name+"|"+path] = int(free * 100 / total)
	}
	if rows.Err() != nil {
		t.Fatalf("rows err: %v", rows.Err())
	}

	got := map[string]int{}
	for _, m := range reportAll.Matches {
		got[m.DiskName+"|"+m.Path] = m.FreePercent
	}
	for k, wp := range want {
		gp, ok := got[k]
		if !ok {
			t.Errorf("漏报磁盘 %s（占比 %d%%）", k, wp)
			continue
		}
		if gp != wp {
			t.Errorf("%s 占比 = %d, want %d", k, gp, wp)
		}
	}
	if len(got) != len(want) {
		t.Errorf("匹配数 = %d, want %d（不应多报）", len(got), len(want))
	}
	t.Logf("阈值100 outcome=%s matches=%d（服务端磁盘 %d 块）", reportAll.Outcome, len(reportAll.Matches), len(want))
}

// TestCheckDiskSpaceHTTP 确认 database/sql（HTTP DSN）路径同样可用。
func TestCheckDiskSpaceHTTP(t *testing.T) {
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

	report, err := diagnose.CheckDiskSpaceLow(ctx,
		[]diagnose.Target{{NodeID: "it-http", ShardID: "shard-1", Querier: &sqlQuerier{db: db}}},
		diagnose.DiskSpaceOptions{})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != diagnose.ExecutionComplete {
		t.Fatalf("execution = %s, want complete", report.Execution)
	}
	t.Logf("HTTP 路径 outcome=%s matches=%d", report.Outcome, len(report.Matches))
}
