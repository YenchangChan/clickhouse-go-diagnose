package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"

	diagnose "github.com/YenchangChan/clickhouse-go-diagnose"
)

// TestCheckRealNoReplicaHasPart 验证库对**真实** NO_REPLICA_HAS_PART 队列条目的
// 匹配与归并。故障由 scripts/inject_no_replica_has_part.py 注入：同一 Keeper
// 路径上两个副本，队列里出现一个任何活跃副本都没有的 part（merge 的源 part）。
//
// 与其他集成测试不同，这里读的是 system.replication_queue 的真实失败条目，
// 异常文本包含完整堆栈（4KB+），可以覆盖「真实文本 vs 匹配规则」的差异。
func TestCheckRealNoReplicaHasPart(t *testing.T) {
	cfg := loadConfig(t)
	conn := openNative(t, cfg, cfg.user, cfg.password, clickhouse.Native)
	ctx := context.Background()

	// 先确认故障仍在：注入是外部动作，测试不自己造故障。
	var pending uint64
	err := conn.QueryRow(ctx,
		"SELECT count() FROM system.replication_queue WHERE positionCaseInsensitive(last_exception, 'NO_REPLICA_HAS_PART') > 0").
		Scan(&pending)
	if err != nil {
		t.Fatalf("预检查 queue 失败: %v", err)
	}
	if pending == 0 {
		t.Skip("当前没有真实 NO_REPLICA_HAS_PART 队列条目；先运行 scripts/inject_no_replica_has_part.py 注入故障")
	}
	t.Logf("真实故障条目数: %d", pending)

	// 期望的 part 清单直接取服务端事实，避免测试与注入脚本硬编码耦合。
	rows, err := conn.Query(ctx,
		"SELECT DISTINCT database, table, new_part_name FROM system.replication_queue "+
			"WHERE positionCaseInsensitive(last_exception, 'NO_REPLICA_HAS_PART') > 0")
	if err != nil {
		t.Fatalf("读取期望 part 清单: %v", err)
	}
	type key struct{ db, table, part string }
	var want []key
	for rows.Next() {
		var k key
		if err := rows.Scan(&k.db, &k.table, &k.part); err != nil {
			t.Fatalf("scan: %v", err)
		}
		want = append(want, k)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	_ = rows.Close()

	// 两个副本当作同一分片的两个节点：应各自保留来源，不因同分片而丢失观测。
	targets := []diagnose.Target{
		{NodeID: "fault-r1", ShardID: "shard-1", Querier: &nativeQuerier{conn: conn}},
		{NodeID: "fault-r2", ShardID: "shard-1", Querier: &nativeQuerier{conn: conn}},
	}
	report, err := diagnose.CheckReplicationQueueFailures(ctx, targets,
		diagnose.ReplicationQueueOptions{Timeout: 15 * time.Second, NodeTimeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("err = %v, want nil", err)
	}
	if report.Execution != diagnose.ExecutionComplete || report.Outcome != diagnose.OutcomeMatched {
		t.Fatalf("got %s/%s, want complete/matched", report.Execution, report.Outcome)
	}

	// 归并键是 (分片, 库, 表, part)。注入用的两张表名不同（fault_r1/fault_r2），
	// 所以同一 part 会归并成两条匹配。
	//
	// 每个匹配的来源观测数等于目标节点数：两个 target 指向同一台服务器，
	// 因此都能看到对方副本的队列记录，归并后保留全部来源。
	// 这验证的正是「同分片多副本报同一 part 合并展示、但不丢失来源」的语义。
	got := map[key]int{}
	for _, m := range report.Matches {
		k := key{m.Database, m.Table, m.PartName}
		got[k] = len(m.Observations)
		if m.ShardID != "shard-1" {
			t.Errorf("分片错误: %+v", m)
		}
		for _, o := range m.Observations {
			if o.NodeID != "fault-r1" && o.NodeID != "fault-r2" {
				t.Errorf("来源节点未知: %+v", o)
			}
		}
	}
	for _, k := range want {
		n, ok := got[k]
		if !ok {
			t.Errorf("漏报真实故障 %v（got %v）", k, got)
			continue
		}
		if n != len(targets) {
			t.Errorf("%v 来源观测 = %d, want %d", k, n, len(targets))
		}
	}
	if len(got) != len(want) {
		t.Errorf("匹配数 = %d, want %d（不应多报）", len(got), len(want))
	}

	// 真实异常文本形态：包含错误码名与完整堆栈。匹配不能依赖整洁文案。
	for _, m := range report.Matches {
		for _, o := range m.Observations {
			if !strings.Contains(o.LastException, "No active replica has part") {
				t.Errorf("异常文本与预期不符: %.160s", o.LastException)
			}
			if !strings.Contains(o.LastException, "NO_REPLICA_HAS_PART") {
				t.Errorf("异常文本缺少错误码名（匹配依赖此标记）")
			}
			t.Logf("part %s 异常长度 %d 字节，首行: %.120s",
				m.PartName, len(o.LastException), strings.SplitN(o.LastException, "\n", 2)[0])
		}
	}

	if !hasCode(report.NextSteps, diagnose.AdviceVerifyReplicasForPart) {
		t.Errorf("匹配结果应给出逐副本核查建议: %+v", report.NextSteps)
	}
}
