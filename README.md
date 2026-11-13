# clickhouse-go-diagnose

独立的 ClickHouse 诊断库，基于既有诊断平台的诊断案例抽取固定查询、证据整理与判定能力，首个计划消费端为 ckman。

许可：[Apache-2.0](LICENSE)

**当前状态：核心库原型 + 真机验证完成（M1/M2/M3）。** 单元测试、`go vet`、`-race` 全部通过；已在 ClickHouse 26.10.3.7（native + HTTP/database/sql）上完成查询契约、采集机制、取消、权限路径，以及**真实 `NO_REPLICA_HAS_PART` 队列条目**、**真实碎片化分区**、**真实副本异常**与**真实卡住 mutation**的匹配验证，见 `docs/compatibility.md`。已实现四个检查：`CheckReplicationQueueFailures`（NO_REPLICA_HAS_PART）、`CheckTooManyParts`（单分区 part 数超阈值）、`CheckReplicaStatus`（副本只读/会话过期/active 缺口/延迟超阈值/丢件记录）与 `CheckMutationsStuck`（mutation 失败重试/未完成超龄）。仍缺：现场真实案例数据（M0）、ckman 接入（M4）。

- [实现计划](docs/implementation-plan.md)：首个检查的接口、实施步骤、验收与接入方式。
- [检查路线图](docs/checks-roadmap.md)：后续候选检查清单与优先级。
- [方案背景](docs/ai-diagnostics.md)：产品边界、已有实现依据与后续方向。
- [兼容性](docs/compatibility.md)：已验证版本、协议与实测限制。

## v0 范围

四个独立检查，各自返回结构化报告：复制队列的 `NO_REPLICA_HAS_PART` 异常（`CheckReplicationQueueFailures`）、单分区活跃 part 数超阈值（`CheckTooManyParts`）、副本状态异常（`CheckReplicaStatus`：只读、会话过期、active 副本缺口、延迟超阈值、丢件记录）与卡住的 mutation（`CheckMutationsStuck`：失败重试、未完成超龄）。所有检查保留节点来源和检查完整性。匹配/超阈值**不等于**确认故障起因或需要修复；无匹配**不等于**集群健康。不自动修复，不依赖 LLM。

## 使用

```go
import "github.com/YenchangChan/clickhouse-go-diagnose"

// Querier 由调用方实现：包装 clickhouse-go 或 database/sql 的连接。
// 库不管理连接、凭据与拓扑，只对返回的 Rows 调用 Close。
type ckQuerier struct{ conn *clickhouse.Conn }

func (q *ckQuerier) Query(ctx context.Context, query string, args ...any) (diagnose.Rows, error) {
    rows, err := q.conn.Query(ctx, query, args...)
    if err != nil { return nil, err }
    return &rowsAdapter{rows}, nil // 薄适配：Next/Scan/Err/Close
}

report, err := diagnose.CheckReplicationQueueFailures(ctx, []diagnose.Target{
    {NodeID: "host-a:9000", ShardID: "shard-1", Querier: q1},
    {NodeID: "host-b:9000", ShardID: "shard-1", Querier: q2},
}, diagnose.ReplicationQueueOptions{})

// 关键：即使 err 非空（取消、超时、ErrNoUsableObservation），
// report 中已保留的证据仍然有效，应先处理报告再展示错误。
```

## 结果语义

| 字段组 | 内容 |
|---|---|
| `Execution` | `complete` / `partial` / `failed`，采集完整性，与异常判断分离 |
| `Outcome` | `matched` / `no_match` / `unknown`，仅表示本检查的匹配结果 |
| `Targets` | 每个节点状态、读取/保留行数、截断标记、错误类别（`query_failed`、`timeout`、`canceled` 等） |
| `Matches` | 按检查归并：队列检查按 `(ShardID, Database, Table, PartName)`，分区检查按 `(ShardID, Database, Table, Partition)`，副本检查按 `(ShardID, Database, Table)`，mutation 检查按 `(ShardID, Database, Table, MutationID)`；同分片多副本合并展示但保留各来源节点；不同分片绝不合并 |
| `NextSteps` | 稳定建议代码（如 `verify_replicas_for_part`），文案由消费端决定 |

默认预算：总 10s、单节点 2s、并发 4、单节点保留 1000 条匹配（读 N+1 条检测截断）。零值选项使用默认值，负值报错。

## 适配器约定

- `Query` 必须真实传递 context，不得用后台 goroutine 伪装取消；库无法强制终止不遵守 context 的实现。
- `Rows.Next/Scan/Err/Close`：迭代错误通过 `Err()` 报告，不得静默结束；`Close` 幂等且不关闭调用方共享连接。
- 返回 `driver.Rows` / `sql.Rows` 的方法不会自动满足 `diagnose.Rows`，各路径需薄适配器。

## 开发

```bash
go test ./...
go test -race ./...
go vet ./...

# 真实服务器验证（默认不访问外部集群，未设变量则跳过）
GOTMPDIR=~/go-tmp NO_PROXY=<集群 IP> \
  DIAGNOSE_IT_ADDR=<host>:9000 DIAGNOSE_IT_HTTP_ADDR=<host>:8123 \
  go test -count=1 -v ./integration

# 在隔离集群注入一次真实 NO_REPLICA_HAS_PART（需 Keeper；用 --cleanup 复原）
CH_ADDR=<host>:8123 CH_PASSWORD=... ZK_ADDR=<host>:9181 \
  python3 scripts/inject_no_replica_has_part.py

# 在隔离集群注入幽灵副本（active_replicas 缺口；用 --cleanup 复原）
CH_ADDR=<host>:8123 CH_PASSWORD=... ZK_ADDR=<host>:9181 \
  python3 scripts/inject_ghost_replica.py
```

核心库仅依赖标准库。`testdata/replication_queue/` 为合成回放案例（非真实现场）；`integration/` 为独立模块，含真实服务器契约测试与真实故障匹配测试（`real_fault_test.go`，需先用上面的脚本注入故障，否则自动 skip）。
