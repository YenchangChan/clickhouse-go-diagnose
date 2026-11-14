# clickhouse-go-diagnose

独立 ClickHouse 诊断库，基于既有诊断平台的诊断案例抽取固定查询、证据整理与判定能力。两种使用方式：

- **[方式一：作为 Go 库引用](#方式一作为-go-库引用)** —— 在你的 Go 程序（ckman、运维平台、Agent 工具等）里直接调用检查；
- **[方式二：使用 clickhouse-doctor 工具](#方式二使用-clickhouse-doctor-工具)** —— 开箱即用的命令行（零 LLM 轻量 CLI + 可选对话式 Agent 入口）。

许可：[Apache-2.0](LICENSE)

**当前状态：核心库原型 + 真机验证完成（M1/M2/M3）。** 单元测试、`go vet`、`-race` 全部通过；已在 ClickHouse 26.10.3.7 与 26.3.13.31（native + HTTP/database/sql）上完成查询契约、采集机制、取消、权限路径，以及真实故障匹配验证（`NO_REPLICA_HAS_PART` 队列条目、碎片化分区、副本异常、卡住 mutation、Keeper 会话丢失窗口），见 `docs/compatibility.md`。仍缺：现场真实案例数据（M0）、ckman 接入（M4）。

文档索引：[实现计划](docs/implementation-plan.md) · [检查路线图](docs/checks-roadmap.md) · [方案背景](docs/ai-diagnostics.md) · [抽取分析笔记](docs/extraction-notes.md) · [兼容性](docs/compatibility.md)

## v0 范围

七个独立检查，各自返回结构化报告：复制队列的 `NO_REPLICA_HAS_PART` 异常（`CheckReplicationQueueFailures`）、单分区活跃 part 数超阈值（`CheckTooManyParts`）、副本状态异常（`CheckReplicaStatus`：Keeper 不可达、只读、会话过期、active 副本缺口、延迟超阈值、丢件记录）、卡住的 mutation（`CheckMutationsStuck`：失败重试、未完成超龄）、队列积压（`CheckReplicationQueueBacklog`：在队条目数、最老条目年龄、重试次数超阈值）、磁盘水位（`CheckDiskSpaceLow`：剩余占比阈值、keep_free_space 保留量击穿）与 detached part（`CheckDetachedParts`：损坏、服务端自行下线、单表数量超阈值）。所有检查保留节点来源和检查完整性。匹配/超阈值**不等于**确认故障起因或需要修复；无匹配**不等于**集群健康。库只产出证据与稳定理由码，**不判定严重级别/告警档位**——分级由消费端基于证据自行决定。不自动修复，不依赖 LLM。

## 方式一：作为 Go 库引用

核心库仅依赖标准库，不引入 ClickHouse 驱动——连接由调用方管理，通过 `Querier` 接口注入。

### 安装

```bash
go get github.com/YenchangChan/clickhouse-go-diagnose@latest
```

> 下文的统一入口（`ListChecks`/`RunCheck`）需要 v0.3.0+；更早版本可直接调用各 `CheckXxx` 函数（见下文"两个调用层次"）。

### 最小可运行示例

```go
package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2" // 或任意 database/sql 驱动
	diagnose "github.com/YenchangChan/clickhouse-go-diagnose"
)

//Querier 由调用方实现：包装既有连接。库不管理连接、凭据与拓扑，
// 只对返回的 Rows 调用 Close。
type ckQuerier struct{ db *sql.DB }

func (q *ckQuerier) Query(ctx context.Context, query string, args ...any) (diagnose.Rows, error) {
	rows, err := q.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return &rowsAdapter{rows}, nil // 薄适配：Next/Scan(*sql.NullString)/Err/Close
}

func main() {
	db, _ := sql.Open("clickhouse", "clickhouse://default:***@127.0.0.1:9000")
	q := &ckQuerier{db: db}

	rep, err := diagnose.RunCheck(context.Background(), "replica_status", []diagnose.Target{
		{NodeID: "node-a:9000", ShardID: "shard-1", Querier: q},
	}, diagnose.CheckOptions{Timeout: 10 * time.Second})

	// 关键契约：即使 err 非空（取消、超时、ErrNoUsableObservation），
	// report 中已保留的证据仍然有效，应先处理报告再展示错误。
	if rep != nil {
		h := rep.Header() // ID/RuleVersion/Execution/Outcome/Targets/NextSteps
		fmt.Printf("%s outcome=%s execution=%s\n", h.ID, h.Outcome, h.Execution)
	}
	if err != nil {
		fmt.Println("error:", err)
	}
}
```

报告可作为 JSON 输出给下游消费（键名 snake_case，如 `shard_id`/`matches`/`observations`）；需要逐条证据时把 `Report` 断言回具体类型（如 `diagnose.ReplicaStatusReport`）取 `Matches`。

### 两个调用层次

```go
// 层次 A：直接调用 CheckXxx —— 需要精确的 Options 类型与报告类型时。
// 各函数完全等价于统一入口，语义文档见 godoc。
rep, err := diagnose.CheckReplicationQueueFailures(ctx, targets, diagnose.ReplicationQueueOptions{})

// 层次 B：统一入口 —— 面向编排与机器消费（CLI/ckman/Agent 工具注册）。
for _, meta := range diagnose.ListChecks() {
	fmt.Println(meta.ID, meta.RuleVersion, meta.Description) // 稳定 ID 与阈值槽位声明
}
rep, err := diagnose.RunCheck(ctx, "replica_status", targets, diagnose.CheckOptions{
	Timeout: 10 * time.Second,
	// Thresholds 按 CheckMeta.Thresholds 槽位序覆盖，缺省用库默认值
})
```

### 结果语义

| 字段组 | 内容 |
|---|---|
| `Execution` | `complete` / `partial` / `failed`，采集完整性，与异常判断分离 |
| `Outcome` | `matched` / `no_match` / `unknown`，仅表示本检查的匹配结果 |
| `Targets` | 每个节点状态、读取/保留行数、截断标记、错误类别（`query_failed`、`timeout`、`canceled` 等） |
| `Matches` | 按检查归并：队列异常检查按 `(ShardID, Database, Table, PartName)`，分区检查按 `(ShardID, Database, Table, Partition)`，副本/积压/detached 检查按 `(ShardID, Database, Table)`，mutation 检查按 `(ShardID, Database, Table, MutationID)`，磁盘检查按 `(ShardID, DiskName, Path)`；同分片多副本合并展示但保留各来源节点；不同分片绝不合并 |
| `NextSteps` | 稳定建议代码（如 `verify_replicas_for_part`），文案由消费端决定 |

默认预算：总 10s、单节点 2s、并发 4、单节点保留 1000 条匹配（读 N+1 条检测截断）。零值选项使用默认值，负值报错。

### 适配器约定

- `Query` 必须真实传递 context，不得用后台 goroutine 伪装取消；库无法强制终止不遵守 context 的实现。
- `Rows.Next/Scan/Err/Close`：迭代错误通过 `Err()` 报告，不得静默结束；`Close` 幂等且不关闭调用方共享连接。
- 返回 `driver.Rows` / `sql.Rows` 的方法不会自动满足 `diagnose.Rows`，各路径需薄适配器。

## 方式二：使用 clickhouse-doctor 工具

[`clickhouse-doctor/`](clickhouse-doctor/README.md) 是库的命令行消费端，两个入口：

- **`clickhouse-doctor`**：零 LLM 的轻量 CLI，运行即退出，适合 CI、脚本、日常排障；
- **`clickhouse-doctor-chat`**（可选）：对话式 Agent 入口，基于 nova，工具面只有两个只读检查工具，Agent 结构上无法执行 SQL 或修复。

### 安装

前置：Go 1.26+（`chat` 入口要求 Go 1.26.6+；轻量 CLI 自身兼容旧版）。

```bash
# 轻量 CLI（零 LLM、零 Agent 依赖）
go install github.com/YenchangChan/clickhouse-go-diagnose/clickhouse-doctor@latest

# chat 入口（可选；内含 nova 与 markdown 渲染依赖）
go install github.com/YenchangChan/clickhouse-go-diagnose/clickhouse-doctor/chat@latest
```

> `clickhouse-doctor` 是仓库内的嵌套独立模块，其版本 tag 带子目录前缀（`clickhouse-doctor/vX.Y.Z`）；`@latest` 解析该前缀的最新 tag。

也可以从源码构建（产物在 `clickhouse-doctor/bin/`）：

```bash
git clone https://github.com/YenchangChan/clickhouse-go-diagnose
cd clickhouse-go-diagnose && make build
```

### 快速上手

```bash
# 单节点，全部 7 个检查（密码走环境变量，不进 shell 历史）
export CLICKHOUSE_DOCTOR_PASSWORD='...'
clickhouse-doctor --addr 127.0.0.1:9000

# 多节点集群（可重复 --node；跨分片不归并）
clickhouse-doctor --node shard-1@node-a:9000 --node shard-2@node-b:9000

# 只跑指定检查、机器可读输出
clickhouse-doctor --addr 127.0.0.1:9000 --check replica_status,disk_space_low --json

# 自定义阈值（0 用库默认值）
clickhouse-doctor --addr 127.0.0.1:9000 --parts-threshold 500 --disk-free-percent 20

# 慢集群（弱磁盘虚拟机等）：放宽预算，默认 2s 单节点预算必然超时
clickhouse-doctor --node shard-1@node-a:9000 --node shard-1@node-b:9000 \
  --timeout 120s --node-timeout 60s

# 查看检查清单 / 版本
clickhouse-doctor --list-checks
```

退出码：`0` 全部干净 · `1` 连接/调用错误 · `2` 至少一个检查 matched · `3` 覆盖不完整（需复跑）。完整 flag、阈值表与输出语义见 [clickhouse-doctor/README.md](clickhouse-doctor/README.md)。

### 对话式入口

```bash
clickhouse-doctor-chat \
  --node shard-1@node-a:9000 --node shard-2@node-b:9000 \
  --base-url https://llm.example.com/v1 --model <model> \
  --api-key sk-...   # 或环境变量 CLICKHOUSE_CHAT_API_KEY
```

LLM 为任意 OpenAI 兼容端点。判定逻辑仍全部来自核心库；模型回复标注"AI 解释"，以结构化报告为准。自测流程见 [clickhouse-doctor/TESTING.md](clickhouse-doctor/TESTING.md)。

## 开发

```bash
make check   # fmt + vet + test + 分层依赖断言
make race    # -race 全量测试

# 真实服务器验证（默认不访问外部集群，未设变量则跳过）
NO_PROXY=<集群 IP> \
  DIAGNOSE_IT_ADDR=<host>:9000 DIAGNOSE_IT_HTTP_ADDR=<host>:8123 \
  go test -count=1 -v ./integration

# 在隔离集群注入一次真实 NO_REPLICA_HAS_PART（需 Keeper；用 --cleanup 复原）
CH_ADDR=<host>:8123 CH_PASSWORD=... ZK_ADDR=<host>:9181 \
  python3 scripts/inject_no_replica_has_part.py

# 在隔离集群注入幽灵副本（active_replicas 缺口；用 --cleanup 复原）
CH_ADDR=<host>:8123 CH_PASSWORD=... ZK_ADDR=<host>:9181 \
  python3 scripts/inject_ghost_replica.py
```

`testdata/replication_queue/` 为合成回放案例（非真实现场）；`integration/` 为独立模块，含真实服务器契约测试与真实故障匹配测试（`real_fault_test.go`，需先用上面的脚本注入故障，否则自动 skip）。
