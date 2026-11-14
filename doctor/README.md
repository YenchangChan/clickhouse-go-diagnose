# clickhouse-doctor

`clickhouse-go-diagnose` 库的命令行消费端：对指定节点运行诊断检查并渲染报告。
只做编排与渲染——判定逻辑全部在核心库，本工具不修复、不引入 LLM。

独立 Go 模块（`replace` 指向核心库），`clickhouse-go` 依赖只出现在这里，
核心库保持仅依赖标准库。

## 使用

```bash
# native 协议，全部 7 个检查
clickhouse-doctor --addr 127.0.0.1:9000 --password '...'

# 多节点（可重复 --node；跨分片不归并，同主机多实例须显式区分 node-id）
clickhouse-doctor --node shard-1@node-a:9000 --node shard-2@node-b:9000 --password '...'

# 同主机双副本：三段式 shard@node-id@addr
clickhouse-doctor --node shard-1@r1@127.0.0.1:9000 --node shard-2@r2@127.0.0.1:9000 --password '...'

# 混合协议：地址写 http:// 前缀的节点走 HTTP/database/sql 路径
clickhouse-doctor --node shard-1@node-a:9000 --node shard-2@http://node-b:8123 --password '...'

# HTTP/database/sql 路径（单节点兼容形式）
clickhouse-doctor --http 127.0.0.1:8123 --password '...'

# 指定检查、机器可读输出
clickhouse-doctor --addr 127.0.0.1:9000 --check replica_status,disk_space_low --json

# 自定义阈值（0 表示用库默认值）
clickhouse-doctor --addr 127.0.0.1:9000 --parts-threshold 500 --disk-free-percent 20

# 慢集群（弱磁盘虚拟机等）：放宽单节点预算与保留上限
clickhouse-doctor --node shard-1@node-a:9000 --node shard-1@node-b:9000 \
  --timeout 120s --node-timeout 60s --max-rows-per-node 5000

# 查看检查清单 / 版本
clickhouse-doctor --list-checks
clickhouse-doctor --version
```

密码优先级：`--password` > 环境变量 `CLICKHOUSE_DOCTOR_PASSWORD`。

连接策略是全有或全无：任一节点建连失败则整体退出（exit 1，错误指明节点），
不会静默只跑部分节点后渲染成完整结果。

## 检查阈值

全部可选，负数在解析层拒绝；`0` 使用核心库默认值。

| flag | 检查 | 库默认 |
|---|---|---|
| `--replica-delay-seconds` | replica_status：副本 absolute_delay（秒） | 300 |
| `--parts-threshold` | too_many_parts：单分区活跃 part 数 | 1000 |
| `--queue-depth-threshold` | replication_queue_backlog：单表在队条目数 | 100 |
| `--queue-age-seconds` | replication_queue_backlog：最老条目年龄（秒） | 3600 |
| `--queue-retries-threshold` | replication_queue_backlog：最大重试次数 | 10 |
| `--mutation-age-seconds` | mutations_stuck：mutation 运行时长（秒） | 86400 |
| `--disk-free-percent` | disk_space_low：磁盘剩余空间百分比 | 10 |
| `--detached-count-threshold` | detached_parts：单表 detached part 数 | 10 |

阈值只影响判定，不影响采集：证据行始终完整收集，理由码由核心库生成。

## 预算与编排

| flag | 默认 | 说明 |
|---|---|---|
| `--timeout` | 10s | 单次检查总预算 |
| `--node-timeout` | 2s | 单节点查询与读取预算 |
| `--max-rows-per-node` | 库默认 1000 | 单节点保留条目上限（读 N+1 条即截断） |
| `--parallel-checks` | 关 | 并发运行多个检查，输出顺序不变 |

慢集群（弱磁盘虚拟机、高负载节点）实测参考：`system.detached_parts` 单表
`count()` 可达分钟级，`system.replicas` 在 Keeper 会话丢失窗口内会长时间
无响应——默认 2s 单节点预算必然超时（表现为全部节点 `timeout`，
Execution=failed/unknown）。此类集群建议 `--node-timeout 60s --timeout 120s`；
detached part 海量（数千以上）时配合 `--max-rows-per-node` 调大，避免截断。

`--parallel-checks` 把多个检查并发打向同一批节点（总并发 ≈ 检查数 × 单检查
worker 数）。串行（默认）对慢集群更温和，也更容易区分"节点慢"与"节点挂"；
快集群想缩短整体耗时再开。

## 输出语义（遵循核心库契约）

- `Execution`（complete/partial/failed）与 `Outcome`（matched/no_match/unknown）
  分离；`matched` 可能因覆盖缺口而不全，`no_match` 不等于集群健康。
- 即使顶层返回 error，报告中已保留的证据仍然成立；文本输出先渲染报告再呈现错误。
- 节点失败、未启动、截断（TRUNCATED）都会显式呈现，不会渲染成绿色健康。
- 服务端异常文本（last_exception、命令、错误信息）可能包含敏感字面量，
  文本输出每条截断至 160 字符，完整内容以 `--json` 输出为准。

## 退出码

| 码 | 含义 |
|---|---|
| 0 | 全部检查 complete + no_match |
| 1 | 调用或连接错误（flag 非法、无法连接等） |
| 2 | 至少一个检查 matched（可结合 CI/告警） |
| 3 | 至少一个检查覆盖不完整（unknown / failed / partial+unknown），需要复跑或补覆盖 |

## 当前边界

- 多节点：`--node` 每次一个目标，跨分片不归并（核心库契约）；NodeID 默认取地址，
  同主机多实例用三段式显式区分。
- 原始路线图中的 CPU / 慢查询 / 内存检查属核心库新检查（依赖
  `query_log`/`metric_log`），不在本 CLI 的范围内。
