# 更新日志

格式参考 Keep a Changelog；版本遵循语义化版本。

## [未发布]

### 变更

- **模块重命名**：消费端模块目录 `doctor/` → `clickhouse-doctor/`，module 路径 `github.com/YenchangChan/clickhouse-go-diagnose/doctor` → `.../clickhouse-doctor`（避免与 nova 自带的 `doctor` 安装自检包概念混淆）。建连与目标解析抽为 `clickhouse-doctor/targets` 包，供两个入口共用。

### 新增

- **`clickhouse-doctor-chat`（Agent 入口，可选）**：`clickhouse-doctor/chat/` 基于 nova v0.4.1 库嵌入形态，对话式诊断解释与按需触发检查。工具面只有 `list_checks`/`run_check` 两个只读工具（分别映射核心库 `ListChecks`/`RunCheck`），targets 会话级固定，Agent 结构上无法执行 SQL 或修复；每轮回复标注"AI 解释"，system prompt 禁止生成修复指令。LLM 为任意 OpenAI 兼容端点（`--base-url`/`--model` + `CLICKHOUSE_CHAT_API_KEY`）。分层断言：轻量 CLI 依赖闭包不含 nova（测试强制）；chat 亦不引入 nova 的 charm TUI（nova 核心本就无 UI 依赖）。引入 nova 后本模块构建要求 Go 1.26.6+。

### 新增

- 统一检查入口（Agent-friendly，不引入任何 Agent 概念）：`ListChecks() []CheckMeta` 枚举全部检查（稳定 ID、描述、RuleVersion、阈值槽位声明 `ThresholdSpec`）；`RunCheck(ctx, id, targets, CheckOptions)` 按执行；`Report`/`ReportHeader` 是各检查报告的公共视图（Execution/Outcome/Targets/NextSteps），类型各异的 Matches 仍在具体报告类型上，类型断言取用。`CheckOptions.Thresholds` 按槽位序透传，缺省用库默认值；未知 ID/槽位溢出/负值统一报 `ErrInvalidInput` 且返回 nil 报告。判定严重级别/告警档位不属于库的职责——库只产出证据与稳定理由码，由消费端（LLM/人）基于证据判断。
- `IsInvalidInput(err)` 辅助函数。
- `clickhouse-doctor` 改为统一入口的表驱动消费端：`--list-checks` 直接来自核心库 `ListChecks()`（输出 ID + 描述）；检查清单不再在 doctor 硬编码；`runCheck` 只保留 CLI 阈值到槽位的对位与 Matches 渲染转换两个注册点。

### 变更

- `replica_status` 新增判定规则与理由码 `keeper_unreachable`（`total_replicas=0`，物理不可能值，标识节点读不到 Keeper 副本清单；2026-11-14 真实集群 Keeper 会话丢失窗口实测发现，见 docs/compatibility.md）。RuleVersion `1` → `2`。
- `clickhouse-doctor` 预算与编排 flag：`--max-rows-per-node`（单节点保留上限，默认库默认 1000；detached part 海量场景需调大避免截断）、`--parallel-checks`（默认关闭；串行对慢集群更温和）。doctor/README.md 新增"预算与编排"一节与慢集群实测参考值（弱磁盘虚拟机上默认 2s 单节点预算必然超时，建议 `--node-timeout 60s`）。

### 变更

- RuleVersion 格式统一为 `"1"`/`"2"` 整数递增：`replication_queue_failures` 的 `"v0.1"` 改为 `"1"`，常量重命名为 `replicationQueueCheckID`/`replicationQueueRuleVersion`（与其他检查命名一致）。机器消费方若按字符串解析版本号需同步。
- doctor 移除 `clickhouse-go` 的重复 import（普通导入一行同时完成驱动注册）。

- `clickhouse-doctor` CLI（`doctor/` 独立 Go 模块）：命令行消费端，对指定节点运行 7 个检查并渲染报告。native 与 HTTP/database/sql 两条路径（可按节点混合）；`--node` 可重复传参（`shard-id@addr` / `shard-id@node-id@addr`，跨分片不归并）；`--json` 稳定输出契约（doctor 侧自有视图类型，核心库结构不变）；文本输出遵循结果契约纪律（partial/unknown 显式呈现、截断与节点失败可见、先证据后错误）。退出码：0 全部干净 / 1 调用连接错误 / 2 有匹配 / 3 覆盖不完整。真机验证：单节点全检查（replica_status 命中真实 readonly 病变表，exit 2）、同机双副本双目标（跨分片各一条匹配）、混合协议、JSON、连接失败路径。
- `clickhouse-doctor` 检查阈值 flag：8 个可选阈值（`--parts-threshold`、`--replica-delay-seconds`、`--queue-depth-threshold`、`--queue-age-seconds`、`--queue-retries-threshold`、`--mutation-age-seconds`、`--disk-free-percent`、`--detached-count-threshold`），0 用库默认值、负数解析层拒绝。真机验证：3-part 夹具下低阈值 matched / 默认阈值 no_match / 负值拒绝。

## [v0.2.0] - 2026-11-13

v0.1.0 之后新增六个独立检查，并将首检查的采集调度泛化为通用框架（runner.go）。
全部检查在 ClickHouse 26.10.3.7（native + HTTP/database/sql）真机验证，含故障注入与恢复回归，见 docs/compatibility.md。

### 新增

- `CheckTooManyParts`：单分区活跃 part 数超阈值（system.parts 按 (库, 表, 分区) 聚合，阈值判定）；同时把 `Target/Querier/Options/Report` 公共骨架从 ReplicationQueue 提升为通用检查框架（worker 池、总/单节点预算、N+1 截断检测）。
- `CheckReplicaStatus`：副本只读、会话过期、active 副本缺口、延迟超阈值（默认 300s）、丢件记录（system.replicas）。
- `CheckMutationsStuck`：mutation 失败重试（latest_fail_reason 非空）与未完成超龄（默认 86400s）（system.mutations，服务端过滤 is_done）。
- `CheckReplicationQueueBacklog`：复制队列积压深度（默认 100）、最老条目年龄（默认 3600s）、重试次数（默认 10）三条阈值规则（system.replication_queue 服务端按 (库, 表) 聚合）。
- `CheckDiskSpaceLow`：磁盘剩余占比低于水位（默认 10%）与 keep_free_space 保留量击穿两条规则（system.disks）。
- `CheckDetachedParts`：detached part 服务端判定损坏（broken 前缀）、服务端自行下线（非空 reason）、单表数量超阈值（默认 10）（system.detached_parts，默认排除 system/information_schema，所有行保留为证据）。
- 注入脚本：`scripts/inject_no_replica_has_part.py`（真实队列条目）、`scripts/inject_ghost_replica.py`（幽灵副本制造 active 缺口），均可 `--cleanup` 复原。

### 测试

- 单元测试合计 121 个，`go test -race ./...`、`go vet`、`gofmt` 全部通过。
- 真机集成测试覆盖全部七个检查（健康/故障两态分支断言 + HTTP 冒烟）。

## [v0.1.0] - 2026-11-12

- 首个检查 `CheckReplicationQueueFailures`：识别复制队列（system.replication_queue）中记录的 `NO_REPLICA_HAS_PART` 异常，按 (分片, 库, 表, part) 归并。
- 结果契约（Execution/Outcome 分离）、错误契约（ErrInvalidInput/ErrNoUsableObservation、ErrCat* 稳定错误类别）、Querier/Rows 最小适配器接口、默认预算（总 10s / 单节点 2s / 并发 4 / 单节点保留 1000 行 + N+1 截断检测）。
- 合成回放案例 `testdata/replication_queue/`；独立集成模块 `integration/`（clickhouse-go 不进入核心依赖，核心库仅依赖标准库）。
- 真机验证 ClickHouse 26.10.3.7，含真实 NO_REPLICA_HAS_PART 队列条目匹配。
