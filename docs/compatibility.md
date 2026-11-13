# 兼容性记录

本文件在 implementation-plan M3 完成过程中逐项填写：版本、协议、验证日期、通过与未覆盖项。此处未记录的版本不构成兼容性声明；单元测试与合成回放不能替代真实服务器验证。

## 已验证记录

### 2026-11-12 · ClickHouse 26.10.3.7（official build）· 192.168.56.101（本地隔离验证集群）

- 驱动：clickhouse-go v2.15.0（与 ckman 当前依赖一致）；Go 1.25.5。
- 验证方式：`integration/` 独立模块（`DIAGNOSE_IT_ADDR` 显式启用），全部通过；核心库 `go test -race`、`go vet` 通过。
- 空队列端到端：`CheckReplicationQueueFailures` 对空 `system.replication_queue` 返回 `complete + no_match + nil`（native）。
- 查询契约：`DESCRIBE system.replication_queue` 确认四个投影列存在；该版本 `last_exception` 为**非 Nullable String**；固定查询语法有效，投影列序正确，`positionCaseInsensitive` 可用。
- 采集机制（隔离仿表 `diagnose_it_verify.mimic_replication_queue`，镜像列结构）：跨目标归并、N+1 截断（读到第 N+1 条停止并标记 `Truncated`）、`Nullable(String)` NULL 行由服务端 WHERE 过滤（客户端 `RowsRead==RowsKept`）、读取中途取消返回 `timeout` 分类且保留已读证据。
- 权限路径：无权限用户查询返回可识别失败（`query_failed` + `ErrNoUsableObservation`，服务端 code 497），不会被误判为空结果。
- HTTP 协议：通过 database/sql + `http://` DSN 验证可用。

### 2026-11-13 · ClickHouse 26.10.3.7 · 同集群 · `TooManyParts` 检查

- 固定查询直接在真实 `system.parts` 上执行：`active` 过滤、`GROUP BY database, table, partition`、`toString(count())`/`toString(sum(bytes_on_disk))` 聚合与字符串列扫描全部可用。
- 真实碎片化场景（`integration/too_many_parts_test.go`）：非复制表 + `SYSTEM STOP MERGES <table>`（该语法存在，可按表停合并）+ 12 次单行 INSERT → 12 个独立 part，阈值 5 时返回 `complete + matched`，`Parts=12`，`DiskBytes` 非空，三条建议（合并吞吐/写入批次/分区键）齐全；阈值 2^30 时返回 `complete + no_match`。
- HTTP/database/sql 路径同样验证通过。
- 默认阈值 1000 与既有运维经验对齐（warning 档）；阈值是判定基线而非服务端硬限制。
- `SYSTEM STOP MERGES` 为会话/表级状态，测试通过 DROP DATABASE 清理；未验证跨版本该语法（更旧版本可能只支持全局）。

**真实 `NO_REPLICA_HAS_PART` 异常匹配（同一天，同一集群，Keeper 在线）：**

- 故障构造：`scripts/inject_no_replica_has_part.py` —— 同一 Keeper 路径挂两个副本（`r1`/`r2`），向复制日志追加一条 **merge** 条目，其源 part 在任何副本上都不存在；恢复回放后两个副本的队列条目都带着真实异常持续重试。
- 实测异常文本（`last_exception`，约 1986 字节，含完整堆栈）：

  ```
  Code: 234. DB::Exception: No active replica has part all_7_8_1 or covering part
  (cannot execute queue-0000000009: MERGE_PARTS with virtual parts [all_7_8_1]).
  (NO_REPLICA_HAS_PART), Stack trace (when copying this message, always include the lines below): ...
  ```

- 匹配结果：`complete + matched`，`integration/real_fault_test.go` 断言全部通过；真实文本（含堆栈）与 `positionCaseInsensitive` + `Contains` 的匹配逻辑一致，未出现截断或转义问题。
- 归并行为：两个目标指向同一服务器时，同一 `(分片, 库, 表, part)` 归并为一条匹配并保留**两个来源观测**，符合“同分片多副本合并但保留来源”的契约。
- 两种状态均已验证：注入后 `complete/matched`；`--cleanup` 后队列归零、测试自动 skip（不会把“没注入”误判为通过）。

**构造故障时观察到的服务器行为（写入此处的目的是避免后续重复踩坑）：**

1. 26.x 的复制日志条目是**多行 key-value 格式**（`format version: 4\n...\nget\n<part>\npart_type: ...`），不是旧版 TSV；merge 条目形如 `merge\n<源part...>\ninto\n<新part>\ndeduplicate: 0\npart_type: Compact`。
2. 日志节点名必须是**顺序数字**（`log-%010d`）。写入非数字后缀会让副本读日志直接失败（`Cannot parse input: expected 'eof' before: '...'`，`system.replicas` 查询同样报错），删除该节点后恢复。
3. `get` 一个任何副本都没有的 part **不会**产生 `NO_REPLICA_HAS_PART`：副本会把它判为过期条目，改写为自己的 `drop` 条目并静默跳过队列。需要 merge 类条目才会走 fetch 路径并重试报错。
4. 没有按表的 `SYSTEM STOP REPLICATION QUEUE`，只有全局 `SYSTEM STOP REPLICATION QUEUES`；队列暂停期间 `OPTIMIZE ... FINAL` 会阻塞（合并任务进不来），恢复队列后才完成。

**已知匹配风险（未在 v0 处理）：** 匹配依赖异常文本中出现符号名 `NO_REPLICA_HAS_PART`。26.x 会带上该符号名，但更旧版本只打印自然语言（如 `No replica has part covering <part>`）。若目标版本省略符号名，当前规则会漏报 —— 接入新的服务器版本时必须重新验证这一条，不能假定匹配成立。

### 2026-11-13 · ClickHouse 26.10.3.7 · 同集群 · `ReplicaStatus` 检查

- 固定查询直接在真实 `system.replicas` 上执行：八个投影列（含 `is_readonly`/`is_session_expired`/`absolute_delay`/`total_replicas`/`active_replicas`/`lost_part_count`）在该版本全部存在，`toString` 字符串列扫描可用。
- **真实 readonly 病变表**：集群上有一张 Keeper 路径失效的残留复制表（`is_readonly=1`、`total=0`、`active=0`、`absolute_delay≈1.79e9`），检查如实报出并给出 `replica_readonly` 理由码；`absolute_delay` 的天文数字证实了"长期无写入/无 Keeper 数据的表延迟值不可信"的告警，已写入 godoc 语义说明。
- **真实 active 缺口**（`scripts/inject_ghost_replica.py`）：同一 Keeper 路径的手工 ghost znode 让 `total=2, active=1`，检查报出 `active_replica_deficit` 并给定位建议；`--cleanup` 后恢复健康分支（`no_match`），两种状态均已验证。
- 注入期间的实测服务器行为（避免重复踩坑）：
  1. 判定副本是否活跃看的是 **host 可达性/会话信息**，不是 znode 存在性：克隆真实副本的全部子节点（含 `is_active` 会话标记、指向本机真实 9009 端口的 host）会被判为 **active**，缺口不出现；ghost 只留 `host` 子节点且指向不可达地址（192.0.2.1）才稳定判为不活跃。
  2. ghost 只需 `replicas/<name>/` + `host` 子节点即可计入 `total_replicas`。
  3. `DROP TABLE` 对副本 znode 的清理是异步收尾的，紧接 CREATE 会撞 `REPLICA_ALREADY_EXISTS`（HTTP 400 与驱动报错都会出现）；集成测试用唯一副本名规避，注入脚本先递归清空 Keeper 路径再建表。
  4. 测试进程被强杀时 Keeper 上会留下孤儿副本 znode（无表无会话），表现为该表 `total > active`——恰好是真实故障形态，集成测试按服务端事实分支断言两种状态。
- 单副本表 `total=active=1` 不产生缺口类匹配，为诚实结果（godoc 已注明）。
- HTTP/database/sql 路径同样验证通过。
- Keeper 客户端端口（9181）的 HTTP 命令接口与四字命令在本环境均不可用（空响应/断连），Keeper 操作统一走 kazoo。

### 2026-11-13 · ClickHouse 26.10.3.7 · 同集群 · `MutationsStuck` 检查

- 固定查询直接在真实 `system.mutations` 上执行：列集（`database/table/mutation_id/command/create_time/is_done/latest_fail_reason`，另含 26.x 新增的 `latest_fail_error_code_name`、`is_killed`、`parts_to_do` 等未使用）确认存在。
- **固定查询陷阱（真机实测）**：`toString(is_done) AS is_done` 会让 `WHERE NOT is_done` 里的列名被 SELECT 别名遮蔽成 String，报 `Illegal type (String) of argument of function not`（code 43）。别名必须改用其他名字（本实现用 `done`）。
- **真实失败 mutation 构造**（`integration/mutations_test.go` 内联完成，无需外部脚本）：
  - `ALTER ... UPDATE b = toString(intDiv(a, 0)) WHERE 1` → mutation 卡住：`is_done=0`、`latest_fail_reason` 带完整堆栈（`ILLEGAL_DIVISION`），1 秒内出现，持续重试；
  - **`a / 0` 不是除零异常**：`/` 是 IEEE 浮点除，Int64/0 = inf，`toString(inf)` 得 `'nan'`，mutation "成功"执行——必须用 `intDiv`；
  - **ORDER BY 键列不允许 UPDATE**：提交期直接报 `CANNOT_UPDATE_COLUMN`（code 420），mutation 不产生——构造失败 mutation 必须作用于非键列。
- 匹配结果：`complete + matched`，理由码 `mutation_failing`，原始命令与失败全文保留。
- 清理路径：`KILL MUTATION ... SYNC` 后未完成条目归零（killed 条目移出未完成集合），检查回到 `complete/no_match`；`DROP DATABASE` 兜底。
- 年轻且无失败文本的条目"只读不报"语义由服务端 `WHERE NOT is_done` + 客户端阈值保证；"未完成超龄"规则默认阈值 86400 秒，真机无长任务窗口可稳定捕捉，由单测覆盖。
- HTTP/database/sql 路径同样验证通过。

### 2026-11-13 · ClickHouse 26.10.3.7 · 同集群 · `ReplicationQueueBacklog` 检查

- 固定查询直接在真实 `system.replication_queue` 上执行（按 库/表 服务端聚合）：列集确认只依赖最古老列 `database/table/create_time/num_tries/last_exception`。**26.10 上没有 `postponed_due_to_error`、`is_fetching` 等列**——印证了"只用古老列"策略的必要性，这些诊断列一律不进固定查询。
- `dateDiff('second', min(create_time), now())` 服务端计算最老条目年龄可用；空队列返回空结果集（无行，非报错）。
- 健康路径：单副本复制表插入后队列秒级清空，`complete/no_match`；HTTP/database/sql 路径同样通过。
- 故障路径：复用 `scripts/inject_no_replica_has_part.py` 注入的失败 merge 条目（该条目持续重试、常驻队列），收紧阈值（depth≥1、age≥1s、重试规则关闭）后故障表被报出且深度理由命中；报出的匹配集合与服务端 `GROUP BY` 聚合事实逐项一致。默认阈值下的健康判定（depth<100、age<1h）对单条在途故障条目正确保持静默。
- 集成测试的 Keeper 夹具路径按运行时刻唯一：`DROP TABLE` 对副本 znode 的清理是异步收尾（ReplicaStatus 阶段已实测），固定路径重跑会撞 `REPLICA_ALREADY_EXISTS` 或吃到上一轮残留的 log 条目；路径唯一从根上规避，留下的空 znode 目录无害。

### 2026-11-13 · ClickHouse 26.10.3.7 · 同集群 · `DiskSpaceLow` 检查

- 固定查询直接在真实 `system.disks` 上执行：26.10 列集含 `name/path/free_space/total_space/unreserved_space/keep_free_space` 及较新的 `type/object_storage_type/is_encrypted/is_read_only/is_write_once/is_remote/is_broken/cache_path`。本检查只用前五个中的古老列（`unreserved_space` 与 `is_broken` 不进查询，旧版本没有）。
- 真机水位：`default` 盘 47.96GB / 55.04GB ≈ 87% 空闲，`keep_free_space=0`；默认阈值（10%）`complete/no_match`，阈值提到 100% 时每块 `total>0` 的磁盘命中且客户端算出的占比与服务端事实逐块一致（`integration/disk_space_test.go` 以服务端 `system.disks` 为期望集合，杜绝测试与实现同源）。
- 判定语义：占比按 `free*100/total` 向下取整、`<=` 阈值命中；`total_space=0` 的磁盘占比记 -1、不参与占比规则；`keep_free_space > 0 且 free < keep_free_space` 单独命中"保留量击穿"（与占比无关）。
- **真机无法制造真实磁盘满**：本机磁盘未做填充实验（写满会影响集群与其他测试），"真实低水位"路径未在真机复现，由阈值 100 的判定链路验证 + 单测边界覆盖；接入现场前建议在隔离环境做一次真实水位验证。

### 2026-11-13 · ClickHouse 26.10.3.7 · 同集群 · `DetachedParts` 检查

- 固定查询直接在真实 `system.detached_parts` 上执行：26.10 列集 `database/table/partition_id/name/bytes_on_disk/modification_time/disk/path/reason/min_block_number/max_block_number/level`，本检查只用其中古老列。真机上该表已有 60 条 `system` 库日志表的 `broken-on-start` 残留——**默认排除 system/information_schema 是必要的**，否则用户库检查被环境噪音淹没。
- **reason 字段实测语义**：用户 `ALTER TABLE ... DETACH PART` 后 reason 为**空字符串（非 NULL）**（`isNull(reason)=0`），与官方文档一致——reason 非空即服务端自行下线。判定规则据此设计：`broken` 前缀 → 损坏；非空非 broken → 服务端自行下线；空 → 用户动作，只参与数量阈值。
- **驱动层 Nullable 限制（真机实测，v2.15.0）**：`system.detached_parts.reason` 是 `Nullable(String)`，native 路径可直接 `Scan` 进 `sql.NullString`，但 **database/sql（HTTP）路径会报 `unsupported Scan, storing driver.Value type *string into type *string`**——该驱动对 Nullable 列下发的 driver.Value 是 `*string`。修法是服务端 `coalesce(reason, '')`：NULL 的语义与空串完全一致（都是用户 detach），不损失事实。此前各检查未踩坑是因为没有直接扫描 Nullable 列（backlog 的 `last_exception` 走了服务端聚合）。
- 真机验证：fixture 表 `DETACH PART` 后默认阈值 `complete/no_match`（单条空 reason 用户 detach 不触发），阈值 1 时 `complete/matched` 且观测集合与服务端 `system.detached_parts` 逐条一致（非同源核对）；HTTP/database/sql 路径通过；cleanup 先 `ATTACH DETACHED` 再 DROP，无 detached 目录残留。
- **环境 flake（真机实测）**：该环境的 INSERT 偶发走 async insert 且 `Wait for async insert timeout (120000 ms) exceeded`——多个测试快速建表/插数/删表循环时后台异步插入池偶发饿死。fixture 的 INSERT 显式 `SETTINGS async_insert = 0, wait_for_async_insert = 0` 后连续 5 轮全量集成稳定通过。
- `broken`/服务端自行下线的 part 未在用户库真机复现（system 库的 broken-on-start 残留被默认排除；用户表制造损坏 part 需写坏文件，不做）——两条逐 part 理由规则由单测覆盖判定链路，真机验证了空 reason 语义与数量规则。

### 2026-11-13 · v0.2.0 收尾复核

- **fixture 秒级时间戳撞车（真机实测）**：`count=3` 连跑时 native 与 HTTP 两个测试在同一秒内各自建 ReplicatedMergeTree fixture，zkBase/副本名由 `time.Now().Unix()` 生成而秒级撞车，`CREATE TABLE` 报 code 253 `REPLICA_ALREADY_EXISTS`（Keeper znode 由 DROP 异步收尾，加重复现概率）。detached_parts/backlog/replica_status 三处 fixture 统一改为 `UnixNano()`。
- 全量复核：`gofmt`/`go vet`/`go test ./...`/`go test -race ./...`（核心库）与真机集成测试（native + HTTP）通过。
- **`TestCheckCancelTinyBudget` 时序 flake（真机实测）**：50000 行 + 200ms 总预算 / 100ms 节点预算下，出现第三种契约内合法结果——节点级预算先耗尽、有效证据已保留、runner 在总预算内正常收尾，报告为 **partial/matched 且顶层 error 为 nil**（对应契约矩阵"部分失败且有有效匹配"行）。原测试只断言了 complete/matched 或 DeadlineExceeded 两个分支，已补上 partial/matched 分支（并校验节点 ErrCategory 属于超时允许分类）。教训：**小预算取消测试必须覆盖"节点超时 + 证据保留 + nil error"这一契约分支，机器负载波动会触发它**。


**v2.15.0 驱动实测限制（对 ckman 接入重要）：**

1. `clickhouse-go v2.15.0` 的 `ScanRow` 不支持 `*any` 目标（"converting String to *interface {} is unsupported"）——核心库因此改用 `*sql.NullString`（两条路径都支持，见 query.go）。
2. `Options.Protocol = HTTP` 在 `Open()` 直连路径下**不被消费**（`dial()` 只走 native 握手，收到 HTTP 响应报 "unexpected packet [72]"）；HTTP 仅在 DSN scheme（`http://...`）或 database/sql 路径生效。ckman 若走 HTTP 必须用 database/sql 或 DSN 方式。

**测试环境注意事项：**

- HTTP 请求走 `http.ProxyFromEnvironment`：本机设置了 `HTTP_PROXY` 时必须把集群地址加入 `NO_PROXY`，否则代理会改写请求（服务端报 "Conflicting database specification: 'http:' in URL path"）。
- `/tmp` 为小容量 tmpfs 时需设置 `GOTMPDIR`。

## 未覆盖项

- [ ] ckman 首发环境的服务器版本（以 ckman 实际使用版本为准，不预先承诺宽范围）
- [x] 隔离复制表／Keeper 环境下的真实 `NO_REPLICA_HAS_PART` 异常匹配 —— 已完成（见上，可复现来源为 `scripts/inject_no_replica_has_part.py`，非生产故障）
- [ ] 更旧服务器版本的列集与 `positionCaseInsensitive` 可用性
- [ ] 更旧服务器版本的异常文本是否包含 `NO_REPLICA_HAS_PART` 符号名（见“已知匹配风险”）
- [ ] 会话设置（result_overflow_mode 等）对结果完整性的影响
- [ ] HTTP 直连路径（v2.15 `Options.Protocol` 字段失效，见上）在 ckman 实际封装中的等价验证
- [ ] 多分片／多服务器拓扑下的真实跨节点归并（本次为单机双副本）
- [ ] `TooManyParts` 固定查询在更旧服务器版本的列集与 `SYSTEM STOP MERGES` 按表语法（26.10 已验证）
- [ ] `ReplicaStatus` 的 readonly/session_expired 在**可操控 Keeper** 环境下的主动复现（停 Keeper/断会话；本次 readonly 证据来自真实病变残留表，active 缺口由 ghost znode 构造）
- [ ] `ReplicaStatus` 固定查询在更旧服务器版本的 `system.replicas` 列集（26.10 已验证）
- [ ] `MutationsStuck` 固定查询在更旧服务器版本的 `system.mutations` 列集与 `dateDiff('second', ...)` 可用性（26.10 已验证；所选列均为古老列，预期旧版本可用，未实测）
- [ ] `ReplicationQueueBacklog` 固定查询在更旧服务器版本的 `system.replication_queue` 列集与 `countIf`/`dateDiff('second', min(create_time), now())` 可用性（26.10 已验证；所选列均为古老列，预期旧版本可用，未实测）
- [ ] `DiskSpaceLow` 真实低水位/保留量击穿场景（本机未做磁盘填充；判定链路由阈值 100 与服务端事实比对验证，边界由单测覆盖），以及更旧服务器版本的 `system.disks` 列集
- [ ] `DetachedParts` 的 `broken`/服务端自行下线 part 在用户库的真实复现（需写坏文件，未做；空 reason 语义与数量规则已真机验证，broken 规则由单测覆盖），以及更旧服务器版本的 `system.detached_parts` 列集与 `coalesce` 可用性
- [ ] clickhouse-go database/sql 对 Nullable 列的 Scan 限制在其他 Nullable 列/其他驱动版本的复验（见 DetachedParts 一节；当前以服务端 coalesce 规避）
