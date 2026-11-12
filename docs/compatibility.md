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
