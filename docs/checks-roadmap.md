# 后续检查候选清单

日期：2026-11-12。范围与首个检查一致：固定查询、证据与判定分离、不做修复、不依赖 LLM、
不发现拓扑。每项标注在现有测试集群（192.168.56.101，Keeper 在线）上的验证方式与难度。

优先级 = 运维价值 × 版本稳定性 × 本集群可验证性。

## P1（建议先做）

| # | 检查 | 状态 | 验证方式 | 难度 |
|---|---|---|---|---|
| 1 | `TooManyParts` | **✅ 已完成（2026-11-13）**：框架泛化 + 真机验证见 docs/compatibility.md | 循环小批量 INSERT 制造几百个 part，无需 Keeper | 低（纯阈值+聚合，最稳的表） |
| 2 | `ReplicaStatus` | **✅ 已完成（2026-11-13）**：真机验证见 docs/compatibility.md（readonly 来自真实病变表，active 缺口由 ghost znode 注入，脚本 `scripts/inject_ghost_replica.py`） | 停 Keeper 或断开会话触发 readonly/session_expired；延迟项造 active_replicas 差异 | 中（需要 Keeper 操控，注入脚本框架已有） |
| 3 | `MutationsStuck` | **✅ 已完成（2026-11-13）**：真机验证见 docs/compatibility.md（intDiv 除零构造失败 mutation，测试内联完成无需外部脚本） | 构造必然失败的 ALTER（如 MODIFY 不兼容类型），mutation 卡住并留下异常文本 | 低 |

## P2（有价值，第二批）

| # | 检查 | 检测什么 | 数据来源 | 真机验证方式 | 难度 |
|---|---|---|---|---|---|
| 4 | `ReplicationQueueBacklog` | **✅ 已完成（2026-11-13）**：服务端按 (库,表) 聚合 + 客户端三条阈值规则（深度/最老条目年龄/重试次数），真机验证见 docs/compatibility.md（故障分支复用 NO_REPLICA_HAS_PART 注入条目） | 已有注入手段；普通 get/merge 积压也可由停 Keeper 制造 | 低（复用度高） |
| 5 | `DiskSpaceLow` | 各磁盘/卷剩余空间与水位线 | `system.disks` | 本机磁盘可控，造小阈值即可触发 | 低 |
| 6 | `DetachedParts` | detached 目录中异常数量的 part（误操作、清理残留） | `system.detached_parts` | `DETACH PART` 即可制造 | 低 |

## 暂缓 / 需条件

| 检查 | 为什么暂缓 |
|---|---|
| `text_log` 错误扫描 | 依赖服务器开启 text_log 配置，不能假定存在；且错误噪音大，判定规则难做诚实 |
| Keeper 健康深查（znode 数、延迟） | 需要独立 Keeper 观测视角，库只有 SQL 通道，`system.zookeeper` 只能做浅探测 |
| S3 队列/对象异常 | 仅特定存储策略下有意义，等真实使用场景 |
| 分区/数据倾斜分析 | 属于分析类而非异常判定，与"证据→判定"契约不匹配 |

## 与第二个检查绑定的框架工作

第二个检查落地时强制要做的事（文档已预留）：

1. **检查框架泛化**：`Target/Querier/Options/Report` 的公共骨架从
   `ReplicationQueue*` 提升为通用接口，各检查只声明自己的固定 SQL、投影、
   归并函数与建议代码；首个检查作为第一个迁移对象，回归由现有测试兜底。
2. **结果契约扩展需谨慎**：`Execution/Outcome` 语义保持不变，新增字段必须
   有明确语义（不把阈值类检查硬塞进"异常匹配"模型——TooManyParts 是
   "超阈值"而非"队列记录"）。
3. **注入脚本目录化**：`scripts/` 按检查分目录，复用 Keeper/HTTP 公共函数。

## 建议顺序

~~`TooManyParts`~~（✅ 2026-11-13 完成，框架已泛化为 runner.go）→
~~`ReplicaStatus`~~（✅ 2026-11-13 完成，复用 Keeper 注入思路）→
~~`MutationsStuck`~~（✅ 2026-11-13 完成）→
~~`ReplicationQueueBacklog`~~（✅ 2026-11-13 完成，P2 首项收官）→ `DiskSpaceLow` → 其余。
