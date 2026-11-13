// Package diagnose 提供基于固定查询的 ClickHouse 独立诊断检查。
//
// # 当前范围
//
// 当前实现四个检查，共用同一套采集调度（固定 worker 数、总/单节点预算、
// N+1 截断检测）与结果契约：
//
//   - [CheckReplicationQueueFailures]：识别复制队列（system.replication_queue）
//     中记录的 NO_REPLICA_HAS_PART 异常，按 (分片, 库, 表, part) 归并；
//   - [CheckTooManyParts]：识别活跃 part 数超过阈值的表分区
//     （system.parts 按 (分片, 库, 表, 分区) 聚合），属阈值判定而非异常匹配；
//   - [CheckReplicaStatus]：识别复制表的副本异常（只读、会话过期、
//     active 副本缺口、延迟超阈值、丢件记录），数据源 system.replicas，
//     每条观测附带稳定理由码（Reason* 常量）；
//   - [CheckMutationsStuck]：识别卡住的 mutation（失败重试、未完成超龄），
//     数据源 system.mutations，已完成条目由固定查询在服务端过滤。
//
// 结果只陈述"观测到了什么"，不推断数据是否永久丢失、故障起因或能否自愈。
// 匹配/超阈值不等于确认需要修复；无匹配不等于集群健康。库不执行修复，
// 不发现拓扑、不管理凭据、不管理连接生命周期，只关闭自己取得的 Rows。
//
// # 结果契约
//
// [ReplicationQueueReport] 的 Execution 与 Outcome 分离：
//
//	Execution      Outcome      含义
//	complete       matched      已完整覆盖的范围内发现异常
//	complete       no_match     已完整覆盖的范围内未见异常；不代表集群健康
//	partial        matched      异常成立，但部分节点未覆盖或结果被截断，清单可能不全
//	partial        unknown      有有效证据产出但覆盖不完整，无法给出匹配判断
//	failed         unknown      所有目标都失败，没有可用观测
//
// 展示"partial + matched"时必须同时呈现覆盖缺口（见 NextSteps），不得把
// 部分结果渲染成完整清单，也不得把 unknown 渲染为健康或正常。
//
// 时间戳是各节点各自的采集时刻，不是跨节点原子快照。即使顶层返回 error
// （取消、超时、[ErrNoUsableObservation]），报告中已保留的有效证据仍然成立，
// 消费端应先处理报告再处理错误。
//
// # 错误契约
//
//   - 输入不合法：返回包装 [ErrInvalidInput] 的错误，不执行任何查询，报告为零值；
//   - 所有目标无有效观测：返回报告（Execution=failed）与 [ErrNoUsableObservation]；
//   - 取消或总预算耗尽：返回报告与对应 context 错误，已获得的证据保留。
//
// 单节点的失败细节在 [NodeResult].ErrCategory / ErrMessage 中，使用稳定
// 类别常量（ErrCat*）而非错误类型断言；ErrMessage 可能包含服务端文本，
// UI 必须按文本转义展示。
//
// # 适配器约定
//
// [Querier] 与 [Rows] 是最小接口。集群发现、凭据、TLS 与连接生命周期由
// 调用方管理；库只调用 Query，并对返回的 Rows 调用 Close。
//
//   - Rows.Scan 必须支持 *sql.NullString：检查以 NullString 读取四个字符串
//     投影列。clickhouse-go v2 的 ScanRow 不支持 *any 目标，NullString 在
//     clickhouse-go v2 与 database/sql 两条路径上都受支持；
//   - Next 必须遵守传入 Query 的 context，取消后尽快返回 false；适配器必须
//     真实传递 context，不能用后台 goroutine 提前返回来伪装取消；
//   - 迭代过程或结束时的错误通过 Err 报告，不得静默结束；Close 必须幂等，
//     且不得关闭调用方的共享连接；
//   - 注意 Go 类型系统限制：返回 driver.Rows 或 sql.Rows 的方法不会自动满足
//     [Rows] 接口，各路径需要薄适配器（database/sql 的 *sql.Rows 除外，
//     它的 Next/Scan/Err/Close 方法签名恰好匹配）。
//
// # 默认预算
//
// [ReplicationQueueOptions] 零值字段使用默认值：总预算 10s，单节点 2s，
// 并发 4，单节点保留上限 1000 行（超出即截断并标记，不静默丢弃）。
//
// # 版本兼容
//
// 固定查询在 ClickHouse 26.10.3.7（native + HTTP/database/sql）上通过真机
// 验证，含真实 NO_REPLICA_HAS_PART 队列条目匹配。异常匹配依赖异常文本包含
// 符号名 NO_REPLICA_HAS_PART；更旧版本可能只打印自然语言描述，换服务器
// 版本前须复核。已验证版本与未覆盖项见仓库 docs/compatibility.md。
//
// 依赖方向：消费端 -> 本库 -> 查询适配器 / clickhouse-go。
package diagnose
