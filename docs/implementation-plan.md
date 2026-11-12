# clickhouse-go-diagnose v0 实现计划

日期：2026-11-12  
状态：M1（纯函数核心）、M2（采集与预算）、M3（真实查询与真实异常匹配）已完成，见 docs/compatibility.md；M0 真实案例、M4 ckman 接入未开始。  
目标：完成一个独立诊断检查，并在 ckman 中展示可追溯的异常和检查覆盖情况。

## 1. 交付目标与边界

首个入口为 `CheckReplicationQueueFailures`。v0 只回答：指定节点的复制队列是否记录了 `NO_REPLICA_HAS_PART`，涉及哪些对象，以及本次检查遗漏了哪些范围。

交付包括独立 Go 库、一个 ckman 查询适配器、结构化结果、案例回放测试、真实 ClickHouse 查询验证及 ckman 手动检查入口。没有 CLI、注册表、调度器、修复器、LLM 或通用 SQL 执行服务。

异常匹配不等于确认永久数据丢失；无匹配不等于集群健康。库不发现拓扑、不管理凭据、不关闭调用方连接，只关闭自己取得的 Rows。

方案背景见 [ai-diagnostics.md](ai-diagnostics.md)。

## 2. 已核对的本地实现

| 来源 | 观察 | 实施影响 |
|---|---|---|
| 参考实现的丢失 part 扫描 | 查询可注入，跨副本归并，返回部分结果与错误 | 复用思路和测试场景，不复制平台依赖或修复假设 |
| 参考实现的扫描测试 | 同分片归并、跨分片隔离、节点失败仍保留结果 | 作为第一批回归场景 |
| 参考实现的节点覆盖处理 | 部分覆盖信息目前进入展示文本 | 新库改为独立结构字段 |
| [ckman 连接封装](../../eoitek/ckman/common/ck_cli.go) | Query 无调用方 context，Rows 未暴露 Err | 增量补 QueryContext 与 Rows.Err，再包装成库接口 |
| [ckman 连接创建](../../eoitek/ckman/common/ck.go) | native 与 HTTP 两条路径；连接创建有 Ping | 查询取消和建连／Ping 预算分别验证 |
| [ckman go.mod](../../eoitek/ckman/go.mod) | Go 1.24，clickhouse-go/v2 v2.15.0 | 先兼容现有消费端，不以驱动升级作为前置条件 |

本节是抽取前只做代码阅读时的观察记录。真实案例快照仍未取得；服务器版本与复跑验证见 [compatibility.md](compatibility.md)。

## 3. v0 接口草案

根包使用 `diagnose`，仅依赖标准库。以一个具体入口开始，不引入通用 Check 接口。

```go
type Rows interface {
    Next() bool
    Scan(dest ...any) error
    Err() error
    Close() error
}

type Querier interface {
    Query(ctx context.Context, query string, args ...any) (Rows, error)
}

type Target struct {
    NodeID  string
    ShardID string
    Querier Querier
}

type ReplicationQueueOptions struct {
    Timeout        time.Duration
    NodeTimeout    time.Duration
    Concurrency    int
    MaxRowsPerNode int
}

func CheckReplicationQueueFailures(
    ctx context.Context,
    targets []Target,
    opts ReplicationQueueOptions,
) (ReplicationQueueReport, error)
```

约定：

- 一次调用处理一个逻辑集群；NodeID 全局唯一，ShardID 在本次调用中稳定。同一主机多个实例必须使用不同 NodeID。
- 校验空目标、空身份、重复 NodeID、nil Querier 和非法预算，校验失败不执行查询。
- 零值选项使用默认值；负值报错。暂定总预算 10 秒、单节点 2 秒、并发 4、单节点最多保留 1000 条；这些是初始配置，需集成测试验证。
- 适配器必须真实传递 context，不能用后台 goroutine 提前返回来伪装取消。库无法强制终止不遵守 context 的实现，应在接口文档说明。
- 多目标可以并发，但同一目标的 Rows 只由一个 worker 迭代。使用固定数量 worker，调度等待也纳入总预算。
- Go 方法返回类型必须匹配；现有 driver.Rows、sql.Rows 或 common.Rows 均需薄适配，不能直接假定连接满足 Querier。

## 4. 结果与错误契约

`ReplicationQueueReport` 至少包含：

| 字段组 | 内容 |
|---|---|
| 检查身份 | CheckID、规则版本 |
| 时间 | StartedAt、FinishedAt；每节点采集区间 |
| 状态 | Execution：complete / partial / failed；Outcome：matched / no_match / unknown |
| 覆盖 | 每个目标的状态、读取与保留行数、Truncated、错误类别 |
| 匹配 | ShardID、Database、Table、PartName，以及所有来源节点和对应异常证据 |
| 后续核查 | 稳定的建议代码；消费端决定展示语言 |

匹配以 `(ShardID, Database, Table, PartName)` 归并，保留来源证据，不混淆不同分片。相同节点的多条队列记录可以保留为多个观测，不声称合并后的数量就是队列任务数。最终排序固定，避免并发导致输出抖动。

**v0 不解析 part 名推断分区。** 现有分区归并主要服务修复；当前展示以表和 part 为单位足够。分区解析留到有明确需求和兼容性依据后增加。

执行与匹配矩阵：

| 情况 | Execution | Outcome | 顶层 error |
|---|---|---|---|
| 全部成功，无匹配 | complete | no_match | nil |
| 全部成功，有匹配 | complete | matched | nil |
| 部分失败／截断／解析失败，有有效匹配 | partial | matched | nil |
| 部分失败／未完成，未发现匹配 | partial | unknown | nil |
| 全部失败，且无有效观测 | failed | unknown | ErrNoUsableObservation |
| context 取消或总预算耗尽 | partial 或 failed | 根据已保留证据为 matched 或 unknown | 对应 context 错误 |
| 输入不合法 | failed | unknown | 参数错误 |

一个节点先读到有效行再发生迭代错误，保留有效证据并标记不完整；不能把该节点计入完整成功。取消后已完整获得的证据也保留。消费者应先处理报告，再展示顶层错误。

错误类别至少包含 query_failed、scan_failed、iteration_failed、close_failed、timeout、canceled、not_started。权限和不兼容错误只有在适配器能可靠分类时才细分，不能用不稳定的字符串猜测覆盖通用错误。错误文本和 last_exception 可能敏感，报告不自动写日志，UI 必须按文本转义展示。

## 5. 查询、截断和预算

固定查询的逻辑投影为 `database, table, new_part_name, last_exception`，只匹配 `last_exception` 中的 `NO_REPLICA_HAS_PART`。具体 SQL 在支持版本验证后固定；不用 SELECT *，不用模型生成 SQL。

第一版不额外依赖重试次数、任务类型等辅助列；不支持必需字段时报告无法检查，不静默退化为其他语义。

截断策略：

1. 将保留上限设为 N，查询最多读取 N+1 条匹配，按稳定字段排序。
2. 读到第 N+1 条即标记 Truncated，只保留前 N 条。
3. 遍历结束必须检查 Rows.Err，所有路径关闭 Rows，关闭错误不能静默丢弃。
4. 服务端行数／读取限制必须采用报错语义，不能使用 `result_overflow_mode='break'` 将不完整查询伪装成正常结束。
5. 适配器必须保证查询未被其他层静默截断；若当前连接配置无法保证，不能标为 complete。对影响结果完整性的会话设置制定覆盖或检测策略，并在真实服务器上验证。

LIMIT 只限制输出，不代表限制扫描工作。查询执行同时使用 context deadline 和服务器执行时间预算；具体 SETTINGS、参数绑定及 readonly 用户下的可设置性在集成阶段确认。检查不执行 FLUSH LOGS，不绕过权限，不能为取消而关闭调用方共享连接。

## 6. 计划目录

以下为计划落位，不代表已实现：

```text
clickhouse-go-diagnose/
  README.md
  go.mod                         # module github.com/YenchangChan/clickhouse-go-diagnose
  query.go                       # Querier / Rows
  report.go                      # 结构化结果与错误
  replication_queue.go           # 检查入口、预算、调度
  replication_queue_sql.go       # 固定查询与能力边界
  replication_queue_reduce.go    # 纯函数归并和状态计算
  replication_queue_test.go
  replication_queue_reduce_test.go
  testdata/replication_queue/     # 脱敏案例及合成边界数据
  integration/                   # 独立测试模块，驱动不进入核心依赖
  docs/implementation-plan.md
  docs/compatibility.md          # 实测后记录精确版本和结果
```

ckman 适配器放在 ckman 侧，避免核心库引入 ckman。v0 不发布通用驱动适配器包；真实集成测试可在独立 module 中使用 clickhouse-go 的 native 和 database/sql 路径。

## 7. 实施任务与完成条件

### M0：确定样本与模块边界

- [x] 确定正式 module 路径：`github.com/YenchangChan/clickhouse-go-diagnose`；源码复用许可仍待确认。
- [ ] 整理一个真实现场案例：版本、拓扑、输入快照、人工确认和预期结果。
- [ ] 将合成数据与真实脱敏数据分别标记来源；没有现场材料时先实现合成回放，但真实案例验收保持未完成。
- [x] 固定结果状态和错误契约，写入包文档。（doc.go：结果契约矩阵、错误契约、适配器约定、默认预算、版本兼容指引）

完成条件：一个可复用的输入／预期结果样本及清晰的来源说明；不依赖业务平台即可描述检查行为。

### M1：实现纯函数核心

- [x] 定义目标、观测、匹配、节点结果及总报告。
- [x] 实现异常匹配、跨副本归并、稳定排序和状态汇总。
- [x] 测试同分片重复、不同分片同名、无匹配、部分失败、有证据但不完整等边界。
- [x] 明确 no_match 的含义，检查输出中不出现未经验证的永久丢失／确定根因结论。

完成条件：无需数据库即可由输入快照确定报告；测试验证结构与语义，不依靠整段文案快照。

### M2：实现采集与执行预算

- [x] 实现固定查询、N+1 截断检测、Scan/Err/Close 的完整处理。
- [x] 实现固定 worker 数、总超时、单节点超时和未启动节点记录。
- [x] 覆盖读取中失败、空值／非法数据、取消期间返回部分证据、连接查询失败。
- [x] 使用遵守 context 的测试适配器验证取消；检测 goroutine 与 Rows 资源释放。

完成条件：单元测试及 race 检查通过；任何不完整路径都不能产出 complete + no_match。

### M3：真实查询验证

- [x] 选择 ckman 当前实际使用的精确服务器版本作为第一条支持记录，不预先承诺宽版本范围。（首轮：26.10.3.7 本地隔离集群；ckman 首发环境版本待确认）
- [x] 验证四个必需字段、过滤、排序、参数、N+1 和 SETTINGS 行为。（首轮完成；SETTINGS 行为未覆盖）
- [x] 验证 native 与 HTTP 查询、Rows.Err、取消和权限不足路径。（HTTP 经 database/sql 路径；v2.15 Options.Protocol 失效已记录）
- [x] 使用隔离的复制表／Keeper 测试环境验证 system.replication_queue 查询；不在生产制造故障。（同机双副本共享 Keeper 路径，隔离库 `__diagnose_it`，验证后 DROP）
- [x] 实际异常匹配至少有一条可复现来源：隔离故障场景，或经核实的现场查询记录。合成回放不能冒充真实故障集成测试。（`scripts/inject_no_replica_has_part.py` 注入 merge 条目 → 真实 code 234 `NO_REPLICA_HAS_PART`，`integration/real_fault_test.go` 断言）
- [x] 在 compatibility.md 记录版本、协议、验证日期、通过与未覆盖项。

完成条件：至少一个精确版本的查询兼容性得到验证；真实异常验证的来源明确。未完成的协议不列为支持。

### M4：ckman 接入

- [ ] 在 common.Conn 增加 QueryContext(ctx, ...)，native 传递 ctx，HTTP 使用 QueryContext；原 Query 保持兼容。
- [ ] 在 common.Rows 增加 Err 转发，并分别测试两种协议。
- [ ] 检查建连与 Ping 的预算，诊断入口的总耗时不能遗漏连接准备；必要时增加带 context 的连接入口。
- [ ] 增加薄适配器，转为 diagnose.Rows；不修改共享连接上的可变 context。
- [ ] 从现有拓扑生成明确节点／分片身份，复用消费端权限和凭据处理。
- [ ] 先用本地 go.work 或临时 replace 验证跨项目集成，发布前移除不可移植的绝对路径。
- [ ] 在现有集群管理流程中增加手动复制队列检查，呈现异常、来源、采集时间及覆盖情况；不提供自动修复按钮。
- [ ] 增加“部分结果”“未发现匹配”“检查失败”三个消费端场景，避免统一渲染成绿色健康。

完成条件：ckman 用户能够发起检查并区分结果状态；请求取消能传至数据库；调用方不会因 error 丢弃已有有效证据。

### M5：交付收尾

- [ ] 运行已完成阶段对应的必要测试，并记录实际命令与结果。
- [x] 补 README 使用示例、默认预算、结果语义与适配器约定。（README 使用示例；结果语义/预算/适配器约定在 doc.go godoc 中）
- [ ] 以首个案例核对输出和人工确认结果，记录剩余局限。
- [ ] 发布前确定版本号（module 路径已定为 `github.com/YenchangChan/clickhouse-go-diagnose`）；核心库不依赖 ckman、既有诊断平台或 LLM SDK。
- [ ] 再决定是否让既有诊断平台回用该库，以及第二个异常类型的范围。

## 8. 验证命令与发布门槛

核心库建立后执行：

```bash
go test ./...
go test -race ./...
go vet ./...
```

integration 独立模块单独运行；需要真实实例的测试通过显式环境配置启用，默认测试不访问外部集群。ckman 修改遵循其届时有效的仓库指引，重点验证连接封装、适配器及展示状态；文档阶段不运行或宣称这些测试已经通过。

v0 的完成条件是 M0–M5 的必要项有证据闭环：实际案例、可靠采集、真实版本验证、ckman 接入。只有纯函数测试通过时，标记为核心原型；只有查询跑通时，标记为查询验证完成，都不宣称整个诊断产品已交付。

## 9. 当前待落实信息

- 正式仓库／module 归属。
- 首个真实案例的脱敏快照与人工确认记录。
- ckman 首发环境的服务器版本、协议和节点访问方式。

这些信息不阻碍接口与合成测试工作，但决定正式发布、兼容声明和现场验收。实施时优先从已有项目配置与案例材料确认，无法获得的内容明确保留为未验证，不用推测补齐。
