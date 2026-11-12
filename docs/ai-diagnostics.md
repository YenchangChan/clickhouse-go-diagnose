# ClickHouse 诊断库：从既有诊断案例抽取可复用检查

**状态：** 讨论总结与提案草稿，待首个检查验证  
**更新日期：** 2026-11-12  
**读者：** clickhouse-go 维护者、ckman 开发者、贡献者  
**范围：** 确定产品边界、首个检查和验证路径；公共 Go API 尚未定稿。

> **当前决策：** 基于既有诊断平台的已实现能力与用户反馈中的跑通案例，先建设独立库 `clickhouse-go-diagnose`。首个候选检查为 `ReplicationQueueFailures`，第一版只识别复制队列中的 `NO_REPLICA_HAS_PART` 异常记录。ckman 是首个计划接入的消费端，既有诊断平台是现有实现和案例来源。先交付可靠的证据与判定，再接可选 LLM。
>
> 本次更新阅读了本地既有诊断平台代码及相关测试文件，没有连接实际集群复跑。文中的现场数字来自代码注释，尚不能替代可回放的案例证据。附录保留此前的上游调研记录，PR 状态和实现细节未在本轮重新核验。

---

## 1. 为什么做这个库

既有诊断平台已经实现了副本只读／复制延迟、parts 过多、磁盘空间不足、丢失 part 扫描以及 AI 深挖等能力。当前问题不是从零构建诊断框架，而是从这些实现中提取可被其他 Go 应用使用的诊断知识。

面向 ckman 的第一项价值是：用户发现复制异常后，能得到受影响对象、实际采集到的证据、尚不能确认的事项，以及下一步核查建议。这个流程应在没有 LLM 时仍然成立。

这不需要以否定通用 NL2SQL 为前提。SQL 生成和运维诊断服务不同需求；本项目优先处理有现有案例支撑、可通过明确输入输出验证的诊断场景。

### 决策记录

| 事项 | 当前决定 |
|---|---|
| 给 clickhouse-go 驱动增加诊断方法 | 否；驱动继续提供查询原语 |
| 诊断代码的位置 | 独立库 `clickhouse-go-diagnose`，组织归属后定 |
| 已有实现来源 | 既有诊断平台的探针、规则、丢失 part 扫描和测试 |
| 首个计划接入端 | ckman |
| 第一版范围 | 一个检查；只识别 `NO_REPLICA_HAS_PART` 异常记录 |
| 第一版是否自动修复 | 否；提供证据与核查建议 |
| 第一版是否依赖 LLM | 否 |
| 是否先做 CLI、注册表、通用 agent | 否；由真实接入需求推动 |
| 上游提案时机 | 完成检查、消费端接入和案例验证之后 |

## 2. 既有实现提供的设计依据

源码位于既有诊断平台的 `mvc/service/diagnosis/` 下（本地参考实现，不在本仓库内）。

| 已有能力 | 实现入口 | 可提炼的经验 |
|---|---|---|
| 副本只读／复制延迟 | `rule/replica_readonly.go` | 先发现症状，再查询队列、磁盘和 Keeper 补充解释 |
| 单分区 parts 过多 | `rule/too_many_parts.go` | 将阈值异常与合并状态、配置关联 |
| 磁盘空间不足 | `rule/disk_full.go` | 将异常、影响和后续动作一起呈现 |
| 丢失 part 扫描 | `lostparts/scan.go` | 固定查询、按分片／表／分区归并、保留部分结果和错误 |
| 节点覆盖 | `sync_exec.go` | 区分无异常与检查未完成，避免把部分节点正常报告为全集群正常 |
| AI 深挖 | `ai/reactagent.go` | 记录工具取证及失败、限制轮次、证据不足时降级 |

### 应当保留的结构

现有 `Detect / Diagnose` 分离值得保留为实现思路：先判定可观测症状，再补充证据。纯函数判定便于回放案例；SQL 与探针负责收集事实；消费端负责展示和后续操作。第一版不必为此立即发布一个通用 `Check` 接口。

节点覆盖信息需要成为结果的一部分，并贯穿补充取证阶段。查询失败、空结果、被截断的结果必须有不同表达，不能只在自然语言摘要里提示。

### 抽取前需要收紧的推断

已有实现证明了流程可用，但部分结论比直接证据更强：

- `replica_readonly.go` 用队列非空推导“同步任务反复失败”。独立库应结合重试、异常和时间信息，或仅报告队列状态。
- `too_many_parts.go` 用存在进行中的 merge 推导“合并吞吐跟不上写入”。单次快照应将其表达为待验证的解释。
- `probe/keeper.go` 从一个健康节点探测 Keeper，并将查询错误统一表示为不可达。它不能直接代表异常副本的连接状态，也应区分权限不足与连通性失败。
- 部分 `Diagnose` 路径忽略补充探针返回的覆盖信息；独立库需要保留这类证据缺口。
- `lostparts/scan.go` 注释中的“必然同时丢失”“永不自愈”属于需要额外验证的现场解释。匹配到异常记录本身不足以证明所有副本当前都没有可用 part，更不能直接确定故障起因。

这些边界应通过案例和测试逐项确定，而不是原样复制到公共 API 的承诺中。

## 3. 首个检查：ReplicationQueueFailures

### 3.1 第一版回答的问题

> 在调用方指定且本次成功覆盖的节点中，哪些复制队列条目记录了 `NO_REPLICA_HAS_PART`，涉及哪些分片、表和 part？本次检查还有哪些未覆盖范围？

第一版只支持这一种异常。名称为后续扩展保留空间，但不承诺检测所有复制故障。

它不直接回答“数据是否永久丢失”“是否由断电引起”“是否一定不能自愈”。要回答这些问题，需要继续核对各副本的当前状态、覆盖 part、相关日志及服务器行为。

### 3.2 输入与采集

调用方提供明确的目标节点及稳定的分片／节点标识、查询适配器和检查预算。集群发现、凭据、连接池与 TLS 由消费端管理，库不读取消费端的全局配置。

检查持有固定 SQL，读取 `system.replication_queue` 的异常条目。现有实现中的 `last_exception` 匹配是起点；具体列集、版本要求、截断检测方式在实现设计中确定。

初版应保留节点、数据库、表、part 名和匹配到的异常证据。重试次数、任务类型、创建时间等只在能力已确认时采集；不因缺少辅助列而静默改变检查含义。

异常记录可能反映此前的失败。报告必须标注采集时间，并使用“队列记录了该异常”等表述，避免把历史异常直接视为当前根因。

### 3.3 归并与范围

- 以调用方提供的分片标识、数据库和表归并，保留每条证据的来源节点。
- 同一分片不同副本报告同一 part 时可合并展示，但不能丢失来源信息。
- 不同分片不能因同名表或 part 而合并。
- 分区归并沿用现有案例作为候选方案；part 名解析的适用范围必须有测试和支持条件，解析不了时保留原始 part 名，不猜分区。
- 返回的受影响范围是已观测范围。节点失败或结果截断时，不声称已列出全部异常。

### 3.4 第一版输出

输出包含异常记录、受影响对象、证据来源、采集时间、检查完整性，以及后续核查建议。

建议应引导用户核对对应副本、队列状态和相关 part。第一版不自动执行 `DETACH / ATTACH`、重启副本或其他修复，也不因为匹配到一条异常就把这些操作作为已确认的解决方案。

既有平台的修复单元、预检信息和界面交互可继续留在应用侧；以后如要复用修复能力，应另行定义前置条件、影响范围和执行契约。

## 4. 结果契约：事实、判断与局限

公共类型尚未定稿，但以下语义应从第一版就稳定：

| 信息 | 要求 |
|---|---|
| 执行状态 | `complete`、`partial`、`failed`，与异常判断分开 |
| 检查结果 | `matched`、`no_match`、`unknown`；仅表示该检查的匹配结果，不表示集群整体健康 |
| 目标与覆盖 | 计划检查哪些节点、哪些成功、哪些失败 |
| 完整性 | 节点覆盖完整不等于结果完整；截断和解析失败也要记录 |
| 观测时间 | 保留每次采集时间或采集区间，不暗示跨节点原子快照 |
| 证据 | 来源节点、查询标识、结构化字段、单位及取证状态 |
| 判断 | 规则标识及其支持的有限结论，与候选解释区分 |
| 后续建议 | 需要补充验证的事项，执行由消费端另行决定 |

例如：

- 所有目标成功查询且未截断，没有匹配记录：`complete + no_match`。
- 部分节点失败，已查询节点没有匹配：`partial + unknown`，可附“已覆盖节点未见匹配”。
- 部分节点失败，但已发现匹配：`partial + matched`，异常成立，范围可能不全。
- 所有目标查询失败或未配置目标：检查失败，不能输出正常。

原先 `Finding{Summary, Evidence, NextSteps}` 可以作为展示视图，但不足以单独承担库的结果契约。摘要由结构化结果生成；机器消费不能依靠解析摘要或 `Target` 文本。

库也不应把任意数量都装入 `float64`。计数、时长、比例应有明确类型和单位，原始值与派生指标分开。

## 5. 抽取边界与 Go API

依赖方向为：

```text
ckman / 其他消费端 / 后续 CLI 或 agent
                  |
                  v
       clickhouse-go-diagnose
                  |
                  v
       查询适配器 / clickhouse-go
```

### 进入独立库

- 首个检查的固定查询及必要的能力检测。
- 查询结果解析、异常匹配和归并。
- 不依赖平台状态的纯函数判定。
- 结构化报告、完整性信息和有限的核查建议。
- 从现有案例提取的测试数据与回归测试。

### 留在消费端

- 集群拓扑发现、凭据、TLS 和连接生命周期管理。
- 定时任务、报告持久化、用户权限和界面跳转。
- 修复操作及执行交互。
- 模型配置、agent 框架、MCP 和 AI 报告编排。

`probe.Ctx` 当前绑定既有平台的持久化模型和连接配置，不能原样成为公共 API。`lostparts.Scanner` 的查询注入和纯数据归并可作为抽取起点。

API 可以先从一个类型明确的函数开始：

```go
// 示意；Targets、查询接口和报告类型在实现设计中确定。
func CheckReplicationQueueFailures(
    ctx context.Context,
    targets []Target,
    opts ReplicationQueueOptions,
) (ReplicationQueueReport, error)
```

`Target` 需要表达节点身份及查询入口。执行由调用方显式调用检查触发；检查通过注入的查询接口执行固定 SQL。第一版不再引入“先生成 SQL 计划，再另行执行”的协议。

最小 `Querier / Rows` 接口仍是候选，但应由实际采集需要决定。Go 方法返回类型必须精确匹配：返回 `driver.Rows` 的方法不会自动满足返回自定义 `Rows` 的接口，`clickhouse-go` 和 `database/sql` 需要各自的适配器。第一版先支持 ckman 实际需要的路径。

部分失败由报告明确表达；无效输入、取消和无法开展检查等错误的返回约定需要文档化。消费端即使收到错误，也不能未经检查就丢弃已有有效证据。

## 6. 执行预算与兼容性

固定查询仍然需要控制成本和观测范围：

- 支持调用方取消，设置明确的总预算、单节点超时和并发上限。
- 对结果量设置上限，并可靠标记截断；不把静默截断的结果当成完整结果。
- 通过调用方提供的权限查询所需系统表；权限不足应成为可识别的失败原因。
- 库不隐式执行 `SYSTEM FLUSH LOGS` 或其他管理操作。
- 版本信息只能辅助选取查询，列和表的可用性仍需按支持范围确认。
- 不硬编码集群名 `default`。第一版优先沿用调用方明确提供目标的方式；无法逐节点访问的部署需要专门适配和验证。

现有探针的超时、行数和并发限制可以作为参考值，不能直接认定适用于所有部署。尤其应检查 SQL 自身的 `LIMIT` 和探针层截断是否都会反映到报告完整性中。

## 7. 案例与验收

### 7.1 将“跑通过”变成可回放材料

从既有平台的已有案例中选一个首发案例，保存脱敏后的：

1. 部署与服务器版本、拓扑及触发症状。
2. 实际查询和原始结果快照，包括失败节点与采集时间。
3. 检查应该给出的结论，以及不能给出的结论。
4. 人工后续核查结果；如包含修复，记录修复前后观测，不将单次成功泛化为通用保证。

源码注释记录了“88 条卡住条目归并为 33 个单元”等现场信息。这些可作为寻找原始案例材料的线索，当前文档不把它们列为已独立验证的验收结果。

### 7.2 首个检查的最低验收集合

| 场景 | 预期行为 |
|---|---|
| 完整覆盖、无匹配 | 返回 `complete + no_match`，不声称全集群健康 |
| 单节点发现匹配异常 | 返回对象、part、来源和异常证据 |
| 同分片多个副本报告同一 part | 合并展示，保留各节点来源 |
| 不同分片出现同名 part | 保持独立 |
| 部分节点失败 | 保留有效证据，明确缺失范围 |
| 全部失败或零目标 | 无法完成检查，不能输出正常 |
| 权限不足或必需列不可用 | 返回可识别原因，不当作空结果 |
| 命中行数限制或解析失败 | 标记不完整，不声称异常清单完整 |
| 取消或超时 | 在约定预算内结束，正确标记未完成范围 |
| 只有异常记录、缺少进一步证据 | 不声称永久丢失、共同故障起因或必然无法自愈 |

现有 `lostparts/scan_test.go`、`rule/replica_readonly_test.go` 和节点覆盖相关测试可提供起点。测试应验证结果语义，而不只是固定文案。除快照／纯函数测试外，需要在明确支持的真实 ClickHouse 版本上验证查询与适配器；不能只靠模拟数据宣布兼容。

## 8. LLM 的后续角色

既有平台已有 AI 深挖实现，后续可以继续使用，但独立诊断库不依赖模型或 agent 框架。

LLM 可以选择已支持的检查、补充参数、解释结构化结果，并提出继续取证的请求。不能把未知补写成正常，把异常记录补写成确定根因，或将模型自述的证据视为已执行查询的结果。

工具执行证据和模型解释应在输出中可区分。模型失败、超时或未收敛时，保留规则检查结果；没有足够证据就明确说明局限。仅有一次工具成功也不足以证明所有结论具有高置信度，后续需要将具体判断与对应证据关联。

异常文本可能包含敏感信息。发给模型前应按字段筛选和脱敏，默认不发送原始查询、凭据或完整服务端异常。记录对外发送数据的机制本身也要避免重复保存敏感内容。

## 9. 交付顺序

1. **整理一个真实现场案例。** 明确实际症状、证据、人工确认和预期报告，补齐不能推断的边界。
2. **实现独立库的单个检查。** 抽取采集、解析、归并和报告语义；先完成一个实际需要的查询适配器，不建设通用检查框架。
3. **接入 ckman。** 展示异常与覆盖范围，验证调用方如何处理部分失败、超时和结果截断。
4. **完成回归与兼容验证。** 在支持版本上运行检查，用真实案例及反例证明结论边界。
5. **再决定扩展。** 根据反馈增加其他复制异常、评估既有平台回用独立库，随后再考虑 CLI、LLM 路由或公共注册表。
6. **提交上游 Discussion。** 以可运行结果和案例为依据讨论配套模块，不修改驱动职责。

## 10. 尚需在实现设计中确定的问题

- 选用哪个实际案例作为首个验收样本，原始证据是否完整且可脱敏？
- ckman 的首个查询适配器和目标节点提供方式是什么？
- 第一版支持哪些服务器版本与部署方式？
- 如何可靠检测行数截断，如何处理 part 名无法解析？
- 公共报告字段、部分结果与 Go `error` 的具体契约是什么？
- 独立库先由哪个仓库维护，何时需要稳定 JSON 格式？

独立库、单检查、LLM 后置已经是当前决定，不再作为待重新选择的路线。

---

以下附录保留上游背景，供理解讨论起点。它们不是首个检查的验收证据；引用到正式上游提案前，应补充源码版本或 commit 并重新核验状态。

## 附录 A：此前的客户端 AI 调研记录

### 2.1 入口

`??` 的处理在 `src/Client/ClientBase.cpp`：

```cpp
#if USE_CLIENT_AI
if (text.starts_with("??"))
{
    auto free_text = trim(text.substr(2), isWhitespaceASCII);
    ...
    std::string generated_sql = ai_generator->generateSQL(free_text);
    next_query_to_prepopulate = generated_sql;
    return true;
}
#endif
```

生成的 SQL 不会立即执行。它写入 `next_query_to_prepopulate`。随后交互循环调用：

```cpp
lr->setInitialText(next_query_to_prepopulate);
```

用户会在下一个提示符中看到 SQL，可以编辑，然后按 Enter 执行。

> 文档说查询会立即执行。代码只是把查询预填充到提示符并等待用户。这个差异影响用户预期和安全评审。

### 2.2 组件

| 组件 | 位置 | 职责 |
|---|---|---|
| `AIConfiguration` | `src/Client/AI/AIConfiguration.h` | 提供方、密钥、模型、限制、提示词、schema 访问 |
| `AIClientFactory` | `src/Client/AI/AIClientFactory.cpp` | 加载配置，选择 OpenAI 或 Anthropic |
| `AISQLGenerator` | `src/Client/AI/AISQLGenerator.cpp` | 构造提示词，调用模型，提取 SQL |
| `AIPrompts` | `src/Client/AI/AIPrompts.h` | 系统提示词和少样本示例 |
| `SchemaExplorationTools` | `src/Client/AI/SchemaExplorationTools.cpp` | 列出数据库、列出表、获取 DDL |
| `ai-sdk-cpp` | `contrib/ai-sdk-cpp` | 提供方客户端和多步工具循环 |

### 2.3 配置

客户端配置的 `ai` 段支持：

| 字段 | 含义 |
|---|---|
| `provider` | `openai` 或 `anthropic` |
| `api_key` | 显式密钥；否则从提供方环境变量读取 |
| `base_url` | 自定义 OpenAI 兼容端点 |
| `model` | 模型名 |
| `temperature` | 可选；只有显式设置时才发送 |
| `max_tokens` | 默认 1000 |
| `timeout_seconds` | 默认 30 |
| `max_steps` | 工具调用步数限制 |
| `system_prompt` | 自定义系统提示词 |
| `enable_schema_access` | 启用三个 schema 工具 |

如果没有配置提供方，客户端先尝试 `OPENAI_API_KEY`，再尝试 `ANTHROPIC_API_KEY`。如果密钥来自环境变量，第一次使用时会请求确认。

### 2.4 工具循环

流程是：

```text
User prompt
  -> system prompt + user prompt + tools
  -> model returns tool_calls
  -> client runs the tools
  -> tool results are sent back to the model
  -> repeat until stop or max_steps
  -> extract <sql>...</sql>
```

三个工具是：

| 工具 | SQL |
|---|---|
| `list_databases` | `SELECT name FROM system.databases ORDER BY name` |
| `list_tables_in_database` | `SELECT name FROM system.tables WHERE database = '<escaped>' ORDER BY name` |
| `get_schema_for_table` | `SHOW CREATE TABLE db.table` |

查询执行器在现有客户端连接上运行这些查询。这样会保留当前用户的访问权限。

### 2.5 安全措施

- 嵌入式客户端（SSH 和 WebSocket 终端）禁用该功能，因为嵌入式客户端运行在服务器进程内，不能暴露服务器的环境变量。见 ClickHouse/ClickHouse#100290。
- 模型只接收 schema，不接收表数据。
- 基于环境变量的 API 密钥需要用户确认。
- 该功能由 `ENABLE_CLIENT_AI` 构建选项控制。

---

## 附录 B：此前的公开反馈调研记录

### 3.1 维护者观点

功能合并得很快。引入 PR 是 ClickHouse/ClickHouse#83314。评审者称它是“really amazing feature”并批准。

后续 PR ClickHouse/ClickHouse#83787 增加了零配置的环境变量支持。

最强的信号是 ClickHouse/ClickHouse#114043，由 Alexey Milovidov 发起：

> "Turns the primitive `??` 'generate one SQL query' helper in the client into a full interactive AI agent."

该 PR 增加最近查询上下文、`system.user_query_log`、文档查找、只读查询沙箱、对其他查询的确认，以及服务端 `aiGenerate` 回退。截至 2026-11-12 仍在开放。

还有 ClickHouse/ClickHouse#130312，提议把 SQL 生成切换到 AI function 基础设施。

结论：维护者认为方向重要，但第一个实现不是最终形态。

### 3.2 可用性

该功能对简单的单表问题有效。对需要多次 schema 查找的问题很脆弱。

主要已知 bug 记录在 ClickHouse/ClickHouse#113959 和 ClickHouse/ClickHouse#115509：

> "AI SQL generation (`??`) failed on any prompt needing more than one schema lookup: the bundled `ai-sdk-cpp` rebuilt every step's request from the initial options, so the model lost all earlier tool results and kept re-requesting the same schemas until the step limit."

两个修复 PR 都已关闭且未合并。当前 master 把 `contrib/ai-sdk-cpp` 固定在 `ab06ef7`，而 `create_next_step_options` 会从 `initial_options` 重建。真正的修复是 ClickHouse/ClickHouse#114043 中开放 agent 重写的一部分。

其他已报告问题：

| Issue | 问题 | 状态 |
|---|---|---|
| #87099 | 配置的 OpenAI 模型被忽略 | 已修复 |
| #85736 | 不支持自定义 base URL | 后来支持 |
| #108014 | `temperature` 被发送给拒绝该参数的模型 | 已修复 |
| #122590 | AI 回复中的 tab 导致 `clickhouse-local` 崩溃 | 开放 |
| #100290 | 嵌入式客户端读取服务器环境变量 | 已修复并回移 |

产品限制：

- 没有对话历史。每个 `??` 都从 schema 发现重新开始。
- 只有 schema。模型看不到表数据。
- 重复的 schema 发现消耗 token 和时间。
- 大 schema 可能超出 token 预算。
- 文档与实现在是否自动执行上不一致。

### 3.3 社区反馈

公开反馈非常有限：

- Hacker News Algolia 搜索没有找到关于 `??` 的讨论。唯一相关条目是作者关于 `ai-sdk-cpp` 的 Show HN 帖子。
- 公开网络搜索只找到官方文档和一个官方视频，没有独立评测。
- GitHub 互动很低。自定义 base URL 请求只有一个 reaction。bug 报告的 reaction 很少。agent 重写没有 reaction，主要是 CI 和维护者评论。
- 一个影响所有多表问题的 bug 在功能发布一年多后才被记录，而且修复仍未合并。

这些材料只能说明**可见的公开反馈有限**，不能据此推断实际使用量。

### 3.4 评分表

| 维度 | 评估 |
|---|---|
| 维护者投入 | 高 |
| 可见的公众反馈 | 有限；不足以推断实际使用量 |
| 简单查询可用性 | 中到高 |
| 复杂查询可用性 | 中到低 |
| 成熟度 | 早期；正在重写 |
| “全是演示，没有实质” | 否，但演示效果强于日常复杂使用 |

---

## 参考

### ClickHouse 客户端 AI

- ClickHouse/ClickHouse#83314: Add AI-powered SQL generation to ClickHouse client
- ClickHouse/ClickHouse#83787: Client AI agent improvements
- ClickHouse/ClickHouse#100290: Disable AI SQL generation in embedded client
- ClickHouse/ClickHouse#108014: Do not send `temperature` unless explicitly configured
- ClickHouse/ClickHouse#113959: Fix AI SQL generation failing on prompts that need several schema lookups
- ClickHouse/ClickHouse#115509: Supersedes #113959
- ClickHouse/ClickHouse#114043: Add a full-featured AI agent to the client (the `?` command)
- ClickHouse/ClickHouse#122590: `clickhouse-local` aborts when a `??` AI reply contains a tab
- ClickHouse/ClickHouse#130312: Switch SQL query generation to AI function infrastructure
- ClickHouse/ClickHouse#85736: Custom base URL for AI SQL generation
- ClickHouse/ClickHouse#87099: Configured model ignored for OpenAI

### 文档

- [AI-powered SQL generation](https://clickhouse.com/docs/guides/use-cases/ai-ml/ai-powered-sql-generation)
- [ClickHouse Client: AI SQL generation](https://clickhouse.com/docs/concepts/features/interfaces/client#ai-sql-generation)
- [AI functions in ClickHouse](https://clickhouse.com/blog/ai-functions-in-clickhouse)

### clickhouse-go 设计

- `lib/driver/driver.go`：`Conn` 是供消费的，不是供实现的
- `.claude/CLAUDE.md`：API 设计原则
- `CONTRIBUTING.md`：集成测试设置
