# clickhouse-go 的 AI 诊断能力

**状态：** 讨论总结与提案草稿  
**日期：** 2026-11-12  
**读者：** clickhouse-go 维护者、ClickHouse 客户端团队、贡献者  
**范围：** 本文不是正式设计文档。它记录一次设计讨论的结论，并提出一个待评审的方向。  
**英文原版：** [`ai-diagnostics.en.md`](ai-diagnostics.en.md)

> **后续决策（2026-11-12）：** 先做独立库 `clickhouse-go-diagnose`，ckman 是第一个 consumer；第一版只做一个 check 的垂直切片（候选：`ReplicationHealth` / `SlowQuery`），不做三检查框架。LLM 仍然后置。API 倾向使用最小的 `Querier` / `Rows` 接口，后续设计文档再定。

---

## 1. 执行摘要

`clickhouse-client` 有一个用 `??` 触发的 AI SQL 生成功能。它真实可用，对简单查询有帮助，但它是按通用自然语言转 SQL（NL2SQL）工具来构建的。

讨论的主要结论：

1. **通用 NL2SQL 是错误的 product frame。** 市场已经很拥挤，权限和安全要求让通用 SQL 生成器很难用。
2. **更强的产品是可观测性与故障排查助手。** 它回答“CPU 为什么高？”“这条查询为什么慢？”“这条查询的内存在哪里？”等问题。
3. **正确的实现方式是一组诊断检查，而不是自由生成 SQL。** LLM 选择检查并解释结果。SQL 由检查持有。
4. **不要把诊断集成进驱动。** 驱动提供查询原语。诊断属于独立模块或独立仓库。
5. **先不要 LLM。** 让检查本身就有用。之后再把 LLM 作为可选的路由器和总结器加入。

### 决策记录

| 事项 | 决策 |
|---|---|
| 把 `??` 作为驱动功能移植到 clickhouse-go | 否 |
| 给 `driver.Conn` 增加诊断方法 | 否 |
| 把诊断放在独立模块或独立仓库 | 是 |
| 在驱动中加入 LLM 依赖 | 否 |
| ckman 的角色 | 第一个 consumer，不把诊断逻辑写进 ckman |
| 第一个交付物 | 独立库 + 一个 check（ckman 作为第一个 consumer） |
| 正式提案 | 面向配套模块的 Discussion，不是驱动变更 |

---

## 2. `clickhouse-client` 如何实现 `??`

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

## 3. 反响与证据

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

证据表明**公开使用量低、反馈少**，而不是一个有争议或被广泛批评的功能。

### 3.4 评分表

| 维度 | 评估 |
|---|---|
| 维护者投入 | 高 |
| 公众社区兴趣 | 低 |
| 简单查询可用性 | 中到高 |
| 复杂查询可用性 | 中到低 |
| 成熟度 | 早期；正在重写 |
| “全是演示，没有实质” | 否，但演示效果强于日常复杂使用 |

---

## 4. 为什么产品方向应该改变

### 4.1 通用 NL2SQL 已经商品化

许多工具都能把自然语言转成 SQL。只生成 SQL 的数据库客户端没有强差异化。

### 4.2 权限让通用 SQL 生成很难用

通用 SQL 生成器必须处理：

- 任意表；
- 任意语句；
- 数据泄漏；
- 破坏性操作；
- 成本控制；
- 幻觉列和表。

每一条限制都会降低功能的可用性。结果要么不安全，要么太受限。

### 4.3 系统表才是真正的资产

ClickHouse 系统表已经包含故障排查所需的证据：

| 问题 | 数据来源 |
|---|---|
| CPU 为什么高？ | `system.metric_log`、`system.query_log`、`system.trace_log`（CPU）、`system.processes`、`system.merges` |
| 这条查询为什么慢？ | `system.query_log`、`EXPLAIN ANALYZE`、`system.trace_log`、`system.query_thread_log` |
| 内存为什么高？ | `system.query_log.memory_usage`、`system.trace_log`（MemorySample/MemoryPeak）、`system.metric_log` |
| 写入或合并为什么慢？ | `system.parts`、`system.replicas`、`system.replication_queue`、`system.detached_parts` |
| 为什么有错误？ | `system.errors`、`system.query_log.exception`、`system.text_log` |

数据已经存在。难的不是查询。难的是知道该跑哪条查询、如何解释结果、如何跨时间和副本关联证据。

### 4.4 只做“系统表 NL2SQL”还不够

把模型限制在系统表能缩小 schema，但不能解决核心问题。模型仍然可能：

- 选错表；
- 用错指标；
- 把相关当成因果；
- 给出看似合理但错误的诊断；
- 泄漏 `system.query_log` 中的敏感字面量。

正确的抽象是**受控的诊断检查**。检查持有 SQL 和解释规则。模型选择检查并解释 finding。

---

## 5. 提议的产品：诊断检查

### 5.1 三层

```text
clickhouse-doctor (CLI / agent / MCP)
        |
        v
clickhouse-go-diagnose (checks, Finding)
        |
        v
clickhouse-go (driver)
```

依赖只向上：

- 驱动不知道诊断。
- 诊断包只使用公开的驱动 API。
- CLI 或 agent 使用诊断包。
- LLM 是可选的，位于最上层。

### 5.2 Go API 草图

```go
package diagnose

type Check interface {
    Name() string
    Description() string
    Run(ctx context.Context, q Querier, params Params) (Finding, error)
}

type Finding struct {
    Summary   string
    Evidence  []Evidence
    NextSteps []string
}

type Evidence struct {
    QueryID  string
    Metric   string
    Value    float64
    Details  map[string]string
}

func TopCPUConsumers(ctx context.Context, q Querier, window time.Duration) (Finding, error)
func ExplainSlowQuery(ctx context.Context, q Querier, queryID string) (Finding, error)
func MemoryBreakdown(ctx context.Context, q Querier, queryID string) (Finding, error)
```

`Querier` 接口应该很小，例如：

```go
type Querier interface {
    Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
}
```

> 2026-11-12 更新：实际实现倾向于使用最小的 `Querier` / `Rows` 接口，而不是直接绑定 `driver.Rows`，以便 ckman、`database/sql` 和其他客户端适配。上面的代码保留为讨论起点。

### 5.3 示例输出

```json
{
  "summary": "CPU 主要由三条查询占用，占总量的 78%。",
  "evidence": [
    {
      "query_id": "8f3c...",
      "metric": "OSCPUVirtualTimeMicroseconds",
      "value": 0.61
    },
    {
      "query_id": "8f3c...",
      "metric": "read_rows",
      "value": 1800000000
    }
  ],
  "next_steps": [
    "对 query_id=8f3c... 运行 EXPLAIN ANALYZE",
    "检查是否缺少分区裁剪"
  ]
}
```

### 5.4 LLM 的角色

LLM 应该：

- 从注册表中选择检查；
- 填充参数，例如时间窗口或 query ID；
- 用自然语言解释 `Finding`；
- 如果证据不完整，提出后续问题。

LLM 不应该：

- 编写任意 SQL；
- 直接执行查询；
- 在没有证据的情况下声称因果；
- 查看包含密钥的原始查询文本。

---

## 6. 集成决策

### 6.1 “集成”的四个层级

| 方案 | 含义 | 独立版本 | 依赖隔离 | 建议 |
|---|---|---|---|---|
| 根包或 `driver.Conn` | 驱动暴露 `Diagnose()` | 否 | 否 | 不要用 |
| 同模块子包 | `clickhouse-go/v2/diagnose` | 否 | 部分 | 仅 POC |
| 同仓库嵌套模块 | `diagnose/` 带自己的 `go.mod` | 是 | 是 | 可接受的折中 |
| 独立仓库或模块 | `clickhouse-go-diagnose` | 是 | 是 | 推荐 |

### 6.2 为什么驱动核心不合适

1. **职责不同。** 驱动负责协议、类型映射和连接管理。诊断编码的是关于系统表、指标和版本的操作知识。
2. **发布节奏不同。** 服务器的系统表和指标变化比协议更频繁。诊断变更不应要求驱动发布。
3. **API 稳定性。** `Finding` 和检查目录会快速演进。驱动的 semver 不应与它们耦合。
4. **接口稳定性。** `lib/driver/driver.go` 说明 `Conn` 是供消费的，不是供实现的，新增方法会破坏装饰器和适配器的编译。诊断不能给 `Conn` 加方法。
5. **测试矩阵。** 驱动测试已经覆盖 native、HTTP、`database/sql` 和多个服务器版本。诊断会引入 `query_log` 开关、采样、Cloud 多副本、权限和合成负载。这不应拖慢或破坏驱动 CI。
6. **权限。** 诊断需要访问 `system.query_log`、`system.processes` 和 `system.trace_log`。驱动不能暗示这些授权。
7. **依赖。** LLM SDK 或 agent 框架绝不能成为驱动依赖。

### 6.3 推荐的依赖方向

```text
clickhouse-go (driver)
        ^
        |
clickhouse-go-diagnose (checks)
        ^
        |
clickhouse-doctor (CLI / agent)
```

驱动暴露 `Query`、`Rows`、`ServerVersion` 和 `QueryFormat`。这些对诊断包已经足够。如果诊断包需要很多新的驱动方法，说明抽象错了。

### 6.4 选项

#### 选项 A：独立仓库（推荐）

```text
github.com/ClickHouse/clickhouse-go-diagnose
github.com/ClickHouse/clickhouse-doctor
```

优点：

- 独立版本管理；
- 不影响驱动 CI；
- 可以有独立维护者；
- 可以先作为社区项目，之后再移到 ClickHouse 组织。

缺点：

- 多一次 `go get`；
- 发现性较低；
- 项目必须维护自己的兼容性矩阵。

#### 选项 B：同仓库嵌套模块

```text
clickhouse-go/
  go.mod
  diagnose/
    go.mod
```

优点：

- 一个仓库；
- 独立打 tag；
- 用户会认为它是官方项目的一部分。

缺点：

- 多模块 CI 更复杂；
- tag 命名和 `replace` 处理需要小心；
- 维护者必须接受仓库中出现一个非驱动模块。

如果维护者想要官方所有权但不想开新仓库，这是最好的折中。

#### 选项 C：同模块子包（长期不推荐）

```go
import "github.com/ClickHouse/clickhouse-go/v2/diagnose"
```

优点：

- 最容易发现；
- 不需要新模块；
- 适合 POC。

缺点：

- 共享驱动的 semver；
- `Finding` 的变更会成为驱动的破坏性变更；
- 诊断测试进入驱动 CI；
- 维护责任默认落到驱动维护者身上。

只在小规模、临时的 POC 中使用。

---

## 7. 安全与权限

基于系统表的诊断缩小了影响范围，但没有消除安全工作。

### 7.1 所需授权

每个检查应声明它需要的系统表。专用只读用户只获得这些授权。

示例：

```sql
GRANT SELECT ON system.query_log TO ai_diagnostics;
GRANT SELECT ON system.metric_log TO ai_diagnostics;
GRANT SELECT ON system.trace_log TO ai_diagnostics;
```

### 7.2 敏感数据

`system.query_log.query` 可能包含字面量和密钥。`system.processes` 可能显示当前查询。`system.text_log` 和 `system.crash_log` 可能包含路径和配置值。

缓解措施：

- 在把数据发给 LLM 之前，脱敏字符串字面量；
- 在适用处设置 `format_display_secrets_in_show_and_select = 0`；
- 除非用户明确要求，否则不要发送原始查询文本；
- 记录发送给提供方的数据。

### 7.3 只读沙箱

如果检查运行 `EXPLAIN` 或任何服务端查询，应用：

- `readonly = 1`；
- `max_execution_time`；
- `max_memory_usage`；
- 单语句限制；
- 对表函数和外部访问使用拒绝列表。

### 7.4 Cloud 与多副本

在 ClickHouse Cloud 和集群部署中：

- `system.query_log` 是每个副本本地的；
- 当问题是集群范围时，使用 `clusterAllReplicas('default', system.query_log)`；
- 当需要最新日志时，使用 `SYSTEM FLUSH LOGS ON CLUSTER 'default'`；
- 处理 Cloud 与自托管部署的差异。

---

## 8. 测试与版本管理

### 8.1 确定性检查

每个检查都应有针对真实 ClickHouse 实例的集成测试。覆盖：

- 有已知数据的正常情况；
- 空结果；
- 缺少权限；
- `query_log` 关闭；
- 较旧的服务器版本；
- 类似 Cloud 的多副本设置（适用时）。

### 8.2 LLM 层

不要让 CI 依赖实时模型。单元测试使用假提供方。实时测试用环境变量门控。

### 8.3 版本门控

使用 `Conn.ServerVersion()` 为不同服务器版本选择 SQL 变体。不要假设每个受支持版本都有某个系统表列。

### 8.4 输出稳定性

把 `Finding` 当作公共 API。以向后兼容的方式加字段。如果输出被其他工具消费，对输出格式进行版本管理。

---

## 9. 路线图

> 2026-11-12 更新：第一版改为“独立库 + 一个 check”，不再先做三检查 CLI。下面保留原始路线图作为上下文。

### 阶段 1：CLI 与内部检查

构建带三个检查的 `clickhouse-doctor`：

```bash
clickhouse-doctor --since 1h --check cpu
clickhouse-doctor --query-id abc123 --check explain
clickhouse-doctor --since 1h --check memory
```

把检查放在 `internal/`。验证：

- 没有 LLM 时检查是否有用；
- 输出对 SRE 是否清晰；
- SQL 是否跨版本和 Cloud 可用。

### 阶段 2：抽取公共诊断包

把稳定的检查移到 `clickhouse-go-diagnose`：

```go
finding, err := diagnose.TopCPUConsumers(ctx, conn, time.Hour)
```

此时 API 已有真实使用。

### 阶段 3：可选 LLM 层

增加一个 agent，把自然语言问题映射到检查并总结结果：

```go
answer, err := agent.Ask(ctx, "Why is CPU high?")
```

LLM 不编写 SQL。

### 阶段 4：正式提案

在 POC 和使用数据之后，在 `ClickHouse/clickhouse-go` 开一个功能请求 Discussion，讨论配套模块。不要提议驱动变更。

建议的提案范围：

- 独立模块或仓库；
- 驱动不变；
- 驱动中无 LLM 依赖；
- 检查返回 SQL 和结构化 finding；
- 执行由调用方决定；
- 初始检查：CPU、慢查询、内存。

---

## 10. 开放问题

1. 第一版应该是 CLI、Go 库，还是两者都要？
2. 诊断包应该放在 ClickHouse 组织下，还是先作为社区项目？
3. 哪三个检查能以最低版本风险提供最大价值？
4. 应该如何检测和处理 Cloud 多副本查询？
5. LLM 层是否应该与检查分开成独立模块？
6. 哪种输出格式应该稳定：JSON、Markdown，还是两者都要？
7. 在数据到达模型之前，应该如何脱敏敏感查询文本？
8. 工具应该提出澄清问题，还是总是使用默认时间窗口？

---

## 11. 参考

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
