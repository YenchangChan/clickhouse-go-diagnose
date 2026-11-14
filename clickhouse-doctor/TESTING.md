# clickhouse-doctor 自测教程

从零开始，把两个入口（轻量 CLI 与 Agent 对话入口）完整测试一遍。
示例全部使用本机已验证过的真实环境，命令可直接复制，替换成你自己的地址即可。

## 0. 前置条件

| 项 | 要求 | 检查命令 |
|---|---|---|
| Go | 1.26.6+（nova 的硬性要求） | `go version` |
| ClickHouse 集群 | 任意可连通节点（native 9000 / HTTP 8123） | 见第 2 步 |
| LLM | 任意 OpenAI 兼容端点 + 支持 function calling 的模型 | 见第 4 步 |

环境变量（后面所有命令都依赖，先 export）：

```bash
export CLICKHOUSE_DOCTOR_PASSWORD='你的集群密码'
export CLICKHOUSE_CHAT_API_KEY='你的LLM-API-Key'
```

> 安全提醒：API Key 不要写进脚本文件或 shell 历史；测试用的 Key 测完记得去控制台轮换。

## 1. 构建

```bash
cd ~/chenyc/src/clickhouse-go-diagnose

# 一条命令构建两个入口，产物在 clickhouse-doctor/bin/
make build

# 单元测试 + 分层断言（轻量 CLI 依赖闭包不含 nova）
make test assert-deps
```

验证安装：

```bash
clickhouse-doctor/bin/clickhouse-doctor --version
clickhouse-doctor/bin/clickhouse-doctor-chat --version
```

## 2. 基线：先用轻量 CLI 确认集群连通

跑 Agent 之前，先用不带 LLM 的 CLI 确认集群、账号、预算都正常——
这样后面 chat 出问题时可以排除集群侧因素。

```bash
# 单节点全检查（第一次建议先单节点，快）
clickhouse-doctor/bin/clickhouse-doctor --addr 192.168.110.49:9000 --check all --node-timeout 30s
echo "退出码: $?"
```

预期：正常情况下部分检查 `no_match`；这台集群有真实病变，`replication_queue_failures`
和 `replication_queue_backlog` 应显示 `matched`（exit 2）。若见 `timeout`，
把 `--node-timeout` 调大到 60s（弱磁盘虚拟机必需）。

多节点 + 跨分片（可选）：

```bash
clickhouse-doctor/bin/clickhouse-doctor \
  --node shard-1@192.168.110.48:9000 \
  --node shard-1@192.168.110.49:9000 \
  --node shard-2@192.168.110.50:9000 \
  --node shard-2@192.168.110.51:9000 \
  --check all --node-timeout 60s --timeout 120s
```

观察点：同一张表的匹配按分片归并、保留双副本来源；不同分片的同名表**不**合并。

## 3. chat 入口的参数与建连校验（不需要 LLM 也能测）

```bash
# 缺 LLM 参数应干净报错
clickhouse-doctor/bin/clickhouse-doctor-chat --node shard-1@192.168.110.49:9000

# 集群地址错误应报连接失败（i/o timeout 或 refused）
clickhouse-doctor/bin/clickhouse-doctor-chat --node shard-1@192.168.110.99:9000 \
  --base-url https://api.siliconflow.cn/v1 --model deepseek-ai/DeepSeek-V3.2
```

## 4. 单轮对话：指定检查（最短路径）

选一个快的检查先跑通全链路（磁盘水位只查 `system.disks`，秒级）：

```bash
printf "检查一下磁盘水位\nexit\n" | clickhouse-doctor/bin/clickhouse-doctor-chat \
  --node shard-1@192.168.110.49:9000 \
  --base-url https://api.siliconflow.cn/v1 \
  --model deepseek-ai/DeepSeek-V3.2
```

预期输出（两侧分离；交互终端下带圆角面板 banner、彩色图标与 spinner 动画，
管道重定向时自动降级为纯文本，两侧内容一致）：
- **stderr（进度流）**：启动面板（目标/模型/纪律/退出方式）→ `● list_checks`
  → `✓ 完成（N 字节）` → `● run_check check=disk_space_low` → `✦ AI 解释 · N 轮工具调用 · …`
- **stdout（模型回复）**：glamour 渲染的 markdown（标题/列表/表格带样式），
  先引用报告字段（`Execution: complete`、`Outcome: no_match`），再给标注"AI 解释"的分析

模型应正确使用 `no_match` 的边界语义（限定在会话节点范围内，不声称全集群健康）。

退出方式（四种，任选）：输入 `exit` 或 `quit` 回车；按 `Ctrl+D`（EOF）；
按 `Ctrl+C`（直接终止，包括工具执行中）。

## 5. 单轮对话：阈值覆盖

验证 CLI 参数如何变成工具参数、阈值槽位如何生效：

```bash
printf "检查副本状态，延迟阈值用60秒\nexit\n" | clickhouse-doctor/bin/clickhouse-doctor-chat \
  --node shard-1@192.168.110.49:9000 \
  --base-url https://api.siliconflow.cn/v1 \
  --model deepseek-ai/DeepSeek-V3.2
```

观察点：
- 模型应先 `list_checks` 看到 `delay_seconds` 槽位（默认 300），再把 60 传给 `run_check`
- 对照基线：`--replica-delay-seconds 60 clickhouse-doctor/bin/clickhouse-doctor --addr 192.168.110.49:9000 --check replica_status --node-timeout 30s`
  的匹配数量应与 chat 引用的一致

## 6. 开放性诊断（全面体检，重头戏）

```bash
printf "全面体检一下这个节点\nexit\n" | clickhouse-doctor/bin/clickhouse-doctor-chat \
  --node shard-1@192.168.110.49:9000 \
  --base-url https://api.siliconflow.cn/v1 \
  --model deepseek-ai/DeepSeek-V3.2 \
  --max-turns 10 > chat_report.out 2> chat_progress.err
```

预期（这台集群有真实病变）：
- 进度流出现全部 7 个检查的 `[工具] run_check check=...` 行
- `detached_parts` 报告可能很大（几百 KB，进度行会显示字节数）
- 模型回复应引用具体报告证据（积压天数、重试次数、lost_parts 表清单），
  解释部分带"AI 解释"标注，结尾给"下一步"建议且只使用真实检查 ID

验收清单：
- [ ] 模型没有生成任何修复 SQL / ALTER / restart 指令
- [ ] partial / timeout 的检查被模型指出"覆盖不完整"而不是当作健康
- [ ] 严重程度判断有依据（引用了理由码或阈值），且标注为解释

## 7. 交互式多轮对话

直接进 REPL（不加管道），体验多轮追问：

```bash
clickhouse-doctor/bin/clickhouse-doctor-chat \
  --node shard-1@192.168.110.48:9000 --node shard-1@192.168.110.49:9000 \
  --node shard-2@192.168.110.50:9000 --node shard-2@192.168.110.51:9000 \
  --base-url https://api.siliconflow.cn/v1 \
  --model deepseek-ai/DeepSeek-V3.2 \
  --timeout 120s --node-timeout 60s
```

建议的提问序列（验证上下文累积与跨分片归并）：

```
> 这个集群有什么异常？
> 刚才提到的积压最严重的表是哪个？两个分片都有吗？
> 把它的队列检查阈值放宽到 depth=100 再跑一次看看
> exit
```

观察点：第二轮追问不应重新调用 list_checks（上下文里有）；跨分片的同名表
在报告里是两条独立匹配（ShardID 不同）。

## 8. 常见问题

| 现象 | 原因与处理 |
|---|---|
| `[工具] 失败: 未知检查 ID` | 模型幻觉了不存在的 ID；正常会自纠，反复出现换模型 |
| replica_status / detached_parts `timeout` | 慢磁盘集群正常现象；调大 `--node-timeout`（60s+），模型会在解释里指出覆盖不完整 |
| 模型不调工具，直接闲聊 | 所选模型不支持 function calling；换 DeepSeek-V3 系列 / Qwen 系列已验证可用的模型 |
| `runtime error: invalid memory address` | 应已修复；若复现，去掉 `Emit` 二分并报 issue |
| 输出出现乱码颜色码 | 终端不支持 ANSI 时设 `NO_COLOR=1`；管道消费输出本来就会降级为无色 |
| Key 泄露风险 | 对话里贴过的 Key 一律视为已泄露，测完立即轮换 |

退出码（chat 入口）：正常退出 0；flag 非法或建连失败 1。
对话中的单轮失败不影响会话，可换一种问法继续。

## 9. 测完之后

```bash
# 完整回归（fmt + vet + 测试 + race + 分层断言）
cd ~/chenyc/src/clickhouse-go-diagnose
make check race
```

发现问题直接把 stderr 进度流（`chat_progress.err`）和模型回复贴出来即可定位。
