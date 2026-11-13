# clickhouse-doctor

`clickhouse-go-diagnose` 库的命令行消费端：对指定节点运行诊断检查并渲染报告。
只做编排与渲染——判定逻辑全部在核心库，本工具不修复、不引入 LLM。

独立 Go 模块（`replace` 指向核心库），`clickhouse-go` 依赖只出现在这里，
核心库保持仅依赖标准库。

## 使用

```bash
# native 协议，全部 7 个检查
clickhouse-doctor --addr 127.0.0.1:9000 --password '...'

# HTTP/database/sql 路径
clickhouse-doctor --http 127.0.0.1:8123 --password '...'

# 指定检查、机器可读输出
clickhouse-doctor --addr 127.0.0.1:9000 --check replica_status,disk_space_low --json

# 查看检查清单 / 版本
clickhouse-doctor --list-checks
clickhouse-doctor --version
```

密码优先级：`--password` > 环境变量 `CLICKHOUSE_DOCTOR_PASSWORD`。

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

- 一次调用一个目标节点（v0）。多节点拓扑的 Target 编排属消费端扩展，核心库已支持。
- 检查阈值使用核心库默认值；阈值暴露成 flag 待真实使用反馈后决定。
- 原始路线图中的 CPU / 慢查询 / 内存检查属核心库新检查（依赖
  `query_log`/`metric_log`），不在本 CLI 的范围内。
