package diagnose

// replicaStatusSQL 是 ReplicaStatus 检查的固定查询。
//
// 数据源语义：system.replicas 每行是一个"本节点持有副本的复制表"，
// total_replicas / active_replicas 描述该表复制组在 Keeper 中的状态
// （从本节点视角），不是本节点进程的状态。
//
// 能力边界：
//   - 只读标志与计数全部用 toString 下发为字符串列：统一走 NullString
//     扫描，不给驱动数值列留下兼容面（clickhouse-go v2 不支持 Scan 到 *any）；
//   - 不排除 system 系库：复制版 system 表（如 query_log）的副本异常
//     同样是真实证据，这里不存在 TooManyParts 场景下的噪音问题；
//   - 健康行在客户端被丢弃，只保留至少命中一条判定规则的行；
//   - 不设置任何会话级截断/溢出设置，结果完整性由客户端 N+1 检测保证。
const replicaStatusSQL = `
SELECT
    database,
    table,
    toString(is_readonly)        AS is_readonly,
    toString(is_session_expired) AS is_session_expired,
    toString(absolute_delay)     AS absolute_delay,
    toString(total_replicas)     AS total_replicas,
    toString(active_replicas)    AS active_replicas,
    toString(lost_part_count)    AS lost_part_count
FROM system.replicas
ORDER BY database, table`
