package diagnose

// tooManyPartsSQL 是 TooManyParts 检查的固定查询。
//
// 能力边界：
//   - 只统计 active part；非活跃（已过期/被替换）part 不构成写入压力；
//   - 服务端完成聚合（GROUP BY partition），客户端只做阈值判定；
//   - 计数与字节数用 toString 下发为字符串列：统一走 NullString 扫描，
//     不给驱动数值列留下兼容面；
//   - 排除 system / information_schema 系库；不排除 .inner 表（物化视图
//     内表 part 极少，误报可由阈值与人工核查兜住，不为猜测加规则）；
//   - 不设置任何会话级截断/溢出设置，结果完整性由客户端 N+1 检测保证。
const tooManyPartsSQL = `
SELECT
    database,
    table,
    partition,
    toString(count())            AS parts,
    toString(sum(bytes_on_disk)) AS disk_bytes
FROM system.parts
WHERE active
  AND database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema', 'INFCACHE')
GROUP BY database, table, partition
ORDER BY database, table, partition`
