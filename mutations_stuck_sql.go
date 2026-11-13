package diagnose

// mutationsStuckSQL 是 MutationsStuck 检查的固定查询。
//
// 数据源语义：system.mutations 每行是一个未完成的 mutation 条目
// （WHERE NOT is_done 过滤已完成项）。失败过但最终成功重试的 mutation
// （is_done=1 且 latest_fail_reason 非空）不再是卡住状态，被服务端
// WHERE 过滤——这是诚实范围：检查报告的是"当前卡住"，不是"曾经失败"。
//
// 能力边界：
//   - 年龄由服务端 dateDiff 计算下发为字符串秒数：避免客户端解析
//     DateTime 的时区歧义，统一走 NullString 扫描；
//   - is_done 的投影别名必须是 done 而非 is_done：ClickHouse 的 WHERE
//     里列名引用会被 SELECT 别名遮蔽，别名成 String 后 `NOT is_done`
//     直接报"Illegal type (String) of argument of function not"（真机实测）；
//   - 只用最古老的列集（database/table/mutation_id/command/is_done/
//     create_time/latest_fail_reason）：不依赖 23.x+ 才有的
//     latest_fail_error_code_name 等列，旧版本服务器也能执行；
//   - 判定在客户端完成：失败文本只判"非空"，不解析符号名或错误码——
//     与异常文本形态解耦，旧版本的自由文本同样成立；
//   - 不排除 system 系库：复制版 system 表的卡住 mutation 同样是真实证据；
//   - command 全文下发（人工核查需要原始命令），可能包含敏感字面量，
//     UI 必须按文本转义展示。
const mutationsStuckSQL = `
SELECT
    database,
    table,
    mutation_id,
    command,
    toString(is_done)                                  AS done,
    toString(dateDiff('second', create_time, now()))   AS age_seconds,
    latest_fail_reason
FROM system.mutations
WHERE NOT is_done
ORDER BY database, table, mutation_id`
