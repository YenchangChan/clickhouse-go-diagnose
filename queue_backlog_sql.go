package diagnose

// 检查身份。
const (
	queueBacklogCheckID     = "replication_queue_backlog"
	queueBacklogRuleVersion = "1"
)

// queueBacklogSQL 是固定查询：服务端按 (库, 表) 聚合当前节点复制队列的
// 积压特征，客户端只做阈值判定。
//
// 约束：
//   - 只使用最古老版本就存在的列（database/table/create_time/num_tries/
//     last_exception）；26.10 上没有 postponed_due_to_error 等诊断列，
//     证明依赖古老列是必要的，本查询不引用它们；
//   - 最老条目年龄由服务端 dateDiff 计算并下发为字符串秒数：避免客户端
//     解析 DateTime 的时区歧义，统一走 NullString 扫描；
//   - 不加 WHERE：积压判定覆盖全部条目类型（get/merge/mutate/clone 等），
//     在途正常条目造成的噪音由阈值消化；
//   - SQL 自身不加 LIMIT：LIMIT 只限制输出不代表限制扫描工作，扫描成本
//     由单节点超时与 context 控制；
//   - 别名不得遮蔽被引用的列名（真机验证发现别名遮蔽会让 WHERE 语义
//     漂移），queue_depth 等别名与列名无冲突。
//
// failing_entries 统计的是"带异常文本的在队条目数"，作为背景证据随观测
// 保留；具体异常的识别归 CheckReplicationQueueFailures，本检查只回答
// "队列是否消化不良"。
const queueBacklogSQL = `SELECT
    database,
    table,
    toString(count())                                     AS queue_depth,
    toString(dateDiff('second', min(create_time), now())) AS oldest_age,
    toString(max(num_tries))                              AS max_tries,
    toString(countIf(last_exception != ''))               AS failing_entries
FROM system.replication_queue
GROUP BY database, table
ORDER BY database, table`
