package diagnose

import "strings"

// 检查身份。
const (
	checkID     = "replication_queue_failures"
	ruleVersion = "v0.1"
)

// exceptionMarker 是本检查识别的异常特征。异常文本可能携带完整堆栈等上下文，
// 因此在 last_exception 上做不区分大小写的包含匹配，而不是全等。
const exceptionMarker = "NO_REPLICA_HAS_PART"

// replicationQueueSQL 是固定查询。只投影必需的四个字段，按稳定字段排序，
// 保证截断时保留的是确定的前缀。
//
// 约束：
//   - 不使用 SELECT *，不用模型生成 SQL；
//   - 不使用 result_overflow_mode='break' 之类的静默截断设置；行数上限由
//     客户端 N+1 读取策略实现（见 replication_queue.go）；
//   - SQL 自身不加 LIMIT：LIMIT 只限制输出不代表限制扫描工作，扫描成本由
//     单节点超时与 context 控制。
//
// 兼容性尚未在真实服务器上验证（见 docs/compatibility.md）；列集与
// positionCaseInsensitive 的可用版本范围在集成阶段确认后固定。
const replicationQueueSQL = `SELECT database, table, new_part_name, last_exception
FROM system.replication_queue
WHERE positionCaseInsensitive(last_exception, 'NO_REPLICA_HAS_PART') > 0
ORDER BY database, table, new_part_name, last_exception`

// matchesException 判断一行队列记录是否命中本检查识别的异常。
// 空异常不匹配。与 SQL 端 positionCaseInsensitive 语义一致。
func matchesException(lastException string) bool {
	return strings.Contains(strings.ToUpper(lastException), exceptionMarker)
}
