package diagnose

// 检查身份。
const (
	detachedPartsCheckID     = "detached_parts"
	detachedPartsRuleVersion = "1"
	// defaultDetachedCountThreshold 单表 detached part 数量的判定阈值。
	// 10 与"少量手工 detach 属于正常运维动作"的经验对齐；超过它说明
	// 该表存在批量 detach（备份演练、批量清理）或服务端反复自行下线
	// part，值得人工核查。阈值不是服务端限制。
	defaultDetachedCountThreshold = 10
)

// detachedPartsSQL 是固定查询：列出每个节点上用户库表的 detached part。
//
// 约束：
//   - 只使用最古老版本就存在的列（database/table/name/reason/disk/
//     bytes_on_disk）；
//   - reason 在服务端 coalesce 为空串：NULL reason 的语义就是"用户通过
//     ALTER 手工 detach"，与空串判定规则完全一致；同时规避 database/sql
//     层的已知限制——clickhouse-go 对 Nullable 列下发的 driver.Value 是
//     *string，无法直接 Scan 进 sql.NullString（26.10 + HTTP 路径实测）；
//   - system / information_schema 下的 detached part（例如服务日志表的
//     broken-on-start 残留）不进结果：26.10 真机上这类残留动辄几十条，
//     属于环境噪音而非用户数据风险；
//   - 服务端只提供事实（含 reason 原始值）；判定规则与阈值属于客户端契约；
//   - SQL 自身不加 LIMIT：detached part 天然稀少，扫描成本由单节点
//     超时与 context 控制。
const detachedPartsSQL = `SELECT
    database,
    table,
    name,
    coalesce(reason, '')   AS reason,
    disk,
    toString(bytes_on_disk) AS bytes_on_disk
FROM system.detached_parts
WHERE database NOT IN ('system', 'INFORMATION_SCHEMA', 'information_schema', 'INFCACHE')
ORDER BY database, table, name`
