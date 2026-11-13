package diagnose

// 检查身份。
const (
	diskSpaceCheckID     = "disk_space_low"
	diskSpaceRuleVersion = "1"
)

// diskSpaceSQL 是固定查询：列出每个节点上的磁盘及其容量水位。
//
// 约束：
//   - 只使用最古老版本就存在的列（name/path/free_space/total_space/
//     keep_free_space）；26.10 上的 unreserved_space、is_broken 等列
//     属于较新版本，不进固定查询；
//   - 容量由服务端 toString 下发为十进制字符串，统一走 NullString 扫描，
//     客户端解析为 uint64；不在 SQL 里算百分比——判定规则与阈值属于
//     客户端契约，服务端只提供事实；
//   - SQL 自身不加 LIMIT：磁盘数量很少，扫描成本由单节点超时与 context 控制。
const diskSpaceSQL = `SELECT
    name,
    path,
    toString(free_space)      AS free_bytes,
    toString(total_space)     AS total_bytes,
    toString(keep_free_space) AS keep_free_bytes
FROM system.disks
ORDER BY name, path`
