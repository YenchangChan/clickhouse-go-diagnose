package diagnose

import "context"

// Rows 是检查从查询结果读取行的最小接口。
//
// 实现方约定：
//   - Next 必须遵守传入 Query 的 context：取消后尽快返回 false；
//   - 迭代过程或结束时的错误通过 Err 报告，不得静默结束；
//   - Close 必须幂等，且不得关闭调用方的共享连接；
//   - Scan 必须支持 *sql.NullString 目标：检查以 NullString 读取四个
//     字符串投影列（database, table, new_part_name, last_exception）。
//     注意 clickhouse-go v2 的 ScanRow 不支持 *any 目标，NullString 在
//     clickhouse-go v2 与 database/sql 两条路径上都受支持。
//
// 库无法强制实现方遵守 context。适配器必须真实传递 context，
// 不能用后台 goroutine 提前返回来伪装取消；不遵守的实现会导致检查
// 超出预算仍未结束，此类行为由适配器负责。
type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
	Close() error
}

// Querier 是检查在单个目标节点上执行查询的入口。
// 集群发现、凭据、TLS 与连接生命周期由调用方管理；库只调用 Query，
// 并保证对返回的 Rows 调用 Close。
//
// 注意 Go 方法返回类型必须精确匹配：返回 driver.Rows 或 sql.Rows 的
// 方法不会自动满足本接口，各路径需要薄适配器。
type Querier interface {
	Query(ctx context.Context, query string, args ...any) (Rows, error)
}
