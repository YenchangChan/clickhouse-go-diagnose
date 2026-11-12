#!/usr/bin/env python3
"""在真实 ClickHouse 集群上注入一次 NO_REPLICA_HAS_PART 故障。

背景：真实世界的 NO_REPLICA_HAS_PART 出现在「副本 A 的队列需要某个 part，
但被指定为来源的副本 B 已经没有这个 part」的跨副本 fetch 路径上。
因此这里在同一台服务器上用两个副本（r1/r2）共享同一 Keeper 路径，
让 r2 去 fetch 一个 r1 根本不存在的 part。

步骤：
  1. 建 __diagnose_it.fault_r1 / fault_r2（ReplicatedMergeTree，同一 Keeper 路径）；
  2. 向 r1 插入数据，等 r2 同步；
  3. SYSTEM STOP REPLICATION QUEUES（只有全局语法）暂停回放；
  4. 再插一条数据，从 Keeper 读到真实 log 条目作为格式模板；
  5. 向 log 追加一条 merge 条目：源 part 为两个任何副本都不存在的 part；
     （get 条目不行：实测会被副本判为过期并改写为 drop，队列不报错）
  6. 恢复回放：两个副本都拿不到 merge 的源 part → 队列条目带着
     真实的 "No replica has part" 异常停留在 system.replication_queue。

用法：
  CH_ADDR=192.168.56.101:8123 CH_USER=default CH_PASSWORD=123456 \
  ZK_ADDR=192.168.56.101:9181 \
  python3 inject_no_replica_has_part.py [--cleanup]

--cleanup：DROP 两个表并删除 Keeper 路径，恢复集群原状。
"""
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

from kazoo.client import KazooClient
from kazoo.retry import KazooRetry

CH_ADDR = os.environ.get("CH_ADDR", "192.168.56.101:8123")
CH_USER = os.environ.get("CH_USER", "default")
CH_PASSWORD = os.environ.get("CH_PASSWORD", "")
ZK_ADDR = os.environ.get("ZK_ADDR", "192.168.56.101:9181")

DB = "__diagnose_it"
TABLE_R1 = "fault_r1"
TABLE_R2 = "fault_r2"
REPLICA_R1 = "r1"
REPLICA_R2 = "r2"
# 固定 Keeper 路径（{uuid} 宏要求建表时显式给 UUID 或走 ON CLUSTER）。
ZK_BASE = "/clickhouse/tables/diagnose-it/fault_table"

# 显式禁用代理：环境里的 HTTP_PROXY 会把请求转发到本地代理，
# 导致 ClickHouse 收到的路径异常（与 clickhouse-go HTTP 路径同样的坑）。
_OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))


def ch(sql: str, database: str = "default") -> str:
    url = f"http://{CH_ADDR}/"
    params = urllib.parse.urlencode({"user": CH_USER, "password": CH_PASSWORD, "database": database})
    req = urllib.request.Request(f"{url}?{params}", data=sql.encode())
    try:
        with _OPENER.open(req, timeout=15) as resp:
            return resp.read().decode()
    except urllib.error.HTTPError as e:
        raise RuntimeError(f"CH {e.code}: {e.read().decode(errors='replace')} | SQL: {sql[:120]}") from None


def zk_client() -> KazooClient:
    return KazooClient(hosts=ZK_ADDR, connection_retry=KazooRetry(max_tries=5, delay=0.5))


def wait_queue_drained(timeout: float = 20.0) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        n = int(ch("SELECT count() FROM system.replication_queue").strip())
        if n == 0:
            return True
        time.sleep(1)
    return False


def main(cleanup: bool) -> int:
    if cleanup:
        for t in (TABLE_R1, TABLE_R2):
            ch(f"DROP TABLE IF EXISTS {DB}.{t}")
        ch(f"DROP DATABASE IF EXISTS {DB}")
        zk = zk_client()
        zk.start(timeout=15)
        try:
            zk.delete(ZK_BASE, recursive=True)
        finally:
            zk.stop()
            zk.close()
        print("cleanup done")
        return 0

    # 1. 两个副本共享同一 Keeper 路径
    ch(f"CREATE DATABASE IF NOT EXISTS {DB}")
    for name, replica in ((TABLE_R1, REPLICA_R1), (TABLE_R2, REPLICA_R2)):
        ch(
            f"CREATE TABLE IF NOT EXISTS {DB}.{name} (a Int64) "
            f"ENGINE = ReplicatedMergeTree('{ZK_BASE}', '{replica}') ORDER BY a"
        )
    print(f"tables ready, zk base = {ZK_BASE}")

    # 2. 先让两个副本都有数据、队列清空
    ch(f"INSERT INTO {DB}.{TABLE_R1} SELECT toInt64(rand64())")
    if not wait_queue_drained():
        print("WARN: 队列 20s 内未清空，继续（注入前会再次检查）")

    # 3. 暂停回放（只有全局语法，没有按表版本）
    ch("SYSTEM STOP REPLICATION QUEUES")
    try:
        # 4. 插入新 part 产生真实 log 条目，作为格式模板
        #    随机值：相同块会被复制表去重，不会产生新条目。
        ch(f"INSERT INTO {DB}.{TABLE_R1} SELECT toInt64(rand64())")
        time.sleep(2)
        doc = json.loads(
            ch(
                "SELECT name, value FROM system.zookeeper "
                f"WHERE path='{ZK_BASE}/log' FORMAT JSONCompact"
            )
        )
        rows = [(r[0], r[1]) for r in doc["data"]]
        print("log nodes:", [n for n, _ in rows])
        real_entries = [v for n, v in rows if n.startswith("log-")]
        if not real_entries:
            print("ERROR: 没有拿到真实 log 条目样例，中止")
            return 1

        template = real_entries[-1]
        # 26.x 条目是多行 key-value（"format version: 4\n...\nget\nall_1_1_0\npart_type: ..."），
        # 不是老的 TSV。实测得到的 merge 条目形态：
        #   ...\nmerge\n<源part...>\ninto\n<新part>\ndeduplicate: 0\npart_type: Compact\n
        #
        # 为什么用 merge 而不是 get：实测 get 一个「任何副本都没有」的 part 时，
        # 副本会把它判为过期条目并改写为 drop 条目（日志里能看到 source replica 自己
        # 写的 "drop all_x"），队列不会报错。merge 条目则必须拿到全部源 part，
        # 任何副本都没有时会持续重试并报 NO_REPLICA_HAS_PART。
        header = template.split("\nget\n")[0]  # "format version: ..\ncreate_time: ..\nsource replica: r1\nblock_id: "
        injected = (
            header
            + "\nmerge\nall_7_7_7_7\nall_8_8_8_8\ninto\nall_7_8_1\ndeduplicate: 0\npart_type: Compact\n"
        )
        print("injected entry:", repr(injected))

        # 节点名必须是顺序数字：非数字后缀会让 replica 读日志直接报解析错误。
        seq = max(int(nm.split("-", 1)[1]) for nm, _ in rows if nm.startswith("log-")) + 1
        path = f"{ZK_BASE}/log/log-{seq:010d}"
        zk = zk_client()
        zk.start(timeout=15)
        try:
            if zk.exists(path):
                zk.delete(path, recursive=True)
            zk.create(path, injected.encode())
            print("zk node created:", path)
        finally:
            zk.stop()
            zk.close()
    finally:
        # 5. 无论成功与否都恢复回放，避免把集群队列留在停止状态
        ch("SYSTEM START REPLICATION QUEUES")

    print("queue replay resumed; 等待 r2 fetch 失败...")
    deadline = time.time() + 30
    while time.time() < deadline:
        out = ch(
            "SELECT table, new_part_name, num_tries, last_exception "
            "FROM system.replication_queue "
            "WHERE positionCaseInsensitive(last_exception, 'NO_REPLICA_HAS_PART') > 0 "
            "FORMAT TabSeparated"
        ).strip()
        if out:
            print("NO_REPLICA_HAS_PART 已出现：")
            print(out)
            return 0
        time.sleep(2)
    print("ERROR: 30s 内未出现 NO_REPLICA_HAS_PART")
    return 1


if __name__ == "__main__":
    sys.exit(main(cleanup="--cleanup" in sys.argv))
