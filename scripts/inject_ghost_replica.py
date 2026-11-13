#!/usr/bin/env python3
"""为 ReplicaStatus 检查注入「幽灵副本」故障：active_replicas < total_replicas。

原理：system.replicas 的 total_replicas 数的是 Keeper 上 replicas/ 目录的
子节点数，active_replicas 数的是其中有活 Keeper 会话的副本。手工在
replicas/ 下创建一个没有任何进程注册的 znode，它就会被计入 total 但
不活跃——这正是「某个副本掉线后复制组的真实视角」。

步骤：
  1. 建 __diagnose_it_rs.rep_t（ReplicatedMergeTree，固定 Keeper 路径，副本 r1）；
  2. 插入数据，等队列清空（r1 完全激活）；
  3. 用 kazoo 在 replicas/ 下创建最小 ghost znode（仅 host 子节点，指向
     不可达地址 192.0.2.1）；实测克隆完整副本结构会被判为 active，缺口不出现；
  4. 验证 system.replicas 上出现 total=2 / active=1。

用法：
  CH_ADDR=192.168.56.101:8123 CH_USER=default CH_PASSWORD=123456 \
  ZK_ADDR=192.168.56.101:9181 \
  python3 inject_ghost_replica.py [--cleanup]

--cleanup：DROP 表并删除 Keeper 路径，恢复集群原状。
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

DB = "__diagnose_it_rs"
TABLE = "rep_t"
REPLICA = "r1"
GHOST = "ghost"
# 固定 Keeper 路径（{uuid} 宏要求建表时显式给 UUID 或走 ON CLUSTER）。
ZK_BASE = "/clickhouse/tables/diagnose-it/rs_table"

# 显式禁用代理：环境里的 HTTP_PROXY 会把请求转发到本地代理。
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


def replicas_view() -> str:
    return ch(
        "SELECT total_replicas, active_replicas, is_readonly, is_session_expired "
        f"FROM system.replicas WHERE database='{DB}' AND table='{TABLE}' FORMAT TabSeparated"
    ).strip()


def main(cleanup: bool) -> int:
    if cleanup:
        ch(f"DROP TABLE IF EXISTS {DB}.{TABLE}")
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

    # 1. 真实副本 r1
    ch(f"CREATE DATABASE IF NOT EXISTS {DB}")
    ch(f"DROP TABLE IF EXISTS {DB}.{TABLE}")
    # DROP TABLE 清理副本 znode 是异步收尾的，紧接 CREATE 会撞
    # REPLICA_ALREADY_EXISTS：先递归清空 Keeper 路径再建表。
    zk = zk_client()
    zk.start(timeout=15)
    try:
        zk.delete(ZK_BASE, recursive=True)
    except Exception:
        pass
    finally:
        zk.stop()
        zk.close()
    time.sleep(2)
    ch(
        f"CREATE TABLE {DB}.{TABLE} (a Int64) "
        f"ENGINE = ReplicatedMergeTree('{ZK_BASE}', '{REPLICA}') ORDER BY a"
    )
    # 随机值：相同块会被复制表去重。
    ch(f"INSERT INTO {DB}.{TABLE} SELECT toInt64(rand64())")
    if not wait_queue_drained():
        print("WARN: 队列 20s 内未清空，继续")
    time.sleep(2)
    print("r1 视图（注入前）:", replicas_view() or "(system.replicas 无行)")

    # 2. 创建最小 ghost 副本
    #
    # 实测结论（26.10.3.7）：
    #   - 只需 replicas/ghost/ + host 子节点即可让 total+1；
    #   - 克隆真实副本的全部子节点（含 is_active 会话标记、指向本机
    #     真实 9009 端口的 host）会被判定为 active——缺口不出现；
    #   - host 指向不可达地址（192.0.2.1 TEST-NET-1）后，副本被稳定
    #     判为不活跃：total=2, active=1。
    zk = zk_client()
    zk.start(timeout=15)
    try:
        ghost_path = f"{ZK_BASE}/replicas/{GHOST}"
        host_data = (
            "host: 192.0.2.1\nport: 9009\ntcp_port: 9000\n"
            f"database: {DB}\ntable: {TABLE}\nscheme: http\n"
        ).encode()
        if zk.exists(ghost_path):
            zk.delete(ghost_path, recursive=True)
        zk.create(ghost_path)
        zk.create(f"{ghost_path}/host", host_data)
        print("ghost znode created:", ghost_path)
    finally:
        zk.stop()
        zk.close()

    # 3. 等服务端感知（表级结构有 Watch 通知，通常秒级）
    deadline = time.time() + 20
    while time.time() < deadline:
        view = replicas_view()
        if view == "2\t1\t0\t0":
            print("注入成功（total=2, active=1）:", view)
            return 0
        time.sleep(1)
    print("ERROR: 20s 内未观察到 total=2/active=1，当前:", replicas_view() or "(无行)")
    return 1


if __name__ == "__main__":
    sys.exit(main(cleanup="--cleanup" in sys.argv))
