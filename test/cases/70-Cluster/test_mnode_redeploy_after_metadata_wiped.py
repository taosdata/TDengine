# Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
#
# SPDX-License-Identifier: AGPL-3.0-or-later
#
# This program is free software: you can use, redistribute, and/or modify
# it under the terms of the GNU Affero General Public License, version 3
# or later ("AGPL"), as published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import glob
import os
import time

from new_test_framework.utils import tdLog, tdSql, sc, tdDnodes, clusterComCheck


# ---------------------------------------------------------------------------
# Paths (single node, dnode index 1). Data dir layout:
#   <dnodeDir>/data/mnode/data/sdb.data   <- sdb snapshot
#   <dnodeDir>/data/mnode/wal/*           <- mnode wal
#   <dnodeDir>/data/dnode/dnode.json      <- dnode identity (dnodeId/clusterId)
#   <dnodeDir>/data/dnode/dnode.info      <- dnode reserve info
# ---------------------------------------------------------------------------
DNODE = 1


def _data_dir():
    return os.path.join(tdDnodes.getDnodeDir(DNODE), "data")


def _wipe_core_metadata():
    """Reproduce an interrupted-first-deploy on-disk state.

    Removing sdb.data + the whole mnode wal leaves nothing to replay, so the
    single mnode restores an empty sdb and ensure-default must recreate every
    default object (cluster/dnode/mnode/user...). Removing dnode.json/.info
    clears the persisted dnode identity so dnodeId/clusterId start from 0 and
    self-heal through the first status round-trip. A fresh clusterId proves the
    redeploy path actually ran (vs. a WAL replay of the old data).
    """
    data = _data_dir()
    targets = [
        os.path.join(data, "mnode", "data", "sdb.data"),
        os.path.join(data, "dnode", "dnode.json"),
        os.path.join(data, "dnode", "dnode.info"),
    ]
    targets += glob.glob(os.path.join(data, "mnode", "wal", "*"))

    for path in targets:
        if os.path.exists(path):
            os.remove(path)
            tdLog.info(f"removed {path}")
        else:
            tdLog.info(f"skip (absent) {path}")


def _wait_default_objects(timeout=90):
    """Wait until ensure-default has recreated every core object and the dnode is online.

    After the wipe, ensure-default runs asynchronously on the mnode write queue after the
    restore finishes and recreates the default sdb objects one raft trans at a time
    (cluster is the last one). A dnode that already reports ready / alive therefore does
    not prove the cluster, users and dnode rows exist yet; on a loaded (ASan, concurrent CI)
    host this window stretches to many seconds, during which queries see 'Invalid user',
    'Dnode does not exist', 'Out of dnodes' or an empty ins_cluster.
    """
    checks = [
        ("cluster", "select id from information_schema.ins_cluster"),
        ("mnodes", "select id from information_schema.ins_mnodes"),
        ("dnodes", "select id from information_schema.ins_dnodes where status = 'ready'"),
        ("users", "select name from information_schema.ins_users"),
    ]
    pending = [name for name, _ in checks]
    last_pending = None
    start = time.time()
    while time.time() - start < timeout:
        pending = []
        for name, sql in checks:
            try:
                tdSql.query(sql, queryTimes=1)
                if tdSql.queryRows == 0:
                    pending.append(name)
            except Exception as e:
                tdLog.info(f"{name} not queryable yet: {e}")
                pending.append(name)
        if not pending:
            tdLog.info(f"default objects ready within {time.time() - start:.1f}s")
            return
        # log only when progress changes, so the log shows whether ensure-default is advancing or stuck
        if pending != last_pending:
            tdLog.info(f"[{time.time() - start:.1f}s] default objects still missing: {pending}")
            last_pending = pending
        time.sleep(1)
    assert False, f"default objects still missing after {timeout}s: {pending}"


def _get_cluster_id():
    tdSql.query("select id from information_schema.ins_cluster")
    assert tdSql.queryRows == 1, f"ins_cluster should have 1 row, got {tdSql.queryRows}"
    return tdSql.queryResult[0][0]


def _check_core_shows_non_empty():
    """show mnodes / dnodes / cluster / users must all return non-empty."""
    for sql, name in [
        ("select * from information_schema.ins_mnodes", "mnodes"),
        ("select * from information_schema.ins_dnodes", "dnodes"),
        ("select * from information_schema.ins_cluster", "cluster"),
        ("select * from information_schema.ins_users", "users"),
    ]:
        tdSql.query(sql)
        assert tdSql.queryRows > 0, f"{name} returned empty after redeploy"
        tdLog.info(f"{name}: {tdSql.queryRows} row(s)")

    # mnode must present itself as leader: selfDnodeId must match its object id
    # (id=1). Regression guard for the ensure-default selfDnodeId restore.
    # The role converges from 'offline' to 'leader' shortly after restart, so
    # poll until it settles instead of asserting immediately.
    _wait_mnode_leader()


def _wait_mnode_leader(timeout=15):
    role = None
    for _ in range(timeout):
        tdSql.query("select id, `role` from information_schema.ins_mnodes")
        if tdSql.queryRows > 0:
            role = tdSql.queryResult[0][1]
            if role == "leader":
                tdLog.info("mnode role: leader")
                return
        time.sleep(1)
    assert False, f"mnode role expected 'leader' within {timeout}s, last got '{role}'"


def _basic_read_write(tag):
    db = f"redeploy_{tag}"
    tdSql.execute(f"drop database if exists {db}")
    tdSql.execute(f"create database {db} vgroups 2 keep 36500d")
    clusterComCheck.checkDbReady(db)
    tdSql.execute(f"use {db}")
    tdSql.execute("create table st (ts timestamp, v int) tags (g int)")
    tdSql.execute("create table t1 using st tags (1)")

    base = 1780000000000
    for i in range(10):
        tdSql.execute(f"insert into t1 values ({base + i * 1000}, {i})")

    tdSql.query("select count(*), sum(v) from t1")
    tdSql.checkData(0, 0, 10)
    tdSql.checkData(0, 1, 45)
    tdLog.info(f"[{tag}] basic create/insert/query ok on {db}")


class TestMnodeRedeployAfterMetadataWiped:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_mnode_redeploy_after_metadata_wiped(self):
        """Single mnode redeploys and self-heals after core metadata is wiped

        1. Fresh single-node cluster; record the original clusterId.
        2. Stop the dnode.
        3. Delete mnode/data/sdb.data, mnode/wal/*, dnode/dnode.json,
           dnode/dnode.info (interrupted-first-deploy on-disk state).
        4. Restart: cluster must come back ready. ensure-default recreates all
           default objects and a NEW clusterId is generated (proves redeploy,
           not WAL replay). show mnodes/dnodes/cluster/users all non-empty, and
           the local mnode reports role 'leader' (selfDnodeId restore guard).
        5. Basic create db/table + insert + query works.
        6. Restart once more: still works, clusterId now stable.

        Since: v3.3.6.x

        Labels: ci,cluster,integration,functional

        Jira: None

        History:
            - 2026-07-15 kailixu Created

        """
        clusterComCheck.checkDnodes(1)
        old_cluster_id = _get_cluster_id()
        tdLog.info(f"original clusterId: {old_cluster_id}")

        # --- wipe core metadata and restart ---
        sc.dnodeStop(DNODE)
        _wipe_core_metadata()
        sc.dnodeStart(DNODE)

        _wait_default_objects()
        clusterComCheck.checkDnodes(1)
        clusterComCheck.checkClusterAlive(1)

        new_cluster_id = _get_cluster_id()
        tdLog.info(f"clusterId after redeploy: {new_cluster_id}")
        assert new_cluster_id != old_cluster_id, (
            "clusterId should change after wiping sdb.data + wal "
            f"(old={old_cluster_id}, new={new_cluster_id})"
        )

        _check_core_shows_non_empty()
        _basic_read_write("first")

        # --- restart again: everything stable, clusterId unchanged now ---
        sc.dnodeStop(DNODE)
        sc.dnodeStart(DNODE)
        _wait_default_objects()
        clusterComCheck.checkDnodes(1)
        clusterComCheck.checkClusterAlive(1)

        stable_cluster_id = _get_cluster_id()
        assert stable_cluster_id == new_cluster_id, (
            "clusterId must be stable across a normal restart "
            f"(was={new_cluster_id}, now={stable_cluster_id})"
        )
        _check_core_shows_non_empty()
        _basic_read_write("second")

        tdLog.success(f"{__file__} passed")
