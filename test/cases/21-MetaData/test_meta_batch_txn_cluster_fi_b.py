###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-


"""Cluster batch meta txn: fault injection tests (s68-s69).

Requires: ./ci/pytest.sh pytest ... -N 3 -M 3

Tests cover:
  - VNode restart mid-vacuum (s68)
  - MNode leader switch before vacuum broadcast (s69)

Split from test_meta_batch_txn_cluster_fi.py to keep per-file execution
time manageable.  s66-s67 → cluster_fi.py, s70-s71 → cluster_fi_c.py.
"""

from new_test_framework.utils import tdLog, tdSql, tdCom, sc, clusterComCheck
import time

class TestBatchMetaTxnClusterFIB:
    """Cluster batch meta txn: fault injection (s68-s69)."""

    updatecfgDict = {
        "supportVnodes": "1000",
    }

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)


    def _get_vgroup_leader_dnode(self, db_name, vgId, timeout=30):
        """Get the dnode ID of the vgroup leader, with retry."""
        for attempt in range(timeout):
            tdSql.query(f"show {db_name}.vgroups")
            for i in range(tdSql.queryRows):
                if tdSql.queryResult[i][0] == vgId:
                    row = tdSql.queryResult[i]
                    for j in range(len(row)):
                        if row[j] == 'leader':
                            return row[j - 1]
            if attempt < timeout - 1:
                time.sleep(1)
        return None


    def _poll_table_count(self, expected, db_name="txn_cdb", timeout=180):
        """Poll 'show tables' until expected row count or timeout."""
        last_count = -1
        for i in range(timeout):
            time.sleep(1)
            try:
                tdSql.execute(f"use {db_name}")
                tdSql.query("show tables")
                last_count = tdSql.queryRows
                if last_count == expected:
                    tdLog.info(f"Table count reached {expected} after {i+1}s")
                    return True
            except Exception as e:
                tdLog.info(f"_poll_table_count: query failed at {i+1}s: {e}")
                continue
        tdLog.exit(f"Table count {last_count} != expected {expected} after {timeout}s")
        return False

    # =========================================================================
    # s68: Fault injection — VNode restart mid-vacuum
    #
    # Large txn COMMIT, then immediately kill VNode leader. On restart
    # taosd finds txn.idx entries + pTxnFinalIdx COMMITTED and re-runs
    # vacuum. All tables must reach final promoted state.
    #
    # Optimization: use a single batch 'create table' for all 50 tables.
    # 50 < TSDB_TXN_INLINE_THRESHOLD(64) → inline synchronous vacuum, so the
    # _poll_table_count returns quickly after VNode restart instead of waiting
    # up to 120s for async vacuum to drain.
    # =========================================================================

    def s68_fi_vnode_restart_mid_vacuum(self):
        db = "txn_fi68"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 2 replica 3 keep 36500")
        tdSql.execute(f"use {db}")
        tdLog.info("======== s68_fi_vnode_restart_mid_vacuum")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        num_tables = 50
        tdSql.execute("BEGIN")
        # Batch create: all 50 tables in one statement (50 < inline_threshold=64,
        # so vacuum is synchronous — avoids up to 120s of async vacuum polling).
        parts = [f"ct_{i} using stb tags({i})" for i in range(num_tables)]
        tdSql.execute("create table " + " ".join(parts))
        tdSql.execute("COMMIT")

        tdSql.query(f"show {db}.vgroups")
        vgId = tdSql.queryResult[0][0]
        leader_dnode = self._get_vgroup_leader_dnode(db, vgId)
        tdLog.info(f"Killing VNode leader (dnode {leader_dnode}) mid-vacuum")
        sc.dnodeForceStop(leader_dnode)
        time.sleep(1)

        sc.dnodeStart(leader_dnode)
        clusterComCheck.checkDnodes(3, timeout=60)

        self._poll_table_count(num_tables, db_name=db, timeout=120)

        tdSql.execute(f"use {db}")
        tdSql.execute("insert into ct_0 values(now, 0)")
        tdSql.execute("insert into ct_49 values(now, 49)")
        tdSql.query("select count(*) from stb")
        tdSql.checkData(0, 0, 2)

        tdSql.execute(f"drop database {db}")
        tdLog.info("s68 PASSED")

    # =========================================================================
    # s69: Fault injection — MNode leader switch between COMMIT and vacuum broadcast
    #
    # MNode writes pTxnFinalIdx (COMMITTED) then is killed before sending
    # vacuum broadcast. New MNode leader reads pTxnFinalIdx and re-broadcasts.
    # Vacuum must complete correctly.
    # =========================================================================

    def s69_fi_mnode_leader_switch_before_vacuum_broadcast(self):
        db = "txn_fi69"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 2 replica 3 keep 36500")
        tdSql.execute(f"use {db}")
        tdLog.info("======== s69_fi_mnode_leader_switch_before_vacuum_broadcast")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        tdSql.query("select * from information_schema.ins_mnodes")
        mnode_leader_id = None
        for i in range(tdSql.queryRows):
            if tdSql.queryResult[i][2] == 'leader':
                mnode_leader_id = tdSql.queryResult[i][0]
                break
        assert mnode_leader_id is not None, "No MNode leader found"
        tdLog.info(f"MNode leader: dnode {mnode_leader_id}")

        tdSql.execute("BEGIN")
        for i in range(10):
            tdSql.execute(f"create table ct_{i} using stb tags({i})")
        tdSql.execute("COMMIT")
        sc.dnodeForceStop(mnode_leader_id)
        tdLog.info(f"Killed MNode leader (dnode {mnode_leader_id}) after COMMIT")

        new_mnode_leader = None
        for _ in range(30):
            time.sleep(1)
            try:
                tdSql.query("select * from information_schema.ins_mnodes")
                for i in range(tdSql.queryRows):
                    if tdSql.queryResult[i][2] == 'leader' and \
                       tdSql.queryResult[i][0] != mnode_leader_id:
                        new_mnode_leader = tdSql.queryResult[i][0]
                        break
                if new_mnode_leader:
                    tdLog.info(f"New MNode leader: dnode {new_mnode_leader}")
                    break
            except Exception:
                continue
        assert new_mnode_leader is not None, "No new MNode leader elected"

        sc.dnodeStart(mnode_leader_id)
        clusterComCheck.checkDnodes(3)

        self._poll_table_count(10, db_name=db)

        tdSql.execute(f"use {db}")
        tdSql.execute("insert into ct_0 values(now, 100)")
        tdSql.query("select v from ct_0")
        tdSql.checkData(0, 0, 100)

        tdSql.execute(f"drop database {db}")
        tdLog.info("s69 PASSED")


    def test_meta_batch_txn_cluster_fi_b(self):
        """Cluster batch meta txn: fault injection (s68-s69)

        68. fi_vnode_restart_mid_vacuum
        69. fi_mnode_leader_switch_before_vacuum_broadcast

        Since: v3.3.6.0
        Labels: common,ci
        """
        self.s68_fi_vnode_restart_mid_vacuum()
        self.s69_fi_mnode_leader_switch_before_vacuum_broadcast()
