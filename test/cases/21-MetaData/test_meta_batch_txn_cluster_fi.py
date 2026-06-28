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


"""Cluster batch meta txn: fault injection tests (s66-s67).

Requires: ./ci/pytest.sh pytest ... -N 3 -M 3

Tests cover:
  - Leader switch during vacuum (s66)
  - Concurrent DROP during vacuum (s67)

Split from original s66-s71 file to keep per-file execution under 200s.
  s68-s69 → test_meta_batch_txn_cluster_fi_b.py
  s70-s71 → test_meta_batch_txn_cluster_fi_c.py
"""

from new_test_framework.utils import tdLog, tdSql, tdCom, sc, clusterComCheck
import time

class TestBatchMetaTxnClusterFI:
    """Cluster batch meta txn: fault injection (s66-s67)."""

    updatecfgDict = {
        "supportVnodes": "1000",
    }

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)


    def _reset_env(self, db_name="txn_cdb"):
        """Reset test database. Uses replica 3 for VNode HA tests."""
        tdSql.execute(f"drop database if exists {db_name}")
        tdSql.execute(f"create database {db_name} vgroups 2 replica 3 keep 36500")
        tdSql.execute(f"use {db_name}")


    def _get_mnode_leader_dnode_id(self):
        """Get the dnode ID of the current MNode leader."""
        tdSql.query("select * from information_schema.ins_mnodes")
        for i in range(tdSql.queryRows):
            if tdSql.queryResult[i][2] == 'leader':
                return tdSql.queryResult[i][0]      # id column (1-based dnode id)
        return None


    def _get_vgroup_leader_dnode(self, db_name, vgId, timeout=30):
        """Get the dnode ID of the vgroup leader, with retry."""
        for attempt in range(timeout):
            tdSql.query(f"show {db_name}.vgroups")
            for i in range(tdSql.queryRows):
                if tdSql.queryResult[i][0] == vgId:
                    row = tdSql.queryResult[i]
                    for j in range(len(row)):
                        if row[j] == 'leader':
                            return row[j - 1]           # dnode id is the column before status
            if attempt < timeout - 1:
                time.sleep(1)
        return None


    def _wait_mnode_leader_elected(self, timeout=30):
        """Wait for any MNode leader to be elected (ignoring offline nodes)."""
        for i in range(timeout):
            time.sleep(1)
            try:
                tdSql.query("select * from information_schema.ins_mnodes")
                for r in range(tdSql.queryRows):
                    if tdSql.queryResult[r][2] == 'leader':
                        tdLog.info(f"MNode leader found: dnode {tdSql.queryResult[r][0]} after {i+1}s")
                        return True
            except Exception:
                continue
        tdLog.exit(f"No MNode leader elected within {timeout}s")
        return False

    # =========================================================================
    # s40: MNode leader switch during active txn -> COMMIT succeeds
    # =========================================================================

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


    def _get_table_name_set(self, db_name):
        """Return current table names as a set for exact object-level assertions."""
        tdSql.execute(f"use {db_name}")
        tdSql.query("show tables")
        return set(tdSql.queryResult[i][0] for i in range(tdSql.queryRows))

    # =========================================================================
    # s42: Client disconnect -> txn auto-rollback after timeout
    # =========================================================================

    def s66_fi_leader_switch_during_vacuum(self):
        db = "txn_fi66"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 2 replica 3 keep 36500")
        tdSql.execute(f"use {db}")
        tdLog.info("======== s66_fi_leader_switch_during_vacuum")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        tdSql.execute("BEGIN")
        for i in range(10):
            tdSql.execute(f"create table ct_{i} using stb tags({i})")
        tdSql.execute("COMMIT")

        tdSql.query(f"show {db}.vgroups")
        vgId = tdSql.queryResult[0][0]
        leader_dnode = self._get_vgroup_leader_dnode(db, vgId)
        tdLog.info(f"Killing VNode leader (dnode {leader_dnode}) immediately after COMMIT")
        sc.dnodeForceStop(leader_dnode)

        new_leader = self._get_vgroup_leader_dnode(db, vgId, timeout=30)
        assert new_leader is not None, "No new VNode leader elected"

        sc.dnodeStart(leader_dnode)
        clusterComCheck.checkDnodes(3)

        self._poll_table_count(10, db_name=db)

        tdSql.execute(f"use {db}")
        for i in range(10):
            tdSql.execute(f"insert into ct_{i} values(now, {i})")
        tdSql.query("select count(*) from stb")
        tdSql.checkData(0, 0, 10)

        tdSql.execute(f"drop database {db}")
        tdLog.info("s66 PASSED")

    # =========================================================================
    # s67: Fault injection — Concurrent DROP on table being vacuumed (PRE_CREATE)
    #
    # COMMIT creates ct_target (PRE_CREATE). DROP arrives while vacuum is
    # promoting it. Both orderings (vacuum-first and drop-first) must not
    # corrupt pUidIdx/pTbDb.
    # =========================================================================

    def s67_fi_concurrent_drop_during_vacuum(self):
        db = "txn_fi67"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 2 replica 3 keep 36500")
        tdSql.execute(f"use {db}")
        tdLog.info("======== s67_fi_concurrent_drop_during_vacuum")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        tdSql.execute("BEGIN")
        tdSql.execute("create table ct_target using stb tags(99)")
        tdSql.execute("COMMIT")

        try:
            tdSql.execute("drop table ct_target")
        except Exception:
            pass  # PRE_CREATE still invisible — correct

        time.sleep(3)

        tdSql.execute(f"use {db}")
        tdSql.query("show tables")
        names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert "ct_target" not in names, "ct_target must not exist after DROP"

        # Schema must be intact
        tdSql.execute("create table ct_safe using stb tags(1)")
        tdSql.execute("insert into ct_safe values(now, 1)")
        tdSql.query("select v from ct_safe")
        tdSql.checkData(0, 0, 1)

        tdSql.execute(f"drop database {db}")
        tdLog.info("s67 PASSED")



    def test_meta_batch_txn_cluster_fi(self):
        """Cluster batch meta txn: fault injection (s66-s67)

        66. fi_leader_switch_during_vacuum
        67. fi_concurrent_drop_during_vacuum

        Split from s66-s71: s68-s69 → cluster_fi_b.py, s70-s71 → cluster_fi_c.py

        Since: v3.3.6.0
        Labels: common,ci
        """
        self.s66_fi_leader_switch_during_vacuum()
        self.s67_fi_concurrent_drop_during_vacuum()
