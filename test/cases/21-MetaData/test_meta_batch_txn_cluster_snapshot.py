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


"""Cluster batch meta txn: STB lifecycle with cluster restart (s62-s65).

Requires: ./ci/pytest.sh pytest ... -N 3 -M 3

Split from original s52-s65 file to keep per-file execution time manageable.
  s52-s54 -> test_meta_batch_txn_cluster_snapshot_a.py
  s55-s58 -> test_meta_batch_txn_cluster_snapshot_b.py
  s59-s61 -> test_meta_batch_txn_cluster_snapshot_c.py
"""

from new_test_framework.utils import tdLog, tdSql, tdCom, sc, clusterComCheck
import time

class TestBatchMetaTxnClusterSnapshot:
    """Cluster batch meta txn: STB lifecycle with restart (s62-s65)."""

    updatecfgDict = {
        "supportVnodes": "1000",
    }

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)


    def _reset_env(self, db_name="txn_cdb"):
        """Reset test database. Uses replica 3 for VNode HA tests."""
        tdSql.execute(f"drop database if exists {db_name}")
        tdSql.execute(f"create database {db_name} vgroups 2 replica 3")
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

    def s62_stb_create_restart_commit(self):
        """STB created in txn -> cluster restart -> COMMIT -> STB visible"""
        db = "txn_stb_rc"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 1 replica 3")
        tdSql.execute(f"use {db}")
        tdLog.info("======== s62_stb_create_restart_commit")

        # Create a pre-existing STB for reference
        tdSql.execute("create table stb_pre (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table ct0 using stb_pre tags(0)")

        # Create STB within txn
        tdSql.execute("BEGIN")
        tdSql.execute("create table stb_txn (ts timestamp, v int, v2 float) tags (t1 int, t2 binary(16))")
        tdSql.execute("create table ct1 using stb_txn tags(1, 'hello')")

        # Full cluster restart
        tdLog.info("Stopping all dnodes")
        sc.dnodeStopAll()
        time.sleep(2)
        tdLog.info("Starting all dnodes")
        sc.dnodeStartAll()
        clusterComCheck.checkDnodes(3, timeout=30)

        # COMMIT after restart
        tdSql.execute("COMMIT")

        # Verify STB exists and child table is usable
        tdSql.execute(f"use {db}")
        tdSql.query("show stables")
        stb_names = {tdSql.queryResult[i][0] for i in range(tdSql.queryRows)}
        assert "stb_txn" in stb_names, f"stb_txn should exist after COMMIT, got {stb_names}"

        tdSql.query("show tables")
        tdSql.checkRows(2)  # ct0 + ct1

        tdSql.execute("insert into ct1 values(now, 1, 1.5)")
        tdSql.query("select * from ct1")
        tdSql.checkRows(1)

        tdSql.execute(f"drop database {db}")
        tdLog.info("s62 PASSED")


    def s63_stb_alter_restart_commit(self):
        """STB altered in txn -> cluster restart -> COMMIT -> schema updated"""
        db = "txn_stb_ac"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 1 replica 3")
        tdSql.execute(f"use {db}")
        tdLog.info("======== s63_stb_alter_restart_commit")

        tdSql.execute("create table stb1 (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table ct0 using stb1 tags(0)")
        tdSql.execute("insert into ct0 values(now, 100)")

        # ALTER STB within txn: add column
        tdSql.execute("BEGIN")
        tdSql.execute("alter table stb1 add column v2 float")
        tdSql.execute("create table ct1 using stb1 tags(1)")

        # Full cluster restart
        tdLog.info("Stopping all dnodes")
        sc.dnodeStopAll()
        time.sleep(2)
        tdLog.info("Starting all dnodes")
        sc.dnodeStartAll()
        clusterComCheck.checkDnodes(3, timeout=30)

        # COMMIT after restart
        tdSql.execute("COMMIT")

        # Verify ALTER took effect
        tdSql.execute(f"use {db}")
        tdSql.query("describe stb1")
        col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert "v2" in col_names, f"v2 should exist after ALTER+COMMIT, got {col_names}"

        # Insert with new schema
        tdSql.execute("insert into ct0 values(now, 200, 3.14)")
        tdSql.query("select v2 from ct0 where v2 is not null")
        tdSql.checkRows(1)
        val = float(tdSql.queryResult[0][0])
        assert abs(val - 3.14) < 0.001, f"v2 should be ~3.14, got {val}"

        tdSql.execute(f"drop database {db}")
        tdLog.info("s63 PASSED")


    def s64_stb_drop_restart_rollback(self):
        """STB marked for DROP in txn -> cluster restart -> ROLLBACK -> STB restored"""
        db = "txn_stb_dr"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 1 replica 3")
        tdSql.execute(f"use {db}")
        tdLog.info("======== s64_stb_drop_restart_rollback")

        tdSql.execute("create table stb1 (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("create table ct0 using stb1 tags(0)")
        tdSql.execute("insert into ct0 values(now, 42)")

        # DROP STB within txn
        tdSql.execute("BEGIN")
        tdSql.execute("drop table stb1")

        # Full cluster restart
        tdLog.info("Stopping all dnodes")
        sc.dnodeStopAll()
        time.sleep(2)
        tdLog.info("Starting all dnodes")
        sc.dnodeStartAll()
        clusterComCheck.checkDnodes(3, timeout=30)

        # ROLLBACK after restart -> STB should be restored
        tdSql.execute("ROLLBACK")

        tdSql.execute(f"use {db}")
        tdSql.query("show stables")
        stb_names = {tdSql.queryResult[i][0] for i in range(tdSql.queryRows)}
        assert "stb1" in stb_names, f"stb1 should be restored after ROLLBACK, got {stb_names}"

        # Child table and data should be intact
        tdSql.query("select v from ct0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 42)

        # Can still insert into the restored STB
        tdSql.execute("create table ct1 using stb1 tags(1)")
        tdSql.execute("insert into ct1 values(now, 99)")
        tdSql.query("select v from ct1")
        tdSql.checkRows(1)

        tdSql.execute(f"drop database {db}")
        tdLog.info("s64 PASSED")


    def s65_stb_create_alter_restart_commit(self):
        """STB CREATE + ALTER chain in txn -> cluster restart -> COMMIT -> final schema visible"""
        db = "txn_stb_cac"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db} vgroups 1 replica 3")
        tdSql.execute(f"use {db}")
        tdLog.info("======== s65_stb_create_alter_restart_commit")

        # Entire STB lifecycle in one txn
        tdSql.execute("BEGIN")
        tdSql.execute("create table stb_chain (ts timestamp, v int) tags (t1 int)")
        tdSql.execute("alter table stb_chain add column v2 float")
        tdSql.execute("create table ct0 using stb_chain tags(0)")

        # Full cluster restart
        tdLog.info("Stopping all dnodes")
        sc.dnodeStopAll()
        time.sleep(2)
        tdLog.info("Starting all dnodes")
        sc.dnodeStartAll()
        clusterComCheck.checkDnodes(3, timeout=30)

        # COMMIT after restart
        tdSql.execute("COMMIT")

        # Verify final schema includes ALTER
        tdSql.execute(f"use {db}")
        tdSql.query("describe stb_chain")
        col_names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        assert "v2" in col_names, f"v2 should exist after CREATE+ALTER+COMMIT, got {col_names}"

        # Verify child table usable with full schema
        tdSql.execute("insert into ct0 values(now, 1, 2.5)")
        tdSql.query("select v, v2 from ct0")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1)
        val = float(tdSql.queryResult[0][1])
        assert abs(val - 2.5) < 0.001, f"v2 should be ~2.5, got {val}"

        tdSql.execute(f"drop database {db}")
        tdLog.info("s65 PASSED")

    # =========================================================================
    # s66: Fault injection — Raft leader switch *during* vacuum
    #
    # COMMIT writes pTxnFinalIdx then triggers vacuum. Kill VNode leader
    # immediately. New leader must run vacuum exactly once (pTxnFinalIdx
    # guards double-vacuum if old leader had partially completed it).
    # =========================================================================


    def test_meta_batch_txn_cluster_snapshot(self):
        """Cluster batch meta txn: STB lifecycle with restart (s62-s65)

        62. stb_create_restart_commit
        63. stb_alter_restart_commit
        64. stb_drop_restart_rollback
        65. stb_create_alter_restart_commit

        Since: v3.3.6.0
        Labels: common,ci
        """
        self.s62_stb_create_restart_commit()
        self.s63_stb_alter_restart_commit()
        self.s64_stb_drop_restart_rollback()
        self.s65_stb_create_alter_restart_commit()
