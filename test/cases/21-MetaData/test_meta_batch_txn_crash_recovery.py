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

"""Batch meta txn crash recovery tests (s80-s83).

Requires: ./ci/pytest.sh pytest ... -N 3 -M 3

Tests cover:
  s80: Kill VNode mid-vacuum after COMMIT → tables visible after restart
  s81: Kill VNode after BEGIN+CREATE but before COMMIT → tables NOT visible
  s82: Kill VNode and MNode simultaneously mid-COMMIT → tables visible after restart
  s83: Double-restart idempotency — WAL replay of finalized txn is harmless
"""

from new_test_framework.utils import tdLog, tdSql, tdCom, sc, clusterComCheck
import time


class TestBatchMetaTxnCrashRecovery:
    """Batch meta txn crash recovery (s80-s83)."""

    updatecfgDict = {
        "supportVnodes": "1000",
    }

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)

    def _reset_env(self, db_name):
        """Reset test database with replica 3."""
        tdSql.execute(f"drop database if exists {db_name}")
        tdSql.execute(f"create database {db_name} vgroups 2 replica 3")
        tdSql.execute(f"use {db_name}")

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

    def _poll_table_count(self, expected, db_name, timeout=120):
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
            except Exception:
                continue
        tdLog.exit(f"Table count stuck at {last_count}, expected {expected} (timeout={timeout}s)")
        return False

    def _poll_table_absent(self, table_prefix, db_name, count, timeout=30):
        """Verify tables with given prefix are NOT visible."""
        time.sleep(3)  # let any pending vacuum/replay finish
        tdSql.execute(f"use {db_name}")
        tdSql.query("show tables")
        names = [tdSql.queryResult[i][0] for i in range(tdSql.queryRows)]
        found = [n for n in names if n.startswith(table_prefix)]
        if len(found) > 0:
            tdLog.exit(f"Expected 0 tables with prefix '{table_prefix}', found {len(found)}: {found[:5]}")
        return True

    # =========================================================================
    # s80: Kill VNode leader immediately after COMMIT → tables visible after
    #      restart via WAL replay + vacuum.
    # =========================================================================
    def s80_kill_after_commit_tables_visible(self):
        db = "txn_cr80"
        self._reset_env(db)
        tdLog.info("======== s80_kill_after_commit_tables_visible")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # Create 20 child tables in a txn (below inline threshold → inline promote)
        tdSql.execute("BEGIN")
        for i in range(20):
            tdSql.execute(f"create table ct80_{i} using stb tags({i})")
        tdSql.execute("COMMIT")

        # Kill VNode leader immediately after COMMIT returns
        tdSql.query(f"show {db}.vgroups")
        vgId = tdSql.queryResult[0][0]
        leader_dnode = self._get_vgroup_leader_dnode(db, vgId)
        tdLog.info(f"Killing VNode leader (dnode {leader_dnode}) after COMMIT")
        sc.dnodeForceStop(leader_dnode)

        # Wait for new leader election
        new_leader = self._get_vgroup_leader_dnode(db, vgId, timeout=30)
        assert new_leader is not None, "No new VNode leader elected"

        # Restart killed node
        sc.dnodeStart(leader_dnode)
        clusterComCheck.checkDnodes(3)

        # Verify: all 20 tables must be visible (WAL replay completes inline promote)
        self._poll_table_count(20, db_name=db)

        # Verify data can be written to all tables
        for i in range(20):
            tdSql.execute(f"insert into ct80_{i} values(now, {i})")
        tdSql.query("select count(*) from stb")
        tdSql.checkData(0, 0, 20)

        tdSql.execute(f"drop database {db}")
        tdLog.info("s80 PASSED")

    # =========================================================================
    # s81: Kill VNode after BEGIN+CREATE but BEFORE COMMIT → uncommitted tables
    #      must NOT be visible after restart (txn was never committed).
    # =========================================================================
    def s81_kill_before_commit_tables_invisible(self):
        db = "txn_cr81"
        self._reset_env(db)
        tdLog.info("======== s81_kill_before_commit_tables_invisible")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        # Open txn and create tables, but do NOT commit
        tdSql.execute("BEGIN")
        for i in range(10):
            tdSql.execute(f"create table ct81_{i} using stb tags({i})")
        # Intentionally skip COMMIT — kill the VNode leader now

        tdSql.query(f"show {db}.vgroups")
        vgId = tdSql.queryResult[0][0]
        leader_dnode = self._get_vgroup_leader_dnode(db, vgId)
        tdLog.info(f"Killing VNode leader (dnode {leader_dnode}) WITHOUT COMMIT")
        sc.dnodeForceStop(leader_dnode)

        # Wait for new leader
        new_leader = self._get_vgroup_leader_dnode(db, vgId, timeout=30)
        assert new_leader is not None, "No new VNode leader elected"

        # Restart
        sc.dnodeStart(leader_dnode)
        clusterComCheck.checkDnodes(3)

        # Wait and verify: tables must NOT be visible (uncommitted txn → PRE_CREATE)
        time.sleep(5)
        self._poll_table_absent("ct81_", db, 10)

        # New txn should work fine (connection was lost, server-side txn timed out)
        tdSql.execute(f"use {db}")
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct81_new using stb tags(99)")
        tdSql.execute("COMMIT")
        self._poll_table_count(1, db_name=db)

        tdSql.execute(f"drop database {db}")
        tdLog.info("s81 PASSED")

    # =========================================================================
    # s82: Kill both MNode leader and VNode leader simultaneously during
    #      COMMIT processing → after full restart, tables must be visible.
    # =========================================================================
    def s82_kill_mnode_and_vnode_simultaneously(self):
        db = "txn_cr82"
        self._reset_env(db)
        tdLog.info("======== s82_kill_mnode_and_vnode_simultaneously")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        tdSql.execute("BEGIN")
        for i in range(15):
            tdSql.execute(f"create table ct82_{i} using stb tags({i})")
        tdSql.execute("COMMIT")

        # Kill VNode leader
        tdSql.query(f"show {db}.vgroups")
        vgId = tdSql.queryResult[0][0]
        vnode_leader = self._get_vgroup_leader_dnode(db, vgId)

        # Kill MNode leader
        tdSql.query("select * from information_schema.ins_mnodes")
        mnode_leader = None
        for i in range(tdSql.queryRows):
            if tdSql.queryResult[i][2] == 'leader':
                mnode_leader = tdSql.queryResult[i][0]
                break

        tdLog.info(f"Killing VNode leader (dnode {vnode_leader}) and MNode leader (dnode {mnode_leader})")
        sc.dnodeForceStop(vnode_leader)
        if mnode_leader != vnode_leader:
            sc.dnodeForceStop(mnode_leader)

        time.sleep(5)

        # Restart all killed nodes
        sc.dnodeStart(vnode_leader)
        if mnode_leader != vnode_leader:
            sc.dnodeStart(mnode_leader)
        clusterComCheck.checkDnodes(3)

        # Verify all tables visible
        self._poll_table_count(15, db_name=db)

        tdSql.execute(f"drop database {db}")
        tdLog.info("s82 PASSED")

    # =========================================================================
    # s83: Double-restart idempotency — WAL replay of an already-finalized txn
    #      must be harmless (no duplicate entries, no crash).
    # =========================================================================
    def s83_double_restart_idempotency(self):
        db = "txn_cr83"
        self._reset_env(db)
        tdLog.info("======== s83_double_restart_idempotency")

        tdSql.execute("create table stb (ts timestamp, v int) tags (t1 int)")

        tdSql.execute("BEGIN")
        for i in range(10):
            tdSql.execute(f"create table ct83_{i} using stb tags({i})")
        tdSql.execute("COMMIT")

        # First kill + restart
        tdSql.query(f"show {db}.vgroups")
        vgId = tdSql.queryResult[0][0]
        leader_dnode = self._get_vgroup_leader_dnode(db, vgId)
        sc.dnodeForceStop(leader_dnode)
        time.sleep(2)
        sc.dnodeStart(leader_dnode)
        clusterComCheck.checkDnodes(3)
        self._poll_table_count(10, db_name=db)

        # Second kill + restart (WAL replay should be idempotent)
        leader_dnode = self._get_vgroup_leader_dnode(db, vgId)
        sc.dnodeForceStop(leader_dnode)
        time.sleep(2)
        sc.dnodeStart(leader_dnode)
        clusterComCheck.checkDnodes(3)
        self._poll_table_count(10, db_name=db)

        # Verify data integrity (no duplicates)
        tdSql.execute(f"use {db}")
        tdSql.query("select tbname from stb")
        assert tdSql.queryRows == 10, f"Expected 10 child tables, got {tdSql.queryRows}"

        # Verify new txns still work
        tdSql.execute("BEGIN")
        tdSql.execute("create table ct83_extra using stb tags(100)")
        tdSql.execute("COMMIT")
        self._poll_table_count(11, db_name=db)

        tdSql.execute(f"drop database {db}")
        tdLog.info("s83 PASSED")

    def test_crash_recovery(self):
        """Run all crash recovery tests."""
        self.s80_kill_after_commit_tables_visible()
        self.s81_kill_before_commit_tables_invisible()
        self.s82_kill_mnode_and_vnode_simultaneously()
        self.s83_double_restart_idempotency()

    def teardown_class(cls):
        tdLog.success(f"{__file__} successfully executed")
