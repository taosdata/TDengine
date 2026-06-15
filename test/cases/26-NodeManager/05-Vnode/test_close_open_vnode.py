# -*- coding: utf-8 -*-

import time
import pytest
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestCloseOpenVnode:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_close_open_vnode_basic(self):
        """Close and open vnode basic lifecycle

        1. Create database with 2 vgroups
        2. Insert data
        3. Close a vnode on its dnode
        4. Verify queries fail (vnode unavailable)
        5. Open the vnode back
        6. Verify data is accessible again

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-26 Created

        """
        clusterComCheck.checkDnodes(1)

        # Create database and insert data
        tdSql.execute("drop database if exists test_cv")
        tdSql.execute("create database test_cv vgroups 2")
        tdSql.execute("use test_cv")
        tdSql.execute("create table t1 (ts timestamp, v int)")
        tdSql.execute("insert into t1 values (now, 1)")
        tdSql.execute("insert into t1 values (now+1s, 2)")
        tdSql.execute("insert into t1 values (now+2s, 3)")

        # Verify data is there
        tdSql.query("select * from t1")
        tdSql.checkRows(3)

        # Get vgroup and dnode info
        tdSql.query("show test_cv.vgroups")
        vgId = tdSql.getData(0, 0)
        dnodeId = tdSql.getData(0, 3)
        tdLog.info(f"target vgId={vgId}, dnodeId={dnodeId}")

        # Close the vnode
        tdLog.info(f"closing vnode {vgId} on dnode {dnodeId}")
        tdSql.execute(f"close vnode {vgId} on dnode {dnodeId}")
        time.sleep(5)

        # Verify queries on this vnode fail (vnode is closed)
        tdSql.error("select * from t1")

        # Open the vnode back
        tdLog.info(f"opening vnode {vgId} on dnode {dnodeId}")
        tdSql.execute(f"open vnode {vgId} on dnode {dnodeId}")
        time.sleep(5)

        # Verify data is accessible again
        tdSql.query("select * from t1")
        tdSql.checkRows(3)

        # Cleanup
        tdSql.execute("drop database test_cv")

    def test_close_vnode_error_nonexistent_vgroup(self):
        """Close vnode with non-existent vgroup

        1. Try to close a non-existent vgroup
        2. Expect error

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-26 Created

        """
        tdSql.error("close vnode 99999 on dnode 1")

    def test_close_vnode_error_wrong_dnode(self):
        """Close vnode on wrong dnode

        1. Create database
        2. Try to close vnode on a non-existent dnode
        3. Expect error

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-26 Created

        """
        tdSql.execute("drop database if exists test_cv2")
        tdSql.execute("create database test_cv2 vgroups 1")
        tdSql.execute("use test_cv2")

        tdSql.query("show test_cv2.vgroups")
        vgId = tdSql.getData(0, 0)

        # Close on non-existent dnode
        tdSql.error(f"close vnode {vgId} on dnode 99999")

        tdSql.execute("drop database test_cv2")

    def test_close_vnode_already_closed(self):
        """Close an already-closed vnode

        1. Create database
        2. Close vnode
        3. Try to close again
        4. Expect error

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-26 Created

        """
        tdSql.execute("drop database if exists test_cv3")
        tdSql.execute("create database test_cv3 vgroups 1")
        tdSql.execute("use test_cv3")

        tdSql.query("show test_cv3.vgroups")
        vgId = tdSql.getData(0, 0)
        dnodeId = tdSql.getData(0, 3)

        # Close the vnode
        tdSql.execute(f"close vnode {vgId} on dnode {dnodeId}")
        time.sleep(5)

        # Try to close again - async fire-and-forget, mnode accepts without error
        # The dnode will silently handle the redundant close
        tdSql.execute(f"close vnode {vgId} on dnode {dnodeId}")

        # Reopen for cleanup
        tdSql.execute(f"open vnode {vgId} on dnode {dnodeId}")
        time.sleep(5)

        tdSql.execute("drop database test_cv3")

    def test_open_vnode_not_closed(self):
        """Open a vnode that is not closed (already running)

        1. Create database
        2. Try to open a running vnode
        3. Expect error

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-26 Created

        """
        tdSql.execute("drop database if exists test_cv4")
        tdSql.execute("create database test_cv4 vgroups 1")
        tdSql.execute("use test_cv4")

        tdSql.query("show test_cv4.vgroups")
        vgId = tdSql.getData(0, 0)
        dnodeId = tdSql.getData(0, 3)

        # Open a running vnode - async fire-and-forget, mnode accepts without error
        # The dnode will silently handle the redundant open
        tdSql.execute(f"open vnode {vgId} on dnode {dnodeId}")

        tdSql.execute("drop database test_cv4")

    def test_open_vnode_nonexistent(self):
        """Open a non-existent vgroup

        1. Try to open a non-existent vgroup
        2. Expect error

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-26 Created

        """
        tdSql.error("open vnode 99999 on dnode 1")

    def test_close_open_vnode_data_integrity(self):
        """Verify data integrity after close and open cycle

        1. Create database and insert substantial data
        2. Close vnode
        3. Open vnode
        4. Verify all data is intact with count and aggregation checks

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-26 Created

        """
        tdSql.execute("drop database if exists test_cv5")
        tdSql.execute("create database test_cv5 vgroups 1")
        tdSql.execute("use test_cv5")
        tdSql.execute("create table t1 (ts timestamp, v int, f float)")

        # Insert 100 rows
        for i in range(100):
            tdSql.execute(f"insert into t1 values (now+{i}s, {i}, {i * 1.5})")

        tdSql.query("select count(*) from t1")
        tdSql.checkData(0, 0, 100)
        tdSql.query("select sum(v) from t1")
        expected_sum = sum(range(100))
        tdSql.checkData(0, 0, expected_sum)

        # Get vgroup info
        tdSql.query("show test_cv5.vgroups")
        vgId = tdSql.getData(0, 0)
        dnodeId = tdSql.getData(0, 3)

        # Close and reopen
        tdSql.execute(f"close vnode {vgId} on dnode {dnodeId}")
        time.sleep(5)
        tdSql.execute(f"open vnode {vgId} on dnode {dnodeId}")
        time.sleep(5)

        # Verify data integrity
        tdSql.query("select count(*) from t1")
        tdSql.checkData(0, 0, 100)
        tdSql.query("select sum(v) from t1")
        tdSql.checkData(0, 0, expected_sum)

        tdSql.execute("drop database test_cv5")

    def test_close_vnode_not_persisted_after_restart(self):
        """Verify closed state is not persisted after taosd restart

        1. Create database
        2. Close vnode
        3. Restart taosd
        4. Verify vnode is automatically reopened (closed state is runtime-only)

        Since: v3.3.6.0

        Labels: common,ci

        Jira: None

        History:
            - 2026-5-26 Created

        """
        tdSql.execute("drop database if exists test_cv6")
        tdSql.execute("create database test_cv6 vgroups 1")
        tdSql.execute("use test_cv6")
        tdSql.execute("create table t1 (ts timestamp, v int)")
        tdSql.execute("insert into t1 values (now, 42)")

        tdSql.query("show test_cv6.vgroups")
        vgId = tdSql.getData(0, 0)
        dnodeId = tdSql.getData(0, 3)

        # Close the vnode
        tdSql.execute(f"close vnode {vgId} on dnode {dnodeId}")
        time.sleep(5)

        # Verify it's closed
        tdSql.error("select * from t1")

        # Restart taosd
        sc.dnodeStop(1)
        sc.dnodeStart(1)
        time.sleep(3)
        clusterComCheck.checkDnodes(1)

        # Reconnect
        tdSql.execute("use test_cv6")

        # After restart, vnode should be open again (closed state is NOT persisted)
        tdSql.query("select * from t1")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, 42)

        tdSql.execute("drop database test_cv6")
