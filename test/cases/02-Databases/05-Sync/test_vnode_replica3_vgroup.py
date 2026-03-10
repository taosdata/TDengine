import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestVnodeReplica3Vgroup:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_vnode_replica3_vgroup(self):
        """Write: repica-3 vgroup-2

        1. Start a 4-node cluster.
        2. Create a 3-replica database with 2 vgroups and create a super table.
        3. Create 300 child tables and insert one record into each.
        4. Insert one earlier-timestamp record into each child table.
        5. Verify the query results are correct.

        Catalog:
            - DataBase:Sync

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-5 Simon Guan Migrated from tsim/vnode/replica3_vgroup.sim

        """

        clusterComCheck.checkDnodes(4)

        N = 10
        table = "table_r3"
        db = "db1"

        tdLog.info(f"=================== step 1")
        tdSql.execute(f"create database {db} replica 3 vgroups 2")
        tdSql.execute(f"use {db}")
        clusterComCheck.checkDbReady(db)

        tdSql.execute(f"create table st (ts timestamp, speed int) tags (t1 int)")

        tbPre = "m"
        N = 300
        x = 0
        y = x + N
        while x < y:
            table = tbPre + str(x)
            tdSql.execute(f"create table {table} using st tags ( {x} )")
            ms = str(x) + "m"
            tdSql.execute(f"insert into {table} values (now + {ms} , {x} )")
            x = x + 1

        # print =================== step2
        x = -500
        y = x + N
        while x < y:
            ms = str(x) + "m"
            tdSql.execute(f"insert into {table} values (now {ms} , {x} )")
            x = x + 1

        expect = N + 1
        tdSql.query(f"select * from {table}")
        tdLog.info(
            f"sql select * from {table} -> {tdSql.getRows()}) points expect {expect}"
        )
        tdSql.checkRows(expect)
