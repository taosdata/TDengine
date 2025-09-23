import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestVnodeReplica3Basic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_vnode_replica3_basic(self):
        """Write: replica-3 mnode-3

        1. Start a 3-node cluster with 3 mnodes.
        2. Create a 1-replica, 1-vgroup database; create table, insert data, and verify.
        3. Stop dnode2 → insert data → start dnode2 → insert data.
        4. Stop dnode3 → insert data → start dnode3 → insert data.
        5. Stop dnode1 → insert data → start dnode1 → insert data.
        6. Verify data integrity across all nodes.

        Catalog:
            - DataBase:Sync

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-5 Simon Guan Migrated from tsim/vnode/replica3_basic.sim

        """

        tdLog.info(f"========== step0")
        clusterComCheck.checkDnodes(3)
        tdSql.execute(f"create mnode on dnode 2")
        tdSql.execute(f"create mnode on dnode 3")
        clusterComCheck.checkMnodeStatus(3)

        N = 10
        table = "table_r3"
        db = "db1"

        tdLog.info(f"=================== step 1")
        tdSql.execute(f"create database {db} replica 3 vgroups 1")
        tdSql.execute(f"use {db}")
        clusterComCheck.checkDbReady(db)

        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {table} (ts timestamp, speed int)")

        tdLog.info(f"=================== step2")
        x = 1
        y = x + N
        expect = N
        while x < y:
            ms = str(x) + "m"
            tdSql.execute(f"insert into {table} values (now + {ms} , {x} )")
            x = x + 1

        tdSql.query(f"select * from {table}")
        tdLog.info(f"sql select * from {table} -> {tdSql.getRows()}) points")
        tdSql.checkRows(expect)

        tdLog.info(f"=================== step3")
        sc.dnodeStop(2)
        clusterComCheck.checkDnodes(2)
        clusterComCheck.checkDbReady(db)

        y = x + N
        expect = N * 2
        while x < y:
            ms = str(x) + "m"
            tdSql.execute(f"insert into {table} values (now + {ms} , {x} )")
            x = x + 1

        tdLog.info(f"=================== step4")
        sc.dnodeStart(2)
        clusterComCheck.checkDnodes(3)
        clusterComCheck.checkDbReady(db)

        y = x + N
        expect = N * 3
        while x < y:
            ms = str(x) + "m"
            tdSql.execute(f"insert into {table} values (now + {ms} , {x} )")
            x = x + 1

        tdSql.query(f"select * from {table}")
        tdLog.info(f"sql select * from {table} -> {tdSql.getRows()}) points")

        tdLog.info(f"=================== step5")
        sc.dnodeStop(3)
        clusterComCheck.checkDnodes(2)
        clusterComCheck.checkDbReady(db)

        y = x + N
        expect = N * 4
        while x < y:
            ms = str(x) + "m"
            tdSql.execute(f"insert into {table} values (now + {ms} , 10)")
            x = x + 1

        tdSql.query(f"select * from {table}")
        tdLog.info(f"sql select * from {table} -> {tdSql.getRows()}) points")

        tdLog.info(f"=================== step6")
        sc.dnodeStart(3)
        clusterComCheck.checkDnodes(3)
        clusterComCheck.checkDbReady(db)

        y = x + N
        expect = N * 5
        while x < y:
            ms = str(x) + "m"
            tdSql.execute(f"insert into {table} values (now + {ms} , {x} )")
            x = x + 1

        tdSql.query(f"select * from {table}")
        tdLog.info(f"sql select * from {table} -> {tdSql.getRows()}) points")

        tdLog.info(f"=================== step7")
        sc.dnodeStop(1)
        clusterComCheck.checkDnodes(2)
        clusterComCheck.checkDbReady(db)

        y = x + N
        expect = N * 6
        while x < y:
            ms = str(x) + "m"
            tdSql.execute(f"insert into {table} values (now + {ms} , 10)")
            x = x + 1

        tdSql.query(f"select * from {table}")
        tdLog.info(f"sql select * from {table} -> {tdSql.getRows()}) points")

        tdLog.info(f"=================== step 8")
        sc.dnodeStart(1)
        clusterComCheck.checkDnodes(3)
        clusterComCheck.checkDbReady(db)

        y = x + N
        expect = N * 7
        while x < y:
            ms = str(x) + "m"
            tdSql.execute(f"insert into {table} values (now + {ms} , 10)")
            x = x + 1

        tdSql.query(f"select * from {table}")
        tdLog.info(f"sql select * from {table} -> {tdSql.getRows()}) points")
