import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestStableBalanceReplica1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stable_balance_replica1(self):
        """Query: after balance

        1. Launch a single-node cluster
        2. Create a 1-replica database with 4 vgroups
        3. Create one super table and 13 child tables; insert 200 rows into each
        4. Add dnode2 to the cluster
        5. Execute BALANCE VGROUP
        6. Verify data integrity in all tables

        Catalog:
            - DataBase:Sync

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-5 Simon Guan Migrated from tsim/vnode/stable_balance_replica1.sim

        """

        clusterComCheck.checkDnodes(2)
        tdSql.execute(f"alter dnode 1 'supportVnodes' '5'")
        tdSql.execute(f"alter dnode 2 'supportVnodes' '5'")

        clusterComCheck.checkDnodeSupportVnodes(1, 5)
        clusterComCheck.checkDnodeSupportVnodes(2, 5)

        sc.dnodeStop(2)
        clusterComCheck.checkDnodes(1)

        dbPrefix = "br1_db"
        tbPrefix = "br1_tb"
        mtPrefix = "br1_mt"
        tbNum = 13
        rowNum = 200
        totalNum = 200

        tdLog.info(f"============== step1")
        tdLog.info(f"========= start dnode1")

        i = 0
        db = dbPrefix
        mt = mtPrefix

        tdSql.execute(f"create database {db} vgroups 4")
        tdSql.execute(f"use {db}")
        tdSql.execute(
            f"create table {mt} (ts timestamp, tbcol int, tbcol2 float) TAGS(tgcol int)"
        )

        i = 0
        while i < tbNum:
            tb = tbPrefix + str(i)
            tdSql.execute(f"create table {tb} using {mt} tags( {i} )")

            x = 0
            y = 0
            while y < rowNum:
                val = x * 60000
                ms = 1519833600000 + val
                tdSql.execute(f"insert into {tb} values ({ms} , {y} , {y} )")
                x = x + 1
                y = y + 1

            i = i + 1

        tdLog.info(f"=============== step2")
        time.sleep(2)
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkKeyData(1, 2, 4)

        tdLog.info(f"=============== step3 start dnode2")
        sc.dnodeStart(2)
        clusterComCheck.checkDnodes(2)

        tdSql.execute("balance vgroup")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkKeyData(1, 2, 2)
        tdSql.checkKeyData(2, 2, 2)

        tdLog.info(f"=============== step4")
        i = 1
        tb = tbPrefix + str(i)
        tdSql.query(f"select * from {tb}")
        tdLog.info(f"select * from {tb} ==> {tdSql.getRows()})")
        tdSql.checkRows(200)

        i = 5
        tb = tbPrefix + str(i)
        tdSql.query(f"select * from {tb}")
        tdLog.info(f"select * from {tb} ==> {tdSql.getRows()})")
        tdSql.checkRows(200)

        i = 8
        tb = tbPrefix + str(i)
        tdSql.query(f"select * from {tb}")
        tdLog.info(f"select * from {tb} ==> {tdSql.getRows()})")
        tdSql.checkRows(200)

        tdLog.info(f"=============== step5")
        tdSql.query(f"select * from {mt}")
        tdLog.info(f"select * from {mt} ==> {tdSql.getRows()})")
        tdSql.checkRows(2600)
