import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestBalance2:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_balance_2(self):
        """balance 2

        1. -

        Catalog:
            - Database:Sync

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/dnode/balance2.sim

        """

        clusterComCheck.checkDnodes(3)
        tdSql.execute(f"alter dnode 1 'supportVnodes' '5'")
        tdSql.execute(f"alter dnode 2 'supportVnodes' '5'")
        tdSql.execute(f"alter dnode 3 'supportVnodes' '5'")
        clusterComCheck.checkDnodeSupportVnodes(1, 5)
        clusterComCheck.checkDnodeSupportVnodes(2, 5)
        clusterComCheck.checkDnodeSupportVnodes(3, 5)

        tdLog.info(f"========== step1")

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(3)
        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        tdSql.checkKeyData(3, 4, "ready")

        tdLog.info(f"========== step2")
        tdSql.execute(f"create database d1 replica 3 vgroups 1")
        tdSql.execute(f"create table d1.t1 (t timestamp, i int)")
        tdSql.execute(f"insert into d1.t1 values(now+1s, 15)")
        tdSql.execute(f"insert into d1.t1 values(now+2s, 14)")
        tdSql.execute(f"insert into d1.t1 values(now+3s, 13)")
        tdSql.execute(f"insert into d1.t1 values(now+4s, 12)")
        tdSql.execute(f"insert into d1.t1 values(now+5s, 11)")

        tdSql.execute(f"create database d2 replica 3 vgroups 1")
        tdSql.execute(f"create table d2.t2 (t timestamp, i int)")
        tdSql.execute(f"insert into d2.t2 values(now+1s, 25)")
        tdSql.execute(f"insert into d2.t2 values(now+2s, 24)")
        tdSql.execute(f"insert into d2.t2 values(now+3s, 23)")
        tdSql.execute(f"insert into d2.t2 values(now+4s, 22)")
        tdSql.execute(f"insert into d2.t2 values(now+5s, 21)")

        tdSql.execute(f"create database d3 replica 3 vgroups 1")
        tdSql.execute(f"create table d3.t3 (t timestamp, i int)")
        tdSql.execute(f"insert into d3.t3 values(now+1s, 35)")
        tdSql.execute(f"insert into d3.t3 values(now+2s, 34)")
        tdSql.execute(f"insert into d3.t3 values(now+3s, 33)")
        tdSql.execute(f"insert into d3.t3 values(now+4s, 32)")
        tdSql.execute(f"insert into d3.t3 values(now+5s, 31)")

        tdSql.execute(f"create database d4 replica 3 vgroups 1")
        tdSql.execute(f"create table d4.t4 (t timestamp, i int)")
        tdSql.execute(f"insert into d4.t4 values(now+1s, 45)")
        tdSql.execute(f"insert into d4.t4 values(now+2s, 44)")
        tdSql.execute(f"insert into d4.t4 values(now+3s, 43)")
        tdSql.execute(f"insert into d4.t4 values(now+4s, 42)")
        tdSql.execute(f"insert into d4.t4 values(now+5s, 41)")

        tdLog.info(f"========== step2.1")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkKeyData(1, 2, 4)
        tdSql.checkKeyData(2, 2, 4)
        tdSql.checkKeyData(3, 2, 4)

        tdLog.info(f"========== step3")
        tdSql.execute(f"create dnode localhost port 6330")
        tdSql.execute(f"create dnode localhost port 6430")
        clusterComCheck.checkDnodes(5)
        tdSql.execute(f"alter dnode 4 'supportVnodes' '5'")
        tdSql.execute(f"alter dnode 5 'supportVnodes' '5'")
        clusterComCheck.checkDnodeSupportVnodes(4, 5)
        clusterComCheck.checkDnodeSupportVnodes(5, 5)

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(5)
        tdSql.checkKeyData(1, 4, "ready")
        tdSql.checkKeyData(2, 4, "ready")
        tdSql.checkKeyData(3, 4, "ready")
        tdSql.checkKeyData(4, 4, "ready")
        tdSql.checkKeyData(5, 4, "ready")

        tdSql.execute(f"balance vgroup")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkKeyData(1, 2, 2)
        tdSql.checkKeyData(2, 2, 3)
        tdSql.checkKeyData(3, 2, 3)
        tdSql.checkKeyData(4, 2, 2)
        tdSql.checkKeyData(5, 2, 2)

        tdLog.info(f"========== step4")
        tdSql.execute(f"drop dnode 2")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkKeyData(1, 2, 3)
        tdSql.checkKeyData(3, 2, 3)
        tdSql.checkKeyData(4, 2, 3)
        tdSql.checkKeyData(5, 2, 3)

        sc.dnodeStop(2)
        clusterComCheck.checkDnodes(4)

        tdLog.info(f"========== step5")
        tdSql.execute(f"drop dnode 3")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkKeyData(1, 2, 4)
        tdSql.checkKeyData(4, 2, 4)
        tdSql.checkKeyData(5, 2, 4)

        tdSql.execute(f"reset query cache")

        sc.dnodeStop(3)
        clusterComCheck.checkDnodes(3)

        tdLog.info(f"========== step6")
        tdSql.query(f"select * from d1.t1 order by t desc")
        tdSql.checkData(0, 1, 11)
        tdSql.checkData(1, 1, 12)
        tdSql.checkData(2, 1, 13)
        tdSql.checkData(3, 1, 14)
        tdSql.checkData(4, 1, 15)

        tdSql.query(f"select * from d2.t2 order by t desc")
        tdSql.checkData(0, 1, 21)
        tdSql.checkData(1, 1, 22)
        tdSql.checkData(2, 1, 23)
        tdSql.checkData(3, 1, 24)
        tdSql.checkData(4, 1, 25)

        tdSql.query(f"select * from d3.t3 order by t desc")
        tdSql.checkData(0, 1, 31)
        tdSql.checkData(1, 1, 32)
        tdSql.checkData(2, 1, 33)
        tdSql.checkData(3, 1, 34)
        tdSql.checkData(4, 1, 35)

        tdSql.query(f"select * from d4.t4 order by t desc")
        tdSql.checkData(0, 1, 41)
        tdSql.checkData(1, 1, 42)
        tdSql.checkData(2, 1, 43)
        tdSql.checkData(3, 1, 44)
        tdSql.checkData(4, 1, 45)
