import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestVnodeClean:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_vnode_clean(self):
        """Scale-up: repica-1

        1. Start a 1-node cluster
        2. Create database d1 (1 vgroup, 1 replica); create table, insert data, verify
        3. Add dnode2 → run BALANCE VGROUP
        4. Create database d2 (1 vgroup, 1 replica); create table, insert data, verify
        5. Remove dnode2
        6. Add dnode3 → run BALANCE VGROUP
        7. Create database d3 (1 vgroup, 1 replica); create table, insert data, verify
        8. Add dnode4 → run BALANCE VGROUP
        9. Create database d4 (4 vgroups, 1 replica); create tables, insert data, verify
        10. Remove dnode3
        11. Check data integrity for all databases d1, d2, d3, and d4

        Catalog:
            - Database:Sync

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/dnode/vnode_clean.sim

        """

        clusterComCheck.checkDnodes(2)
        sc.dnodeStop(2)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"========== step1")
        tdSql.execute(f"create database d1 vgroups 1")
        tdSql.execute(f"create table d1.t1 (t timestamp, i int)")
        tdSql.execute(f"insert into d1.t1 values(now+1s, 15)")
        tdSql.execute(f"insert into d1.t1 values(now+2s, 14)")
        tdSql.execute(f"insert into d1.t1 values(now+3s, 13)")
        tdSql.execute(f"insert into d1.t1 values(now+4s, 12)")
        tdSql.execute(f"insert into d1.t1 values(now+5s, 11)")

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkKeyData(1, 2, 1)

        tdLog.info(f"========== step2")
        sc.dnodeStart(2)
        clusterComCheck.checkDnodes(2)

        tdSql.execute(f"balance vgroup")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkKeyData(1, 2, 0)
        tdSql.checkKeyData(2, 2, 1)

        tdLog.info(f"========== step3")
        tdSql.execute(f"create database d2 vgroups 1")

        tdSql.execute(f"create table d2.t2 (t timestamp, i int)")
        tdSql.execute(f"insert into d2.t2 values(now+1s, 25)")
        tdSql.execute(f"insert into d2.t2 values(now+2s, 24)")
        tdSql.execute(f"insert into d2.t2 values(now+3s, 23)")
        tdSql.execute(f"insert into d2.t2 values(now+4s, 22)")
        tdSql.execute(f"insert into d2.t2 values(now+5s, 21)")

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkKeyData(1, 2, 1)
        tdSql.checkKeyData(2, 2, 1)

        tdLog.info(f"========== step4")
        tdSql.execute(f"drop dnode 2")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkKeyData(1, 2, 2)

        sc.dnodeStop(2)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"========== step5")

        tdSql.execute(f"create dnode localhost port 6230")
        clusterComCheck.checkDnodes(2)

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdLog.info(
            f"===> {tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)}"
        )
        tdLog.info(
            f"===> {tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)} {tdSql.getData(1,4)} {tdSql.getData(1,5)}"
        )
        tdSql.checkRows(2)

        tdSql.execute(f"balance vgroup")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkKeyData(1, 2, 1)
        tdSql.checkKeyData(3, 2, 1)

        tdLog.info(f"========== step6")
        tdSql.execute(f"create database d3 vgroups 1")
        tdSql.execute(f"create table d3.t3 (t timestamp, i int)")
        tdSql.execute(f"insert into d3.t3 values(now+1s, 35)")
        tdSql.execute(f"insert into d3.t3 values(now+2s, 34)")
        tdSql.execute(f"insert into d3.t3 values(now+3s, 33)")
        tdSql.execute(f"insert into d3.t3 values(now+4s, 32)")
        tdSql.execute(f"insert into d3.t3 values(now+5s, 31)")

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkKeyData(1, 2, 1)
        tdSql.checkKeyData(3, 2, 2)

        tdLog.info(f"========== step7")
        tdSql.execute(f"create dnode localhost port 6330")
        clusterComCheck.checkDnodes(3)

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkRows(3)

        tdSql.execute(f"balance vgroup")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkKeyData(1, 2, 1)
        tdSql.checkKeyData(3, 2, 1)
        tdSql.checkKeyData(4, 2, 1)

        tdLog.info(f"========== step8")
        tdSql.execute(f"create database d4 vgroups 1")
        tdSql.execute(f"create table d4.t4 (t timestamp, i int)")
        tdSql.execute(f"insert into d4.t4 values(now+1s, 45)")
        tdSql.execute(f"insert into d4.t4 values(now+2s, 44)")
        tdSql.execute(f"insert into d4.t4 values(now+3s, 43)")
        tdSql.execute(f"insert into d4.t4 values(now+4s, 42)")
        tdSql.execute(f"insert into d4.t4 values(now+5s, 41)")

        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkKeyData(1, 2, 1)
        tdSql.checkKeyData(3, 2, 2)
        tdSql.checkKeyData(4, 2, 1)

        tdLog.info(f"========== step9")
        tdSql.execute(f"drop dnode 3")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkKeyData(1, 2, 2)
        tdSql.checkKeyData(4, 2, 2)

        sc.dnodeStop(3)
        clusterComCheck.checkDnodes(2)

        tdSql.execute(f"reset query cache")

        tdLog.info(f"========== step10")
        tdSql.query(f"select * from d1.t1  order by t desc")
        tdSql.checkData(0, 1, 11)
        tdSql.checkData(1, 1, 12)
        tdSql.checkData(2, 1, 13)
        tdSql.checkData(3, 1, 14)
        tdSql.checkData(4, 1, 15)

        tdSql.query(f"select * from d2.t2  order by t desc")
        tdSql.checkData(0, 1, 21)
        tdSql.checkData(1, 1, 22)
        tdSql.checkData(2, 1, 23)
        tdSql.checkData(3, 1, 24)
        tdSql.checkData(4, 1, 25)

        tdSql.query(f"select * from d3.t3  order by t desc")
        tdSql.checkData(0, 1, 31)
        tdSql.checkData(1, 1, 32)
        tdSql.checkData(2, 1, 33)
        tdSql.checkData(3, 1, 34)
        tdSql.checkData(4, 1, 35)

        tdSql.query(f"select * from d4.t4  order by t desc")
        tdSql.checkData(0, 1, 41)
        tdSql.checkData(1, 1, 42)
        tdSql.checkData(2, 1, 43)
        tdSql.checkData(3, 1, 44)
        tdSql.checkData(4, 1, 45)
