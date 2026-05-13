import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseKeep:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_Keep(self):
        """Options: keep

            1. Create database with the KEEP option
            2. Write and query data—including data outside the KEEP range
            3. ALTER database KEEP option
            4. Write and query data again

        Catalog:
            - Database:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/db/keep.sim

        """

        sc.dnodeForceStop(2)
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"======== step1 create db")
        tdSql.execute(f"create database keepdb replica 1 keep 30 duration 7 vgroups 2")
        tdSql.execute(f"use keepdb")
        tdSql.execute(f"create table tb (ts timestamp, i int)")

        x = 1
        while x < 41:
            time = str(x) + "d"
            tdSql.isErrorSql(f"insert into tb values (now - {time} , {x} )")
            x = x + 1

        tdSql.query(f"select * from tb")
        tdLog.info(f"===> rows {tdSql.getRows()}) last {tdSql.getData(0,1)}")
        tdSql.checkAssert(tdSql.getRows() < 40)

        tdLog.info(f"======== step2 stop dnode")
        sc.dnodeStop(2)
        sc.dnodeStart(2)
        clusterComCheck.checkDnodes(2)

        tdSql.query(f"select * from tb")
        tdLog.info(f"===> rows {tdSql.getRows()}) last {tdSql.getData(0,1)}")
        tdSql.checkAssert(tdSql.getRows() < 40)
        tdSql.checkAssert(tdSql.getRows() > 20)

        num1 = tdSql.getRows() + 40

        tdLog.info(f"======== step3 alter db")
        tdSql.execute(f"alter database keepdb keep 60")
        tdSql.execute(f"flush database keepdb")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkData(2, 2, 2)

        tdSql.checkData(2, 7, "60d,60d,60d")

        tdLog.info(f"======== step4 insert data")
        x = 41
        while x < 81:
            time = str(x) + "d"
            tdSql.isErrorSql(f"insert into tb values (now - {time} , {x} )")
            x = x + 1

        tdSql.query(f"select * from tb")
        tdLog.info(f"===> rows {tdSql.getRows()}) last {tdSql.getData(0,1)}")
        tdSql.checkAssert(tdSql.getRows() < 80)
        tdSql.checkAssert(tdSql.getRows() > 45)

        tdLog.info(f"======== step5 stop dnode")
        sc.dnodeStop(2)
        sc.dnodeStart(2)
        clusterComCheck.checkDnodes(2)

        tdSql.query(f"select * from tb")
        tdLog.info(f"===> rows {tdSql.getRows()}) last {tdSql.getData(0,1)}")
        tdSql.checkAssert(tdSql.getRows() < 80)
        tdSql.checkAssert(tdSql.getRows() > 45)

        tdLog.info(f"======== step6 alter db")
        tdSql.execute(f"alter database keepdb keep 30")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(2, 7, "30d,30d,30d")

        tdLog.info(f"======== step7 stop dnode")
        sc.dnodeStop(2)
        sc.dnodeStart(2)
        clusterComCheck.checkDnodes(2)

        tdSql.query(f"select * from tb")
        tdLog.info(f"===> rows {tdSql.getRows()}) last {tdSql.getData(0,1)}")
        tdSql.checkAssert(tdSql.getRows() < 40)
        tdSql.checkAssert(tdSql.getRows() > 20)

        tdLog.info(f"======== step8 insert data")
        x = 81
        while x < 121:
            time = str(x) + "d"
            tdSql.isErrorSql(f"insert into tb values (now - {time} , {x} )")
            x = x + 1

        tdSql.query(f"select * from tb")
        tdLog.info(f"===> rows {tdSql.getRows()}) last {tdSql.getData(0,1)}")
        tdSql.checkAssert(tdSql.getRows() < 40)
        tdSql.checkAssert(tdSql.getRows() > 20)

        tdLog.info(f"======== step9 alter db")
        tdSql.error(f"alter database keepdb keep -1")
        tdSql.error(f"alter database keepdb keep 0")
        tdSql.error(f"alter database keepdb duration 1")

        tdLog.info(f"======= test success")
