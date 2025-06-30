import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseBasic1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_basic1(self):
        """create database 1

        1. creat database use vgroup option
        2. show vgroups
        3. show vnodes

        Catalog:
            - Databases:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/db/basic1.sim

        """

        tdLog.info(f"=============== create database")
        tdSql.prepare(dbname="d1", vgroups=2)
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 0, "d1")
        tdSql.checkData(2, 2, 2)
        tdSql.checkData(2, 3, 0)

        tdLog.info(f"=============== show vgroups1")
        tdSql.execute(f"use d1")
        tdSql.query(f"show vgroups")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 2)
        tdSql.checkData(1, 0, 3)

        tdLog.info(f"=============== drop database")
        tdSql.execute(f"drop database d1")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        tdLog.info(f"=============== more databases")
        tdSql.execute(f"create database d2 vgroups 2")
        tdSql.execute(f"create database d3 vgroups 3")
        tdSql.execute(f"create database d4 vgroups 4")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(5)

        tdLog.info(f"=============== show vgroups2")
        tdSql.query(f"show d2.vgroups")
        tdSql.checkRows(2)
        tdSql.checkData(0, 0, 4)
        tdSql.checkData(1, 0, 5)

        tdLog.info(f"=============== show vgroups3")
        tdSql.query(f"show d3.vgroups")
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 6)
        tdSql.checkData(1, 0, 7)
        tdSql.checkData(2, 0, 8)

        tdLog.info(f"=============== show vgroups4")
        tdSql.query(f"show d4.vgroups")
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 9)
        tdSql.checkData(1, 0, 10)
        tdSql.checkData(2, 0, 11)
        tdSql.checkData(3, 0, 12)

        tdLog.info(f"=============== show vnodes on dnode 1")
        tdLog.info(
            f"=============== Wait for the synchronization status of vnode and Mnode, heartbeat for one second"
        )

        time.sleep(2)
        tdSql.query(f"show vnodes on dnode 1")
        tdSql.checkRows(9)
        tdSql.checkData(1, 0, 1)
        tdSql.checkData(1, 1, 5)
        tdSql.checkData(1, 2, "d2")
        tdSql.checkData(1, 3, "leader")

        tdLog.info(f"{tdSql.getData(1,4)} , {tdSql.getData(1,5)}")
        tdSql.checkData(1, 6, 1)

        tdLog.info(f"================ show vnodes")
        tdSql.query(f"show vnodes")
        tdSql.checkRows(9)

        tdLog.info(f"=============== drop database")
        tdSql.execute(f"drop database d2")
        tdSql.execute(f"drop database d3")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 0, "d4")
        tdSql.checkData(2, 2, 4)
        tdSql.checkData(2, 3, 0)

        tdLog.info(f"=============== show vgroups4 again")
        tdSql.error(f"use d1")

        tdSql.execute(f"use d4")
        tdSql.query(f"show vgroups")
        tdSql.checkRows(4)

        tdLog.info(f"=============== select * from information_schema.ins_dnodes")
        tdSql.query(f"select * from information_schema.ins_dnodes")
        tdSql.checkData(0, 0, 1)
        tdSql.checkData(0, 2, 4)

        tdLog.info(f"=============== restart")
        sc.dnodeForceStop(1)
        sc.dnodeStart(1)

        tdLog.info(f"=============== select * from information_schema.ins_databases")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdSql.error(f"use d1")

        tdSql.execute(f"use d4")
        tdSql.query(f"show vgroups")
        tdSql.checkRows(4)

        tdLog.info(f"=============== create databases")
        tdSql.execute(f"create database d5 vgroups 5;")

        tdLog.info(f"=============== show vgroups")
        tdSql.execute(f"use d5")
        tdSql.query(f"show vgroups")
        tdSql.checkRows(5)

        tdSql.query(f"show d4.vgroups")
        tdSql.checkRows(4)

        tdSql.query(f"show d5.vgroups")
        tdSql.checkRows(5)
