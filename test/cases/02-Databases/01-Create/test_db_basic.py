import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseBasic1:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_basic(self):
        """Database create basic

        1. Create database with vgroup option
        2. Show vgroups and verify vgroups info
        3. Drop database and verify
        4. Create multiple databases and verify vgroups info
        5. Drop some databases and verify
        6. Restart dnode and verify database and vgroup info
        7. Create more databases and verify vgroup info
        8. Create same name db and drop loop 100 times(TD-25762)
        9. Create super/child/normal tables in multiple databases
        10. Create normal table with db. prefix in multiple databases
        11. Create/drop tables in a database multiple times and verify
        12. Create database with options (replica, duration, keep, minrows)
        13. Query information_schema.ins_databases and verify results
        14. Repeatedly execute create database, drop database, create table, and write data  

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/db/basic1.sim
            - 2025-5-12 Simon Guan Migrated from tsim/db/basic2.sim
            - 2025-5-12 Simon Guan Migrated from tsim/db/basic3.sim
            - 2025-5-12 Simon Guan Migrated from tsim/db/basic4.sim
            - 2025-5-12 Simon Guan Migrated from tsim/db/basic5.sim
            - 2025-5-12 Simon Guan Migrated from tsim/db/basic6.sim
            - 2025-11-04 Alex Duan Migrated from uncatalog/system-test/0-others/test_create_same_name_db.py

        """
        self.do_database_basic1()
        self.do_database_basic2()
        self.do_database_basic3()
        self.do_database_basic4()
        self.do_database_basic5()
        self.do_database_basic6()
    
    def do_database_basic1(self):
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
        
        # test_create_same_name_db.py
        self.do_create_same_name_db()
        tdSql.execute(f"drop database d4")
        tdSql.execute(f"drop database d5")

    def do_create_same_name_db(self):
        try:
            # create same name database multiple times
            for i in range(100):
                tdSql.execute(f"create database db")
                tdSql.execute(f"drop database db")
        except Exception as ex:
            tdLog.exit(str(ex))

    def do_database_basic2(self):
        tdLog.info(f"=============== conflict stb")
        tdSql.execute(f"create database db vgroups 4;")
        tdSql.execute(f"use db;")
        tdSql.execute(f"create table stb (ts timestamp, i int) tags (j int);")
        tdSql.error(f"create table stb using stb tags (1);")
        tdSql.error(f"create table stb (ts timestamp, i int);")

        tdSql.execute(f"create table ctb (ts timestamp, i int);")
        tdSql.error(f"create table ctb (ts timestamp, i int) tags (j int);")

        tdSql.execute(f"create table ntb (ts timestamp, i int);")
        tdSql.error(f"create table ntb (ts timestamp, i int) tags (j int);")

        tdSql.execute(f"drop table ntb")
        tdSql.execute(f"create table ntb (ts timestamp, i int) tags (j int);")

        tdSql.execute(f"drop database db")

        tdLog.info(f"=============== create database d1")
        tdSql.execute(f"create database d1")
        tdSql.execute(f"use d1")
        tdSql.execute(f"create table t1 (ts timestamp, i int);")
        tdSql.execute(f"create table t2 (ts timestamp, i int);")
        tdSql.execute(f"create table t3 (ts timestamp, i int);")
        tdSql.execute(f"create table t4 (ts timestamp, i int);")

        tdSql.query(f"select * from information_schema.ins_databases")
        tdLog.info(f"rows: {tdSql.getRows()})")
        tdSql.checkRows(3)
        tdSql.checkData(2, 0, "d1")
        tdSql.checkData(2, 2, 2)

        tdSql.query(f"show tables")
        tdSql.checkRows(4)

        tdLog.info(f"=============== create database d2")
        tdSql.execute(f"create database d2")
        tdSql.execute(f"use d2")
        tdSql.execute(f"create table t1 (ts timestamp, i int);")
        tdSql.execute(f"create table t2 (ts timestamp, i int);")
        tdSql.execute(f"create table t3 (ts timestamp, i int);")

        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(4)

        tdSql.query(f"show tables")
        tdSql.checkRows(3)
        # clear
        tdSql.execute(f"drop database d1")
        tdSql.execute(f"drop database d2")

    def do_database_basic3(self):
        tdLog.info(f"=============== create database d1")
        tdSql.execute(f"create database d1")
        tdSql.execute(f"create table d1.t1 (ts timestamp, i int);")
        tdSql.execute(f"create table d1.t2 (ts timestamp, i int);")
        tdSql.execute(f"create table d1.t3 (ts timestamp, i int);")
        tdSql.execute(f"create table d1.t4 (ts timestamp, i int);")

        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 0, "d1")
        tdSql.checkData(2, 2, 2)

        tdSql.query(f"show d1.tables")
        tdSql.checkRows(4)

        tdLog.info(f"=============== create database d2")
        tdSql.execute(f"create database d2")
        tdSql.execute(f"create table d2.t1 (ts timestamp, i int);")
        tdSql.execute(f"create table d2.t2 (ts timestamp, i int);")
        tdSql.execute(f"create table d2.t3 (ts timestamp, i int);")

        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(4)

        tdSql.query(f"show d2.tables")
        tdSql.checkRows(3)
        # clear
        tdSql.execute(f"drop database d1")
        tdSql.execute(f"drop database d2")
        

    def do_database_basic4(self):
        tdLog.info(f"=============== create database d1")
        tdSql.execute(f"create database d1 vgroups 1")
        tdSql.execute(f"create table d1.t1 (ts timestamp, i int);")
        tdSql.execute(f"create table d1.t2 (ts timestamp, i int);")
        tdSql.execute(f"create table d1.t3 (ts timestamp, i int);")
        tdSql.execute(f"create table d1.t4 (ts timestamp, i int);")

        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 0, "d1")
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(2, 4, 1)

        tdSql.query(f"show d1.tables")
        tdSql.checkRows(4)

        tdSql.query(f"show d1.vgroups")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "d1")

        tdLog.info(f"=============== drop table")
        tdSql.execute(f"drop table d1.t1")

        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 0, "d1")
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(2, 4, 1)

        tdSql.query(f"show d1.tables")
        tdSql.checkRows(3)

        tdSql.query(f"show d1.vgroups")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "d1")

        tdLog.info(f"=============== drop all table")
        tdSql.execute(f"drop table d1.t2")
        tdSql.execute(f"drop table d1.t3")
        tdSql.execute(f"drop table d1.t4")

        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)
        tdSql.checkData(2, 0, "d1")
        tdSql.checkData(2, 2, 1)
        tdSql.checkData(2, 4, 1)

        tdSql.query(f"show d1.tables")
        tdSql.checkRows(0)

        tdSql.query(f"show d1.vgroups")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "d1")
        # clear
        tdSql.execute(f"drop database d1")

    def do_database_basic5(self):
        tdLog.info(f"=============== create database d1")
        tdSql.execute(f"create database db1 vgroups 1;")
        tdSql.execute(f"use db1;")

        tdLog.info(f"=============== create stable and table")
        tdSql.execute(f"create stable st1 (ts timestamp, f1 int) tags(t1 int);")
        tdSql.execute(f"create table tb1 using st1 tags(1);")
        tdSql.execute(f"insert into tb1 values (now, 1);")

        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdSql.query(f"show tables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== drop db")
        tdSql.execute(f"drop database db1;")

        tdSql.error(f"show stables")
        tdSql.error(f"show tables")

        tdLog.info(f"=============== re-create db and stb")
        tdSql.execute(f"create database db1;")
        tdSql.execute(f"use db1;")
        tdSql.execute(f"create stable st1 (ts timestamp, f1 int) tags(t1 int)")

        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdSql.query(f"show tables")
        tdSql.checkRows(0)
        # clear
        tdSql.execute(f"drop database db1")

    def do_database_basic6(self):
        tdLog.info(f"============================ dnode1 start")

        i = 0
        dbPrefix = "db"
        stPrefix = "st"
        tbPrefix = "tb"
        db = dbPrefix + str(i)
        st = stPrefix + str(i)
        tb = tbPrefix + str(i)

        tdLog.info(f"=============== step1")
        tdSql.execute(
            f"create database {db} vgroups 8 replica 1 duration 2 keep 10 minrows 80 maxrows 10000 wal_level 2 wal_fsync_period 1000 comp 0 cachemodel 'last_value' precision 'us'"
        )
        tdSql.query(f"select * from information_schema.ins_databases")
        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)} {tdSql.getData(0,4)} {tdSql.getData(0,5)} {tdSql.getData(0,6)} {tdSql.getData(0,7)} {tdSql.getData(0,8)} {tdSql.getData(0,9)}"
        )

        tdSql.checkRows(3)
        tdSql.checkData(2, 0, db)
        tdSql.checkData(2, 2, 8)
        tdSql.checkData(2, 3, 0)
        tdSql.checkData(2, 4, 1)
        tdSql.checkData(2, 6, "2d")
        tdSql.checkData(2, 7, "10d,10d,10d")

        tdLog.info(f"=============== step2")
        tdSql.error(f"create database {db}")
        tdSql.execute(f"create database if not exists {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(3)

        tdLog.info(f"=============== step3")
        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step4")
        tdSql.error(f"drop database {db}")

        tdLog.info(f"=============== step5")
        tdSql.execute(f"create database {db} replica 1 duration 15 keep 1500")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkData(2, 0, db)
        tdSql.checkData(2, 3, 0)
        tdSql.checkData(2, 4, 1)
        tdSql.checkData(2, 6, "15d")

        tdLog.info(f"=============== step6")
        i = i + 1
        while i < 5:
            db = dbPrefix + str(i)
            st = stPrefix + str(i)
            tb = tbPrefix + str(i)

            tdLog.info(f"create database {db}")
            tdSql.execute(f"create database {db}")

            tdLog.info(f"use {db}")
            tdSql.execute(f"use {db}")

            tdLog.info(f"create table {st} (ts timestamp, i int) tags (j int)")
            tdSql.execute(f"create table {st} (ts timestamp, i int) tags (j int)")

            tdLog.info(f"create table {tb} using {st} tags(1)")
            tdSql.execute(f"create table {tb} using {st} tags(1)")

            tdSql.query(f"show stables")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, st)

            tdSql.query(f"show tables")
            tdSql.checkRows(1)
            tdSql.checkData(0, 0, tb)

            i = i + 1

        tdLog.info(f"=============== step7")
        i = 0
        while i < 5:
            db = dbPrefix + str(i)
            tdSql.execute(f"drop database {db}")
            i = i + 1

        tdLog.info(f"=============== step8")
        i = 1
        db = dbPrefix + str(i)
        st = stPrefix + str(i)
        tb = tbPrefix + str(i)
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")
        tdSql.execute(f"create table {st} (ts timestamp, i int) tags (j int)")
        tdSql.execute(f"create table {tb} using {st} tags(1)")

        tdSql.query(f"show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, st)

        tdSql.query(f"show tables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, tb)

        tdLog.info(f"=============== step9")
        tdSql.execute(f"drop database {db}")

        tdLog.info(f"=============== step10")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        tdSql.query(f"show stables")
        tdSql.checkRows(0)

        tdSql.query(f"show tables")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step11")
        tdSql.execute(f"create table {st} (ts timestamp, i int) tags (j int)")
        tdSql.execute(f"create table {tb} using {st} tags(1)")

        tdSql.query(f"show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, st)

        tdSql.query(f"show tables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, tb)

        tdLog.info(f"=============== step12")
        tdSql.execute(f"drop database {db}")

        tdLog.info(f"=============== step13")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        tdSql.query(f"show stables")
        tdSql.checkRows(0)

        tdSql.query(f"show tables")
        tdSql.checkRows(0)

        tdLog.info(f"============== step14")
        tdSql.execute(f"create table {st} (ts timestamp, i int) tags (j int)")
        tdSql.execute(f"create table {tb} using {st} tags(1)")

        tdSql.query(f"show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, st)

        tdSql.query(f"show tables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, tb)

        tdSql.execute(f"insert into {tb} values (now+1a, 0)")
        tdSql.execute(f"insert into {tb} values (now+2a, 1)")
        tdSql.execute(f"insert into {tb} values (now+3a, 2)")
        tdSql.execute(f"insert into {tb} values (now+4a, 3)")
        tdSql.execute(f"insert into {tb} values (now+5a, 4)")

        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(5)

        tdSql.query(f"select * from {st}")
        tdSql.checkRows(5)

        tdLog.info(f"=============== step14")
        tdSql.execute(f"drop database {db}")

        tdLog.info(f"=============== step15")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        tdSql.query(f"show stables")
        tdSql.checkRows(0)

        tdSql.query(f"show tables")
        tdSql.checkRows(0)

        tdLog.info(f"=============== step16")
        tdSql.execute(f"create table {st} (ts timestamp, i int) tags (j int)")
        tdSql.execute(f"create table {tb} using {st} tags(1)")

        tdSql.query(f"show stables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, st)

        tdSql.query(f"show tables")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, tb)

        tdSql.execute(f"insert into {tb} values (now+1a, 0)")
        tdSql.execute(f"insert into {tb} values (now+2a, 1)")
        tdSql.execute(f"insert into {tb} values (now+3a, 2)")
        tdSql.execute(f"insert into {tb} values (now+4a, 3)")
        tdSql.execute(f"insert into {tb} values (now+5a, 4)")

        tdSql.query(f"select * from {tb}")
        tdSql.checkRows(5)

        tdSql.query(f"select * from {st}")
        tdSql.checkRows(5)

        tdSql.execute(f"drop database {db}")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.checkRows(2)