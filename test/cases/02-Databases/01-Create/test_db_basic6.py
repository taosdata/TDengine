import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, clusterComCheck


class TestDatabaseBasic6:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_database_basic6(self):
        """create database 6

        1. create database use options such as replcai, duraiton, keep, minrows
        2. select from information_schema.ins_databases and check result
        3. repeatedly perform operations such as create database, drop database, create table, and writing data

        Catalog:
            - Database:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-12 Simon Guan Migrated from tsim/db/basic6.sim

        """

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
