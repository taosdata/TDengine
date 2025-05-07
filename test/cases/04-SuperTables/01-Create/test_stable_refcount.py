from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestStableRefcount:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stable_refcount(self):
        """create super table 2

        1. create database
        2. create stable
        3. insert data
        4. drop table
        5. repeat 5 times

        Catalog:
            - SuperTables:Create

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-4-30 Simon Guan Migrated from tsim/stable/refcount.sim

        """

        tdLog.info(f"=============== step1")
        tdSql.prepare(dbname="d1", drop=True)
        tdSql.execute(f"use d1;")
        tdSql.execute(f"create table d1.t1 (ts timestamp, i int);")
        tdSql.execute(f"create table d1.t2 (ts timestamp, i int);")
        tdSql.execute(f"create table d1.t3 (ts timestamp, i int);")
        tdSql.execute(f"insert into d1.t1 values(now, 1);")
        tdSql.execute(f"insert into d1.t2 values(now, 1);")
        tdSql.execute(f"drop table d1.t1;")
        tdSql.execute(f"drop database d1;")

        tdSql.query(f"select * from information_schema.ins_databases;")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step2")
        tdSql.prepare(dbname="d2", drop=True)
        tdSql.execute(f"use d2;")
        tdSql.execute(f"create table d2.t1 (ts timestamp, i int);")
        tdSql.execute(f"create table d2.t2 (ts timestamp, i int);")
        tdSql.execute(f"create table d2.t3 (ts timestamp, i int);")
        tdSql.execute(f"insert into d2.t1 values(now, 1);")
        tdSql.execute(f"insert into d2.t2 values(now, 1);")
        tdSql.execute(f"drop table d2.t1;")
        tdSql.execute(f"drop table d2.t2;")
        tdSql.execute(f"drop table d2.t3;")

        tdSql.query(f"show d2.tables;")
        tdSql.checkRows(0)

        tdSql.query(f"show d2.vgroups;")
        tdSql.checkRows(2)

        tdSql.execute(f"drop database d2;")

        tdSql.query(f"select * from information_schema.ins_databases;")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step3")
        tdSql.prepare(dbname="d3", drop=True)
        tdSql.execute(f"use d3;")
        tdSql.execute(f"create table d3.st (ts timestamp, i int) tags (j int);")
        tdSql.execute(f"create table d3.t1 using d3.st tags(1);")
        tdSql.execute(f"create table d3.t2 using d3.st tags(1);")
        tdSql.execute(f"create table d3.t3 using d3.st tags(1);")
        tdSql.execute(f"insert into d3.t1 values(now, 1);")
        tdSql.execute(f"drop table d3.t1;")
        tdSql.execute(f"drop table d3.t2;")
        tdSql.execute(f"drop table d3.t3;")

        tdSql.query(f"show d3.tables;")
        tdSql.checkRows(0)

        tdSql.query(f"show d3.vgroups;")
        tdSql.checkRows(2)

        tdSql.execute(f"drop database d3;")

        tdSql.query(f"select * from information_schema.ins_databases;")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step4")
        tdSql.prepare(dbname="d4", drop=True)
        tdSql.execute(f"use d4;")
        tdSql.execute(f"create table d4.st (ts timestamp, i int) tags (j int);")
        tdSql.execute(f"create table d4.t1 using d4.st tags(1);")
        tdSql.execute(f"create table d4.t2 using d4.st tags(1);")
        tdSql.execute(f"create table d4.t3 using d4.st tags(1);")
        tdSql.execute(f"insert into d4.t1 values(now, 1);")
        tdSql.execute(f"drop table d4.t1;")
        tdSql.execute(f"drop table d4.st;")

        tdSql.query(f"show d4.tables;")
        tdSql.checkRows(0)

        tdSql.query(f"show d4.stables;")
        tdSql.checkRows(0)

        tdSql.execute(f"drop database d4;")

        tdSql.query(f"select * from information_schema.ins_databases;")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step5")
        tdSql.prepare(dbname="d5", drop=True)
        tdSql.execute(f"create table d5.st (ts timestamp, i int) tags (j int);")
        tdSql.execute(f"create table d5.t1 using d5.st tags(1);")
        tdSql.execute(f"create table d5.t2 using d5.st tags(1);")
        tdSql.execute(f"create table d5.t3 using d5.st tags(1);")
        tdSql.execute(f"insert into d5.t1 values(now, 1);")
        tdSql.execute(f"drop table d5.t1;")

        tdSql.execute(f"drop database d5;")

        tdSql.query(f"select * from information_schema.ins_databases;")
        tdSql.checkRows(2)
