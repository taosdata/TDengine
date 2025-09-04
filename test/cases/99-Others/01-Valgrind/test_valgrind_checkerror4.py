import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestValgrindCheckError4:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_valgrind_check_error4(self):
        """valgrind check error 4

        1. -

        Catalog:
            - Others:Valgrind

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-5 Simon Guan Migrated from tsim/valgrind/checkError4.sim

        """

        tdLog.info(
            f"=============== step1: create drop select * from information_schema.ins_dnodes"
        )
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"=============== step2: create db")
        tdSql.execute(f"create database d1 vgroups 2 buffer 3")
        tdSql.execute(f"create database d2 vgroups 2 buffer 3")
        tdSql.query(f"select * from information_schema.ins_databases;")
        tdSql.query(f"show d1.vgroups;")

        tdLog.info(f"=============== step3: create show stable")
        tdSql.execute(
            f"create table if not exists d1.stb1 (ts timestamp, c1 int, c2 float, c3 double) tags (t1 int unsigned)"
        )
        tdSql.execute(
            f"create table if not exists d1.stb2 (ts timestamp, c1 int, c2 float, c3 double) tags (t1 int unsigned)"
        )
        tdSql.execute(
            f"create table if not exists d2.stb1 (ts timestamp, c1 int, c2 float, c3 double) tags (t1 int unsigned)"
        )
        tdSql.query(f"show d1.stables")
        tdSql.query(f"show d2.stables")

        tdLog.info(f"=============== step4: create show table")
        tdSql.execute(f"create table d1.ct1 using d1.stb1 tags(1000)")
        tdSql.execute(f"create table d1.ct2 using d1.stb1 tags(2000)")
        tdSql.execute(f"create table d1.ct3 using d1.stb2 tags(3000)")
        tdSql.execute(f"create table d2.ct1 using d2.stb1 tags(1000)")
        tdSql.execute(f"create table d2.ct2 using d2.stb1 tags(2000)")
        tdSql.query(f"show d1.tables")
        tdSql.query(f"show d2.tables")

        tdLog.info(f"=============== step5: insert data")
        tdSql.execute(f"insert into d1.ct1 values(now+0s, 10, 2.0, 3.0)")
        tdSql.execute(
            f"insert into d2.ct1 values(now+1s, 11, 2.1, 3.1)(now+2s, -12, -2.2, -3.2)(now+3s, -13, -2.3, -3.3)"
        )
        tdSql.execute(f"insert into d1.ct2 values(now+0s, 10, 2.0, 3.0)")
        tdSql.execute(
            f"insert into d2.ct2 values(now+1s, 11, 2.1, 3.1)(now+2s, -12, -2.2, -3.2)(now+3s, -13, -2.3, -3.3)"
        )
        tdSql.execute(
            f"insert into d1.ct3 values('2021-01-01 00:00:00.000', 10, 2.0, 3.0)"
        )

        tdLog.info(f"=============== step6: create db")
        tdSql.execute(f"drop table d1.ct1")
        tdSql.execute(f"drop table d1.stb2")
        tdSql.execute(f"drop database d1")
        tdSql.execute(f"drop database d2")

        tdLog.info(f"=============== step7: repeat")
        tdSql.execute(f"create database d1 vgroups 2 buffer 3")
        tdSql.execute(f"create database d2 vgroups 2 buffer 3")
        tdSql.execute(
            f"create table if not exists d1.stb1 (ts timestamp, c1 int, c2 float, c3 double) tags (t1 int unsigned)"
        )
        tdSql.execute(
            f"create table if not exists d1.stb2 (ts timestamp, c1 int, c2 float, c3 double) tags (t1 int unsigned)"
        )
        tdSql.execute(
            f"create table if not exists d2.stb1 (ts timestamp, c1 int, c2 float, c3 double) tags (t1 int unsigned)"
        )
        tdSql.execute(f"create table d1.ct1 using d1.stb1 tags(1000)")
        tdSql.execute(f"create table d1.ct2 using d1.stb1 tags(2000)")
        tdSql.execute(f"create table d1.ct3 using d1.stb2 tags(3000)")
        tdSql.execute(f"create table d2.ct1 using d2.stb1 tags(1000)")
        tdSql.execute(f"create table d2.ct2 using d2.stb1 tags(2000)")
        tdSql.execute(f"insert into d1.ct1 values(now+0s, 10, 2.0, 3.0)")
        tdSql.execute(
            f"insert into d2.ct1 values(now+1s, 11, 2.1, 3.1)(now+2s, -12, -2.2, -3.2)(now+3s, -13, -2.3, -3.3)"
        )
        tdSql.execute(f"insert into d1.ct2 values(now+0s, 10, 2.0, 3.0)")
        tdSql.execute(
            f"insert into d2.ct2 values(now+1s, 11, 2.1, 3.1)(now+2s, -12, -2.2, -3.2)(now+3s, -13, -2.3, -3.3)"
        )
        tdSql.execute(
            f"insert into d1.ct3 values('2021-01-01 00:00:00.000', 10, 2.0, 3.0)"
        )
        tdSql.execute(f"drop table d1.ct1")
        tdSql.execute(f"drop table d1.stb2")
        tdSql.execute(f"drop database d1")
        tdSql.execute(f"drop database d2")
