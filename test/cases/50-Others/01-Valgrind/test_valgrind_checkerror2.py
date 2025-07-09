import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestValgrindCheckError2:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_valgrind_check_error2(self):
        """valgrind check error 2

        1. -

        Catalog:
            - Others:Valgrind

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-5 Simon Guan Migrated from tsim/valgrind/checkError2.sim

        """

        tdLog.info(
            f"=============== step1: create drop select * from information_schema.ins_dnodes"
        )
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"=============== step2: create db")
        tdSql.execute(f"create database d1 vgroups 2 buffer 3")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.execute(f"use d1")
        tdSql.query(f"show vgroups")

        tdLog.info(f"=============== step3: create show stable")
        tdSql.execute(
            f"create table if not exists stb (ts timestamp, c1 int, c2 float, c3 double) tags (t1 int unsigned)"
        )
        tdSql.query(f"show stables")
        tdSql.checkRows(1)

        tdLog.info(f"=============== step4: create show table")
        tdSql.execute(f"create table ct1 using stb tags(1000)")
        tdSql.execute(f"create table ct2 using stb tags(2000)")
        tdSql.execute(f"create table ct3 using stb tags(3000)")
        tdSql.query(f"show tables")
        tdSql.checkRows(3)

        tdLog.info(f"=============== step5: insert data (null / update)")
        tdSql.execute(f"insert into ct1 values(now+0s, 10, 2.0, 3.0)")
        tdSql.execute(
            f"insert into ct1 values(now+1s, 11, 2.1, NULL)(now+2s, -12, -2.2, -3.2)(now+3s, -13, -2.3, -3.3)"
        )
        tdSql.execute(f"insert into ct2 values(now+0s, 10, 2.0, 3.0)")
        tdSql.execute(
            f"insert into ct2 values(now+1s, 11, 2.1, 3.1)(now+2s, -12, -2.2, -3.2)(now+3s, -13, -2.3, -3.3)"
        )
        tdSql.execute(
            f"insert into ct3 values('2021-01-01 00:00:00.000', NULL, NULL, 3.0)"
        )
        tdSql.execute(
            f"insert into ct3 values('2022-03-02 16:59:00.010', 3  , 4, 5), ('2022-03-02 16:59:00.010', 33 , 4, 5), ('2022-04-01 16:59:00.011', 4,  4, 5), ('2022-04-01 16:59:00.011', 6,  4, 5), ('2022-03-06 16:59:00.013', 8,  4, 5);"
        )
        tdSql.execute(
            f"insert into ct3 values('2022-03-02 16:59:00.010', 103, 1, 2), ('2022-03-02 16:59:00.010', 303, 3, 4), ('2022-04-01 16:59:00.011', 40, 5, 6), ('2022-04-01 16:59:00.011', 60, 4, 5), ('2022-03-06 16:59:00.013', 80, 4, 5);"
        )

        tdLog.info(f"=============== step6: query data")
        tdSql.query(f"select * from ct1")
        tdSql.query(f"select * from stb")
        tdSql.query(f"select c1, c2, c3 from ct1")
        tdSql.query(f"select ts, c1, c2, c3 from stb")
        tdSql.query(f"select * from ct1 where ts < now -1d and ts > now +1d")
        tdSql.query(f"select * from stb where ts < now -1d and ts > now +1d")
        tdSql.query(
            f"select * from ct1 where ts < now -1d and ts > now +1d order by ts desc"
        )
        tdSql.query(
            f"select * from stb where ts < now -1d and ts > now +1d order by ts desc"
        )
        tdSql.query(f"select * from ct1 where t1 between 1000 and 2500")
        tdSql.query(f"select * from stb where t1 between 1000 and 2500")

        tdLog.info(f"=============== step7: count")
        tdSql.query(f"select count(*) from ct1;")
        tdSql.query(f"select count(*) from stb;")
        tdSql.query(f"select count(ts), count(c1), count(c2), count(c3) from ct1")
        tdSql.query(f"select count(ts), count(c1), count(c2), count(c3) from stb")
        tdSql.query(f"select count(*) from ct1 where ts < now -1d and ts > now +1d")
        tdSql.query(f"select count(*) from stb where ts < now -1d and ts > now +1d")

        tdLog.info(f"=============== step8: func")
        tdSql.query(f"select first(ts), first(c1), first(c2), first(c3) from ct1")
        tdSql.query(f"select min(c1), min(c2), min(c3) from ct1")
        tdSql.query(f"select max(c1), max(c2), max(c3) from ct1")
        tdSql.query(f"select sum(c1), sum(c2), sum(c3) from ct1")

        tdLog.info(f"=============== step9: insert select")
        tdSql.execute(f"create table ct4 using stb tags(4000);")
        tdSql.execute(f"insert into ct4 select * from ct1;")
        tdSql.query(f"select * from ct4;")
        tdSql.execute(f"insert into ct4 select ts,c1,c2,c3 from stb;")

        tdSql.execute(f"create table tb1 (ts timestamp, c1 int, c2 float, c3 double);")
        tdSql.execute(f"insert into tb1 (ts, c1, c2, c3) select * from ct1;")
        tdSql.query(f"select * from tb1;")

        tdSql.execute(
            f"create table tb2 (ts timestamp, f1 binary(10), c1 int, c2 double);"
        )
        tdSql.execute(f"insert into tb2 (c2, c1, ts) select c2+1, c1, ts+3 from ct2;")
        tdSql.query(f"select * from tb2;")
