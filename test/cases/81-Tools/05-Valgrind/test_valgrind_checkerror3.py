import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestValgrindCheckError3:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_valgrind_check_error3(self):
        """valgrind check error 3

        1. -

        Catalog:
            - Others:Valgrind

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-5 Simon Guan Migrated from tsim/valgrind/checkError3.sim

        """

        tdLog.info(
            f"=============== step1: create drop select * from information_schema.ins_dnodes"
        )
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"=============== step2: create db")
        tdSql.execute(f"create database d1 vgroups 3 buffer 3")
        tdSql.query(f"select * from information_schema.ins_databases")
        tdSql.execute(f"use d1")
        tdSql.query(f"show vgroups")

        tdLog.info(f"=============== step3: create show stable, include all type")
        tdSql.execute(
            f"create table if not exists stb (ts timestamp, c1 bool, c2 tinyint, c3 smallint, c4 int, c5 bigint, c6 float, c7 double, c8 binary(16), c9 nchar(16), c10 timestamp, c11 tinyint unsigned, c12 smallint unsigned, c13 int unsigned, c14 bigint unsigned) tags (t1 bool, t2 tinyint, t3 smallint, t4 int, t5 bigint, t6 float, t7 double, t8 binary(16), t9 nchar(16), t10 timestamp, t11 tinyint unsigned, t12 smallint unsigned, t13 int unsigned, t14 bigint unsigned)"
        )
        tdSql.execute(
            f"create stable if not exists stb_1 (ts timestamp, c1 int) tags (j int)"
        )
        tdSql.execute(f"create table stb_2 (ts timestamp, c1 int) tags (t1 int)")
        tdSql.execute(f"create stable stb_3 (ts timestamp, c1 int) tags (t1 int)")
        tdSql.query(f"show stables")
        tdSql.checkRows(4)

        tdSql.query(f"show stables like 'stb'")

        tdLog.info(f"=============== step4: ccreate child table")
        tdSql.execute(
            f"create table c1 using stb tags(true, -1, -2, -3, -4, -6.0, -7.0, 'child tbl 1', 'child tbl 1', '2022-02-25 18:00:00.000', 10, 20, 30, 40)"
        )
        tdSql.execute(
            f"create table c2 using stb tags(false, -1, -2, -3, -4, -6.0, -7.0, 'child tbl 2', 'child tbl 2', '2022-02-25 18:00:00.000', 10, 20, 30, 40)"
        )
        tdSql.query(f"show tables")
        tdSql.checkRows(2)

        tdLog.info(f"=============== step5: insert data")
        tdSql.execute(
            f"insert into c1 values(now-1s, true, -1, -2, -3, -4, -6.0, -7.0, 'child tbl 1', 'child tbl 1', '2022-02-25 18:00:00.000', 10, 20, 30, 40)"
        )
        tdSql.execute(
            f"insert into c1 values(now+0s, true, -1, -2, -3, -4, -6.0, -7.0, 'child tbl 1', 'child tbl 1', '2022-02-25 18:00:00.000', 10, 20, 30, 40) (now+1s, true, -1, -2, -3, -4, -6.0, -7.0, 'child tbl 1', 'child tbl 1', '2022-02-25 18:00:00.000', 10, 20, 30, 40) (now+2s, true, -1, -2, -3, -4, -6.0, -7.0, 'child tbl 1', 'child tbl 1', '2022-02-25 18:00:00.000', 10, 20, 30, 40)"
        )
        tdSql.execute(
            f"insert into c2 values(now-1s, true, -1, -2, -3, -4, -6.0, -7.0, 'child tbl 1', 'child tbl 1', '2022-02-25 18:00:00.000', 10, 20, 30, 40)"
        )
        tdSql.execute(
            f"insert into c2 values(now+0s, true, -1, -2, -3, -4, -6.0, -7.0, 'child tbl 1', 'child tbl 1', '2022-02-25 18:00:00.000', 10, 20, 30, 40) (now+1s, true, -1, -2, -3, -4, -6.0, -7.0, 'child tbl 1', 'child tbl 1', '2022-02-25 18:00:00.000', 10, 20, 30, 40) (now+2s, true, -1, -2, -3, -4, -6.0, -7.0, 'child tbl 1', 'child tbl 1', '2022-02-25 18:00:00.000', 10, 20, 30, 40)"
        )

        tdLog.info(f"=============== step6: alter insert")
        tdSql.execute(
            f"insert into c3 using stb tags(true, -1, -2, -3, -4, -6.0, -7.0, 'child tbl 1', 'child tbl 1', '2022-02-25 18:00:00.000', 10, 20, 30, 40) values(now-1s, true, -1, -2, -3, -4, -6.0, -7.0, 'child tbl 1', 'child tbl 1', '2022-02-25 18:00:00.000', 10, 20, 30, 40)"
        )
        tdSql.execute(
            f"insert into c3 using stb tags(true, -1, -2, -3, -4, -6.0, -7.0, 'child tbl 1', 'child tbl 1', '2022-02-25 18:00:00.000', 10, 20, 30, 40) values(now+0s, true, -1, -2, -3, -4, -6.0, -7.0, 'child tbl 1', 'child tbl 1', '2022-02-25 18:00:00.000', 10, 20, 30, 40)"
        )

        tdLog.info(f"===============  restart")
        sc.dnodeStop(1)
        sc.dnodeStart(1)

        tdLog.info(f"=============== stepa: query data")
        clusterComCheck.checkDnodes(1)

        tdSql.query(f"select * from c1")
        tdSql.query(f"select * from stb")
        tdSql.query(f"select * from stb_1")
        tdSql.query(f"select ts, c1, c2, c3 from c1")
        tdSql.query(f"select ts, c1, c2, c3 from stb")
        tdSql.query(f"select ts, c1 from stb_2")
        tdSql.query(f"select ts, c1, t1 from c1")
        tdSql.query(f"select ts, c1, t1 from stb")
        tdSql.query(f"select ts, c1, t1 from stb_2")

        tdLog.info(f"=============== stepb: count")
        tdSql.query(f"select count(*) from c1;")
        tdSql.query(f"select count(*) from stb;")
        tdSql.query(f"select count(ts), count(c1), count(c2), count(c3) from c1")
        tdSql.query(f"select count(ts), count(c1), count(c2), count(c3) from stb")

        tdLog.info(f"=============== stepc: func")
        tdSql.query(f"select first(ts), first(c1), first(c2), first(c3) from c1")
        tdSql.query(f"select min(c2), min(c3), min(c4) from c1")
        tdSql.query(f"select max(c2), max(c3), max(c4) from c1")
        tdSql.query(f"select sum(c2), sum(c3), sum(c4) from c1")
