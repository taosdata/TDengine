import time
from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestValgrindCheckError5:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_valgrind_check_error5(self):
        """valgrind check error 5

        1. -

        Catalog:
            - Others:Valgrind

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-5 Simon Guan Migrated from tsim/valgrind/checkError5.sim

        """

        tdLog.info(
            f"=============== step1: create drop select * from information_schema.ins_dnodes"
        )
        clusterComCheck.checkDnodes(1)

        tdLog.info(f"=============== step2: create db")
        tdSql.execute(f"create database db")
        tdSql.execute(f"use db")
        tdSql.execute(
            f'create table db.stb (ts timestamp, c1 int, c2 binary(4)) tags(t1 int, t2 float, t3 binary(16)) comment "abd"'
        )
        tdSql.execute(f'create table db.c1 using db.stb tags(101, 102, "103")')

        tdLog.info(f"=============== step3: alter stb")
        tdSql.error(f"alter table db.stb add column ts int")
        tdSql.execute(f"alter table db.stb add column c3 int")
        tdSql.execute(f"alter table db.stb add column c4 bigint")
        tdSql.execute(f"alter table db.stb add column c5 binary(12)")
        tdSql.execute(f"alter table db.stb drop column c1")
        tdSql.execute(f"alter table db.stb drop column c4")
        tdSql.execute(f"alter table db.stb MODIFY column c2 binary(32)")
        tdSql.execute(f"alter table db.stb add tag t4 bigint")
        tdSql.execute(f"alter table db.stb add tag c1 int")
        tdSql.execute(f"alter table db.stb add tag t5 binary(12)")
        tdSql.execute(f"alter table db.stb drop tag c1")
        tdSql.execute(f"alter table db.stb drop tag t5")
        tdSql.execute(f"alter table db.stb MODIFY tag t3 binary(32)")
        tdSql.execute(f"alter table db.stb rename tag t1 tx")
        tdSql.execute(f"alter table db.stb comment 'abcde' ;")
        tdSql.execute(f"drop table db.stb")

        tdLog.info(f"=============== step4: alter tb")
        tdSql.execute(f"create table tb (ts timestamp, a int)")
        tdSql.execute(f"insert into tb values(now-28d, -28)")
        tdSql.query(f"select count(a) from tb")
        tdSql.execute(f"alter table tb add column b smallint")
        tdSql.execute(f"insert into tb values(now-25d, -25, 0)")
        tdSql.query(f"select count(b) from tb")
        tdSql.execute(f"alter table tb add column c tinyint")
        tdSql.execute(f"insert into tb values(now-22d, -22, 3, 0)")
        tdSql.query(f"select count(c) from tb")
        tdSql.execute(f"alter table tb add column d int")
        tdSql.execute(f"insert into tb values(now-19d, -19, 6, 0, 0)")
        tdSql.query(f"select count(d) from tb")
        tdSql.execute(f"alter table tb add column e bigint")
        tdSql.execute(f"alter table tb add column f float")
        tdSql.execute(f"alter table tb add column g double")
        tdSql.execute(f"alter table tb add column h binary(10)")
        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from tb"
        )
        tdSql.query(f"select * from tb order by ts desc")

        tdLog.info(f"=============== step5: alter stb and insert data")
        tdSql.execute(
            f'create table stb (ts timestamp, c1 int, c2 binary(4)) tags(t1 int, t2 float, t3 binary(16)) comment "abd"'
        )
        tdSql.query(
            f"select * from information_schema.ins_stables where db_name = 'db'"
        )
        tdSql.query(f"describe stb")
        tdSql.error(f"alter table stb add column ts int")

        tdSql.execute(f'create table db.ctb using db.stb tags(101, 102, "103")')
        tdSql.execute(f'insert into db.ctb values(now, 1, "2")')
        tdSql.query(f"select * from information_schema.ins_tables where db_name = 'db'")
        tdSql.query(f"select * from db.stb")
        tdSql.query(f"select * from tb")

        tdSql.execute(f"alter table stb add column c3 int")
        tdSql.query(f"describe stb")
        tdSql.query(f"select * from db.stb")
        tdSql.query(f"select * from tb")
        tdSql.execute(f"insert into db.ctb values(now+1s, 1, 2, 3)")
        tdSql.query(f"select * from db.stb")

        tdSql.execute(f"alter table db.stb add column c4 bigint")
        tdSql.query(f"select * from db.stb")
        tdSql.execute(f"insert into db.ctb values(now+2s, 1, 2, 3, 4)")

        tdSql.execute(f"alter table db.stb drop column c1")
        tdSql.execute(f"reset query cache")
        tdSql.query(f"select * from tb")
        tdSql.execute(f"insert into db.ctb values(now+3s, 2, 3, 4)")
        tdSql.query(f"select * from db.stb")

        tdSql.execute(f"alter table db.stb add tag t4 bigint")
        tdSql.query(f"select * from db.stb")
        tdSql.query(f"select * from db.stb")
        tdSql.error(f'create table db.ctb2 using db.stb tags(101, "102")')
        tdSql.execute(f'create table db.ctb2 using db.stb tags(101, 102, "103", 104)')
        tdSql.execute(f"insert into db.ctb2 values(now, 1, 2, 3)")

        tdLog.info(f"=============== step6: query data")
        tdSql.query(f"select * from db.stb where tbname = 'ctb2';")
        tdSql.execute(f"alter table ctb2 set tag t1=1;")
        tdSql.execute(f"alter table ctb2 set tag t3='3';")
        tdSql.query(f"select * from db.stb where t1 = 1;")

        tdLog.info(f"=============== step7: normal table")
        tdSql.execute(f"create database d1 replica 1 duration 7 keep 50")
        tdSql.execute(f"use d1")
        tdSql.execute(f"create table tb (ts timestamp, a int)")
        tdSql.execute(f"insert into tb values(now-28d, -28)")
        tdSql.execute(f"alter table tb add column b smallint")
        tdSql.execute(f"insert into tb values(now-25d, -25, 0)")
        tdSql.execute(f"alter table tb add column c tinyint")
        tdSql.execute(f"insert into tb values(now-22d, -22, 3, 0)")
        tdSql.execute(f"alter table tb add column d int")
        tdSql.execute(f"insert into tb values(now-19d, -19, 6, 0, 0)")
        tdSql.execute(f"alter table tb add column e bigint")
        tdSql.execute(f"insert into tb values(now-16d, -16, 9, 0, 0, 0)")
        tdSql.execute(f"alter table tb add column f float")
        tdSql.execute(f"insert into tb values(now-13d, -13, 12, 0, 0, 0, 0)")
        tdSql.execute(f"alter table tb add column g double")
        tdSql.execute(f"insert into tb values(now-10d, -10, 15, 0, 0, 0, 0, 0)")
        tdSql.execute(f"alter table tb add column h binary(10)")
        tdSql.execute(f"insert into tb values(now-7d, -7, 18, 0, 0, 0, 0, 0, '0')")
        tdSql.query(
            f"select count(a), count(b), count(c), count(d), count(e), count(f), count(g), count(h) from d1.tb;"
        )
        tdSql.execute(f"alter table tb drop column a")
        tdSql.execute(f"insert into tb values(now-4d, 1, 1, 1, 1, 1, 1, '1')")
        tdSql.execute(f"alter table tb drop column b")
        tdSql.execute(f"insert into tb values(now-3d, 1, 1, 1, 1, 1, '1')")
        tdSql.execute(f"alter table tb drop column c")
        tdSql.execute(f"insert into tb values(now-2d, 1, 1, 1, 1, '1')")
        tdSql.execute(f"alter table tb drop column d")
        tdSql.execute(f"insert into tb values(now-1d, 1, 1, 1, '1')")
        tdSql.execute(f"alter table tb drop column e")
        tdSql.execute(f"insert into tb values(now, 1, 1, '1')")
        tdSql.query(f"select count(h) from tb")
