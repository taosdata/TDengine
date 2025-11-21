from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestRegex:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_regex(self):
        """Operator match

        Using MATCH and NMATCH for regular expression matching


        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-8-20 Simon Guan Migrated from tsim/parser/regex.sim

        """

        db = "testdb"
        tdSql.execute(f"drop database if exists {db}")
        tdSql.execute(f"create database {db}")
        tdSql.execute(f"use {db}")

        tdLog.info(f"======================== regular expression match test")
        st_name = "st"
        ct1_name = "ct1"
        ct2_name = "ct2"

        tdSql.execute(
            f"create table {st_name} (ts timestamp, c1b binary(20)) tags(t1b binary(20));"
        )
        tdSql.execute(f"create table {ct1_name} using {st_name} tags('taosdata1')")
        tdSql.execute(f"create table {ct2_name} using {st_name} tags('taosdata2')")
        tdSql.execute(f"create table not_match using {st_name} tags('NOTMATCH')")

        tdSql.execute(f"insert into {ct1_name} values(now, 'this is engine')")
        tdSql.execute(f"insert into {ct2_name} values(now, 'this is app egnine')")
        tdSql.execute(f"insert into not_match values (now + 1s, '1234')")

        tdSql.query(f"select tbname from {st_name} where tbname match '.*'")
        tdSql.checkRows(3)

        tdSql.query(f"select tbname from {st_name} where tbname match '^ct[[:digit:]]'")
        tdSql.checkRows(2)

        tdSql.query(
            f"select tbname from {st_name} where tbname nmatch '^ct[[:digit:]]'"
        )
        tdSql.checkRows(1)

        tdSql.query(f"select tbname from {st_name} where tbname match '.*'")
        tdSql.checkRows(3)

        tdSql.query(f"select tbname from {st_name} where tbname nmatch '.*'")
        tdSql.checkRows(0)

        tdSql.query(f"select tbname from {st_name} where t1b match '[[:lower:]]+'")
        tdSql.checkRows(2)

        tdSql.query(f"select tbname from {st_name} where t1b nmatch '[[:lower:]]+'")
        tdSql.checkRows(1)

        tdSql.query(f"select c1b from {st_name} where c1b match 'engine'")
        tdSql.checkData(0, 0, "this is engine")

        tdSql.checkRows(1)

        tdSql.query(f"select c1b from {st_name} where c1b nmatch 'engine' order by ts")
        tdSql.checkData(0, 0, "this is app egnine")

        tdSql.checkRows(2)

        tdSql.error(f"select c1b from {st_name} where c1b match e;")
        tdSql.error(f"select c1b from {st_name} where c1b nmatch e;")

        tdSql.execute(
            f"create table wrong_type(ts timestamp, c0 tinyint, c1 smallint, c2 int, c3 bigint, c4 float, c5 double, c6 bool, c7 nchar(20)) tags(t0 tinyint, t1 smallint, t2 int, t3 bigint, t4 float, t5 double, t6 bool, t7 nchar(10))"
        )
        tdSql.execute(
            f"insert into wrong_type_1 using wrong_type tags(1, 2, 3, 4, 5, 6, true, 'notsupport') values(now, 1, 2, 3, 4, 5, 6, false, 'notsupport')"
        )
        tdSql.error(f"select * from wrong_type where ts match '.*'")
        tdSql.error(f"select * from wrong_type where ts nmatch '.*'")
        tdSql.error(f"select * from wrong_type where c0 match '.*'")
        tdSql.error(f"select * from wrong_type where c0 nmatch '.*'")
        tdSql.error(f"select * from wrong_type where c1 match '.*'")
        tdSql.error(f"select * from wrong_type where c1 nmatch '.*'")
        tdSql.error(f"select * from wrong_type where c2 match '.*'")
        tdSql.error(f"select * from wrong_type where c2 nmatch '.*'")
        tdSql.error(f"select * from wrong_type where c3 match '.*'")
        tdSql.error(f"select * from wrong_type where c3 nmatch '.*'")
        tdSql.error(f"select * from wrong_type where c4 match '.*'")
        tdSql.error(f"select * from wrong_type where c4 nmatch '.*'")
        tdSql.error(f"select * from wrong_type where c5 match '.*'")
        tdSql.error(f"select * from wrong_type where c5 nmatch '.*'")
        tdSql.error(f"select * from wrong_type where c6 match '.*'")
        tdSql.error(f"select * from wrong_type where c6 nmatch '.*'")
        tdSql.query(f"select * from wrong_type where c7 match '.*'")
        tdSql.query(f"select * from wrong_type where c7 nmatch '.*'")
        tdSql.error(f"select * from wrong_type where t1 match '.*'")
        tdSql.error(f"select * from wrong_type where t1 nmatch '.*'")
        tdSql.error(f"select * from wrong_type where t2 match '.*'")
        tdSql.error(f"select * from wrong_type where t2 nmatch '.*'")
        tdSql.error(f"select * from wrong_type where t3 match '.*'")
        tdSql.error(f"select * from wrong_type where t3 nmatch '.*'")
        tdSql.error(f"select * from wrong_type where t4 match '.*'")
        tdSql.error(f"select * from wrong_type where t4 nmatch '.*'")
        tdSql.error(f"select * from wrong_type where t5 match '.*'")
        tdSql.error(f"select * from wrong_type where t5 nmatch '.*'")
        tdSql.error(f"select * from wrong_type where t6 match '.*'")
        tdSql.error(f"select * from wrong_type where t6 nmatch '.*'")
        tdSql.query(f"select * from wrong_type where t7 match '.*'")
        tdSql.query(f"select * from wrong_type where t7 nmatch '.*'")
