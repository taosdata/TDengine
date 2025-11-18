from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestDbTbNameValidate:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_dbtbname_validate(self):
        """Name database

        1. Validates naming conventions and boundary checks for databases and tables.
        2. Verifies both valid and invalid identifiers across operations like CREATE, USE, DROP, and DESCRIBE, including handling of quoted identifiers and case sensitivity.
        3. Ensures compliance with naming rules, which allow only English letters, numbers, and underscores, and prohibit names starting with numbers or containing spaces.

        Catalog:
            - NameLimits

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-6 Simon Guan Migrated from tsim/parser/dbtbnameValidate.sim

        """

        tdLog.info(
            f"========== db name and table name check in create and drop, describe"
        )
        tdSql.execute(f"create database abc keep 36500")
        tdSql.error(f"create database 'abc123'")
        tdSql.error(f"create database '_ab1234'")
        tdSql.error(f"create database 'ABC123'")
        tdSql.error(f"create database '_ABC123'")
        tdSql.error(f"create database 'aABb123 '")
        tdSql.error(f"create database ' xyz '")
        tdSql.error(f"create database ' XYZ '")

        tdSql.error(f"use 'abc123'")
        tdSql.error(f"use '_ab1234'")
        tdSql.error(f"use 'ABC123'")
        tdSql.error(f"use '_ABC123'")
        tdSql.error(f"use 'aABb123'")
        tdSql.error(f"use ' xyz '")
        tdSql.error(f"use ' XYZ '")

        tdSql.error(f"drop database if exists 'abc123'")
        tdSql.error(f"drop database if exists '_ab1234'")
        tdSql.error(f"drop database if exists 'ABC123'")
        tdSql.error(f"drop database if exists '_ABC123'")
        tdSql.error(f"drop database if exists 'aABb123'")
        tdSql.error(f"drop database if exists ' xyz '")
        tdSql.error(f"drop database if exists ' XYZ '")

        tdSql.execute(f"use abc")
        tdSql.execute(f"create table  abc.cc    (ts timestamp, c int)")
        tdSql.error(f"create table 'abc.Dd'   (ts timestamp, c int)")
        tdSql.error(f"create table 'abc'.ee   (ts timestamp, c int)")
        tdSql.error(f"create table 'abc'.'FF' (ts timestamp, c int)")
        tdSql.error(f"create table  abc.'gG'  (ts timestamp, c int)")
        tdSql.error(f"create table  table.'a1'  (ts timestamp, c int)")
        tdSql.error(f"create table 'table'.'b1' (ts timestamp, c int)")
        tdSql.error(f"create table 'table'.'b1' (ts timestamp, c int)")

        tdSql.execute(
            f"create table mt (ts timestamp, c1 int, c2 bigint, c3 float, c4 double, c5 smallint, c6 tinyint, c7 bool, c8 binary(10), c9 nchar(10)) tags(t1 int, t2 nchar(20), t3 binary(20), t4 bigint, t5 smallint, t6 double)"
        )
        tdSql.execute(
            f"create table sub_001 using mt tags ( 1 , 'tag_nchar' , 'tag_bianry' , 4 , 5 , 6.1 )"
        )
        tdSql.error(
            f"create table sub_002 using mt tags( 2 , tag_nchar , tag_bianry , 4 , 5 , 6.2 )"
        )
        tdSql.execute(
            f"insert into sub_dy_tbl using mt tags ( 3 , 'tag_nchar' , 'tag_bianry' , 4 , 5 , 6.3 ) values (now, 1, 2, 3.01, 4.02, 5, 6, true, 'binary_8', 'nchar_9')"
        )

        tdSql.query(f"describe abc.cc")
        tdSql.error(f"describe 'abc.Dd'")
        tdSql.error(f"describe 'abc'.ee")
        tdSql.error(f"describe 'abc'.'FF'")
        tdSql.error(f"describe abc.'gG'")

        tdSql.query(f"describe cc")
        tdSql.error(f"describe 'Dd'")
        tdSql.error(f"describe ee")
        tdSql.error(f"describe 'FF'")
        tdSql.error(f"describe 'gG'")

        tdSql.query(f"describe mt")
        tdSql.query(f"describe sub_001")
        tdSql.query(f"describe sub_dy_tbl")

        tdSql.error(f"describe Dd")
        tdSql.error(f"describe FF")
        tdSql.error(f"describe gG")

        tdSql.execute(f"drop table abc.cc")
        tdSql.error(f"drop table  'abc.Dd'")
        tdSql.error(f"drop table  'abc'.ee")
        tdSql.error(f"drop table  'abc'.'FF'")
        tdSql.error(f"drop table  abc.'gG'")

        tdSql.execute(f"drop table   sub_001")
        tdSql.execute(f"drop table   sub_dy_tbl")
        tdSql.execute(f"drop table   mt")

        tdLog.info(f"========== insert data by multi-format")
        tdSql.execute(
            f"create table abc.tk_mt (ts timestamp, a int, b binary(16), c bool, d float, e double, f nchar(16)) tags (t1 int, t2 binary(16))"
        )

        tdSql.execute(f"create table abc.tk_subt001 using tk_mt tags(1, 'subt001')")
        tdSql.execute(
            f"insert into abc.tk_subt001                  values (now-1w, 3, 'binary_3', true,  1.003, 2.003, 'nchar_3')"
        )
        tdSql.execute(
            f"insert into abc.tk_subt001 (ts, a, c, e, f) values (now-1d, 4,             false,        2.004, 'nchar_4')"
        )
        tdSql.execute(
            f"insert into abc.tk_subt001 (ts, a, c, e, f) values (now-1h, 5,             false,        2.005, 'nchar_5')"
        )
        tdSql.execute(
            f"insert into abc.tk_subt001 (ts, b, d)       values (now-1m,    'binary_6',        1.006)"
        )
        tdSql.execute(
            f"insert into abc.tk_subt001 (ts, b, d)       values (now-1s,    'binary_7',        1.007)"
        )
        tdSql.execute(
            f"insert into abc.tk_subt001 (ts, b, d)       values (now-1a,    'binary_8',        1.008)"
        )
        tdSql.query(f"select * from tk_subt001")
        tdSql.checkRows(6)

        tdSql.execute(
            f"insert into abc.tk_subt002 using tk_mt tags (22, 'subt002x')   values (now+1s, 2001, 'binary_2001', true,  2001.001, 2001.001, 'nchar_2001')"
        )
        tdSql.execute(
            f"insert into abc.tk_subt002 using tk_mt tags (2, 'subt002')   values (now+1m, 2002, 'binary_2002', false, 2002.001, 2002.001, 'nchar_2002')"
        )
        tdSql.execute(
            f"insert into abc.tk_subt002 using tk_mt tags (2, 'subt002')   values (now+1h, 2003, 'binary_2003', false, 2003.001, 2003.001, 'nchar_2003')"
        )
        tdSql.execute(
            f"insert into abc.tk_subt002 using tk_mt tags (2, 'subt002')   values (now+1d, 2004, 'binary_2004', true,  2004.001, 2004.001, 'nchar_2004')"
        )
        tdSql.execute(
            f"insert into abc.tk_subt002 using tk_mt tags (2, 'subt002')   values (now+1w, 2005, 'binary_2005', false, 2005.001, 2005.001, 'nchar_2005')"
        )
        tdSql.query(f"select * from tk_subt002")
        tdSql.checkRows(5)

        tdSql.execute(
            f"insert into abc.tk_subt003 (ts, a, c, e, f) using tk_mt tags (3, 'subt003')    values (now-38d, 3004,                false, 3004.001,           'nchar_3004')"
        )
        tdSql.execute(
            f"insert into abc.tk_subt003 (ts, a, c, e, f) using tk_mt tags (3, 'subt003')    values (now-37d, 3005,                false, 3005.001,           'nchar_3005')"
        )
        tdSql.execute(
            f"insert into abc.tk_subt003                                                     values (now-36d, 3006, 'binary_3006', true,  3006.001, 3006.001, 'nchar_3006')"
        )
        tdSql.execute(
            f"insert into abc.tk_subt003 (ts, a, c, e, f) using tk_mt tags (33, 'subt003x')  values (now-35d, 3007,                false, 3007.001,           'nchar_3007')"
        )
        tdSql.query(f"select * from tk_subt003")
        tdSql.checkRows(4)

        tdLog.info(f"================>td-4147")
        tdSql.error(
            f"create table tx(ts timestamp, a1234_0123456789_0123456789_0123456789_0123456789_0123456789_0123456789 int)"
        )
