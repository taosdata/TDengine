from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck

class TestBitChangeFunc:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_func_bit_change(self):
        """Bit Change Func Test

        1.Create db
        2.Create supper table and sub table
        5.Insert data into and select use bit_change func and check the result

        Catalog:
            - Select

        Since: v3.0.0.0

        Labels: common,ci

        Jira: TD-6485

        History:
            - 2025-8-13 Ethan liu adds test for test value_change func

        """

        tdLog.info(f"========== start func bit change test")
        tdSql.execute(f"drop database if exists test_func_bit_change")
        tdSql.execute(f"create database test_func_bit_change")
        tdSql.execute(f"use test_func_bit_change")

        # create super table and sub table
        tdSql.execute(f"create table super_t (ts timestamp, col_int int, col_float float, col_double double, col_bool bool, col_var varchar(50), col_nvar nchar(50)) tags (t1 VARCHAR(10))")
        tdSql.execute(f"create table sub_t1 using super_t tags('t1')")
        tdSql.execute(f"create table sub_t2 using super_t tags('t2')")

        # test all normal data for sub table
        for i in range(0,1000, 1):
            intVal = 10 + i
            doubleVal = 12.78 + i*10
            tdSql.execute(f'insert into sub_t1 values(now+{i}s,{intVal},0.10,{doubleVal}, false, "tb1_varchar{i}", "tb1_nchar{i}")')


        tdLog.info(f"end func bit change test successfully")