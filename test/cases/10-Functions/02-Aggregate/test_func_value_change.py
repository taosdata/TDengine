from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck

class TestValueChangeFunc:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_func_value_change(self):
        """Value Change Func Test

        1.Create db
        2.Create supper table and sub table
        5.Insert data into and select use value_change func and check the result

        Catalog:
            - Select

        Since: v3.0.0.0

        Labels: common,ci

        Jira: TD-6485

        History:
            - 2025-8-13 Ethan liu adds test for test value_change func

        """

        tdLog.info(f"========== start func value change test")
        tdSql.execute(f"drop database if exists test_func_value_change")
        tdSql.execute(f"create database test_func_value_change")
        tdSql.execute(f"use test_func_value_change")

        # create super table and sub table
        tdSql.execute(f"create table super_t (ts timestamp, col_int int, col_float float, col_double double, col_bool bool, col_var varchar(50), col_nvar nchar(50)) tags (t1 VARCHAR(10))")
        tdSql.execute(f"create table sub_t1 using super_t tags('t1')")
        tdSql.execute(f"create table sub_t2 using super_t tags('t2')")

        # test all normal data
        for i in range(0,40, 1):
            intVal = 10 + i
            doubleVal = 12.78 + i*10
            tdSql.execute(f'insert into sub_t1 values(now+{i}s,{intVal},0.10,{doubleVal}, false, "tb1_varchar{i}", "tb1_nchar{i}")')
        
        # tdSql.query(f"select value_change(ts) from sub_t1")
        # tdSql.checkData(0,0,999)

        # tdSql.query(f"select value_change(ts,0) from sub_t1")
        # tdSql.checkData(0,0,999)

        # tdSql.query(f"select value_change(col_int) from sub_t1")
        # tdSql.checkData(0,0,999)

        # tdSql.query(f"select value_change(col_int,0) from sub_t1")
        # tdSql.checkData(0,0,999)

        # tdSql.query(f"select value_change(col_float) from sub_t1")
        # tdSql.checkData(0,0,0)

        # tdSql.query(f"select value_change(col_float,0) from sub_t1")
        # tdSql.checkData(0,0,0)

        # tdSql.query(f"select value_change(col_double) from sub_t1")
        # tdSql.checkData(0,0,999)

        # tdSql.query(f"select value_change(col_double,0) from sub_t1")
        # tdSql.checkData(0,0,999)

        # tdSql.query(f"select value_change(col_bool) from sub_t1")
        # tdSql.checkData(0,0,0)

        # tdSql.query(f"select value_change(col_bool,0) from sub_t1")
        # tdSql.checkData(0,0,0)

        tdSql.query(f"select value_change(col_var) from sub_t1")
        tdSql.checkData(0,0,39)

        tdSql.query(f"select value_change(col_var,0) from sub_t1")
        tdSql.checkData(0,0,39)

        # tdSql.query(f"select value_change(col_nvar) from sub_t1")
        # tdSql.checkData(0,0,999)

        # tdSql.query(f"select value_change(col_nvar,0) from sub_t1")
        # tdSql.checkData(0,0,999)

        # for i in range(1,100, 10):
        #     intVal = 10 + i
        #     doubleVal = 12.78 + i*10
        #     tdSql.execute(f'insert into sub_t1 values(now+{i}s,{intVal},0.10,{doubleVal}, false, "tb1_varchar{i}", "tb1_nchar{i}")')
        #     #tdSql.execute(f'insert into sub_t1 values(now+{i}s,{intVal},null,{doubleVal}, false, "tb1_varchar{i}", "tb1_nchar{i}")')
        #     tdSql.execute(f'insert into sub_t1 values(now+{i}s,10,0.10,12.78, false, "", "tb1_nchar{1}")')
        #     tdSql.execute(f'insert into sub_t1(ts, col_int,col_var) values(now+{i}s,10,"tb1_varchar{i}")')
        #     tdSql.execute(f'insert into sub_t2 values(now+{i}m,{intVal},0.10,{doubleVal}, true, "tb2_varchar{i}", "tb2_nchar{i}")')
        #     #tdSql.execute(f'insert into sub_t2 values(now+{i}m,{intVal},0.10,{doubleVal}, true, null, "tb2_nchar{i}")')
        #     tdSql.execute(f'insert into sub_t2(ts, col_float, col_nvar) values(now+{i}s,10.9,"tb1_nchar{i}")')
        #     tdSql.execute(f'insert into sub_t2(ts , col_int, col_var) values(now+{i}s,10,"tb1_varchar{i}")')

        # tdSql.query(f"select value_change(ts) from sub_t1")
        # tdSql.checkData(0,0,29)

        # tdSql.query(f"select value_change(col_int) from sub_t1")
        # tdSql.checkData(0,0,19)

        # tdSql.query(f"select value_change(col_float) from sub_t1")
        # tdSql.checkData(0,0,19)

        # tdSql.query(f"select value_change(col_float, 0) from sub_t1")
        # tdSql.checkData(0,0,19)

        # tdSql.query(f"select value_change(ts) from sub_t2")
        # tdSql.checkData(0,0,29)

        tdLog.info(f"end func value change test successfully")