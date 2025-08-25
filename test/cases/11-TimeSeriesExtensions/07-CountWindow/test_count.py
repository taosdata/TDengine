from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck


class TestCount:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_count(self):
        """Count Window

        1. -

        Catalog:
            - Timeseries:EventWindow

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-8 Simon Guan Migrated from tsim/query/query_count0.sim

        """

        tdLog.info(f"step1: normatable")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test vgroups 1;")
        tdSql.execute(f"use test;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")

        tdSql.execute(f"insert into t1 values(1648791213000,0,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001,9,2,2,1.1);")
        tdSql.execute(f"insert into t1 values(1648791213009,0,3,3,1.0);")

        tdSql.execute(f"insert into t1 values(1648791223000,0,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001,9,2,2,1.1);")
        tdSql.execute(f"insert into t1 values(1648791223009,0,3,3,1.0);")

        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(3);"
        )
        tdSql.checkData(0, 1, 3)

        tdSql.checkData(0, 2, 6)

        tdSql.checkData(0, 3, 3)

        # row 1
        tdSql.checkData(1, 1, 3)

        tdSql.checkData(1, 2, 6)

        tdSql.checkData(1, 3, 3)

        tdSql.execute(f"insert into t1 values(1648791213010,NULL,3,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213011,0,NULL,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223005,NULL,NULL,3,1.0);")
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(3, 3, a);"
        )
        
        tdSql.checkData(0, 1, 3)

        tdSql.checkData(0, 2, 6)

        tdSql.checkData(0, 3, 3)

        # row 1
        tdSql.checkData(1, 1, 3)

        tdSql.checkData(1, 2, 3)

        tdSql.checkData(1, 3, 3)
        
        # row 2
        tdSql.checkData(2, 1, 1)

        tdSql.checkData(2, 2, 3)

        tdSql.checkData(2, 3, 3)
        
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(3, 3, b);"
        )
        
        tdSql.checkData(0, 1, 3)

        tdSql.checkData(0, 2, 6)

        tdSql.checkData(0, 3, 3)

        # row 1
        tdSql.checkData(1, 1, 3)

        tdSql.checkData(1, 2, 6)

        tdSql.checkData(1, 3, 3)
        
        # row 2
        tdSql.checkData(2, 1, 1)

        tdSql.checkData(2, 2, 3)

        tdSql.checkData(2, 3, 3)
    
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from t1 count_window(3, 3, a, b);"
        )
        
        tdSql.checkData(0, 1, 3)

        tdSql.checkData(0, 2, 6)

        tdSql.checkData(0, 3, 3)

        # row 1
        tdSql.checkData(1, 1, 3)

        tdSql.checkData(1, 2, 4)

        tdSql.checkData(1, 3, 3)
        
        # row 2
        tdSql.checkData(2, 1, 2)

        tdSql.checkData(2, 2, 5)

        tdSql.checkData(2, 3, 3)

        tdLog.info(f"step2: subper table")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test2 vgroups 4;")
        tdSql.execute(f"use test2;")

        tdSql.execute(
            f"create stable st(ts timestamp, a int, b int , c int, d double) tags(ta int,tb int,tc int);"
        )
        tdSql.execute(f"create table t1 using st tags(1,1,1);")
        tdSql.execute(f"create table t2 using st tags(2,2,2);")

        tdSql.execute(f"insert into t1 values(1648791213000,0,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001,9,2,2,1.1);")
        tdSql.execute(f"insert into t1 values(1648791213009,0,3,3,1.0);")

        tdSql.execute(f"insert into t2 values(1648791213000,0,1,1,1.0);")
        tdSql.execute(f"insert into t2 values(1648791213001,9,2,2,1.1);")
        tdSql.execute(f"insert into t2 values(1648791213009,0,3,3,1.0);")

        tdSql.execute(f"insert into t1 values(1648791223000,0,1,1,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223001,9,2,2,1.1);")
        tdSql.execute(f"insert into t1 values(1648791223009,0,3,3,1.0);")

        tdSql.execute(f"insert into t2 values(1648791223000,0,1,1,1.0);")
        tdSql.execute(f"insert into t2 values(1648791223001,9,2,2,1.1);")
        tdSql.execute(f"insert into t2 values(1648791223009,0,3,3,1.0);")

        tdLog.info(
            f"1 sql select  _wstart as s, count(*) c1,  sum(b), max(c) from st partition by tbname count_window(3);"
        )
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from st partition by tbname count_window(3);"
        )

        tdLog.info(
            f"{tdSql.getData(0,0)} {tdSql.getData(0,1)} {tdSql.getData(0,2)} {tdSql.getData(0,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(1,0)} {tdSql.getData(1,1)} {tdSql.getData(1,2)} {tdSql.getData(1,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(2,0)} {tdSql.getData(2,1)} {tdSql.getData(2,2)} {tdSql.getData(2,3)}"
        )
        tdLog.info(
            f"{tdSql.getData(3,0)} {tdSql.getData(3,1)} {tdSql.getData(3,2)} {tdSql.getData(3,3)}"
        )

        tdSql.checkData(0, 1, 3)

        tdSql.checkData(0, 2, 6)

        tdSql.checkData(0, 3, 3)

        # row 1
        tdSql.checkData(1, 1, 3)

        tdSql.checkData(1, 2, 6)

        tdSql.checkData(1, 3, 3)

        # row 2
        tdSql.checkData(2, 1, 3)

        tdSql.checkData(2, 2, 6)

        tdSql.checkData(2, 3, 3)

        # row 3
        tdSql.checkData(3, 1, 3)

        tdSql.checkData(3, 2, 6)

        tdSql.checkData(3, 3, 3)
        
        tdSql.execute(f"insert into t1 values(1648791213005,NULL,2,2,1.1);")
        tdSql.execute(f"insert into t1 values(1648791213008,0,NULL,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791223011,NULL,NULL,1,1.0);")
        
        tdSql.execute(f"insert into t2 values(1648791213005,NULL,NULL,2,1.1);")
        tdSql.execute(f"insert into t2 values(1648791213008,NULL,7,3,1.0);")
        tdSql.execute(f"insert into t2 values(1648791223011,NULL,5,1,1.0);")

        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from st partition by tbname count_window(3, 3, a) order by tbname, s;"
        )
        
        tdSql.checkData(0, 1, 3)

        tdSql.checkData(0, 2, 3)

        tdSql.checkData(0, 3, 3)

        # row 1
        tdSql.checkData(1, 1, 3)

        tdSql.checkData(1, 2, 6)

        tdSql.checkData(1, 3, 3)

        # row 2
        tdSql.checkData(2, 1, 1)

        tdSql.checkData(2, 2, 3)

        tdSql.checkData(2, 3, 3)

        # row 3
        tdSql.checkData(3, 1, 3)

        tdSql.checkData(3, 2, 6)

        tdSql.checkData(3, 3, 3)
        
        # row 4
        tdSql.checkData(4, 1, 3)

        tdSql.checkData(4, 2, 6)

        tdSql.checkData(4, 3, 3)
        
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from st partition by tbname count_window(3, 3, b) order by tbname, s;"
        )
        
        tdSql.checkData(0, 1, 3)

        tdSql.checkData(0, 2, 5)

        tdSql.checkData(0, 3, 2)

        # row 1
        tdSql.checkData(1, 1, 3)

        tdSql.checkData(1, 2, 6)

        tdSql.checkData(1, 3, 3)

        # row 2
        tdSql.checkData(2, 1, 1)

        tdSql.checkData(2, 2, 3)

        tdSql.checkData(2, 3, 3)

        # row 3
        tdSql.checkData(3, 1, 3)

        tdSql.checkData(3, 2, 10)

        tdSql.checkData(3, 3, 3)
        
        # row 4
        tdSql.checkData(4, 1, 3)

        tdSql.checkData(4, 2, 6)

        tdSql.checkData(4, 3, 3)

        # row 5
        tdSql.checkData(5, 1, 2)

        tdSql.checkData(5, 2, 8)

        tdSql.checkData(5, 3, 3) 
        
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from st partition by tbname count_window(3, 3, a, b) order by tbname, s;"
        )
        
        tdSql.checkData(0, 1, 3)

        tdSql.checkData(0, 2, 5)

        tdSql.checkData(0, 3, 2)

        # row 1
        tdSql.checkData(1, 1, 3)

        tdSql.checkData(1, 2, 4)

        tdSql.checkData(1, 3, 3)

        # row 2
        tdSql.checkData(2, 1, 2)

        tdSql.checkData(2, 2, 5)

        tdSql.checkData(2, 3, 3)

        # row 3
        tdSql.checkData(3, 1, 3)

        tdSql.checkData(3, 2, 10)

        tdSql.checkData(3, 3, 3)
        
        # row 4
        tdSql.checkData(4, 1, 3)

        tdSql.checkData(4, 2, 6)

        tdSql.checkData(4, 3, 3)

        # row 5
        tdSql.checkData(5, 1, 2)

        tdSql.checkData(5, 2, 8)

        tdSql.checkData(5, 3, 3) 
        
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from st partition by tbname count_window(3, 1, b) order by tbname, s;"
        )
        
        tdSql.checkData(0, 1, 3)

        tdSql.checkData(0, 2, 5)

        tdSql.checkData(0, 3, 2)

        # row 1
        tdSql.checkData(1, 1, 3)

        tdSql.checkData(1, 2, 7)

        tdSql.checkData(1, 3, 3)

        # row 2
        tdSql.checkData(2, 1, 3)

        tdSql.checkData(2, 2, 6)

        tdSql.checkData(2, 3, 3)

        # row 3
        tdSql.checkData(3, 1, 3)

        tdSql.checkData(3, 2, 6)

        tdSql.checkData(3, 3, 3)
        
        # row 4
        tdSql.checkData(4, 1, 3)

        tdSql.checkData(4, 2, 6)

        tdSql.checkData(4, 3, 3)

        # row 5
        tdSql.checkData(5, 1, 2)

        tdSql.checkData(5, 2, 5)

        tdSql.checkData(5, 3, 3) 
        
        # row 6
        tdSql.checkData(6, 1, 1)

        tdSql.checkData(6, 2, 3)

        tdSql.checkData(6, 3, 3) 
        
        tdSql.query(
            f"select  _wstart as s, count(*) c1,  sum(b), max(c) from st partition by tbname count_window(3, 1, a, b) order by tbname, s;"
        )
        
        tdSql.checkData(0, 1, 3)

        tdSql.checkData(0, 2, 5)

        tdSql.checkData(0, 3, 2)

        # row 1
        tdSql.checkData(1, 1, 3)

        tdSql.checkData(1, 2, 4)

        tdSql.checkData(1, 3, 3)

        # row 2
        tdSql.checkData(2, 1, 3)

        tdSql.checkData(2, 2, 5)

        tdSql.checkData(2, 3, 3)

        # row 3
        tdSql.checkData(3, 1, 3)

        tdSql.checkData(3, 2, 4)

        tdSql.checkData(3, 3, 3)
        
        # row 4
        tdSql.checkData(4, 1, 3)

        tdSql.checkData(4, 2, 6)

        tdSql.checkData(4, 3, 3)

        # row 5
        tdSql.checkData(4, 1, 3)

        tdSql.checkData(4, 2, 6)

        tdSql.checkData(4, 3, 3) 

        tdLog.info(f"step3: subper table with having")
        tdLog.info(f"=============== create database")
        tdSql.execute(f"create database test3 vgroups 1;")
        tdSql.execute(f"use test3;")

        tdSql.execute(f"create table t1(ts timestamp, a int, b int , c int, d double);")
        tdSql.execute(f"insert into t1 values(1648791213000,0,1,3,1.0);")
        tdSql.execute(f"insert into t1 values(1648791213001,2,2,3,1.1);")
        tdSql.execute(f"insert into t1 values(1648791223002,0,3,3,2.1);")
        tdSql.execute(f"insert into t1 values(1648791223003,1,4,3,3.1);")
        tdSql.execute(f"insert into t1 values(1648791223004,1,5,3,4.1);")
        tdSql.execute(f"insert into t1 values(1648791223005,2,6,3,4.1);")

        tdLog.info(
            f"1 sql select _wstart, count(*),max(b) from t1 count_window(3) having max(b) > 3;"
        )
        tdSql.query(
            f"select _wstart, count(*),max(b) from t1 count_window(3) having max(b) > 3;"
        )

        # row 0
        tdSql.checkRows(1)

        tdLog.info(
            f"1 sql select _wstart, count(*),max(b) from t1 count_window(3) having max(b) > 6;"
        )
        tdSql.query(
            f"select _wstart, count(*),max(b) from t1 count_window(3) having max(b) > 6;"
        )

        # row 0
        tdSql.checkRows(0)

        tdLog.info(f"query_count0 end")
