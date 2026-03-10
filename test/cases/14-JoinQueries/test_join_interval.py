from new_test_framework.utils import tdLog, tdSql, sc, clusterComCheck, etool
import time

class TestJoinInterval:

    def prepare_data(self):
        tdSql.execute("drop database if exists test;")
        
        etool.benchMark(command ="-t 2 -n 1000000 -b int,float,nchar -y")
        
        while True:
            tdSql.query("select ts from test.d0;")
            num1 = tdSql.queryRows
            tdSql.query("select ts from test.d1;")
            num2 = tdSql.queryRows
            if num1 == 1000000 and num2 == 1000000:
                break
            tdLog.info(f"waiting for data ready, d0: {num1}, d1: {num2}")
            time.sleep(1)

    def ts5803(self):           
        tdSql.query("select d0.ts,d0.c1,d0.c2 from test.d0 join test.d1 on d0.ts=d1.ts;")
        num1 = tdSql.queryRows
        
        tdSql.query("select d0.ts,d0.c1,d0.c2 from test.d0 join test.d1 on d0.ts=d1.ts limit 1000000;")
        tdSql.checkRows(num1)
        
        tdSql.query("select d0.ts from test.d0 join test.d1 on d0.ts=d1.ts limit 1000000;")
        tdSql.checkRows(num1)
        
        tdSql.query("select d0.ts,d0.c1,d0.c2 from test.d0 left join test.d1 on d0.ts=d1.ts;")
        num1 = tdSql.queryRows
        
        tdSql.query("select d0.ts,d0.c1,d0.c2 from test.d0 left join test.d1 on d0.ts=d1.ts limit 1000000;")
        tdSql.checkRows(num1)
        
        tdSql.query("select d0.ts from test.d0 left join test.d1 on d0.ts=d1.ts limit 1000000;")
        tdSql.checkRows(num1)

    def test_join_interval(self):
        """Join interval

        1.Create database d1 and d2
        2.Create table t1 in d1 with tags and t1 in d2 without tags
        3.Insert data into d1.t1 with different tag values
        4.Insert data into d2.t1 with same timestamps as d1.t1
        5.Join query between d1.t1 and d2.t1 with interval(1a)
        6. Check the result of join correctly
        7. Jira TS-5803

        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-5-7 Simon Guan migrated from tsim/query/join_interval.sim
            - 2025-12-19 Alex Duan Migrated from uncatalog/system-test/2-query/test_large_data.py

        """

        tdLog.info(f"======== step create databases")
        tdSql.execute(f"create database d1")
        tdSql.execute(f"create database d2")
        tdSql.execute(f"create table d1.t1(ts timestamp, i int) tags(t int);")
        tdSql.execute(f"create table d2.t1(ts timestamp, i int);")
        tdSql.execute(
            f"insert into d1.t11 using d1.t1 tags(1) values(1500000000000, 0)(1500000000001, 1)(1500000000002,2)(1500000000003,3)(1500000000004,4)"
        )
        tdSql.execute(
            f"insert into d1.t12 using d1.t1 tags(2) values(1500000000000, 0)(1500000000001, 1)(1500000000002,2)(1500000000003,3)(1500000000004,4)"
        )
        tdSql.execute(
            f"insert into d1.t13 using d1.t1 tags(3) values(1500000000000, 0)(1500000000001, 1)(1500000000002,2)(1500000000003,3)(1500000000004,4)"
        )
        tdSql.execute(
            f"insert into d2.t1 values(1500000000000,0)(1500000000001,1)(1500000000002,2)"
        )

        tdSql.query(
            f"select _wstart,_wend,count((a.ts)),count(b.ts) from d1.t1 a, d2.t1 b where a.ts is not null and a.ts = b.ts  interval(1a) ;"
        )
        tdSql.checkData(0, 2, 3)

        tdSql.checkData(0, 3, 3)

        tdSql.checkData(1, 2, 3)

        tdSql.checkData(1, 3, 3)

        tdSql.checkData(2, 2, 3)

        tdSql.checkData(2, 3, 3)
        
        # ts 5803
        self.prepare_data()
        self.ts5803()        
