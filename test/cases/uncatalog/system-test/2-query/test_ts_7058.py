from new_test_framework.utils import tdLog, tdSql

class TestTs7058:
    def setup_class(cls):
        cls.replicaVar = 1  # 设置默认副本数
        tdLog.debug(f"start to excute {__file__}")

        cls.dbname = 'db'
        cls.stbname = 'st'

    def prepareData(self):
        # db
        tdSql.execute(f"create database db;")
        tdSql.execute(f"use db")

        # create table and insert data

        tdSql.execute("CREATE STABLE st (ts timestamp, a int, b int, c int) TAGS (ta varchar(12))")
        tdSql.execute("insert into t1 using st tags('1') values('2025-03-06 11:17:29.202', 1, 1, 1) ('2025-03-07 11:17:30.361', 2, 2, 2)")
        tdSql.execute("insert into t2 using st tags('2') values('2025-03-06 11:17:29.202', 4, 4, 4) ('2025-03-07 11:17:30.361', 9, 9, 9)")


    def check(self):
        tdSql.query(f"select ts, count(*) from (select _wstart ts, timetruncate(ts, 1d, 1) dt, max(a)/10 from st partition by tbname interval(1s) order by dt) partition by ts order by ts")
        tdSql.checkRows(2)

        tdSql.checkData(0, 0, '2025-03-06 11:17:29')
        tdSql.checkData(1, 0, '2025-03-07 11:17:30')

        tdSql.checkData(0, 1, 2)
        tdSql.checkData(1, 1, 2)


    def test_ts_5761(self):
        """summary: xxx

        description: test case for TS-7058

        Since: 2025-08-22

        Labels: test case for TS-7058

        Jira: https://jira.taosdata.com:18080/projects/TS/issues/TS-7085

        Catalog:
            - query:xxx

        History:
            - 2025-08-22 

        """

        self.prepareData()
        self.check()
        tdLog.success(f"{__file__} successfully executed")

