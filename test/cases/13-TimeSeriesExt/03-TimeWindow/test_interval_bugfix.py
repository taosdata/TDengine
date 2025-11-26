from numpy import select
import taos
import socket
from new_test_framework.utils import tdLog, tdSql, TDSql, tdDnodes


class TestIntervalBugFix:
    updatecfgDict = {"timezone": "UTC"}

    def setup_class(cls):
        host = socket.gethostname()
        con = taos.connect(
            host=f"{host}", config=tdDnodes.getSimCfgPath(), timezone="UTC"
        )
        tdLog.debug("start to execute %s" % __file__)
        cls.testSql = TDSql()
        cls.testSql.init(con.cursor())

    def ts_5400_prepare_data(self):
        tdLog.info("prepare data for TS-5400")
        self.testSql.execute("create database db_ts5400 BUFFER 512 CACHESIZE 1024 CACHEMODEL 'both' COMP 2 DURATION 10d WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 KEEP 365000d;")
        self.testSql.execute("use db_ts5400;")
        #tdSql.execute("create stable st(ts TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `uk` VARCHAR(64) ENCODE 'disabled' COMPRESS 'lz4' LEVEL 'medium' PRIMARY KEY ) tags(ta int,tb int,tc int);")
        self.testSql.execute("create stable st(ts TIMESTAMP, `uk` VARCHAR(64)) tags(ta int,tb int,tc int);")
        self.testSql.execute("create table t1 using st tags(1,1,1);")
        self.testSql.execute("insert into t1 values ('1970-01-29 05:04:53.000','22:: ');")

    def test_interval_bugfix(self):
        """Interval: Bug TS-BUG-FIX
        
        test interval query bug fixes

        Catalog:
            - Timeseries:TimeWindow

        Since: v3.3.0.0

        Labels: common,ci

        History:
            - 2025-11-20 xs Ren Created
        """
        
        self.ts_5400_test()
        self.ts_7676_test_dup_ts()
        self.ts_7676_test_uni_ts()

    def ts_5400_test(self):
        """Interval: Bug TS-5400

        test interval query when ts = 0 error fix

        Catalog:
            - Timeseries:TimeWindow

        Since: v3.3.0.0

        Labels: common,ci

        Jira: TS-5400

        History:
            - 2024-9-14 Feng Chao Created
            - 2025-5-08 Huo Hong Migrated from cases/uncatalog/army/query/accuracy/test_ts5400.py
        """
        
        self.ts_5400_prepare_data()
        self.testSql.execute("use db_ts5400;")
        self.testSql.query("select to_char(_wstart, 'YYYY-MM-DD HH24:MI:SS.MS'), count(*) from st interval(1y);")
        self.testSql.checkRows(1)
        self.testSql.checkData(0, 0, "1970-01-01 00:00:00.000")
        self.testSql.checkData(0, 1, 1)
        tdLog.info("TS-5400 test passed")
        
    def ts_7676_test_dup_ts(self):
        tdLog.info("prepare data for TS-7676")
        tdSql.execute("create database db_ts7676 BUFFER 512 CACHESIZE 1024 CACHEMODEL 'both' COMP 2 DURATION 10d WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 KEEP 365000d;")
        tdSql.execute("use db_ts7676;")
        tdSql.execute("create stable st(ts TIMESTAMP, event_time TIMESTAMP, `status` int) tags(t1 int);")
        tdSql.execute("create table t1 using st tags(1);")
        tdSql.execute("create table t2 using st tags(2);")
        tdSql.execute("create table t3 using st tags(1);")
        tdSql.execute("create table t4 using st tags(2);")
        tdSql.execute("create table t5 using st tags(1);")
        
        for i in range(5):
           tdSql.execute(f"insert into t{i+1} values (1763617916000, 1763617916000, {i+1});")
           tdSql.execute(f"insert into t{i+1} values (1763617917000, 1763617917000, {i+1});")
           tdSql.execute(f"insert into t{i+1} values (1763617918000, 1763617918000, {i+2});")
           tdSql.execute(f"insert into t{i+1} values (1763617919000, 1763617915000, {i+2});")
           tdSql.execute(f"insert into t{i+1} values (1763617920000, 1763617919000, {i+2});")
           tdSql.execute(f"insert into t{i+1} values (1763617921000, 1763617912000, {i+2});")
           tdSql.execute(f"insert into t{i+1} values (1763617922000, 1763617920000, {i+3});")
           tdSql.execute(f"insert into t{i+1} values (1763617923000, 1763617921000, {i+4});")
        
        # interval window: subquery is interval window with order by
        sql = f"select _wstart, sum(`status`) from (select _wstart, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname interval(2s) order by 1) interval(3s);"
        tdSql.query(sql)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1763617914000)
        tdSql.checkData(0, 1, 15)
        tdSql.checkData(1, 0, 1763617917000)
        tdSql.checkData(1, 1, 20)
        tdSql.checkData(2, 0, 1763617920000)
        tdSql.checkData(2, 1, 45)
        
        # interval window: subquery is interval window with order by
        sql = f"select _wstart, sum(`status`) from (select _wstart, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname interval(2s) order by 1 desc) interval(3s);"
        tdSql.query(sql)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1763617914000)
        tdSql.checkData(0, 1, 15)
        tdSql.checkData(1, 0, 1763617917000)
        tdSql.checkData(1, 1, 20)
        tdSql.checkData(2, 0, 1763617920000)
        tdSql.checkData(2, 1, 45)
        # interval window: subquery is interval window without order by
        sql = f"select _wstart, sum(`status`) from (select _wstart, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname interval(2s)) interval(3s);"
        tdSql.query(sql)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1763617914000)
        tdSql.checkData(0, 1, 15)
        tdSql.checkData(1, 0, 1763617917000)
        tdSql.checkData(1, 1, 20)
        tdSql.checkData(2, 0, 1763617920000)
        tdSql.checkData(2, 1, 45)
        
        # interval window: subquery is interval window with order by desc on non-pk column
        # sql = f"select _wstart, sum(`status`) from (select _wstart, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname interval(2s) order by 2 desc) interval(3s);"
        # tdSql.error(sql)
        
        sql = f"select _wstart, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname interval(2s) order by 2 desc;"
        tdSql.query(sql)
        tdSql.checkRows(20)
        tdSql.checkData(0, 1, 1763617920000)
        tdSql.checkData(19, 1, 1763617916000)

        # interval window: subquery is union all window with order by desc
        sql = f"select  _wstart, first(`event_time`) from (select _wstart, first(`event_time`), `event_time`, `status`,  tbname from st partition by tbname interval(2s) union all (select _wstart, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname interval(2s)) order by _wstart desc) interval(3s)"
        tdSql.query(sql)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1763617914000)
        tdSql.checkData(1, 0, 1763617917000)
        tdSql.checkData(2, 0, 1763617920000)
 
        # interval window: subquery is union all window with order by asc
        sql = f"select  _wstart, first(`event_time`) from (select _wstart, first(`event_time`), `event_time`, `status`,  tbname from st partition by tbname interval(2s) union all (select _wstart, first(`event_time`) as t2, `event_time`, `status`, tbname from st partition by tbname interval(2s)) order by _wstart asc) interval(3s)"
        tdSql.query(sql)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1763617914000)
        tdSql.checkData(1, 0, 1763617917000)
        tdSql.checkData(2, 0, 1763617920000)
        
        # interval window: subquery is union all window with order by non-pk column
        sql = f"select  _wstart, first(`event_time`) from (select _wstart, first(`event_time`) as t2, `event_time`, `status`,  tbname from st partition by tbname interval(2s) union all (select _wstart, first(`event_time`) as t2, `event_time`, `status`, tbname from st partition by tbname interval(2s)) order by t2 asc) interval(3s)"
        tdSql.error(sql)

        # union all window without order by
        sql = f"select  _wstart, first(`event_time`) from (select _wstart, first(`event_time`), `event_time`, `status`,  tbname from st partition by tbname interval(2s) union all (select _wstart, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname interval(2s))) interval(3s)"
        tdSql.error(sql)
        
        # interval window: subquery is state window with order by asc
        sql = f"select _wstart, sum(`status`) from (select _wstart, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname state_window(`status`) order by 1) interval(3s);"
        tdSql.query(sql)
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1763617914000)
        tdSql.checkData(0, 1, 15)
        tdSql.checkData(1, 0, 1763617917000)
        tdSql.checkData(1, 1, 20)
        tdSql.checkData(2, 0, 1763617920000)
        tdSql.checkData(2, 1, 25)
        tdSql.checkData(3, 0, 1763617923000)
        tdSql.checkData(3, 1, 30)
        
        # interval window: subquery is state window with order by desc
        sql = f"select _wstart, sum(`status`) from (select _wstart, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname state_window(`status`) order by 1 desc) interval(3s);"
        tdSql.query(sql)
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1763617914000)
        tdSql.checkData(0, 1, 15)
        tdSql.checkData(1, 0, 1763617917000)
        tdSql.checkData(1, 1, 20)
        tdSql.checkData(2, 0, 1763617920000)
        tdSql.checkData(2, 1, 25)
        tdSql.checkData(3, 0, 1763617923000)
        tdSql.checkData(3, 1, 30)
        
        # interval window: subquery is state window with order by non-pk column
        # sql = f"select _wstart, sum(`status`) from (select _wstart, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname state_window(`status`) order by 2 asc) interval(3s);"
        # tdSql.error(sql)

        # interval window: subquery is state window without order by
        sql = f"select _wstart, sum(`status`) from (select _wstart, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname state_window(`status`)) interval(3s);"
        tdSql.query(sql)
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1763617914000)
        tdSql.checkData(0, 1, 15)
        tdSql.checkData(1, 0, 1763617917000)
        tdSql.checkData(1, 1, 20)
        tdSql.checkData(2, 0, 1763617920000)
        tdSql.checkData(2, 1, 25)
        tdSql.checkData(3, 0, 1763617923000)
        tdSql.checkData(3, 1, 30)
        
        # state window: subquery is state window with duplicate timestamp
        sql = f"select _wstart, sum(`status`) from (select _wstart, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname interval(2s) order by 1 asc) state_window(status);"
        tdSql.error(sql)
        
        # session window: subquery is state window with order by asc
        sql = f"select _wstart, sum(`status`) from (select _wstart as t2, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname state_window(`status`) order by 1) session(t2, 500a);"
        tdSql.query(sql)
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1763617916000)
        tdSql.checkData(0, 1, 15)
        tdSql.checkData(1, 0, 1763617918000)
        tdSql.checkData(1, 1, 20)
        tdSql.checkData(2, 0, 1763617922000)
        tdSql.checkData(2, 1, 25)
        tdSql.checkData(3, 0, 1763617923000)
        tdSql.checkData(3, 1, 30)
        
        # session window: subquery is state window with order by desc
        sql = f"select _wstart, sum(`status`) from (select _wstart as t2, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname state_window(`status`) order by 1 desc) session(t2, 500a);"
        tdSql.query(sql)
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1763617916000)
        tdSql.checkData(0, 1, 15)
        tdSql.checkData(1, 0, 1763617918000)
        tdSql.checkData(1, 1, 20)
        tdSql.checkData(2, 0, 1763617922000)
        tdSql.checkData(2, 1, 25)
        tdSql.checkData(3, 0, 1763617923000)
        tdSql.checkData(3, 1, 30)
        
        # session window: subquery is state window with order by non-pk column
        sql = f"select _wstart, sum(`status`) from (select _wstart as t2, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname state_window(`status`) order by 2 desc) session(t2, 500a);"
        tdSql.error(sql)
        
        # session window: subquery is state window without order by
        sql = f"select _wstart, sum(`status`) from (select _wstart as t2, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname state_window(`status`)) session(t2, 500a);"
        tdSql.error(sql)
        
        # count window: subquery is state window with duplicate timestamp
        sql = f"select _wstart, sum(`status`) from (select _wstart as t2, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname state_window(`status`) order by 1) count_window(5);"
        tdSql.error(sql)
        
        # event window: subquery is state window with duplicate timestamp
        sql = f"select _wstart, sum(`status`) from (select _wstart as t2, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname state_window(`status`) order by 1) event_window start with status > 1 end with status > 4;"
        tdSql.error(sql)
        
        sql = f"select _wstart, sum(`status`) from (select _wstart, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname interval(2s) order by 1) partition by tbname interval(3s);"
 
    def ts_7676_test_uni_ts(self):
        tdLog.info("prepare data for TS-7676 test2")
        tdSql.execute("create database db_ts7676_2 BUFFER 512 CACHESIZE 1024 CACHEMODEL 'both' COMP 2 DURATION 10d WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 KEEP 365000d;")
        tdSql.execute("use db_ts7676_2;")
        tdSql.execute("create stable st(ts TIMESTAMP, event_time TIMESTAMP, `status` int) tags(t1 int);")
        tdSql.execute("create table t1 using st tags(1);")
        tdSql.execute("create table t2 using st tags(2);")
        tdSql.execute("create table t3 using st tags(1);")
        tdSql.execute("create table t4 using st tags(2);")
        tdSql.execute("create table t5 using st tags(1);")
        
        for i in range(5):
           tdSql.execute(f"insert into t{i+1} values ({1763617916000 + i}, 1763617916000, 1);")
           tdSql.execute(f"insert into t{i+1} values ({1763617917000 + i}, 1763617917000, 1);")
           tdSql.execute(f"insert into t{i+1} values ({1763617918000 + i}, 1763617918000, 2);")
           tdSql.execute(f"insert into t{i+1} values ({1763617919000 + i}, 1763617915000, 2);")
           tdSql.execute(f"insert into t{i+1} values ({1763617920000 + i}, 1763617919000, 2);")
           tdSql.execute(f"insert into t{i+1} values ({1763617921000 + i}, 1763617912000, 2);")
           tdSql.execute(f"insert into t{i+1} values ({1763617922000 + i}, 1763617920000, 3);")
           tdSql.execute(f"insert into t{i+1} values ({1763617923000 + i}, 1763617921000, {i+1});")
        
        # state window: subquery is state window with order by default
        sql = f"select _wstart, sum(`status`) from (select ts, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname interval(2s) order by 1 asc) state_window(status);"
        tdSql.query(sql)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1763617916000)
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(1, 0, 1763617918000)
        tdSql.checkData(1, 1, 20)
        tdSql.checkData(2, 0, 1763617922000)
        tdSql.checkData(2, 1, 15)
        
        # state window: subquery is state window with order by desc
        sql = f"select _wstart, sum(`status`) from (select ts, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname interval(2s) order by 1 desc) state_window(status);"
        tdSql.query(sql)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1763617916000)
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(1, 0, 1763617918000)
        tdSql.checkData(1, 1, 20)
        tdSql.checkData(2, 0, 1763617922000)
        tdSql.checkData(2, 1, 15)
        
        # state window: subquery is state window without order
        sql = f"select _wstart, sum(`status`) from (select ts, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname interval(2s)) state_window(status);"
        tdSql.error(sql)
        
        # state window: subquery is state window with order by non-pk column
        sql = f"select _wstart, sum(`status`) from (select ts, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname interval(2s) order by 2 desc) state_window(status);"
        tdSql.error(sql)
        
        sql = f"select _wstart, first(`status`), ts from (select ts, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname interval(2s) order by 1 desc) state_window(status);"
        tdSql.query(sql)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1763617916000)
        tdSql.checkData(0, 1, 1)
        tdSql.checkData(0, 2, 1763617916000)
        tdSql.checkData(1, 0, 1763617918000)
        tdSql.checkData(1, 1, 2)
        tdSql.checkData(1, 2, 1763617918000)
        tdSql.checkData(2, 0, 1763617922000)
        tdSql.checkData(2, 1, 3)
        tdSql.checkData(2, 2, 1763617922000)
        
        sql = f"select _wstart, first(ts), last(ts) from (select ts, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname interval(2s) order by 1 asc) state_window(status);"
        tdSql.query(sql)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1763617916000)
        tdSql.checkData(0, 1, 1763617916000)
        tdSql.checkData(0, 2, 1763617916004)
        tdSql.checkData(1, 0, 1763617918000)
        tdSql.checkData(1, 1, 1763617918000)
        tdSql.checkData(1, 2, 1763617920004)
        tdSql.checkData(2, 0, 1763617922000)
        tdSql.checkData(2, 1, 1763617922000)
        tdSql.checkData(2, 2, 1763617922004)
        
        # count window: subquery is state window with order by asc
        sql = f"select _wstart, sum(`status`) from (select _wstart as t2, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname state_window(`status`) order by 1) count_window(5);"
        tdSql.query(sql)
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1763617916000)
        tdSql.checkData(0, 1, 5)
        tdSql.checkData(1, 0, 1763617918000)
        tdSql.checkData(1, 1, 10)
        tdSql.checkData(2, 0, 1763617922000)
        tdSql.checkData(2, 1, 15)
        tdSql.checkData(3, 0, 1763617923000)
        tdSql.checkData(3, 1, 12)
        
        # count window: subquery is state window with order by desc
        sql = f"select _wstart, sum(`status`) from (select _wstart as t2, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname state_window(`status`) order by 1 desc) count_window(5);"
        tdSql.query(sql)
        tdSql.checkRows(4)
        tdSql.checkData(0, 0, 1763617923004)
        tdSql.checkData(0, 1, 15)
        tdSql.checkData(1, 0, 1763617922003)
        tdSql.checkData(1, 1, 14)
        tdSql.checkData(2, 0, 1763617918003)
        tdSql.checkData(2, 1, 9)
        tdSql.checkData(3, 0, 1763617916003)
        tdSql.checkData(3, 1, 4)
        
        # count window: subquery is state window with order by non-pk column
        sql = f"select _wstart, sum(`status`) from (select _wstart as t2, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname state_window(`status`) order by 2 desc) count_window(5);"
        tdSql.error(sql)
        
        # count window: subquery is state window without order by
        sql = f"select _wstart, sum(`status`) from (select _wstart as t2, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname state_window(`status`)) count_window(5);"
        tdSql.error(sql)
        
        # event window: subquery is state window with order by asc
        sql = f"select _wstart, sum(`status`) from (select _wstart as t2, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname state_window(`status`) order by 1) event_window start with status > 1 end with status > 4;"
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1763617918000)
        tdSql.checkData(0, 1, 37)
        
        # event window: subquery is state window with order by non-pk column
        sql = f"select _wstart, sum(`status`) from (select _wstart as t2, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname state_window(`status`) order by 2) event_window start with status > 1 end with status > 4;"
        tdSql.error(sql)
        
        # event window: subquery is state window without order by
        sql = f"select _wstart, sum(`status`) from (select _wstart as t2, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname state_window(`status`)) event_window start with status > 1 end with status > 4;"
        tdSql.error(sql)
        
        # event window: subquery is state window with order by asc
        sql = f"select _wstart, sum(`status`), first(t2), last(t2), count(*) from (select _wstart as t2, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname state_window(`status`) order by 1) event_window start with status%3 == 1 end with status%3 == 0;"
        tdSql.query(sql)
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, 1763617916000)
        tdSql.checkData(0, 1, 18)
        tdSql.checkData(0, 2, 1763617916000)
        tdSql.checkData(0, 3, 1763617922000)
        tdSql.checkData(0, 4, 11)
        
        # event window: subquery is state window with order by desc
        sql = f"select _wstart, sum(`status`), first(t2), last(t2), count(*) from (select _wstart as t2, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname state_window(`status`) order by 1 desc) event_window start with status%3 == 1 end with status%3 == 0;"
        tdSql.query(sql)
        # tdSql.checkRows(1)
        # tdSql.checkData(0, 0, 1763617916000)
        # tdSql.checkData(0, 1, 18)
        # tdSql.checkData(0, 2, 1763617916000)
        # tdSql.checkData(0, 3, 1763617922000)
        # tdSql.checkData(0, 4, 11)
        
        # event window: subquery is state window with order by non-pk column
        sql = f"select _wstart, sum(`status`), first(t2), last(t2), count(*) from (select _wstart as t2, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname state_window(`status`) order by 2 desc) event_window start with status%3 == 1 end with status%3 == 0;"
        tdSql.error(sql)
        
        # event window: subquery is state window without order by
        sql = f"select _wstart, sum(`status`), first(t2), last(t2), count(*) from (select _wstart as t2, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname state_window(`status`)) event_window start with status%3 == 1 end with status%3 == 0;"
        tdSql.error(sql)
                