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

        Catalog:
            - Timeseries:TimeWindow

        Since: v3.3.0.0

        Labels: common,ci

        History:
            - 2025-11-20 xs Ren Created
        """
        self.ts_5400_test()
        self.ts_7676_test()

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
        
    def ts_7676_test(self):
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
        
        # interval window with order by
        sql = f"select _wstart, sum(`status`) from (select _wstart, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname interval(2s) order by 1) interval(3s);"
        tdSql.query(sql)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1763617914000)
        tdSql.checkData(0, 1, 15)
        tdSql.checkData(1, 0, 1763617917000)
        tdSql.checkData(1, 1, 20)
        tdSql.checkData(2, 0, 1763617920000)
        tdSql.checkData(2, 1, 45)
        
        # interval window with order by desc
        sql = f"select _wstart, sum(`status`) from (select _wstart, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname interval(2s) order by 1 desc) interval(3s);"
        tdSql.query(sql)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1763617914000)
        tdSql.checkData(0, 1, 15)
        tdSql.checkData(1, 0, 1763617917000)
        tdSql.checkData(1, 1, 20)
        tdSql.checkData(2, 0, 1763617920000)
        tdSql.checkData(2, 1, 45)
        # interval window without order by
        sql = f"select _wstart, sum(`status`) from (select _wstart, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname interval(2s)) interval(3s);"
        tdSql.query(sql)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1763617914000)
        tdSql.checkData(0, 1, 15)
        tdSql.checkData(1, 0, 1763617917000)
        tdSql.checkData(1, 1, 20)
        tdSql.checkData(2, 0, 1763617920000)
        tdSql.checkData(2, 1, 45)
        
        # interval window with order by desc on non-pk column
        sql = f"select _wstart, sum(`status`) from (select _wstart, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname interval(2s) order by 2 desc) interval(3s);"
        tdSql.error(sql)
        
        sql = f"select _wstart, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname interval(2s) order by 2 desc;"
        tdSql.query(sql)
        tdSql.checkRows(20)
        tdSql.checkData(0, 1, 1763617920000)
        tdSql.checkData(19, 1, 1763617916000)

        # union all window with order by desc
        sql = f"select  _wstart, first(`event_time`) from (select _wstart, first(`event_time`), `event_time`, `status`,  tbname from st partition by tbname interval(2s) union all (select _wstart, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname interval(2s)) order by _wstart desc) interval(3s)"
        tdSql.query(sql)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1763617914000)
        tdSql.checkData(1, 0, 1763617917000)
        tdSql.checkData(2, 0, 1763617920000)
 
        # union all window with order by asc
        sql = f"select  _wstart, first(`event_time`) from (select _wstart, first(`event_time`), `event_time`, `status`,  tbname from st partition by tbname interval(2s) union all (select _wstart, first(`event_time`) as t2, `event_time`, `status`, tbname from st partition by tbname interval(2s)) order by _wstart asc) interval(3s)"
        tdSql.query(sql)
        tdSql.checkRows(3)
        tdSql.checkData(0, 0, 1763617914000)
        tdSql.checkData(1, 0, 1763617917000)
        tdSql.checkData(2, 0, 1763617920000)
        
        # union all window with order by non-pk column
        sql = f"select  _wstart, first(`event_time`) from (select _wstart, first(`event_time`) as t2, `event_time`, `status`,  tbname from st partition by tbname interval(2s) union all (select _wstart, first(`event_time`) as t2, `event_time`, `status`, tbname from st partition by tbname interval(2s)) order by t2 asc) interval(3s)"
        tdSql.error(sql)

        # union all window without order by
        sql = f"select  _wstart, first(`event_time`) from (select _wstart, first(`event_time`), `event_time`, `status`,  tbname from st partition by tbname interval(2s) union all (select _wstart, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname interval(2s))) interval(3s)"
        tdSql.error(sql)
        
        # state window with order by asc
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
        
        # state window with order by desc
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
        
        # state window with order by non-pk column
        sql = f"select _wstart, sum(`status`) from (select _wstart, first(`event_time`), `event_time`, `status`, tbname from st partition by tbname state_window(`status`) order by 2 asc) interval(3s);"
        tdSql.error(sql)

        # state window without order by
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