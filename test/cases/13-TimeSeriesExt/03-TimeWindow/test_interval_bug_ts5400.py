import taos
import socket
from new_test_framework.utils import tdLog, tdSql, TDSql, tdDnodes


class TestTs5400:
    updatecfgDict = {"timezone": "UTC"}

    def setup_class(cls):
        host = socket.gethostname()
        con = taos.connect(
            host=f"{host}", config=tdDnodes.getSimCfgPath(), timezone="UTC"
        )
        tdLog.debug("start to execute %s" % __file__)
        cls.testSql = TDSql()
        cls.testSql.init(con.cursor())

    def prepare_data(self):
        # prepare data for TS-5400
        self.testSql.execute("create database db_ts5400 BUFFER 512 CACHESIZE 1024 CACHEMODEL 'both' COMP 2 DURATION 10d WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 KEEP 365000d;")
        self.testSql.execute("use db_ts5400;")
        #tdSql.execute("create stable st(ts TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `uk` VARCHAR(64) ENCODE 'disabled' COMPRESS 'lz4' LEVEL 'medium' PRIMARY KEY ) tags(ta int,tb int,tc int);")
        self.testSql.execute("create stable st(ts TIMESTAMP, `uk` VARCHAR(64)) tags(ta int,tb int,tc int);")
        self.testSql.execute("create table t1 using st tags(1,1,1);")
        self.testSql.execute("insert into t1 values ('1970-01-29 05:04:53.000','22:: ');")

    def test_ts5400(self):
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
        self.prepare_data()
        self.testSql.execute("use db_ts5400;")
        self.testSql.query("select to_char(_wstart, 'YYYY-MM-DD HH24:MI:SS.MS'), count(*) from st interval(1y);")
        self.testSql.checkRows(1)
        self.testSql.checkData(0, 0, "1970-01-01 00:00:00.000")
        self.testSql.checkData(0, 1, 1)
