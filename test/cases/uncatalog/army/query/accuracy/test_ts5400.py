import taos
import socket
from new_test_framework.utils import tdLog, tdSql, tdDnodes

class TestTs5400:
    updatecfgDict = {
        "timezone": "UTC"
    }

    @classmethod
    def setup_class(cls):
        host = socket.gethostname()
        con = taos.connect(host=f"{host}", config=tdDnodes.getSimCfgPath(), timezone='UTC')
        tdLog.debug(f"start to execute {__file__}")
        tdSql.init(con.cursor())

    def prepare_data(self):
        # prepare data for TS-5400
        tdSql.execute("create database db_ts5400 BUFFER 512 CACHESIZE 1024 CACHEMODEL 'both' COMP 2 DURATION 10d WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 KEEP 365000d;")
        tdSql.execute("use db_ts5400;")
        #tdSql.execute("create stable st(ts TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `uk` VARCHAR(64) ENCODE 'disabled' COMPRESS 'lz4' LEVEL 'medium' PRIMARY KEY ) tags(ta int,tb int,tc int);")
        tdSql.execute("create stable st(ts TIMESTAMP, `uk` VARCHAR(64)) tags(ta int,tb int,tc int);")
        tdSql.execute("create table t1 using st tags(1,1,1);")
        tdSql.execute("insert into t1 values ('1970-01-29 05:04:53.000','22:: ');")

    def test_ts5400(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx

        """
        self.prepare_data()
        tdSql.execute("use db_ts5400;")
        tdSql.query("select _wstart, count(*) from st interval(1y);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '1970-01-01 00:00:00.00+0000')
        tdSql.checkData(0, 1, 1)

        tdLog.success(f"{__file__} successfully executed")
