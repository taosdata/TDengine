import taos
import socket
from frame.log import *
from frame.cases import *
from frame.sql import *
from frame.caseBase import *
from frame import *
from frame.eos import *
from frame.server.dnodes import *

class TDTestCase(TBase):
    """Add test case to cover TS-5400
    """
    updatecfgDict = {
        "timezone": "UTC"
    }

    def init(self, conn, logSql, replicaVar=1):
        host = socket.gethostname()
        con = taos.connect(host=f"{host}", config=tdDnodes.getSimCfgPath(), timezone='UTC')
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
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
        self.prepare_data()
        tdSql.execute("use db_ts5400;")
        tdSql.query("select _wstart, count(*) from st interval(1y);")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, '1970-01-01 00:00:00.000')
        tdSql.checkData(0, 1, 1)

    def run(self):
        self.test_ts5400()

    def stop(self):
        tdSql.execute("drop database db_ts5400;")
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
