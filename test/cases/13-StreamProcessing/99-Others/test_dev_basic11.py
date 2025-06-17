import time
import math
from new_test_framework.utils import tdLog, tdSql, tdStream, StreamTableType


class TestStreamDevBasic:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_dev_basic(self):
        """Stream basic development testing 11

        Verification testing during the development process.

        Catalog:
            - Streams:Others

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-6-16 xsRen Created

        """

        self.basic2()

    def basic1(self):
        tdLog.info(f"basic test 1")

        tdStream.dropAllStreamsAndDbs()
        tdStream.createSnode()
        
        tdStream.init_databases("test")
        tdStream.createTable("test", "trigger", StreamTableType.TYPE_SUP_TABLE, 10)
        tdStream.appendTableData("test.trigger_0", 0, 40)
        tdStream.updateTableRows("test.trigger_0", 3, 4)
        
        tdStream.createTable("test", "st", StreamTableType.TYPE_SUP_TABLE, 10)
        tdStream.appendTableData("test.st_0", 0, 40)
        tdStream.updateTableRows("test.st_0", 3, 4)
                 
        sql = f"create stream s7 state_window (cint) from test.trigger partition by tbname options(fill_history_first(1)) into st7  as select _twstart, avg(cint), count(cint) from test.st;"
        
        tdSql.execute(sql)
        
        tdSql.query("select * from st7;", queryTimes=10)
        tdSql.printResult()
        
        tdLog.info("======over")
        
    def basic2(self):
        tdLog.info(f"basic test 2")
        tdStream.dropAllStreamsAndDbs()
        tdStream.createSnode()
        
        tdStream.init_databases("test")
        tdStream.createTable("test", "trigger", StreamTableType.TYPE_NORMAL_TABLE)
        tdStream.appendTableData("test.trigger", 0, 40)
        tdStream.updateTableRows("test.trigger", 3, 4)
        
        tdStream.createTable("test", "nt", StreamTableType.TYPE_NORMAL_TABLE)
        tdStream.appendTableData("test.nt", 0, 40)
        tdStream.updateTableRows("test.nt", 3, 4)
        
        sql = f"create stream s7 state_window (cint) from test.trigger options(fill_history_first(1)) into st7  as select _twstart, avg(cint), count(cint) from test.nt;"
        
        tdSql.execute(sql)
        
        tdStream.appendTableData("test.trigger", 50, 60)
        tdSql.query("select * from st7;", queryTimes=10)
        tdSql.printResult()
        
        tdStream.deleteTableData(4)
        tdSql.query("select * from st7;", queryTimes=10)
        tdSql.printResult()
        
        tdStream.appendTableData(40, 50)
        
        tdSql.query("select * from st7;", queryTimes=10)
        tdSql.printResult()

        tdSql.execute("createa  table;", queryTimes=1)
        tdLog.info("======over")