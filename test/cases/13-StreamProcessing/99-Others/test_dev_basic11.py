import time
import math
from new_test_framework.utils import tdLog, tdSql, tdStream, StreamTableType, StreamTable


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
        tdStream.createSnode()
        self.basic1()
        self.basic2()
        
    @staticmethod
    def custom_cint_generator(row):
        return str(row * 10)  # 每行的 cint 为 row * 10
        
    def basic1(self):
        tdLog.info(f"basic test 1")

        tdStream.dropAllStreamsAndDbs()
        
        tdStream.init_database("test")
        tdStream.init_database("test1")
        
        st1 = StreamTable("test", "st", StreamTableType.TYPE_SUP_TABLE)
        st1.createTable()
        st1.appendSubTables(200, 240)
        st1.append_data(0, 40)
        st1.append_subtable_data("st_1", 40, 50)
        st1.update_data(3, 4)
        st1.update_subtable_data("st_1", 6, 7)
        st1.delete_data(7, 9)
        st1.delete_subtable_data("st_2", 8, 10)
        
        
        ntb = StreamTable("test", "ntb", StreamTableType.TYPE_NORMAL_TABLE)
        ntb.createTable()
        ntb.append_data(0, 40)
        ntb.update_data(3, 4)
        ntb.delete_data(7, 9)
        
        ntb2 = StreamTable("test1", "ntb2", StreamTableType.TYPE_NORMAL_TABLE)
        ntb2.set_columns("ts timestamp, c0 int, c1 int, c2 int")
        ntb2.register_column_generator("c0", self.custom_cint_generator)
        ntb2.createTable()
        ntb2.append_data(0, 10)
        ntb2.append_data(20, 30)
        ntb2.update_data(3, 6)

    def basic2(self):
        tdLog.info(f"basic test 1")

        tdStream.dropAllStreamsAndDbs()
        
        tdStream.init_database("test")
        
        trigger = StreamTable("test", "trigger", StreamTableType.TYPE_SUP_TABLE)
        trigger.createTable(10)
        trigger.append_data(0, 40)
        trigger.update_data(3, 4)
        
        st1 = StreamTable("test", "st", StreamTableType.TYPE_SUP_TABLE)
        st1.createTable()
        st1.appendSubTables(200, 240)
        st1.append_data(0, 40)
                 
        sql = f"create stream s7 state_window (cint) from test.trigger options(fill_history_first(1)) into st7  as select _twstart, avg(cint), count(cint) from test.st where cts <= _twstart;"
        
        tdSql.execute(sql)
        
        tdSql.query("select * from st7;", queryTimes=10)
        tdSql.printResult()
        
        #time.sleep(1000)

        tdLog.info("======over")