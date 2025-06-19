import time
import math
from new_test_framework.utils import tdLog, tdSql, tdStream, StreamTableType, StreamTable, StreamItem
from datetime import datetime

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
        self.generateDataExample()
        self.stateWindowTest()
        
    @staticmethod
    def custom_cint_generator(row):
        return str(row * 10)  # 每行的 cint 为 row * 10
        
    def generateDataExample(self):
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
        
    def checkStateStreamResults1(self,  expectRows):
        tdSql.query("select ts, avg_cint, count_cint  from test.st7;", queryTimes=1)

        ts = tdSql.getColData(0)
        avg_cint = tdSql.getColData(1)
        count_cint = tdSql.getColData(2)
        
        for i in range(0, expectRows):
            sql = f"select '{ts[i]}', avg(cint), count(cint) from test.st where cts <= '{ts[i]}'"
            tdSql.query(sql, queryTimes=1)
            
            expected_ts = datetime.strptime(tdSql.getData(0, 0), "%Y-%m-%d %H:%M:%S") 
            expected_avg_cint = tdSql.getData(0, 1)
            expected_count_cint = tdSql.getData(0, 2)
            
            assert ts[i] == expected_ts, f"Row {i} ts mismatch: expected {expected_ts}, got {ts[i]}"
            assert math.isclose(avg_cint[i], expected_avg_cint, rel_tol=1e-9), f"Row {i} avg_cint mismatch: expected {expected_avg_cint}, got {avg_cint[i]}"
            assert count_cint[i] == expected_count_cint, f"Row {i} count_cint mismatch: expected {expected_count_cint}, got {count_cint[i]}"

    def stateWindowTest(self):
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
                 
        sql = f"create stream s7 state_window (cint) from test.trigger options(fill_history_first(1)) into st7  as select _twstart ts, _twend, avg(cint) avg_cint, count(cint) count_cint from test.st where cts <= _twstart;"
    
        stream1 = StreamItem(
            id=0,
            stream=sql,
            res_query="select * from test.st7;",
            check_func=self.checkStateStreamResults1,
        )
        stream1.createStream()
        expectRows = 39
        stream1.awaitRowStability(expectRows)
        self.checkStateStreamResults1(expectRows)
        
        # 追加写入
        trigger.append_data(60, 70)
        expectRows = 49
        stream1.awaitRowStability(expectRows)
        self.checkStateStreamResults1(expectRows)
        
        # 乱序写入
        trigger.append_data(50, 55)
        expectRows = 54
        stream1.awaitRowStability(expectRows)
        self.checkStateStreamResults1(expectRows)
        
        # 子表追加写入
        trigger.append_subtable_data("trigger_1", 55, 60)
        expectRows = 59
        stream1.awaitRowStability(expectRows)
        self.checkStateStreamResults1(expectRows)  
        
        # 更新写入
        tdSql.execute("delete * from st7;")
        trigger.update_data(10, 20)
        trigger.append_data(70, 71)
        expectRows = 60
        stream1.awaitRowStability(expectRows)
        self.checkStateStreamResults1(expectRows)        
        
        # 删除数据
        trigger.delete_data(30, 40)
        trigger.append_data(71, 72)
        expectRows = 61
        stream1.awaitRowStability(expectRows)
        self.checkStateStreamResults1(expectRows)        
        
        # 删除子表数据
        trigger.delete_subtable_data("st_1", 20, 30)
        trigger.append_data(72, 73)
        expectRows = 62
        stream1.awaitRowStability(expectRows)
        self.checkStateStreamResults1(expectRows)        
        

        tdLog.info("======over")