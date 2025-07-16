import time
import math
from new_test_framework.utils import (
    tdLog,
    tdSql,
    tdStream,
    StreamTableType,
    StreamTable,
    StreamItem,
    StreamResultCheckMode,
)
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
        # self.generateDataExample()
        self.checkResultExample()
        self.stateWindowTest1()
        self.stateWindowTest2()

    @staticmethod
    def custom_cint_generator(row):
        return str(row / 10)  # 每行的 cint 为 row / 10

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

    def checkResultExample(self):
        tdLog.info(f"basic test 1")

        tdStream.dropAllStreamsAndDbs()

        tdStream.init_database("test")

        st1 = StreamTable("test", "trigger", StreamTableType.TYPE_SUP_TABLE)
        st1.createTable(3)
        st1.append_data(0, 10)

        tdSql.error(
            f"create stream s0 state_window (cint) from test.trigger stream_options(fill_history_first(1)) into test.out0  \
            as select _twstart ts, _twend, avg(cint) avg_cint, count(cint) count_cint from %%trows;"
        )

        sql = f"create stream s0 state_window (cint) from test.trigger_0 stream_options(fill_history_first(1)) into test.out0  \
            as select _twstart ts, _twend, avg(cint) avg_cint, count(cint) count_cint from %%trows;"

        stream = StreamItem(
            id=0,
            stream=sql,
            res_query="select * from test.out0;",
            exp_query="select '{_wstart}','{_twend}', avg(cint), count(cint) from test.trigger_0 where cts >= '{_wstart}' and cts <= '{_twend}';",
            check_mode=StreamResultCheckMode.CHECK_ROW_BY_SQL,
            exp_query_param_mapping={"_wstart": 0, "_twend": 1},
        )
        stream.createStream()
        expectRows = 9
        stream.awaitRowStability(expectRows)
        stream.checkResultsByRow()

    def stateWindowTest1(self):
        tdLog.info(f"state window test 1")

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

        sql = f"create stream s7 state_window (cint) from test.trigger_0 stream_options(fill_history_first(1)) into st7  as select _twstart ts, _twend, avg(cint) avg_cint, count(cint) count_cint from test.st where cts <= _twstart;"

        stream1 = StreamItem(
            id=0,
            stream=sql,
            res_query="select * from test.st7;",
            exp_query="select '{_wstart}','{_twend}', avg(cint), count(cint) from test.st where cts <= '{_wstart}';",
            check_mode=StreamResultCheckMode.CHECK_ROW_BY_SQL,
            exp_query_param_mapping={"_wstart": 0, "_twend": 1},
        )

        stream1.createStream()
        expectRows = 39
        stream1.awaitRowStability(expectRows)
        stream1.checkResultsByRow()

        # 追加写入
        trigger.append_data(60, 70)
        expectRows = 49
        stream1.awaitRowStability(expectRows)
        stream1.checkResultsByRow()

        # 乱序写入
        trigger.append_data(50, 55)
        expectRows = 54
        stream1.awaitRowStability(expectRows)
        stream1.checkResultsByRow()

        # 子表追加写入
        trigger.append_subtable_data("trigger_1", 55, 60)
        expectRows = 59
        stream1.awaitRowStability(expectRows)
        stream1.checkResultsByRow()

        # 更新写入
        tdSql.execute("delete * from st7;")
        trigger.update_data(10, 20)
        trigger.append_data(70, 71)
        expectRows = 60
        stream1.awaitRowStability(expectRows)
        stream1.checkResultsByRow()

        # 删除数据
        trigger.delete_data(30, 40)
        trigger.append_data(71, 72)
        expectRows = 61
        stream1.awaitRowStability(expectRows)
        stream1.checkResultsByRow()

        # 删除子表数据
        trigger.delete_subtable_data("st_1", 20, 30)
        trigger.append_data(72, 73)
        expectRows = 62
        stream1.awaitRowStability(expectRows)
        stream1.checkResultsByRow()

        tdLog.info("======over")

    def stateWindowTest2(self):
        tdLog.info(f"state window test 2")

        tdStream.dropAllStreamsAndDbs()

        tdStream.init_database("test")

        trigger = StreamTable("test", "trigger", StreamTableType.TYPE_SUP_TABLE)
        trigger.register_column_generator("cint", self.custom_cint_generator)
        trigger.setInterval(0.1)
        trigger.createTable(10)
        trigger.append_data(0, 40)
        trigger.update_data(3, 4)

        st1 = StreamTable("test", "st", StreamTableType.TYPE_SUP_TABLE)
        st1.createTable()
        st1.setInterval(0.1)
        st1.appendSubTables(200, 240)
        st1.append_data(0, 40)

        sql1 = f"create stream s1 state_window (cint) from test.trigger stream_options(fill_history_first(1)) into out1  as \
            select _twstart ts, _twend, avg(cint) avg_cint, count(cint) count_cint from test.st where cts <= _twstart;"
        sql2 = f"create stream s2 state_window (cint) from test.trigger partition by tbname stream_options(fill_history_first(1)) into out2  as \
            select _twstart ts, _twend, avg(cint) avg_cint, count(cint) count_cint from test.st where cts <= _twstart;"
        sql3 = f"create stream s3 state_window (cint) from test.trigger partition by tbname stream_options(fill_history_first(1)) into out3  as \
            select _twstart ts, _twend, avg(cint) avg_cint, count(cint) count_cint from %%trows;"
        stream1 = StreamItem(
            id=0,
            stream=sql1,
            res_query="select * from test.out1;",
            exp_query="select '{_wstart}','{_twend}', avg(cint), count(cint) from test.st where cts <= '{_wstart}';",
            check_mode=StreamResultCheckMode.CHECK_ROW_BY_SQL,
            exp_query_param_mapping={"_wstart": 0, "_twend": 1},
        )
        stream2 = StreamItem(
            id=0,
            stream=sql2,
            res_query="select * from test.out2 where tag_tbname='trigger_0';",
            exp_query="select '{_wstart}','{_twend}', avg(cint), count(cint) from test.st where cts <= '{_wstart}';",
            check_mode=StreamResultCheckMode.CHECK_ROW_BY_SQL,
            exp_query_param_mapping={"_wstart": 0, "_twend": 1},
        )
        stream3 = StreamItem(
            id=0,
            stream=sql3,
            res_query="select * from test.out3 where tag_tbname='trigger_0';",
            exp_query="select '{_wstart}','{_twend}', avg(cint), count(cint) from test.trigger_0 where cts <= '{_wstart}' and cts <= '{_twend}';",
            check_mode=StreamResultCheckMode.CHECK_ROW_BY_SQL,
            exp_query_param_mapping={"_wstart": 0, "_twend": 1},
        )
        stream1.createStream()
        stream2.createStream()
        stream3.createStream()

        expectRows1 = 5
        stream1.awaitRowStability(expectRows1)
        stream1.checkResultsByRow()

        expectRows2 = 50
        stream2.awaitRowStability(expectRows2)
        stream2.checkResultsByRow()

        expectRows3 = 50
        stream3.awaitRowStability(expectRows3)
        stream3.checkResultsByRow()
