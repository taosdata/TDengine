import time
import math
from new_test_framework.utils import tdLog, tdSql, tdStream, StreamTableType, StreamTable, StreamItem
from datetime import datetime

class TestStreamSlidingTrigger:
    dbname = "test"
    trigTbname = ""
    calcTbname = ""
    outTbname = ""
    stName = ""
    sliding = 
    subTblNum = 3
    tblRowNum = 40
    tableList = []
    querySql = ""
    querySqls = [ # (SQL, (minPartitionColNum, partitionByTbname), PositiveCase)
        ("select cts, cint from {calcTbname} order by cts limit 3", (0, False), True),
        ("select cts, cint, _tprev_ts, _tcurrent_ts, _tnext_ts, _tlocaltime from {calcTbname} order by cts", (0, False), True),
        ("select cts, cint, _tgrpid, %%1, %%2, %%tbname from {calcTbname} order by cts", (2, True), True),
        ("select cts, cint from {calcTbname} where _tcurrent_ts % 2 = 0 order by cts", (0, False), True),
        ("select cts, cint, _tgrpid, %%1, %%2, %%tbname from {calcTbname} where %%tbname like '%1' order by cts", (2, True), True),

        ("select cts, cint from %%tbname order by cts limit 3", (1, True), True),
        ("select cts, cint, _tprev_ts, _tcurrent_ts, _tnext_ts, _tlocaltime from %%tbname order by cts", (1, True), True),
        ("select cts, cint, _tgrpid, %%1, %%2, %%tbname from %%tbname order by cts", (2, True), True),
        ("select cts, cint from %%tbname where _tcurrent_ts % 2 = 0 order by cts", (1, True), True),
        ("select cts, cint, _tgrpid, %%1, %%2, %%tbname from %%tbname where %%tbname like '%1' order by cts", (2, True), True),

        ("select cts, cint from %%tbname partition by cint order by cts", (1, True), True),
        ("select cts, cint, _tprev_ts, _tcurrent_ts, _tnext_ts, _tlocaltime from %%tbname partition by cint order by cts", (1, True), True),
        ("select cts, cint, _tgrpid, %%1, %%2, %%tbname from %%tbname partition by cint order by cts", (2, True), True),
        ("select cts, cint from %%tbname where _tcurrent_ts % 2 = 0 partition by cint order by cts", (1, True), True),
        ("select cts, cint, _tgrpid, %%1, %%2, %%tbname from %%tbname where %%tbname like '%1' partition by cint order by cts", (2, True), True),

        #("select _twstart, avg(cint), count(cint) from {calcTbname} where cts <= _twstart", ),
        #("")
    ]

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_sliding_trigger(self):
        """Stream sliding trigger

        Catalog:
            - Streams:TriggerMode

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-6-18 dapan Created

        """

        tdStream.createSnode()
        self.prepareData()
        self.execCase()
        self.basic2()
        
    @staticmethod
    def custom_cint_generator(row):
        return str(row * 10)  # 每行的 cint 为 row * 10
        
    def prepareData(self):
        tdLog.info(f"prepare data")

        tdStream.dropAllStreamsAndDbs()
        
        tdStream.init_database("test1")
        
        st1 = StreamTable("test1", "st1", StreamTableType.TYPE_SUP_TABLE)
        st1.createTable(3)
        st1.append_data(0, self.tblRowNum)
        for i in range(1, self.subTblNum + 1):
            self.tableList.append(f"st1_{i}")
        
        ntb = StreamTable("test1", "ntb1", StreamTableType.TYPE_NORMAL_TABLE)
        ntb.append_data(0, self.tblRowNum)
        self.tableList.append(f"ntb1")

    def execCase(self, tbname):
        tdLog.info(f"execCase begin")

        sql = [
            (f"create stream {self.stName} sliding({self.sliding}s) from {self.trigTbname} into {self.outTbname} as {self.querySql};", (0, False)),
            (f"create stream {self.stName} sliding({self.sliding}s) from {self.trigTbname} partition by tbname into {self.outTbname} as {self.querySql};", (1, True)),
            (f"create stream {self.stName} sliding({self.sliding}s) from {self.trigTbname} partition by tvarchar, tint into {self.outTbname} as {self.querySql};", (2, False)),  
            (f"create stream {self.stName} sliding({self.sliding}s) from {self.trigTbname} partition by tbname, tint into {self.outTbname} as {self.querySql};", (2, True)),  
        ]
    
        stream1 = StreamItem(
            id=0,
            stream=sql,
            res_query="select * from test.st7;",
            check_func=self.checkBaic2Results,
        )
        stream1.createStream()
        stream1.awaitRowStability(39)
        stream1.checkResults()

    def checkBaic2Results(self):
        tdSql.query("select * from test.st7;", queryTimes=1)

        ts = tdSql.getColData(0)
        avg_cint = tdSql.getColData(1)
        count_cint = tdSql.getColData(2)
        
        for i in range(0, 40):
            sql = f"select '{ts[i]}', avg(cint), count(cint) from test.st where cts <= '{ts[i]}'"
            tdSql.query(sql, queryTimes=1)
            
            expected_ts = datetime.strptime(tdSql.getData(0, 0), "%Y-%m-%d %H:%M:%S") 
            expected_avg_cint = tdSql.getData(0, 1)
            expected_count_cint = tdSql.getData(0, 2)
            
            assert ts[i] == expected_ts, f"Row {i} ts mismatch: expected {expected_ts}, got {ts[i]}"
            assert math.isclose(avg_cint[i], expected_avg_cint, rel_tol=1e-9), f"Row {i} avg_cint mismatch: expected {expected_avg_cint}, got {avg_cint[i]}"
            assert count_cint[i] == expected_count_cint, f"Row {i} count_cint mismatch: expected {expected_count_cint}, got {count_cint[i]}"

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
    
        stream1 = StreamItem(
            id=0,
            stream=sql,
            res_query="select * from test.st7;",
            check_func=self.checkBaic2Results,
        )
        stream1.createStream()
        stream1.awaitRowStability(39)
        stream1.checkResults()

        tdLog.info("======over")
