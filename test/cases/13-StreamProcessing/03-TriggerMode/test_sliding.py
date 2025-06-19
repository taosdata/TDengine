import time
import math
from new_test_framework.utils import tdLog, tdSql, tdStream, StreamTableType, StreamTable, StreamItem, tdCom
import datetime

class TestStreamSlidingTrigger:
    caseName = "test_stream_sliding_trigger"
    dbname = "test"
    trigTbname = ""
    calcTbname = ""
    outTbname = ""
    stName = ""
    sliding = 1
    subTblNum = 3
    tblRowNum = 40
    caseIdx = 0
    slidingList = [1, 10, 100, 1000]
    tableList = []
    skipCaseList = range(3)
    streamSql = ""
    querySql = ""
    querySqls = [ # (SQL, (minPartitionColNum, partitionByTbname), PositiveCase)
        ("select cts, cint from {calcTbname} order by cts limit 3", (0, False), True),
        ("select cts, cint from {calcTbname} order by cts desc limit 4", (0, False), True),
        ("select cts, cint, _tprev_ts, _tcurrent_ts, _tnext_ts, _tlocaltime from {calcTbname} order by cts", (0, False), True),
        ("select cts, cint, _tgrpid, %%1, %%2, %%tbname from {calcTbname} order by cts", (2, True), True),
        ("select cts, cint from {calcTbname} where _tcurrent_ts % 2 = 0 order by cts", (0, False), True),
        ("select cts, cint, _tgrpid, %%1, %%2, %%tbname from {calcTbname} where %%tbname like '%1' order by cts", (2, True), True),
        ("select _tcurrent_ts, cint from {calcTbname} order by cts limit 4", (0, False), True),

        ("select cts, cint from %%tbname order by cts limit 3", (1, True), True),
        ("select cts, cint, _tprev_ts, _tcurrent_ts, _tnext_ts, _tlocaltime from %%tbname order by cts", (1, True), True),
        ("select cts, cint, _tgrpid, %%1, %%2, %%tbname from %%tbname order by cts", (2, True), True),
        ("select cts, cint from %%tbname where _tcurrent_ts % 2 = 0 order by cts", (1, True), True),
        ("select cts, cint, _tgrpid, %%1, %%2, %%tbname from %%tbname where %%tbname like '%1' order by cts", (2, True), True),
        ("select _tcurrent_ts, cint from %%tbname order by cts limit 7", (1, True), True),

        ("select cts, cint from %%tbname partition by cint order by cts", (1, True), True),
        ("select cts, cint, _tprev_ts, _tcurrent_ts, _tnext_ts, _tlocaltime from %%tbname partition by cint order by cts", (1, True), True),
        ("select cts, cint, _tgrpid, %%1, %%2, %%tbname from %%tbname partition by cint order by cts", (2, True), True),
        ("select cts, cint from %%tbname where _tcurrent_ts % 2 = 0 partition by cint order by cts", (1, True), True),
        ("select cts, cint, _tgrpid, %%1, %%2, %%tbname from %%tbname where %%tbname like '%1' partition by cint order by cts", (2, True), True),
        ("select _tcurrent_ts, cint from %%tbname partition by cint order by cts", (1, True), True),

        #("select _twstart, avg(cint), count(cint) from {calcTbname} where cts <= _twstart", ),
        #("")
    ]

    queryResults = [
        #[expectedRows, compareFunc, hasResultFile, [{rowIdx:[col0Value, col1Value...]}]]
        [1, None, False, [{0:(datetime.datetime(2025, 1, 1, 0, 0), 0)}]],
        [2, None, False, [{0:(datetime.datetime(2025, 1, 1, 0, 19), 38)}, {1:(datetime.datetime(2025, 1, 1, 0, 19, 30), 39)}]],
        [40, None, True, []], #FAILED
        [-1, None, True, []],
        [-1, None, True, []],
        [-1, None, True, []],
        [-1, None, True, []],
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

        self.caseIdx = 0
        for self.trigTbname in self.tableList:
            for self.calcTbname in self.tableList:
                for self.sliding in self.slidingList:
                    self.execCase()

     
    def prepareData(self):
        tdLog.info(f"prepare data")

        tdStream.dropAllStreamsAndDbs()
        
        tdStream.init_database("test1")
        
        st1 = StreamTable("test1", "st1", StreamTableType.TYPE_SUP_TABLE)
        st1.createTable(3)
        st1.append_data(0, self.tblRowNum)
        
        self.tableList.append("st1")
        for i in range(1, self.subTblNum + 1):
            self.tableList.append(f"st1_{i}")
        
        ntb = StreamTable("test1", "ntb1", StreamTableType.TYPE_NORMAL_TABLE)
        ntb.createTable()
        ntb.append_data(0, self.tblRowNum)
        self.tableList.append(f"ntb1")

    def checkResultWithResultFile(self, caseIdx):
        tdCom.compare_query_with_result_file(caseIdx, f"select * from {self.outTbname};", f"ans/{self.caseName}.{caseIdx}.csv", self.caseName)
        tdLog.info("check result with result file succeed")

    def checkResultWithExpectedList(self, expectedList):
        tdSql.query(f"select * from {self.outTbname};", queryTimes=1)
        total_rows = tdSql.getRows()
        for row in expectedList:
            print(f"row:{row}")
            for rowIdx, rowValue in row.items():
                print(f"rowIdx:{rowIdx}, rowValue:{rowValue}")
                if rowIdx >= total_rows:
                    raise AssertionError(f"Expected row index {rowIdx} but only {total_rows} rows returned")
                rowData = tdSql.getRowData(rowIdx)
                print(f"rowData:{rowData}")
                assert rowData == rowValue, f"Expected value {rowValue} does not match actual value {rowData} for row index {rowIdx}"
        tdLog.info("check result with expected list succeed")

    def execCase(self):
        tdLog.info(f"execCase begin")

        sql = [ #(SQL, (minPartitionColNum, partitionByTbname))
            (f"create stream stName sliding({self.sliding}s) from {self.trigTbname} into outTbname as querySql;", (0, False)),
            (f"create stream stName sliding({self.sliding}s) from {self.trigTbname} into outTbname partition by tbname as querySql;", (1, True)),
            (f"create stream stName sliding({self.sliding}s) from {self.trigTbname} into outTbname partition by cint, tbname as querySql;", (2, True)),
            (f"create stream stName sliding({self.sliding}s) from {self.trigTbname} into outTbname partition by cvarchar, tbname, cint as querySql;", (3, True)),
        ]

        for sql_idx in range(len(sql)):
            for query_idx in range(len(self.querySqls)):
                tdLog.info(f"start {self.caseIdx} case:")
                if self.caseIdx in self.skipCaseList:
                    tdLog.info(f"skip case {self.caseIdx}")
                    self.caseIdx += 1
                    continue

                self.stName = f"s{self.caseIdx}"
                self.outTbname = f"{self.stName}_out"
                if sql[sql_idx][1][0] < self.querySqls[query_idx][1][0] or (sql[sql_idx][1][1] == True and False == self.querySqls[query_idx][1][1]):
                    self.caseIdx += 1
                    continue

                # Format the querySql with the current calcTbname
                self.querySql = self.querySqls[query_idx][0].replace("{calcTbname}", self.calcTbname)
                self.streamSql = sql[sql_idx][0].replace("querySql", self.querySql).replace("stName", self.stName).replace("outTbname", self.outTbname)
                print(f"{sql_idx} - {query_idx} exec sql: {self.querySql}")
                print(f"{sql_idx} - {query_idx} stream sql: {self.streamSql}")
                stream1 = StreamItem(
                    id=self.caseIdx,
                    stream=self.streamSql,
                    res_query=f"select * from {self.outTbname};",
                    check_func=self.queryResults[self.caseIdx][1],
                )
                stream1.createStream()
                stream1.awaitStreamRunning()
                stream1.awaitRowStability(self.queryResults[self.caseIdx][0])
                if stream1.check_func is not None:
                    stream1.check_func()
                elif True == self.queryResults[self.caseIdx][2]:
                    self.checkResultWithResultFile(self.caseIdx)
                else:
                    self.checkResultWithExpectedList(self.queryResults[self.caseIdx][3])

                self.caseIdx += 1

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
        stream1.awaitStreamRunning()
        stream1.awaitRowStability(39)
        stream1.checkResults()

        tdLog.info("======over")
