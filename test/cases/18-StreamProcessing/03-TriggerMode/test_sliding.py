import time
import math
import os
from new_test_framework.utils import tdLog, tdSql, tdStream, StreamTableType, StreamTable, StreamItem, tdCom
import datetime

class TestStreamSlidingTrigger:
    caseName = "test_stream_sliding_trigger"
    currentDir = os.path.dirname(os.path.abspath(__file__))
    runAll = False
    dbname = "test1"
    trigTbname = ""
    calcTbname = ""
    outTbname = ""
    stName = ""
    resultIdx = ""
    sliding = 1
    subTblNum = 3
    tblRowNum = 40
    caseIdx = 0
    trigTbIdx = 0
    calcTbIdx = 0
    slidIdx = 0
    createStmIdx = 0
    queryIdx = 0
    slidingList = [1, 10, 100, 1000]
    tableList = []
    # all success example
    # runCaseList = ["0-0-0-0-0", "0-0-0-0-1", "0-0-0-0-2", "0-0-0-0-4", "0-0-0-0-6", "0-0-0-0-27", "0-0-0-1-0", "0-0-0-1-1", "0-0-0-1-2", "0-0-0-1-4", "0-0-0-1-6", "0-0-0-1-7", "0-0-0-1-8", "0-0-0-1-10", "0-0-0-1-12", "0-0-0-1-13", "0-0-0-1-14", "0-0-0-1-16", "0-0-0-1-18", "0-0-0-2-0", "0-0-0-2-1", "0-0-0-2-2", "0-0-0-2-3", "0-0-0-2-4", "0-0-0-2-5", "0-0-0-2-6", "0-0-0-2-7", "0-0-0-2-8", "0-0-0-2-9", "0-0-0-2-10", "0-0-0-2-11", "0-0-0-2-12", "0-0-0-2-13", "0-0-0-2-14", "0-0-0-2-15", "0-0-0-2-16", "0-0-0-2-17", "0-0-0-2-18", "0-0-0-2-23", "0-0-0-3-0", "0-0-0-3-1", "0-0-0-3-2", "0-0-0-3-3", "0-0-0-3-4", "0-0-0-3-5", "0-0-0-3-6", "0-0-0-3-7", "0-0-0-4-2", "0-0-0-4-41", "0-0-0-4-6", "0-0-0-5-2", "0-0-0-5-41", "0-0-0-5-6", "0-0-0-6-0", "0-0-0-10-2", "0-0-0-10-39","0-0-0-10-45", "0-0-0-15-38", "0-0-0-15-43", "1-1-0-0-0", "1-1-0-0-26", "1-1-0-0-35", "1-1-0-0-36", "1-1-0-0-37", "1-1-0-0-38"]
    # less success example,no tcurrent_ts
    runCaseList = [
        "0-0-0-1-1", "0-0-0-1-7","0-0-0-1-13","0-0-0-2-13", 
        "0-0-0-3-7", "0-0-0-4-41", "0-0-0-6-0", "0-0-0-10-39", 
    ]
    # runCaseList = [ "0-0-0-15-38","0-0-0-15-43", "1-1-0-0-38"]
    # runCaseList = ["0-0-0-8-35", "0-0-0-8-36", "0-0-0-8-37", "0-0-0-8-38", "0-0-0-8-39", "0-0-0-8-40", "0-0-0-8-41", "0-0-0-8-42", "0-0-0-8-43", "0-0-0-8-44", "0-0-0-8-45"]
    #failed 
    # runCaseList = [  "0-0-0-10-42", "0-0-0-11-40","0-0-0-12-44","0-0-0-15-37"]
    
    #t_current_ts 
    #sql :3 4 6 8 10 12 14 16  18 19 20 21 22 23 24  25 26 27 28 29 30 31 32 33 35 36 45
    #tcurrent_ts = True
    # runCaseList = [
    #     "0-0-0-0-4", "0-0-0-1-6","0-0-0-1-8","0-0-0-2-18", 
    #     "0-0-0-3-20", "0-0-0-4-26", "0-0-0-6-28", "0-0-0-10-31", 
    #     "0-0-0-12-33", "0-0-0-15-35", "0-0-0-15-45", "1-1-0-0-22"
    # ]
    streamSql = ""
    querySql = ""
    querySqls = [ # (SQL, (minPartitionColNum, partitionByTbname), PositiveCase)
        ("select cts, cint from {calcTbname} order by cts limit 3", (0, False), True), #0
        ("select cts, cint from {calcTbname} order by cts desc limit 4", (0, False), True), #1
        ("select cts, cint, _tprev_ts, _tcurrent_ts, _tnext_ts from {calcTbname} order by cts", (0, False), True), #2
        ("select cts, cint,  %%1, %%2, %%tbname from {calcTbname} order by cts", (2, True), True), #3
        ("select cts, cint from {calcTbname} where _tcurrent_ts % 2 = 0 order by cts", (0, False), True), #4
        ("select cts, cint,  %%1, %%2, %%tbname from {calcTbname} where %%tbname like '%1' order by cts", (2, True), True), #5
        ("select _tcurrent_ts, cint from {calcTbname} order by cts limit 4", (0, False), True), #6

        ("select cts, cint from %%tbname order by cts limit 3", (1, True), True), #7
        ("select cts, cint, _tprev_ts, _tcurrent_ts, _tnext_ts from %%tbname order by cts", (1, True), True), #8
        ("select cts, cint,  %%1, %%2, %%tbname from %%tbname order by cts", (2, True), True), #9
        ("select cts, cint from %%tbname where _tcurrent_ts % 2 = 0 order by cts", (1, True), True), #10
        ("select cts, cint,  %%1, %%2, %%tbname from %%tbname where %%tbname like '%1' order by cts", (2, True), True), #11
        ("select _tcurrent_ts, cint from %%tbname order by cts limit 7", (1, True), True), #12

        ("select cts, cint from %%tbname partition by cint order by cts", (1, True), True), #13
        ("select cts, cint, _tprev_ts, _tcurrent_ts, _tnext_ts from %%tbname partition by cint order by cts", (1, True), True), #14
        ("select cts, cint,  %%1, %%2, %%tbname from %%tbname partition by cint order by cts", (2, True), True), #15
        ("select cts, cint from %%tbname where _tcurrent_ts % 2 = 0 partition by cint order by cts", (1, True), True), #16
        ("select cts, cint,  %%1, %%2, %%tbname from %%tbname where %%tbname like '%1' partition by cint order by cts", (2, True), True), #17
        ("select _tcurrent_ts, cint from %%tbname partition by cint order by cts", (1, True), True), #18
        ("select cts, _tcurrent_ts, cint from %%trows", (0, False), True), #19

        ("select _tcurrent_ts, avg(cint), sum(cint) from %%tbname", (1, True), True), #20
        ("select _tcurrent_ts, avg(cint), max(cint) from {calcTbname}", (0, False), True), #21
        ("select _tcurrent_ts, avg(cint), max(cint) from {calcTbname} partition by cint", (0, False), True), #22
        ("select _tcurrent_ts, avg(cint), max(cint) from {calcTbname} partition by tbname", (0, False), True), #23
        ("select _tcurrent_ts, avg(cint), sum(cint) from %%tbname group by cint", (1, True), True), #24

        ("select _wstart, _tcurrent_ts, avg(cint), max(cint) from {calcTbname} partition by tbname interval(5s)", (0, False), True), #25
        ("select _wstart, _tcurrent_ts, avg(cint), sum(cint) from {calcTbname} interval(10s)", (0, False), True), #26
        ("select _wstart, _tcurrent_ts, avg(cint), sum(cint) from {calcTbname} where cts >= _tprev_ts and cts < _tcurrent_ts interval(10s)", (0, False), True), #27
        ("select _wstart, _tcurrent_ts, avg(cint), sum(cint) from %%trows interval(1s)", (0, False), True), #28
        ("select _wstart, _tcurrent_ts, avg(cint), sum(cint) from %%trows interval(60s)" , (0, False), True), #29

        ("select _wstart, _tcurrent_ts, avg(cint), sum(cint) from {calcTbname} partition by cint state_window(cuint)", (0, False), True), #30
        ("select _wstart, _tcurrent_ts, avg(cint), sum(cint) from %%tbname where cts % 3 != 0 session(cts, 2s)", (1, True), True), #31
        ("select _wstart, _tcurrent_ts, avg(cint), sum(cint) from %%trows event_window start with cbigint = 0 end with cbigint > 1", (0, False), True), #32
        ("select _wstart, _tcurrent_ts, avg(cint), sum(cint) from %%trows partition by cuint count_window(3)", (0, False), True), #33

        ("select cts, cint from %%tbname where ts >= _twstart and ts < _twend", (1, True), True), #34
        #add
        ("select _tcurrent_ts, avg(cint), max(cint) from {calcTbname} partition by cint order by cint", (1, False), True), #35
        ("select _wstart, _tcurrent_ts, avg(cint), sum(cint) from {calcTbname} interval(60s)", (0, False), True), #36
        ("select _wstart,sum({calcTbname}.cint), max({calcTbname}.cint) from {calcTbname} ,{calcTbname} as t2 where {calcTbname}.cts=t2.cts  interval(120s)", (0, False), True), #37
        ("(select _wstart, avg(cint) c, max(cint) from {calcTbname} partition by cint interval(60s) ) union all (select _wstart, avg(cint) c, max(cint) from {calcTbname} partition by cint interval(60s) order by _wstart,c)", (0, False), True), #38
        ("select last(cts), avg(cint), sum(cint) from %%tbname group by tbname", (1, True), True), #39
        ("select cts, cint,   %%tbname from %%trows order by cts", (0, False), True), #40
        ("select cts, cint from {calcTbname} where _tcurrent_ts % 2 = 1 order by cts", (0, False), True), #41
        ("select last(cts), avg(cint), sum(cint) from %%trows group by tbname", (1, True), True), #42
        ("(select _wstart, avg(cint) c, max(cint) from {calcTbname}  interval(60s) order by _wstart,c) union all (select _wstart, avg(cint) c, max(cint) from {calcTbname}  interval(60s) order by _wstart,c)", (0, False), True), #43
        ("select cts, cint,   %%tbname from %%trows order by cts", (0, False), True), #44
        ("select _tcurrent_ts, avg(cint), sum(cint) from %%tbname group by cint order by cint", (1, True), True), #45

    ]

    queryResults = {
        #{"trigTbIdx-calcTbIdx-slidIdx-createStmIdx-queryIdx":[expectedRows, compareFunc, hasResultFile, [{rowIdx:[col0Value, col1Value...]}, order by clause]]}

        
        "0-0-0-0-0": [1, None, False, [{0:(datetime.datetime(2025, 1, 1, 0, 0), 0)}], ""], #success
        "0-0-0-0-1": [2, None, False, [{0:(datetime.datetime(2025, 1, 1, 0, 19), 38)}, {1:(datetime.datetime(2025, 1, 1, 0, 19, 30), 39)}], ""],#success
        "0-0-0-0-2": [40, None, True, [], ""], #success
        "0-0-0-0-4": [40, None, True, [], ""], #success
        "0-0-0-0-6": [1171, None, True, [], ""], #success

        "0-0-0-0-27": [40, None, True, [], ""], #success

        "0-0-0-1-0": [3, None, True, [], ""], #success
        "0-0-0-1-1": [6, None, True, [], "order by tag_tbname, cts"], #success
        "0-0-0-1-2": [120, None, True, [], ""], #success
        "0-0-0-1-4": [120, None, True, [], "order by cts, tag_tbname"],  #success
        "0-0-0-1-6": [3513, None, True, [], ""], #success

        
        "0-0-0-1-7": [9, None, True, [], "order by tag_tbname,cint"], #success
        "0-0-0-1-8": [120, None, True, [], ""], #success
        "0-0-0-1-10": [120, None, True, [], "order by cts, tag_tbname limit 150"],#success
        "0-0-0-1-12": [3513, None, True, [], ""], #success
        "0-0-0-1-13": [120, None, True, [], "order by cts, tag_tbname limit 150"],#success,ans原先 sql 缺少 limit 150

        
        "0-0-0-1-14": [120, None, True, [], ""], #success
        "0-0-0-1-16": [120, None, True, [], "order by cts, tag_tbname limit 150"],#success,ans原先 sql 缺少 limit 150
        "0-0-0-1-18": [3513, None, True, [], "order by `_tcurrent_ts`, tag_tbname"], #success


        "0-0-0-2-0": [3, None, True, [], "order by cts, tag_tbname"],#success
        "0-0-0-2-1": [6, None, True, [], ""], #success
        
        "0-0-0-2-2": [120, None, True, [], "order by cts, tag_tbname limit 150"], #success
        "0-0-0-2-3": [120, None, True, [], "order by cts, tag_tbname"], #success
        "0-0-0-2-4": [120, None, True, [], "order by cts, tag_tbname"], #success
        "0-0-0-2-5": [40, None, True, [], ""], #success
        "0-0-0-2-6": [3513, None, True, [], ""], #success 

        
        "0-0-0-2-7": [9, None, True, [], ""], #success
        "0-0-0-2-8": [120, None, True, [], ""], #success
        "0-0-0-2-9": [120, None, True, [], ""], #success
        "0-0-0-2-10": [120, None, True, [], ""],#success
        "0-0-0-2-11": [40, None, True, [], ""],#success

        
        "0-0-0-2-12": [3513, None, True, [], ""],#success
        "0-0-0-2-13": [120, None, True, [], "order by tag_tbname,cts"],#success
        "0-0-0-2-14": [120, None, True, [], ""],#success
        "0-0-0-2-15": [120, None, True, [], ""],#success
        "0-0-0-2-16": [120, None, True, [], ""],#success

        
        "0-0-0-2-17": [40, None, True, [], ""],#success
        "0-0-0-2-18": [3513, None, True, [], ""], #success
        "0-0-0-2-23": [3513, None, True, [], ""], #success


        "0-0-0-3-0": [3, None, True, [], "order by cts, tag_tbname"], #success
        "0-0-0-3-1": [6, None, True, [], "order by cts, tag_tbname"], #success    
        "0-0-0-3-2": [120, None, True, [], "order by cts, tag_tbname"], #success      

        "0-0-0-3-3": [120, None, True, [], ""],#success 
        "0-0-0-3-4": [120, None, True, [], ""], #success
        "0-0-0-3-5": [40, None, True, [], ""], #success
        "0-0-0-3-6": [3513, None, True, [], ""],    #success     
        "0-0-0-3-7": [9, None, True, [], "order by tag_tbname,cts"],   #success   

        "0-0-0-4-2": [40, None, True, [], ""],  #success    
        "0-0-0-4-41": [40, None, True, [], ""],  #success  
        "0-0-0-4-6": [1170, None, True, [], ""],  #success

        "0-0-0-5-2": [120, None, True, [], "order by cts, tag_tbname"],     #success
        "0-0-0-5-41": [120, None, True, [], ""],     #success
        "0-0-0-5-6": [3510, None, True, [], ""],    #success

        "0-0-0-6-0": [3, None, True, [], "order by cts, tag_tbname"],#success 
        
        "0-0-0-8-35": [-1, None, True, [], ""],#
        "0-0-0-8-36": [-1, None, True, [], ""],#
        "0-0-0-8-37": [-1, None, True, [], ""],#
        "0-0-0-8-38": [-1, None, True, [], ""],#
        "0-0-0-8-39": [-1, None, True, [], ""],#
        "0-0-0-8-40": [-1, None, True, [], ""],#
        "0-0-0-8-41": [-1, None, True, [], ""],#
        "0-0-0-8-42": [-1, None, True, [], ""],#
        "0-0-0-8-43": [-1, None, True, [], ""],#
        "0-0-0-8-44": [-1, None, True, [], ""],#
        "0-0-0-8-45": [-1, None, True, [], ""],#
        
        
        "0-0-0-10-2": [120, None, True, [], "limit 10"],#success
        "0-0-0-10-39": [3, None, True, [], "order by tag_tbname"],#success
        "0-0-0-10-45": [3513, None, True, [], ""],#success
        #"0-0-0-10-42": [111, None, True, [], ""],#failed fillhistory还是加载不符合范围数据
        #"0-0-0-11-40": [40, None, True, [], ""],#failed，fillhistory还是加载不符合范围数据
        "0-0-0-12-40": [37, None, True, [], ""], #success
        "0-0-0-12-44": [24, None, True, [], ""], #success
        #"0-0-0-13-40": [0, None, True, [], ""], #success，只通知不计算,需要单独用例
        #"0-0-0-14-40": [0, None, True, [], ""],#success，只通知不计算,需要单独用例
        "0-0-0-15-37": [10, None, True, [], ""],#failed, sum结果不对
        "0-0-0-15-38": [20, None, True, [], ""],#success
        "0-0-0-15-43": [20, None, True, [], ""],#success
        
        "1-1-0-0-0": [3, None, True, [], ""],  # 触发表子表、计算表子表 #success
        "1-1-0-0-26": [40, None, True, [], ""],#success
        "1-1-0-0-35": [1171, None, True, [], ""], #success
        "1-1-0-0-36": [20, None, True, [], ""], #success
        "1-1-0-0-37": [10, None, True, [], ""], #success 
        "1-1-0-0-38": [20, None, True, [], ""],  #success
    }

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_stream_sliding_trigger(self):
        """Options: sliding
        
        1. Create stream with sliding trigger mode, different partition by columns and sliding time.
        2. Execute various queries on the stream with different calculation tables and validate the results.
        3. Validate the results against expected outputs or result files.
        4. Clean up the created streams and databases after test execution.

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-6-18 dapan Created

        """

        tdStream.createSnode()
        self.prepareData()

        self.caseIdx = 0
        for self.trigTbIdx in range(len(self.tableList)):
            self.trigTbname = self.tableList[self.trigTbIdx]
            for self.calcTbIdx in range(len(self.tableList)):
                self.calcTbname = self.tableList[self.calcTbIdx]
                for self.slidIdx in range(len(self.slidingList)):
                    self.sliding = self.slidingList[self.slidIdx]
                    if False == self.execCase():
                        return
                    
        # tdStream.checkStreamStatus()
        # self.prepareData()

     
    def prepareData(self):
        tdLog.info(f"prepare data")

        tdStream.dropAllStreamsAndDbs()
        
        tdStream.init_database(self.dbname)
        
        st1 = StreamTable(self.dbname, "st1", StreamTableType.TYPE_SUP_TABLE)
        st1.createTable(3)
        st1.append_data(0, self.tblRowNum)
        
        self.tableList.append("st1")
        for i in range(0, self.subTblNum + 1):
            self.tableList.append(f"st1_{i}")
        
        ntb = StreamTable(self.dbname, "ntb1", StreamTableType.TYPE_NORMAL_TABLE)
        ntb.createTable()
        ntb.append_data(0, self.tblRowNum)
        self.tableList.append(f"ntb1")

    def checkResultWithResultFile(self):
        chkSql = f"select * from {self.dbname}.{self.outTbname} {self.queryResult[4]}"
        tdLog.info(f"check result with sql: {chkSql}")
        if self.queryResult[0] < 0:
            tdCom.generate_query_result_file(self.caseName, self.resultIdx, chkSql)
        else:
            tdCom.compare_query_with_result_file(self.resultIdx, chkSql, f"{self.currentDir}/ans/{self.caseName}.{self.resultIdx}.csv", self.caseName)
            tdLog.info("check result with result file succeed")

    def checkResultWithExpectedList(self):
        chkSql = f"select * from {self.dbname}.{self.outTbname} {self.queryResult[4]}"
        tdLog.info(f"check result with sql: {chkSql}")
        tdSql.query(chkSql, queryTimes=1)
        total_rows = tdSql.getRows()
        for row in self.queryResult[3]:
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

        runnedCaseNum = 0
        createStreamSqls = [ #(SQL, (minPartitionColNum, partitionByTbname))
            (f"create stream stName sliding({self.sliding}s) from {self.trigTbname} stream_options(fill_history('2025-01-01 00:00:00')) into outTbname as querySql;", (0, False)),#0
            (f"create stream stName sliding({self.sliding}s) from {self.trigTbname} partition by tbname stream_options(fill_history('2025-01-01 00:00:00')) into outTbname as querySql;", (1, True)),#1
            (f"create stream stName sliding({self.sliding}s) from {self.trigTbname} partition by tint, tbname stream_options(fill_history('2025-01-01 00:00:00')) into outTbname as querySql;", (2, True)),#2
            (f"create stream stName sliding({self.sliding}s) from {self.trigTbname} partition by tvarchar, tbname, tint stream_options(fill_history('2025-01-01 00:00:00')) into outTbname as querySql;", (3, True)),#3
            (f"create stream stName sliding({self.sliding}s, 1a) from {self.trigTbname} stream_options(fill_history('2025-01-01 00:00:00')) into outTbname as querySql;", (0, False)),#
            
            (f"create stream stName sliding({self.sliding}s, 1a) from {self.trigTbname} partition by tbname stream_options(fill_history('2025-01-01 00:00:00')) into outTbname as querySql;", (1, True)),#5
            (f"create stream stName interval({self.sliding}s) sliding({self.sliding}s) from {self.trigTbname} partition by tbname stream_options(fill_history('2025-01-01 00:00:00')) into outTbname as querySql;", (1, True)),#6
            (f"create stream stName interval({self.sliding + 1}s, 1a) sliding({self.sliding}s) from {self.trigTbname} stream_options(fill_history('2025-01-01 00:00:00')) into outTbname as querySql;", (0, False)),#7
            (f"create stream stName interval({self.sliding + 30}s, 1a) sliding({self.sliding}s, 1a) from {self.trigTbname} partition by tbname stream_options(fill_history('2025-01-01 00:00:00')) into outTbname as querySql;", (1, True)),#8

            (f"create stream stName sliding({self.sliding}s) from {self.trigTbname} stream_options(watermark(10s)|expired_time(40s)|ignore_disorder|delete_output_table) into outTbname as querySql;", (3, True)),#9
            (f"create stream stName sliding({self.sliding}s) from {self.trigTbname} partition by tbname stream_options(fill_history('2025-01-01 00:01:30')) into outTbname as querySql ;", (1, True)),#10
            (f"create stream stName sliding({self.sliding}s) from {self.trigTbname} partition by tbname stream_options(watermark(3s)|fill_history('2025-01-01 00:01:30')) into outTbname as querySql;", (1, True)),#11
            (f"create stream stName sliding({self.sliding}s) from {self.trigTbname} partition by tbname stream_options(pre_filter(cint>2)|fill_history('2025-01-01 00:00:00')) into outTbname as querySql;", (1, True)),#12
            (f"create stream stName sliding({self.sliding}s) from {self.trigTbname} partition by tbname stream_options(calc_notify_only|fill_history('2025-01-01 00:00:00')) into outTbname as querySql;", (1, True)),#13
            (f'create stream stName sliding({self.sliding}s) from {self.trigTbname} partition by tbname stream_options(calc_notify_only|fill_history("2025-01-01 00:00:00")) notify("ws://localhost:8080/notify") into outTbname as querySql;', (1, True)),#14
            (f"create stream stName sliding({self.sliding}s) from {self.trigTbname} partition by tbname,tint stream_options(pre_filter(tint>0)|fill_history('2025-01-01 00:00:00')) into outTbname as querySql;", (1, True)),#15
            (f"create stream stName sliding({self.sliding}s) from {self.trigTbname} partition by tbname stream_options(pre_filter(cint is null)|fill_history('2025-01-01 00:00:00')) into outTbname as querySql;", (1, True)),#16
        ]

        for self.createStmIdx in range(len(createStreamSqls)):
            for self.queryIdx in range(len(self.querySqls)):
                #print(f"caseListLen:{len(self.runCaseList)}, runnedCaseNum:{runnedCaseNum}")

                if not self.runAll and len(self.runCaseList) > 0 and runnedCaseNum == len(self.runCaseList):
                    tdLog.info(f"all cases in runCaseList: {self.runCaseList} has finished")
                    return False

                if not self.runAll and len(self.queryResults) > 0 and runnedCaseNum == len(self.queryResults):
                    tdLog.info(f"all cases in queryResults has finished")
                    return False

                if createStreamSqls[self.createStmIdx][1][0] < self.querySqls[self.queryIdx][1][0] or (createStreamSqls[self.createStmIdx][1][1] == False and True == self.querySqls[self.queryIdx][1][1]):
                    tdLog.debug(f"skip case because createStmIdx={self.createStmIdx} query_idx={self.queryIdx} minPartitionColNum mismatch")
                    continue

                self.resultIdx = f"{self.trigTbIdx}-{self.calcTbIdx}-{self.slidIdx}-{self.createStmIdx}-{self.queryIdx}"

                if not self.runAll and len(self.runCaseList) > 0 and self.resultIdx not in self.runCaseList:
                    tdLog.debug(f"skip case {self.caseIdx} idx: {self.resultIdx}")
                    self.caseIdx += 1
                    continue

                if not self.runAll and self.resultIdx not in self.queryResults:
                    tdLog.debug(f"skip case {self.caseIdx} idx: {self.resultIdx} because not in queryResults")
                    self.caseIdx += 1
                    continue

                runnedCaseNum += 1

                tdLog.info(f"case {self.caseIdx} idx: {self.resultIdx} start:")

                self.stName = f"`s{self.resultIdx}`"
                self.outTbname = f"`s{self.resultIdx}_out`"
                if self.runAll and self.resultIdx not in self.queryResults:
                    self.queryResult = [-1, None, False, [], ""]  # Default empty result
                else:
                    self.queryResult = self.queryResults[self.resultIdx]

                # Format the querySql with the current calcTbname
                self.querySql = self.querySqls[self.queryIdx][0].replace("{calcTbname}", self.calcTbname)
                self.streamSql = createStreamSqls[self.createStmIdx][0].replace("querySql", self.querySql).replace("stName", self.stName).replace("outTbname", self.outTbname)
                tdLog.info(f"exec sql: {self.querySql}")
                tdLog.info(f"stream sql: {self.streamSql}")
                stream1 = StreamItem(
                    id=self.resultIdx,
                    stream=self.streamSql,
                    res_query=f"select * from {self.outTbname};",
                    check_func=self.queryResult[1],
                )
                stream1.createStream()
                stream1.awaitStreamRunning()
                # time.sleep(6)
                if not self.runAll:
                    stream1.awaitRowStability(self.queryResult[0])
                    if stream1.check_func is not None:
                        stream1.check_func()
                    elif True == self.queryResult[2]:
                        self.checkResultWithResultFile()
                    else:
                        self.checkResultWithExpectedList()

                self.caseIdx += 1

        return True

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
                 
        sql = f"create stream s7 state_window (cint) from test.trigger stream_options(fill_history_first(1)) into st7  as select _twstart, avg(cint), count(cint) from test.st where cts <= _twstart;"
    
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
