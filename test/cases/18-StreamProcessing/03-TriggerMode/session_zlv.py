import time
import math
import os
from new_test_framework.utils import tdLog, tdSql, tdStream, StreamTableType, StreamTable, StreamItem, tdCom
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

class TestStreamSlidingTrigger:
    caseName = "test_stream_session_trigger"
    currentDir = os.path.dirname(os.path.abspath(__file__))
    runAll = False
    dbname = "test1"
    trigTbname = ""
    calcTbname = ""
    outTbname = ""
    stName = ""
    resultIdx = ""
    session = 12
    subTblNum = 3
    tblRowNum = 40
    caseIdx = 0
    trigTbIdx = 0
    calcTbIdx = 0
    slidIdx = 0
    createStmIdx = 0
    queryIdx = 0
    sessionList = [1, 10, 100, 1000]
    tableList = []
    runCaseList = [
        "0-0-0-0-6", "0-0-0-1-1", "0-0-0-1-6", "0-0-0-1-7",
        "0-0-0-1-12", "0-0-0-1-13",  "0-0-0-2-10", "0-0-0-2-13",
        "0-0-0-2-18", "0-0-0-2-23", "0-0-0-3-6", "0-0-0-3-7", "0-0-0-4-6",
        "0-0-0-4-41", "0-0-0-5-6", "0-0-0-6-0", "0-0-0-10-39", "0-0-0-10-45",
        "0-0-0-12-40", "0-0-0-15-38", "0-0-0-15-43", "1-1-0-0-36", "1-1-0-0-38"
    ]
    #failed :
    # runCaseList = ["0-0-0-0-4", ]
    querySqls = [ # (SQL, (minPartitionColNum, partitionByTbname), PositiveCase)
        ("select cts, cint from {calcTbname} order by cts limit 3", (0, False), True), #0
        ("select cts, cint from {calcTbname} order by cts desc limit 4", (0, False), True), #1
        ("select cts, cint,  from {calcTbname} order by cts", (0, False), True), #2
        ("select cts, cint,  %%1, %%2, %%tbname from {calcTbname} order by cts", (2, True), True), #3
        ("select cts, cint from {calcTbname} where cint % 2 = 0 order by cts", (0, False), True), #4
        ("select cts, cint,  %%1, %%2, %%tbname from {calcTbname} where %%tbname like '%1' order by cts", (2, True), True), #5
        ("select cts, cint from {calcTbname} order by cts limit 4", (0, False), True), #6

        ("select cts, cint from %%tbname order by cts limit 3", (1, True), True), #7
        ("select cts, cint,  from %%tbname order by cts", (1, True), True), #8
        ("select cts, cint,  %%1, %%2, %%tbname from %%tbname order by cts", (2, True), True), #9
        ("select cts, cint from %%tbname where tint % 2 = 0 order by cts", (1, True), True), #10
        ("select cts, cint,  %%1, %%2, %%tbname from %%tbname where %%tbname like '%1' order by cts", (2, True), True), #11
        ("select cts, cint from %%tbname order by cts limit 7", (1, True), True), #12

        ("select cts, cint from %%tbname partition by cint order by cts", (1, True), True), #13
        ("select cts, cint,  from %%tbname partition by cint order by cts", (1, True), True), #14
        ("select cts, cint,  %%1, %%2, %%tbname from %%tbname partition by cint order by cts", (2, True), True), #15
        ("select cts, cint from %%tbname where cint % 2 = 1 partition by cint order by cts", (1, True), True), #16
        ("select cts, cint,  %%1, %%2, %%tbname from %%tbname where %%tbname like '%1' partition by cint order by cts", (2, True), True), #17
        ("select cts, cint from %%tbname partition by cint order by cts", (1, True), True), #18
        ("select cts, cint from %%trows", (0, False), True), #19

        ("select cts, avg(cint), sum(cint) from %%tbname", (1, True), True), #20
        ("select cts, avg(cint), max(cint) from {calcTbname}", (0, False), True), #21
        ("select cts, avg(cint), max(cint) from {calcTbname} partition by cint", (0, False), True), #22
        ("select cts, avg(cint), max(cint) from {calcTbname} partition by tbname", (0, False), True), #23
        ("select cts, avg(cint), sum(cint) from %%tbname group by cint", (1, True), True), #24

        ("select _wstart, cts, avg(cint), max(cint) from {calcTbname} partition by tbname interval(5s)", (0, False), True), #25
        ("select _wstart, cts, avg(cint), sum(cint) from {calcTbname} interval(10s)", (0, False), True), #26
        ("select _wstart, cts, avg(cint), sum(cint) from {calcTbname} where cts >= '2025-01-01 00:00:00' and cts < '2025-01-01 00:18:00' interval(10s)", (0, False), True), #27
        ("select _wstart, avg(cint), sum(cint) from %%trows where cts >= '2025-01-01 00:00:31' and cts < '2025-01-01 00:21:00' interval(1s)", (0, False), True), #28
        ("select _wstart, avg(cint), sum(cint) from %%trows interval(60s)" , (0, False), True), #29

        ("select _wstart,  avg(cint), sum(cint) from {calcTbname} partition by cint state_window(cuint)", (0, False), True), #30
        ("select _wstart,  avg(cint), sum(cint) from %%tbname where cts % 3 != 0 session(cts, 2s)", (1, True), True), #31
        ("select _wstart,  avg(cint), sum(cint) from %%trows event_window start with cbigint = 0 end with cbigint > 1", (0, False), True), #32
        ("select _wstart,  avg(cint), sum(cint) from %%trows partition by cuint count_window(3)", (0, False), True), #33

        ("select cts, cint from %%tbname where ts >= _twstart and ts < _twend", (1, True), True), #34
        #add
        ("select cts, avg(cint), max(cint) from {calcTbname} partition by cint order by cint", (1, False), True), #35
        ("select _wstart, cts, avg(cint), sum(cint) from {calcTbname} interval(60s)", (0, False), True), #36
        ("select _wstart,sum({calcTbname}.cint), max({calcTbname}.cint) from {calcTbname} ,{calcTbname} as t2 where {calcTbname}.cts=t2.cts  interval(120s)", (0, False), True), #37
        ("(select _wstart, avg(cint) c, max(cint) from {calcTbname} partition by cint interval(60s)) union all (select _wstart, avg(cint) c, max(cint) from {calcTbname} partition by cint interval(60s) order by _wstart,c)", (0, False), True), #38
        ("select last(cts), avg(cint), sum(cint) from %%tbname group by tbname", (1, True), True), #39
        ("select cts, cint,   %%tbname from %%trows where %%tbname like '%1' order by cts", (0, False), True), #40
        ("select cts, cint from {calcTbname} where cint % 2 = 1 order by cts", (0, False), True), #41
        ("select last(cts), avg(cint), sum(cint) from %%trows group by tbname", (1, True), True), #42
        ("select _wstart, avg(cint) c, max(cint) from {calcTbname}  interval(60s) union all select _wstart, avg(cint) c, max(cint) from {calcTbname}  interval(60s) order by _wstart,c", (0, False), True), #43
        ("select cts, cint,   %%tbname from %%trows where cint >15 and tint >0 and  %%tbname like '%2' order by cts", (0, False), True), #44
        ("select cts, avg(cint), sum(cint) from %%tbname group by cint order by cint", (1, True), True), #45

    ]
    queryResults = {
        #{"trigTbIdx-calcTbIdx-slidIdx-createStmIdx-queryIdx":[expectedRows, compareFunc, hasResultFile, [{rowIdx:[col0Value, col1Value...]}, order by clause]]}

        
        "0-0-0-0-0": [-1, None, False, [{0:(datetime.datetime(2025, 1, 1, 0, 0), 0)}], ""], #success
        "0-0-0-0-1": [-1, None, False, [{0:(datetime.datetime(2025, 1, 1, 0, 19), 38)}, {1:(datetime.datetime(2025, 1, 1, 0, 19, 30), 39)}], ""],#success
        "0-0-0-0-2": [-1, None, True, [], ""], #success
        "0-0-0-0-4": [-1, None, True, [], ""], #success
        "0-0-0-0-6": [-1, None, True, [], ""], #success

        "0-0-0-0-27": [-1, None, True, [], ""], #success

        "0-0-0-1-0": [-1, None, True, [], ""], #success
        "0-0-0-1-1": [-1, None, True, [], ""], #success
        "0-0-0-1-2": [-1, None, True, [], ""], #success
        "0-0-0-1-4": [-1, None, True, [], "order by cts, tag_tbname"],  #success
        "0-0-0-1-6": [-1, None, True, [], ""], #success

        
        "0-0-0-1-7": [-1, None, True, [], ""], #success
        "0-0-0-1-8": [-1, None, True, [], ""], #success
        "0-0-0-1-10": [-1, None, True, [], "order by cts, tag_tbname limit 150"],#success
        "0-0-0-1-12": [-1, None, True, [], ""], #success
        "0-0-0-1-13": [-1, None, True, [], "order by cts, tag_tbname limit 150"],#success,ans原先 sql 缺少 limit 150

        
        "0-0-0-1-14": [-1, None, True, [], ""], #success
        "0-0-0-1-16": [-1, None, True, [], "order by cts, tag_tbname limit 150"],#success,ans原先 sql 缺少 limit 150
        "0-0-0-1-18": [-1, None, True, [], "order by `cint`, tag_tbname"], #success


        "0-0-0-2-0": [-1, None, True, [], "order by cts, tag_tbname"],#success
        "0-0-0-2-1": [-1, None, True, [], ""], #success
        
        "0-0-0-2-2": [-1, None, True, [], "order by cts, tag_tbname limit 150"], #success
        "0-0-0-2-3": [-1, None, True, [], "order by cts, tag_tbname"], #success
        "0-0-0-2-4": [-1, None, True, [], "order by cts, tag_tbname"], #success
        "0-0-0-2-5": [-1, None, True, [], ""], #success
        "0-0-0-2-6": [-1, None, True, [], ""], #success 

        
        "0-0-0-2-7": [-1, None, True, [], ""], #success
        "0-0-0-2-8": [-1, None, True, [], ""], #success
        "0-0-0-2-9": [-1, None, True, [], ""], #success
        "0-0-0-2-10": [-1, None, True, [], ""],#success
        "0-0-0-2-11": [-1, None, True, [], ""],#success

        
        "0-0-0-2-12": [-1, None, True, [], ""],#success
        "0-0-0-2-13": [-1, None, True, [], ""],#success
        "0-0-0-2-14": [-1, None, True, [], ""],#success
        "0-0-0-2-15": [-1, None, True, [], ""],#success
        "0-0-0-2-16": [-1, None, True, [], ""],#success

        
        "0-0-0-2-17": [-1, None, True, [], ""],#success
        "0-0-0-2-18": [-1, None, True, [], ""], #success
        "0-0-0-2-23": [-1, None, True, [], ""], #success


        "0-0-0-3-0": [-1, None, True, [], "order by cts, tag_tbname"], #success
        "0-0-0-3-1": [-1, None, True, [], "order by cts, tag_tbname"], #success    
        "0-0-0-3-2": [-1, None, True, [], "order by cts, tag_tbname"], #success      

        "0-0-0-3-3": [-1, None, True, [], ""],#success 
        "0-0-0-3-4": [-1, None, True, [], ""], #success
        "0-0-0-3-5": [-1, None, True, [], ""], #success
        "0-0-0-3-6": [-1, None, True, [], ""],    #success     
        "0-0-0-3-7": [-1, None, True, [], ""],   #success   

        "0-0-0-4-2": [-1, None, True, [], ""],  #success    
        "0-0-0-4-41": [-1, None, True, [], ""],  #success  
        "0-0-0-4-6": [-1, None, True, [], ""],  #success

        "0-0-0-5-2": [-1, None, True, [], "order by cts, tag_tbname"],     #success
        "0-0-0-5-41": [-1, None, True, [], ""],     #success
        "0-0-0-5-6": [-1, None, True, [], ""],    #success

        "0-0-0-6-0": [-1, None, True, [], "order by cts, tag_tbname"],#success 
        
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
        
        
        "0-0-0-10-2": [-1, None, True, [], "limit 10"],#success
        "0-0-0-10-39": [-1, None, True, [], ""],#success
        "0-0-0-10-45": [-1, None, True, [], ""],#success
        #"0-0-0-10-42": [111, None, True, [], ""],#failed fillhistory还是加载不符合范围数据
        #"0-0-0-11-40": [-1, None, True, [], ""],#failed，fillhistory还是加载不符合范围数据
        "0-0-0-12-40": [-1, None, True, [], ""], #success
        "0-0-0-12-44": [-1, None, True, [], ""], #success
        #"0-0-0-13-40": [0, None, True, [], ""], #success，只通知不计算,需要单独用例
        #"0-0-0-14-40": [0, None, True, [], ""],#success，只通知不计算,需要单独用例
        "0-0-0-15-37": [-1, None, True, [], ""],#failed, sum结果不对
        "0-0-0-15-38": [-1, None, True, [], ""],#success
        "0-0-0-15-43": [-1, None, True, [], ""],#success
        
        "1-1-0-0-0": [-1, None, True, [], ""],  # 触发表子表、计算表子表 #success
        "1-1-0-0-26": [-1, None, True, [], ""],#success
        "1-1-0-0-35": [-1, None, True, [], ""], #success
        "1-1-0-0-36": [-1, None, True, [], ""], #success
        "1-1-0-0-37": [-1, None, True, [], ""], #success 
        "1-1-0-0-38": [-1, None, True, [], ""],  #success
    }
    # pytest不允许自定义__init__，如有初始化需求请用setup_class或setup_method

    @classmethod
    def setup_class(cls):
        tdLog.debug("start to execute %s", __file__)
        # 初始化 createStreamSqls
        cls.createStreamSqls = [ #(SQL, (minPartitionColNum, partitionByTbname))
            ("create stream stName session(cts,{session}s) from {trigTbname} stream_options(fill_history('2025-01-01 00:00:00')) into outTbname as querySql;", (0, False)),#0
            ("create stream stName session(cts,{session}s) from {trigTbname} partition by tbname stream_options(fill_history('2025-01-01 00:00:00')) into outTbname as querySql;", (1, True)),#1
            ("create stream stName session(cts,{session}s) from {trigTbname} partition by tint, tbname stream_options(fill_history('2025-01-01 00:00:00')) into outTbname as querySql;", (2, True)),#2
            ("create stream stName session(cts,{session}s) from {trigTbname} partition by tvarchar, tbname, tint stream_options(fill_history('2025-01-01 00:00:00')) into outTbname as querySql;", (3, True)),#3
            ("create stream stName session(cts,{session}s) from {trigTbname} stream_options(fill_history('2025-01-01 00:00:00')) into outTbname as querySql;", (0, False)),#4
            ("create stream stName session(cts,{session}s) from {trigTbname} partition by tbname stream_options(fill_history('2025-01-01 00:00:00')) into outTbname as querySql;", (1, True)),#5
            ("create stream stName session(cts,{session}s) from {trigTbname} partition by tbname stream_options(fill_history('2025-01-01 00:00:00')) into outTbname as querySql;", (1, True)),#6
            ("create stream stName session(cts,{session}+1s)  from {trigTbname} stream_options(fill_history('2025-01-01 00:00:00')) into outTbname as querySql;", (0, False)),#7
            ("create stream stName session(cts,{session}+30s)  from {trigTbname} partition by tbname stream_options(fill_history('2025-01-01 00:00:00')) into outTbname as querySql;", (1, True)),#8
            ("create stream stName session(cts,{session}s) from {trigTbname} stream_options(watermark(10s)|expired_time(40s)|ignore_disorder|delete_recalc|delete_output_table) into outTbname as querySql;", (3, True)),#9
            ("create stream stName session(cts,{session}s) from {trigTbname} partition by tbname stream_options(delete_recalc|fill_history('2025-01-01 00:01:30')) into outTbname as querySql ;", (1, True)),#10
            ("create stream stName session(cts,{session}s) from {trigTbname} partition by tbname stream_options(watermark(3s)|fill_history('2025-01-01 00:01:30')) into outTbname as querySql;", (1, True)),#11
            ("create stream stName session(cts,{session}s) from {trigTbname} partition by tbname stream_options(pre_filter(cint>2)|fill_history('2025-01-01 00:00:00')) into outTbname as querySql;", (1, True)),#12
            ("create stream stName session(cts,{session}s) from {trigTbname} partition by tbname stream_options(calc_notify_only|fill_history('2025-01-01 00:00:00')) into outTbname as querySql;", (1, True)),#13
            ('create stream stName session(cts,{session}s) from {trigTbname} partition by tbname stream_options(calc_notify_only|fill_history("2025-01-01 00:00:00")) notify("ws://localhost:8080/notify") into outTbname as querySql;', (1, True)),#14
            ("create stream stName session(cts,{session}s) from {trigTbname} partition by tbname,tint stream_options(pre_filter(tint>0)|fill_history('2025-01-01 00:00:00')) into outTbname as querySql;", (1, True)),#15
            ("create stream stName session(cts,{session}s) from {trigTbname} partition by tbname stream_options(pre_filter(cint is null)|fill_history('2025-01-01 00:00:00')) into outTbname as querySql;", (1, True)),#16
        ]
            
    def log_variable(self,var, log_file_path):
        with open(log_file_path, 'a') as f:
            f.write(f"\n[{datetime.datetime.now()}] Variable value: {repr(var)}\n")
            
    def check_result_with_result_file(self, dbname, outTbname, resultIdx, queryResult, caseName, currentDir):
        chkSql = f"select * from {dbname}.{outTbname} {queryResult[4]}"
        tdLog.info(f"check result with sql: {chkSql}")
        if queryResult[0] < 0:
            tdCom.generate_query_result_file(caseName, resultIdx, chkSql)
        else:
            tdCom.compare_query_with_result_file(resultIdx, chkSql, f"{currentDir}/ans/{caseName}.{resultIdx}.csv", caseName)
            tdLog.info("check result with result file succeed")

    def check_result_with_expected_list(self, dbname, outTbname, queryResult):
        chkSql = f"select * from {dbname}.{outTbname} {queryResult[4]}"
        tdLog.info(f"check result with sql: {chkSql}")
        tdSql.query(chkSql, queryTimes=1)
        total_rows = tdSql.getRows()
        for row in queryResult[3]:
            for rowIdx, rowValue in row.items():
                if rowIdx >= total_rows:
                    raise AssertionError(f"Expected row index {rowIdx} but only {total_rows} rows returned")
                rowData = tdSql.getRowData(rowIdx)
                assert rowData == rowValue, f"Expected value {rowValue} does not match actual value {rowData} for row index {rowIdx}"
        tdLog.info("check result with expected list succeed")

    def run_test_case(self, trigTbIdx, calcTbIdx, slidIdx, createStmIdx, queryIdx):
        resultIdx = f"{trigTbIdx}-{calcTbIdx}-{slidIdx}-{createStmIdx}-{queryIdx}"
        tdLog.info(f"Starting test case {resultIdx}")
        try:
            trigTbname = self.tableList[trigTbIdx]
            calcTbname = self.tableList[calcTbIdx]
            session = self.sessionList[slidIdx]
            stName = f"`s{resultIdx}`"
            outTbname = f"`s{resultIdx}_out`"
            queryResult = self.queryResults.get(resultIdx, [-1, None, False, [], ""])

            querySql = self.querySqls[queryIdx][0].replace("{calcTbname}", calcTbname)
            streamSql = self.createStreamSqls[createStmIdx][0].replace("querySql", querySql).replace("stName", stName).replace("outTbname", outTbname).replace("{session}", str(session)).replace("{trigTbname}", trigTbname)

            stream1 = StreamItem(
                id=resultIdx,
                stream=streamSql,
                res_query=f"select * from {self.dbname}.{outTbname};",
                check_func=queryResult[1],
            )
            stream1.createStream()
            
            print(f"===========Stream created with SQL: {streamSql}")
            stream1.awaitStreamRunning()
            print(f"Stream {resultIdx} is running, waiting for data...")
            # time.sleep(6)  # 等待流初始化
            if  self.runAll:
                stream1.awaitRowStability(queryResult[0])
                if stream1.check_func is not None:
                    stream1.check_func()
                elif queryResult[2]:
                    self.check_result_with_result_file(self.dbname, outTbname, resultIdx, queryResult, self.caseName, self.currentDir)
                else:
                    self.check_result_with_expected_list(self.dbname, outTbname, queryResult)
            tdLog.info(f"Finished test case {resultIdx}")
        except Exception as e:
            tdLog.error(f"Test case {resultIdx} failed: {str(e)}")
            raise


        
    def test_stream_session_trigger(self):
        """Stream session trigger

        xx

        Catalog:
            - Streams:TriggerMode

        Since: v3.3.3.7

        Labels: common,ci,skip

        Jira: None

        History:
            - 2025-6-18 dapan Created
        """
        tdStream.createSnode()
        self.prepareData()

        all_cases = []
        for trigTbIdx in range(len(self.tableList)):
            for calcTbIdx in range(len(self.tableList)):
                for slidIdx in range(len(self.sessionList)):
                    for createStmIdx in range(len(self.createStreamSqls)):
                        for queryIdx in range(len(self.querySqls)):
                            if self.createStreamSqls[createStmIdx][1][0] < self.querySqls[queryIdx][1][0] or \
                               (not self.createStreamSqls[createStmIdx][1][1] and self.querySqls[queryIdx][1][1]):
                                continue
                            resultIdx = f"{trigTbIdx}-{calcTbIdx}-{slidIdx}-{createStmIdx}-{queryIdx}"
                            if not self.runAll and resultIdx not in self.runCaseList:
                                continue
                            if not self.runAll and resultIdx not in self.queryResults:
                                continue
                            all_cases.append((trigTbIdx, calcTbIdx, slidIdx, createStmIdx, queryIdx))

        # 串行创建、运行、比对所有用例，彻底避免多线程内存问题
        for trigTbIdx, calcTbIdx, slidIdx, createStmIdx, queryIdx in all_cases:
            resultIdx = f"{trigTbIdx}-{calcTbIdx}-{slidIdx}-{createStmIdx}-{queryIdx}"
            try:
                trigTbname = self.tableList[trigTbIdx]
                calcTbname = self.tableList[calcTbIdx]
                session = self.sessionList[slidIdx]
                stName = f"`s{resultIdx}`"
                outTbname = f"`s{resultIdx}_out`"
                queryResult = self.queryResults.get(resultIdx, [-1, None, False, [], ""])
                querySql = self.querySqls[queryIdx][0].replace("{calcTbname}", calcTbname)
                streamSql = self.createStreamSqls[createStmIdx][0].replace("querySql", querySql).replace("stName", stName).replace("outTbname", outTbname).replace("{session}", str(session)).replace("{trigTbname}", trigTbname)
                stream1 = StreamItem(
                    id=resultIdx,
                    stream=streamSql,
                    res_query=f"select * from {self.dbname}.{outTbname};",
                    check_func=queryResult[1],
                )
                stream1.createStream()
                # print(f"===========Stream created with SQL: {streamSql}")
                self.log_variable(streamSql, f"{self.caseName}.log")
                stream1.awaitStreamRunning()
                print(f"Stream {resultIdx} is running, waiting for data...")
                if not self.runAll:
                    stream1.awaitRowStability(queryResult[0])
                    print('Stream row stability achieved, checking results............')
                    if stream1.check_func is not None:
                        stream1.check_func()
                    elif queryResult[2]:
                        print("checking result with result file............")
                        self.check_result_with_result_file(self.dbname, outTbname, resultIdx, queryResult, self.caseName, self.currentDir)
                    else:
                        self.check_result_with_expected_list(self.dbname, outTbname, queryResult)
                tdLog.info(f"==============Finished test case {resultIdx}======================")
            except Exception as e:
                tdLog.error(f"Test case execution failed: {str(e)}")
        tdLog.info("All test cases finished.")

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

    def checkBaic2Results(self):
        tdSql.query("select * from test.st7;", queryTimes=1)
        ts = tdSql.getColData(0)
        avg_cint = tdSql.getColData(1)
        count_cint = tdSql.getColData(2)
        for i in range(0, 40):
            sql = f"select '{ts[i]}', avg(cint), count(cint) from test.st where cts <= '{ts[i]}'"
            tdSql.query(sql, queryTimes=1)
            expected_ts = datetime.datetime.strptime(tdSql.getData(0, 0), "%Y-%m-%d %H:%M:%S")
            expected_avg_cint = tdSql.getData(0, 1)
            expected_count_cint = tdSql.getData(0, 2)
            assert ts[i] == expected_ts, f"Row {i} ts mismatch: expected {expected_ts}, got {ts[i]}"
            assert math.isclose(avg_cint[i], expected_avg_cint, rel_tol=1e-9), f"Row {i} avg_cint mismatch: expected {expected_avg_cint}, got {avg_cint[i]}"
            assert count_cint[i] == expected_count_cint, f"Row {i} count_cint mismatch: expected {expected_count_cint}, got {count_cint[i]}"