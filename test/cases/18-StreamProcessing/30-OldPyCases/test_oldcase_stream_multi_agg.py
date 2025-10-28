###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-


import time
from new_test_framework.utils import tdLog, tdSql, tdStream, streamUtil,StreamTableType, StreamTable, cluster
from random import randint
import os



class TestStreamMultiAggCase:

    caseName = "test_Stream_Multi_Agg"
    currentDir = os.path.dirname(os.path.abspath(__file__))
    runAll = False
    dbname = "test"
    trigTbname = ""
    calcTbname = ""
    outTbname = ""
    stName = ""
    resultIdx = ""
    sliding = 1
    subTblNum = 3
    tblRowNum = 10
    tableList = []

    def setup_class(cls):
        tdLog.info(f"start to excute {__file__}")
    
    def test_steram_multi_agg(self):
        """OldPy: aggregation func

        1. test_Stream_Multi_Agg

        Catalog:
            - Streams:OldPyCases

        Since: v3.0.0.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-7-22 lvze Migrated from community/tests/system-test/8-stream/stream_multi_agg.py

        """
        tdSql.execute("create snode on dnode 1;")
        self.case1()
        self.case2()

    

    def case1(self):
        tdLog.info ("========case1 start========")

        os.system(" taosBenchmark -y -B 1 -t 10 -S 1000 -n 10 -i 1000 -v 5 ")
        time.sleep(10)
        tdSql.execute("use test", queryTimes=3)
        tdSql.query(
        """create stream test.s1 interval(2s) sliding(2s) from test.meters 
                    partition by groupid
                    stream_options(fill_history| low_latency_calc|expired_time(10s))
                    into test.st1 
                    tags(
                    groupid int as groupid)
                    as select
                        _twstart ts,
                        sum(voltage) voltage
                    from
                        %%trows;
        """,queryTimes=2,show=True)
        self.checkStreamRunning()

        tdLog.info ("========create stream and insert data ok========")
        tdSql.query("select _wstart,sum(voltage),groupid from test.meters partition by groupid interval(2s) order by groupid,_wstart")
        rowCnt = tdSql.getRows()
        results_meters = tdSql.queryResult

        sql = "select  ts,voltage,groupid from test.st1 order by groupid,ts"
        tdSql.checkRowsLoop(rowCnt, sql, loopCount=100, waitTime=0.5)

        tdSql.query(sql)
        results_st1 = tdSql.queryResult
        for i in range(rowCnt):
            data1 = results_st1[i]
            data2 = results_meters[i]
            if data1 != data2:
                tdLog.info(f"num: {i}, act data: {data1}, expect data: {data2}")
                tdLog.exit("check data error!")

        tdLog.info ("case1 end")

    def case2(self):
        tdLog.info ("========case2 start========")

        os.system("taosBenchmark -d db -t 20 -v 6 -n 1000 -y")
        # create stream
        tdSql.execute("use db", queryTimes=3)
        tdSql.execute(
        """create stream db.stream1 interval(10a) sliding(10a) from db.meters 
                    stream_options(fill_history)
                    into db.st2
                    as select
                        _twstart ts,
                        count(*) cnt
                    from
                        %%trows;
        """,queryTimes=2,show=True)
        self.checkStreamRunning()
        # loop wait max 60s to check count is ok
        tdLog.info("loop wait result ...")
        
        while True:
            tdSql.query(f"select count(*) from information_schema.ins_tables where table_name='st2';")
            if tdSql.getData(0,0) == 1:
                tdLog.info("stream result table is ok ")
                break
            tdLog.info(f"wait stream out table ...")
            time.sleep(1)
            
        sql = "select count(*) from st2"
        tdSql.checkDataLoop(0, 0, 100, sql, loopCount=100, waitTime=0.5)

        # check all data is correct
        sql = "select * from db.st2 where cnt != 200;"
        tdSql.query(sql)
        tdSql.checkRows(0)

        # check ts interval is correct
        sql = "select * from ( select diff(ts) as tsdif from db.st2 ) where tsdif != 10;"
        tdSql.query(sql)
        tdSql.checkRows(0)
        tdLog.info ("case2 end")



    def checkStreamRunning(self):
        tdLog.info(f"check stream running status:")

        timeout = 60 
        start_time = time.time()

        while True:
            if time.time() - start_time > timeout:
                tdLog.error("Timeout waiting for all streams to be running.")
                tdLog.error(f"Final stream running status: {streamRunning}")
                raise TimeoutError(f"Stream status did not reach 'Running' within {timeout}s timeout.")
            
            tdSql.query(f"select status from information_schema.ins_streams order by stream_name;")
            streamRunning=tdSql.getColData(0)

            if all(status == "Running" for status in streamRunning):
                tdLog.info("All Stream running!")
                tdLog.info(f"stream running status: {streamRunning}")
                return
            else:
                tdLog.info("Stream not running! Wait stream running ...")
                tdLog.info(f"stream running status: {streamRunning}")
                time.sleep(1)