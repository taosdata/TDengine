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


from new_test_framework.utils import tdLog, tdSql, tdStream, streamUtil,StreamTableType, StreamTable, cluster


import random
import time
import traceback
import os
from   os import path
import psutil


class TestStreamBasicCase:
    caseName = "TestStreamBasicCase"
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
        
        
        
    # run
    def test_stream_basic(self):
        """OldPy: basic test 1

        1. test stream basic

        Catalog:
            - Streams:OldPyCases

        Since: v3.0.0.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-7-21 lvze Migrated from community/tests/system-test/8-stream/stream_basic.py
            - new stream outputtable name can not contain '.'
            - When there is too much history data, the stream calculation speed is too slow, especially when debugflag is 135.

        """
        tdSql.execute("create snode on dnode 1")
        self.case1()
        # gen data
        random.seed(int(time.time()))
        os.system(" taosBenchmark -d db -t 2 -v 2 -n 1000 -y")
        # create stream
        tdSql.execute("use db")
        # tdSql.execute("create stream stream3 fill_history 1 into sta as select count(*) as cnt from meters interval(10a);",show=True)
        tdSql.execute("""
                    create stream db.stream3 interval(10a) sliding(10a) from db.meters 
                    stream_options(fill_history)
                    into db.sta 
                    as select
                        _twstart ts, 
                        count(*) cnt
                    from
                        %%trows;
                    """,queryTimes=2,show=True)
        self.checkStreamRunning()
        sql = "select count(*) from sta"
        # loop wait max 60s to check count is ok
        tdLog.info("loop wait result ...")
        while True:
            tdSql.query(f"select count(*) from information_schema.ins_tables where table_name='sta';")
            if tdSql.getData(0,0) == 1:
                tdLog.info("stream result table is ok ")
                break
            tdLog.info(f"wait stream out table ...")
            time.sleep(1)

        tdSql.checkDataLoop(0, 0, 100, sql, loopCount=50, waitTime=0.5)

        # time.sleep(5)

        # check all data is correct
        sql = "select * from sta where cnt != 20;"
        tdSql.query(sql)
        tdSql.checkRows(0)

        # check ts interval is correct
        sql = "select * from ( select diff(ts) as tsdif from sta ) where tsdif != 10;"
        tdSql.query(sql)
        tdSql.checkRows(0)

        # self.caseDropStream()
                        


    def case1(self):
        tdSql.execute(f'create database if not exists d1 vgroups 1')
        tdSql.execute(f'use d1')
        tdSql.execute(f'create table st(ts timestamp, i int) tags(t int)')
        tdSql.execute(f'insert into t1 using st tags(1) values("2025-08-01 00:00:00", 1) ("2025-08-01 00:00:01", 2)')
        tdSql.execute(f'insert into t2 using st tags(2) values("2025-08-01 00:00:00", 1) ("2025-08-01 00:00:01", 2)')
        tdSql.execute(f'insert into t3 using st tags(3) values("2025-08-01 00:00:00", 1) ("2025-08-01 00:00:01", 2)')

        tdSql.execute("""create stream d1.stream1 interval(1m) sliding(1m) from d1.st partition by tbname
                    stream_options(fill_history)
                    into d1.sta output_subtable(concat('neew-', tbname)) 
                    tags(tname varchar(100) as tbname)
                    as select
                        _twstart,
                        count(*),
                        avg(i)
                    from
                        %%tbname;""", queryTimes=2,show=True)

        tdSql.execute("""
                    create stream d1.stream2 interval(1m) sliding(1m) from d1.st partition by tbname
                    stream_options(fill_history)
                    into d1.stb output_subtable(concat('new-', tbname)) 
                    tags(tname varchar(100) as tbname)
                    as select
                        _twstart,
                        count(*),
                        avg(i)
                    from
                        %%tbname;
                    """, queryTimes=2,show=True)
        self.checkStreamRunning()

        sql= "select * from sta"
        tdSql.checkRowsLoop(3, sql, loopCount=100, waitTime=0.5)
        tdSql.query("select tbname from sta order by tbname")
        if not tdSql.getData(0, 0).startswith('neew-t1'):
            tdLog.exit("error1")

        if not tdSql.getData(1, 0).startswith('neew-t2'):
            tdLog.exit("error2")

        if not tdSql.getData(2, 0).startswith('neew-t3'):
            tdLog.exit("error3")

        sql= "select * from stb"
        tdSql.checkRowsLoop(3, sql, loopCount=100, waitTime=0.5)
        tdSql.query("select tbname from stb order by tbname")
        if not tdSql.getData(0, 0).startswith('new-t1'):
            tdLog.exit("error4")

        if not tdSql.getData(1, 0).startswith('new-t2'):
            tdLog.exit("error5")

        if not tdSql.getData(2, 0).startswith('new-t3'):
            tdLog.exit("error6")

    def caseDropStream(self):
        tdLog.info(f"start caseDropStream")
        sql = "drop database if exists d1;"
        tdSql.query(sql)
        sql = "drop database if exists db;"
        tdSql.query(sql)

        sql ="show streams;"
        tdSql.query(sql)
        tdSql.check_rows_loop(0, sql, loopCount=100, waitTime=0.5)

        sql ="select * from information_schema.ins_stream_tasks;"
        tdSql.query(sql)
        tdSql.check_rows_loop(0, sql, loopCount=100, waitTime=0.5)

        self.taosBenchmark(" -d db -t 2 -v 4 -n 1000000 -y")
        # create stream
        tdSql.execute("use db;")
        tdSql.execute("create stream stream4 fill_history 1 into sta4 as select _wstart, sum(current),avg(current),last(current),min(voltage),first(voltage),last(phase),max(phase),count(phase), _wend, _wduration from meters partition by tbname, ts interval(10a);", show=True)
        
        time.sleep(10)

        sql ="select * from information_schema.ins_stream_tasks where status == 'ready';"
        tdSql.query(sql, show=True)
        tdSql.check_rows_loop(4, sql, loopCount=100, waitTime=0.5)

        pl = psutil.pids()
        for pid in pl:
            try:
                if psutil.Process(pid).name() == 'taosd':
                    taosdPid = pid
                    break
            except psutil.NoSuchProcess:
                pass
        tdLog.info("taosd pid:{}".format(taosdPid))
        p = psutil.Process(taosdPid)

        cpuInfo = p.cpu_percent(interval=5)
        tdLog.info("taosd cpu:{}".format(cpuInfo))

        tdSql.execute("drop stream stream4;", show=True)

        sql ="show streams;"
        tdSql.query(sql, show=True)
        tdSql.check_rows_loop(0, sql, loopCount=100, waitTime=0.5)

        sql ="select * from information_schema.ins_stream_tasks;"
        tdSql.query(sql, show=True)
        tdSql.check_rows_loop(0, sql, loopCount=100, waitTime=0.5)

        for i in range(10):
            cpuInfo = p.cpu_percent(interval=5)
            tdLog.info("taosd cpu:{}".format(cpuInfo))
            if cpuInfo < 10:
                return
            else:
                time.sleep(1)
                continue
        cpuInfo = p.cpu_percent(interval=5)
        tdLog.info("taosd cpu:{}".format(cpuInfo))
        if cpuInfo > 10:
            tdLog.exit("drop stream failed, stream tasks are still running")

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


