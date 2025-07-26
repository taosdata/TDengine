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
        """Stream basic test 1

        1. test stream basic

        Catalog:
            - Streams:OldStreamCases

        Since: v3.0.0.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-7-21 lvze Migrated from community/tests/system-test/8-stream/stream_basic.py

        """
        tdSql.execute("create snode on dnode 1")
        self.case1()
        
                    

    def case1(self):

        tdSql.execute(f'create database if not exists d1 vgroups 1')
        tdSql.execute(f'use d1')
        tdSql.execute(f'create table st(ts timestamp, i int) tags(t int)')
        tdSql.execute(f'insert into t1 using st tags(1) values(now, 1) (now+1s, 2)')
        tdSql.execute(f'insert into t2 using st tags(2) values(now, 1) (now+1s, 2)')
        tdSql.execute(f'insert into t3 using st tags(3) values(now, 1) (now+1s, 2)')

        tdSql.execute("""create stream d1.stream1 interval(1m) sliding(1m) from d1.st partition by tbname
                    stream_options(fill_history)
                    into d1.sta output_subtable(concat('nee.w-', tbname)) 
                    tags(tname varchar(100) as tbname)
                    as select
                        _twstart,
                        count(*),
                        avg(i)
                    from
                        %%tbname;""", queryTimes=2,show=True)

        self.checkStreamRunning()

        sql= "select * from sta"
        tdSql.checkRowsLoop(3, sql, loopCount=100, waitTime=0.5)
        tdSql.query("select tbname from sta order by tbname")
        if '.' in tdSql.getData(0,0):
            raise Exception("ERROR :table name have '.'")
        if not tdSql.getData(0, 0).startswith('nee.w-t1'):
            tdLog.exit("error1")
        

        if not tdSql.getData(1, 0).startswith('nee.w-t2'):
            tdLog.exit("error2")

        if not tdSql.getData(2, 0).startswith('nee.w-t3'):
            tdLog.exit("error3")

    
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


