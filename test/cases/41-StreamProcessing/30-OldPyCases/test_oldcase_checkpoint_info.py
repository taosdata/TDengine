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
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream, streamUtil,StreamTableType, StreamTable, cluster
from random import randint
import os
import subprocess
import glob
# should be used by -N  option
class Test_checkpoint_info_Case:
    updatecfgDict = {'debugFlag': 135, 'asynclog': 0, 'checkpointinterval':60}

    caseName = "test_checkpoint_info"
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
    tblRowNum = 10
    tableList = []
    
    def setup_class(cls):
        tdLog.info(f"start to excute {__file__}")
    
    
    def test_checkpoint_info(self):
        """OldPy: checkpoint

        1. create snode
        2. create stream and restart stream
        3. drop snode 
        4. alter db replica
        5. balance vgropu leader 
        6. drop stream
        7. redistribute vgroup
        8. drop snode
        9. drop dnode  
        10. check checkpoint file

        Catalog:
            - Streams:OldPyCases

        Since: v3.0.0.0

        Labels: common, ci

        Jira: None

        History:
            - 2025-7-21 lvze Migrated from community/tests/system-test/8-stream/checkpoint_info.py -N 4
            - In the new version of Stream, the inforamtion_schema db cannot contain checkpoint related information

        """
        tdStream.dropAllStreamsAndDbs()
        
        
        self.prepareData()
        self.createSnodeTest()
        self.createStream()
        self.checkStreamRunning()
        self.StopStream()
        self.StartStream()
        self.checkCheckpointFile()
        self.dropOneSnodeTest()
        self.checkStreamRunning()
        self.checkCheckpointFile()
        tdLog.info(f"alter db replica:")
        tdSql.execute(f"alter database {self.dbname} replica 3 ")
        
        while True:
            tdSql.query(f"select count(*) from information_schema.ins_vgroups where v1_status in ('leader','follower') and v2_status in ('leader','follower') and v3_status in ('leader','follower');")
            if tdSql.getData(0,0) == 1:
                tdLog.info("complete  alter replica ")
                break
            tdLog.info(f"wait alter replica...")
            time.sleep(1)
            
        # self.checkCheckpointFile()
        tdLog.info(f"balance vgroup leader:")
        tdSql.execute("balance vgroup leader",queryTimes=2)
        self.killOneDnode2()
        self.checkCheckpointFile()
        # self.checkCheckpointFile()
        self.dropOneStream()
        self.checkCheckpointFile()
        tdLog.info("redistributed vgroup:")
        tdSql.redistribute_one_vgroup(db_name={self.dbname},replica=3,vgroup_id=2,useful_trans_dnodes_list=[2,3,4])
        # self.checkCheckpointFile()
        tdLog.info("drop one dnode:")
        self.dropOneDnode()
        self.StopStream()
        self.StartStream()
        self.StopStream()
        self.StartStream()
        self.checkCheckpointFile()
        

    def prepareData(self):
        tdLog.info(f"prepare data")

        tdStream.dropAllStreamsAndDbs()
        #wait all dnode ready
        time.sleep(2)
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

    def checkResultRows(self, expectedRows):
        tdSql.checkResultsByFunc(
            f"select * from information_schema.ins_snodes order by id;",
            lambda: tdSql.getRows() == expectedRows,
            delay=0.5, retry=2
        )
    
    def createSnodeTest(self):
        tdLog.info(f"create snode test")
        tdSql.query("select * from information_schema.ins_dnodes order by id;")
        numOfNodes=tdSql.getRows()
        tdLog.info(f"numOfNodes: {numOfNodes}")
        
        for i in range(1, numOfNodes + 1):
            tdSql.execute(f"create snode on dnode {i}")
            tdLog.info(f"create snode on dnode {i} success")
        self.checkResultRows(numOfNodes)
        
        tdSql.checkResultsByFunc(
            f"show snodes;", 
            lambda: tdSql.getRows() == numOfNodes,
            delay=0.5, retry=2
        )

    def dropAllSnodeTest(self):
        tdLog.info(f"drop all snode test")
        tdSql.query("select * from information_schema.ins_snodes order by id;")
        numOfSnodes = tdSql.getRows()
        tdLog.info(f"numOfSnodes: {numOfSnodes}")
        
        for i in range(1, numOfSnodes ):
            tdSql.execute(f"drop snode on dnode {i}")
            tdLog.info(f"drop snode {i} success")
            
        self.checkResultRows(1)
        
        tdSql.checkResultsByFunc(
            f"show snodes;", 
            lambda: tdSql.getRows() == 1,
            delay=0.5, retry=2
        )
        
        numOfRows=tdSql.execute(f"drop snode on dnode {numOfSnodes}")
        if numOfRows != 0:
            raise Exception(f" drop all snodes failed! ")
        tdSql.query("select * from information_schema.ins_snodes order by id;")
        numOfSnodes = tdSql.getRows()
        tdLog.info(f"After drop all snodes numOfSnodes: {numOfSnodes}")

    def dropOneSnodeTest(self):
        tdSql.query("select * from information_schema.ins_snodes  order by id;")
        numOfSnodes=tdSql.getRows()
        #只有一个 snode 的时候不再执行删除
        if numOfSnodes >1:
            tdLog.info(f"drop one snode test")
            tdSql.query("select * from information_schema.ins_streams order by stream_name;")
            snodeid = tdSql.getData(0,6)
            tdSql.execute(f"drop snode on dnode {snodeid}")
            tdLog.info(f"drop snode {snodeid} success")
            #drop snode后流状态有延迟，需要等待才能看到 failed 状态出现
            # time.sleep(15)
            
        
    def createStream(self):
        tdLog.info(f"create stream ")
        tdSql.query("select * from information_schema.ins_dnodes order by id;")
        numOfNodes=tdSql.getRows()
        for i in range(1,numOfNodes+1):
            tdSql.execute(f"create stream `s{i}` sliding(1s) from st1 stream_options(fill_history('2025-01-01 00:00:00')) into `s{i}out` as select cts, cint from st1 where _tcurrent_ts % 2 = 0 order by cts;")
            tdLog.info(f"create stream s{i} success!")
        
    def createOneStream(self):
        sql = (
        "create stream `s99` sliding(1s) from st1  partition by tbname "
        "stream_options(fill_history('2025-01-01 00:00:00')) "
        "into `s99out` as "
        "select cts, cint, %%tbname from st1 "
        "where cint > 5 and tint > 0 and %%tbname like '%%2' "
        "order by cts;"
        )

        tdSql.execute(sql)
        tdLog.info(f"create stream s99 success!")
        
    def dropOneStream(self):
        tdLog.info(f"drop one stream: ")
        tdSql.query("select * from information_schema.ins_streams order by stream_name;")
        numOfStreams = tdSql.getRows()
        tdLog.info(f"Total streams:{numOfStreams}")
        streamid = tdSql.getData(0,0)
        tdSql.execute(f"drop stream {streamid}")
        tdLog.info(f"drop stream {streamid} success")
        
        tdSql.query("select * from information_schema.ins_streams order by stream_name;")
        numOfStreams = tdSql.getRows()
        tdLog.info(f"Total streams:{numOfStreams}")
    
    def dropOneDnode(self):
        tdSql.query("select * from information_schema.ins_dnodes order by id;")
        numOfDnodes = tdSql.getRows()
        tdLog.info(f"Total dnodes:{numOfDnodes}")
        
        tdSql.query(f"select `replica` from information_schema.ins_databases where name='{self.dbname}'")
        numOfReplica = tdSql.getData(0,0)
        
        if numOfDnodes ==3 and numOfReplica == 3:
            tdLog.info(f"Total dndoes: 3,replica:3, can not drop dnode.")
            return
        if numOfDnodes >2:
            tdLog.info(f"drop one dnode: ")
            tdSql.query("select * from information_schema.ins_dnodes order by id;")
            dnodeid = tdSql.getData(2,0)
            tdSql.execute(f"drop dnode {dnodeid}")
            tdLog.info(f"drop dnode {dnodeid} success")
        
            tdSql.query("select * from information_schema.ins_dnodes order by id;")
            numOfDnodes = tdSql.getRows()
            tdLog.info(f"Total dnodes:{numOfDnodes}")
            # time.sleep(3)
            
    def killOneDnode(self):
        tdSql.query("select * from information_schema.ins_dnodes order by id;")
        numOfDnodes = tdSql.getRows()
        if numOfDnodes >2:
            tdLog.info(f"kill one dnode: ")
            cmd = (
            f"ps -ef | grep -wi taosd | grep 'dnode{numOfDnodes}/cfg' "
            "| grep -v grep | awk '{print $2}' | xargs kill -9 > /dev/null 2>&1"
            )

            subprocess.run(cmd, shell=True)
            tdLog.info(f"kill dndoe {numOfDnodes} success")
            #kill dnode后流状态有延迟，需要等待才能看到 failed 状态出现
            time.sleep(15)
    
    def killOneDnode2(self):
        tdSql.query("select * from information_schema.ins_dnodes order by id;")
        numOfDnodes = tdSql.getRows()
        if numOfDnodes >2:
            tdLog.info(f"kill one dnode {numOfDnodes-1}: ")
            tdDnodes=cluster.dnodes
            tdDnodes[numOfDnodes-1].stoptaosd()
            tdLog.info(f"restart dnode {numOfDnodes-1}taosd:")
            tdDnodes[numOfDnodes-1].starttaosd()
    
    
    def checkStreamRunning(self):
        tdLog.info(f"check stream running status:")

        timeout = 120
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
                
    def checkCheckpointFile(self):
        time.sleep(3) #wait file to create
        files = glob.glob("../../sim/dnode*/data/snode/checkpoint/*ck")
        tdLog.info(f"checkpointfile list:{files}")
        tdLog.info(f"checkpoint file is  {len(files)} ")
        tdSql.query(f"show {self.dbname}.streams")
        if len(files) < tdSql.getRows() * 2 or len(files) == tdSql.getRows() * 2 -1:
            tdLog.info(f"ERROR: checkpoint file number is not right")
        else:
            tdLog.info(f"checkpoint files is ok")
            
            
    def StopStream(self):
        tdLog.info(f"stop stream:")
        tdSql.query(f"show {self.dbname}.streams;")
        streamname = tdSql.getColData(0)
        for i in streamname:
            tdLog.info(f"stop stream {i}")
            tdSql.execute(f"stop stream {i}")
        
        tdSql.query(f"show {self.dbname}.streams;")
        stateStream = tdSql.getData(0,1)
        for i in stateStream:
            if stateStream != 'Stopped' :
                raise Exception(f"Stop stream error")
        tdLog.info(f"stop all stream  success")
                
    def StartStream(self):
        tdLog.info(f"start stream:")
        tdSql.query(f"show {self.dbname}.streams;")
        streamname = tdSql.getColData(0)
        for i in streamname:
            tdLog.info(f"start stream {i}")
            tdSql.execute(f"start stream {i}")
            
        self.checkStreamRunning()
        tdSql.query(f"show {self.dbname}.streams;")
        stateStream = tdSql.getData(0,1)
        for i in stateStream:
            if stateStream != 'Running' :
                raise Exception(f"Start stream error")
        
        tdLog.info(f"start all stream  success")
                
