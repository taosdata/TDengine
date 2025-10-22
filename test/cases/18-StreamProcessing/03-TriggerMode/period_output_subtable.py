import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream, streamUtil,StreamTableType, StreamTable, cluster
from random import randint
import os
import subprocess

class TestPeriodOutputSubtable:
    currentDir = os.path.dirname(os.path.abspath(__file__))
    dbname = "test1"
    dbname2 = "test2"
    username1 = "lvze1"
    username2 = "lvze2"
    subTblNum = 3
    tblRowNum = 10
    tableList = []
    
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_snode_mgmt(self):
        """Stream TestPeriodOutputSubtable test
        
        1. test period output subtable 
        

        Catalog:
            - Streams:TestPeriodOutputSubtable

        Since: v3.3.3.7

        Labels: common,ci,skip

        Jira: None

        History:
            - 2025-7-8 lvze Created

        """

        tdStream.dropAllStreamsAndDbs()
        self.createSnodeTest()
        self.createTable()
        sql = (
            f"create stream {self.dbname}.s16 period(1s) from {self.dbname}.stba partition by tbname stream_options(pre_filter(cint>90)) " 
            f"into {self.dbname}.s16_out output_subtable(concat(tbname,'xxxx')) tags(yyyy varchar(100) comment 'table name1' as 'cint+10') " 
            "as  select ts,max(cint),i1,  %%tbname from %%trows  order by ts;"
        )
        
        #create stream
        tdSql.execute(sql)
        self.checkStreamRunning()
        tdSql.execute("insert into test1.a0 values(now,now,200,300);")
        tdSql.execute("insert into test1.a1 values(now,now,200,300);")
        tdSql.execute("insert into test1.a2 values(now,now,500,300);")
        
      
        time.sleep(10)
        tdSql.query(f"select * from {self.dbname}.s16_out;")
        if tdSql.getRows() == 0:
            raise Exception(f"ERROR: stream no result data")
            

    def checkResultRows(self, expectedRows):
        tdSql.checkResultsByFunc(
            f"select * from information_schema.ins_users where name !='root';",
            lambda: tdSql.getRows() == expectedRows,
            delay=0.5, retry=2
        )
        if tdSql.getRows() != expectedRows:
            raise Exception("Error: checkResultRows failed, expected rows not match!")
    
    def createSnodeTest(self):
        tdLog.info(f"create snode test")
        tdSql.query("select * from information_schema.ins_dnodes order by id;")
        numOfNodes=tdSql.getRows()
        tdLog.info(f"numOfNodes: {numOfNodes}")
        
        for i in range(1, numOfNodes + 1):
            try:
                tdSql.execute(f"create snode on dnode {i}")
            except Exception as e:
                if "Insufficient privilege" in str(e):
                    tdLog.info(f"Insufficient privilege to create snode")
                else:
                    raise  Exception(f"create stream failed with error: {e}") 
            tdLog.info(f"create snode on dnode {i} success")

    
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


    def createTable(self):
        tdSql.execute(f'create database test1 vgroups 10 ;')
        tdSql.execute(f"use test1;")
        tdSql.execute(f"CREATE STABLE `stba` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `cts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `cint` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium', `i1` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium') TAGS (`tint` INT, `tdouble` DOUBLE, `tvar` VARCHAR(100), `tnchar` NCHAR(100), `tts` TIMESTAMP, `tbool` BOOL);")
        tdSql.execute(f"CREATE STABLE `stbb` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `cts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `cint` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium', `i1` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium') TAGS (`tint` INT, `tdouble` DOUBLE, `tvar` VARCHAR(100), `tnchar` NCHAR(100), `tts` TIMESTAMP, `tbool` BOOL);")
        tdSql.execute(f"CREATE STABLE `stbc` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `cts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `cint` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium', `i1` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium') TAGS (`tint` INT, `tdouble` DOUBLE, `tvar` VARCHAR(100), `tnchar` NCHAR(100), `tts` TIMESTAMP, `tbool` BOOL);")
        tdSql.execute(f"create table a0 using stba tags(1,1.1,'a0','测试a0','2025-01-01 00:00:01',1);")
        tdSql.execute(f"create table a1 using stba tags(NULL,2.1,'a1','测试a1','2025-01-01 00:00:02',0);")
        tdSql.execute(f"create table a2 using stba tags(2,3.1,'a2','测试a2','2025-01-01 00:00:03',1);")
        tdSql.execute(f"create table b0 using stbb tags(1,1.1,'a0','测试a0','2025-01-01 00:00:01',1);")
        tdSql.execute(f"create table b1 using stbb tags(NULL,2.1,'a1','测试a1','2025-01-01 00:00:02',0);")
        tdSql.execute(f"create table b2 using stbb tags(2,3.1,'a2','测试a2','2025-01-01 00:00:03',1);")
        tdSql.execute(f"create table c0 using stbc tags(1,1.1,'a0','测试a0','2025-01-01 00:00:01',1);")
        tdSql.execute(f"create table c1 using stbc tags(NULL,2.1,'a1','测试a1','2025-01-01 00:00:02',0);")
        tdSql.execute(f"create table c2 using stbc tags(2,3.1,'a2','测试a2','2025-01-01 00:00:03',1);")
        tdSql.execute(f"insert into a1 values('2025-01-01 00:00:00','2025-01-01 00:00:00',1,11);")
        tdSql.execute(f"insert into a1 values('2025-01-01 00:00:01','2025-01-01 00:00:00',2,12);")
        tdSql.execute(f"insert into a1 values('2025-01-01 00:00:02','2025-01-01 00:00:00',3,13);")
        tdSql.execute(f"insert into a1 values('2025-01-01 00:00:03','2025-01-01 00:00:00',4,14);")
        tdSql.execute(f"insert into a1 values('2025-01-01 00:00:04','2025-01-01 00:00:00',5,15);")
        tdSql.execute(f"insert into a1 values('2025-01-01 00:00:05','2025-01-01 00:00:00',6,16);")
        tdSql.execute(f"insert into a1 values('2025-01-01 00:00:06','2025-01-01 00:00:00',7,17);")
        tdSql.execute(f"insert into a1 values('2025-01-01 00:00:07','2025-01-01 00:00:00',8,18);")
        tdSql.execute(f"insert into a1 values('2025-01-01 00:00:08','2025-01-01 00:00:00',9,19);")
        tdSql.execute(f"insert into a1 values('2025-01-01 00:00:09','2025-01-01 00:00:00',10,20);")
        tdSql.execute(f"insert into a2 values('2025-01-01 00:00:00','2025-01-01 00:00:00',1,11);")
        tdSql.execute(f"insert into a2 values('2025-01-01 00:00:01','2025-01-01 00:00:00',2,12);")
        tdSql.execute(f"insert into a2 values('2025-01-01 00:00:02','2025-01-01 00:00:00',3,13);")
        tdSql.execute(f"insert into a2 values('2025-01-01 00:00:03','2025-01-01 00:00:00',4,14);")
        tdSql.execute(f"insert into a2 values('2025-01-01 00:00:04','2025-01-01 00:00:00',5,15);")
        tdSql.execute(f"insert into a2 values('2025-01-01 00:00:05','2025-01-01 00:00:00',6,16);")
        tdSql.execute(f"insert into a2 values('2025-01-01 00:00:06','2025-01-01 00:00:00',7,17);")
        tdSql.execute(f"insert into a2 values('2025-01-01 00:00:07','2025-01-01 00:00:00',8,18);")
        tdSql.execute(f"insert into a2 values('2025-01-01 00:00:08','2025-01-01 00:00:00',9,19);")
        tdSql.execute(f"insert into a2 values('2025-01-01 00:00:09','2025-01-01 00:00:00',10,20);")
        tdSql.execute(f"insert into a0 values('2025-01-01 00:00:00','2025-01-01 00:00:00',1,11);")
        tdSql.execute(f"insert into a0 values('2025-01-01 00:00:01','2025-01-01 00:00:00',2,12);")
        tdSql.execute(f"insert into a0 values('2025-01-01 00:00:02','2025-01-01 00:00:00',3,13);")
        tdSql.execute(f"insert into a0 values('2025-01-01 00:00:03','2025-01-01 00:00:00',4,14);")
        tdSql.execute(f"insert into a0 values('2025-01-01 00:00:04','2025-01-01 00:00:00',5,15);")
        tdSql.execute(f"insert into a0 values('2025-01-01 00:00:05','2025-01-01 00:00:00',6,16);")
        tdSql.execute(f"insert into a0 values('2025-01-01 00:00:06','2025-01-01 00:00:00',7,17);")
        tdSql.execute(f"insert into a0 values('2025-01-01 00:00:07','2025-01-01 00:00:00',8,18);")
        tdSql.execute(f"insert into a0 values('2025-01-01 00:00:08','2025-01-01 00:00:00',9,19);")
        tdSql.execute(f"insert into a0 values('2025-01-01 00:00:09','2025-01-01 00:00:00',10,20);")
        tdSql.execute(f"insert into b1 values('2025-01-01 00:00:00','2025-01-01 00:00:00',1,11);")
        tdSql.execute(f"insert into b1 values('2025-01-01 00:00:01','2025-01-01 00:00:00',2,12);")
        tdSql.execute(f"insert into b1 values('2025-01-01 00:00:02','2025-01-01 00:00:00',3,13);")
        tdSql.execute(f"insert into b1 values('2025-01-01 00:00:03','2025-01-01 00:00:00',4,14);")
        tdSql.execute(f"insert into b1 values('2025-01-01 00:00:04','2025-01-01 00:00:00',5,15);")
        tdSql.execute(f"insert into b1 values('2025-01-01 00:00:05','2025-01-01 00:00:00',6,16);")
        tdSql.execute(f"insert into b1 values('2025-01-01 00:00:06','2025-01-01 00:00:00',7,17);")
        tdSql.execute(f"insert into b1 values('2025-01-01 00:00:07','2025-01-01 00:00:00',8,18);")
        tdSql.execute(f"insert into b1 values('2025-01-01 00:00:08','2025-01-01 00:00:00',9,19);")
        tdSql.execute(f"insert into b1 values('2025-01-01 00:00:09','2025-01-01 00:00:00',10,20);")
        tdSql.execute(f"insert into b2 values('2025-01-01 00:00:00','2025-01-01 00:00:00',1,11);")
        tdSql.execute(f"insert into b2 values('2025-01-01 00:00:01','2025-01-01 00:00:00',2,12);")
        tdSql.execute(f"insert into b2 values('2025-01-01 00:00:02','2025-01-01 00:00:00',3,13);")
        tdSql.execute(f"insert into b2 values('2025-01-01 00:00:03','2025-01-01 00:00:00',4,14);")
        tdSql.execute(f"insert into b2 values('2025-01-01 00:00:04','2025-01-01 00:00:00',5,15);")
        tdSql.execute(f"insert into b2 values('2025-01-01 00:00:05','2025-01-01 00:00:00',6,16);")
        tdSql.execute(f"insert into b2 values('2025-01-01 00:00:06','2025-01-01 00:00:00',7,17);")
        tdSql.execute(f"insert into b2 values('2025-01-01 00:00:07','2025-01-01 00:00:00',8,18);")
        tdSql.execute(f"insert into b2 values('2025-01-01 00:00:08','2025-01-01 00:00:00',9,19);")
        tdSql.execute(f"insert into b2 values('2025-01-01 00:00:09','2025-01-01 00:00:00',10,20);")
        tdSql.execute(f"insert into b0 values('2025-01-01 00:00:00','2025-01-01 00:00:00',1,11);")
        tdSql.execute(f"insert into b0 values('2025-01-01 00:00:01','2025-01-01 00:00:00',2,12);")
        tdSql.execute(f"insert into b0 values('2025-01-01 00:00:02','2025-01-01 00:00:00',3,13);")
        tdSql.execute(f"insert into b0 values('2025-01-01 00:00:03','2025-01-01 00:00:00',4,14);")
        tdSql.execute(f"insert into b0 values('2025-01-01 00:00:04','2025-01-01 00:00:00',5,15);")
        tdSql.execute(f"insert into b0 values('2025-01-01 00:00:05','2025-01-01 00:00:00',6,16);")
        tdSql.execute(f"insert into b0 values('2025-01-01 00:00:06','2025-01-01 00:00:00',7,17);")
        tdSql.execute(f"insert into b0 values('2025-01-01 00:00:07','2025-01-01 00:00:00',8,18);")
        tdSql.execute(f"insert into b0 values('2025-01-01 00:00:08','2025-01-01 00:00:00',9,19);")
        tdSql.execute(f"insert into b0 values('2025-01-01 00:00:09','2025-01-01 00:00:00',10,20);")
        tdSql.execute(f"insert into c1 values('2025-01-01 00:00:00','2025-01-01 00:00:00',1,11);")
        tdSql.execute(f"insert into c1 values('2025-01-01 00:00:01','2025-01-01 00:00:00',2,12);")
        tdSql.execute(f"insert into c1 values('2025-01-01 00:00:02','2025-01-01 00:00:00',3,13);")
        tdSql.execute(f"insert into c1 values('2025-01-01 00:00:03','2025-01-01 00:00:00',4,14);")
        tdSql.execute(f"insert into c1 values('2025-01-01 00:00:04','2025-01-01 00:00:00',5,15);")
        tdSql.execute(f"insert into c1 values('2025-01-01 00:00:05','2025-01-01 00:00:00',6,16);")
        tdSql.execute(f"insert into c1 values('2025-01-01 00:00:06','2025-01-01 00:00:00',7,17);")
        tdSql.execute(f"insert into c1 values('2025-01-01 00:00:07','2025-01-01 00:00:00',8,18);")
        tdSql.execute(f"insert into c1 values('2025-01-01 00:00:08','2025-01-01 00:00:00',9,19);")
        tdSql.execute(f"insert into c1 values('2025-01-01 00:00:09','2025-01-01 00:00:00',10,20);")
        tdSql.execute(f"insert into c2 values('2025-01-01 00:00:00','2025-01-01 00:00:00',1,11);")
        tdSql.execute(f"insert into c2 values('2025-01-01 00:00:01','2025-01-01 00:00:00',2,12);")
        tdSql.execute(f"insert into c2 values('2025-01-01 00:00:02','2025-01-01 00:00:00',3,13);")
        tdSql.execute(f"insert into c2 values('2025-01-01 00:00:03','2025-01-01 00:00:00',4,14);")
        tdSql.execute(f"insert into c2 values('2025-01-01 00:00:04','2025-01-01 00:00:00',5,15);")
        tdSql.execute(f"insert into c2 values('2025-01-01 00:00:05','2025-01-01 00:00:00',6,16);")
        tdSql.execute(f"insert into c2 values('2025-01-01 00:00:06','2025-01-01 00:00:00',7,17);")
        tdSql.execute(f"insert into c2 values('2025-01-01 00:00:07','2025-01-01 00:00:00',8,18);")
        tdSql.execute(f"insert into c2 values('2025-01-01 00:00:08','2025-01-01 00:00:00',9,19);")
        tdSql.execute(f"insert into c2 values('2025-01-01 00:00:09','2025-01-01 00:00:00',10,20);")
        tdSql.execute(f"insert into c0 values('2025-01-01 00:00:00','2025-01-01 00:00:00',1,11);")
        tdSql.execute(f"insert into c0 values('2025-01-01 00:00:01','2025-01-01 00:00:00',2,12);")
        tdSql.execute(f"insert into c0 values('2025-01-01 00:00:02','2025-01-01 00:00:00',3,13);")
        tdSql.execute(f"insert into c0 values('2025-01-01 00:00:03','2025-01-01 00:00:00',4,14);")
        tdSql.execute(f"insert into c0 values('2025-01-01 00:00:04','2025-01-01 00:00:00',5,15);")
        tdSql.execute(f"insert into c0 values('2025-01-01 00:00:05','2025-01-01 00:00:00',6,16);")
        tdSql.execute(f"insert into c0 values('2025-01-01 00:00:06','2025-01-01 00:00:00',7,17);")
        tdSql.execute(f"insert into c0 values('2025-01-01 00:00:07','2025-01-01 00:00:00',8,18);")
        tdSql.execute(f"insert into c0 values('2025-01-01 00:00:08','2025-01-01 00:00:00',9,19);")
        tdSql.execute(f"insert into c0 values('2025-01-01 00:00:09','2025-01-01 00:00:00',10,20);")
        tdSql.execute(f"create stable devices(ts timestamp,cint int,i1 int) tags(tint int,tdouble double) virtual 1;")
        tdSql.execute(f"create vtable d1(a1.cint,b1.i1) using devices tags(1,1.9);")
        tdSql.execute(f"create vtable d2(a2.cint,b2.i1) using devices tags(2,2.9);")
        tdSql.execute(f"create vtable d0(a0.cint,b0.i1) using devices tags(0,0.9);")
        tdSql.execute(f"create vtable vta1(ts timestamp, c1 int from a1.cint ,c2 int from b1.i1 );")
        tdSql.execute(f"create vtable vtb1(ts timestamp, c1 int from b1.cint ,c2 int from c1.i1 );")
        tdSql.execute(f"create vtable vtc1(ts timestamp, c1 int from c1.cint ,c2 int from a1.i1 );")
        tdSql.execute(f"create table pt(ts timestamp,c1 int ,c2 int,c3 varchar(100));")
        tdSql.execute(f"create table pt1(ts timestamp,c1 int ,c2 int,c3 varchar(100));")
        tdSql.execute(f"insert into pt values('2025-01-01 00:00:00',99,9,'test1');")
        tdSql.execute(f"insert into pt values('2025-01-01 00:03:00',100,9,'test2');")
        tdSql.execute(f"insert into pt values('2025-01-01 00:04:00',99,9,'test1');")
        tdSql.execute(f"insert into pt values('2025-01-01 00:07:00',100,9,'test2');")
        tdSql.execute(f"insert into pt1 values('2025-01-01 00:00:00',101,9,'test3');")
        tdSql.execute(f"insert into pt1 values('2025-01-01 00:03:00',102,9,'test1');")
        tdSql.execute(f"insert into pt1 values('2025-01-01 00:05:00',101,9,'test3');")
        tdSql.execute(f"insert into pt1 values('2025-01-01 00:07:00',102,9,'test1');")
        tdSql.execute(f"insert into pt values('2025-01-01 00:10:00',105,9,'test2');")
        tdSql.execute(f"insert into pt1 values('2025-01-01 00:10:00',106,9,'test3');")
