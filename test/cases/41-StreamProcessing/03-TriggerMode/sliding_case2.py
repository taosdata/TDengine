import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream, streamUtil,StreamTableType, StreamTable, cluster
from random import randint
import os
import subprocess
import json

class TestSliding:
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

    def test_sliding_case1(self):
        """Stream test_sliding_case1 test
        
        1. test_sliding_case2
        

        Catalog:
            - Streams:test_sliding_case2

        Since: v3.3.3.7

        Labels: common,ci,skip

        Jira: None

        History:
            - 2025-7-8 lvze Created

        """

        tdStream.dropAllStreamsAndDbs()
        self.createSnodeTest()
        tdSql.execute(f'create database test1 vgroups 10 ;')
        # self.dataIn()
        self.createTable()
        stream1 = (
        """ create stream test1.st1 interval(5m) sliding(5m) from test1.stba 
                        stream_options(fill_history|pre_filter(id>15 and id is not null))
                        into test1.st1out 
                        as select
                            _twstart as start_time,
                            _twend as end_time,
                            id,
                            cint,
                            STATECOUNT(cint , "ge", 5) a
                            from
                            %%trows t1 ;
        """
        )
        stream2 = (
        """ create stream s7 interval(5s) sliding(5s) from stba partition by tint, tbname stream_options(pre_filter(id>15)|fill_history) into s7_out  
        as select _twstart,_twend, sum(id) id, sum(cint) cint ,count(i1) i1 from %%trows; 
        """
        )
        
        stream3 = (
        """ create stream s8 interval(5s) sliding(5s) from stba partition by tint, tbname stream_options(pre_filter(id>15 and id in (16,19))|fill_history) into s8_out  
        as select _twstart,_twend, sum(id) id, sum(cint) cint ,count(i1) i1 from %%trows union all select _twstart,_twend, sum(id)+1 id, sum(cint)+1 cint ,count(i1)+1 i1 from %%trows ;
        """
        )
        

        # tdSql.execute(stream1)
        # tdSql.execute(stream2)
        tdSql.execute(stream3)
        self.checkStreamRunning()
        tdSql.query(f"select id from test1.s8_out")
        data = tdSql.getData(0,0)
        tdLog.info(f"{data}")
        if not data or data[0] is None:
            raise Exception("ERROR :result data is not right")
        
       
            

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

    def dataIn(self):
        tdLog.info(f"insert more data:")
        config = {
            "filetype": "insert",
            "cfgdir": "/etc/taos",
            "host": "localhost",
            "port": 6030,
            "user": "root",
            "password": "taosdata",
            "thread_count": 16,
            "thread_count_create_tbl": 8,
            "result_file": "./insert.txt",
            "confirm_parameter_prompt": "no",
            "insert_interval": 0,
            "num_of_records_per_req": 1000,
            "max_sql_len": 1048576,
            "databases": [{
                    "dbinfo": {
                        "name": "test1",
                        "drop": "no",
                        "replica": 3,
                        "days": 10,
                        "precision": "ms",
                        "keep": 36500,
                        "minRows": 100,
                        "maxRows": 4096
                    },
                    "super_tables": [{
                            "name": "stba",
                            "child_table_exists": "no",
                            "childtable_count": 3000,
                            "childtable_prefix": "a",
                            "auto_create_table": "no",
                            "batch_create_tbl_num": 10,
                            "data_source": "rand",
                            "insert_mode": "taosc",
                            "insert_rows": 500,
                            "childtable_limit": 100000000,
                            "childtable_offset": 0,
                            "interlace_rows": 0,
                            "insert_interval": 0,
                            "max_sql_len": 1048576,
                            "disorder_ratio": 0,
                            "disorder_range": 1000,
                            "timestamp_step": 30000,
                            "start_timestamp": "2025-05-01 00:00:00.000",
                            "sample_format": "",
                            "sample_file": "",
                            "tags_file": "",
                            "columns": [
                                {"type": "timestamp","name":"cts","count": 1,"start":"2025-02-01 00:00:00.000"},
                                {"type": "int","name":"cint","max":100,"min":-1},
                                {"type": "int","name":"i1","max":100,"min":-1}
                            ],
                            "tags": [
                                {"type": "int","name":"tint","max":100,"min":-1},
                                {"type": "double","name":"tdouble","max":100,"min":0},
                                {"type": "varchar","name":"tvar","len":100,"count": 1},
                                {"type": "nchar","name":"tnchar","len":100,"count": 1},
                                {"type": "timestamp","name":"tts"},
                                {"type": "bool","name":"tbool"}
                            ]
                        }

                    ]
                }
            ]
        }
        
        with open('insert_config.json','w') as f:
            json.dump(config,f,indent=4)
        tdLog.info('config file ready')
        cmd = f"taosBenchmark -f insert_config.json "
        # output = subprocess.check_output(cmd, shell=True).decode().strip()
        ret = os.system(cmd)
        if ret != 0:
            raise Exception("taosBenchmark run failed")
        time.sleep(5)
        tdLog.info(f"Insert data:taosBenchmark -f insert_config.json")

    def createTable(self):
        tdSql.execute(f"use test1;")
        tdSql.execute(f"CREATE STABLE `stba` (`ts` TIMESTAMP ,id int primary key, `cts` TIMESTAMP , `cint` INT , `i1` INT ) TAGS (`tint` INT, `tdouble` DOUBLE, `tvar` VARCHAR(100), `tnchar` NCHAR(100), `tts` TIMESTAMP, `tbool` BOOL);")
        tdSql.execute(f"CREATE STABLE `stbb` (`ts` TIMESTAMP ,id int primary key, `cts` TIMESTAMP , `cint` INT , `i1` INT ) TAGS (`tint` INT, `tdouble` DOUBLE, `tvar` VARCHAR(100), `tnchar` NCHAR(100), `tts` TIMESTAMP, `tbool` BOOL);")
        tdSql.execute(f"CREATE STABLE `stbc` (`ts` TIMESTAMP ,id int primary key, `cts` TIMESTAMP , `cint` INT , `i1` INT ) TAGS (`tint` INT, `tdouble` DOUBLE, `tvar` VARCHAR(100), `tnchar` NCHAR(100), `tts` TIMESTAMP, `tbool` BOOL);")
        tdSql.execute(f"create table a0 using stba tags(1,1.1,'a0','测试a0','2025-01-01 00:00:01',1);")
        tdSql.execute(f"create table a1 using stba tags(NULL,2.1,'a1','测试a1','2025-01-01 00:00:02',0);")
        tdSql.execute(f"create table a2 using stba tags(2,3.1,'a2','测试a2','2025-01-01 00:00:03',1);")
        tdSql.execute(f"create table b0 using stbb tags(1,1.1,'a0','测试a0','2025-01-01 00:00:01',1);")
        tdSql.execute(f"create table b1 using stbb tags(NULL,2.1,'a1','测试a1','2025-01-01 00:00:02',0);")
        tdSql.execute(f"create table b2 using stbb tags(2,3.1,'a2','测试a2','2025-01-01 00:00:03',1);")
        tdSql.execute(f"create table c0 using stbc tags(1,1.1,'a0','测试a0','2025-01-01 00:00:01',1);")
        tdSql.execute(f"create table c1 using stbc tags(NULL,2.1,'a1','测试a1','2025-01-01 00:00:02',0);")
        tdSql.execute(f"create table c2 using stbc tags(2,3.1,'a2','测试a2','2025-01-01 00:00:03',1);")
        tdSql.execute(f"insert into a1 values('2025-01-01 00:00:00',10,'2025-01-01 00:00:00',1,11);")
        tdSql.execute(f"insert into a1 values('2025-01-01 00:00:01',11,'2025-01-01 00:00:00',2,12);")
        tdSql.execute(f"insert into a1 values('2025-01-01 00:00:02',12,'2025-01-01 00:00:00',3,13);")
        tdSql.execute(f"insert into a1 values('2025-01-01 00:00:03',13,'2025-01-01 00:00:00',4,14);")
        tdSql.execute(f"insert into a1 values('2025-01-01 00:00:04',14,'2025-01-01 00:00:00',5,15);")
        tdSql.execute(f"insert into a1 values('2025-01-01 00:00:05',15,'2025-01-01 00:00:00',6,16);")
        tdSql.execute(f"insert into a1 values('2025-01-01 00:00:06',16,'2025-01-01 00:00:00',7,17);")
        tdSql.execute(f"insert into a1 values('2025-01-01 00:00:07',17,'2025-01-01 00:00:00',8,18);")
        tdSql.execute(f"insert into a1 values('2025-01-01 00:00:08',18,'2025-01-01 00:00:00',9,19);")
        tdSql.execute(f"insert into a1 values('2025-01-01 00:00:09',19,'2025-01-01 00:00:00',10,20);")
        tdSql.execute(f"insert into a2 values('2025-01-01 00:00:00',10,'2025-01-01 00:00:00',1,11);")
        tdSql.execute(f"insert into a2 values('2025-01-01 00:00:01',11,'2025-01-01 00:00:00',2,12);")
        tdSql.execute(f"insert into a2 values('2025-01-01 00:00:02',12,'2025-01-01 00:00:00',3,13);")
        tdSql.execute(f"insert into a2 values('2025-01-01 00:00:03',13,'2025-01-01 00:00:00',4,14);")
        tdSql.execute(f"insert into a2 values('2025-01-01 00:00:04',14,'2025-01-01 00:00:00',5,15);")
        tdSql.execute(f"insert into a2 values('2025-01-01 00:00:05',15,'2025-01-01 00:00:00',6,16);")
        tdSql.execute(f"insert into a2 values('2025-01-01 00:00:06',16,'2025-01-01 00:00:00',7,17);")
        tdSql.execute(f"insert into a2 values('2025-01-01 00:00:07',17,'2025-01-01 00:00:00',8,18);")
        tdSql.execute(f"insert into a2 values('2025-01-01 00:00:08',18,'2025-01-01 00:00:00',9,19);")
        tdSql.execute(f"insert into a2 values('2025-01-01 00:00:09',19,'2025-01-01 00:00:00',10,20);")
        tdSql.execute(f"insert into a0 values('2025-01-01 00:00:00',10,'2025-01-01 00:00:00',1,11);")
        tdSql.execute(f"insert into a0 values('2025-01-01 00:00:01',11,'2025-01-01 00:00:00',2,12);")
        tdSql.execute(f"insert into a0 values('2025-01-01 00:00:02',12,'2025-01-01 00:00:00',3,13);")
        tdSql.execute(f"insert into a0 values('2025-01-01 00:00:03',13,'2025-01-01 00:00:00',4,14);")
        tdSql.execute(f"insert into a0 values('2025-01-01 00:00:04',14,'2025-01-01 00:00:00',5,15);")
        tdSql.execute(f"insert into a0 values('2025-01-01 00:00:05',15,'2025-01-01 00:00:00',6,16);")
        tdSql.execute(f"insert into a0 values('2025-01-01 00:00:06',16,'2025-01-01 00:00:00',7,17);")
        tdSql.execute(f"insert into a0 values('2025-01-01 00:00:07',17,'2025-01-01 00:00:00',8,18);")
        tdSql.execute(f"insert into a0 values('2025-01-01 00:00:08',18,'2025-01-01 00:00:00',9,19);")
        tdSql.execute(f"insert into a0 values('2025-01-01 00:00:09',19,'2025-01-01 00:00:00',10,20);")
        tdSql.execute(f"insert into b1 values('2025-01-01 00:00:00',10,'2025-01-01 00:00:00',1,11);")
        tdSql.execute(f"insert into b1 values('2025-01-01 00:00:01',11,'2025-01-01 00:00:00',2,12);")
        tdSql.execute(f"insert into b1 values('2025-01-01 00:00:02',12,'2025-01-01 00:00:00',3,13);")
        tdSql.execute(f"insert into b1 values('2025-01-01 00:00:03',13,'2025-01-01 00:00:00',4,14);")
        tdSql.execute(f"insert into b1 values('2025-01-01 00:00:04',14,'2025-01-01 00:00:00',5,15);")
        tdSql.execute(f"insert into b1 values('2025-01-01 00:00:05',15,'2025-01-01 00:00:00',6,16);")
        tdSql.execute(f"insert into b1 values('2025-01-01 00:00:06',16,'2025-01-01 00:00:00',7,17);")
        tdSql.execute(f"insert into b1 values('2025-01-01 00:00:07',17,'2025-01-01 00:00:00',8,18);")
        tdSql.execute(f"insert into b1 values('2025-01-01 00:00:08',18,'2025-01-01 00:00:00',9,19);")
        tdSql.execute(f"insert into b1 values('2025-01-01 00:00:09',19,'2025-01-01 00:00:00',10,20);")
        tdSql.execute(f"insert into b2 values('2025-01-01 00:00:00',10,'2025-01-01 00:00:00',1,11);")
        tdSql.execute(f"insert into b2 values('2025-01-01 00:00:01',11,'2025-01-01 00:00:00',2,12);")
        tdSql.execute(f"insert into b2 values('2025-01-01 00:00:02',12,'2025-01-01 00:00:00',3,13);")
        tdSql.execute(f"insert into b2 values('2025-01-01 00:00:03',13,'2025-01-01 00:00:00',4,14);")
        tdSql.execute(f"insert into b2 values('2025-01-01 00:00:04',14,'2025-01-01 00:00:00',5,15);")
        tdSql.execute(f"insert into b2 values('2025-01-01 00:00:05',15,'2025-01-01 00:00:00',6,16);")
        tdSql.execute(f"insert into b2 values('2025-01-01 00:00:06',16,'2025-01-01 00:00:00',7,17);")
        tdSql.execute(f"insert into b2 values('2025-01-01 00:00:07',17,'2025-01-01 00:00:00',8,18);")
        tdSql.execute(f"insert into b2 values('2025-01-01 00:00:08',18,'2025-01-01 00:00:00',9,19);")
        tdSql.execute(f"insert into b2 values('2025-01-01 00:00:09',19,'2025-01-01 00:00:00',10,20);")
        tdSql.execute(f"insert into b0 values('2025-01-01 00:00:00',10,'2025-01-01 00:00:00',1,11);")
        tdSql.execute(f"insert into b0 values('2025-01-01 00:00:01',11,'2025-01-01 00:00:00',2,12);")
        tdSql.execute(f"insert into b0 values('2025-01-01 00:00:02',12,'2025-01-01 00:00:00',3,13);")
        tdSql.execute(f"insert into b0 values('2025-01-01 00:00:03',13,'2025-01-01 00:00:00',4,14);")
        tdSql.execute(f"insert into b0 values('2025-01-01 00:00:04',14,'2025-01-01 00:00:00',5,15);")
        tdSql.execute(f"insert into b0 values('2025-01-01 00:00:05',15,'2025-01-01 00:00:00',6,16);")
        tdSql.execute(f"insert into b0 values('2025-01-01 00:00:06',16,'2025-01-01 00:00:00',7,17);")
        tdSql.execute(f"insert into b0 values('2025-01-01 00:00:07',17,'2025-01-01 00:00:00',8,18);")
        tdSql.execute(f"insert into b0 values('2025-01-01 00:00:08',18,'2025-01-01 00:00:00',9,19);")
        tdSql.execute(f"insert into b0 values('2025-01-01 00:00:09',19,'2025-01-01 00:00:00',10,20);")
        tdSql.execute(f"insert into c1 values('2025-01-01 00:00:00',10,'2025-01-01 00:00:00',1,11);")
        tdSql.execute(f"insert into c1 values('2025-01-01 00:00:01',11,'2025-01-01 00:00:00',2,12);")
        tdSql.execute(f"insert into c1 values('2025-01-01 00:00:02',12,'2025-01-01 00:00:00',3,13);")
        tdSql.execute(f"insert into c1 values('2025-01-01 00:00:03',13,'2025-01-01 00:00:00',4,14);")
        tdSql.execute(f"insert into c1 values('2025-01-01 00:00:04',14,'2025-01-01 00:00:00',5,15);")
        tdSql.execute(f"insert into c1 values('2025-01-01 00:00:05',15,'2025-01-01 00:00:00',6,16);")
        tdSql.execute(f"insert into c1 values('2025-01-01 00:00:06',16,'2025-01-01 00:00:00',7,17);")
        tdSql.execute(f"insert into c1 values('2025-01-01 00:00:07',17,'2025-01-01 00:00:00',8,18);")
        tdSql.execute(f"insert into c1 values('2025-01-01 00:00:08',18,'2025-01-01 00:00:00',9,19);")
        tdSql.execute(f"insert into c1 values('2025-01-01 00:00:09',19,'2025-01-01 00:00:00',10,20);")
        tdSql.execute(f"insert into c2 values('2025-01-01 00:00:00',10,'2025-01-01 00:00:00',1,11);")
        tdSql.execute(f"insert into c2 values('2025-01-01 00:00:01',11,'2025-01-01 00:00:00',2,12);")
        tdSql.execute(f"insert into c2 values('2025-01-01 00:00:02',12,'2025-01-01 00:00:00',3,13);")
        tdSql.execute(f"insert into c2 values('2025-01-01 00:00:03',13,'2025-01-01 00:00:00',4,14);")
        tdSql.execute(f"insert into c2 values('2025-01-01 00:00:04',14,'2025-01-01 00:00:00',5,15);")
        tdSql.execute(f"insert into c2 values('2025-01-01 00:00:05',15,'2025-01-01 00:00:00',6,16);")
        tdSql.execute(f"insert into c2 values('2025-01-01 00:00:06',16,'2025-01-01 00:00:00',7,17);")
        tdSql.execute(f"insert into c2 values('2025-01-01 00:00:07',17,'2025-01-01 00:00:00',8,18);")
        tdSql.execute(f"insert into c2 values('2025-01-01 00:00:08',18,'2025-01-01 00:00:00',9,19);")
        tdSql.execute(f"insert into c2 values('2025-01-01 00:00:09',19,'2025-01-01 00:00:00',10,20);")
        tdSql.execute(f"insert into c0 values('2025-01-01 00:00:00',10,'2025-01-01 00:00:00',1,11);")
        tdSql.execute(f"insert into c0 values('2025-01-01 00:00:01',11,'2025-01-01 00:00:00',2,12);")
        tdSql.execute(f"insert into c0 values('2025-01-01 00:00:02',12,'2025-01-01 00:00:00',3,13);")
        tdSql.execute(f"insert into c0 values('2025-01-01 00:00:03',13,'2025-01-01 00:00:00',4,14);")
        tdSql.execute(f"insert into c0 values('2025-01-01 00:00:04',14,'2025-01-01 00:00:00',5,15);")
        tdSql.execute(f"insert into c0 values('2025-01-01 00:00:05',15,'2025-01-01 00:00:00',6,16);")
        tdSql.execute(f"insert into c0 values('2025-01-01 00:00:06',16,'2025-01-01 00:00:00',7,17);")
        tdSql.execute(f"insert into c0 values('2025-01-01 00:00:07',17,'2025-01-01 00:00:00',8,18);")
        tdSql.execute(f"insert into c0 values('2025-01-01 00:00:08',18,'2025-01-01 00:00:00',9,19);")
        tdSql.execute(f"insert into c0 values('2025-01-01 00:00:09',19,'2025-01-01 00:00:00',10,20);")
        # tdSql.execute(f"create stable devices(ts timestamp,cint int,i1 int) tags(tint int,tdouble double) virtual 1;")
        # tdSql.execute(f"create vtable d1(a1.cint,b1.i1) using devices tags(1,1.9);")
        # tdSql.execute(f"create vtable d2(a2.cint,b2.i1) using devices tags(2,2.9);")
        # tdSql.execute(f"create vtable d0(a0.cint,b0.i1) using devices tags(0,0.9);")
        # tdSql.execute(f"create vtable vta1(ts timestamp, c1 int from a1.cint ,c2 int from b1.i1 );")
        # tdSql.execute(f"create vtable vtb1(ts timestamp, c1 int from b1.cint ,c2 int from c1.i1 );")
        # tdSql.execute(f"create vtable vtc1(ts timestamp, c1 int from c1.cint ,c2 int from a1.i1 );")
        # tdSql.execute(f"create table pt(ts timestamp,c1 int ,c2 int,c3 varchar(100));")
        # tdSql.execute(f"create table pt1(ts timestamp,c1 int ,c2 int,c3 varchar(100));")
        # tdSql.execute(f"insert into pt values('2025-01-01 00:00:00',99,9,'test1');")
        # tdSql.execute(f"insert into pt values('2025-01-01 00:03:00',100,9,'test2');")
        # tdSql.execute(f"insert into pt values('2025-01-01 00:04:00',99,9,'test1');")
        # tdSql.execute(f"insert into pt values('2025-01-01 00:07:00',100,9,'test2');")
        # tdSql.execute(f"insert into pt1 values('2025-01-01 00:00:00',101,9,'test3');")
        # tdSql.execute(f"insert into pt1 values('2025-01-01 00:03:00',102,9,'test1');")
        # tdSql.execute(f"insert into pt1 values('2025-01-01 00:05:00',101,9,'test3');")
        # tdSql.execute(f"insert into pt1 values('2025-01-01 00:07:00',102,9,'test1');")
        # tdSql.execute(f"insert into pt values('2025-01-01 00:10:00',105,9,'test2');")
        # tdSql.execute(f"insert into pt1 values('2025-01-01 00:10:00',106,9,'test3');")
