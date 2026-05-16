import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream, streamUtil,StreamTableType, StreamTable, cluster
from random import randint
import os
import subprocess
import json

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

    def test_stream(self):
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
        tdSql.execute(f'create database test1 vgroups 10 ;')
        self.createTable()

        sql = (f"""
                create stream test1.stbc interval(1s) sliding(1s) from test1.stbc 
                    partition by tbname,tint
                    stream_options(max_delay(4m)|pre_filter(tint in ('1','2'))|fill_history )
                    into test1.stbcout output_subtable(concat('stbc_',tbname)) 
                    tags(
                    tablename varchar(50) as tbname,
                    tint int as tint
                    )
                    as select
                        _wstart ts,
                        last(cint) cint
                    from
                        %%trows;
                """)  
        sql2 = (f"""
                create stream test1.stbc2 interval(1s) sliding(1s) from test1.stbc 
                    partition by tbname,tint
                    stream_options(max_delay(4m)|pre_filter(tint in ('1','2'))|fill_history )
                    into test1.stbcout2 output_subtable(concat('stbc2_',tbname)) 
                    tags(
                    tablename varchar(50) as tbname,
                    tint int as tint
                    )
                    as select
                        _twstart t,
                        last(cint) cint
                    from
                        %%tbname where ts >= _twstart and ts <_twend;
                """)      

        tdSql.execute(sql)
        tdSql.execute(sql2)

        
        self.checkStreamRunning()
        
        
            

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

    def insert_data(self,table_count=100, total_rows=10, interval_sec=30):
        import time, random

        db_name = "test1"
        stable_name = "stba"
        base_ts = int(time.mktime(time.strptime("2025-01-01 00:00:00", "%Y-%m-%d %H:%M:%S"))) * 1000
        interval_ms = interval_sec * 1000

        
        random.seed(42)

        
        tdSql.execute(f"create database if not exists {db_name} vgroups 6;")
        tdSql.execute(f"""
            CREATE STABLE IF NOT EXISTS {db_name}.{stable_name} (
                ts TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium',
                cts TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium',
                cint INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium',
                i1 INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium'
            ) TAGS (
                tint INT, tdouble DOUBLE, tvar VARCHAR(100),
                tnchar NCHAR(100), tts TIMESTAMP, tbool BOOL
            );
        """)

        # 创建 table_count 张表
        for i in range(table_count):
            tb_name = f"a{i}"
            tag_values = f"({i % 50}, {random.uniform(0, 100):.6e}, 'tagv{i}', 'nchar{i}', {random.randint(1000000000, 2000000000)}, {'true' if i % 2 == 0 else 'false'})"
            tdSql.execute(f"CREATE TABLE IF NOT EXISTS {db_name}.{tb_name} USING {db_name}.{stable_name} TAGS {tag_values};")

        # 写入数据
        for i in range(total_rows):
            ts = base_ts + i * interval_ms
            c1 = random.randint(0, 1000)
            c2 = random.randint(1000, 2000)
            values = f"({ts},{ts},{c1},{c2})"
            for j in range(table_count):
                tb_name = f"a{j}"
                tdSql.execute(f"INSERT INTO {db_name}.{tb_name} VALUES {values}")


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
                            "childtable_count": 100,
                            "childtable_prefix": "a",
                            "auto_create_table": "no",
                            "batch_create_tbl_num": 10,
                            "data_source": "rand",
                            "insert_mode": "taosc",
                            "insert_rows": 10,
                            "childtable_limit": 100000000,
                            "childtable_offset": 0,
                            "interlace_rows": 0,
                            "insert_interval": 0,
                            "max_sql_len": 1048576,
                            "disorder_ratio": 0,
                            "disorder_range": 1000,
                            "timestamp_step": 30000,
                            "start_timestamp": "2025-01-01 20:00:00.000",
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
