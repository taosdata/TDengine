import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream, streamUtil,StreamTableType, StreamTable, cluster
from random import randint
import os
import subprocess
import json

class TestSdnyStream:
    caseName = "test_sdny_case1_bug1"
    currentDir = os.path.dirname(os.path.abspath(__file__))
    runAll = False
    dbname = "test1"
    stbname= "stba"
    stName = ""
    resultIdx = ""
    sliding = 1
    subTblNum = 3
    tblRowNum = 10
    tableList = []
    outTbname = "monitor_info_sldc_gml2"
    streamName = "stream_monitor_info_sldc_gml2"
    tableList = []
    resultIdx = "1"
    
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_sdny_case1_bug1(self):
        """Stream sdny test
        
        1. test sdny stream 
        

        Catalog:
            - Streams:sdny

        Since: v3.3.3.7

        Labels: common,ci,skip

        Jira: None

        History:
            - 2025-7-8 lvze Created

        """

        tdStream.dropAllStreamsAndDbs()
        self.createSnodeTest()
        tdSql.execute(f'create database test1 vgroups 10 ;')
        self.sdnydata()

        sql = (f"""
            create stream test1.stream_monitor_info_sldc_gml2 interval(30s) sliding(30s) from test1.sldc_dp 
                    stream_options(fill_history|pre_filter(ts > '2025-07-17 08:30:00.000'))
                    into test1.monitor_info_sldc_gml2 
                    as select
                        _wstart as start_time,
                        _wend as end_time,
                        '01072016' as org_code,
                        '盛鲁电厂' as org_name,
                        last(jz1gmjassllfk+jz1gmjbssllfk+jz1gmjcssllfk+jz1gmjdssllfk+jz1gmjessllfk+jz1gmjfssllfk+jz2gmjassllfk+
                        jz2gmjbssllfk+jz2gmjcssllfk+jz2gmjdssllfk+jz2gmjessllfk+jz2gmjfssllfk) as gml
                        from
                        test1.sldc_dp 
                        where ts >= _twstart -1s and ts< _twend
                        interval (1s)
                        fill(prev)""")
        
        tdSql.execute(sql,queryTimes=2)
        self.checkStreamRunning()
        
        # tdSql.checkRowsLoop(4,f"select * from test1.monitor_info_sldc_gml2",100,6)
        
            

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

    def sdnydata(self):
        tdLog.info("sdnydata ready insert:")
        tdSql.execute(f"use {self.dbname}")
        stbsql = (
            " CREATE STABLE `sldc_dp` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `data_write_time` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `jz1fdgl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1ssfdfh` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1fdmh` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1gdmh` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1qjrhl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1zhcydl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1zkby` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1zzqyl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1zzqwda` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1zzqwdb` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1zzqll` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1gswd` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1gsll` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1glxl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1qjrh` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1zhrxl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1gmjassllfk` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1gmjasslllj` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1gmjbssllfk` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1gmjbsslllj` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1gmjcssllfk` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1gmjcsslllj` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1gmjdssllfk` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1gmjdsslllj` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1gmjessllfk` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1gmjesslllj` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1gmjfssllfk` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1gmjfsslllj` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1zrqwda` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1zrqwdb` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1zrzqyl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1mmjadl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1mmjbdl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1mmjcdl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1mmjddl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1mmjedl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1mmjfdl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1cyqckwda` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1cyqckwdb` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1njswd` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1nqqxhsckawd` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1nqqxhsckbwd` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1nqqxhsrkawd` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1nqqxhsrkbwd` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1kyqackyqwdsel` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1kyqbckyqwdsel` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1yfjackyqwd` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1yfjbckyqwd` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1trkyqwd` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1trkyqwd1` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1trkyqwd2` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1trkyqwd3` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1tckjyqwd1` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1tckjyqwd2` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1tckyqwd1` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1bya` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1byb` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1pqwda` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1pqwdb` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1gmjadl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1gmjbdl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1gmjcdl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1gmjddl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1gmjedl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1gmjfdl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1yfjadl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1yfjbdl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1ycfjadl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1ycfjbdl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1sfjadl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1sfjbdl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1fdjyggl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1fdjwggl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1sjzs` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1zfl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1ltyl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1smb` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1rll` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1grd` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1zjwd` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1yl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1kyqckwd` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1abmfsybrkcy` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1bbmfsybrkcy` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1abjcsdmfytwdzdz` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1bbjcsdmfytwdzdz` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2fdgl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2ssfdfh` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2fdmh` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2gdmh` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2qjrhl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2zhcydl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2zkby` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2zzqyl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2zzqwda` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2zzqwdb` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2zzqll` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2gswd` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2gsll` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2glxl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2qjrh` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2zhrxl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2gmjassllfk` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2gmjasslllj` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2gmjbssllfk` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2gmjbsslllj` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2gmjcssllfk` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2gmjcsslllj` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2gmjdssllfk` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2gmjdsslllj` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2gmjessllfk` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2gmjesslllj` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2gmjfssllfk` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2gmjfsslllj` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2zrqwda` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2zrqwdb` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2zrzqyl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2mmjadl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2mmjbdl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2mmjcdl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2mmjddl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2mmjedl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2mmjfdl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2cyqckwda` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2cyqckwdb` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2njswd` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2nqqxhsckawd` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2nqqxhsckbwd` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2nqqxhsrkawd` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2nqqxhsrkbwd` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2kyqackyqwdsel` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2kyqbckyqwdsel` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2yfjackyqwd` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2yfjbckyqwd` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2trkyqwd` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2trkyqwd1` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2trkyqwd2` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2trkyqwd3` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2tckjyqwd1` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2tckjyqwd2` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2tckyqwd1` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2bya` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2byb` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2pqwda` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2pqwdb` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2gmjadl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2gmjbdl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2gmjcdl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2gmjddl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2gmjedl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2gmjfdl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2yfjadl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2yfjbdl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2ycfjadl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2ycfjbdl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2sfjadl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2sfjbdl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2fdjyggl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2fdjwggl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2sjzs` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2zfl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2ltyl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2smb` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2rll` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2grd` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2zjwd` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2yl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2kyqckwd` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2abmfsybrkcy` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2bbmfsybrkcy` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2abjcsdmfytwdzdz` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2bbjcsdmfytwdzdz` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1kyqazdjdl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1kyqabydjdl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1kyqbbydjdl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz1kyqbzdjdl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2kyqazdjdl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2kyqabydjdl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2kyqbbydjdl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium', `jz2kyqbzdjdl` DOUBLE ENCODE 'delta-d' COMPRESS 'lz4' LEVEL 'medium') TAGS (`iot_hub_id` VARCHAR(100), `device_group_code` VARCHAR(100), `device_code` VARCHAR(100)) ;"
        )
        tb1sql = (
            'CREATE TABLE `e010720169990001` USING `sldc_dp` (`iot_hub_id`, `device_group_code`, `device_code`) TAGS ("01072016", "1834406116550639618", "e010720169990001");'
        )
        tb2sql = (
            'CREATE TABLE `e010720169990002` USING `sldc_dp` (`iot_hub_id`, `device_group_code`, `device_code`) TAGS ("01072017", "1834406116550639619", "e010720169990002");'
        )
        tdSql.execute(stbsql)
        tdSql.execute(tb1sql)
        tdSql.execute(tb2sql)
        tdSql.execute(f"insert into {self.dbname}.e010720169990001 file 'cases/18-StreamProcessing/20-UseCase/e010720169990001.csv';")
        tdSql.execute(f"insert into {self.dbname}.e010720169990002 file 'cases/18-StreamProcessing/20-UseCase/e010720169990001.csv';")
        tdLog.info("load csv file success.")
        
    
    def createTable(self):
        tdSql.execute(f"use test1;")
        # tdSql.execute(f"CREATE STABLE `stba` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `cts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `cint` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium', `i1` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium') TAGS (`tint` INT, `tdouble` DOUBLE, `tvar` VARCHAR(100), `tnchar` NCHAR(100), `tts` TIMESTAMP, `tbool` BOOL);")
        tdSql.execute(f"CREATE STABLE `stbb` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `cts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `cint` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium', `i1` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium') TAGS (`tint` INT, `tdouble` DOUBLE, `tvar` VARCHAR(100), `tnchar` NCHAR(100), `tts` TIMESTAMP, `tbool` BOOL);")
        tdSql.execute(f"CREATE STABLE `stbc` (`ts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `cts` TIMESTAMP ENCODE 'delta-i' COMPRESS 'lz4' LEVEL 'medium', `cint` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium', `i1` INT ENCODE 'simple8b' COMPRESS 'lz4' LEVEL 'medium') TAGS (`tint` INT, `tdouble` DOUBLE, `tvar` VARCHAR(100), `tnchar` NCHAR(100), `tts` TIMESTAMP, `tbool` BOOL);")
        # tdSql.execute(f"create table a0 using stba tags(1,1.1,'a0','测试a0','2025-01-01 00:00:01',1);")
        # tdSql.execute(f"create table a1 using stba tags(NULL,2.1,'a1','测试a1','2025-01-01 00:00:02',0);")
        # tdSql.execute(f"create table a2 using stba tags(2,3.1,'a2','测试a2','2025-01-01 00:00:03',1);")
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
