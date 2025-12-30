import time
import math
import random
from new_test_framework.utils import tdLog, tdSql, tdStream, streamUtil,StreamTableType, StreamTable, cluster,tdCom
from random import randint
import os
import subprocess
import json
import random
import time
import datetime

class Test_ThreeGorges:
    caseName = "test_three_gorges_second_case19"
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
    outTbname = "stb_hbny_sx_mint_jzzt2"
    streamName = "str_hbny_sx_mint_jzzt2"
    tableList = []
    resultIdx = "1"
    
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_three_gorges_second_case19(self):
        """test_three_gorges_case
        
        1. create snode
        2. create stream


        Catalog:
            - Streams:str_hbny_sx_mint_jzzt2

        Since: v3.3.3.7

        Labels: common,ci,skip

        Jira: None

        History:
            - 2025-7-18 lvze Created

        """


        tdStream.dropAllStreamsAndDbs()
            
        self.sxny_data1()
        self.createSnodeTest()
        self.createStream()
        self.checkStreamRunning()
        self.sxny_data2()
        tdSql.checkRowsLoop(2,f"select v,tablename,senid, sen_name, index_code, jz_location,jz_no,ps_name,ps_code from {self.dbname}.{self.outTbname} order by tablename;",100,1)
        list = tdSql.getResult(f"select v,tablename,senid, sen_name, index_code, jz_location,jz_no,ps_name,ps_code from {self.dbname}.{self.outTbname} order by tablename;")
        tdLog.info(f"result: {list}")
        self.checkResultWithResultFile()


    def createStream(self):
        tdLog.info(f"create stream :")
        stream1 = (
                    f"""create stream test1.str_hbny_sx_mint_jzzt2 interval(10m) sliding(10m) from test1.stb_hbny_sx_mint 
                    partition by tbname,senid, sen_name, index_code, jz_location,jz_no,ps_name,ps_code
                    stream_options(force_output|watermark(5m)|event_type(window_close)|pre_filter(ps_code in ('a0','a2') and index_code='a0'))
                    into test1.stb_hbny_sx_mint_jzzt2 output_subtable(concat_ws('_','cjdl_rtdb_jzzt',senid)) 
                    tags(
                    tablename varchar(50) as tbname,
                    senid varchar(255) as senid,
                    sen_name varchar(255) as sen_name,
                    index_code varchar(255) as index_code,
                    jz_location varchar(255) as jz_location,
                    jz_no varchar(255) as jz_no,
                    ps_name varchar(255) as ps_name,
                    ps_code varchar(255) as ps_code
                    )
                    as select
                        _twstart ts,
                        last(v) v
                    from
                        %%trows;
                    """
        )
        
        
        tdSql.execute(stream1,queryTimes=2)
        tdLog.info(f"create stream success!")
    
    def checkResultWithResultFile(self):
        chkSql = f"select v,tablename,senid, sen_name, index_code, jz_location,jz_no,ps_name,ps_code from {self.dbname}.{self.outTbname} order by tablename;"
        tdLog.info(f"check result with sql: {chkSql}")
        if tdSql.getRows() >0:
            tdCom.generate_query_result_file(self.caseName, self.resultIdx, chkSql)
            tdCom.compare_query_with_result_file(self.resultIdx, chkSql, f"{self.currentDir}/ans/{self.caseName}.{self.resultIdx}.csv", self.caseName)
            tdLog.info("check result with result file succeed")
            
    def sxny_data1(self):
        import random
        import time
        import datetime

        random.seed(42)
        tdSql.execute("create database test1 vgroups 6;")
        tdSql.execute("""CREATE STABLE test1.stb_hbny_sx_mint (ts TIMESTAMP , v DOUBLE ) 
                    TAGS (senid VARCHAR(20), index_code VARCHAR(255), index_name VARCHAR(255), ps_code VARCHAR(255), ps_name VARCHAR(255), 
                    ps_type VARCHAR(255), unit_name VARCHAR(255), province_name VARCHAR(255), area_name VARCHAR(255), company_name VARCHAR(255), 
                    sen_name VARCHAR(255), index_seq VARCHAR(255), jz_seq VARCHAR(255), jz_no VARCHAR(255), jz_location VARCHAR(255), 
                    unit_conversion DOUBLE
        )""")

        tdSql.execute("CREATE TABLE test1.`a0` USING test1.`stb_hbny_sx_mint` TAGS ('a0','a0','a0','a0','a0','a0','a0','a0','a0','a0','a0','a0','a0','a0','a0',9.9)")
        tdSql.execute("CREATE TABLE test1.`a1` USING test1.`stb_hbny_sx_mint` TAGS ('a1','a1','a1','a1','a1','a1','a1','a1','a1','a1','a1','a1','a1','a1','a1',8.8)")
        tdSql.execute("CREATE TABLE test1.`a2` USING test1.`stb_hbny_sx_mint` TAGS ('a2','a2','a2','a2','a2','a2','a2','a2','a2','a2','a2','a2','a2','a2','a2',7.7)")
        # tdSql.execute("CREATE TABLE test1.`a1` USING test1.`stb_hbny_sx_mint` TAGS ('a1','name_a1','/taosdata/a1','a0_1','a1_ch1','index_a1','pscode_a1','cnstationno_a1','level_a1','cz_z1','blq_a1','dcc_a1')")
        # tdSql.execute("CREATE TABLE test1.`a2` USING test1.`stb_hbny_sx_mint` TAGS ('a2','name_a2','/taosdata/a2','a0_2','a2_ch2','index_a2','pscode_a2','cnstationno_a2','level_a2','cz_z2','blq_a2','dcc_a2')")

        tables = ['a0', 'a1', 'a2']

        
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=0)
        base_ts = int(time.mktime(datetime.datetime.combine(yesterday, datetime.time.min).timetuple())) * 1000

        interval_ms = 600 * 1000  # 10分钟
        total_rows = 10

        for i in range(total_rows):
            ts = base_ts + i * interval_ms
            c1 = random.randint(0, 1000)
            c2 = random.randint(0, 1000)
            c3 = random.randint(0, 1000)
            for tb in tables:
                sql = "INSERT INTO test1.%s VALUES (%d,%d)" % (tb, ts, c1)
                tdSql.execute(sql)

    def sxny_data2(self):
        import random
        import time
        import datetime

        random.seed(42)
        
        tables = ['a0', 'a1', 'a2']

        
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=0)
        base_ts = int(time.mktime(datetime.datetime.combine(yesterday, datetime.time.min).timetuple())) * 1000

        interval_ms = 300 * 1000  
        total_rows = 1

        for i in range(total_rows):
            ts = base_ts + i * interval_ms
            c1 = random.randint(0, 1000)
            for tb in tables:
                sql1 = "INSERT INTO test1.%s VALUES ('2025-07-25 10:00:00',%d)" % (tb, c1)
                sql2 = "INSERT INTO test1.%s VALUES ('2025-07-25 10:20:00',%d)" % (tb, c1+1)
                sql3 = "INSERT INTO test1.%s VALUES ('2025-07-25 10:30:00',%d)" % (tb, c1+2)
                
                tdSql.execute(sql1)          
                tdSql.execute(sql2)          
                tdSql.execute(sql3)          
      
        
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
                            "name": "stb_sxny_cn",
                            "child_table_exists": "yes",
                            "childtable_count": 3,
                            "childtable_prefix": "a",
                            "auto_create_table": "no",
                            "batch_create_tbl_num": 10,
                            "data_source": "rand",
                            "insert_mode": "taosc",
                            "insert_rows": 5000,
                            "childtable_limit": 100000000,
                            "childtable_offset": 0,
                            "interlace_rows": 0,
                            "insert_interval": 0,
                            "max_sql_len": 1048576,
                            "disorder_ratio": 0,
                            "disorder_range": 1000,
                            "timestamp_step": 30000,
                            "start_timestamp": "now",
                            "sample_format": "",
                            "sample_file": "",
                            "tags_file": "",
                            "columns": [
                                {"type": "double","name":"val","count": 1,"max":100,"min":100}
                            ],
                            "tags": [
                                {"type": "varchar","name":"point","len":100},
                                {"type": "varchar","name":"point_name","len":100},
                                {"type": "varchar","name":"point_path","len":100},
                                {"type": "varchar","name":"index_name","len":100,},
                                {"type": "varchar","name":"country_equipment_code","len":100},
                                {"type": "varchar","name":"index_code","len":100},
                                {"type": "varchar","name":"ps_code","len":100},
                                {"type": "varchar","name":"cnstationno","len":100,},
                                {"type": "varchar","name":"index_level","len":100},
                                {"type": "varchar","name":"cz_flag","len":100},
                                {"type": "varchar","name":"blq_flag","len":100},
                                {"type": "varchar","name":"dcc_flag","len":100,},
                                
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
        

    def checkResultRows(self, expectedRows):
        tdSql.checkResultsByFunc(
            f"select * from information_schema.ins_snodes order by id;",
            lambda: tdSql.getRows() == expectedRows,
            delay=0.5, retry=2
        )

     
    def get_pid_by_cmdline(self,pattern):
        try:
            cmd = "unset LD_PRELOAD;ps -eo pid,cmd | grep '{}' | grep -v grep | grep -v SCREEN".format(pattern)
            output = subprocess.check_output(cmd, shell=True).decode().strip()
            # 可多行，默认取第一行
            lines = output.split('\n')
            if lines:
                pid = int(lines[0].strip().split()[0])
                return pid
        except subprocess.CalledProcessError:
            return None
  
     
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
