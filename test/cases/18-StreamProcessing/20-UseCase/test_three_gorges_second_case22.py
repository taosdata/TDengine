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
    caseName = "test_three_gorges_second_case22"
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
    outTbname = "stb_dwi_cjdl_rtems_power"
    streamName = "stm_dwi_cjdl_rtems_power"
    tableList = []
    resultIdx = "1"
    
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_three_gorges_second_case22(self):
        """test_three_gorges_case
        
        1. create snode
        2. create stream


        Catalog:
            - Streams:stm_dwi_cjdl_rtems_power

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
        tdSql.checkRowsLoop(4,f"select val,tablename,ps_code,ps_name,province_name,area_name,company_name,ps_type,index_seq from {self.dbname}.{self.outTbname} order by tablename;",100,1)
        self.checkResultWithResultFile()


    def createStream(self):
        tdLog.info(f"create stream :")
        stream1 = (
                    f"""create stream test1.stm_dwi_cjdl_rtems_power interval(5m) sliding(5m) from test1.stb_cjdl_rtems 
                    partition by tbname,ps_code,ps_name,province_name,area_name,company_name,ps_type,index_seq
                    stream_options(max_delay(4m)|pre_filter(index_code in ('a0','a2') and  dt >= today() - 1d ))
                    into test1.stb_dwi_cjdl_rtems_power output_subtable(concat_ws('_','stb_dwi_cjdl_rtems_power',ps_code)) 
                    tags(
                    tablename varchar(50) as tbname,
                    ps_code varchar(50) as ps_code,
                    ps_name varchar(50) as ps_name,
                    province_name varchar(50) as province_name,
                    area_name varchar(50) as area_name,
                    company_name varchar(50) as company_name,
                    ps_type varchar(50) as ps_type,
                    index_seq varchar(50) as index_seq
                    )
                    as select
                        _twstart ts,
                        avg(factv) val
                    from
                        %%trows;
                    """
        )
        
        
        tdSql.execute(stream1,queryTimes=2)
        tdLog.info(f"create stream success!")
    
    def checkResultWithResultFile(self):
        chkSql = f"select val,tablename,ps_code,ps_name,province_name,area_name,company_name,ps_type,index_seq from {self.dbname}.{self.outTbname} order by tablename;"
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
        tdSql.execute("""CREATE STABLE test1.stb_cjdl_rtems (dt TIMESTAMP , ifch DOUBLE , factv DOUBLE , cycle DOUBLE , `state` DOUBLE , ts DOUBLE , dq DOUBLE ) 
                    TAGS (senid VARCHAR(20), index_code VARCHAR(255), index_name VARCHAR(255), ps_code VARCHAR(255), ps_type VARCHAR(255), 
                    ps_name VARCHAR(255), sen_name VARCHAR(255), province_name VARCHAR(255), area_name VARCHAR(255), company_name VARCHAR(255), 
                    index_seq VARCHAR(255), jz_seq VARCHAR(255), jz_no VARCHAR(255), jz_location VARCHAR(255)
        )""")

        tdSql.execute("CREATE TABLE test1.`a0` USING test1.`stb_cjdl_rtems` TAGS ('a0','a0','a0','a0','a0','a0','a0','a0','a0','a0','a0','a0','a0','a0')")
        tdSql.execute("CREATE TABLE test1.`a1` USING test1.`stb_cjdl_rtems` TAGS ('a1','a1','a1','a1','a1','a1','a1','a1','a1','a1','a1','a1','a1','a1')")
        tdSql.execute("CREATE TABLE test1.`a2` USING test1.`stb_cjdl_rtems` TAGS ('a2','a2','a2','a2','a2','a2','a2','a2','a2','a2','a2','a2','a2','a2')")
        
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
            c4 = random.randint(0, 1000)
            c5 = random.randint(0, 1000)
            c6 = random.randint(0, 1000)
            for tb in tables:
                sql = "INSERT INTO test1.%s VALUES (%d,%d,%d,%d,%d,%d,%d)" % (tb, ts, c1,c2,c3,c4,c5,c6)
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
            c2 = random.randint(0, 1000)
            c3 = random.randint(0, 1000)
            c4 = random.randint(0, 1000)
            c5 = random.randint(0, 1000)
            c6 = random.randint(0, 1000)
            for tb in tables:
                sql1 = "INSERT INTO test1.%s VALUES (now,%d,%d,%d,%d,%d,%d)" % (tb, c1,c2,c3,c4,c5,c6)
                sql2 = "INSERT INTO test1.%s VALUES (now+5m,%d,%d,%d,%d,%d,%d)" % (tb, c1,c2+2,c3,c4,c5,c6)
                sql3 = "INSERT INTO test1.%s VALUES (now+350s,%d,%d,%d,%d,%d,%d)" % (tb, c1+1,c2+1,c3,c4,c5,c6)
                sql4 = "INSERT INTO test1.%s VALUES (now+10m,%d,%d,%d,%d,%d,%d)" % (tb, c1+10,c2+10,c3,c4,c5,c6)
                
                tdSql.execute(sql1)          
                tdSql.execute(sql2)          
                tdSql.execute(sql3)          
                tdSql.execute(sql4)          

        
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
