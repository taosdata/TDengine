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
    caseName = "test_three_gorges_case4_bug1"
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
    outTbname = "str_cjdl_point_data_szls_jk_test"
    streamName = "str_cjdl_point_data_szls_jk_test"
    tableList = []
    resultIdx = "1"
    
    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")

    def test_three_gorges_case4_bug1(self):
        """test_three_gorges_case
        
        1. create snode
        2. create stream


        Catalog:
            - Streams:str_cjdl_point_data_szls_jk_test

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
        tdSql.checkResultsByFunc(
            f"select * from {self.dbname}.str_cjdl_point_data_szls_jk_test",
            lambda: tdSql.getRows() > 0
        )
        
        # insert expired time data (expired_time:3d)
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=5)
        base_ts = int(time.mktime(datetime.datetime.combine(yesterday, datetime.time.min).timetuple())) * 1000
        tdLog.info(f"write a data from 5 days ago")
        tdSql.execute(f"insert into {self.dbname}.a1 values({base_ts},998,998) ;") #先写入一条当前时间5天前数据 (比如 7.14 号)
        tdLog.info(f"Write the time data for the next 2 days")
        tdSql.execute(f"insert into {self.dbname}.a1 values({base_ts+86400000*7},999,999) ;") #写入未来2天时间数据(7.21号)
        
        time.sleep(3)
        tdLog.info(f"Write the time data for the next 1 days")
        tdSql.execute(f"insert into {self.dbname}.a1 values({base_ts+86400000*6},997,997) ;")#写入未来1天时间数据（7.20 号）
        # time.sleep(5)
        # #检查过去 5 天数据是否写入
        # tdSql.query(f"select * from {self.dbname}.str_cjdl_point_data_szls_jk_test where _c0 <= today()-5d")
        # tdLog.info(f"select * from {self.dbname}.str_cjdl_point_data_szls_jk_test where _c0 <= today()-5d")
        # if tdSql.getRows() != 1:
        #     raise Exception("ERROR: result is now right!")
        
        # #检查未来 1 天数据是否写入
        # tdSql.query(f"select * from {self.dbname}.str_cjdl_point_data_szls_jk_test where _c0 >today()")
        # tdLog.info(f"select * from {self.dbname}.str_cjdl_point_data_szls_jk_test where _c0 >today()")
        # if tdSql.getRows() != 1:
        #     raise Exception("ERROR: result is now right!")
        tdSql.checkRowsLoop(6,f"select val,senid,senid_name from {self.dbname}.{self.outTbname} order by _c0;",200,1)
        self.checkResultWithResultFile()
        

    def createStream(self):
        tdLog.info(f"create stream :")
        stream = (
                    f"""create stream {self.dbname}.str_cjdl_point_data_szls_jk_test interval(1m) sliding(1m) from {self.dbname}.stb_cjdl_point_data 
                    partition by tbname,senid,senid_name 
                    stream_options(expired_time(3d)|low_latency_calc|fill_history|pre_filter(tag_temp='A001'))
                    into {self.dbname}.str_cjdl_point_data_szls_jk_test 
                    output_subtable(concat_ws('_','cjdl_point_data_szls_jk_test',senid)) 
                    tags(`tbname` varchar(255) as tbname,senid varchar(255) as senid,senid_name varchar(255) as senid_name)
                    as select
                        last(ts) as ts,
                        last(val) as val
                    from
                        %%trows t1 ;
                    """
        )
        tdSql.execute(stream,queryTimes=2)
        tdLog.info(f"create stream success!")
    
    def sxny_data1(self):
        

        random.seed(42)
        tdSql.execute("create database test1 vgroups 6;")
        tdSql.execute(f"use {self.dbname}")
        tdSql.execute("""CREATE STABLE `stb_cjdl_point_data` (`ts` TIMESTAMP , `st` DOUBLE , `val` DOUBLE ) 
                            TAGS (`id` VARCHAR(20), `senid` VARCHAR(255), `senid_name` VARCHAR(255), `tag_temp` VARCHAR(255))  ;
        )""")

        tdSql.execute("CREATE TABLE test1.`a0` USING test1.`stb_cjdl_point_data` TAGS ('a0','sendid_a0','name_a0','A000')")
        tdSql.execute("CREATE TABLE test1.`a1` USING test1.`stb_cjdl_point_data` TAGS ('a1','sendid_a1','name_a1','A001')")
        tdSql.execute("CREATE TABLE test1.`a2` USING test1.`stb_cjdl_point_data` TAGS ('a2','sendid_a2','name_a2','A002')")

        tables = ['a0', 'a1', 'a2']

        
        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)
        base_ts = int(time.mktime(datetime.datetime.combine(yesterday, datetime.time.min).timetuple())) * 1000

        interval_ms = 30 * 1000  
        total_rows = 10

        for i in range(total_rows):
            ts = base_ts + i * interval_ms
            c1 = random.randint(0, 1000)
            c2 = random.randint(0, 1000)
            for tb in tables:
                sql = "INSERT INTO test1.%s VALUES (%d,%d,%d)" % (tb, ts, c1,c2)
                tdSql.execute(sql)

    def checkResultWithResultFile(self):
        chkSql = f"select val,senid,senid_name from {self.dbname}.{self.outTbname} order by _c0;"
        tdLog.info(f"check result with sql: {chkSql}")
        if tdSql.getRows() >0:
            tdCom.generate_query_result_file(self.caseName, self.resultIdx, chkSql)
            tdCom.compare_query_with_result_file(self.resultIdx, chkSql, f"{self.currentDir}/ans/{self.caseName}.{self.resultIdx}.csv", self.caseName)
            tdLog.info("check result with result file succeed")        
        

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
