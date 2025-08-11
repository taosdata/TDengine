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

        1. test_sliding_case3


        Catalog:
            - Streams:test_sliding_case3

        Since: v3.3.3.7

        Labels: common,ci

        Jira: None

        History:
            - 2025-7-8 lvze Created

        """

        tdStream.dropAllStreamsAndDbs()
        self.createSnodeTest()
        # tdSql.execute(f'create database test1 vgroups 10 ;')
        self.dataIn()
        # self.createTable()
        stream1 = (
        """ create stream db1.stream0 interval(10s) sliding(10s) from db1.stb into db1.stb_out as select _twstart, avg(c0) from %%trows """
        )



        tdSql.execute(stream1)
        self.checkStreamRunning()
        tdSql.query(f"select * from test1.st1out order by _c0;",queryTimes=20,show=True)
        raise Exception(f"error:no result data")


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
"rest_port": 6041,
"user": "root",
"password": "taosdata",
"thread_count": 10,
"thread_count_create_tbl": 20,
"result_file": ".vscode/insert_res.txt",
"confirm_parameter_prompt": "no",
"num_of_records_per_req": 2000000,
"connection_pool_size": 50,
"databases": [
{
"dbinfo": {
"name": "db1",
"drop": "yes",
"buffer": 100,
"maxRows": 4096,
"minRows": 100,
"duration": 1,
"pages": 1024,
"pagesize": 32,
"vgroups": 2,
"stt_trigger": 1
},
"super_tables": [
{
"name": "stb",
"child_table_exists": "no",
"childtable_count": 200000,
"childtable_prefix": "stb_",
"auto_create_table": "no",
"batch_create_tbl_num": 5000,
"no_check_for_affected_rows": "yes",
"data_source": "rand",
"insert_mode": "stmt",
"insert_rows": 1000,
"interlace_rows": 10,
"insert_interval": 500,
"max_sql_len": 1048576,
"disorder_ratio": 0,
"disorder_range": 1000,
"timestamp_step": 1000,
"start_timestamp": "2020-05-01 00:00:00.000",
"sample_format": "csv",
"use_sample_ts": "no",
"tags_file": "",
"columns": [
{
"type": "INT",
"max": 100,
"min": 1,
"count": 10
}
],
"tags": [
{
"type": "INT",
"count": 1,
"max": 100,
"min": 1
}
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