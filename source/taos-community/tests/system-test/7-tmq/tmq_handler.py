import random
import taos
import sys
import time
import socket
import os
import threading

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *

sys.path.append("./7-tmq")
from taos.tmq import Consumer

insertJson = '''{
    "filetype": "insert",
    "cfgdir": "/etc/taos",
    "host": "localhost",
    "port": 6030,
    "user": "root",
    "password": "taosdata",
    "connection_pool_size": 10,
    "thread_count": 10,
    "create_table_thread_count": 10,
    "result_file": "./insert-2-2-1.txt",
    "confirm_parameter_prompt": "no",
    "num_of_records_per_req": 3600,
    "prepared_rand": 3600,
    "chinese": "no",
    "escape_character": "yes",
    "continue_if_fail": "no",
    "databases": [
        {
            "dbinfo": {
                "name": "tmq_alter_tag",
                "drop": "yes",
                "vgroups": 2,
                "precision": "ms",
		"buffer": 512,
		"cachemodel":"'both'",
		"stt_trigger": 1
            },
            "super_tables": [
                {
                    "name": "stb",
                    "child_table_exists": "no",
                    "childtable_count": 10,
                    "childtable_prefix": "d_",
                    "auto_create_table": "yes",
                    "batch_create_tbl_num": 10,
                    "data_source": "csv",
                    "insert_mode": "stmt",
                    "non_stop_mode": "no",
                    "line_protocol": "line",
                    "insert_rows": 100,
                    "childtable_limit": 0,
                    "childtable_offset": 0,
                    "interlace_rows": 0,
                    "insert_interval": 0,
                    "partial_col_num": 0,
                    "timestamp_step": 1000,
                    "start_timestamp": "2024-11-01 00:00:00.000",
                    "sample_format": "csv",
                    "sample_file": "./td_double10000_juchi.csv",
                    "use_sample_ts": "no",
                    "tags_file": "",
                    "columns": [
                        {"type": "DOUBLE", "name": "val"},
                        { "type": "INT", "name": "quality"}
                    ],
                    "tags": [
                        {"type": "INT", "name": "id", "max": 1000000, "min": 1}
                    ]
                }
            ]
        }
    ]
}'''

class TDTestCase:
    updatecfgDict = {'debugFlag': 135, 'asynclog': 1}
    clientCfgDict = {'debugFlag': 135, 'asynclog': 1}
    updatecfgDict["clientCfg"] = clientCfgDict

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        self.tableNum = 10
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())

    def consume(self):
        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe(["t0"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        cnt = 0
        try:
            while True:
                res = consumer.poll(10)
                if not res:
                    continue
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    print(block.fetchall(),len(block.fetchall()))
                    cnt += len(block.fetchall())
        finally:
            consumer.close()

    def insertDataBatch(self):
        with open('tmq_alter.json', 'w') as file:
            file.write(insertJson)

        tdLog.info("start to insert data: taosBenchmark -f tmq_alter.json")
        if os.system("taosBenchmark -f tmq_alter.json") != 0:
            tdLog.exit("taosBenchmark -f tmq_alter.json")

        tdLog.info("test tmq_alter ......")
    
    def insertData(self):
        time.sleep(5)
        while True:
            tableIndex = random.randint(1, self.tableNum) - 1;
            alterSql = f'insert into tmq_alter_tag.d_{tableIndex} values(now, 1, 1);'
            tdLog.info(f"insert data for table d_{tableIndex}")
            tdSql.execute(alterSql)
            sleepInterval = random.randint(600, 6000);
            tdLog.info(f"sleep {sleepInterval}")
            time.sleep(sleepInterval)

    def countTableNum(self):
        while True:
            tdSql.execute(f'use tmq_alter_tag')
            tdSql.query(f'select count(*) from information_schema.ins_tables where stable_name = "stb"')
            if tdSql.getData(0, 0) == self.tableNum :
                # wait some times to see the speed of writing
                tdLog.info(f"table count reach {self.tableNum}")
                time.sleep(1)
                break;
            time.sleep(1)
            tdLog.info(f"Waiting for table count to reach {self.tableNum}, current count is {tdSql.getData(0, 0)}")
            continue

    def run(self):
        insertBatchThread = threading.Thread(target=self.insertDataBatch)
        insertBatchThread.start()

        self.countTableNum()

        tdSql.execute(f'create topic t0 as select * from tmq_alter_tag.stb')

        insertThread = threading.Thread(target=self.insertData)
        insertThread.start()

        self.consume()

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
