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
                "name": "ts5617",
                "drop": "yes",
                "vgroups": 10,
                "precision": "ms",
		"buffer": 512,
		"cachemodel":"'both'",
		"stt_trigger": 1
            },
            "super_tables": [
                {
                    "name": "stb_2_2_1",
                    "child_table_exists": "no",
                    "childtable_count": 10000,
                    "childtable_prefix": "d_",
                    "auto_create_table": "yes",
                    "batch_create_tbl_num": 10,
                    "data_source": "csv",
                    "insert_mode": "stmt",
                    "non_stop_mode": "no",
                    "line_protocol": "line",
                    "insert_rows": 10000,
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
                        {"type": "INT", "name": "id", "max": 100, "min": 1}
                    ]
                }
            ]
        }
    ]
}'''

class TDTestCase:
    updatecfgDict = {'debugFlag': 135, 'asynclog': 0, 'streamFailedTimeout': 10000}
    clientCfgDict = {'debugFlag': 135, 'asynclog': 0}
    updatecfgDict["clientCfg"] = clientCfgDict
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        #tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def run(self):

        with open('ts-5617.json', 'w') as file:
            file.write(insertJson)

        tdLog.info("start to insert data: taosBenchmark -f ts-5617.json")
        if os.system("taosBenchmark -f ts-5617.json") != 0:
            tdLog.exit("taosBenchmark -f ts-5617.json")
        
        tdLog.info("test creating stream with history in normal ......")
        start_time = time.time()
        tdSql.execute(f'create stream s21 fill_history 1 async into ts5617.st21 tags(tname varchar(20)) subtable(tname) as select last(val), last(quality) from ts5617.stb_2_2_1 partition by tbname tname interval(1800s);')
        end_time = time.time()
        if end_time - start_time > 1:
            tdLog.exit("create history stream sync too long")

        tdSql.query("show streams")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "init")

        while 1:
            tdSql.query("show streams")
            tdLog.info(f"streams is creating ...")
            if tdSql.getData(0, 1) == "ready":
                break
            else:
                time.sleep(5)

        tdSql.execute(f'drop stream s21')
        tdSql.execute(f'drop table if exists ts5617.st21')
        
        tdLog.info("test creating stream with history in taosd error ......")
        tdSql.execute(f'create stream s211 fill_history 1 async into ts5617.st211 tags(tname varchar(20)) subtable(tname) as select last(val), last(quality) from ts5617.stb_2_2_1 partition by tbname tname interval(1800s);')
        tdSql.execute(f'create stable ts5617.st211(ts timestamp, i int) tags(tname varchar(20))')

        tdSql.query("show streams")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "init")

        while 1:
            tdSql.query("show streams")
            tdLog.info(f"streams is creating ...")
            tdLog.info(tdSql.queryResult)

            if tdSql.getData(0, 1) == "failed" and tdSql.getData(0, 2) == "STable already exists":
                break
            else:
                time.sleep(5)
            time.sleep(10)
        tdSql.execute(f'drop stream s211')
        tdSql.execute(f'drop table if exists ts5617.st211')
        
        tdLog.info("test creating stream with history in taosd error ......")
        tdSql.execute(f'create stream s21 fill_history 1 async into ts5617.st21 as select last(val), last(quality) from ts5617.d_0 interval(1800s);')
        tdSql.execute(f'create stream s211 fill_history 1 async into ts5617.st211 as select last(val), last(quality) from ts5617.d_0 interval(1800s);')

        while 1:
            tdSql.query("show streams")
            tdLog.info(tdSql.queryResult)

            tdLog.info(f"streams is creating ...")
            if "failed" in [tdSql.getData(0, 1), tdSql.getData(1, 1)]:
                break
            else:
                time.sleep(5)

        tdSql.execute(f'drop stream s21')
        tdSql.execute(f'drop stream s211')
        tdSql.execute(f'drop table if exists ts5617.st21')
        tdSql.execute(f'drop table if exists  ts5617.st211')

        tdLog.info("test creating stream with history in taosd restart ......")
        tdSql.execute(f'create stream s21 fill_history 1 async into ts5617.st21 tags(tname varchar(20)) subtable(tname) as select last(val), last(quality) from ts5617.stb_2_2_1 partition by tbname tname interval(1800s);')
        tdSql.query("show streams")
        tdSql.checkRows(1)
        tdSql.checkData(0, 1, "init")

        tdLog.debug("restart taosd")
        tdDnodes.forcestop(1)
        time.sleep(20)
        tdDnodes.start(1)

        while 1:
            tdSql.query("show streams")
            tdLog.info(f"streams is creating ...")
            tdLog.info(tdSql.queryResult)
            if tdSql.getData(0, 1) == "failed":
                break
            else:
                time.sleep(5)
         
        return

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
