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
from taos.tmq import *
sys.path.append("./7-tmq")
from tmqCommon import *


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
                "name": "db_alter_tag",
                "drop": "yes",
                "vgroups": 1,
                "precision": "ms",
		"buffer": 512,
		"cachemodel":"'both'",
		"stt_trigger": 1
            },
            "super_tables": [
                {
                    "name": "stb",
                    "child_table_exists": "no",
                    "childtable_count": 500000,
                    "childtable_prefix": "d_",
                    "auto_create_table": "yes",
                    "batch_create_tbl_num": 10,
                    "data_source": "csv",
                    "insert_mode": "stmt",
                    "non_stop_mode": "no",
                    "line_protocol": "line",
                    "insert_rows": 1,
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
                        {"type": "TINYINT", "name": "groupid", "max": 10, "min": 1},
                        {"type": "BINARY",  "name": "location", "len": 16,
                            "values": ["San Francisco", "Los Angles", "San Diego",
                                "San Jose", "Palo Alto", "Campbell", "Mountain View",
                                "Sunnyvale", "Santa Clara", "Cupertino"]
                        }
                    ]
                }
            ]
        }
    ]
}'''

class TDTestCase:
    updatecfgDict = {'debugFlag': 135}
    clientCfgDict = {'debugFlag': 135}
    updatecfgDict["clientCfg"] = clientCfgDict
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        #tdSql.init(conn.cursor(), logSql)  # output sql.txt file

    def alterTag(self):
        tdSql.execute("create database db_alter_tag")
        tdSql.execute("use db_alter_tag")
        tdSql.execute("create table stb (ts timestamp, val double) tags (id int, location binary(16))")
        tdSql.execute("insert into tb1 using stb tags (1, 'San Francisco') values (now, 10.5)")
        tdSql.execute("insert into tb2 using stb tags (NULL, NULL) values (now, 20.5)")
        tdSql.execute("insert into tb3 using stb tags (NULL, 'San Diego') values (now, 30.5)")
        tdSql.execute("insert into tb4 using stb tags (4, NULL) values (now, 40.5)")

        tdSql.execute("ALTER TABLE tb1 SET TAG id = 1")
        tdSql.execute("ALTER TABLE tb1 SET TAG id = 1, location = 'San Francisco'")
        tdSql.execute("ALTER TABLE tb2 SET TAG id = NULL")
        tdSql.execute("ALTER TABLE tb2 SET TAG id = NULL, location = NULL")
        tdSql.execute("ALTER TABLE tb3 SET TAG id = NULL, location = 'San Diego'")
        tdSql.execute("ALTER TABLE tb4 SET TAG id = 4, location = NULL")
        tdSql.execute("ALTER TABLE tb4 SET TAG location = NULL")

    def consume(self):
        func_name = "tmq_alter_tag"
        print(f"start to excute {func_name}")
        tdSql.execute(f'use db_alter_tag')
        tdSql.execute(f'create topic {func_name} as stable stb where groupid > 2')

        consumer_dict = {
            "group.id": "g1",
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe([f"{func_name}"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        index = 0;
        try:
            while True:
                res = consumer.poll(5)
                print("index:", index)
                print(res)
                if not res:
                    print("res null")
                    break
                val = res.value()
                if val is None:
                    continue

                tdSql.execute(f"alter table d_1 set tag groupid = 3")
                                
                index += 1
                break
        finally:
            consumer.close()

    def insert(self):
        with open('ts-alter-tag.json', 'w') as file:
            file.write(insertJson)

        tdLog.info("start to insert data: taosBenchmark -f ts-alter-tag.json")
        if os.system("taosBenchmark -f ts-alter-tag.json") != 0:
            tdLog.exit("taosBenchmark -f ts-alter-tag.json")

        tdLog.info("test ts-alter-tag ......")

    def run(self):

        self.alterTag()
        # self.insert()
        # self.consume()

        return

    def stop(self):
        tdSql.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
