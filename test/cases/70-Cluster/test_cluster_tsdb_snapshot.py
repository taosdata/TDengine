###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

from new_test_framework.utils import tdLog, tdSql, epath, sc, tdCom, cluster
import time
import os
import glob
import shutil
import subprocess
from datetime import datetime, timedelta
import platform
import json


class TestClusterTsdbSnapshot:
    def setup_class(cls):
        # super(TDTestCase, self).init(conn, logSql, replicaVar=3, db="db")
        tdLog.debug(f"start to excute {__file__}")
        cls.replicaVar = 3
        cls.vgroupNum = 3
        cls.dbName = 'test'
        cls.dnode1Path = tdCom.getTaosdPath()
        cls.dnode1Cfg = f'{cls.dnode1Path}/cfg'
        cls.dnode1Log = f'{cls.dnode1Path}/log'

    def _write_bulk_data(self):
        tdLog.info("============== write bulk data ===============")
        json_content = f"""
{{
    "filetype": "insert",
    "cfgdir": "{self.dnode1Cfg}",
    "host": "localhost",
    "port": 6030,
    "user": "root",
    "password": "taosdata",
    "connection_pool_size": 8,
    "thread_count": 16,
    "create_table_thread_count": 10,
    "result_file": "./insert_res.txt",
    "confirm_parameter_prompt": "no",
    "insert_interval": 0,
    "interlace_rows": 5,
    "num_of_records_per_req": 1540,
    "prepared_rand": 10000,
    "chinese": "no",
    "databases": [
        {{
            "dbinfo": {{
                "name": "{self.dbName}",
                "drop": "yes",
                "vgroups": 5,
                "duration": "10d",
                "wal_retention_period": 0,
                "replica": 3,
                "stt_trigger": 2
            }},
            "super_tables": [
                {{
                    "name": "stb",
                    "child_table_exists": "no",
                    "childtable_count": 100,
                    "childtable_prefix": "ctb",
                    "escape_character": "yes",
                    "auto_create_table": "no",
                    "batch_create_tbl_num": 500,
                    "data_source": "rand",
                    "insert_mode": "taosc",
                    "non_stop_mode": "no",
                    "line_protocol": "line",
                    "insert_rows": 10000,
                    "interlace_rows": 0,
                    "insert_interval": 0,
                    "partial_col_num": 0,
                    "disorder_ratio": 0,
                    "disorder_range": 0,
                    "timestamp_step": 1000,
                    "start_timestamp": "{(datetime.now() - timedelta(days=1)).replace(hour=10, minute=0, second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S')}",
                    "use_sample_ts": "no",
                    "tags_file": "",
                    "columns": [
                        {{
                            "type": "bigint",
                            "count": 10
                        }}
                    ],
                    "tags": [
                        {{
                            "type": "TINYINT",
                            "name": "groupid",
                            "max": 10,
                            "min": 1
                        }},
                        {{
                            "name": "location",
                            "type": "BINARY",
                            "len": 16,
                            "values": [
                                "beijing",
                                "shanghai"
                            ]
                        }}
                    ]
                }}
            ]
        }}
    ]
}}
"""
        if platform.system().lower() == "windows":
            json_file = os.path.abspath("test.json")
            safe_json_content = json_content.replace("\\", "\\\\")
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(json.loads(safe_json_content), f, ensure_ascii=False, indent=2)
        else:
            json_file = '/tmp/test.json'
            with open(json_file, 'w') as f:
                f.write(json_content)
        # Use subprocess.run() to wait for the command to finish
        subprocess.run(f'taosBenchmark -f {json_file}', shell=True, check=True)

    def _write_bulk_data2(self):
        tdLog.info("============== write bulk data ===============")
        json_content = f"""
{{
    "filetype": "insert",
    "cfgdir": "{self.dnode1Cfg}",
    "host": "localhost",
    "port": 6030,
    "user": "root",
    "password": "taosdata",
    "connection_pool_size": 8,
    "thread_count": 16,
    "create_table_thread_count": 10,
    "result_file": "./insert_res.txt",
    "confirm_parameter_prompt": "no",
    "insert_interval": 0,
    "interlace_rows": 5,
    "num_of_records_per_req": 1540,
    "prepared_rand": 10000,
    "chinese": "no",
    "databases": [
        {{
            "dbinfo": {{
                "name": "{self.dbName}",
                "drop": "no",
                "vgroups": 5,
                "duration": "10d",
                "wal_retention_period": 0,
                "replica": 3,
                "stt_trigger": 2
            }},
            "super_tables": [
                {{
                    "name": "stb",
                    "child_table_exists": "yes",
                    "childtable_count": 100,
                    "childtable_prefix": "ctb",
                    "escape_character": "yes",
                    "auto_create_table": "no",
                    "batch_create_tbl_num": 500,
                    "data_source": "rand",
                    "insert_mode": "taosc",
                    "non_stop_mode": "no",
                    "line_protocol": "line",
                    "insert_rows": 10000,
                    "interlace_rows": 0,
                    "insert_interval": 0,
                    "partial_col_num": 0,
                    "disorder_ratio": 0,
                    "disorder_range": 0,
                    "timestamp_step": 1000,
                    "start_timestamp": "{(datetime.now() - timedelta(days=1)).replace(hour=14, minute=0, second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S')}",
                    "use_sample_ts": "no",
                    "tags_file": "",
                    "columns": [
                        {{
                            "type": "bigint",
                            "count": 10
                        }}
                    ],
                    "tags": [
                        {{
                            "type": "TINYINT",
                            "name": "groupid",
                            "max": 10,
                            "min": 1
                        }},
                        {{
                            "name": "location",
                            "type": "BINARY",
                            "len": 16,
                            "values": [
                                "beijing",
                                "shanghai"
                            ]
                        }}
                    ]
                }}
            ]
        }}
    ]
}}
"""
        if platform.system().lower() == "windows":
            json_file = os.path.abspath("test.json")
            safe_json_content = json_content.replace("\\", "\\\\")
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(json.loads(safe_json_content), f, ensure_ascii=False, indent=2)
        else:
            json_file = '/tmp/test.json'
            with open(json_file, 'w') as f:
                f.write(json_content)
        # Use subprocess.run() to wait for the command to finish
        subprocess.run(f'taosBenchmark -f {json_file}', shell=True, check=True)

    def test_tsdb_snapshot(self):
        """Cluster remove wal files

        1. Create 3 dnode cluster environment
        2. taosBenchmark insert 1 stb 100 child tables with 3 replica
        3. flush database
        4. stop dnode 3
        5. taosBenchmark insert more data into the stb again
        6. flush database
        7. stop all dnodes
        8. remove wal files from dnode1 and dnode2
        9. start all dnodes
        10. check coredump not happen
        
        Since: v3.0.0.0

        Labels: common,ci

        Jira: None

        History:
            - 2025-10-23 Alex Duan Migrated from test/cases/uncatalog/army/cluster/test_tsdb_snapshot.py

        """
        tdLog.info("============== write bulk data ===============")
        self._write_bulk_data()

        tdSql.execute(f'flush database {self.dbName}')
        tdLog.sleep(10)

        tdLog.info("============== stop dnode 3 ===============")
        cluster.dnodes[2].stoptaosd()
        tdLog.sleep(10)

        # tdLog.info("============== write more data ===============")
        self._write_bulk_data2()

        tdSql.execute(f'flush database {self.dbName}')
        tdLog.sleep(10)

        cluster.dnodes[0].stoptaosd()
        cluster.dnodes[1].stoptaosd()

        dnode1_wal = f'{self.dnode1Path}/data/vnode/vnode2/wal'
        dnode2_wal = f'{self.dnode1Path}/../dnode2/data/vnode/vnode2/wal'

        tdLog.info("============== remove wal files ===============")
        tdLog.info(f"{dnode1_wal}")
        tdLog.info(f"{dnode2_wal}")
        os.system(f'rm -rf {dnode1_wal}/*')
        os.system(f'rm -rf {dnode2_wal}/*')

        tdLog.info("============== restart cluster ===============")
        cluster.dnodes[0].starttaosd()
        cluster.dnodes[1].starttaosd()
        cluster.dnodes[2].starttaosd()

        tdLog.sleep(10)

        tdLog.success(f"{__file__} successfully executed")


