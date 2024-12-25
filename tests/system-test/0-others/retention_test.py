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


import os
import time
from util.log import *

from util.cases import *
from util.sql import *
from util.common import *
from util.sqlset import *
import subprocess
from datetime import datetime, timedelta


class TDTestCase:
    def _prepare_env1(self):
        tdLog.info("============== prepare environment 1 ===============")

        level_0_path = f'{self.dnode_path}/data00'
        cfg = {
            level_0_path: 'dataDir',
        }
        tdSql.createDir(level_0_path)
        tdDnodes.stop(1)
        tdDnodes.deploy(1, cfg)
        tdDnodes.start(1)

    def _prepare_env2(self):
        tdLog.info("============== prepare environment 2 ===============")

        level_0_path = f'{self.dnode_path}/data00'
        level_1_path = f'{self.dnode_path}/data01'
        cfg = {
            f'{level_0_path}': 'dataDir',
            f'{level_1_path} 1 0': 'dataDir',
        }
        tdSql.createDir(level_1_path)
        tdDnodes.stop(1)
        tdDnodes.deploy(1, cfg)
        tdDnodes.start(1)

    def _write_bulk_data(self):
        tdLog.info("============== write bulk data ===============")
        json_content = f"""
{{
    "filetype": "insert",
    "cfgdir": "{self.cfg_path}",
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
                "name": "{self.db_name}",
                "drop": "yes",
                "vgroups": {self.vgroups},
                "duration": "1d",
                "keep": "3d,6d",
                "wal_retention_period": 0,
                "stt_trigger": 1
            }},
            "super_tables": [
                {{
                    "name": "stb",
                    "child_table_exists": "no",
                    "childtable_count": 1000,
                    "childtable_prefix": "ctb",
                    "escape_character": "yes",
                    "auto_create_table": "no",
                    "batch_create_tbl_num": 500,
                    "data_source": "rand",
                    "insert_mode": "taosc",
                    "non_stop_mode": "no",
                    "line_protocol": "line",
                    "insert_rows": 10000,
                    "childtable_limit": 10,
                    "childtable_offset": 100,
                    "interlace_rows": 0,
                    "insert_interval": 0,
                    "partial_col_num": 0,
                    "disorder_ratio": 0,
                    "disorder_range": 1000,
                    "timestamp_step": 40000,
                    "start_timestamp": "{(datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d %H:%M:%S')}",
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
        json_file = '/tmp/test.json'
        with open(json_file, 'w') as f:
            f.write(json_content)
        # Use subprocess.run() to wait for the command to finish
        subprocess.run(f'taosBenchmark -f {json_file}', shell=True, check=True)

    def _check_retention(self):
        for vgid in range(2, 2+self.vgroups):
            tsdb_path = self.dnode_path+f'/data01/vnode/vnode{vgid}/tsdb'
            # check the path should not be empty
            if not os.listdir(tsdb_path):
                tdLog.error(f'{tsdb_path} is empty')
                assert False

    def run(self):
        self._prepare_env1()
        self._write_bulk_data()
        tdSql.execute(f'flush database {self.db_name}')
        tdDnodes.stop(1)

        self._prepare_env2()
        tdSql.execute(f'trim database {self.db_name}')

        time.sleep(10)

        self._check_retention()

    def init(self, conn, logSql, replicaVar=1):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.dnode_path = tdCom.getTaosdPath()
        self.cfg_path = f'{self.dnode_path}/cfg'
        self.log_path = f'{self.dnode_path}/log'
        self.db_name = 'test'
        self.vgroups = 10

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
