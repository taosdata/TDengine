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


from new_test_framework.utils import tdLog, tdSql, tdDnodes, tdCom
import os
import time
import platform

import subprocess
from datetime import datetime, timedelta

def get_disk_usage(path):
    try:
        result = subprocess.run(['du', '-sb', path], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if result.returncode == 0:
            # The output is in the format "size\tpath"
            size = int(result.stdout.split()[0])
            return size
        else:
            print(f"Error: {result.stderr}")
            return None
    except Exception as e:
        print(f"Exception occurred: {e}")
        return None

class TestShowDiskUsageMultilevel:

    def setup_class(cls):
        tdLog.debug("start to execute %s" % __file__)

        cls.dnode_path = tdCom.getTaosdPath()
        cls.cfg_path = os.path.join(cls.dnode_path, 'cfg')
        if platform.system() == 'Windows':
            cls.cfg_path = cls.cfg_path.replace('\\', '\\\\')
        cls.log_path = os.path.join(cls.dnode_path, 'log')
        cls.db_name = 'test'
        cls.vgroups = 10

    def _prepare_env1(self):
        tdLog.info("============== prepare environment 1 ===============")

        level_0_path = os.path.join(self.dnode_path, 'data00')
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
        json_file = os.path.join(os.path.dirname(__file__), "test.json")
        with open(json_file, 'w') as f:
            f.write(json_content)
        # Use subprocess.run() to wait for the command to finish
        subprocess.run(f'taosBenchmark -f {json_file}', shell=True, check=True)

    def _check_retention(self):
        for vgid in range(2, 2+self.vgroups):
            tsdb_path = os.path.join(self.dnode_path, "data01", "vnode", f"vnode{vgid}", "tsdb")
            # check the path should not be empty
            if not os.listdir(tsdb_path):
                tdLog.error(f'{tsdb_path} is empty')
                assert False
    def _calculate_disk_usage(self, path):
        size = 0
        for vgid in range(2, 2+self.vgroups): 
            tsdb_path = os.path.join(self.dnode_path, path, "vnode", f"vnode{vgid}", "tsdb")
            size += get_disk_usage(tsdb_path) 
        return int(size/1024)

    def _value_check(self, size1, size2, threshold=1000):
        if abs(size1 - size2) < threshold:
            tdLog.info(f"checkEqual success, base_value={size1},check_value={size2}") 
        else :
            tdLog.exit(f"checkEqual error, base_value=={size1},check_value={size2}")
                    

    def test_show_disk_usage_multilevel(self):
        """summary: xxx

        description: xxx

        Since: xxx

        Labels: xxx

        Jira: xxx

        Catalog:
            - xxx:xxx

        History:
            - xxx
            - xxx
        """
        self._prepare_env1()
        self._write_bulk_data()
        tdSql.execute(f'flush database {self.db_name}')
        tdDnodes.stop(1)

        self._prepare_env2()
        tdSql.execute(f'trim database {self.db_name}')

        time.sleep(10)

        self._check_retention()

        size1 = self._calculate_disk_usage('data00')
        size2 = self._calculate_disk_usage('data01')

        tdSql.query(f'select sum(data1), sum(data2) from information_schema.ins_disk_usage where db_name="{self.db_name}"')  
        data1 = int(tdSql.queryResult[0][0])  
        data2 = int(tdSql.queryResult[0][1])  

        self._value_check(size1, data1)
        self._value_check(size2, data2)

        tdLog.success("%s successfully executed" % __file__)


