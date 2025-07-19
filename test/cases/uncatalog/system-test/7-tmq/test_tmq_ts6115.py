import sys
import os


from new_test_framework.utils import tdLog, tdCom

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
                "name": "ts6115",
                "drop": "yes",
                "vgroups": 10,
                "precision": "ms",
		"buffer": 512,
		"cachemodel":"'both'",
		"stt_trigger": 1
            },
            "super_tables": [
                {
                    "name": "stb",
                    "child_table_exists": "no",
                    "childtable_count": 10000,
                    "childtable_prefix": "d_",
                    "auto_create_table": "yes",
                    "batch_create_tbl_num": 10,
                    "data_source": "csv",
                    "insert_mode": "stmt",
                    "non_stop_mode": "no",
                    "line_protocol": "line",
                    "insert_rows": 1000,
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

class TestCase:
    updatecfgDict = {'debugFlag': 135, 'asynclog': 0}
    clientCfgDict = {'debugFlag': 135, 'asynclog': 0}
    updatecfgDict["clientCfg"] = clientCfgDict
    def setup_class(cls):
        tdLog.debug(f"start to excute {__file__}")

    def test_tmq_ts6115(self):
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

        with open('ts-6115.json', 'w') as file:
            file.write(insertJson)

        tdLog.info("start to insert data: taosBenchmark -f ts-6115.json")
        if os.system("taosBenchmark -f ts-6115.json") != 0:
            tdLog.exit("taosBenchmark -f ts-6115.json")

        tdLog.info("test ts-6115 ......")

        buildPath = tdCom.getBuildPath()
        cmdStr = '%s/build/bin/tmq_ts6115'%(buildPath)

        tdLog.info(cmdStr)
        if os.system(cmdStr) != 0:
            tdLog.exit(cmdStr)

        tdLog.success(f"{__file__} successfully executed")

