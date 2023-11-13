###################################################################
#           Copyright (c) 2020 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
# -*- taostest --setup=bug_regression/improve_td26412.yaml --case=bug_regression/improve_td26412.py --keep -*-

from taostest import TDCase, T
from taostest.util.common import TDCom
from datetime import datetime
import os
from taostest.util.remote import Remote
import psutil
# from taostest.performance.result_reduction import Perf_Base_func
from taostest.components import PrometheusServer


class TestTd26412(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.taosd_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")
        self.prometheus_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "prometheus")
        self.host = self.taosd_setting["spec"]["dnodes"][0]["endpoint"].split(":")[0]
        self._remote: Remote = Remote(self.logger)
        self.Prometheus = PrometheusServer(self._remote)
        self.column_count = 150
        self.col_pre = "c"
        self.col_list = list(map(lambda x:f'{self.col_pre}{x}', range(self.column_count)))
        self.col_str = ",".join(self.col_list)
        self.json_file_name1 = "insert0.json"
        self.json_file_name2 = "insert0.json"
        self.replica = int(os.environ["DATABASE_REPLICAS"]) if "DATABASE_REPLICAS" in os.environ else 1
        self.vgroups = 2
        self.create_table_thread_count = 40
        self.thread_count = 40
        self.num_of_records_per_req = 500
        self.childtable_count = 10000
        self.fill_history_rows = 100
        self.insert_rows = 10000
        self.fill_history_start_timestamp = "2020-01-01 00:00:00"
        self.stbname = "stb"
        self.dbname = "stream_test"
        self.fill_history = "1"
        self.stream_stbname = "output_streamtb"
        self.stream_name = "test_stream"
        self.trigger_mode = "at_once"
        self.stream_sql = f"select ts,{self.col_str} from {self.dbname}.{self.stbname} partition by tbname"
        self.child_table_exists = "no"
        self.db_drop = "yes"
        self.wal_retention_period = 300
        self.stream_drop = "yes"
        self.taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
        self.taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        self.keep_trying = -1
        self.trying_interval = 10000
        self.interlace_rows = 500
        self.column_info_list = [
            {
              "type": "INT",
              "count": self.column_count
            }
        ]
        self.tag_info_list = [
            {
              "type": "INT",
              "count": 1
            }
        ]
        # vg_count * 128（单个memTable大小， 单位为M） * 3(memtable 个数限制, 单位为M) + vg_count * 128（cache ）
        self.mem_standard = self.vgroups * 128 * 3 + self.vgroups * 128
        self.limit = self.mem_standard * 2
        
    def prepare_fill_history_data(self):
        self.json_filename_list = [self.json_file_name1]
        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, wal_retention_period=self.wal_retention_period)
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.fill_history_rows, start_timestamp=self.fill_history_start_timestamp, child_table_exists=self.child_table_exists, name=self.stbname, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name1, json_info)
        self.json_data_list = [json_info]
        self.tdCom.put_file(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.run_log_dir)
        self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)

    def insert_data(self):
        self.json_filename_list = [self.json_file_name2]
        self.start_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        self.child_table_exists = "yes"
        self.db_drop = "no"
        stream_db_info = self.tdCom.setStreamDBinfo(name=self.dbname, vgroups=self.vgroups, drop=self.db_drop)
        stream_info = self.tdCom.setStreams(stream_name=self.stream_name, stream_stb=f'{self.dbname}.{self.stream_stbname}', trigger_mode=self.trigger_mode, drop=self.stream_drop, source_sql=self.stream_sql, fill_history=self.fill_history)
        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop)
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=self.start_timestamp, child_table_exists=self.child_table_exists, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, streams=stream_info, stream_db=stream_db_info, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name2, json_info)
        self.json_data_list = [json_info]
        self.tdCom.put_file(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.run_log_dir)
        self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)

    def run(self):
        self.prepare_fill_history_data()
        timestamp_start = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        self.insert_data()
        timestamp_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        taosd_avg_mem = self.Prometheus.cal_range_avg(self.prometheus_setting, "mem_usage", timestamp_start, timestamp_end, 60)[0]
        self.tdSql.checkEqual(taosd_avg_mem<=self.mem_standard, True)

    def cleanup(self):
        pass

    def desc(self) -> str:
        case_description = """
            test_td22412
        """
        return case_description

    def author(self) -> str:
        return "Jayden"

    def tags(self):
        return T.Write