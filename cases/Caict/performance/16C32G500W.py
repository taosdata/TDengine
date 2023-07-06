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

from taostest.util.common import TDCom
from taostest import TDCase
from taostest.components.taosd import TaosD
from taostest.util.remote import Remote
from taostest.performance.result_reduction import Perf_Base_func

class Test16C32G500W(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.file_name = "insert0.json"
        self.vgroups = 16
        self.create_table_thread_count = 16
        self.thread_count = 16
        self.childtable_count = 10000
        self.insert_rows = 100000
        self.num_of_records_per_req = 10000
        self.batch_create_tbl_num = 10000
        self.dbname = "test"
        self.stbname = "stb"
        self.insert_mode = "stmt"

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def run(self):
        taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
        taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")

        column_info_list = [
          {
            "type": "INT",
            "count": 3
          }
        ]
        tag_info_list = [
          {
            "type": "INT",
            "count": 1
          }
        ]

        json_data_list = list()
        json_filename_list = list()

        json_filename_list.append(self.file_name)
        dbinfo = self.tdCom.setDBinfo(name=self.dbname, vgroups=self.vgroups)
        stb_into = [self.tdCom.setStbinfo(columns=column_info_list, tags=tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, batch_create_tbl_num=self.batch_create_tbl_num, insert_mode=self.insert_mode)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name, json_info)
        json_data_list.append(json_info)

        self.tdCom.put_file(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, self.run_log_dir)
        # timestamp_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        result_file_list = self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, taosBenchmark_env_setting, self.run_log_dir)
        # timestamp_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        # env_setting = self.get_component_by_name("prometheus")
        Insert_file = Perf_Base_func(self._remote._logger, self.run_log_dir)
        Insert_file.taosBenchmark_insert_summary_result(result_file_list, version="3.0")
        # Insert_file.get_process_exporter_info(env_setting, 30, timestamp_start, timestamp_end)
        # Insert_file.get_node_exporter_info(env_setting, 30, timestamp_start, timestamp_end)
        print(self.run_log_dir + '/perf_report.txt')
