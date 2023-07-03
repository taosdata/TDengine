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

import os
from taostest.util.file import read_yaml
from taostest.util.common import TDCom
from typing import List
from taostest import TDCase
from taostest.components.taosd import TaosD
from taostest.util.remote import Remote
from taostest.performance.result_reduction import Perf_Base_func
from datetime import datetime
class Timeline_100B(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.result_file_name = ""
        self.file_name1 = "insert0.json"
        self.file_name2 = "insert1.json"
        self.file_name3 = "insert3.json"
        self._tmp_dir: str = os.path.join(self.run_log_dir, "tmp")
        self.replica = 1
        self.vgroups = 40
        self.create_table_thread_count = 40
        self.thread_count = 40
        self.childtable_count1 = 33333333
        self.childtable_count2 = 33333333
        self.childtable_count3 = 33333334
        self.interlace_rows = 5000
        # self.childtable_count1 = 100
        # self.childtable_count2 = 100
        # self.childtable_count3 = 100
        self.childtable_prefix1 = "ctb1_"
        self.childtable_prefix2 = "ctb2_"
        self.childtable_prefix3 = "ctb3_"
        self.insert_rows = 10
        self.num_of_records_per_req = 10000
        self.batch_create_tbl_num = 10000
        self.dbname = "test"
        self.stbname = "stb"


    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def run(self):
        expected_res = (self.childtable_count1 + self.childtable_count2 + self.childtable_count3) * self.insert_rows
        taosBenchmark_iplist: List = self.get_fqdn("taosBenchmark")
        taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")

        column_info_list = [
          {
            "type": "INT",
            "count": 99
          }
        ]
        tag_info_list = [
          {
            "type": "INT",
            "count": 1
          }
        ]

        self.tdSql.execute(f'drop database if exists {self.dbname}')
        self.tdSql.execute(f'create database if not exists {self.dbname} vgroups {self.vgroups} replica {self.replica}')
        json_data_list = list()
        json_filename_list = list()

        json_filename_list.append(self.file_name1)
        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop="no")
        stb_into = [self.tdCom.setStbinfo(columns=column_info_list, tags=tag_info_list, childtable_count=self.childtable_count1, childtable_prefix=self.childtable_prefix1, insert_rows=self.insert_rows, interlace_rows=self.interlace_rows, batch_create_tbl_num=self.batch_create_tbl_num)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        json_info1 = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name1, json_info1)
        json_data_list.append(json_info1)

        json_filename_list.append(self.file_name2)
        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop="no")
        stb_into = [self.tdCom.setStbinfo(columns=column_info_list, tags=tag_info_list, childtable_count=self.childtable_count2, childtable_prefix=self.childtable_prefix2, insert_rows=self.insert_rows, interlace_rows=self.interlace_rows, batch_create_tbl_num=self.batch_create_tbl_num)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        json_info2 = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name2, json_info2)
        json_data_list.append(json_info2)

        json_filename_list.append(self.file_name3)
        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop="no")
        stb_into = [self.tdCom.setStbinfo(columns=column_info_list, tags=tag_info_list, childtable_count=self.childtable_count3, childtable_prefix=self.childtable_prefix3, insert_rows=self.insert_rows, interlace_rows=self.interlace_rows, batch_create_tbl_num=self.batch_create_tbl_num)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        json_info3 = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name3, json_info3)
        json_data_list.append(json_info3)
        self.tdCom.put_file(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, self.run_log_dir)
        timestamp_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        result_file_list = self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, taosBenchmark_env_setting, self.run_log_dir)
        timestamp_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        env_setting = self.get_component_by_name("prometheus")
        Insert_file = Perf_Base_func(self._remote._logger, self.run_log_dir)
        Insert_file.taosBenchmark_insert_summary_result(result_file_list, version="3.0")
        Insert_file.get_process_exporter_info(env_setting, 30, timestamp_start, timestamp_end)
        Insert_file.get_node_exporter_info(env_setting, 30, timestamp_start, timestamp_end)
        print(self.run_log_dir + '/perf_report.txt')
