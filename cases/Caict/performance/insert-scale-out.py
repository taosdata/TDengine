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
from taostest.util.file import read_yaml, dict2yaml
from taostest.util.common import TDCom
from typing import List
from taostest import TDCase
from taostest.components.taosd import TaosD
from taostest.util.remote import Remote
from taostest.performance.result_reduction import Perf_Base_func
from datetime import datetime
import sys
import shutil
class InsertScale(TDCase):
    def init(self):
        self.yaml_path = os.path.join(os.environ["TEST_ROOT"], "env")
        self.yaml_file_name = sys.argv[1].split("=")[1]
        shutil.copy2(os.path.join(self.yaml_path, self.yaml_file_name), os.path.join(self.yaml_path, f'{self.yaml_file_name}_tmp'))
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.result_file_name = self.run_log_dir + '/perf_report.txt'
        self.file_name1 = "insert0.json"
        self.file_name2 = "insert1.json"
        self.file_name3 = "insert3.json"
        self._tmp_dir: str = os.path.join(self.run_log_dir, "tmp")
        self.replica = 1
        self.vgroups = 40
        self.create_table_thread_count = 40
        self.thread_count = 40
        self.interlace_rows = 1000
        self.childtable_count1 = 1000
        self.childtable_count2 = 1000
        self.childtable_count3 = 1000
        self.childtable_prefix1 = "ctb1_"
        self.childtable_prefix2 = "ctb2_"
        self.childtable_prefix3 = "ctb3_"
        self.insert_rows = 100000
        self.num_of_records_per_req = 10000
        self.batch_create_tbl_num = 10000
        self.dbname1 = "test1"
        self.dbname2 = "test2"
        self.dbname3 = "test3"
        self.stbname = "stb"
        self.insert_mode = "stmt"
        self.buffer = 4096
        self.json_data_list = list()
        self.json_filename_list = list()
        self.taosd_host = self.get_fqdn("taosd")[0]
        self.column_info_list = [
          {
            "type": "INT",
            "count": 3
          }
        ]
        self.tag_info_list = [
          {
            "type": "INT",
            "count": 1
          }
        ]
        self._tmp_dir: str = os.path.join(self.run_log_dir, "tmp")


    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def modify_taosBenchmark_fqdn(self, fqdn):
        for index, element in enumerate(self.env_setting["settings"]):
            if element["name"] == "taosBenchmark":
                self.env_setting["settings"][index]["fqdn"].append(fqdn)

    def dnode1scale(self):
        with open(self.result_file_name, 'a') as f:
            f.write('****************************** 1 dnode ******************************')
        taosBenchmark_iplist: List = self.get_fqdn("taosBenchmark")
        taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")

        self.tdCom.createDb(dbname=self.dbname1, vgroups=self.vgroups, buffer=self.buffer)

        self.json_filename_list.append(self.file_name1)
        dbinfo = self.tdCom.setDBinfo(name=self.dbname1, replica=self.replica, vgroups=self.vgroups, drop="no")
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count1, childtable_prefix=self.childtable_prefix1, insert_rows=self.insert_rows, batch_create_tbl_num=self.batch_create_tbl_num, insert_mode=self.insert_mode, interlace_rows=self.interlace_rows)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]

        json_info1 = self.tdCom.setJsoninfo(host=self.taosd_host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name1, json_info1)
        self.json_data_list.append(json_info1)

        self.tdCom.put_file(self._remote, taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.run_log_dir)
        timestamp_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        result_file_list = self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, self.json_data_list, self.json_filename_list, taosBenchmark_env_setting, self.run_log_dir)
        timestamp_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        Insert_file = Perf_Base_func(self._remote._logger, self.run_log_dir)
        Insert_file.taosBenchmark_insert_summary_result(result_file_list, version="3.0")
        # Insert_file.get_process_exporter_info(self.get_component_by_name("prometheus"), 30, timestamp_start, timestamp_end)
        # Insert_file.get_node_exporter_info(self.get_component_by_name("prometheus"), 30, timestamp_start, timestamp_end)

    def dnode2scale(self, reserve_dnodes_index):
        with open(self.result_file_name, 'a') as f:
            f.write('\n\n****************************** 2 dnodes ******************************')
        self.taosd.configure_and_start_specified_dnode(self._tmp_dir, self.taosd_setting, self.taosd_setting["spec"]["reserve_dnodes"][reserve_dnodes_index])
        self.tdCom.createDb(dbname=self.dbname2, vgroups=self.vgroups, buffer=self.buffer)
        add_taosBenchmark_fqdn = self.taosd_setting["spec"]["reserve_dnodes"][reserve_dnodes_index]["endpoint"].split(":")[0]
        self.modify_taosBenchmark_fqdn(add_taosBenchmark_fqdn)
        dict2yaml(self.env_setting, self.yaml_path, self.yaml_file_name)
        self.tdCom.createDb(dbname=self.dbname1, vgroups=self.vgroups, buffer=self.buffer)

        taosBenchmark_iplist: List = self.get_fqdn("taosBenchmark")
        taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        self.json_filename_list.append(self.file_name2)
        dbinfo = self.tdCom.setDBinfo(name=self.dbname2, replica=self.replica, vgroups=self.vgroups, drop="no")
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count2, childtable_prefix=self.childtable_prefix2, insert_rows=self.insert_rows, batch_create_tbl_num=self.batch_create_tbl_num, insert_mode=self.insert_mode, interlace_rows=self.interlace_rows)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        json_info2 = self.tdCom.setJsoninfo(host=self.taosd_host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name2, json_info2)
        self.json_data_list.append(json_info2)

        self.tdCom.put_file(self._remote, taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.run_log_dir)
        timestamp_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        result_file_list = self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, self.json_data_list, self.json_filename_list, taosBenchmark_env_setting, self.run_log_dir)
        timestamp_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        Insert_file = Perf_Base_func(self._remote._logger, self.run_log_dir)
        Insert_file.taosBenchmark_insert_summary_result(result_file_list, version="3.0")
        # Insert_file.get_process_exporter_info(self.get_component_by_name("prometheus"), 30, timestamp_start, timestamp_end)
        # Insert_file.get_node_exporter_info(self.get_component_by_name("prometheus"), 30, timestamp_start, timestamp_end)

    def dnode3scale(self, reserve_dnodes_index):
        with open(self.result_file_name, 'a') as f:
            f.write('\n\n****************************** 3 dnodes ******************************')
        self.taosd.configure_and_start_specified_dnode(self._tmp_dir, self.taosd_setting, self.taosd_setting["spec"]["reserve_dnodes"][reserve_dnodes_index])
        self.tdCom.createDb(dbname=self.dbname3, vgroups=self.vgroups, buffer=self.buffer)
        add_taosBenchmark_fqdn = self.taosd_setting["spec"]["reserve_dnodes"][reserve_dnodes_index]["endpoint"].split(":")[0]
        self.modify_taosBenchmark_fqdn(add_taosBenchmark_fqdn)
        dict2yaml(self.env_setting, self.yaml_path, self.yaml_file_name)
        self.tdCom.createDb(dbname=self.dbname1, vgroups=self.vgroups, buffer=self.buffer)
        self.tdCom.createDb(dbname=self.dbname2, vgroups=self.vgroups, buffer=self.buffer)

        taosBenchmark_iplist: List = self.get_fqdn("taosBenchmark")
        taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        self.json_filename_list.append(self.file_name3)
        dbinfo = self.tdCom.setDBinfo(name=self.dbname3, replica=self.replica, vgroups=self.vgroups, drop="no")
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count3, childtable_prefix=self.childtable_prefix3, insert_rows=self.insert_rows, batch_create_tbl_num=self.batch_create_tbl_num, insert_mode=self.insert_mode, interlace_rows=self.interlace_rows)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        json_info3 = self.tdCom.setJsoninfo(host=self.taosd_host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name3, json_info3)
        self.json_data_list.append(json_info3)
        self.tdCom.put_file(self._remote, taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.run_log_dir)
        timestamp_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        result_file_list = self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, self.json_data_list, self.json_filename_list, taosBenchmark_env_setting, self.run_log_dir)
        timestamp_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        Insert_file = Perf_Base_func(self._remote._logger, self.run_log_dir)
        Insert_file.taosBenchmark_insert_summary_result(result_file_list, version="3.0")
        # Insert_file.get_process_exporter_info(self.get_component_by_name("prometheus"), 30, timestamp_start, timestamp_end)
        # Insert_file.get_node_exporter_info(self.get_component_by_name("prometheus"), 30, timestamp_start, timestamp_end)


    def run(self):
        self.dnode1scale()
        self.dnode2scale(0)
        self.dnode3scale(1)

        shutil.move(os.path.join(self.yaml_path, f'{self.yaml_file_name}_tmp'), os.path.join(self.yaml_path, self.yaml_file_name))
        self._remote.cmd("127.0.0.1", [f'cat {self.result_file_name}'])