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
from datetime import datetime,timedelta


class DnodeAddInsert(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.result_file_name = ""
        self._tmp_dir: str = os.path.join(self.run_log_dir, "tmp")
        self.replica = 3
        self.vgroups = 40
        self.create_table_thread_count=40
        self.thread_count=40
        self.childtable_count = 10000
        self.insert_rows = 10000

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def run(self):
        json_data_list = list()
        json_filename_list = list()
        taosBenchmark_iplist: List = self.get_fqdn("taosBenchmark")
        taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        file_name1 = "insert0.json"
        json_filename_list.append(file_name1)
        childtable_count = self.childtable_count
        insert_rows = self.insert_rows
        
        column_info_list = [
          {
            "type": "INT",
            "count": 1
          }
        ]
        tag_info_list = [
          {
            "type": "INT",
            "count": 1
          }
        ]
        
        for start_index in range(len(self.taosd_setting["spec"]["reserve_dnodes"])):
            start_timestamp = (datetime.now() + timedelta(days=start_index)).strftime("%Y-%m-%d %H:%M:%S")
            child_table_exists = "yes" if start_index != 0 else "no"
            db_drop = "no" if start_index != 0 else "yes"
            dbinfo = self.tdCom.setDBinfo(replica=self.replica, vgroups=self.vgroups, drop=db_drop)
            stb_into = [self.tdCom.setStbinfo(columns=column_info_list, tags=tag_info_list, childtable_count=childtable_count, insert_rows=insert_rows, start_timestamp=start_timestamp, child_table_exists=child_table_exists)]
            database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
            host = self.get_fqdn("taosd")[0]
            json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count)

            self.tdCom.genBenchmarkJson(self.run_log_dir, file_name1, json_info)
            json_data_list.append(json_info)
            self.tdCom.put_file(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, self.run_log_dir)
            self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, taosBenchmark_env_setting, self.run_log_dir)

            self.taosd.configure_and_start_specified_dnode(self._tmp_dir, self.taosd_setting, self.taosd_setting["spec"]["reserve_dnodes"][start_index])
            self.tdSql.query('show dnodes')
            db_kv_dict = self.tdSql.get_db_field_kv(1, self.taosd_setting["spec"]["dnodes"][start_index+1]["endpoint"])
            self.tdSql.execute(f'drop dnode {db_kv_dict["id"]}')

            # stb_into = [self.tdCom.setStbinfo(columns=column_info_list, tags=tag_info_list, childtable_count=childtable_count, insert_rows=insert_rows, start_timestamp=start_timestamp, child_table_exists=child_table_exists)]
            # self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, taosBenchmark_env_setting, self.run_log_dir)

        # taosBenchmark_iplist: List = self.get_fqdn("taosBenchmark")
        # json_data: List = []
        # file_name = []
        # test_root = os.environ['TEST_ROOT']
        # cfg = read_yaml(test_root + "/cases/stability/insert/long_insert/insert.yaml")

        # jfile = InsertFile()
        # Insert_file = Perf_Base_func(self.logger, self.run_log_dir)
        # self.tdSql.execute(f'drop database if exists perf_test')
        # timestamp_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        # # # run taosBenchmark
        # taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        # result_filename = Insert_file.threads_run_taosBenchmark(
        #     taosBenchmark_iplist, json_data, file_name, taosBenchmark_env_setting
        # )

        # timestamp_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        # # get insert result
        # # Insert_file.full_create_tb_result(result_filename)
        # Insert_file.taosBenchmark_insert_summary_result(
        #     result_filename, version="3.0"
        # )
        # Insert_file.taosBenchmark_id_insert_result(result_filename)

        # # get node_info and process_info
        # env_setting = self.get_component_by_name("prometheus")
        # Insert_file.get_process_exporter_info(env_setting, 1, timestamp_start, timestamp_end)
        # Insert_file.get_node_exporter_info(env_setting, 1, timestamp_start, timestamp_end)
        # print(self.result_file_name)
