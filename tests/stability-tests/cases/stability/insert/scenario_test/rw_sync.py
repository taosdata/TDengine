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
from apscheduler.schedulers.background import BackgroundScheduler



class RwSyncTest(TDCase):
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
        self._tmp_dir: str = os.path.join(self.run_log_dir, "tmp")
        self.replica = 1
        self.vgroups = 40
        self.create_table_thread_count = 40
        self.thread_count = 40
        self.childtable_count = 100000
        self.insert_rows = 100000
        self.num_of_records_per_req1 = 1000
        self.num_of_records_per_req2 = 10000
        self.childtable_prefix1 = "ctb1_"
        self.childtable_prefix2 = "ctb2_"
        self.dbname = "db_test"
        self.stbname = "stb"
        self.query_interval = 120

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
        
        json_filename_list.append(self.file_name1)
        json_filename_list.append(self.file_name2)
        
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

        self.tdSql.execute(f'drop database if exists {self.dbname}')
        self.tdSql.execute(f'create database if not exists {self.dbname} vgroups {self.vgroups}')

        dbinfo = self.tdCom.setDBinfo(replica=self.replica, vgroups=self.vgroups, drop="no")
        stb_into = [self.tdCom.setStbinfo(columns=column_info_list, tags=tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, childtable_prefix=self.childtable_prefix1)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        json_info1 = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req1)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name1, json_info1)
        json_data_list.append(json_info1)

        dbinfo = self.tdCom.setDBinfo(replica=self.replica, vgroups=self.vgroups, drop="no")
        stb_into = [self.tdCom.setStbinfo(columns=column_info_list, tags=tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, childtable_prefix=self.childtable_prefix2)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        json_info2 = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req1)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name2, json_info2)
        json_data_list.append(json_info2)
        self.tdCom.put_file(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, self.run_log_dir)

        self.tdCom.add_back_ground_scheduler(self.tdCom.multi_thread_query, 'interval', seconds=self.query_interval, max_instances=10, args=[f'{self.dbname}.{self.stbname}', None, 10])

        self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, taosBenchmark_env_setting, self.run_log_dir)
        self.tdSql.query(f'select count(*) from {self.dbname}.{self.stbname}')
        self.tdSql.checkEqual(self.tdSql.query_data[0][0], self.childtable_count*self.insert_rows*2)

        # jfile = InsertFile()
        # Insert_file = Perf_Base_func(self.logger, self.run_log_dir)
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
