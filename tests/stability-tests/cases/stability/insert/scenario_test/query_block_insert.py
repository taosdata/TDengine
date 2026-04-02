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
import threading
from datetime import datetime,timedelta
import traceback
import random
class QueryBlockInsertTest(TDCase):
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
        self.replica = 3
        self.vgroups = 40
        self.create_table_thread_count = 40
        self.thread_count = 40
        self.childtable_count = 10000
        self.insert_rows = 100000
        self.num_of_records_per_req = 2500
        self.dbname = "db_test"
        self.stbname = "stb"
        self.query_interval = 120
        self.exitcode = 0

    def double_query(self, expected_res, expected_rows):
        try:
          self.tdSql.no_fetch_query(f'select * from {self.dbname}.{self.stbname}', 10, expected_rows)
          self.tdSql.query(f'select count(*) from {self.dbname}.{self.stbname}')
          self.tdSql.checkEqual(self.tdSql.query_data[0][0], expected_res)
        except Exception as e:
          self.logger.error(f'ERROR: {e}')
          self.exitcode = 1
          # raise Exception('An error occured.')
        

    def kill_a_dnode(self):
        dnodes_out_mnodes = self.tdSql.get_dnodes_out_mnodes()
        random_endpoint = random.choice(dnodes_out_mnodes[1])  
        self.taosd.kill_by_port(random_endpoint)

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def run(self):
        expected_res = self.childtable_count*self.insert_rows*2
        taosBenchmark_iplist: List = self.get_fqdn("taosBenchmark")
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
        
        self.tdSql.execute(f'drop database if exists {self.dbname}')
        self.tdSql.execute(f'create database if not exists {self.dbname} vgroups {self.vgroups} replica {self.replica}')
        json_data_list = list()
        json_filename_list = list()
        json_filename_list.append(self.file_name1)
        dbinfo = self.tdCom.setDBinfo(replica=self.replica, vgroups=self.vgroups, drop="no")
        start_timestamp = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")
        stb_into = [self.tdCom.setStbinfo(columns=column_info_list, tags=tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=start_timestamp)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        json_info1 = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name1, json_info1)
        json_data_list.append(json_info1)
        self.tdCom.put_file(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, self.run_log_dir)
        self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, taosBenchmark_env_setting, self.run_log_dir)

        thread_list = list()
        thread_list.append(threading.Thread(target=self.double_query, args=(expected_res, self.childtable_count*self.insert_rows/5)))

        json_data_list = list()
        json_filename_list = list()
        json_filename_list.append(self.file_name2)
        dbinfo = self.tdCom.setDBinfo(replica=self.replica, vgroups=self.vgroups, drop="no")
        start_timestamp = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S")
        stb_into = [self.tdCom.setStbinfo(columns=column_info_list, tags=tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, child_table_exists="yes", start_timestamp=start_timestamp)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        json_info2 = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name2, json_info2)
        json_data_list.append(json_info2)
        self.tdCom.put_file(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, self.run_log_dir)

        # self.tdCom.add_back_ground_scheduler(self.tdCom.multi_thread_query, 'interval', seconds=self.query_interval, max_instances=10, args=[f'{self.dbname}.{self.stbname}', None, 10])
        thread_list.append(threading.Thread(target=self.tdCom.threads_run_taosBenchmark, args=(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, taosBenchmark_env_setting, self.run_log_dir)))

        if self.replica == 3:
          thread_list.append(threading.Thread(target=self.kill_a_dnode, args=()))

        for t in thread_list:
          t.start()
        for t in thread_list:
          t.join()
        if self.exitcode == 1:
          self.tdSql.checkEqual(self.exitcode, 0)
              

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
