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
from taostest.util.common import TDCom
from taostest import TDCase
from taostest.components.taosd import TaosD
from taostest.util.remote import Remote
from taostest.performance.result_reduction import Perf_Base_func
import sys
class Timeline_100B(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.result_file_name = self.run_log_dir + '/perf_report.txt'
        self.file_name1 = "insert0.json"
        self._tmp_dir: str = os.path.join(self.run_log_dir, "tmp")
        self.replica = 1
        self.vgroups = 4
        self.create_table_thread_count = 40
        self.thread_count = 40
        self.interlace_rows = 10000
        self.childtable_count1 = 1000
        self.childtable_prefix1 = "ctb1_"
        self.insert_rows = 100000
        self.num_of_records_per_req = 10000
        self.batch_create_tbl_num = 10000
        self.dbname1 = "test1"
        self.stbname = "stb"
        self.insert_mode = "stmt"
        self.json_data_list = list()
        self.json_filename_list = list()
        self.taosd_host = self.get_fqdn("taosd")[0]
        self.column_info_list = [
          {
            "type": "INT",
            "count": 1
          }
        ]
        self.tag_info_list = [
          {
            "type": "INT",
            "count": 1
          }
        ]
        self.interval = "2h"
        self.insert = False

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def query_interval(self, insert):
        if insert:
            taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
            taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")

            self.json_filename_list.append(self.file_name1)
            dbinfo = self.tdCom.setDBinfo(name=self.dbname1, replica=self.replica, vgroups=self.vgroups)
            stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count1, childtable_prefix=self.childtable_prefix1, insert_rows=self.insert_rows, batch_create_tbl_num=self.batch_create_tbl_num, insert_mode=self.insert_mode, interlace_rows=self.interlace_rows)]
            database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]

            json_info1 = self.tdCom.setJsoninfo(host=self.taosd_host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
            self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name1, json_info1)
            self.json_data_list.append(json_info1)

            self.tdCom.put_file(self._remote, taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.run_log_dir)
            result_file_list = self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, self.json_data_list, self.json_filename_list, taosBenchmark_env_setting, self.run_log_dir)
            Insert_file = Perf_Base_func(self._remote._logger, self.run_log_dir)
            Insert_file.taosBenchmark_insert_summary_result(result_file_list, version="3.0")
        with open(self.result_file_name, 'a') as f:
            f.write('****************************** Memory ******************************\n')
            f.write(self._remote.cmd(self.taosd_host, ['free -h']))
            f.write('\n\n****************************** Disk ******************************\n')
            f.write(self._remote.cmd(self.taosd_host, [f'du -sh {self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"]}']))
            f.write('\n\n****************************** Query ******************************\n')
            f.write(self._remote.cmd(self.taosd_host, [f'taos -s "select count(*) from {self.dbname1}.{self.stbname};"']))
            f.write('\n\n****************************** Export ******************************\n')
            f.write(self._remote.cmd(self.taosd_host, [f'taos -s "select * from {self.dbname1}.{self.stbname} limit 100 >> test.sql;"']))
        print(self.result_file_name)
    def run(self):
        if "--setup" in sys.argv[1]:
            self.insert = True
        self.query_interval(self.insert)