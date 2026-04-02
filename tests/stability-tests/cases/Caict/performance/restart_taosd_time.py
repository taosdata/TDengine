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
import time

class RestartTaosdTime(TDCase):
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
        self.vgroups = 32
        self.create_table_thread_count = 32
        self.thread_count = 32
        self.interlace_rows = 10000
        self.childtable_count1 = 10000
        self.childtable_prefix1 = "ctb1_"
        self.insert_rows1 = 10000
        self.insert_rows2 = 50000
        self.num_of_records_per_req = 10000
        self.batch_create_tbl_num = 10000
        self.dbname1 = "test1"
        self.stbname = "stb"
        self.insert_mode = "stmt"
        self.json_data_list = list()
        self.json_filename_list = list()
        self.taosd_host = self.get_fqdn("taosd")[0]
        self.firstEp = self.taosd_setting["spec"]["dnodes"][0]["endpoint"]
        self.restart_timeout = 10
        self.column_info_list1 = [
          {
            "type": "INT",
            "count": 1
          }
        ]
        self.tag_info_list1 = [
          {
            "type": "INT",
            "count": 1
          }
        ]
        self.column_info_list2 = [
          {
            "type": "INT",
            "count": 5
          }
        ]
        self.tag_info_list2 = [
          {
            "type": "INT",
            "count": 5
          }
        ]

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def record_restart_time(self):
        taosd_process_count = self._remote.cmd(self.taosd_host, [f"ps -ef | grep taosd | grep -v grep | grep -v sudo | grep -v defunct | wc -l"])
        if int(taosd_process_count) > 0:
            ready_count = self._remote.cmd(self.taosd_host, [f'taos -s "show dnodes" | grep {self.firstEp} | grep ready | wc -l'])
            ready_flag = 0
            while int(ready_count) != 1:
                taosd_process_count = self._remote.cmd(self.taosd_host, [f"ps -ef | grep taosd | grep -v grep | grep -v sudo | grep -v defunct | wc -l"])
                if ready_flag < self.restart_timeout and int(taosd_process_count) > 0:
                    ready_flag += 0.1
                    time.sleep(0.1)
                    ready_count = self._remote.cmd(self.taosd_host, [f'taos -s "show dnodes" | grep {self.firstEp} | grep ready | wc -l'])
                else:
                    return
            return ready_flag

    def restart_taosd_time(self):
        for column_info_list, tag_info_list, insert_rows in [(self.column_info_list1, self.tag_info_list1, self.insert_rows1), (self.column_info_list2, self.tag_info_list2, self.insert_rows2)]:
            taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
            taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")

            self.json_filename_list.append(self.file_name1)
            dbinfo = self.tdCom.setDBinfo(name=self.dbname1, replica=self.replica, vgroups=self.vgroups)
            stb_into = [self.tdCom.setStbinfo(columns=column_info_list, tags=tag_info_list, childtable_count=self.childtable_count1, childtable_prefix=self.childtable_prefix1, insert_rows=insert_rows, batch_create_tbl_num=self.batch_create_tbl_num, insert_mode=self.insert_mode, interlace_rows=self.interlace_rows)]
            database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]

            json_info1 = self.tdCom.setJsoninfo(host=self.taosd_host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
            self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name1, json_info1)
            self.json_data_list.append(json_info1)

            self.tdCom.put_file(self._remote, taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.run_log_dir)
            result_file_list = self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, self.json_data_list, self.json_filename_list, taosBenchmark_env_setting, self.run_log_dir)
            Insert_file = Perf_Base_func(self._remote._logger, self.run_log_dir)
            Insert_file.taosBenchmark_insert_summary_result(result_file_list, version="3.0")
            self._remote.cmd(self.taosd_host, [f'ps -ef|grep -wi taosd| grep -v grep | awk \'{{print $2}}\' | xargs kill -9 > /dev/null 2>&1', "screen -d -m taosd"])
            record_restart_time = self.record_restart_time()
            with open(self.result_file_name, 'a') as f:
                f.write(f'\n****************************** restart taosd time ({column_info_list}, {tag_info_list}, {insert_rows}) ---> {record_restart_time}s ******************************\n')

    def run(self):
        self.restart_taosd_time()
        self._remote.cmd("127.0.0.1", [f'cat {self.result_file_name}'])