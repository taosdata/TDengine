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
import time
import threading

class QueryScale(TDCase):
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
        self._tmp_dir: str = os.path.join(self.run_log_dir, "tmp")
        self.replica = 1
        self.vgroups = 1
        self.create_table_thread_count = 40
        self.thread_count = 40
        self.interlace_rows = 1000
        self.childtable_count1 = 10000
        self.childtable_prefix1 = "ctb1_"
        self.insert_rows = 10000
        self.num_of_records_per_req = 10000
        self.batch_create_tbl_num = 10000
        self.dbname1 = "test1"
        self.stbname = "stb"
        self.buffer = 4096
        self.insert_mode = "stmt"
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
        self.query_sql_list = [f'select count(*), max(c0) from {self.dbname1}.{self.stbname} interval(1s);']
        self.query_sql = f'select count(*), max(c0) from {self.dbname1}.{self.stbname} interval(1s) limit 1;'
        self.concurrent_list = [i for i in range(1, 4)]
        # self.concurrent_list = (i for i in range(1,2))
        self.query_time_list = list()

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def gen_multi_thread_sql(self, concurrent):
        tlist = list()
        for i in range(concurrent):
            t = threading.Thread(target=self.get_query_time,args=())
            tlist.append(t)
        return tlist

    def get_query_time(self):
        res = self._remote.cmd(self.taosd_host, [f'taos -s "{self.query_sql}"'])
        res_list = res.split("\n")
        query_time = [i for i in res_list if "Query OK," in res][-1].split(" ")[-1].replace("(", "").replace("s)", "")
        self.query_time_list.append(round(float(query_time), 2))

    def multi_thread_run(self, tlist):
        for t in tlist:
            t.start()
        for t in tlist:
            t.join()


    def thread_query(self, concurrent):
        start_ts = time.time()
        self.tdCom.multi_thread_query(None, self.query_sql_list*concurrent, concurrent)
        end_ts = time.time()
        return (end_ts-start_ts)

    def taosBenchmark_insert(self):
        taosBenchmark_iplist: List = self.get_fqdn("taosBenchmark")
        taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        self.json_filename_list.append(self.file_name1)
        dbinfo = self.tdCom.setDBinfo(name=self.dbname1, replica=self.replica, vgroups=self.vgroups, buffer=self.buffer)
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
        self.tdSql.execute(f'flush database {self.dbname1}')

    def run_summary(self, f, concurrent):
        self.query_time_list = list()
        tlist = self.gen_multi_thread_sql(concurrent)
        self.multi_thread_run(tlist)
        f.write(f'\n****************************** concurrent {concurrent} use: {max(self.query_time_list)} ******************************\n')

    def query_scale(self):
        with open(self.result_file_name, 'a') as f:
            f.write('****************************** 1 dnode ******************************\n')
            self.taosBenchmark_insert()
            self.run_summary(f, 1)

            f.write('\n\n****************************** 2 dnodes ******************************')
            self.taosd.configure_and_start_specified_dnode(self._tmp_dir, self.taosd_setting, self.taosd_setting["spec"]["reserve_dnodes"][0])
            self.taosBenchmark_insert()
            self.run_summary(f, 2)

            f.write('\n\n****************************** 3 dnodes ******************************')
            self.taosd.configure_and_start_specified_dnode(self._tmp_dir, self.taosd_setting, self.taosd_setting["spec"]["reserve_dnodes"][1])
            self.taosBenchmark_insert()
            self.run_summary(f, 3)


    def run(self):
        self.query_scale()
        self._remote.cmd("127.0.0.1", [f'cat {self.result_file_name}'])