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
from datetime import datetime,timedelta

class TestPI37(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.result_file_name = ""
        self.file_name1 = "insert0.json"
        self._tmp_dir: str = os.path.join(self.run_log_dir, "tmp")
        self.replica = 1
        self.vgroups = 10
        self.create_table_thread_count = 40
        self.thread_count = 40
        self.childtable_count = 100
        self.insert_rows = 100000000
        self.num_of_records_per_req = 10000
        self.dbname = "test"
        self.stbname = "stb"
        self.loop_count = 10
        self.long_query_interval = 2
        self.interp_interval = 5
        self.query_concurrent = 100

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def long_query(self):
        self.logger.info("long querying")
        self.tdSql.query(f'select * from {self.dbname}.{self.stbname} partition by tbname,c0;')

    def interp_query(self):
        self.logger.info("interp querying")
        query_list = [f'select interp(c0) from {self.dbname}.ctb0 range("2024-01-10 17:00:00", "2024-01-12 00:00:00") every(10s) fill(prev);'] * self.query_concurrent
        self.tdCom.multi_thread_query(self.stbname, query_list, self.query_concurrent)

    def run(self):
        taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
        taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")

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
        self.long_query_schedular = self.tdCom.add_back_ground_scheduler(self.long_query, "interval", seconds=self.long_query_interval, max_instances=5, args=[])
        self.long_query_schedular = self.tdCom.add_back_ground_scheduler(self.interp_query, "interval", seconds=self.interp_interval, max_instances=100, args=[])
        json_data_list = list()
        json_filename_list = list()
        json_filename_list.append(self.file_name1)
        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop="yes")
        start_timestamp = (datetime.now() + timedelta(days=0)).strftime("%Y-%m-%d %H:%M:%S")
        stb_into = [self.tdCom.setStbinfo(columns=column_info_list, tags=tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=start_timestamp)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        json_info1 = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name1, json_info1)
        json_data_list.append(json_info1)
        self.tdCom.put_file(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, self.run_log_dir)
        self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, taosBenchmark_env_setting, self.run_log_dir)
        self.tdSql.query(f'select count(*) from {self.dbname}.stb')
        self.tdSql.checkEqual(self.tdSql.query_row, self.childtable_count*self.insert_rows)

