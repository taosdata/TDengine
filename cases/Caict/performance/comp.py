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
import time

class TestCompRatio(TDCase):
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
        self.childtable_count = 1000
        self.insert_rows = 30000
        self.num_of_records_per_req = 10000
        self.batch_create_tbl_num = 10000
        self.dbname = "test"
        self.stbname = "stb"
        self.insert_mode = "taosc"
        self.comp_value_list = [0, 2]
        self.flush_time = 5

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
            "count": 1,
            "min": 1,
            "max": 1
          }
        ]
        tag_info_list = [
          {
            "type": "INT",
            "count": 1,
            "min": 1,
            "max": 1
          }
        ]
        for comp_value in self.comp_value_list:
          json_data_list = list()
          json_filename_list = list()

          json_filename_list.append(self.file_name)
          dbinfo = self.tdCom.setDBinfo(name=self.dbname, vgroups=self.vgroups, comp=comp_value)
          stb_into = [self.tdCom.setStbinfo(columns=column_info_list, tags=tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, batch_create_tbl_num=self.batch_create_tbl_num, insert_mode=self.insert_mode)]
          database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
          host = self.get_fqdn("taosd")[0]
          data_dir = self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"]
          json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
          self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name, json_info)
          json_data_list.append(json_info)

          self.tdCom.put_file(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, self.run_log_dir)
          self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, taosBenchmark_env_setting, self.run_log_dir)
          self.tdSql.execute(f'flush database {self.dbname}')
          time.sleep(self.flush_time)
          res = self._remote.cmd(host, [f'du -sh {data_dir}'])
          if comp_value == self.comp_value_list[0]:
            uncomp = res.split("\t")[0][:-1]
          else:
            comp = res.split("\t")[0][:-1]
        comp_ratio = float(comp)/float(uncomp)
        self._remote._logger.info(f'uncomp data use {uncomp}M')
        self._remote._logger.info(f'comp data use {comp}M')
        self._remote._logger.info(f'comp ratio is {round(comp_ratio*100, 2)}%')