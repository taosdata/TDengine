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
from typing import List
from taostest import TDCase
from taostest.components.taosd import TaosD
from taostest.util.remote import Remote
class Timeline_100B(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.taosadapter_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosadapter"
        )
        self.host = self.get_fqdn("taosd")[0]
        self.result_file_name = ""
        self.file_name1 = "insert0.json"
        self.file_name2 = "insert1.json"
        self.file_name3 = "insert3.json"
        self._tmp_dir: str = os.path.join(self.run_log_dir, "tmp")
        self.vgroups = 10
        self.create_table_thread_count = 40
        self.thread_count = 10000
        self.interlace_rows = 0
        self.dbname1 = "test1"
        self.dbname2 = "test2"
        self.dbname3 = "test3"
        self.stbname = "stb"
        self.childtable_count = 10000
        self.childtable_prefix = "ctb"
        self.insert_mode = "rest"
        self.insert_rows = 1000000000
        self.num_of_records_per_req = 1
        self.batch_create_tbl_num = 10000
        self.dropdb = "no"
        self.expected_conn_count = 30000
        self.confirm_interval = 10
        self.counter = 0
        self.timeout = 60
        self.exitcode = 0

    def comfirm(self, taosadapter_port, expected_concurrent):
        self.logger.info("in confirming scheduler")
        conn = self._remote.cmd(self.host, [f'netstat -nao | grep {taosadapter_port} | grep ESTABLISHED | wc -l'])
        if int(conn) == expected_concurrent:
            self.logger.info(f"concurrent connection count reach {self.expected_conn_count}, successful!")
            for taosBenchmark_host in self.get_fqdn("taosBenchmark"):
                self._remote.cmd(taosBenchmark_host, ['ps -ef|grep -wi taosBenchmark| grep -v grep | awk \'{print $2}\' | xargs kill -9 > /dev/null 2>&1'])

    def timeout_about(self):
        if self.counter < self.timeout:
            self.counter += 1
        else:
            self.exitcode = 1
            for taosBenchmark_host in self.get_fqdn("taosBenchmark"):
                self._remote.cmd(taosBenchmark_host, ['ps -ef|grep -wi taosBenchmark| grep -v grep | awk \'{print $2}\' | xargs kill -9 > /dev/null 2>&1'])

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def run(self):
        taosBenchmark_iplist: List = self.get_fqdn("taosBenchmark")
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
        for dbname in [self.dbname1, self.dbname2, self.dbname3]:
            self.tdSql.execute(f'drop database if exists {dbname}')
            self.tdSql.execute(f'create database if not exists {dbname} vgroups {self.vgroups}')

        json_data_list = list()
        json_filename_list = list()
        taosadapter_port = int(self.taosadapter_setting["spec"]["adapter_config"]["port"])
        double_counter = 0
        for taosBenchmark_host in self.get_fqdn("taosBenchmark"):
            if taosBenchmark_host == self.host:
                double_counter += 2
            else:
                double_counter += 1
        expected_concurrent = (self.thread_count + self.create_table_thread_count) * double_counter

        self.tdCom.add_back_ground_scheduler(self.comfirm, 'interval', seconds=self.confirm_interval, max_instances=1, args=[taosadapter_port, expected_concurrent])
        self.tdCom.add_back_ground_scheduler(self.timeout_about, 'interval', seconds=1, max_instances=1, args=[])

        json_filename_list.append(self.file_name1)
        dbinfo = self.tdCom.setDBinfo(name=self.dbname1, vgroups=self.vgroups, drop=self.dropdb)
        stb_into = [self.tdCom.setStbinfo(columns=column_info_list, tags=tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, interlace_rows=self.interlace_rows, batch_create_tbl_num=self.batch_create_tbl_num, insert_mode=self.insert_mode)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]

        json_info1 = self.tdCom.setJsoninfo(host=self.host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name1, json_info1)
        json_data_list.append(json_info1)

        json_filename_list.append(self.file_name2)
        dbinfo = self.tdCom.setDBinfo(name=self.dbname2, vgroups=self.vgroups, drop=self.dropdb)
        stb_into = [self.tdCom.setStbinfo(columns=column_info_list, tags=tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, interlace_rows=self.interlace_rows, batch_create_tbl_num=self.batch_create_tbl_num, insert_mode=self.insert_mode)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        json_info2 = self.tdCom.setJsoninfo(host=self.host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name2, json_info2)
        json_data_list.append(json_info2)

        json_filename_list.append(self.file_name3)
        dbinfo = self.tdCom.setDBinfo(name=self.dbname3, vgroups=self.vgroups, drop=self.dropdb)
        stb_into = [self.tdCom.setStbinfo(columns=column_info_list, tags=tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, interlace_rows=self.interlace_rows, batch_create_tbl_num=self.batch_create_tbl_num, insert_mode=self.insert_mode)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        json_info3 = self.tdCom.setJsoninfo(host=self.host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name3, json_info3)
        json_data_list.append(json_info3)
        self.tdCom.put_file(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, self.run_log_dir)
        self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, taosBenchmark_env_setting, self.run_log_dir)
        if self.exitcode == 1:
            self._remote._logger.error('========== Timeout ==========')
            self.tdSql.checkEqual(self.exitcode, 0)

