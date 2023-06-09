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

class TestTs2910(TDCase):
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
        self.vgroups = 10
        self.create_table_thread_count = 40
        self.thread_count = 40
        self.childtable_count = 1000
        self.insert_rows = 1000
        self.num_of_records_per_req = 10000
        self.dbname = "test"
        self.stbname = "stb"
        self.loop_count = 10

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def get_vnode_disk(self):
        return self._remote.cmd(self.get_fqdn("taosd")[0], [f'du -sh {self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"]}/vnode | awk \'{{print $1}}\''])

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
        json_data_list = list()
        json_filename_list = list()
        json_filename_list.append(self.file_name1)
        dbinfo = self.tdCom.setDBinfo(replica=self.replica, vgroups=self.vgroups, drop="yes")
        start_timestamp = (datetime.now() + timedelta(days=0)).strftime("%Y-%m-%d %H:%M:%S")
        stb_into = [self.tdCom.setStbinfo(columns=column_info_list, tags=tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=start_timestamp)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        json_info1 = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name1, json_info1)
        json_data_list.append(json_info1)
        self.tdCom.put_file(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, self.run_log_dir)
        self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, taosBenchmark_env_setting, self.run_log_dir)
        vnode_disk_usage1 = self.get_vnode_disk()
        json_data_list = list()
        json_filename_list = list()
        json_filename_list.append(self.file_name2)
        dbinfo = self.tdCom.setDBinfo(replica=self.replica, vgroups=self.vgroups, drop="no")
        start_timestamp = (datetime.now() - timedelta(days=3651)).strftime("%Y-%m-%d %H:%M:%S")
        stb_into = [self.tdCom.setStbinfo(columns=column_info_list, tags=tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, child_table_exists="yes", start_timestamp=start_timestamp)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        json_info2 = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name2, json_info2)
        json_data_list.append(json_info2)
        self.tdCom.put_file(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, self.run_log_dir)

        for i in range(self.loop_count):
            self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, taosBenchmark_env_setting, self.run_log_dir)
        vnode_disk_usage2 = self.get_vnode_disk()
        self.tdSql.checkEqual(vnode_disk_usage1, vnode_disk_usage2)
