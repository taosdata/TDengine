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
# -*- taostest --setup=cluster/redistribute_split_test.yaml --case=cluster/redistribute_test.py --keep -*-
# -*- taostest --setup=cluster/redistribute_split_test_rep3.yaml --case=cluster/redistribute_test.py --keep -*-

import os
from taostest.util.common import TDCom
from typing import List
from taostest import TDCase
from taostest.components.taosd import TaosD
from taostest.util.remote import Remote
from datetime import datetime
from copy import deepcopy
import pandas as pd
import copy

class BugTs5393(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.cfg = self.tdCom.Boundary.DB_PARAM_VGROUPS_CONFIG
        self.base_dnode_list = self.taosd_setting["spec"]["dnodes"]
        self.result_file_name = ""
        self._tmp_dir: str = os.path.join(self.run_log_dir, "tmp")
        self.replica = int(os.environ["DATABASE_REPLICAS"]) if "DATABASE_REPLICAS" in os.environ else 1
        self.vgroups = 3
        self.create_table_thread_count = 40
        self.thread_count = 200
        # self.thread_count = 10
        self.num_of_records_per_req = 100
        # self.num_of_records_per_req = 100
        self.childtable_count = 2000000
        self.insert_rows = 1000
        self.start_timestamp = "2020-01-01 00:00:00"
        self.stbname = "stb"
        self.dbname1 = "test1"
        self.dbname2 = "test2"
        self.dbname3 = "test3"
        self.dbname4 = "test4"
        self.dbname5 = "test5"
        self.dbname6 = "test6"
        self.dbname_list = [self.dbname1, self.dbname2, self.dbname3, self.dbname4, self.dbname5, self.dbname6]
        self.child_table_exists = "no"
        self.auto_create_table = "yes"
        self.db_drop = "yes"
        self.wal_retention_period = 1800
        self.keep_trying = -1
        self.trying_interval = 10000
        self.interlace_rows = 1

        self.primary_key = 0

        self.pre_num_of_records_per_req = 10000
        self.json_file_name1 = "insert0.json"
        self.json_file_name2 = "insert1.json"
        self.json_file_name3 = "insert2.json"
        self.json_file_name4 = "insert3.json"
        self.json_file_name5 = "insert4.json"
        self.json_file_name6 = "insert5.json"
        self.json_data_list = list()
        self.taosBenchmark_iplist: List = self.get_fqdn("taosBenchmark")
        self.taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        self.dnode_id_list = list()
        self.vgid = 1
        self.column_info_list = [
            {
              "type": "BIGINT",
              "count": 1,
              "gen": "order",
              "fillNull": "false"
            },
            {
              "type": "INT",
              "count": 2
            }
        ]
        self.tag_info_list = [
            {
              "type": "INT",
              "count": 1
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

    # def get_dnode_id_list(self):
    #     self.tdSql.query('show dnodes')
    #     self.dnode_id_list = list(map(lambda x:x[0], self.tdSql.query_data))

    def check_restored_true(self):
        self.tdCom.check_restored_true(self._remote)
        self.show_transactions()

    def insert_data(self):
        self.json_filename_list = [self.json_file_name1, self.json_file_name2, self.json_file_name3, self.json_file_name4, self.json_file_name5, self.json_file_name6]
        dbinfo1 = self.tdCom.setDBinfo(name=self.dbname1, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, wal_retention_period=self.wal_retention_period)
        dbinfo2 = self.tdCom.setDBinfo(name=self.dbname2, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, wal_retention_period=self.wal_retention_period)
        dbinfo3 = self.tdCom.setDBinfo(name=self.dbname3, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, wal_retention_period=self.wal_retention_period)
        dbinfo4 = self.tdCom.setDBinfo(name=self.dbname4, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, wal_retention_period=self.wal_retention_period)
        dbinfo5 = self.tdCom.setDBinfo(name=self.dbname5, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, wal_retention_period=self.wal_retention_period)
        dbinfo6 = self.tdCom.setDBinfo(name=self.dbname6, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, wal_retention_period=self.wal_retention_period)
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=self.start_timestamp, child_table_exists=self.child_table_exists, name=self.stbname, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows, primary_key=self.primary_key, auto_create_table=self.auto_create_table)]
        database_info1 = [self.tdCom.setDatabases(dbinfo=dbinfo1, super_tables=stb_into)]
        database_info2 = [self.tdCom.setDatabases(dbinfo=dbinfo2, super_tables=stb_into)]
        database_info3 = [self.tdCom.setDatabases(dbinfo=dbinfo3, super_tables=stb_into)]
        database_info4 = [self.tdCom.setDatabases(dbinfo=dbinfo4, super_tables=stb_into)]
        database_info5 = [self.tdCom.setDatabases(dbinfo=dbinfo5, super_tables=stb_into)]
        database_info6 = [self.tdCom.setDatabases(dbinfo=dbinfo6, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        json_info1 = self.tdCom.setJsoninfo(host=host, databases=database_info1, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.pre_num_of_records_per_req)
        json_info2 = self.tdCom.setJsoninfo(host=host, databases=database_info2, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.pre_num_of_records_per_req)
        json_info3 = self.tdCom.setJsoninfo(host=host, databases=database_info3, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.pre_num_of_records_per_req)
        json_info4 = self.tdCom.setJsoninfo(host=host, databases=database_info4, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.pre_num_of_records_per_req)
        json_info5 = self.tdCom.setJsoninfo(host=host, databases=database_info5, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.pre_num_of_records_per_req)
        json_info6 = self.tdCom.setJsoninfo(host=host, databases=database_info6, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.pre_num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name1, json_info1)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name2, json_info2)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name3, json_info3)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name4, json_info4)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name5, json_info5)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name6, json_info6)
        self.json_data_list = [json_info1, json_info2, json_info3, json_info4, json_info5, json_info6]
        self.tdCom.put_file(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.run_log_dir)
        self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)


    def reinsert(self):
        self.json_filename_list = [self.json_file_name1]
        dbinfo1 = self.tdCom.setDBinfo(name=self.dbname1, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, wal_retention_period=self.wal_retention_period)
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=self.start_timestamp, child_table_exists=self.child_table_exists, name=self.stbname, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows, primary_key=self.primary_key, auto_create_table=self.auto_create_table)]
        database_info1 = [self.tdCom.setDatabases(dbinfo=dbinfo1, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        json_info1 = self.tdCom.setJsoninfo(host=host, databases=database_info1, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.pre_num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name1, json_info1)
        self.json_data_list = [json_info1]
        self.tdCom.put_file(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.run_log_dir)
        self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)

    def show_transactions(self):
        self.tdSql.query('show transactions')
        self._remote._logger.info(pd.DataFrame(self.tdSql.query_data))

    def restart_dnodes(self):
        # dnodes_out_mnodes = self.tdSql.get_dnodes_out_mnodes()[0]
        # print("----dnodes_out_mnodes: ", dnodes_out_mnodes)
        restart_endpoint_list = self.tdCom.get_fqdn_by_dnode_id(self.dnode_id_list)
        # print("-----restart_endpoint_list: ", restart_endpoint_list)
        for endpoint in restart_endpoint_list:
            taosd_setting = copy.deepcopy(self.taosd_setting)
            self.taosd.update_cfg('/tmp',taosd_setting , {"supportVnodes": self.cfg["boundary"][-1]}, endpoint, True)
        self.check_restored_true()

    def deleteDnodeDataDir(self):
        dnode = self.base_dnode_list[1]
        self._remote.cmd(dnode["endpoint"], [f'rm -rf {dnode["config"]["dataDir"]}'])

    def run(self):
        self.insert_data()
        self.deleteDnodeDataDir()
        self.tdSql.execute(f'drop table {self.dbname1}.{self.stbname}')
        self.reinsert()
        for i in self.dbname_list:
            self.tdSql.execute(f'compact database {i}')
        self.tdSql.execute(f'restore dnode 2')

