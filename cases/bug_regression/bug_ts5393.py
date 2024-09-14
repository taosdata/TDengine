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
import random
import threading
import concurrent.futures
import multiprocessing

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
        self.thread_count = 80
        # self.thread_count = 10
        self.num_of_records_per_req = 10000
        # self.num_of_records_per_req = 100
        self.childtable_count = 20000
        self.insert_rows = 10
        self.start_timestamp = "2020-01-01 00:00:00"
        self.dbname = "test"
        self.stbname1 = "stb1"
        self.stbname2 = "stb2"
        self.stbname3 = "stb3"
        self.stbname4 = "stb4"
        self.stbname5 = "stb5"
        self.stbname6 = "stb6"
        self.childtable_prefix1 = "ctb1_"
        self.childtable_prefix2 = "ctb2_"
        self.childtable_prefix3 = "ctb3_"
        self.childtable_prefix4 = "ctb4_"
        self.childtable_prefix5 = "ctb5_"
        self.childtable_prefix6 = "ctb6_"
        self.stbname_list = [self.stbname1, self.stbname2, self.stbname3, self.stbname4, self.stbname5, self.stbname6]
        self.child_table_exists = "no"
        self.auto_create_table = "yes"
        self.db_drop = "no"
        self.wal_retention_period = 1800
        self.keep_trying = -1
        self.trying_interval = 10000
        self.interlace_rows = 0

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
              "type": "BIGINT",
              "count": 2
            }
        ]
        self.tag_info_list = [
            {
              "type": "BIGINT",
              "count": 1
            }
        ]
        self.tag_field = "t0"
        self.col_field = "ts,c0,c1,c2"

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def get_dnode_id_list(self):
        self.tdSql.query('show dnodes')
        self.dnode_id_list = list(map(lambda x:x[0], self.tdSql.query_data))

    def check_restored_true(self):
        self.tdCom.check_restored_true(self._remote)
        self.show_transactions()

    def insert_data(self):
        self.json_filename_list = [self.json_file_name1, self.json_file_name2, self.json_file_name3, self.json_file_name4, self.json_file_name5, self.json_file_name6]
        dbinfo1 = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, wal_retention_period=self.wal_retention_period)
        stb_info1 = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=self.start_timestamp, child_table_exists=self.child_table_exists, name=self.stbname1, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows, primary_key=self.primary_key, auto_create_table=self.auto_create_table, childtable_prefix=self.childtable_prefix1)]
        stb_info2 = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=self.start_timestamp, child_table_exists=self.child_table_exists, name=self.stbname2, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows, primary_key=self.primary_key, auto_create_table=self.auto_create_table, childtable_prefix=self.childtable_prefix2)]
        stb_info3 = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=self.start_timestamp, child_table_exists=self.child_table_exists, name=self.stbname3, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows, primary_key=self.primary_key, auto_create_table=self.auto_create_table, childtable_prefix=self.childtable_prefix3)]
        stb_info4 = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=self.start_timestamp, child_table_exists=self.child_table_exists, name=self.stbname4, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows, primary_key=self.primary_key, auto_create_table=self.auto_create_table, childtable_prefix=self.childtable_prefix4)]
        stb_info5 = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=self.start_timestamp, child_table_exists=self.child_table_exists, name=self.stbname5, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows, primary_key=self.primary_key, auto_create_table=self.auto_create_table, childtable_prefix=self.childtable_prefix5)]
        stb_info6 = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=self.start_timestamp, child_table_exists=self.child_table_exists, name=self.stbname6, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows, primary_key=self.primary_key, auto_create_table=self.auto_create_table, childtable_prefix=self.childtable_prefix6)]
        database_info1 = [self.tdCom.setDatabases(dbinfo=dbinfo1, super_tables=stb_info1)]
        database_info2 = [self.tdCom.setDatabases(dbinfo=dbinfo1, super_tables=stb_info2)]
        database_info3 = [self.tdCom.setDatabases(dbinfo=dbinfo1, super_tables=stb_info3)]
        database_info4 = [self.tdCom.setDatabases(dbinfo=dbinfo1, super_tables=stb_info4)]
        database_info5 = [self.tdCom.setDatabases(dbinfo=dbinfo1, super_tables=stb_info5)]
        database_info6 = [self.tdCom.setDatabases(dbinfo=dbinfo1, super_tables=stb_info6)]
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
        self.tdSql.execute(f'flush database {self.dbname}')

    def reinsert(self):
        self.json_filename_list = [self.json_file_name1]
        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, wal_retention_period=self.wal_retention_period)
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=self.start_timestamp, child_table_exists=self.child_table_exists, name=self.stbname1, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows, primary_key=self.primary_key, auto_create_table=self.auto_create_table)]
        database_info1 = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        json_info1 = self.tdCom.setJsoninfo(host=host, databases=database_info1, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.pre_num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name1, json_info1)
        self.json_data_list = [json_info1]
        self.tdCom.put_file(self._remote, [self.taosBenchmark_iplist[0]], self.json_data_list, self.json_filename_list, self.run_log_dir)
        self.tdCom.threads_run_taosBenchmark(self._remote, [self.taosBenchmark_iplist[0]], self.json_data_list, self.json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)
        self.tdSql.execute(f'flush database {self.dbname}')

    def show_transactions(self):
        self.tdSql.query('show transactions')
        self._remote._logger.info(pd.DataFrame(self.tdSql.query_data))

    def restart_dnodes(self):
        # dnodes_out_mnodes = self.tdSql.get_dnodes_out_mnodes()[0]
        # print("----dnodes_out_mnodes: ", dnodes_out_mnodes)
        self.get_dnode_id_list()
        restart_endpoint_list = sorted(self.tdCom.get_fqdn_by_dnode_id(self.dnode_id_list))
        print(restart_endpoint_list)
        # print("-----restart_endpoint_list: ", restart_endpoint_list)
        taosd_setting = copy.deepcopy(self.taosd_setting)
        self.taosd.update_cfg('/tmp',taosd_setting , {"supportVnodes": self.cfg["boundary"][-1]}, restart_endpoint_list[1], True)
        self.check_restored_true()

    def deleteDnodeDataDir(self):
        dnode = self.base_dnode_list[1]
        dnode_fqdn = dnode["endpoint"].split(":")[0]
        self._remote.cmd(dnode_fqdn, [f'rm -rf {dnode["config"]["dataDir"]}'])

    def auto_create_table_insert(self, args):
        # ctbname, stbname, tag_field, tag_values, col_field, col_values
        ctbname, stbname, tag_value = args
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        c0, c1, c2 = random.randint(0, 100), random.randint(0, 100), random.randint(0, 100)
        self.tdSql.execute(f'insert into {self.dbname}.{ctbname} using {self.dbname}.{stbname} ({self.tag_field}) tags ({tag_value}) ({self.col_field}) values ("{ts}", {c0}, {c1}, {c2})')

    def thread_insert(self, ctb_prefix, stbname):
        self._remote._logger.info(f"thread_insert with {self.thread_count} threads")
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.thread_count) as executor:
            tasks = [(f"{ctb_prefix}{i}", stbname, i) for i in range(self.childtable_count)]
            executor.map(self.auto_create_table_insert, tasks)

    def run(self):
        
        # for stbname in self.stbname_list:
        #     self.tdCom.create_stable(dbname=self.dbname, stbname=stbname, column_elm_list=self.column_info_list, tag_elm_list=self.tag_info_list, default_column_index_start_num=0, default_tag_index_start_num=0)
        # self.thread_insert(self.childtable_prefix1, self.stbname1)
        # self.thread_insert(self.childtable_prefix2, self.stbname2)
        # self.thread_insert(self.childtable_prefix3, self.stbname3)
        # self.thread_insert(self.childtable_prefix4, self.stbname4)
        # self.thread_insert(self.childtable_prefix5, self.stbname5)
        # self.thread_insert(self.childtable_prefix6, self.stbname6)
        self.tdCom.createDb(dbname=self.dbname, replica=self.replica, vgroups=self.vgroups, wal_retention_period=self.wal_retention_period)
        self.insert_data()
        
        self.deleteDnodeDataDir()
        # self.restart_dnodes()
        self.tdSql.execute(f'use {self.dbname}')
        self.tdSql.execute(f'drop table {self.dbname}.{self.stbname1}')
        self.tdCom.create_stable(dbname=self.dbname, stbname=self.stbname1, column_elm_list=self.column_info_list, tag_elm_list=self.tag_info_list, default_column_index_start_num=0, default_tag_index_start_num=0)
        self.reinsert()
        # self.thread_insert(self.childtable_prefix1, self.stbname1)
        self.tdSql.execute(f'compact database {self.dbname}')
        self.tdSql.execute(f'restore dnode 2')




        """
        
        
        
        process_count = len(self.stbname_list)
        processes = []
        for i in range(process_count):
            process = multiprocessing.Process(target=self.thread_insert, args=(self.childtable_prefix1, self.stbname1))
            processes.append(process)
            process.start()
            process = multiprocessing.Process(target=self.thread_insert, args=(self.childtable_prefix2, self.stbname2))
            processes.append(process)
            process.start()
            process = multiprocessing.Process(target=self.thread_insert, args=(self.childtable_prefix3, self.stbname3))
            processes.append(process)
            process.start()
            process = multiprocessing.Process(target=self.thread_insert, args=(self.childtable_prefix4, self.stbname4))
            processes.append(process)
            process.start()
            process = multiprocessing.Process(target=self.thread_insert, args=(self.childtable_prefix5, self.stbname5))
            processes.append(process)
            process.start()
            process = multiprocessing.Process(target=self.thread_insert, args=(self.childtable_prefix6, self.stbname6))
            processes.append(process)
            process.start()

        for process in processes:
            process.join()
        """