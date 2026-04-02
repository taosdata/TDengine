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
from datetime import datetime
from copy import deepcopy
import random

class VnodeRedistribute(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.result_file_name = ""
        self._tmp_dir: str = os.path.join(self.run_log_dir, "tmp")
        self.replica = int(os.environ["DATABASE_REPLICAS"]) if "DATABASE_REPLICAS" in os.environ else 1
        self.vgroups = 5
        self.create_table_thread_count = 40
        self.thread_count = 40
        self.childtable_count = 100
        self.insert_rows = 10
        self.start_timestamp = "2020-01-01 00:00:00"
        self.stbname = "stb"
        self.dbname = "stream_test"
        self.stream_stbname = "output_streamtb"
        self.stream_name = "test_stream"
        self.trigger_mode = "at_once"
        self.child_table_exists = "no"
        self.db_drop = "yes"
        self.stream_drop = "yes"
        self.stream_sql = f"select ts,max(c1) from {self.dbname}.{self.stbname} where c1>0 partition by tbname interval(1s) sliding(1s)"
        self.fill_history_rows = 10
        self.json_file_name = "insert0.json"
        self.json_data_list = list()
        self.json_filename_list = list()
        self.taosBenchmark_iplist: List = self.get_fqdn("taosBenchmark")
        self.taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        self.json_filename_list.append(self.json_file_name)

        self.dnode_id_list = list()
        self.vgid_dnodeid_kv_list = list()
        self.redistributed_list = list()

        self.column_info_list = [
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

    def get_dnode_id_list(self):
        self.tdSql.query('show dnodes')
        self.dnode_id_list = list(map(lambda x:x[0], self.tdSql.query_data))

    def get_vgid_dnodeid_kv_list(self):
        self.tdSql.query(f'show {self.dbname}.vgroups')
        self.vgid_dnodeid_kv_list = list(map(lambda x:{x[0]:x[3]}, self.tdSql.query_data)) if self.replica == 1 else list(map(lambda x:{x[0]:[x[3], x[5], x[7]]}, self.tdSql.query_data))

    def get_redistributed_list(self):
        self.tdSql.query(f'show {self.dbname}.vgroups')
        return list(map(lambda x:x[3], self.tdSql.query_data)) if self.replica == 1 else list(map(lambda x:[x[3], x[5], x[7]], self.tdSql.query_data))

    def prepare_fill_history_data(self):
        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop)
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.fill_history_rows, start_timestamp=self.start_timestamp, child_table_exists=self.child_table_exists, name=self.stbname)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name, json_info)
        self.json_data_list.append(json_info)
        self.tdCom.put_file(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.run_log_dir)
        self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)

    def insert_data(self):
        self.start_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        self.child_table_exists = "yes"
        self.db_drop = "no"
        stream_db_info = self.tdCom.setStreamDBinfo(vgroups=1)
        stream_info = self.tdCom.setStreams(stream_name=self.stream_name, stream_stb=f'{self.dbname}.{self.stream_stbname}', trigger_mode=self.trigger_mode, drop=self.stream_drop, source_sql=self.stream_sql)
        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop)
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=self.start_timestamp, child_table_exists=self.child_table_exists)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, streams=stream_info, stream_db=stream_db_info)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name, json_info)
        self.json_data_list = [json_info]
        self.tdCom.put_file(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.run_log_dir)
        self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)

    def get_query_result(self):
        self.tdSql.query(f'select count(*) from {self.dbname}.{self.stbname}')
        res1 = self.tdSql.query_data[0][0]
        self.tdSql.query(f'select count(*) from {self.dbname}.{self.stream_stbname}')
        res2 = self.tdSql.query_data[0][0]
        return [res1, res2]

    def redistribute_vnode(self):
        for vgid_dnodeid_kv in self.vgid_dnodeid_kv_list:
            for vgid, dnodeid in vgid_dnodeid_kv.items():
                dnode_id_list = deepcopy(self.dnode_id_list)
                if self.replica == 1:
                    dnode_id_list.remove(dnodeid)
                    redistribute_dnode_id = random.choice(dnode_id_list)
                    self.redistributed_list.append(redistribute_dnode_id)
                    self.tdSql.execute(f'redistribute vgroup {vgid} dnode {redistribute_dnode_id}')
                else:
                    redistribute_dnode_id_list = [x for x in dnode_id_list if x not in dnodeid]
                    redistribute_dnode_id_str = str()
                    for redistribute_dnode_id in redistribute_dnode_id_list:
                        redistribute_dnode_id_str += f"dnode {redistribute_dnode_id} "
                    self.redistributed_list.append(redistribute_dnode_id_list)
                    self.tdSql.execute(f'redistribute vgroup {vgid} {redistribute_dnode_id_str}')

        final_redistributed_list = self.get_redistributed_list()
        self.tdSql.checkEqual(sorted(self.redistributed_list), sorted(final_redistributed_list))

    def run(self):
        self.prepare_fill_history_data()
        self.insert_data()
        initial_res1, initial_res2 = self.get_query_result()
        self.get_dnode_id_list()
        self.get_vgid_dnodeid_kv_list()
        self.redistribute_vnode()
        final_res1, final_res2 = self.get_query_result()
        self.tdSql.checkEqual(initial_res1, final_res1)
        self.tdSql.checkEqual(initial_res2, final_res2)
