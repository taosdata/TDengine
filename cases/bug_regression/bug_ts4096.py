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
import pandas as pd
import time

class VnodeRedistribute(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.reserve_dnode_list = self.taosd_setting["spec"]["reserve_dnodes"]
        self.result_file_name = ""
        self._tmp_dir: str = os.path.join(self.run_log_dir, "tmp")
        self.replica = int(os.environ["DATABASE_REPLICAS"]) if "DATABASE_REPLICAS" in os.environ else 1
        self.vgroups = 10
        self.create_table_thread_count = 40
        self.thread_count = 200
        self.num_of_records_per_req = 1
        self.childtable_count = 10000
        self.insert_rows = 1000000000
        self.start_timestamp = "2020-01-01 00:00:00"
        self.stbname = "stb"
        self.dbname = "stream_test"
        self.stream_stbname = "output_streamtb"
        self.stream_name = "test_stream"
        self.trigger_mode = "at_once"
        self.child_table_exists = "no"
        self.db_drop = "yes"
        self.wal_retention_period = 0
        self.stream_drop = "yes"
        self.keep_trying = -1
        self.trying_interval = 10
        self.interlace_rows = 0
        self.stream_sql = f"select ts,max(c1) from {self.dbname}.{self.stbname} where c1>0 partition by tbname interval(1s) sliding(1s)"
        self.fill_history_rows = 2000000
        # self.fill_history_rows = 3000
        self.pre_num_of_records_per_req = 10000
        self.json_file_name = "insert0.json"
        self.json_data_list = list()
        self.json_filename_list = list()
        self.taosBenchmark_iplist: List = self.get_fqdn("taosBenchmark")
        self.taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        self.json_filename_list.append(self.json_file_name)
        self.redistribute_status = 0
        self.dnode_id_list = list()
        self.vgid = 1
        self.vgid_dnodeid_kv_list = list()
        self.redistributed_list = list()
        self.start_redistribute_row_count = self.fill_history_rows * self.childtable_count
        self.scheduler_interval = 300
        # self.scheduler_interval = 30
        self.query_vgid_interval = 60
        self.restore_timeout = 10800
        self.loop_redistribute_times = 10
        self.tdSql.query('show dnodes')
        self.source_dnode_id_list = list(map(lambda x:x[0], self.tdSql.query_data))
        self.cluster_to_redistribute_list = list()
        self.use_stream = False
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

    def add_reserve_dnodes(self):
        self._remote._logger.info('------- add reserve dnodes -------')
        for reserve_dnodes_index in range(len(self.reserve_dnode_list)):
            self.taosd.configure_and_start_specified_dnode(self._tmp_dir, self.taosd_setting, self.taosd_setting["spec"]["reserve_dnodes"][reserve_dnodes_index])

    def get_cluster_to_redistribute_list(self, reserver_dnode_count):
        self.tdSql.query('show dnodes')
        tmp_dnode_id_list = list(map(lambda x:x[0], self.tdSql.query_data))
        reserve_dnode_id_list = [x for x in tmp_dnode_id_list if x not in self.source_dnode_id_list]
        if self.replica == 3 :
            self.cluster_to_redistribute_list = random.sample(self.source_dnode_id_list, self.replica-reserver_dnode_count) + random.sample(reserve_dnode_id_list, reserver_dnode_count)

    def check_restored_true(self):
        self.tdSql.query(f'show vnodes;')
        restored_list = list(map(lambda x:x[-1], self.tdSql.query_data))
        latency = 0
        while False in restored_list:
            self.tdSql.query(f'show vnodes;')
            restored_list = list(map(lambda x:x[-1], self.tdSql.query_data))
            if latency < self.restore_timeout:
                latency += 2
                time.sleep(2)
            else:
                return False

    def prepare_fill_history_data(self):
        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, wal_retention_period=self.wal_retention_period)
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.fill_history_rows, start_timestamp=self.start_timestamp, child_table_exists=self.child_table_exists, name=self.stbname, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.pre_num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name, json_info)
        self.json_data_list.append(json_info)
        self.tdCom.put_file(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.run_log_dir)
        self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)

    def insert_data(self):
        self.start_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        self.child_table_exists = "yes"
        self.db_drop = "no"
        if self.use_stream:
            stream_db_info = self.tdCom.setStreamDBinfo(vgroups=1)
            stream_info = self.tdCom.setStreams(stream_name=self.stream_name, stream_stb=f'{self.dbname}.{self.stream_stbname}', trigger_mode=self.trigger_mode, drop=self.stream_drop, source_sql=self.stream_sql)
        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop)
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=self.start_timestamp, child_table_exists=self.child_table_exists, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        if self.use_stream:
            json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, streams=stream_info, stream_db=stream_db_info, num_of_records_per_req=self.num_of_records_per_req)
        else:
            json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
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

    def get_vgid_info(self):
        if self.redistribute_status == 1:
            self._remote._logger.info(f'------------query vgroup_id: {self.vgid}')
            self.tdSql.query(f'select * from information_schema.ins_vnodes where vgroup_id = {self.vgid};')
            self._remote._logger.info(pd.DataFrame(self.tdSql.query_data, columns=["dnode_id", "vgroup_id", "dbname", "status", "role_time", "start_time", "restored"]))

    def redistribute_vnode(self, reserver_dnode_count=1):
        self.tdSql.query(f'select count(*) from {self.dbname}.{self.stbname}')
        if self.tdSql.query_data[0][0] >= self.start_redistribute_row_count:
            self._remote._logger.info(f"------self.tdSql.query_data[0][0]: {self.tdSql.query_data[0][0]}")
            self._remote._logger.info(f"------self.start_redistribute_row_count: {self.start_redistribute_row_count}")
            self.add_reserve_dnodes()
            if self.redistribute_status == 0:
                # start redistribute
                for i in range(self.loop_redistribute_times):
                    self._remote._logger.info(f"------range times: {i}")
                    for vgid_dnodeid_kv in self.vgid_dnodeid_kv_list:
                        for vgid, dnodeid in vgid_dnodeid_kv.items():
                            dnode_id_list = deepcopy(self.dnode_id_list)
                            if self.replica == 1:
                                dnode_id_list.remove(dnodeid)
                                redistribute_dnode_id = random.choice(dnode_id_list)
                                self.redistributed_list.append(redistribute_dnode_id)
                                self.redistribute_status = 1
                                self.vgid = vgid
                                self.tdSql.execute(f'redistribute vgroup {vgid} dnode {redistribute_dnode_id}')
                            else:
                                self.get_cluster_to_redistribute_list(reserver_dnode_count)
                                redistribute_dnode_id_str = str()
                                for redistribute_dnode_id in self.cluster_to_redistribute_list:
                                    redistribute_dnode_id_str += f"dnode {redistribute_dnode_id} "
                                self.redistribute_status = 1
                                self.vgid = vgid
                                self.tdSql.execute(f'redistribute vgroup {vgid} {redistribute_dnode_id_str}')
                                self.check_restored_true()
                    # restore redistribute
                    for vgid_dnodeid_kv in self.vgid_dnodeid_kv_list:
                        for vgid, dnodeid in vgid_dnodeid_kv.items():
                            if self.replica == 1:
                                pass
                            else:
                                dnode_id_list = deepcopy(self.source_dnode_id_list)
                                redistribute_dnode_id_str = str()
                                for redistribute_dnode_id in dnode_id_list:
                                    redistribute_dnode_id_str += f"dnode {redistribute_dnode_id} "
                                self.tdSql.execute(f'redistribute vgroup {vgid} {redistribute_dnode_id_str}')
                                self.check_restored_true()
                        # else:
                        #     print("----rep3")
                        #     redistribute_dnode_id_list = [x for x in dnode_id_list if x not in dnodeid]
                        #     redistribute_dnode_id_str = str()
                        #     for redistribute_dnode_id in redistribute_dnode_id_list:
                        #         redistribute_dnode_id_str += f"dnode {redistribute_dnode_id} "
                        #     self.redistributed_list.append(redistribute_dnode_id_list)
                        #     self.redistribute_status = 1
                        #     self.tdSql.execute(f'redistribute vgroup {vgid} {redistribute_dnode_id_str}')
                final_redistributed_list = self.get_redistributed_list()
                self.tdSql.checkEqual(sorted(self.redistributed_list), sorted(final_redistributed_list))

    def run(self):
        self.prepare_fill_history_data()
        self.get_vgid_dnodeid_kv_list()
        self.tdCom.add_back_ground_scheduler(self.redistribute_vnode, "interval", seconds=self.scheduler_interval, max_instances=1, args=[])
        self.tdCom.add_back_ground_scheduler(self.get_vgid_info, "interval", seconds=self.query_vgid_interval, max_instances=1, args=[])
        self.insert_data()
        initial_res1, initial_res2 = self.get_query_result()
        self.get_dnode_id_list()
        self.get_vgid_dnodeid_kv_list()
        self.redistribute_vnode()
        final_res1, final_res2 = self.get_query_result()
        self.tdSql.checkEqual(initial_res1, final_res1)
        self.tdSql.checkEqual(initial_res2, final_res2)
