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
from taos.tmq import Consumer
import copy

class VnodeSplit(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.cfg = self.tdCom.Boundary.DB_PARAM_VGROUPS_CONFIG
        # self.base_dnode_list = self.taosd_setting["spec"]["dnodes"]
        self.reserve_dnode_list = self.taosd_setting["spec"]["reserve_dnodes"]
        self.result_file_name = ""
        self._tmp_dir: str = os.path.join(self.run_log_dir, "tmp")
        self.replica = int(os.environ["DATABASE_REPLICAS"]) if "DATABASE_REPLICAS" in os.environ else 1
        self.vgroups = 5
        self.create_table_thread_count = 40
        self.thread_count = 200
        # self.thread_count = 10
        self.num_of_records_per_req = 1
        # self.num_of_records_per_req = 100
        self.childtable_count = 10000
        self.insert_rows = 1000000
        self.start_timestamp = "2020-01-01 00:00:00"
        self.stbname = "stb"
        self.dbname = "stream_test"
        self.stream_stbname = "output_streamtb"
        self.stream_name = "test_stream"
        self.trigger_mode = "at_once"
        self.child_table_exists = "no"
        self.db_drop = "yes"
        self.wal_retention_period = 86400
        self.stream_drop = "yes"
        self.keep_trying = -1
        self.trying_interval = 10
        self.interlace_rows = 0
        self.stream_sql = f"select ts,max(c1) from {self.dbname}.{self.stbname} where c1>0 partition by tbname interval(1s) sliding(1s)"
        self.fill_history_rows = 10000
        # self.fill_history_rows = 300
        self.pre_num_of_records_per_req = 10000
        self.json_file_name1 = "insert0.json"
        self.json_file_name2 = "insert1.json"
        self.json_data_list = list()
        self.taosBenchmark_iplist: List = self.get_fqdn("taosBenchmark")
        self.taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        self.split_status = 0
        self.reserve_split_status = 0
        self.dnode_id_list = list()
        self.vgid = 1
        self.vgid_dnodeid_kv_list = list()
        self.restart_dnode_id_list = list()
        self.start_split_row_count = self.fill_history_rows * self.childtable_count
        # self.scheduler_interval = 300
        self.scheduler_interval = 10
        # self.tmq_schedular_interval = 60
        self.tmq_schedular_interval = 10
        self.query_vgid_interval = 60
        self.show_vnodes_interval = 2
        self.restart_dnode_interval = 300
        self.restore_timeout = 10800
        self.check_transactions_timeout = 10800
        self.wait_false_timeout = 10
        self.tmq_schedular = None
        self.split_schedular = None
        self.vgid_info_schedular = None
        self.vgid_info_schedular_pause = False
        self.restart_dnode_schedular = None
        self.loop_split_times = 1
        # * not support
        self.use_stream = False
        self.use_tmq = True
        self.topic_name = "tp_name"
        self.tmq_status = 0
        self.offset_value = "earliest"
        self.commit_value = "true"
        self.tbname_value = "true"
        self.group_id = "tq_1"
        self.auto_commit_interval = "100"
        self.consumer_ip = self.taosd_setting["spec"]["config"]["firstEP"].split(":")[0]
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
        self.vgid_dnodeid_kv_list = list(map(lambda x:{x[0]:[x[3]]}, self.tdSql.query_data)) if self.replica == 1 else list(map(lambda x:{x[0]:[x[3], x[5], x[7]]}, self.tdSql.query_data))

    def add_reserve_dnodes(self):
        self._remote._logger.info('------------ add reserve dnodes -------------')
        for reserve_dnodes_index in range(len(self.reserve_dnode_list)):
            self.taosd.configure_and_start_specified_dnode(self._tmp_dir, self.taosd_setting, self.taosd_setting["spec"]["reserve_dnodes"][reserve_dnodes_index])

    def check_transactions(self, expected_rows=0):
        self.tdSql.query(f'show transactions;')
        query_data = self.tdSql.query_data
        self._remote._logger.info(f'------------ checking show-transactions ------------')
        latency = 0
        while len(query_data) > expected_rows:
            self._remote._logger.info(f'------------ waiting to confirm show-transactions finish (use {latency}s) ------------')
            self.tdSql.query(f'show transactions;')
            query_data = self.tdSql.query_data
            if latency < self.check_transactions_timeout:
                latency += self.show_vnodes_interval
                time.sleep(self.show_vnodes_interval)
            else:
                return False

    def check_restored_true(self):
        self.tdSql.query(f'show vnodes;')
        restored_list = list(map(lambda x:x[-1], self.tdSql.query_data))
        latency = 0
        while False in restored_list:
            self._remote._logger.info(f'------------ waiting to check vnodes-restored all True (use {latency}s) ------------')
            self.tdSql.query(f'show vnodes;')
            restored_list = list(map(lambda x:x[-1], self.tdSql.query_data))
            if latency < self.restore_timeout:
                latency += self.show_vnodes_interval
                time.sleep(self.show_vnodes_interval)
            else:
                return False
            # time.sleep(self.show_vnodes_interval)

    def prepare_fill_history_data(self):
        self.json_filename_list = [self.json_file_name1]
        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, wal_retention_period=self.wal_retention_period)
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.fill_history_rows, start_timestamp=self.start_timestamp, child_table_exists=self.child_table_exists, name=self.stbname, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.pre_num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name1, json_info)
        self.json_data_list.append(json_info)
        self.tdCom.put_file(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.run_log_dir)
        self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)

    def insert_data(self):
        self.json_filename_list = [self.json_file_name2]
        self.start_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        self.child_table_exists = "yes"
        self.db_drop = "no"
        if self.use_stream:
            stream_db_info = self.tdCom.setStreamDBinfo(name=self.dbname, vgroups=self.vgroups, drop=self.db_drop)
            stream_info = self.tdCom.setStreams(stream_name=self.stream_name, stream_stb=f'{self.dbname}.{self.stream_stbname}', trigger_mode=self.trigger_mode, drop=self.stream_drop, source_sql=self.stream_sql)
        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop)
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=self.start_timestamp, child_table_exists=self.child_table_exists, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        if self.use_stream:
            json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, streams=stream_info, stream_db=stream_db_info, num_of_records_per_req=self.num_of_records_per_req)
        else:
            json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name2, json_info)
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
        if self.split_status == 1:
            if self.vgid_info_schedular is not None:
                if self.vgid not in self.get_vgid_list():
                    self._remote._logger.info(f"------------ pause vginfo schedular job ------------: {self.vgid_info_schedular}")
                    self.tdCom.pause_schedular_job(self.vgid_info_schedular)
                    self.vgid_info_schedular_pause = True
            self._remote._logger.info(f'------------ query vgroup_id: {self.vgid} ------------')
            self.tdSql.query(f'select * from information_schema.ins_vnodes where vgroup_id = {self.vgid};')
            self._remote._logger.info(pd.DataFrame(self.tdSql.query_data))

    def get_vg_kv(self, vgid):
        self.tdSql.query(f'show {self.dbname}.vgroups')
        vg_kv = self.tdSql.get_db_field_kv(0, vgid)
        return vg_kv

    def get_vgid_list(self):
        self.tdSql.query(f'show {self.dbname}.vgroups')
        return list(map(lambda x:x[0], self.tdSql.query_data))

    def get_split_vgid_list(self, source_vgid_list, source_vgid):
        split_vgid_list = list()
        self.tdSql.query(f'show {self.dbname}.vgroups')
        vgid_list = list(map(lambda x:x[0], self.tdSql.query_data))
        split_vgid_list = [item for item in vgid_list if item not in source_vgid_list]
        return split_vgid_list

    def get_split_vg_table_total(self, split_vgid_list):
        vg_table_total = 0
        for vgid in split_vgid_list:
            vg_table_total += self.get_vg_kv(vgid)["tables"]
        return vg_table_total

    def split_vnode_in_base_dnode(self):
        if self.split_status == 0:
            split_counter = 0
            self._remote._logger.info(f"------------ split in base dnodes ------------")
            for vgid_dnodeid_kv in self.vgid_dnodeid_kv_list:
                for vgid, dnodeid in vgid_dnodeid_kv.items():
                    self._remote._logger.info(f"------------ base dnodes split vgroup plan: {split_counter+1}/{len(self.vgid_dnodeid_kv_list)} ------------")
                    self.vgid = vgid
                    self.split_status = 1
                    self.restart_dnode_id_list.append(random.choice(dnodeid))
                    source_vg_info = self.get_vg_kv(self.vgid)
                    source_vgid_list = self.get_vgid_list()
                    self.tdSql.execute(f'split vgroup {vgid}')
                    self.check_transactions()
                    self.check_restored_true()
                    split_vgid_list = self.get_split_vgid_list(source_vgid_list, source_vg_info)
                    self.tdSql.checkEqual(self.get_split_vg_table_total(split_vgid_list), source_vg_info["tables"])
                    split_counter += 1

    def split_vnode_in_reserve_dnode(self):
        if self.reserve_split_status == 0:
            split_counter = 0
            for i in range(self.loop_split_times):
                self.get_dnode_id_list()
                self.get_vgid_dnodeid_kv_list()
                self._remote._logger.info(f"------------ split in reserve dnodes range times: {i+1}/{self.loop_split_times} ------------")
                self.restart_dnode_id_list = list()
                for vgid_dnodeid_kv in self.vgid_dnodeid_kv_list:
                    for vgid, dnodeid in vgid_dnodeid_kv.items():
                        self._remote._logger.info(f"------------ reserve dnodes split vgroup plan: {split_counter+1}/{len(self.vgid_dnodeid_kv_list)*self.loop_split_times} ------------")
                        self.vgid = vgid
                        if self.vgid_info_schedular_pause:
                            if self.vgid in self.get_vgid_list():
                                self._remote._logger.info(f"------------ resume vginfo schedular job ------------: {self.vgid_info_schedular}")
                                self.tdCom.resume_schedular_job(self.vgid_info_schedular)
                                self.vgid_info_schedular_pause = False
                        self.restart_dnode_id_list.append(random.choice(dnodeid))
                        source_vg_info = self.get_vg_kv(self.vgid)
                        source_vgid_list = self.get_vgid_list()
                        self.tdSql.execute(f'split vgroup {vgid}')
                        self.check_transactions()
                        self.check_restored_true()
                        split_vgid_list = self.get_split_vgid_list(source_vgid_list, source_vg_info)
                        self.tdSql.checkEqual(self.get_split_vg_table_total(split_vgid_list), source_vg_info["tables"])
                        split_counter += 1

    def split_vnode(self):
        if self.reserve_split_status == 0 and self.split_status == 0:
            self.tdSql.query(f'select count(*) from {self.dbname}.{self.stbname}')
            if self.tdSql.query_data[0][0] >= self.start_split_row_count:
                self._remote._logger.info(f"------------ remove split schedular job ------------: {self.split_schedular}")
                self.remove_schedular(self.split_schedular)
                self._remote._logger.info(f"------------ current query_rows ------------: {self.tdSql.query_data[0][0]}")
                self._remote._logger.info(f"------------ start-split row_count ------------: {self.start_split_row_count}")
                self.split_vnode_in_base_dnode()
                self.add_reserve_dnodes()
                self.split_vnode_in_reserve_dnode()
                self.reserve_split_status = 1
        self._remote._logger.info(f"------------ remove vginfo schedular job ------------: {self.vgid_info_schedular}")
        self.remove_schedular(self.vgid_info_schedular)
        self._remote._logger.info(f"------------ remove restart dnode schedular job ------------: {self.restart_dnode_schedular}")
        self.remove_schedular(self.restart_dnode_schedular)

    def remove_schedular(self, schedular):
        if schedular is not None:
            self.tdCom.remove_schedular_job(schedular)
            schedular = None

    def tmq_subcribe(self):
        if self.use_tmq:
            if self.tmq_status == 0:
                self.tdSql.query(f'show {self.dbname}.stables')
                if self.stbname in str(self.tdSql.query_data):
                    queryString = "select ts, log(c0), ceil(pow(c0,3)) from %s.%s where c0 %% 7 >= 0" %(self.dbname, self.stbname)
                    sqlString = "create topic if not exists %s as %s" %(self.topic_name, queryString)
                    self.tdSql.execute(sqlString)
                    consumer_dict = {
                                "group.id": self.group_id,
                                "td.connect.user": "root",
                                "td.connect.pass": "taosdata",
                                "td.connect.ip": self.consumer_ip,
                                "auto.commit.interval.ms": self.auto_commit_interval,
                                "enable.auto.commit": self.commit_value,
                                "auto.offset.reset": self.offset_value,
                                "msg.with.table.name": self.tbname_value
                            }
                    consumer = Consumer(consumer_dict)
                    consumer.subscribe([self.topic_name])
                    while True:
                        self.tmq_status = 1
                        if self.tmq_schedular is not None:
                            self._remote._logger.info(f"------------ remove tmq schedular job ------------: {self.tmq_schedular}")
                            self.tdCom.remove_schedular_job(self.tmq_schedular)
                            self.tmq_schedular = None
                        # self.remove_schedular(self.tmq_schedular)
                        res = consumer.poll(timeout=10000)
                        if not res:
                            break
                        # print(res)
                        # val = res.value()
                        # for block in val:
                        #     print(block.fetchall())
                    consumer.close()
                    self.tmq_status = 0

    def get_fqdn_by_dnode_id(self, dnode_id_list):
        self.tdSql.query('show dnodes')
        field_list = list(map(lambda x: {x[0]:x[1]}, self.tdSql.query_data))
        field_dict =  {k: v for dict in field_list for k, v in dict.items()}
        return list(map(lambda x:field_dict[x], dnode_id_list))

    def restart_dnodes(self):
        dnodes_out_mnodes = self.tdSql.get_dnodes_out_mnodes()[0]
        self.restart_dnode_id_list = list(set(self.restart_dnode_id_list).intersection(dnodes_out_mnodes))
        if len(self.restart_dnode_id_list) > 0:
            self.restart_dnode_id_list = list(set(self.restart_dnode_id_list))
            restart_endpoint_list = self.get_fqdn_by_dnode_id(self.restart_dnode_id_list)
            for endpoint in restart_endpoint_list:
                taosd_setting = copy.deepcopy(self.taosd_setting)
                self.taosd.update_cfg('/tmp',taosd_setting , {"supportVnodes": self.cfg["boundary"][-1]}, endpoint, True)


    def run(self):
        self.prepare_fill_history_data()
        self.get_dnode_id_list()
        self.get_vgid_dnodeid_kv_list()
        self.tmq_schedular = self.tdCom.add_back_ground_scheduler(self.tmq_subcribe, "interval", seconds=self.tmq_schedular_interval, max_instances=1, args=[])
        self.split_schedular = self.tdCom.add_back_ground_scheduler(self.split_vnode, "interval", seconds=self.scheduler_interval, max_instances=1, args=[])
        self.vgid_info_schedular = self.tdCom.add_back_ground_scheduler(self.get_vgid_info, "interval", seconds=self.query_vgid_interval, max_instances=1, args=[])
        self.restart_dnode_schedular = self.tdCom.add_back_ground_scheduler(self.restart_dnodes, "interval", seconds=self.restart_dnode_interval, max_instances=1, args=[])
        self.insert_data()
        self.tdSql.query(f'select count(*) from {self.dbname}.{self.stbname}')
        self.tdSql.checkEqual(self.tdSql.query_data[0][0], self.childtable_count*(self.insert_rows+self.fill_history_rows))
