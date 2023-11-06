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
import time
from taos.tmq import Consumer
from collections import Counter
class VnodeRedistributePerfTest(TDCase):
    def init(self):
        # TODO add stream/tmq/red-when-inserting
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
        self.fill_history_rows = 500000
        # self.fill_history_rows = 300
        self.pre_num_of_records_per_req = 10000
        self.json_file_name = "insert0.json"
        self.json_data_list = list()
        self.json_filename_list = list()
        self.taosBenchmark_iplist: List = self.get_fqdn("taosBenchmark")
        self.taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        self.json_filename_list.append(self.json_file_name)
        self.redistribute_status = 0
        self.reserve_redistribute_status = 0
        self.dnode_id_list = list()
        self.vgid = 1
        self.vgid_dnodeid_kv_list = list()
        self.redistributed_list = list()
        self.start_redistribute_row_count = self.fill_history_rows * self.childtable_count
        self.schedular_interval = 300
        self.schedular_interval = 10
        self.tmq_schedular_interval = 60
        self.tmq_schedular_interval = 5
        self.query_vgid_interval = 60
        self.show_vnodes_interval = 2
        self.restore_timeout = 10800
        self.tmq_schedular = None
        self.redistribute_schedular = None
        self.vgid_info_schedular = None
        self.loop_redistribute_times = 1
        self.tdSql.query('show dnodes')
        self.source_dnode_id_list = list(map(lambda x:x[0], self.tdSql.query_data))
        self.cluster_to_redistribute_list = list()
        self.continue_insert_perf = False
        if self.continue_insert_perf:
            self.use_stream = True
            self.use_tmq = True
        else:
            self.use_stream = False
            self.use_tmq = False
        self.disk_usage = 0
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

    def get_redistributed_list(self):
        self.tdSql.query(f'show {self.dbname}.vgroups')
        return list(map(lambda x:x[3], self.tdSql.query_data)) if self.replica == 1 else list(map(lambda x:[x[3], x[5], x[7]], self.tdSql.query_data))

    def add_reserve_dnodes(self):
        self._remote._logger.info('------------- add reserve dnodes -------------')
        for reserve_dnodes_index in range(len(self.reserve_dnode_list)):
            self.taosd.configure_and_start_specified_dnode(self._tmp_dir, self.taosd_setting, self.taosd_setting["spec"]["reserve_dnodes"][reserve_dnodes_index])

    def get_cluster_to_redistribute_list(self, reserver_dnode_count):
        self.tdSql.query('show dnodes')
        tmp_dnode_id_list = list(map(lambda x:x[0], self.tdSql.query_data))
        reserve_dnode_id_list = [x for x in tmp_dnode_id_list if x not in self.source_dnode_id_list]
        if self.replica == 1:
            self.cluster_to_redistribute_list = random.sample(reserve_dnode_id_list, self.replica)
        elif self.replica == 3:
            self.cluster_to_redistribute_list = random.sample(self.source_dnode_id_list, self.replica-reserver_dnode_count) + random.sample(reserve_dnode_id_list, reserver_dnode_count)

    def get_dnode_info_list(self, id_list):
        self.tdSql.query('show dnodes')
        dnode_info_list = list()
        for id in id_list:
            for query_data in self.tdSql.query_data:
                if query_data[0] == int(id):
                    endpoint = query_data[1]
                    for dnode_info in self.taosd_setting["spec"]["dnodes"]+self.taosd_setting["spec"]["reserve_dnodes"]:
                        if dnode_info["endpoint"] == endpoint:
                            dnode_info_list.append(dnode_info)
        return dnode_info_list

    def check_restored_true(self):
        self.tdSql.query(f'show vnodes;')
        restored_list = list(map(lambda x:x[-1], self.tdSql.query_data))
        latency = 0
        expected_false_count = 0 if self.replica == 1 else 1
        false_count = Counter(restored_list)[False]
        while false_count > expected_false_count:
            self._remote._logger.info(f'------------ waiting to check vnodes-restored False count<={expected_false_count} and now is {false_count}, (use {latency}s) ------------')
            self.tdSql.query(f'show vnodes;')
            restored_list = list(map(lambda x:x[-1], self.tdSql.query_data))
            false_count = Counter(restored_list)[False]
            if latency < self.restore_timeout:
                latency += self.show_vnodes_interval
                time.sleep(self.show_vnodes_interval)
            else:
                return False

    def prepare_base_data(self):
        self.start_timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, wal_retention_period=self.wal_retention_period)
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.fill_history_rows, start_timestamp=self.start_timestamp, child_table_exists=self.child_table_exists, name=self.stbname, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        if self.use_stream:
            stream_db_info = self.tdCom.setStreamDBinfo(name=self.dbname, vgroups=self.vgroups, drop=self.db_drop)
            stream_info = self.tdCom.setStreams(stream_name=self.stream_name, stream_stb=f'{self.dbname}.{self.stream_stbname}', trigger_mode=self.trigger_mode, drop=self.stream_drop, source_sql=self.stream_sql)
            json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, streams=stream_info, stream_db=stream_db_info, num_of_records_per_req=self.pre_num_of_records_per_req)
        else:
            json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.pre_num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name, json_info)
        self.json_data_list.append(json_info)
        self.tdCom.put_file(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.run_log_dir)
        self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)
        self.tdSql.execute(f'flush database {self.dbname}')

    def cal_perf(self):
        self.add_reserve_dnodes()
        self.get_vgid_dnodeid_kv_list()
        self.get_dnode_id_list()
        if self.continue_insert_perf:
            # TODO
            pass
        else:
            start_ts = time.time()
            vgid_dnodeid_kv = self.vgid_dnodeid_kv_list[0]
            disk_usage_list = list()
            for vgid, dnodeid in vgid_dnodeid_kv.items():
                self._remote._logger.info(f"------------ start redistribute ------------")
                self.vgid = vgid
                dnode_info_list = self.get_dnode_info_list(dnodeid)
                for dnode in dnode_info_list:
                    self.disk_usage = self._remote.cmd(dnode["endpoint"].split(":")[0], [f'du -sh -k {dnode["config"]["dataDir"]}/vnode/vnode{self.vgid}']).split('\t')[0]
                    disk_usage_list.append(int(self.disk_usage))
                if self.replica == 1:
                    redistribute_dnode_id = random.choice(self.dnode_id_list)
                    self.tdSql.execute(f'redistribute vgroup {vgid} dnode {redistribute_dnode_id}')
                elif self.replica == 3:
                    self.add_reserve_dnodes()
                    self.get_cluster_to_redistribute_list(self.replica)
                    redistribute_dnode_id_str = str()
                    for redistribute_dnode_id in self.cluster_to_redistribute_list:
                        redistribute_dnode_id_str += f"dnode {redistribute_dnode_id} "
                    self.tdSql.execute(f'redistribute vgroup {vgid} {redistribute_dnode_id_str}')
                else:
                    pass
                self.check_restored_true()
            end_ts = time.time()
            self._remote._logger.info(f'total_trans: {disk_usage_list[0]} kb, use time: {int(end_ts-start_ts)} s, speed: {disk_usage_list[0]/int(end_ts-start_ts)} kb/s')

    def insert_data(self):
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
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name, json_info)
        self.json_data_list = [json_info]
        self.tdCom.put_file(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.run_log_dir)
        self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)

    def remove_schedular(self, schedular):
        if schedular is not None:
            self.tdCom.remove_schedular_job(schedular)
            schedular = None

    def tmq_subcribe(self):
        if self.use_tmq:
            if self.tmq_status == 0:
                self.tdSql.query(f'show {self.dbname}.stables')
                if self.stbname in str(self.tdSql.query_data):
                    queryString = f"select ts, log(c0), ceil(pow(c0,3)) from {self.dbname}.{self.stbname} where c0 % 7 >= 0"
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
                        res = consumer.poll(timeout=10000)
                        if not res:
                            break
                    consumer.close()
                    self.tmq_status = 0

    def get_fqdn_by_dnode_id(self, dnode_id_list):
        self.tdSql.query('show dnodes')
        field_list = list(map(lambda x: {x[0]:x[1]}, self.tdSql.query_data))
        field_dict =  {k: v for dict in field_list for k, v in dict.items()}
        return list(map(lambda x:field_dict[x], dnode_id_list))

    def run(self):
        self.prepare_base_data()
        self.cal_perf()