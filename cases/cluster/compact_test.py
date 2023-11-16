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
import re
from taostest.performance.result_reduction import Perf_Base_func
from taostest.components import PrometheusServer
import copy
from datetime import datetime,timedelta
from taos.tmq import Consumer
import os
import psutil

class CompactTest(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.prometheus_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "prometheus")
        self.Prometheus = PrometheusServer(self._remote)
        self.json_filename = "insert0.json"
        self.replica = int(os.environ["DATABASE_REPLICAS"]) if "DATABASE_REPLICAS" in os.environ else 1
        self.create_table_thread_count = 40
        self.childtable_count = 10000
        self.keep = "11d"
        # self.keep = "7d"
        self.duration = "1d"
        self.stt_trigger = 8
        self.today_zero_ts = self.tdCom.genTodayZeroTs()
        print(self.today_zero_ts)
        self.today_zero_dt = self.tdCom.genTs(ts=self.today_zero_ts/1000)[1]
        self.stage_1_timestamp = self.today_zero_ts - self.tdCom.trans_time_to_s(self.keep) * 1000 + 86400 * 1000
        self.stage_1_dt = self.tdCom.genTs(ts=self.stage_1_timestamp/1000)[1]
        self.disorder_start_ts = self.today_zero_ts
        self.disorder_day = 2
        self.stage_2_timestamp = self.disorder_start_ts + 86400 * 1000 * self.disorder_day
        self.stage_2_dt = self.tdCom.genTs(ts=self.stage_2_timestamp/1000)[1]
        self.stage_rows = 20000
        self.insert_rows = 1000000
        self.compact_interval = 180
        # self.compact_interval = 60
        self.compact_wait = 180
        # self.compact_wait = 60

        self.wal_retention_period = 1
        self.stbname = "stb"
        self.dbname1 = "stream_test"
        self.dbname2 = "compact_disk_usage_test"
        self.stream_stbname = "output_streamtb"
        self.stream_name = "test_stream"
        self.trigger_mode = "max_delay 1s"
        self.child_table_exists = "no"
        self.db_drop = "yes"
        self.stream_drop = "yes"
        self.keep_trying = -1
        self.trying_interval = 10
        self.vgroups = 10
        self.host = self.get_fqdn("taosd")[0]
        self.thread_count = 40
        self.num_of_records_per_req = 1000
        self.interlace_rows = 0
        self.disorder_ratio = 20
        self.update_ratio = 20
        self.delete_ratio = 20
        self.disorder_fill_interval = 300
        self.update_fill_interval = 25
        self.generate_row_rule = 2
        self.stream_sql1 = f"select ts,max(c1) from {self.dbname1}.{self.stbname} where c1>0 partition by tbname interval(1s) sliding(1s)"
        self.stream_sql2 = f"select ts,max(c1) from {self.dbname2}.{self.stbname} where c1>0 partition by tbname interval(1s) sliding(1s)"

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
        self.json_file_name1 = "insert0.json"
        self.json_file_name2 = "insert1.json"
        self.json_file_name3 = "insert2.json"
        self.json_data_list = list()
        self.taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
        self.taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")

        self.tsdbCommitCompact_count = 0
        self.expected_tsdbCommitCompact_count = self.vgroups * self.replica
        self.log_files = self.taosd_setting["spec"]["dnodes"][0]["config"]["logDir"] + "/" + "taosdlog*"
        self.tmq_schedular = None
        self.range_compact_schedular = None
        self.compact_schedular = None
        self.disorder_schedular = None
        self.restart_dnode_schedular = None

        self.tmq_schedular_interval = 60
        self.compact_schedular_interval = 300
        # self.compact_schedular_interval = 60
        self.disorder_schedular_interval = 300
        self.restart_dnode_interval = 300
        self.standard_record_time = 180

        self.compacting = False
        self.compact_end = False
        self.compact_counter = 0
        self.compact_pat_list = ["start to compact", "compact.*rows"]
        self.compact_confirm_interval = 30
        self.compact_times = 3
        self.pat_log_info = str()
        self.compact_confirm_timeout = 10800
        self.standard_taosd_avg_cpu = 0
        self.standard_taosBenchmark_avg_cpu = 0
        self.final_taosd_avg_cpu_list = list()
        self.final_taosBenchmark_avg_cpu_list = list()
        self.compact_end_timestamp = None
        self.query_time_before_compact = 0
        self.query_time_after_compact = 0
        self.query_rows_before_compact = 0
        self.query_rows_after_compact = 0
        self.disk_usage_init = 0
        self.disk_usage_before_compact = 0
        self.disk_usage_after_compact = 0
        self.mem_usage_before_compact = 0
        self.mem_usage_after_compact = 0

    def insert_fh_data(self, dbname):
        json_filename_list = [self.json_file_name1]
        dbinfo = self.tdCom.setDBinfo(name=dbname, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, wal_retention_period=self.wal_retention_period, stt_trigger=self.stt_trigger, keep=self.keep, duration=self.duration)
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.stage_rows, start_timestamp=self.stage_1_timestamp, child_table_exists=self.child_table_exists, name=self.stbname, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows, disorder_ratio=self.disorder_ratio, update_ratio=self.update_ratio, delete_ratio=self.delete_ratio, disorder_fill_interval=self.disorder_fill_interval, update_fill_interval=self.update_fill_interval, generate_row_rule=self.generate_row_rule)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name1, json_info)
        self.json_data_list.append(json_info)
        self.tdCom.put_file(self._remote, self.taosBenchmark_iplist, self.json_data_list, json_filename_list, self.run_log_dir)
        self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, self.json_data_list, json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)
        self.tdSql.execute(f'flush database {dbname}')

    def insert_base_data(self, dbname):
        json_filename_list = [self.json_file_name1]
        advance_timestamp = copy.deepcopy(self.stage_1_timestamp)
        self.child_table_exists = "yes"
        self.db_drop = "no"
        for i in range(2, int(''.join(filter(str.isdigit, self.keep)))):
            advance_timestamp += 86400*1000
            dbinfo = self.tdCom.setDBinfo(name=dbname, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, wal_retention_period=self.wal_retention_period, stt_trigger=self.stt_trigger, keep=self.keep, duration=self.duration)
            stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.stage_rows, start_timestamp=advance_timestamp, child_table_exists=self.child_table_exists, name=self.stbname, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows, disorder_ratio=self.disorder_ratio, update_ratio=self.update_ratio, delete_ratio=self.delete_ratio, disorder_fill_interval=self.disorder_fill_interval, update_fill_interval=self.update_fill_interval, generate_row_rule=self.generate_row_rule)]
            # if i % 2 == 0:
            #     self.interlace_rows = 0
            #     stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.stage_rows, start_timestamp=advance_timestamp, child_table_exists=self.child_table_exists, name=self.stbname, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows, disorder_ratio=self.disorder_ratio, update_ratio=self.update_ratio, delete_ratio=self.delete_ratio, disorder_fill_interval=self.disorder_fill_interval, update_fill_interval=self.update_fill_interval, generate_row_rule=self.generate_row_rule)]
            # else:
            #     self.interlace_rows = 1000
            #     stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.stage_rows, start_timestamp=advance_timestamp, child_table_exists=self.child_table_exists, name=self.stbname, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows)]
            database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
            host = self.get_fqdn("taosd")[0]
            if self.use_stream:
                self.stream_drop = "no" if i > 2 else "yes"
                stream_db_info = self.tdCom.setStreamDBinfo(name=dbname, vgroups=self.vgroups, drop=self.db_drop)
                stream_info = self.tdCom.setStreams(stream_name=self.stream_name, stream_stb=f'{dbname}.{self.stream_stbname}', trigger_mode=self.trigger_mode, drop=self.stream_drop, source_sql=self.stream_sql1)
                json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, streams=stream_info, stream_db=stream_db_info, num_of_records_per_req=self.num_of_records_per_req)
            else:
                json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
            self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name1, json_info)
            self.json_data_list.append(json_info)
            self.tdCom.put_file(self._remote, self.taosBenchmark_iplist, self.json_data_list, json_filename_list, self.run_log_dir)
            self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, self.json_data_list, json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)
        self.tdSql.execute(f'flush database {dbname}')
        self._remote.cmd(self.taosBenchmark_iplist[0], ['cp -rf /tmp/0.log /tmp/0_0.log'])

    def alter_db_keep_param(self):
        keep_i = int(''.join(filter(str.isdigit, self.keep)))
        keep_s = str(''.join(filter(str.isalpha, self.keep)))
        self.tdSql.execute(f'flush database {self.dbname1}')
        self.tdSql.execute(f'alter database {self.dbname1} keep {round(keep_i/2)}{keep_s}')
        self.tdSql.execute(f'flush database {self.dbname1}')

    def continue_insert(self):
        self._remote._logger.info(f'------------ schedular will compact database {self.dbname1} start with "{self.stage_1_dt}" end with "{self.today_zero_dt}" ------------')
        self._remote._logger.info(f'------------ new insert start with "{self.today_zero_dt}" ------------')
        json_filename_list = [self.json_file_name2]
        dbinfo = self.tdCom.setDBinfo(name=self.dbname1, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, wal_retention_period=self.wal_retention_period, stt_trigger=self.stt_trigger, keep=self.keep, duration=self.duration)
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=self.today_zero_ts, child_table_exists=self.child_table_exists, name=self.stbname, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows)]
        # stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=self.today_zero_ts, child_table_exists=self.child_table_exists, name=self.stbname, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows, disorder_ratio=self.disorder_ratio, update_ratio=self.update_ratio, delete_ratio=self.delete_ratio, disorder_fill_interval=self.disorder_fill_interval, update_fill_interval=self.update_fill_interval, generate_row_rule=self.generate_row_rule)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name2, json_info)
        self.json_data_list.append(json_info)
        self.tdCom.put_file(self._remote, self.taosBenchmark_iplist, self.json_data_list, json_filename_list, self.run_log_dir)
        self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, self.json_data_list, json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)
        self._remote.cmd(self.taosBenchmark_iplist[0], ['cp -rf /tmp/0.log /tmp/0_1.log'])

    def disorder_update_delete_data(self):
        self._remote._logger.info(f"------------ in disorder-update-delete schedular ------------")
        json_filename_list = [self.json_file_name3]
        advance_timestamp = copy.deepcopy(self.today_zero_ts)
        for i in range(self.disorder_day):
            dbinfo = self.tdCom.setDBinfo(name=self.dbname1, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, wal_retention_period=self.wal_retention_period, stt_trigger=self.stt_trigger, keep=self.keep, duration=self.duration)
            stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.stage_rows, start_timestamp=advance_timestamp, child_table_exists=self.child_table_exists, name=self.stbname, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows, disorder_ratio=self.disorder_ratio, update_ratio=self.update_ratio, delete_ratio=self.delete_ratio, disorder_fill_interval=self.disorder_fill_interval, update_fill_interval=self.update_fill_interval, generate_row_rule=self.generate_row_rule)]
            database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
            host = self.get_fqdn("taosd")[0]
            json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
            self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name3, json_info)
            self.json_data_list = [json_info]
            self.tdCom.put_file(self._remote, self.taosBenchmark_iplist, self.json_data_list, json_filename_list, self.run_log_dir)
            self._remote.cmd(self.taosBenchmark_iplist[0], [f'taosBenchmark -c {self.taosBenchmark_env_setting[0]["spec"]["config_dir"]} -f {self.json_data_list[0]["test_log"]}{json_filename_list[0]} 2>&1'])
            advance_timestamp += 86400*1000
        # self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, self.json_data_list, json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)

    def tmq_subcribe(self):
        if self.use_tmq:
            if self.tmq_status == 0:
                self.tdSql.query(f'show {self.dbname1}.stables')
                if self.stbname in str(self.tdSql.query_data):
                    queryString = f"select ts, log(c0), ceil(pow(c0,3)) from {self.dbname1}.{self.stbname} where c0 % 7 >= 0"
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

    def start_range_compact(self):
        # cal standard cpu
        timestamp_start = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        time.sleep(self.standard_record_time)
        timestamp_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        self.standard_taosd_avg_cpu, self.standard_taosBenchmark_avg_cpu, _ = self.Prometheus.cal_range_avg(self.prometheus_setting, "cpu_utilization", timestamp_start, timestamp_end, 60)
        self._remote._logger.info(f"------------ standard taosd avg cpu: {self.standard_taosd_avg_cpu} between {timestamp_start} and {timestamp_end} ------------")
        self._remote._logger.info(f"------------ standard taosBenchmark avg cpu: {self.standard_taosBenchmark_avg_cpu} between {timestamp_start} and {timestamp_end} ------------")
        self.logger.info("in start_range_compact scheduler")
        self.tdSql.query(f'select count(*) from {self.dbname1}.{self.stbname}')
        self._remote._logger.info(f"------------ range-compact: query result before compact: {self.tdSql.query_data[0][0]} ------------")
        # Insert_file = Perf_Base_func(self.logger, self.run_log_dir)
        timestamp_start = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        self.tdSql.execute(f'compact database {self.dbname1} start with "{self.stage_1_dt}" end with "{self.today_zero_dt}"')
        self._remote._logger.info(f"------------ remove range compact schedular job ------------: {self.range_compact_schedular}")
        self.tdCom.remove_schedular_job(self.range_compact_schedular)
        self.range_compact_schedular = None
        self.confirm_compact_end()
        # timestamp_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        taosd_avg_cpu, taosBenchmark_avg_cpu, _ = self.Prometheus.cal_range_avg(self.prometheus_setting, "cpu_utilization", timestamp_start, self.compact_end_timestamp, 60)
        self._remote._logger.info(f"------------ range-compact taosd avg cpu: {taosd_avg_cpu} between {timestamp_start} and {self.compact_end_timestamp} ------------")
        self._remote._logger.info(f"------------ range-compact taosBenchmark avg cpu: {taosBenchmark_avg_cpu} between {timestamp_start} and {self.compact_end_timestamp} ------------")
        self.compact_schedular = self.tdCom.add_back_ground_scheduler(self.start_compact, "interval", seconds=self.compact_schedular_interval, max_instances=1, args=[])

    def start_compact(self):
        self._remote._logger.info(f"------------ remove compact schedular job ------------: {self.compact_schedular}")
        self.logger.info("------------ in start_compact scheduler ------------")
        self.tdCom.remove_schedular_job(self.compact_schedular)
        for i in range(self.compact_times):
            timestamp_start = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            self.tdSql.execute(f'compact database {self.dbname1}')
            self.confirm_compact_end()
            # timestamp_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            taosd_avg_cpu, taosBenchmark_avg_cpu, _ = self.Prometheus.cal_range_avg(self.prometheus_setting, "cpu_utilization", timestamp_start, self.compact_end_timestamp, 60)
            self._remote._logger.info(f"------------ compact taosd avg cpu: {taosd_avg_cpu} between {timestamp_start} and {self.compact_end_timestamp} ------------")
            self._remote._logger.info(f"------------ compact taosBenchmark avg cpu: {taosBenchmark_avg_cpu} between {timestamp_start} and {self.compact_end_timestamp} ------------")
            self.final_taosd_avg_cpu_list.append(taosd_avg_cpu)
            self.final_taosBenchmark_avg_cpu_list.append(taosBenchmark_avg_cpu)
            time.sleep(self.compact_schedular_interval)

    def cal_compact_resource(self):
        # flush db1
        self.tdSql.execute(f'flush database {self.dbname1}')
        # record init disk usage
        self.disk_usage_init = self._remote.cmd(self.host, [f'du -sh -k {self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"]}']).split('\t')[0]
        self._remote._logger.info(f"------------ init disk usage: {self.disk_usage_init} ------------")
        self.child_table_exists = "no"
        self.db_drop = "yes"
        # disorder/update/delete rows
        self.insert_fh_data(self.dbname2)
        self.insert_base_data(self.dbname2)
        # flush db2
        self.tdSql.execute(f'flush database {self.dbname2}')
        # record disk usage before compact
        self.disk_usage_before_compact = self._remote.cmd(self.host, [f'du -sh -k {self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"]}']).split('\t')[0]
        self._remote._logger.info(f"------------ disk usage before compact: {self.disk_usage_before_compact} ------------")
        # record mem usage before compact
        self.mem_usage_before_compact = psutil.virtual_memory().used/1024/1024
        self._remote._logger.info(f"------------ mem usage before compact: {self.mem_usage_before_compact}M ------------")
        # record query result before compact
        t1 = datetime.now().timestamp()
        self.tdSql.query(f'select count(*) from {self.dbname2}.{self.stbname}')
        t2 = datetime.now().timestamp()
        self.query_time_before_compact = t2 - t1
        self.query_rows_before_compact = self.tdSql.query_data[0][0]
        self._remote._logger.info(f"------------ query rows before compact: {self.query_rows_before_compact} ------------")
        self._remote._logger.info(f"------------ query time before compact: {self.query_time_before_compact}s ------------")
        timestamp_start = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        self.tdSql.execute(f'compact database {self.dbname2}')
        self.confirm_compact_end()
        # timestamp_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        taosd_avg_cpu, taosBenchmark_avg_cpu, _ = self.Prometheus.cal_range_avg(self.prometheus_setting, "cpu_utilization", timestamp_start, self.compact_end_timestamp, 60)
        self._remote._logger.info(f"------------ compact taosd avg cpu: {taosd_avg_cpu} between {timestamp_start} and {self.compact_end_timestamp} ------------")
        self._remote._logger.info(f"------------ compact taosBenchmark avg cpu: {taosBenchmark_avg_cpu} between {timestamp_start} and {self.compact_end_timestamp} ------------")
        # record disk usage after compact
        self.disk_usage_after_compact = self._remote.cmd(self.host, [f'du -sh -k {self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"]}']).split('\t')[0]
        self._remote._logger.info(f"------------ disk usage after compact: {self.disk_usage_after_compact} ------------")
        # record mem usage before compact
        self.mem_usage_after_compact = psutil.virtual_memory().used/1024/1024
        self._remote._logger.info(f"------------ mem usage after compact: {self.mem_usage_after_compact}M ------------")
        # record query result after compact
        t1 = datetime.now().timestamp()
        self.tdSql.query(f'select count(*) from {self.dbname2}.{self.stbname}')
        t2 = datetime.now().timestamp()
        self.query_time_after_compact = t2 - t1
        self.query_rows_after_compact = self.tdSql.query_data[0][0]
        self._remote._logger.info(f"------------ query rows after compact: {self.query_rows_after_compact} ------------")
        self._remote._logger.info(f"------------ query time after compact: {self.query_time_after_compact}s ------------")
        self._remote.cmd(self.taosBenchmark_iplist[0], ['cp -rf /tmp/0.log /tmp/0_2.log'])

    # def cal_query_result(self):
    #     self.tdSql.execute(f'flush database {self.dbname1}')
    #     self.tdSql.query(f'select count(*) from {self.dbname1}.{self.stbname}')
    #     timestamp_start = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    #     self.tdSql.execute(f'compact database {self.dbname1}')
    #     timestamp_end = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
    #     taosd_avg_cpu, taosBenchmark_avg_cpu, _ = self.Prometheus.cal_range_avg(self.prometheus_setting, "cpu_utilization", timestamp_start, timestamp_end, 60)
    #     self._remote._logger.info(f"------------ compact taosd avg cpu: {taosd_avg_cpu} between {timestamp_start} and {timestamp_end} ------------")
    #     self._remote._logger.info(f"------------ compact taosBenchmark avg cpu: {taosBenchmark_avg_cpu} between {timestamp_start} and {timestamp_end} ------------")
    #     disk_usage = self._remote.cmd(self.host, [f'du -sh -k {self.taosd_setting["spec"]["dnodes"][0]["config"]["dataDir"]}']).split('\t')[0]
    #     self._remote._logger.info(f"------------ disk usage after compact: {disk_usage} ------------")

    def confirm_compact_end(self):
        pattern = "compact.*rows"
        log_file = self.taosd_setting["spec"]["dnodes"][0]["config"]["logDir"] + "/taosdlog.0"
        # pattern = '\|'.join(self.compact_pat_list)
        pat_log_info = self._remote.cmd(self.host, [f'grep "{pattern}" {log_file} | tail -n 1'])
        time.sleep(self.compact_wait)
        self.pat_log_info = self._remote.cmd(self.host, [f'grep "{pattern}" {log_file} | tail -n 1'])
        t_counter = 0
        while pat_log_info != self.pat_log_info:
            if t_counter < self.compact_confirm_timeout:
                pat_log_info = self._remote.cmd(self.host, [f'grep "{pattern}" {log_file} | tail -n 1'])
                time.sleep(self.compact_wait)
                self.pat_log_info = self._remote.cmd(self.host, [f'grep "{pattern}" {log_file} | tail -n 1'])
                t_counter += self.compact_wait
                self._remote._logger.info(f"------------ expected end: {pat_log_info} ------------")
                self._remote._logger.info(f"------------ actual   end: {self.pat_log_info} ------------")
                if pat_log_info == self.pat_log_info:
                    self._remote._logger.info(f"------------ compact already finished ------------")
                self.compact_end_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
            else:
                self._remote._logger.error(f"------------ confirm compact end timeout after {self.compact_confirm_timeout}s ------------")
                self._remote._logger.error(f"------------ expected end: {pat_log_info} ------------")
                self._remote._logger.error(f"------------ actual   end: {self.pat_log_info} ------------")
                return

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def run(self):
        self.insert_fh_data(self.dbname1)
        self.insert_base_data(self.dbname1)
        self.alter_db_keep_param()
        self.tmq_schedular = self.tdCom.add_back_ground_scheduler(self.tmq_subcribe, "interval", seconds=self.tmq_schedular_interval, max_instances=1, args=[])
        self.range_compact_schedular = self.tdCom.add_back_ground_scheduler(self.start_range_compact, "interval", seconds=self.compact_schedular_interval, max_instances=1, args=[])
        # self.disorder_schedular = self.tdCom.add_back_ground_scheduler(self.disorder_update_delete_data, "interval", seconds=self.disorder_schedular_interval, max_instances=1, args=[])
        self.continue_insert()
        self.cal_compact_resource()
        self._remote._logger.info(f"------------ compact blocking taosd cpu list: {self.final_taosd_avg_cpu_list} ------------")
        self._remote._logger.info(f"------------ compact blocking taosBenchmark cpu list: {self.final_taosBenchmark_avg_cpu_list} ------------")

        self._remote._logger.info(f"------------ compact standard taosd cpu: {self.standard_taosd_avg_cpu} ------------")
        self._remote._logger.info(f"------------ compact standard taosBenchmark cpu: {self.standard_taosBenchmark_avg_cpu} ------------")

        self._remote._logger.info(f"------------ compact blocking taosd cpu: {sum(self.final_taosd_avg_cpu_list)/len(self.final_taosd_avg_cpu_list)} ------------")
        self._remote._logger.info(f"------------ compact blocking taosBenchmark cpu: {sum(self.final_taosBenchmark_avg_cpu_list)/len(self.final_taosBenchmark_avg_cpu_list)} ------------")
        
        self._remote._logger.info(f"------------ query rows before compact: {self.query_rows_before_compact} ------------")
        self._remote._logger.info(f"------------ query rows after compact: {self.query_rows_after_compact} ------------")
        
        self._remote._logger.info(f"------------ query time before compact: {self.query_time_before_compact}s ------------")
        self._remote._logger.info(f"------------ query time after compact: {self.query_time_after_compact}s ------------")
        
        self._remote._logger.info(f"------------ init disk usage: {self.disk_usage_init} ------------")
        self._remote._logger.info(f"------------ disk usage before compact: {self.disk_usage_before_compact} ------------")
        self._remote._logger.info(f"------------ disk usage after compact: {self.disk_usage_after_compact} ------------")
        
        self._remote._logger.info(f"------------ mem usage before compact: {self.mem_usage_before_compact}M ------------")
        self._remote._logger.info(f"------------ mem usage after compact: {self.mem_usage_after_compact}M ------------")
        