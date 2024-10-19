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

# -*- taostest --setup=cluster/compact_test.yaml --case=cluster/compact_test.py --keep -*-
# -*- taostest --setup=cluster/compact_test_rep3.yaml --case=cluster/compact_test.py --keep -*-

from taostest.util.common import TDCom
from taostest import TDCase
from taostest.components.taosd import TaosD
from taostest.util.remote import Remote
import copy
from taos.tmq import Consumer
import os

class CompactTest(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.json_filename = "insert0.json"
        self.replica = int(os.environ["DATABASE_REPLICAS"]) if "DATABASE_REPLICAS" in os.environ else 1
        self.create_table_thread_count = 40
        self.childtable_count = 3000
        self.keep = "5d"
        # self.keep = "7d"
        self.duration = "1d"
        self.today_zero_ts = self.tdCom.genTodayZeroTs()
        self.today_zero_dt = self.tdCom.genTs(ts=self.today_zero_ts/1000)[1]
        self.stage_1_timestamp = self.today_zero_ts - self.tdCom.trans_time_to_s(self.keep) * 1000 + 86400 * 1000
        self.stage_1_dt = self.tdCom.genTs(ts=self.stage_1_timestamp/1000)[1]
        self.disorder_start_ts = self.today_zero_ts
        self.disorder_day = 2
        self.stage_rows = 3000
        self.insert_rows = 1000000

        self.wal_retention_period = 1
        self.stbname = "stb"
        self.trans_stbname = "trans_stb"
        self.trans_ctbname = "trans_ctb"
        self.trans_tbname = "trans_tb"
        self.dbname1 = "stream_test"
        self.dbname2 = "compact_disk_usage_test"
        self.stream_stbname = "output_streamtb"
        self.stream_stbname2 = "output_streamtb2"
        self.stream_name = "test_stream"
        self.stream_name2 = "test_stream2"
        self.trigger_mode = "max_delay 6s"
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
        self.disorder_ratio = 30
        self.update_ratio = 30
        self.delete_ratio = 10
        self.disorder_fill_interval = 300
        self.update_fill_interval = 25
        self.generate_row_rule = 2
        self.stream_sql1 = f"select ts,max(c1) from {self.dbname1}.{self.stbname} where c1>0 partition by tbname interval(1s) sliding(1s)"
        self.stream_sql2 = f"select ts,max(c1) from {self.dbname2}.{self.stbname} where c1>0 partition by tbname interval(1s) sliding(1s)"

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
        self.json_data_list = list()
        self.taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
        self.taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")

        self.tmq_schedular = None
        self.trans_schedular = None

        self.tmq_schedular_interval = 60
        self.trans_schedular_interval = 60

    def insert_fh_data(self, dbname):
        json_filename_list = [self.json_file_name1]
        dbinfo = self.tdCom.setDBinfo(name=dbname, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, wal_retention_period=self.wal_retention_period, keep=self.keep, duration=self.duration)
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.stage_rows, start_timestamp=self.stage_1_timestamp, child_table_exists=self.child_table_exists, name=self.stbname, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows, disorder_ratio=self.disorder_ratio, update_ratio=self.update_ratio, delete_ratio=self.delete_ratio, disorder_fill_interval=self.disorder_fill_interval, update_fill_interval=self.update_fill_interval, generate_row_rule=self.generate_row_rule)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name1, json_info)
        self.json_data_list = [json_info]
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
            dbinfo = self.tdCom.setDBinfo(name=dbname, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, wal_retention_period=self.wal_retention_period, keep=self.keep, duration=self.duration)
            stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.stage_rows, start_timestamp=advance_timestamp, child_table_exists=self.child_table_exists, name=self.stbname, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows, disorder_ratio=self.disorder_ratio, update_ratio=self.update_ratio, delete_ratio=self.delete_ratio, disorder_fill_interval=self.disorder_fill_interval, update_fill_interval=self.update_fill_interval, generate_row_rule=self.generate_row_rule)]
            database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
            host = self.get_fqdn("taosd")[0]
            self.stream_drop = "no" if i > 2 else "yes"
            stream_db_info = self.tdCom.setStreamDBinfo(name=dbname, vgroups=self.vgroups, drop=self.db_drop)
            stream_info = self.tdCom.setStreams(stream_name=self.stream_name, stream_stb=f'{dbname}.{self.stream_stbname}', trigger_mode=self.trigger_mode, drop=self.stream_drop, source_sql=self.stream_sql1)
            json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, streams=stream_info, stream_db=stream_db_info, num_of_records_per_req=self.num_of_records_per_req)
            self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name1, json_info)
            self.json_data_list = [json_info]
            self.tdCom.put_file(self._remote, self.taosBenchmark_iplist, self.json_data_list, json_filename_list, self.run_log_dir)
            self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, self.json_data_list, json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)
        self.tdSql.execute(f'flush database {dbname}')
        self._remote.cmd(self.taosBenchmark_iplist[0], ['cp -rf /tmp/0.log /tmp/0_0.log'])

    def transaction_nblock_test(self):
        self.tdSql.execute(f'create stream if not exists {self.stream_name2} trigger {self.trigger_mode} ignore expired 0 ignore update 0  into {self.dbname1}.{self.stream_stbname2} as {self.stream_sql1};')
        self.tdSql.execute(f'create stable if not exists {self.dbname1}.{self.trans_stbname} (ts timestamp, c1 int) tags (t1 int);')
        self.tdSql.execute(f'create table if not exists {self.dbname1}.{self.trans_ctbname} using {self.dbname1}.{self.trans_stbname} tags (1);')
        self.tdSql.execute(f'create table if not exists {self.dbname1}.{self.trans_tbname} (ts timestamp, c1 int);')
        self.tdSql.execute(f'insert into {self.dbname1}.{self.trans_ctbname} values (now, 1);')
        self.tdSql.execute(f'insert into {self.dbname1}.{self.trans_tbname} values (now, 1);')
        self.tdSql.query(f'select count(*) from {self.dbname1}.{self.trans_stbname};')
        self.tdSql.checkEqual(self.tdSql.query_data[0][0], 1)
        self.tdSql.query(f'select count(*) from {self.dbname1}.{self.trans_ctbname};')
        self.tdSql.checkEqual(self.tdSql.query_data[0][0], 1)
        self.tdSql.query(f'select count(*) from {self.dbname1}.{self.trans_tbname};')
        self.tdSql.checkEqual(self.tdSql.query_data[0][0], 1)
        self.tdSql.execute(f'pause stream if exists {self.stream_name} ')
        self.tdSql.execute(f'insert into {self.dbname1}.{self.trans_ctbname} values (now, 1);')
        self.tdSql.execute(f'resume stream if exists {self.stream_name} ')
        self.tdSql.execute(f'alter stable  {self.dbname1}.{self.trans_stbname} add column c2 int')
        self.tdSql.execute(f'drop table if exists {self.dbname1}.{self.trans_stbname}')
        self.tdSql.execute(f'drop table if exists {self.dbname1}.{self.trans_ctbname}')
        self.tdSql.execute(f'drop table if exists {self.dbname1}.{self.trans_tbname}')
        self.tdSql.execute(f'drop stream if exists {self.stream_name} ')
        self.tdSql.execute(f'drop stream if exists {self.stream_name2} ')

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
        # self.trans_schedular = self.tdCom.add_back_ground_scheduler(self.transaction_nblock_test, "interval", seconds=self.trans_schedular_interval, max_instances=1, args=[])
        self.tmq_schedular = self.tdCom.add_back_ground_scheduler(self.tmq_subcribe, "interval", seconds=self.tmq_schedular_interval, max_instances=1, args=[])
        self.insert_base_data(self.dbname1)
        self.tdSql.query('show transactions')
        self.tdSql.checkEqual(self.tdSql.query_row, 1)
        self.transaction_nblock_test()
        self.tdSql.query('show transactions')
        self.tdSql.checkEqual(self.tdSql.query_row, 0)
