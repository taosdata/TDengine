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
from taostest.performance.result_reduction import Perf_Base_func
import sys
from datetime import datetime

class MultiTableLowFreq(TDCase):
    def init(self):
        self.vgroups_list = [10, 20, 40]
        self.childtable_count_list = [10000000, 20000000, 40000000]
        self.buffer = 4096
        self.stt_trigger = 16
        self.column_info_list1 = [
            {"type": "INT", "name": "xint1", "min": 1, "max": 100, "count": 1},
            {"type": "INT", "name": "xint2", "min": -10000, "max": 10000, "count": 1},
            {"type": "DOUBLE", "name": "xdouble1", "min": 1.0, "max": 9999.0, "count": 1},
            {"type": "DOUBLE", "name": "xdouble2", "min": -99999.0, "max": 99999.0, "count": 1}
        ]
        self.column_info_list2 = [
            {"type": "INT", "name": "xint1", "min": 1, "max": 100, "count": 1},
            {"type": "INT", "name": "xint2", "min": 1, "max": 100, "count": 1},
            {"type": "INT", "name": "xint3", "min": 1, "max": 100, "count": 1},
            {"type": "INT", "name": "xint4", "min": 1, "max": 100, "count": 1},
            {"type": "INT", "name": "xint5", "min": 1, "max": 100, "count": 1},
            {"type": "INT", "name": "xint6", "min": -10000, "max": 10000, "count": 1},
            {"type": "INT", "name": "xint7", "min": -10000, "max": 10000, "count": 1},
            {"type": "INT", "name": "xint8", "min": -10000, "max": 10000, "count": 1},
            {"type": "INT", "name": "xint9", "min": -10000, "max": 10000, "count": 1},
            {"type": "INT", "name": "xint10", "min": -10000, "max": 10000, "count": 1},
            {"type": "DOUBLE", "name": "xdouble1", "min": 1.0, "max": 9999.0, "count": 1},
            {"type": "DOUBLE", "name": "xdouble2", "min": 1.0, "max": 9999.0, "count": 1},
            {"type": "DOUBLE", "name": "xdouble3", "min": 1.0, "max": 9999.0, "count": 1},
            {"type": "DOUBLE", "name": "xdouble4", "min": 1.0, "max": 9999.0, "count": 1},
            {"type": "DOUBLE", "name": "xdouble5", "min": 1.0, "max": 9999.0, "count": 1},
            {"type": "DOUBLE", "name": "xdouble6", "min": -99999.0, "max": 99999.0, "count": 1},
            {"type": "DOUBLE", "name": "xdouble7", "min": -99999.0, "max": 99999.0, "count": 1},
            {"type": "DOUBLE", "name": "xdouble8", "min": -99999.0, "max": 99999.0, "count": 1},
            {"type": "DOUBLE", "name": "xdouble9", "min": -99999.0, "max": 99999.0, "count": 1},
            {"type": "DOUBLE", "name": "xdouble10", "min": -99999.0, "max": 99999.0, "count": 1},
            {"type": "BINARY", "name": "xbinary1", "values": ["colvalue1", "colvalue2", "colvalue3", "colvalue4", "colvalue5", "colvalue6","colvalue7", "colvalue8", "colvalue9", "colvalue10"], "len": 80, "count": 1},
            {"type": "BINARY", "name": "xbinary2", "values": ["colvalue1", "colvalue2", "colvalue3", "colvalue4", "colvalue5", "colvalue6","colvalue7", "colvalue8", "colvalue9", "colvalue10"], "len": 80, "count": 1},
            {"type": "BINARY", "name": "xbinary3", "values": ["colvalue1", "colvalue2", "colvalue3", "colvalue4", "colvalue5", "colvalue6","colvalue7", "colvalue8", "colvalue9", "colvalue10"], "len": 80, "count": 1},
            {"type": "BINARY", "name": "xbinary4", "values": ["colvalue1", "colvalue2", "colvalue3", "colvalue4", "colvalue5", "colvalue6","colvalue7", "colvalue8", "colvalue9", "colvalue10"], "len": 80, "count": 1},
            {"type": "BINARY", "name": "xbinary5", "values": ["colvalue1", "colvalue2", "colvalue3", "colvalue4", "colvalue5", "colvalue6","colvalue7", "colvalue8", "colvalue9", "colvalue10"], "len": 80, "count": 1},
            {"type": "BINARY", "name": "xbinary6", "values": ["colvalue1", "colvalue2", "colvalue3", "colvalue4", "colvalue5", "colvalue6","colvalue7", "colvalue8", "colvalue9", "colvalue10"], "len": 80, "count": 1},
            {"type": "BINARY", "name": "xbinary7", "values": ["colvalue1", "colvalue2", "colvalue3", "colvalue4", "colvalue5", "colvalue6","colvalue7", "colvalue8", "colvalue9", "colvalue10"], "len": 80, "count": 1},
            {"type": "BINARY", "name": "xbinary8", "values": ["colvalue1", "colvalue2", "colvalue3", "colvalue4", "colvalue5", "colvalue6","colvalue7", "colvalue8", "colvalue9", "colvalue10"], "len": 80, "count": 1},
            {"type": "BINARY", "name": "xbinary9", "values": ["colvalue1", "colvalue2", "colvalue3", "colvalue4", "colvalue5", "colvalue6","colvalue7", "colvalue8", "colvalue9", "colvalue10"], "len": 80, "count": 1},
            {"type": "BINARY", "name": "xbinary10", "values": ["colvalue1", "colvalue2", "colvalue3", "colvalue4", "colvalue5", "colvalue6","colvalue7", "colvalue8", "colvalue9", "colvalue10"], "len": 80, "count": 1}
        ]
        self.tag_info_list = [
          {"type": "BINARY", "name": "xtag", "values": ["tag1", "tag2", "tag3", "tag4", "tag5", "tag6","tag7", "tag8", "tag9", "tag10"], "len": 8, "count": 1}
        ]
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.result_file_name = self.run_log_dir + '/perf_report.txt'
        self.file_name1 = "insert0.json"
        self.file_name2 = "insert1.json"
        self._tmp_dir: str = os.path.join(self.run_log_dir, "tmp")
        self.replica = 1
        # self.vgroups = 10
        self.create_table_thread_count = 40
        self.thread_count = 40
        self.interlace_rows = 1
        # self.interlace_rows = 0
        self.childtable_prefix = "ctb1_"
        self.childtable_prefix2 = "ct2_"
        self.disorder_ratio = 10
        self.disorder_range = 864000000
        self.timestamp_step = 86400000
        self.insert_rows = 100
        # self.insert_rows = 10000
        self.num_of_records_per_req = 1000
        self.batch_create_tbl_num = 1000
        self.dbname1 = "test"
        self.stbname = "stb"
        self.insert_mode = "taosc"
        self.json_data_list = list()
        self.json_filename_list = list()
        self.taosd_host = self.get_fqdn("taosd")[0]
        self.interval = "2h"
        self.insert = False

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def order_insert(self, vgroups, column_info_list, childtable_count):
        taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
        taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")

        self.json_filename_list.append(self.file_name1)
        dbinfo = self.tdCom.setDBinfo(name=self.dbname1, replica=self.replica, vgroups=vgroups, buffer=self.buffer, stt_trigger=self.stt_trigger)
        stb_into = [self.tdCom.setStbinfo(columns=column_info_list, tags=self.tag_info_list, childtable_count=childtable_count, childtable_prefix=self.childtable_prefix, insert_rows=self.insert_rows, batch_create_tbl_num=self.batch_create_tbl_num, insert_mode=self.insert_mode, interlace_rows=self.interlace_rows, timestamp_step=self.timestamp_step)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]

        json_info1 = self.tdCom.setJsoninfo(host=self.taosd_host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name1, json_info1)
        self.json_data_list.append(json_info1)

        self.tdCom.put_file(self._remote, taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.run_log_dir)
        timestamp_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        result_file_list = self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, self.json_data_list, self.json_filename_list, taosBenchmark_env_setting, self.run_log_dir)
        timestamp_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        Insert_file = Perf_Base_func(self._remote._logger, self.run_log_dir)
        Insert_file.taosBenchmark_insert_summary_result(result_file_list, version="3.0")
        env_setting = self.get_component_by_name("prometheus")
        Insert_file.get_process_exporter_info(env_setting, 30, timestamp_start, timestamp_end)
        Insert_file.get_node_exporter_info(env_setting, 10, timestamp_start, timestamp_end)

    def order_insert_2b(self, vgroups, column_info_list, childtable_count):
        taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
        taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        self.tdCom.createDb(dbname=self.dbname1, replica=self.replica, vgroups=vgroups, buffer=self.buffer, stt_trigger=self.stt_trigger)

        self.json_filename_list.append(self.file_name1)
        dbinfo = self.tdCom.setDBinfo(name=self.dbname1, replica=self.replica, vgroups=vgroups, buffer=self.buffer, stt_trigger=self.stt_trigger, drop="no")
        stb_into = [self.tdCom.setStbinfo(columns=column_info_list, tags=self.tag_info_list, childtable_count=int(childtable_count/2), childtable_prefix=self.childtable_prefix, insert_rows=int(self.insert_rows/2), batch_create_tbl_num=self.batch_create_tbl_num, insert_mode=self.insert_mode, interlace_rows=self.interlace_rows, timestamp_step=self.timestamp_step)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]

        json_info1 = self.tdCom.setJsoninfo(host=self.taosd_host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name1, json_info1)
        self.json_data_list.append(json_info1)

        self.json_filename_list.append(self.file_name2)
        stb_into = [self.tdCom.setStbinfo(columns=column_info_list, tags=self.tag_info_list, childtable_count=int(childtable_count/2), childtable_prefix=self.childtable_prefix2, insert_rows=int(self.insert_rows/2), batch_create_tbl_num=self.batch_create_tbl_num, insert_mode=self.insert_mode, interlace_rows=self.interlace_rows, timestamp_step=self.timestamp_step)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        json_info2 = self.tdCom.setJsoninfo(host=self.taosd_host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name2, json_info2)
        self.json_data_list.append(json_info2)

        self.tdCom.put_file(self._remote, taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.run_log_dir)
        timestamp_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        result_file_list = self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, self.json_data_list, self.json_filename_list, taosBenchmark_env_setting, self.run_log_dir)
        timestamp_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        Insert_file = Perf_Base_func(self._remote._logger, self.run_log_dir)
        Insert_file.taosBenchmark_insert_summary_result(result_file_list, version="3.0")
        env_setting = self.get_component_by_name("prometheus")
        Insert_file.get_process_exporter_info(env_setting, 30, timestamp_start, timestamp_end)
        Insert_file.get_node_exporter_info(env_setting, 10, timestamp_start, timestamp_end)

    def disorder_insert(self, vgroups, column_info_list, childtable_count):
        self.write_log(f'\n****************************** disorder ratio: {self.disorder_ratio} ******************************\n\n')
        taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
        taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")

        self.json_filename_list.append(self.file_name1)
        dbinfo = self.tdCom.setDBinfo(name=self.dbname1, replica=self.replica, vgroups=vgroups, buffer=self.buffer, stt_trigger=self.stt_trigger)
        stb_into = [self.tdCom.setStbinfo(columns=column_info_list, tags=self.tag_info_list, childtable_count=childtable_count, childtable_prefix=self.childtable_prefix, insert_rows=self.insert_rows, batch_create_tbl_num=self.batch_create_tbl_num, insert_mode=self.insert_mode, interlace_rows=self.interlace_rows, timestamp_step=self.timestamp_step, disorder_range=self.disorder_range, disorder_ratio=self.disorder_ratio)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]

        json_info1 = self.tdCom.setJsoninfo(host=self.taosd_host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name1, json_info1)
        self.json_data_list.append(json_info1)

        self.tdCom.put_file(self._remote, taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.run_log_dir)
        timestamp_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        result_file_list = self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, self.json_data_list, self.json_filename_list, taosBenchmark_env_setting, self.run_log_dir)
        timestamp_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        Insert_file = Perf_Base_func(self._remote._logger, self.run_log_dir)
        Insert_file.taosBenchmark_insert_summary_result(result_file_list, version="3.0")
        env_setting = self.get_component_by_name("prometheus")
        Insert_file.get_process_exporter_info(env_setting, 30, timestamp_start, timestamp_end)
        Insert_file.get_node_exporter_info(env_setting, 10, timestamp_start, timestamp_end)

    def diff_disorder_insert(self, vgroups, column_info_list, childtable_count, disorder_ratio):
        self.write_log(f'\n****************************** disorder ratio: {disorder_ratio} ******************************\n\n')
        taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
        taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")

        self.json_filename_list.append(self.file_name1)
        dbinfo = self.tdCom.setDBinfo(name=self.dbname1, replica=self.replica, vgroups=vgroups, buffer=self.buffer, stt_trigger=self.stt_trigger)
        stb_into = [self.tdCom.setStbinfo(columns=column_info_list, tags=self.tag_info_list, childtable_count=childtable_count, childtable_prefix=self.childtable_prefix, insert_rows=self.insert_rows, batch_create_tbl_num=self.batch_create_tbl_num, insert_mode=self.insert_mode, interlace_rows=self.interlace_rows, timestamp_step=self.timestamp_step, disorder_range=self.disorder_range, disorder_ratio=disorder_ratio)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]

        json_info1 = self.tdCom.setJsoninfo(host=self.taosd_host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name1, json_info1)
        self.json_data_list.append(json_info1)

        self.tdCom.put_file(self._remote, taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.run_log_dir)
        timestamp_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        result_file_list = self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, self.json_data_list, self.json_filename_list, taosBenchmark_env_setting, self.run_log_dir)
        timestamp_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        Insert_file = Perf_Base_func(self._remote._logger, self.run_log_dir)
        Insert_file.taosBenchmark_insert_summary_result(result_file_list, version="3.0")
        env_setting = self.get_component_by_name("prometheus")
        Insert_file.get_process_exporter_info(env_setting, 30, timestamp_start, timestamp_end)
        Insert_file.get_node_exporter_info(env_setting, 10, timestamp_start, timestamp_end)

    # def query_interval(self, insert):
    #     if insert:
    #         taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
    #         taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")

    #         self.json_filename_list.append(self.file_name1)
    #         dbinfo = self.tdCom.setDBinfo(name=self.dbname1, replica=self.replica, vgroups=self.vgroups)
    #         stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count1, childtable_prefix=self.childtable_prefix1, insert_rows=self.insert_rows, batch_create_tbl_num=self.batch_create_tbl_num, insert_mode=self.insert_mode, interlace_rows=self.interlace_rows)]
    #         database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]

    #         json_info1 = self.tdCom.setJsoninfo(host=self.taosd_host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
    #         self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name1, json_info1)
    #         self.json_data_list.append(json_info1)

    #         self.tdCom.put_file(self._remote, taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.run_log_dir)
    #         result_file_list = self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, self.json_data_list, self.json_filename_list, taosBenchmark_env_setting, self.run_log_dir)
    #         Insert_file = Perf_Base_func(self._remote._logger, self.run_log_dir)
    #         Insert_file.taosBenchmark_insert_summary_result(result_file_list, version="3.0")
    #     total_rows = self._remote.cmd(self.taosd_host, [f'taos -s "select count(*) from {self.dbname1}.{self.stbname};"'])
    #     query_res = self._remote.cmd(self.taosd_host, [f'taos -s "select count(*) from {self.dbname1}.{self.stbname} interval ({self.interval});"'])
    #     with open(self.result_file_name, 'a') as f:
    #         f.write('****************************** total rows ******************************\n')
    #         f.write(total_rows)
    #         f.write('\n\n****************************** 10000000000 interval ******************************\n')
    #         f.write(query_res)

    def write_log(self, msg):
        f = open(self.result_file_name, 'a')
        f.write(msg)
        f.close()

    def run(self):
        # for column_info_list in [self.column_info_list1, self.column_info_list2]:
        #     self.write_log(f'\n****************************** column_info_list: {column_info_list} ******************************\n\n')
        #     for vgroups, childtable_count in zip(self.vgroups_list, self.childtable_count_list):
        #         self.write_log(f'\n\n****************************** vgroups: {vgroups} childtable_count: {childtable_count} ******************************\n\n')
                # self.order_insert(vgroups, column_info_list, childtable_count)
                # self.disorder_insert(vgroups, column_info_list, childtable_count)
        # self.order_insert(40, self.column_info_list1, 10000)
        # for disorder_ratio in [20, 50, 100]:
        #     self.diff_disorder_insert(vgroups=10, column_info_list=self.column_info_list1, childtable_count=10000000, disorder_ratio=disorder_ratio)
        # self.order_insert_2b(self.vgroups_list[0], self.column_info_list2, self.childtable_count_list[0])
        self.replica = 3
        for column_info_list in [self.column_info_list1, self.column_info_list2]:
            self.write_log(f'\n****************************** column_info_list: {column_info_list} ******************************\n\n')
            for vgroups, childtable_count in zip(self.vgroups_list, self.childtable_count_list):
                self.write_log(f'\n\n****************************** vgroups: {vgroups} childtable_count: {childtable_count} ******************************\n\n')
                self.order_insert(vgroups, column_info_list, childtable_count)
                self.disorder_insert(vgroups, column_info_list, childtable_count)
        print(self.result_file_name)