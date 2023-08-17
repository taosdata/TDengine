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

class PartialColUpdate(TDCase):
    def init(self):
        self.column_info_list1 = [
            {"type": "INT", "name": "xint1", "min": 1, "max": 100, "count": 1},
            {"type": "INT", "name": "xint2", "min": -10000, "max": 10000, "count": 1},
            {"type": "INT", "name": "xint3", "min": -10000, "max": 10000, "count": 1},
            {"type": "INT", "name": "xint4", "min": -10000, "max": 10000, "count": 1},
            {"type": "DOUBLE", "name": "xdouble1", "min": 1.0, "max": 9999.0, "count": 1},
            {"type": "DOUBLE", "name": "xdouble2", "min": 1.0, "max": 9999.0, "count": 1},
            {"type": "DOUBLE", "name": "xdouble3", "min": 1.0, "max": 9999.0, "count": 1},
            {"type": "DOUBLE", "name": "xdouble4", "min": -99999.0, "max": 99999.0, "count": 1},
            {"type": "BINARY", "name": "xbinary1", "values": ["cv1", "cv2", "cv3", "cv4", "cv5", "cv6","cv7", "cv8", "cv9", "cv10"], "len": 4, "count": 1},
            {"type": "BINARY", "name": "xbinary2", "values": ["cv1", "cv2", "cv3", "cv4", "cv5", "cv6","cv7", "cv8", "cv9", "cv10"], "len": 4, "count": 1},
        ]
        self.column_info_list2 = [
            {"type": "INT", "name": "xint5", "min": -10000, "max": 10000, "count": 1},
            {"type": "DOUBLE", "name": "xdouble5", "min": -99999.0, "max": 99999.0, "count": 1},
            {"type": "BINARY", "name": "xbinary3", "values": ["cv1", "cv2", "cv3", "cv4", "cv5", "cv6","cv7", "cv8", "cv9", "cv10"], "len": 4, "count": 1},
            {"type": "BINARY", "name": "xbinary4", "values": ["cv1", "cv2", "cv3", "cv4", "cv5", "cv6","cv7", "cv8", "cv9", "cv10"], "len": 4, "count": 1}
        ]
        self.column_info_list3 = [
            {"type": "INT", "name": "xint1", "min": 1, "max": 100, "count": 1},
            {"type": "INT", "name": "xint2", "min": -10000, "max": 10000, "count": 1},
            {"type": "INT", "name": "xint3", "min": -10000, "max": 10000, "count": 1},
            {"type": "INT", "name": "xint4", "min": -10000, "max": 10000, "count": 1},
            {"type": "DOUBLE", "name": "xdouble1", "min": 1.0, "max": 9999.0, "count": 1},
            {"type": "DOUBLE", "name": "xdouble2", "min": 1.0, "max": 9999.0, "count": 1},
            {"type": "DOUBLE", "name": "xdouble3", "min": 1.0, "max": 9999.0, "count": 1},
            {"type": "DOUBLE", "name": "xdouble4", "min": -99999.0, "max": 99999.0, "count": 1},
            {"type": "BINARY", "name": "xbinary1", "values": ["cv1", "cv2", "cv3", "cv4", "cv5", "cv6","cv7", "cv8", "cv9", "cv10"], "len": 4, "count": 1},
            {"type": "BINARY", "name": "xbinary2", "values": ["cv1", "cv2", "cv3", "cv4", "cv5", "cv6","cv7", "cv8", "cv9", "cv10"], "len": 4, "count": 1},
            {"type": "INT", "name": "xint5", "min": -10000, "max": 10000, "count": 1},
            {"type": "DOUBLE", "name": "xdouble5", "min": -99999.0, "max": 99999.0, "count": 1},
            {"type": "BINARY", "name": "xbinary3", "values": ["cv1", "cv2", "cv3", "cv4", "cv5", "cv6","cv7", "cv8", "cv9", "cv10"], "len": 4, "count": 1}
        ]
        self.tag_info_list = [
          {"type": "BINARY", "name": "xtag", "values": ["tag1", "tag2", "tag3", "tag4", "tag5", "tag6","tag7", "tag8", "tag9", "tag10"], "len": 5, "count": 1}
        ]
        self.partial_col_num = 3
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.result_file_name = self.run_log_dir + '/perf_report.txt'
        self.file_name1 = "insert0.json"
        self.file_name2 = "insert1.json"
        self.file_name3 = "insert2.json"
        self._tmp_dir: str = os.path.join(self.run_log_dir, "tmp")
        self.replica = 1
        self.buffer = 4096
        self.vgroups = 40
        self.childtable_count = 10000
        self.insert_rows = 10000
        self.create_table_thread_count = 40
        self.thread_count = 100
        self.interlace_rows = 80
        self.childtable_prefix = "ctb_"
        self.start_timestamp = "2023-01-01 00:00:00"
        self.timestamp_step = 1000
        self.num_of_records_per_req = 8000
        self.batch_create_tbl_num = 10000
        self.dbname = "test"
        self.stbname = "stb"
        self.insert_mode = "taosc"
        self.json_data_list = list()
        self.json_filename_list = list()
        self.taosd_host = self.get_fqdn("taosd")[0]

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def prepare(self):
        taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
        taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")

        self.json_filename_list = [self.file_name1]
        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups)
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list1, tags=self.tag_info_list, childtable_count=self.childtable_count, childtable_prefix=self.childtable_prefix, insert_rows=self.insert_rows, batch_create_tbl_num=self.batch_create_tbl_num, interlace_rows=self.interlace_rows, timestamp_step=self.timestamp_step, start_timestamp=self.start_timestamp)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]

        json_info1 = self.tdCom.setJsoninfo(host=self.taosd_host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name1, json_info1)
        self.json_data_list = [json_info1]

        self.tdCom.put_file(self._remote, taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.run_log_dir)
        timestamp_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        result_file_list = self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, self.json_data_list, self.json_filename_list, taosBenchmark_env_setting, self.run_log_dir)
        timestamp_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        Insert_file = Perf_Base_func(self._remote._logger, self.run_log_dir)
        Insert_file.taosBenchmark_insert_summary_result(result_file_list, version="3.0")
        env_setting = self.get_component_by_name("prometheus")
        Insert_file.get_process_exporter_info(env_setting, 30, timestamp_start, timestamp_end)
        Insert_file.get_node_exporter_info(env_setting, 10, timestamp_start, timestamp_end)

    def partial_insert(self):
        self.tdSql.execute(f'alter stable {self.dbname}.{self.stbname} add column xint5 int')
        self.tdSql.execute(f'alter stable {self.dbname}.{self.stbname} add column xdouble5 double')
        self.tdSql.execute(f'alter stable {self.dbname}.{self.stbname} add column xbinary3 binary(12)')
        taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
        taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        self.json_filename_list = [self.file_name2]
        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop="no")
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list2, tags=self.tag_info_list, childtable_count=self.childtable_count, childtable_prefix=self.childtable_prefix, insert_rows=self.insert_rows, batch_create_tbl_num=self.batch_create_tbl_num, interlace_rows=self.interlace_rows, timestamp_step=self.timestamp_step, start_timestamp=self.start_timestamp, partial_col_num=self.partial_col_num, child_table_exists="yes")]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        json_info2 = self.tdCom.setJsoninfo(host=self.taosd_host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name2, json_info2)
        self.json_data_list = [json_info2]

        self.tdCom.put_file(self._remote, taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.run_log_dir)
        timestamp_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        result_file_list = self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, self.json_data_list, self.json_filename_list, taosBenchmark_env_setting, self.run_log_dir)
        timestamp_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        Insert_file = Perf_Base_func(self._remote._logger, self.run_log_dir)
        Insert_file.taosBenchmark_insert_summary_result(result_file_list, version="3.0")
        env_setting = self.get_component_by_name("prometheus")
        Insert_file.get_process_exporter_info(env_setting, 30, timestamp_start, timestamp_end)
        Insert_file.get_node_exporter_info(env_setting, 10, timestamp_start, timestamp_end)

    def full_col_insert(self):
        taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
        taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        self.json_filename_list = [self.file_name3]
        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop="no")
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list3, tags=self.tag_info_list, childtable_count=self.childtable_count, childtable_prefix=self.childtable_prefix, insert_rows=self.insert_rows, batch_create_tbl_num=self.batch_create_tbl_num, interlace_rows=self.interlace_rows, timestamp_step=self.timestamp_step, start_timestamp=self.start_timestamp, child_table_exists="yes")]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        json_info3 = self.tdCom.setJsoninfo(host=self.taosd_host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name3, json_info3)
        self.json_data_list = [json_info3]

        self.tdCom.put_file(self._remote, taosBenchmark_iplist, self.json_data_list, self.json_filename_list, self.run_log_dir)
        timestamp_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        result_file_list = self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, self.json_data_list, self.json_filename_list, taosBenchmark_env_setting, self.run_log_dir)
        timestamp_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        Insert_file = Perf_Base_func(self._remote._logger, self.run_log_dir)
        Insert_file.taosBenchmark_insert_summary_result(result_file_list, version="3.0")
        env_setting = self.get_component_by_name("prometheus")
        Insert_file.get_process_exporter_info(env_setting, 30, timestamp_start, timestamp_end)
        Insert_file.get_node_exporter_info(env_setting, 10, timestamp_start, timestamp_end)

    def write_log(self, msg):
        f = open(self.result_file_name, 'a')
        self.logger.info(msg)
        f.write(msg)
        f.close()

    def run(self):
        self.write_log(f'\n****************************** preparing data ******************************\n\n')
        self.prepare()
        self.write_log(f'\n\n****************************** partial inserting ******************************\n\n')
        self.partial_insert()
        self.write_log(f'\n\n****************************** full colume inserting ******************************\n\n')
        self.full_col_insert()
        print(self.result_file_name)