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

from taostest.util.file import read_yaml
from taostest.util.common import TDCom
from typing import List
from taostest import TDCase
from taostest.components.taosd import TaosD
from taostest.util.remote import Remote
from datetime import datetime,timedelta
from taostest.performance.result_reduction import Perf_Base_func
import threadpool

class CreateStreamPerftest(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.result_file_name = self.run_log_dir + '/perf_report.txt'
        self.replica = 1
        self.vgroups = 40
        self.create_table_thread_count = 40
        self.thread_count = 40
        self.childtable_count_list = [10000]
        self.vgroups_list = [40]
        self.stream_count = 60
        self.insert_rows = 0
        self.stbname = "stb"
        self.dbname = "db"
        self.stream_dbname = "stream_db"
        self.stream_sql_list = [f"select _wstart as wstart, _wend as wend, max(current) as max_current from {self.dbname}.{self.stbname} where voltage <= 220 interval (5s);",
                                f"select _wstart as wstart, _wend as wend, max(current) as max_current from {self.dbname}.{self.stbname} where voltage <= 220 partition by tbname interval (5s);"]
        

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def run(self):
        json_data_list = list()
        json_filename_list = list()
        taosBenchmark_iplist: List = self.get_fqdn("taosBenchmark")
        taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        file_name1 = "insert0.json"
        json_filename_list.append(file_name1)
        insert_rows = self.insert_rows
        
        column_info_list = [
          {
            "type": "float",
            "count": 1,
            "name": "current"
          },
          {
            "type": "int",
            "count": 1,
            "name": "voltage"
          },
          {
            "type": "float",
            "count": 1,
            "name": "phase"
          }
        ]
        tag_info_list = [
          {
            "type": "varchar",
            "len": 64,
            "count": 1,
            "name": "location"
          },
          {
            "type": "int",
            "count": 1,
            "name": "groupId"
          },
        ]
        for fill_history_value in [0, 1]:
            for stream_sql in self.stream_sql_list:
                for vgroups in self.vgroups_list:
                    for childtable_count in self.childtable_count_list:
                        f = open(self.result_file_name, 'a')
                        f.write(f'\n\n----------------------------------vgroups: {vgroups} childtable_count: {childtable_count} fill_history_value: {fill_history_value}----------------------------------\n')
                        f.write(f'\n----------------------------------stream_sql: {stream_sql}----------------------------------\n')
                        f.close()
                        Insert_file = Perf_Base_func(self.logger, self.run_log_dir)
                        start_timestamp = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S")
                        child_table_exists = "no"
                        db_drop = "yes"
                        stream_db_info = self.tdCom.setStreamDBinfo(vgroups=self.vgroups)
                        # stream_info_list = list()
                        # stream_info = self.tdCom.setStreams(stream_name=f"stream_max_test{i}", stream_stb=f'{self.stream_dbname}.output_stream_tb{i}', trigger_mode="at_once", drop="yes", source_sql=f"select _wstart as wstart, _wend as wend, max(current) as max_current from {self.dbname}.{self.stbname} where voltage <= 220 interval (5s);")
                        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=vgroups, drop=db_drop)
                        stb_into = [self.tdCom.setStbinfo(columns=column_info_list, tags=tag_info_list, childtable_count=childtable_count, insert_rows=insert_rows, start_timestamp=start_timestamp, child_table_exists=child_table_exists)]
                        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
                        host = self.get_fqdn("taosd")[0]
                        json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, stream_db=stream_db_info)
                        # json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, streams=stream_info, stream_db=stream_db_info)

                        
                        self.tdCom.genBenchmarkJson(self.run_log_dir, file_name1, json_info)
                        json_data_list.append(json_info)
                        self.tdCom.put_file(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, self.run_log_dir)
                        self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, taosBenchmark_env_setting, self.run_log_dir)
                        result_dict_list = list()
                        self.tdCom.drop_all_streams()
                        timestamp_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                        
                        
                        # 1 thread
                        for i in range(self.stream_count):
                            result_dict = dict()
                            create_start = datetime.now().timestamp()
                            self.tdCom.create_stream(stream_name=f'stream_max_test{i}', des_table=f'{self.stream_dbname}.output_stream_tb{i}', trigger_mode="at_once", fill_history_value=fill_history_value, source_sql=stream_sql)
                            create_end = datetime.now().timestamp()
                            create_use = round(create_end-create_start, 1)
                            result_dict["stream_no"] = str(i)
                            result_dict["create_use_time"] = f'{create_use}s'
                            result_dict_list.append(result_dict)
                            
                        # # n thread
                        # sql_list = list()
                        # pool = threadpool.ThreadPool(10)
                        # for i in range(self.stream_count):
                        #     stream_name=f'stream_max_test{i}'
                        #     des_table = f'{self.stream_dbname}.output_stream_tb{i}'
                        #     trigger_mode = "at_once"
                        #     source_sql = f"select _wstart as wstart, _wend as wend, max(current) as max_current from {self.dbname}.{self.stbname} where voltage <= 220 interval (5s);"
                        #     stream_sql = f'create stream if not exists {stream_name} trigger {trigger_mode} ignore expired 0  into {des_table} as {source_sql}'
                        #     sql_list.append(stream_sql)
                        # self.tdCom.thread_pool(sql_list, self.tdSql.execute, 2)
                        
                        timestamp_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                        f = open(self.result_file_name, 'a')
                        f.write(f'----------------------------------spent and usage----------------------------------\n')
                        for i in result_dict_list:
                            f.write(f'{str(i)}\n')
                        f.close()
                        env_setting = self.get_component_by_name("prometheus")
                        Insert_file.get_process_exporter_info(env_setting, 1, timestamp_start, timestamp_end)
                        Insert_file.get_node_exporter_info(env_setting, 1, timestamp_start, timestamp_end)
