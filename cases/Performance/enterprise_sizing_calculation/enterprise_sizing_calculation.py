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
from taostest.util.file import read_yaml
from taostest.util.common import TDCom
from typing import List
from taostest import TDCase
from taostest.components.taosd import TaosD
from taostest.util.remote import Remote
from datetime import datetime
from taostest.performance.result_reduction import Perf_Base_func
from apscheduler.schedulers.background import BackgroundScheduler
import time

class EnterpriseSizingCalculation(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.result_file_name = f'{self.run_log_dir}/perf_report.txt'
        self.file_name1 = "insert0.json"
        self.file_name2 = "insert1.json"
        self.file_name3 = "insert2.json"
        self._tmp_dir: str = os.path.join(self.run_log_dir, "tmp")
        self.create_table_thread_count = 50
        self.thread_count = 50
        self.childtable_count_list = [500000, 1000000, 2000000]
        self.insert_rows_list = [100, 50, 25]
        self.query_time_list = [100, 70, 40]
        # self.childtable_count_list = [2000000]
        # self.insert_rows_list = [25]
        # self.query_time_list = [70]
        self.num_of_records_per_req = 10000
        self.dbname = "test"
        self.stbname = "meters"
        self.insert_interval1 = 30000
        self.insert_interval2 = 900000
        self.ms_offset = 1000
        self.vgroup_per_tables = 50000
        # self.insert_interval_list = [self.insert_interval1, self.insert_interval2]
        self.insert_interval_list = [self.insert_interval1]
        self.interlace_rows = 1
        self.insert_mode = "sml"
        self.line_protocol = "line"
        self.query_interval = 20
        self.query_times = 100
        self.query_threads = 1
        self.query = True
        self.taosBenchmark_step_sleep = 1

    def kill_query_taosBenchmark(self, insert_host, query_host):
        insert_taosBenchmark_count = self._remote.cmd(insert_host, [f'ps -ef | grep taosBenchmark | grep -v grep | grep -v SCREEN | wc -l'])
        if int(insert_taosBenchmark_count) == 0:
            self._remote.cmd(query_host, [f'ps -ef | grep taosBenchmark | grep -v grep | grep -v SCREEN | awk \'{{print $2}}\' | xargs kill -9'])

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def run(self):
        taosBenchmark_iplist: List = self.get_fqdn("taosBenchmark")
        insert_host = taosBenchmark_iplist[0]
        if len(taosBenchmark_iplist) > 1:
            query_host = taosBenchmark_iplist[1]
        else:
            query_host = ""

        taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        
        column_info_list = [
                    {"type": "FLOAT", "name": "current", "count": 1},
                    {"type": "INT", "name": "voltage"},
                    {"type": "FLOAT", "name": "phase"}
        ]
        tag_info_list = [
                    {"type": "TINYINT", "name": "groupid"},
                    {"name": "location", "type": "BINARY", "len": 16}
        ]
        for insert_interval in self.insert_interval_list:
            for i in range(len(self.childtable_count_list)):
                self.tdSql.execute(f'drop database if exists {self.dbname}')
                self.tdSql.execute(f'create database if not exists {self.dbname} vgroups {int(self.childtable_count_list[i]/self.vgroup_per_tables)}')
                json_data_list = list()
                json_filename_list = list()
                json_filename_list.append(self.file_name1)
                dbinfo = self.tdCom.setDBinfo(name=self.dbname, vgroups=int(self.childtable_count_list[i]/self.vgroup_per_tables), drop="no")
                if len(taosBenchmark_iplist) == 3:
                    stb_into = [self.tdCom.setStbinfo(name=self.stbname, columns=column_info_list, tags=tag_info_list, childtable_count=int(self.childtable_count_list[i]/2), insert_rows=self.insert_rows_list[i], interlace_rows=self.interlace_rows, insert_mode=self.insert_mode, line_protocol=self.line_protocol, insert_interval=insert_interval)]
                elif len(taosBenchmark_iplist) == 2 and self.query == False:
                    stb_into = [self.tdCom.setStbinfo(name=self.stbname, columns=column_info_list, tags=tag_info_list, childtable_count=int(self.childtable_count_list[i]/2), insert_rows=self.insert_rows_list[i], interlace_rows=self.interlace_rows, insert_mode=self.insert_mode, line_protocol=self.line_protocol, insert_interval=insert_interval)]
                else:
                    stb_into = [self.tdCom.setStbinfo(name=self.stbname, columns=column_info_list, tags=tag_info_list, childtable_count=self.childtable_count_list[i], insert_rows=self.insert_rows_list[i], interlace_rows=self.interlace_rows, insert_mode=self.insert_mode, line_protocol=self.line_protocol, insert_interval=insert_interval)]

                database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
                host = self.get_fqdn("taosd")[0]
                if len(taosBenchmark_iplist) == 2 and self.query == False:
                    json_info1 = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
                else:
                    json_info1 = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
                self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name1, json_info1)
                json_data_list.append(json_info1)

                
                query_sql_list = [
                    {
                        "sql": "select count(*) from meters",
                        "result": "./query_res0.txt"
                    },
                    {
                        "sql": "select count(*) from meters where voltage > 10",
                        "result": "./query_res1.txt"
                    },
                    {
                        "sql": "select avg(current), max(voltage), min(phase) from meters",
                        "result": "./query_res2.txt"
                    },
                    {
                        "sql": "select avg(current), max(voltage), min(phase) from meters interval(10s)",
                        "result": "./query_res3.txt"
                    },
                    {
                        "sql": "select last_row(*) from meters",
                        "result": "./query_res5.txt"
                    },
                    {
                        "sql": "select last(*) from meters",
                        "result": "./query_res6.txt"
                    },
                    {
                        "sql": "select avg(current), max(voltage), min(phase) from meters group by tbname limit 100",
                        "result": "./query_res4.txt"
                    }
                ]
                if len(taosBenchmark_iplist) == 2 and self.query == True:
                    json_filename_list.append(self.file_name2)
                    specified_table_query_dict = self.tdCom.set_specified_table_query(query_interval=int(self.insert_interval1/self.ms_offset), concurrent=self.query_threads, sqls=query_sql_list)
                    json_info2 = self.tdCom.setQueryJsoninfo(host=host, database=self.dbname, query_times=self.query_time_list[i], specified_table_query=specified_table_query_dict)
                    self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name2, json_info2)
                    json_data_list.append(json_info2)

                elif len(taosBenchmark_iplist) == 2 and self.query == False:
                    json_filename_list.append(self.file_name2)
                    dbinfo = self.tdCom.setDBinfo(name=self.dbname, vgroups=int(self.childtable_count_list[i]/self.vgroup_per_tables), drop="no")
                    stb_into = [self.tdCom.setStbinfo(name=self.stbname, columns=column_info_list, tags=tag_info_list, childtable_count=int(self.childtable_count_list[i]/2), insert_rows=self.insert_rows_list[i], interlace_rows=self.interlace_rows, insert_mode=self.insert_mode, line_protocol=self.line_protocol, insert_interval=insert_interval)]
                    database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
                    host = self.get_fqdn("taosd")[0]
                    json_info2 = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=int(self.create_table_thread_count), thread_count=int(self.thread_count), num_of_records_per_req=self.num_of_records_per_req)
                    self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name2, json_info2)
                    json_data_list.append(json_info2)

                elif len(taosBenchmark_iplist) == 3:
                    json_filename_list.append(self.file_name2)
                    dbinfo = self.tdCom.setDBinfo(name=self.dbname, vgroups=int(self.childtable_count_list[i]/self.vgroup_per_tables), drop="no")
                    stb_into = [self.tdCom.setStbinfo(name=self.stbname, columns=column_info_list, tags=tag_info_list, childtable_count=int(self.childtable_count_list[i]/2), insert_rows=self.insert_rows_list[i], interlace_rows=self.interlace_rows, insert_mode=self.insert_mode, line_protocol=self.line_protocol, insert_interval=insert_interval)]
                    database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
                    host = self.get_fqdn("taosd")[0]
                    json_info2 = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=int(self.create_table_thread_count), thread_count=int(self.thread_count), num_of_records_per_req=self.num_of_records_per_req)
                    self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name2, json_info2)
                    json_data_list.append(json_info2)
                    
                    json_filename_list.append(self.file_name3)
                    specified_table_query_dict = self.tdCom.set_specified_table_query(query_interval=int(self.insert_interval1/self.ms_offset), concurrent=self.query_threads, sqls=query_sql_list)
                    json_info3 = self.tdCom.setQueryJsoninfo(host=host, database=self.dbname, query_times=self.query_time_list[i], specified_table_query=specified_table_query_dict)
                    self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name3, json_info3)
                    json_data_list.append(json_info3)

                self.tdCom.put_file(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, self.run_log_dir)
                if len(taosBenchmark_iplist) > 1 and self.query:
                    scheduler = BackgroundScheduler()
                    scheduler.add_job(self.kill_query_taosBenchmark, 'interval', seconds=self.query_interval, max_instances=1, args=[insert_host, query_host])
                    scheduler.start()
                Insert_file = Perf_Base_func(self.logger, self.run_log_dir)
                timestamp_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

                result_filename = self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, taosBenchmark_env_setting, self.run_log_dir, sleep_time=self.taosBenchmark_step_sleep)
                timestamp_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                if len(taosBenchmark_iplist) == 3 and self.query:
                    Insert_file.taosBenchmark_insert_summary_result([result_filename[0], result_filename[1]], version="3.0")
                    Insert_file.taosBenchmark_id_insert_result([result_filename[0], result_filename[1]])
                elif len(taosBenchmark_iplist) == 2 and not self.query:
                    Insert_file.taosBenchmark_insert_summary_result([result_filename[0], result_filename[1]], version="3.0")
                    Insert_file.taosBenchmark_id_insert_result([result_filename[0], result_filename[1]])
                else:
                    Insert_file.taosBenchmark_insert_summary_result([result_filename[0]], version="3.0")
                    Insert_file.taosBenchmark_id_insert_result([result_filename[0]])

                # get node_info and process_info
                env_setting = self.get_component_by_name("prometheus")
                Insert_file.get_process_exporter_info(env_setting, 1, timestamp_start, timestamp_end)
                Insert_file.get_node_exporter_info(env_setting, 1, timestamp_start, timestamp_end)
                if len(taosBenchmark_iplist) > 1 and self.query:
                    time.sleep(100)
        print(self.result_file_name)

        # query
        # json_data_list = list()
        # json_filename_list = list()
        # json_filename_list.append(self.file_name2)
        # query_sql_list = [
        #     {
        #         "sql": "select count(*) from meters",
        #         "result": "./query_res0.txt"
        #     },
        #     {
        #         "sql": "select last(*) from meters",
        #         "result": "./query_res1.txt"
        #     }
       
        # ] 
        # specified_table_query_dict = self.tdCom.set_specified_table_query(sqls=query_sql_list)
        # host = self.get_fqdn("taosd")[0]
        # json_info2 = self.tdCom.setQueryJsoninfo(host=host, database=self.dbname, specified_table_query=specified_table_query_dict)
        # self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name2, json_info2)
        # json_data_list.append(json_info2)
        # self.tdCom.put_file(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, self.run_log_dir)

        # Insert_file = Perf_Base_func(self.logger, self.run_log_dir)
        # timestamp_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

        # result_filename = self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, taosBenchmark_env_setting, self.run_log_dir)

        # json_data_list = list()
        # json_filename_list = list()
        # json_filename_list.append(self.file_name2)
        # dbinfo = self.tdCom.setDBinfo(replica=self.replica, vgroups=self.vgroups, drop="no")
        # start_timestamp = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d %H:%M:%S")
        # stb_into = [self.tdCom.setStbinfo(columns=column_info_list, tags=tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, child_table_exists="yes", start_timestamp=start_timestamp)]
        # database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        # host = self.get_fqdn("taosd")[0]
        # json_info2 = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        # self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name2, json_info2)
        # json_data_list.append(json_info2)
        # self.tdCom.put_file(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, self.run_log_dir)

        # # self.tdCom.add_back_ground_scheduler(self.tdCom.multi_thread_query, 'interval', seconds=self.query_interval, max_instances=10, args=[f'{self.dbname}.{self.stbname}', None, 10])
        # thread_list.append(threading.Thread(target=self.tdCom.threads_run_taosBenchmark, args=(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, taosBenchmark_env_setting, self.run_log_dir)))

        # if self.replica == 3:
        #   thread_list.append(threading.Thread(target=self.kill_a_dnode, args=()))

        # for t in thread_list:
        #   t.start()
        # for t in thread_list:
        #   t.join()
        # if self.exitcode == 1:
        #   self.tdSql.checkEqual(self.exitcode, 0)
              

        # jfile = InsertFile()
        # Insert_file = Perf_Base_func(self.logger, self.run_log_dir)
        # timestamp_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        # # # run taosBenchmark
        # taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        # result_filename = Insert_file.threads_run_taosBenchmark(
        #     taosBenchmark_iplist, json_data, file_name, taosBenchmark_env_setting
        # )

        # timestamp_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        # # get insert result
        # # Insert_file.full_create_tb_result(result_filename)
        # Insert_file.taosBenchmark_insert_summary_result(
        #     result_filename, version="3.0"
        # )
        # Insert_file.taosBenchmark_id_insert_result(result_filename)

        # # get node_info and process_info
        # env_setting = self.get_component_by_name("prometheus")
        # Insert_file.get_process_exporter_info(env_setting, 1, timestamp_start, timestamp_end)
        # Insert_file.get_node_exporter_info(env_setting, 1, timestamp_start, timestamp_end)
        # print(self.result_file_name)
