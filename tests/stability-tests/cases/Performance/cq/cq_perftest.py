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
from datetime import datetime,timedelta
from taostest.performance.perfor_basic import InsertFile
from taostest.performance.result_reduction import Perf_Base_func
import time



class DnodeAddInsert(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.result_file_name = ""
        self._tmp_dir: str = os.path.join(self.run_log_dir, "tmp")
        self.replica = 1
        self.vgroups = 40
        self.create_table_thread_count= 40
        self.thread_count= 40
        self.childtable_count = 1
        self.insert_rows = 1000000
        self.stbname = "stb"
        self.dbname = "db"
        self.timestamp_step = 1
        self.timeout = 300
        self.sleep_time = 1
        # self.stream_dbname = "stream_db"
        self.stream_tbname = "stream_tb"
        self.interval = 10
        self.query_sql = f"select avg(c1) from {self.dbname}.{self.stbname} interval({self.interval}a)"

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
        childtable_count = self.childtable_count
        insert_rows = self.insert_rows

        column_info_list = [
          {
            "type": "INT",
            "count": 2
          }
        ]
        tag_info_list = [
          {
            "type": "INT",
            "count": 1
          }
        ]
        self.tdCom.createDb(self.dbname)
        self.tdCom.create_stable(dbname=self.dbname, stbname=self.stbname, column_elm_list=column_info_list, tag_elm_list=tag_info_list)
        # self.tdSql.execute(f'create table {self.dbname}.{self.stream_tbname} as {self.query_sql}')
        Insert_file = Perf_Base_func(self.logger, self.run_log_dir)
        start_timestamp = (datetime.now() + timedelta(days=0)).strftime("%Y-%m-%d %H:%M:%S")
        child_table_exists = "no"
        db_drop = "no"
        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop=db_drop)
        stb_into = [self.tdCom.setStbinfo(columns=column_info_list, tags=tag_info_list, childtable_count=childtable_count, insert_rows=insert_rows, start_timestamp=start_timestamp, child_table_exists=child_table_exists, name=self.stbname, timestamp_step=self.timestamp_step)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count)

        self.tdCom.genBenchmarkJson(self.run_log_dir, file_name1, json_info)
        json_data_list.append(json_info)
        self.tdCom.put_file(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, self.run_log_dir)
        self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, taosBenchmark_env_setting, self.run_log_dir)
        self.tdSql.query(self.query_sql)
        expected_row = self.tdSql.query_row
        self.tdSql.query(self.query_sql)
        expected_data = self.tdSql.query_data
        time.sleep(5)
        timestamp_start = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        self.tdSql.execute(f'create table {self.dbname}.{self.stream_tbname} as {self.query_sql}')
        time.sleep(5)
        delay = 0
        # time.sleep(self.interval+1)
        self.tdSql.query(f'select count(*) from {self.dbname}.{self.stream_tbname}')
        counter = 0
        while len(self.tdSql.query_data) == 0:
            time.sleep(1)
            counter += 1
            if counter > self.timeout:
                return
            self.tdSql.query(f'select count(*) from {self.dbname}.{self.stream_tbname}')
        res = self.tdSql.query_data[0][0]
        while int(res) != int(expected_row):
            print(res)
            time.sleep(self.sleep_time)
            self.tdSql.query(f'select count(*) from {self.dbname}.{self.stream_tbname}')
            res = self.tdSql.query_data[0][0]
            delay += self.sleep_time
            if delay > self.timeout:
                return
        self.tdSql.query(f'select * from {self.dbname}.{self.stream_tbname}')
        res = self.tdSql.query_data
        delay1 = 0
        while res != expected_data:
            time.sleep(self.sleep_time)
            self.tdSql.query(f'select * from {self.dbname}.{self.stream_tbname}')
            res = self.tdSql.query_data
            delay1 += self.sleep_time
            if delay1 > self.timeout:
                return
        # self._remote._logger.info(f'************ cq finish in {delay+delay1-self.interval-1}s ************')
        self._remote._logger.info(f'************ cq finish in {delay+delay1}s ************')
        timestamp_end = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        self._remote.cmd(host, ['cp -r /tmp/0.log /tmp/0_0.log'])
        env_setting = self.get_component_by_name("prometheus")
        Insert_file.get_process_exporter_info(env_setting, 1, timestamp_start, timestamp_end)
        Insert_file.get_node_exporter_info(env_setting, 1, timestamp_start, timestamp_end)
