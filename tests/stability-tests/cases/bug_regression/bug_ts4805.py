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

class TestTs4805(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.taosd = TaosD(self._remote)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.result_file_name = ""
        self.file_name1 = "insert0.json"
        self.replica = 1
        self.vgroups = 10
        self.create_table_thread_count = 40
        self.thread_count = 40
        self.childtable_count = 10000
        self.insert_rows = 100000
        self.num_of_records_per_req = 1000
        self.dbname = "meter"
        self.stbname = "meters"
        self.insert_mode = "taosc"
        self.column_info_list = [
          {
            "type": "double",
            "count": 1,
            "name": "current"
          },
          {
            "type": "double",
            "count": 1,
            "name": "voltage"
          }
        ]
        self.tag_info_list = [
          {
            "type": "varchar",
            "len": 32,
            "name": "node_id",
            "count": 1
          }
        ]

        self.start_timestamp = self.tdCom.genTs()[0]
        self.timestep = 1

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass


    def run(self):
        self.tdSql.execute('drop stream if exists meter_h')
        self.tdSql.execute('drop stream if exists meter_d')
        self.tdSql.execute('drop database if exists meter')
        self.tdSql.execute('drop database if exists meter_h')
        self.tdSql.execute('drop database if exists meter_d')
        self.tdSql.execute('drop database if exists meter_mo')
        self.tdSql.execute('create database meter cachemodel "both" cachesize 10;')
        self.tdSql.execute('create database meter_h cachemodel "both" cachesize 10;')
        self.tdSql.execute('create database meter_d cachemodel "both" cachesize 10;')
        self.tdSql.execute('create database meter_mo cachemodel "both" cachesize 10;')
        taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
        taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        json_data_list = list()
        json_filename_list = list()
        json_filename_list.append(self.file_name1)


        for rows in [0, self.insert_rows]:
            child_table_exists = "no" if rows == 0 else "yes"
            dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop="no")
            stb_into = [self.tdCom.setStbinfo(name=self.stbname, child_table_exists=child_table_exists, columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=rows, start_timestamp=self.start_timestamp, insert_mode=self.insert_mode, timestamp_step=self.timestep)]
            print(stb_into)
            database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
            host = self.taosd_setting["fqdn"][0]
            json_info1 = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
            print(json_info1)
            print(self.run_log_dir)
            self.tdCom.genBenchmarkJson(self.run_log_dir, self.file_name1, json_info1)
            json_data_list = [json_info1]
            self.tdCom.put_file(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, self.run_log_dir)
            self.tdCom.threads_run_taosBenchmark(self._remote, taosBenchmark_iplist, json_data_list, json_filename_list, taosBenchmark_env_setting, self.run_log_dir)
            
            if rows == 0:
                self.tdSql.execute('create stream meter_h trigger max_delay 5s fill_history 1 ignore update 0 ignore expired 0 into meter_h.meters tags(node_id varchar(32), tname varchar(32)) subtable(tname) as select _wstart, _wend, avg(current) as current_avg, max(current) as current_max, min(current) as current_min from meter.meters partition by tbname tname, node_id interval(1h) fill(null);')
                self.tdSql.execute('create stream meter_d trigger at_once fill_history 1 ignore update 0 ignore expired 0 into meter_d.meters tags(node_id varchar(32), tname varchar(32)) subtable(tname) as select _wstart, _wend, avg(current_avg) as current_avg, max(current_max) as current_max, min(current_min) as current_min from meter_h.meters partition by tname, node_id interval(1d) fill(null);')
        time.sleep(300)
        self.tdSql.query("show databases")
