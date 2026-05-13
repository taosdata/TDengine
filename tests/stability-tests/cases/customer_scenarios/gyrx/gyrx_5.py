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
import datetime
from taostest import TDCase
from taostest.performance.result_reduction import Perf_Base_func
from taostest.util.remote import Remote
import random


class Demo(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.perf = Perf_Base_func(self.logger, self.run_log_dir)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.prom_env_setting = self.get_component_by_name("prometheus")
        # self.Prometheus = PrometheusServer(self._remote)
        self.json_filename = "test.json"
        self.replica = int(os.environ["DATABASE_REPLICAS"]) if "DATABASE_REPLICAS" in os.environ else 1
        self.start_timestamp = self.tdCom.genTodayZeroTs()
        self.create_table_thread_count = 40
        self.childtable_count = 10000
        self.insert_rows = 1000
        self.default_interval = 5
        self.range_count = 10
        self.precision = "ms"
        self.pk_test = False
        self.pk_dict_list = [{"pname": "pk", "ptype": "bigint"}, {"pname": "pk", "ptype": "int"}]
        self.pk_dict = random.choice(self.pk_dict_list) if self.pk_test else None
        self.stt_trigger = 8
        self.stbname = "stb"
        self.ctbname = "ctb"
        self.dbname = "test"
        self.child_table_exists = "no"
        self.db_drop = "yes"
        self.keep_trying = -1
        self.trying_interval = 10
        self.vgroups = 10
        self.host = self.get_fqdn("taosd")[0]
        self.thread_count = 40
        self.num_of_records_per_req = 1000
        self.interlace_rows = 0
        self.full_type_list = ["tinyint", "smallint", "int", "bigint", "tinyint unsigned", "smallint unsigned", "int unsigned", "bigint unsigned", "float", "double", "binary", "nchar", "bool"]
        self.offset = 1000
        self.date_time = self.tdCom.genTs(precision=self.precision)[0]
        self.date_time = int(datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0).timestamp()*self.offset)
        self.env_root = os.path.join(os.environ["TEST_ROOT"], "env")
        self.json_file_name = "test.json"
        self.json_file = os.path.join(self.env_root, f"pocs/gyrx/{self.json_file_name}")
        self.run_test_log_dir = "/root/testlog/"
        self.json_log ={}

        self.column_info_list = [
            {
              "type": "BIGINT",
              "count": 1,
              "gen": "order",
              "fillNull": "false"
            },
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
        self.json_file_name = "test.json"
        self.json_data_list = list()
        self.taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
        self.taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")

    def execute_sql(self,sql):
        self.tdSql.execute(sql)
        self.json_log[sql] = '执行成功'

    def query_sql(self,sql,expected_count,expected_res=None):
        self.tdSql.query(sql)
        self.json_log[sql] = self.tdSql.query_data
        
    def insert_with_python_connector(self):
        # Using pre-packaged functions
        self.tdCom.createDb(dbname=self.dbname)
        self.tdCom.create_stable(dbname=self.dbname, stbname=self.stbname, pk_dict=self.pk_dict)
        self.tdCom.create_ctable(dbname=self.dbname, stbname=self.stbname, ctbname=self.ctbname)
        for i in range(self.range_count):
            ts_value = str(self.date_time)+f'-{self.default_interval*(i+1)}s'
            self.tdCom.insert_rows(tbname=self.ctbname, ts_value=ts_value, pk_dict=self.pk_dict)
        # Custom Write
        self.tdSql.execute(f'insert into {self.dbname}.{self.ctbname} (ts, c1) values (now, 1)')

    def query(self, sql, expected_count, expected_res=None):
        self.tdSql.query(sql)
        self.tdSql.checkEqual(self.tdSql.query_row, expected_count)
        if expected_res is not None:
            self.tdSql.checkEqual(self.tdSql.query_data[0][0], expected_res)

    def insert_with_taosBenchmark(self):
        json_filename_list = [self.json_file_name]
        dbinfo = self.tdCom.setDBinfo(name=self.dbname, replica=self.replica, vgroups=self.vgroups, drop=self.db_drop, stt_trigger=self.stt_trigger)
        stb_into = [self.tdCom.setStbinfo(columns=self.column_info_list, tags=self.tag_info_list, childtable_count=self.childtable_count, insert_rows=self.insert_rows, start_timestamp=self.start_timestamp, child_table_exists=self.child_table_exists, name=self.stbname, keep_trying=self.keep_trying, trying_interval=self.trying_interval, interlace_rows=self.interlace_rows)]
        database_info = [self.tdCom.setDatabases(dbinfo=dbinfo, super_tables=stb_into)]
        host = self.get_fqdn("taosd")[0]
        json_info = self.tdCom.setJsoninfo(host=host, databases=database_info, create_table_thread_count=self.create_table_thread_count, thread_count=self.thread_count, num_of_records_per_req=self.num_of_records_per_req)
        print(json_info)
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name, json_info)
        self.json_data_list = [json_info]
        self.tdCom.put_file(self._remote, self.taosBenchmark_iplist, self.json_data_list, json_filename_list, self.run_log_dir)
        self.result_filename = self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, self.json_data_list, json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)
        self.tdSql.execute(f'flush database {self.dbname}')

    def insert_with_load_json(self):
        json_filename_list = [self.json_file_name]
        json_info = self.tdCom.load_json(self.json_file)
        json_info["test_log"] = self.run_test_log_dir
        self.tdCom.dump_json(f'{self.run_log_dir}/{self.json_file_name}', json_info)
        self.json_data_list = [json_info]
        
        self.tdCom.put_file(self._remote, self.taosBenchmark_iplist, self.json_data_list, json_filename_list, self.run_log_dir)

        self.result_filename = self.tdCom.threads_run_taosBenchmark(
            self._remote,
            self.taosBenchmark_iplist,
            self.json_data_list,
            json_filename_list,
            self.taosBenchmark_env_setting,
            self.run_log_dir
        )
        self.tdSql.execute(f'flush database {self.dbname}')

    def save_csv(self,sql,file_name):
        cmd = f"taos -s '{sql} >> {self.run_log_dir}/{file_name}.csv'"
        os.system(cmd)
        self.json_log[cmd] = f"输出到文件: {self.run_test_log_dir}/{file_name}.csv"

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def run(self):


        # taosBenchmark insert
        result_file_name = self.run_log_dir + '/perf_report.txt'
        timestamp_start = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

        # taosBenchmark insert with loaded_json
        self.insert_with_load_json()
        # # taosBenchmark insert with custom parameters in case
        # self.insert_with_taosBenchmark()
        timestamp_end = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        
        self.save_csv(f"select * from {self.dbname}.{self.stbname}", self.stbname)
        self.perf.taosBenchmark_insert_summary_result(self.result_filename, version="3.0")
        self.perf.get_process_exporter_info(self.prom_env_setting, 1, timestamp_start, timestamp_end)
        self.perf.get_node_exporter_info(self.prom_env_setting, 1, timestamp_start, timestamp_end)
        # 保存json_log到文件
        self.tdCom.dump_json(f'{self.run_log_dir}/json_log.json', self.json_log)
        print(result_file_name)
