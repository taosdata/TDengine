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
        self.json_file_name = "case_13.json"
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
        
        self.json_data_list = list()
        self.taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
        self.taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")

    def execute_sql(self,sql):
        self.tdSql.execute(sql)
        self.json_log[sql] = '执行成功'

    def query_sql(self,sql,expected_count,expected_res=None,db_name=None):
        if db_name is None:
            db_name = self.dbname
        self.tdSql.execute(f'use {db_name}')
        self.tdSql.query(sql)

        self.tdSql.checkEqual(self.tdSql.query_row, expected_count)
        
        # 记录查询结果的详细信息
        query_result = {
            "row_count": self.tdSql.query_row,
            "column_count": len(self.tdSql.query_data[0]) if self.tdSql.query_data and len(self.tdSql.query_data) > 0 else 0,
            "query_data": self.tdSql.query_data
        }
        self.json_log[sql] = query_result
        
        # if expected_res is not None:
        #     self.tdSql.checkEqual(self.tdSql.query_data[0][0], expected_res)
        
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
        
        # 查询表结构并格式化显示
        desc_sql = "desc functiontest.usecase13"
        self.query_sql(desc_sql, 4096)
        
        # 为表结构添加格式化信息
        if desc_sql in self.json_log and "query_data" in self.json_log[desc_sql]:
            desc_data = self.json_log[desc_sql]["query_data"]
            formatted_desc = {
                "table_name": "functiontest.usecase13",
                "total_columns": len(desc_data),
                "columns": []
            }
            
            for row in desc_data:
                if len(row) >= 4:  # 确保有足够的列
                    column_info = {
                        "field": row[0],
                        "type": row[1], 
                        "length": row[2],
                        "note": row[3] if len(row) > 3 else ""
                    }
                    formatted_desc["columns"].append(column_info)
            
            # 更新json_log中的表结构信息
            self.json_log[f"{desc_sql}_formatted"] = formatted_desc
        # 保存json_log到文件
        json_log_path = f'{self.run_log_dir}/json_log.json'
        self.tdCom.dump_json(json_log_path, self.json_log)
        print(result_file_name)
        
        # 打印JSON路径和内容
        print(f"\n=== JSON 文件路径信息 ===")
        print(f"json_log 路径: {json_log_path}")
        print(f"原始配置文件路径: {self.json_file}")
        print(f"生成的benchmark配置路径: {self.run_log_dir}/{self.json_file_name}")
        
        print(f"\n=== JSON 内容 ===")
        print(f"json_log 内容:")
        try:
            import json
            with open(json_log_path, 'r', encoding='utf-8') as f:
                json_log_content = json.load(f)
            print(json.dumps(json_log_content, indent=2, ensure_ascii=False))
            
            # 格式化显示表结构（如果存在desc查询）
            desc_key = None
            formatted_key = None
            for key in json_log_content.keys():
                if key.startswith("desc "):
                    desc_key = key
                    formatted_key = f"{key}_formatted"
                    break
            
            if desc_key and formatted_key in json_log_content:
                print(f"\n=== 表结构详情 (格式化显示) ===")
                formatted_data = json_log_content[formatted_key]
                print(f"表名: {formatted_data['table_name']}")
                print(f"总列数: {formatted_data['total_columns']}")
                print()
                
                # 表头 - 仿照原始输出格式
                print(f"{'行号':<6} | {'字段名':<30} | {'类型':<22} | {'长度':<11} | {'备注':<18} |")
                print("-" * 100)
                
                # 表数据 - 仿照原始输出格式
                for idx, col in enumerate(formatted_data['columns'], 1):
                    field_name = col.get('field', '')
                    field_type = col.get('type', '')
                    field_length = str(col.get('length', ''))
                    field_note = col.get('note', '')
                    
                    print(f"{idx:<6} | {field_name:<30} | {field_type:<22} | {field_length:<11} | {field_note:<18} |")
                
        except Exception as e:
            print(f"读取 json_log 失败: {e}")
            print(f"json_log 变量内容: {self.json_log}")
        
