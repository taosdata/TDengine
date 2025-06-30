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
import json
import yaml


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
        self.json_file = os.path.join(self.env_root, "pocs/gyrx/test.json")
        self.query_sql_file = os.path.join(self.env_root, "pocs/gyrx/query.yaml")
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

    def convert_to_json_serializable(self, data):
        """将数据转换为JSON可序列化的格式"""
        if isinstance(data, datetime.datetime):
            return data.strftime('%Y-%m-%d %H:%M:%S.%f')
        elif isinstance(data, list):
            return [self.convert_to_json_serializable(item) for item in data]
        elif isinstance(data, tuple):
            return tuple(self.convert_to_json_serializable(item) for item in data)
        else:
            return data

    def load_yaml(self, yaml_file):
        """加载YAML文件"""
        try:
            with open(yaml_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            self.logger.error(f"YAML文件不存在: {yaml_file}")
            return {}
        except yaml.YAMLError as e:
            self.logger.error(f"YAML文件解析错误: {e}")
            return {}

    def execute_sql(self,sql):
        self.tdSql.execute(sql)
        self.json_log[sql] = '执行成功'

    def query_sql(self,sql,expected_count,expected_res=None,db_name=None):
        if db_name is None:
            db_name = self.dbname
        self.tdSql.execute(f'use {db_name}')
        self.tdSql.query(sql)
        print(self.tdSql.query_data)
        # 转换查询结果为JSON可序列化格式
        serializable_data = self.convert_to_json_serializable(self.tdSql.query_data)
        self.json_log[sql] = serializable_data

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
        self.tdCom.genBenchmarkJson(self.run_log_dir, self.json_file_name, json_info)
        self.json_data_list = [json_info]
        self.tdCom.put_file(self._remote, self.taosBenchmark_iplist, self.json_data_list, json_filename_list, self.run_log_dir)
        self.result_filename = self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, self.json_data_list, json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)
        self.tdSql.execute(f'flush database {self.dbname}')

    def insert_with_load_json(self):
        json_filename_list = [self.json_file_name]
        json_info = self.tdCom.load_json(self.json_file)
        
        self.json_data_list = [json_info]
        print(
            "远程主机:", self._remote,
            "\ntaosBenchmark IP列表:", self.taosBenchmark_iplist,
            "\nJSON数据列表:", self.json_data_list,
            "\nJSON文件列表:", json_filename_list,
            "\ntaosBenchmark环境设置:", self.taosBenchmark_env_setting,
            "\n运行日志目录:", self.run_log_dir
        )
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


    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def run(self):
      

        config = self.load_yaml(self.query_sql_file)
        if 'database_config' in config:
            db_config = config['database_config']
            db_name = db_config.get('db', 'test')
            sql_list = db_config.get('sql_list', [])
            
            self.logger.info(f"正在执行数据库 '{db_name}' 的查询...")
            for sql in sql_list:
                self.query_sql(sql, 1, db_name=db_name)
        else:
            self.logger.error("YAML配置文件格式错误，缺少database_config节点")
        self.tdCom.dump_json(f'{self.run_log_dir}/json_log.json', self.json_log)
        print(self.json_log)
