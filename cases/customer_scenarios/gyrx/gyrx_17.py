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
import shutil
import copy
import re
from taostest.util.common import TDCom
import datetime
from taostest import TDCase
from taostest.performance.result_reduction import Perf_Base_func
from taostest.util.remote import Remote
import random
from taostest.util.playwright_util import PlaywrightUtil
from taostest.util.jmeter_util import JMeterUtil
import yaml


class Demo(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.perf = Perf_Base_func(self._remote._logger, self.run_log_dir)
        self.taosd_setting = self.tdCom.get_components_setting(
            self.env_setting["settings"], "taosd"
        )
        self.prom_env_setting = self.get_component_by_name("prometheus")
        # self.Prometheus = PrometheusServer(self._remote)
        self.json_filename = "insert0.json"
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
        self.json_file = os.path.join(self.env_root, "pocs/gyrx/case_17.json")
        self.run_test_log_dir = "/root/testlog/"
        self.playwright = PlaywrightUtil(self.envMgr)

        # Initialize JMeter if configured
        self.jmeter_setting = None
        self.jmeter_util = None
        try:
            self.jmeter_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "jmeter")
            if self.jmeter_setting:
                self.jmeter_util = JMeterUtil(self.envMgr)
                self.logger.info("JMeter component found and initialized")
        except Exception as e:
            self.logger.info(f"JMeter component not configured or failed to initialize: {e}")
            self.jmeter_setting = None
            self.jmeter_util = None

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
        self.json_file_name = "insert0.json"
        self.json_data_list = list()
        self.taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
        self.taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        self.env_root = os.path.join(os.environ["TEST_ROOT"], "env")
        
        
        # 加载查询配置文件
        self.query_config_file = os.path.join(self.env_root, "pocs/gyrx/query_config.yaml")
        self.query_config = self.load_query_config()
        
        # 数据库名称将在insert_with_load_json后设置
        self.actual_db_name = None
        
        # JMeter环境设置状态标志
        self.jmeter_setup_done = False

    def get_current_database_name(self):
        """获取当前应该使用的数据库名称"""
        return self.actual_db_name if self.actual_db_name else self.dbname

    def ensure_database_connection(self):
        """确保连接到正确的数据库"""
        db_name = self.get_current_database_name()
        try:
            self.logger.info(f"切换到数据库: {db_name}")
            self.tdSql.execute(f"use {db_name}")
            return True
        except Exception as e:
            self.logger.error(f"切换数据库失败: {db_name}, 错误: {e}")
            return False

    def smart_process_sql(self, sql_query):
        """智能处理SQL查询，自动添加数据库名称前缀"""
        db_name = self.get_current_database_name()
        if not db_name:
            self.logger.warning("无法获取数据库名称，返回原始SQL")
            return sql_query
        
        # 将SQL转换为小写进行分析，但保持原始大小写
        sql_lower = sql_query.lower().strip()
        
        # 检查是否是查询语句
        if not (sql_lower.startswith('select') or sql_lower.startswith('with')):
            self.logger.info("非查询语句，不进行表名处理")
            return sql_query
        
        # 改进的表名匹配策略
        # 1. 首先找到所有可能的表名位置和表名
        # 2. 然后统一替换，避免重复替换导致的问题
        
        processed_sql = sql_query
        tables_found = set()
        
        # 更全面的表名匹配模式
        # 匹配FROM、JOIN后的表名，以及逗号分隔的表名
        table_patterns = [
            r'\bfrom\s+([a-zA-Z_][a-zA-Z0-9_]*)',  # FROM table_name
            r'\bjoin\s+([a-zA-Z_][a-zA-Z0-9_]*)',  # JOIN table_name  
            r'\binner\s+join\s+([a-zA-Z_][a-zA-Z0-9_]*)',  # INNER JOIN table_name
            r'\bleft\s+join\s+([a-zA-Z_][a-zA-Z0-9_]*)',   # LEFT JOIN table_name
            r'\bright\s+join\s+([a-zA-Z_][a-zA-Z0-9_]*)',  # RIGHT JOIN table_name
            r'\bfull\s+join\s+([a-zA-Z_][a-zA-Z0-9_]*)',   # FULL JOIN table_name
            r',\s*([a-zA-Z_][a-zA-Z0-9_]*)',  # , table_name (用于多表查询)
        ]
        
        # 收集所有需要处理的表名
        all_table_names = set()
        
        for pattern in table_patterns:
            matches = re.finditer(pattern, sql_query, re.IGNORECASE)
            for match in matches:
                table_name = match.group(1)
                # 检查表名是否已经包含数据库前缀
                if '.' not in table_name:
                    all_table_names.add(table_name)
        
        # 对于 FROM table1, table2, table3 这种格式的特殊处理
        # 匹配 FROM 后跟多个用逗号分隔的表名
        from_multi_pattern = r'\bfrom\s+([a-zA-Z_][a-zA-Z0-9_]*(?:\s*,\s*[a-zA-Z_][a-zA-Z0-9_]*)*)'
        from_match = re.search(from_multi_pattern, sql_query, re.IGNORECASE)
        if from_match:
            from_clause = from_match.group(1)
            # 提取逗号分隔的表名
            table_list = re.findall(r'([a-zA-Z_][a-zA-Z0-9_]*)', from_clause)
            for table_name in table_list:
                if '.' not in table_name:
                    all_table_names.add(table_name)
        
        # 现在统一处理所有找到的表名
        for table_name in all_table_names:
            # 检查表名是否已经包含数据库前缀
            if '.' not in table_name:
                # 表名没有数据库前缀，需要添加
                full_table_name = f"{db_name}.{table_name}"
                
                # 使用更精确的替换策略
                # 匹配表名，但确保不在数据库前缀之后（避免重复替换）
                table_pattern = r'(?<!\.)\b' + re.escape(table_name) + r'\b'
                
                # 检查是否真的需要替换（避免已经有前缀的情况）
                if re.search(table_pattern, processed_sql):
                    processed_sql = re.sub(table_pattern, full_table_name, processed_sql)
                    tables_found.add(table_name)
                    self.logger.info(f"为表 '{table_name}' 添加数据库前缀: {full_table_name}")
            else:
                self.logger.info(f"表 '{table_name}' 已包含数据库前缀，跳过处理")
        
        if tables_found:
            self.logger.info(f"SQL处理完成，共处理了 {len(tables_found)} 个表: {', '.join(sorted(tables_found))}")
            self.logger.info(f"原始SQL: {sql_query}")
            self.logger.info(f"处理后SQL: {processed_sql}")
        else:
            self.logger.info("SQL中的表名已包含数据库前缀或未找到需要处理的表名")
        
        return processed_sql

    def ensure_jmeter_setup(self):
        """确保JMeter环境已设置，避免重复设置"""
        if not self.jmeter_util or not self.jmeter_setting:
            return False
            
        if self.jmeter_setup_done:
            self.logger.debug("JMeter environment already set up, skipping")
            return True
            
        self.logger.info("Setting up JMeter environment (first time)")
        if self.jmeter_util.setup_jmeter(self.jmeter_setting):
            self.jmeter_setup_done = True
            self.logger.info("JMeter environment setup completed")
            return True
        else:
            self.logger.error("Failed to setup JMeter environment")
            return False

    def prepare_jmx_template(self, test_name):
        """准备JMX模板文件，返回工作目录的JMX路径"""
        # 获取JMeter配置
        server_config = self.jmeter_setting.get("spec", {}).get("server", {})
        jmx_template = server_config.get("jmx_template")
        
        if not jmx_template:
            self.logger.error("JMX template not specified in JMeter configuration")
            return None
        
        # 复制JMX模板到工作目录
        template_jmx_path = os.path.join(os.environ['TEST_ROOT'], f"env/jmeter/{jmx_template}")
        work_jmx_path = os.path.join(self.run_log_dir, f"{test_name}_{jmx_template}")
        
        if not os.path.exists(template_jmx_path):
            self.logger.error(f"JMX template not found: {template_jmx_path}")
            return None
        
        shutil.copy2(template_jmx_path, work_jmx_path)
        self.logger.debug(f"Copied JMX template: {os.path.basename(work_jmx_path)}")
        
        return work_jmx_path

    def prepare_sql_file(self, sql_content, file_name):
        """准备SQL文件，返回SQL文件路径"""
        sql_file_path = os.path.join(self.run_log_dir, file_name)
        
        # 使用智能SQL处理方法
        processed_sql = self.smart_process_sql(sql_content)
        
        with open(sql_file_path, 'w', encoding='utf-8') as f:
            f.write(processed_sql)
        
        self.logger.debug(f"Created SQL file: {os.path.basename(sql_file_path)}")
        return sql_file_path

    def load_query_config(self):
        """加载查询配置文件"""
        try:
            if os.path.exists(self.query_config_file):
                with open(self.query_config_file, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
                    self.logger.info(f"成功加载查询配置文件: {self.query_config_file}")
                    return config
            else:
                self.logger.warning(f"查询配置文件不存在: {self.query_config_file}")
                # 返回默认配置
                return self.get_default_query_config()
        except Exception as e:
            self.logger.error(f"加载查询配置文件失败: {e}")
            return self.get_default_query_config()

    def get_default_query_config(self):
        """返回默认查询配置"""
        return {
            "basic_queries": {
                "condition_filter_queries": [
                    {
                        "name": "条件过滤查询_ID等值",
                        "description": "时序表查询-条件过滤(ID等值查询)",
                        "sql": "select * from case17 where id = 600001",
                        "iterations": 10000
                    },
                    {
                        "name": "条件过滤查询_价格范围",
                        "description": "时序表查询-条件过滤(价格范围查询)",
                        "sql": "select * from case17 where price > 10",
                        "iterations": 10000
                    }
                ],
                "join_queries": [
                    {
                        "name": "多表关联查询",
                        "description": "时序表查询-多表关联",
                        "sql": "select * from d17_0 a, d17_1 b, d17_2 c where a.ts=b.ts and a.ts=c.ts",
                        "iterations": 10000
                    }
                ],
                "order_queries": [
                    {
                        "name": "字段排序查询",
                        "description": "时序表查询-字段排序",
                        "sql": "select * from d17_0 order by ts",
                        "iterations": 10000
                    }
                ],
                "aggregate_queries": [
                    {
                        "name": "字段聚合查询",
                        "description": "时序表查询-字段聚合",
                        "sql": "select count(*) from d17_0",
                        "iterations": 10000
                    }
                ]
            },
            "concurrent_query": {
                "name": "多表关联并发查询",
                "description": "时序表查询-多表关联并发测试",
                "sql": "select * from d17_0 a, d17_1 b, d17_2 c where a.ts=b.ts and a.ts=c.ts",
                "concurrency_levels": [10, 50, 100]
            },
            "test_config": {
                "enable_sql_validation": True,
                "validation_limit": 1,
                "enable_screenshots": True,
                "jmeter_timeout": 3600
            }
        }

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
        json_info["test_log"] = self.run_test_log_dir
        
        # 从json_file中读取数据库名称并保存
        self.actual_db_name = json_info.get("databases", [{}])[0].get("dbinfo", {}).get("name", self.dbname)
        self.logger.info(f"从JSON配置中读取到数据库名称: {self.actual_db_name}")
        
        self.tdCom.dump_json(f'{self.run_log_dir}/{self.json_file_name}', json_info)
        self.json_data_list = [json_info]
        self.tdCom.put_file(self._remote, self.taosBenchmark_iplist, self.json_data_list, json_filename_list, self.run_log_dir)
        self.result_filename = self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, self.json_data_list, json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)
        
        # 使用读取到的数据库名称
        self.tdSql.execute(f'flush database {self.actual_db_name}')

    # jmeter_demo_test方法已删除 - 使用query_config.yaml中的实际查询测试代替

    def perform_basic_queries(self):
        """执行基础查询验证和JMeter性能测试"""
        try:
            self.logger.info("开始执行基础查询JMeter性能测试...")
            
            # 从配置文件读取查询列表
            queries = self.get_all_queries_from_config()
            test_config = self.query_config.get("test_config", {})
            
            if not queries:
                self.logger.warning("未找到有效的查询配置，跳过基础查询测试")
                return
            
            self.logger.info(f"从配置文件加载了 {len(queries)} 个查询")
            
            # 根据配置决定是否进行SQL验证
            if test_config.get("enable_sql_validation", True):
                self.logger.info("首先验证所有SQL的正确性...")
                validation_limit = test_config.get("validation_limit", 1)
                
                # 确保使用正确的数据库
                self.ensure_database_connection()
                
                for query in queries:
                    try:
                        self.logger.info(f"验证查询: {query['description']}")
                        
                        # 检查SQL是否已经包含LIMIT子句（使用正则表达式精确匹配）
                        original_sql = query['sql'].strip()
                        
                        # 使用正则表达式检查是否有LIMIT关键字（作为独立单词）
                        limit_pattern = r'\blimit\s+\d+'
                        has_limit = bool(re.search(limit_pattern, original_sql, re.IGNORECASE))
                        
                        if has_limit:
                            # 如果已经有LIMIT，直接使用原SQL验证
                            validation_sql = original_sql
                            self.logger.debug(f"SQL已包含LIMIT子句，使用原SQL验证")
                        else:
                            # 如果没有LIMIT，添加验证限制
                            validation_sql = f"{original_sql} limit {validation_limit}"
                            self.logger.debug(f"SQL无LIMIT子句，添加limit {validation_limit}")
                        
                        self.logger.debug(f"query: {validation_sql}")
                        self.tdSql.query(validation_sql)
                        self.logger.info(f"验证成功: {query['name']}")
                    except Exception as e:
                        self.logger.warning(f"查询验证失败: {query['name']}, 错误: {e}")
            
            # 使用JMeter对每个查询执行测试
            self.logger.info("开始JMeter性能测试...")
            
            for query in queries:
                iterations = query.get('iterations', 10000)
                self.logger.info(f"测试查询: {query['name']}, 执行次数: {iterations}")
                
                self.run_single_query_jmeter_test(
                    query_sql=query['sql'],
                    query_name=query['name'],
                    query_description=query['description'],
                    iterations=iterations
                )
                    
        except Exception as e:
            self.logger.error(f"基础查询测试失败: {e}")

    def get_all_queries_from_config(self):
        """从配置文件获取所有查询"""
        all_queries = []
        
        try:
            basic_queries = self.query_config.get("basic_queries", {})
            
            # 获取各类查询
            query_categories = [
                "condition_filter_queries",
                "join_queries", 
                "order_queries",
                "aggregate_queries"
            ]
            
            for category in query_categories:
                category_queries = basic_queries.get(category, [])
                if isinstance(category_queries, list):
                    all_queries.extend(category_queries)
                    self.logger.info(f"加载 {category} 类别的 {len(category_queries)} 个查询")
            
            # 获取自定义查询
            custom_queries = self.query_config.get("custom_queries", [])
            if isinstance(custom_queries, list):
                all_queries.extend(custom_queries)
                self.logger.info(f"加载自定义查询 {len(custom_queries)} 个")
            
            return all_queries
            
        except Exception as e:
            self.logger.error(f"从配置文件获取查询失败: {e}")
            return []

    def run_single_query_jmeter_test(self, query_sql, query_name, query_description, iterations=10000):
        """为单个查询运行JMeter测试"""
        if not self.jmeter_util or not self.jmeter_setting:
            self.logger.warning("JMeter未配置，跳过查询测试")
            return None

        try:
            self.logger.info(f"=== 开始JMeter单查询测试: {query_description} ===")
            self.logger.info(f"查询SQL: {query_sql}")
            self.logger.info(f"执行次数: {iterations}")

            # 确保JMeter环境已设置（避免重复设置）
            if not self.ensure_jmeter_setup():
                return None

            # 创建安全的文件名
            safe_name = query_name.replace("_", "_").replace(" ", "_")
            
            # 准备JMX模板和SQL文件
            work_jmx_path = self.prepare_jmx_template(safe_name)
            if not work_jmx_path:
                return None

            sql_file_path = self.prepare_sql_file(query_sql, f"{safe_name}.sql")
            
            # 创建结果目录
            query_results_dir = os.path.join(self.run_log_dir, f"jmeter_results_{safe_name}")
            os.makedirs(query_results_dir, exist_ok=True)
            
            # 修改JMeter设置以支持高性能测试
            modified_jmeter_setting = copy.deepcopy(self.jmeter_setting)
            modified_jmeter_setting["spec"]["server"]["jmx_template_path"] = work_jmx_path
            
            # 为性能测试自定义并发配置
            if iterations >= 1000:
                # 对于高执行次数，使用少量线程但高循环次数
                thread_count = "1"
                loop_count = str(iterations)
                description = f"性能测试 - {iterations}次执行"
            else:
                # 对于低执行次数，使用默认配置
                thread_count = "1"
                loop_count = str(iterations)
                description = f"功能测试 - {iterations}次执行"
            
            # 添加性能测试的并发配置
            performance_concurrency = {
                "name": f"performance_{safe_name}",
                "thread_count": thread_count,
                "loop_count": loop_count,
                "description": description
            }
            
            # 为单查询性能测试，只使用自定义的性能配置，清除默认配置
            modified_jmeter_setting["spec"]["server"]["concurrency_levels"] = [performance_concurrency]
            
            self.logger.info(f"配置性能测试: 线程数={thread_count}, 循环次数={loop_count}")
            
            # 临时替换util的设置
            original_setting = self.jmeter_util._jmeter_setting
            self.jmeter_util._jmeter_setting = modified_jmeter_setting
            
            try:
                # 运行JMeter测试，直接使用run_multi_concurrency_test方法
                if hasattr(self.jmeter_util, 'run_multi_concurrency_test'):
                    # 使用多并发测试方法
                    results = self.jmeter_util.run_multi_concurrency_test(
                        sql_file_path=sql_file_path,
                        results_dir=query_results_dir,
                        test_root=os.environ['TEST_ROOT']
                    )
                    # 将结果转换为单个结果格式
                    result = results[0] if results and len(results) > 0 else None
                elif hasattr(self.jmeter_util, 'run_single_concurrency_test'):
                    # 备选方案：使用单并发测试
                    result = self.jmeter_util.run_single_concurrency_test(
                        sql_file_path=sql_file_path,
                        concurrency=1,  # 单线程但执行多次
                        results_dir=query_results_dir,
                        test_root=os.environ['TEST_ROOT']
                    )
                else:
                    self.logger.warning(f"JMeter工具类不支持所需的测试方法，跳过: {query_name}")
                    return None
            finally:
                # 恢复原始设置
                self.jmeter_util._jmeter_setting = original_setting
            
            if result:
                self.logger.info(f"JMeter测试完成: {query_description}")
                
                # 保存测试文件到结果目录
                test_files_dir = os.path.join(query_results_dir, "test_files")
                os.makedirs(test_files_dir, exist_ok=True)
                
                # 复制SQL和JMX文件
                if os.path.exists(sql_file_path):
                    shutil.copy2(sql_file_path, test_files_dir)
                if os.path.exists(work_jmx_path):
                    shutil.copy2(work_jmx_path, test_files_dir)
                
                self.logger.info(f"Test files saved to: {test_files_dir}")
                
                # 生成截图
                self.generate_query_screenshots(query_results_dir, safe_name, query_description)
                
                return {
                    'query_name': query_name,
                    'query_sql': query_sql,
                    'iterations': iterations,
                    'result_dir': query_results_dir,
                    'test_files_dir': test_files_dir,
                    'result': result
                }
            else:
                self.logger.warning(f"JMeter测试失败: {query_description}")
                return None

        except Exception as e:
            self.logger.error(f"JMeter查询测试异常: {query_description}, 错误: {e}")
            return None
        finally:
            if self.jmeter_util:
                self.jmeter_util.cleanup()

    def generate_query_screenshots(self, result_dir, safe_name, description):
        """为查询结果生成截图"""
        try:
            # 检查是否启用截图功能
            test_config = self.query_config.get("test_config", {})
            if not test_config.get("enable_screenshots", True):
                self.logger.info(f"根据配置跳过 {safe_name} 的截图生成")
                return
                
            # 查找并截图HTML报告
            for root, _, files in os.walk(result_dir):
                for file in files:
                    if file == "index.html":
                        html_file = os.path.join(root, file)
                        self.playwright.take_screenshot(
                            target=html_file,
                            output_file=f"jmeter_report_{safe_name}.png"
                        )
                        self.logger.info(f"生成HTML报告截图: jmeter_report_{safe_name}.png")
                        
                    elif file == "test_summary.txt":
                        summary_file = os.path.join(root, file)
                        self.playwright.take_screenshot_text(
                            text_file=summary_file,
                            output_file=f"jmeter_summary_{safe_name}.png",
                            title=f"JMeter测试摘要 - {description}"
                        )
                        self.logger.info(f"生成摘要报告截图: jmeter_summary_{safe_name}.png")
                        
        except Exception as e:
            self.logger.warning(f"生成截图失败: {safe_name}, 错误: {e}")

    def create_jmeter_query_test_plan(self, query_sql, concurrency_levels, query_name="concurrent_query"):
        """创建JMeter查询测试计划"""
        if not self.jmeter_util or not self.jmeter_setting:
            self.logger.warning("JMeter未配置，跳过JMeter查询测试")
            return None
            
        try:
            self.logger.info("=== 开始JMeter并发查询测试计划 ===")
            
            # 确保JMeter环境已设置（避免重复设置）
            if not self.ensure_jmeter_setup():
                return None

            # 准备JMX模板和SQL文件
            safe_name = re.sub(r'[^\w\-_]', '_', query_name)
            work_jmx_path = self.prepare_jmx_template(f"concurrent_{safe_name}")
            if not work_jmx_path:
                return None

            sql_file_path = self.prepare_sql_file(query_sql, f"{safe_name}_test.sql")

            self.logger.info(f"创建查询SQL文件: {sql_file_path}")
            self.logger.info(f"查询SQL: {query_sql}")

            # 创建JMeter结果目录
            jmeter_results_dir = os.path.join(self.run_log_dir, "jmeter_query_results")
            os.makedirs(jmeter_results_dir, exist_ok=True)

            # 修改JMeter设置以使用工作目录的JMX
            modified_jmeter_setting = copy.deepcopy(self.jmeter_setting)
            modified_jmeter_setting["spec"]["server"]["jmx_template_path"] = work_jmx_path

            # 临时替换util的设置
            original_setting = self.jmeter_util._jmeter_setting
            self.jmeter_util._jmeter_setting = modified_jmeter_setting
            
            results = []
            
            try:
                for concurrency in concurrency_levels:
                    self.logger.info(f"开始{concurrency}并发查询测试...")
                    
                    try:
                        # 创建子目录用于存放每个并发级别的结果
                        concurrency_dir = os.path.join(jmeter_results_dir, f"concurrency_{concurrency}")
                        os.makedirs(concurrency_dir, exist_ok=True)
                        
                        # 运行JMeter测试 - 为每个并发级别创建独立的配置
                        if hasattr(self.jmeter_util, 'run_multi_concurrency_test'):
                            # 为当前并发级别创建临时配置
                            temp_jmeter_setting = copy.deepcopy(modified_jmeter_setting)
                            
                            # 创建单一并发级别配置
                            concurrency_config = {
                                "name": f"concurrency_{concurrency}",
                                "thread_count": str(concurrency),
                                "loop_count": "10",  # 每线程执行10次
                                "description": f"并发测试 - {concurrency}线程"
                            }
                            
                            # 设置并发配置
                            temp_jmeter_setting["spec"]["server"]["concurrency_levels"] = [concurrency_config]
                            
                            # 临时替换设置
                            self.jmeter_util._jmeter_setting = temp_jmeter_setting
                            
                            # 运行测试
                            test_results = self.jmeter_util.run_multi_concurrency_test(
                    sql_file_path=sql_file_path,
                                results_dir=concurrency_dir,
                    test_root=os.environ['TEST_ROOT']
                )
                            
                            if test_results and len(test_results) > 0:
                                result = test_results[0]  # 取第一个结果
                                
                                # 保存测试文件到每个并发级别的结果目录
                                test_files_dir = os.path.join(concurrency_dir, "test_files")
                                os.makedirs(test_files_dir, exist_ok=True)
                                
                                # 复制SQL和JMX文件
                                if os.path.exists(sql_file_path):
                                    shutil.copy2(sql_file_path, test_files_dir)
                                if os.path.exists(work_jmx_path):
                                    shutil.copy2(work_jmx_path, test_files_dir)
                                
                                results.append({
                                    'concurrency': concurrency,
                                    'result_dir': concurrency_dir,
                                    'test_files_dir': test_files_dir,
                                    'result': result
                                })
                                self.logger.info(f"{concurrency}并发测试完成")
                            else:
                                self.logger.warning(f"{concurrency}并发测试失败")
                        else:
                            self.logger.warning("JMeter工具类不支持多并发测试方法")
                            
                    except Exception as e:
                        self.logger.error(f"{concurrency}并发测试异常: {e}")
            finally:
                # 恢复原始设置
                self.jmeter_util._jmeter_setting = original_setting

            if results:
                # 保存总体测试文件到主结果目录
                main_test_files_dir = os.path.join(jmeter_results_dir, "main_test_files")
                os.makedirs(main_test_files_dir, exist_ok=True)
                
                # 复制SQL和JMX文件到主目录
                if os.path.exists(sql_file_path):
                    shutil.copy2(sql_file_path, main_test_files_dir)
                if os.path.exists(work_jmx_path):
                    shutil.copy2(work_jmx_path, main_test_files_dir)
                
                self.logger.info(f"Main test files saved to: {main_test_files_dir}")
            
            return results if results else None

        except Exception as e:
            self.logger.error(f"创建JMeter查询测试计划失败: {e}")
            return None
        finally:
            if self.jmeter_util:
                self.jmeter_util.cleanup()

    def perform_jmeter_concurrent_queries(self):
        """执行JMeter并发查询测试"""
        try:
            test_config = self.query_config.get("test_config", {})
            
            # 支持单个concurrent_query配置（向后兼容）
            concurrent_config = self.query_config.get("concurrent_query", {})
            # 支持多个concurrent_queries配置（新功能）
            concurrent_queries = self.query_config.get("concurrent_queries", [])
            
            # 如果有单个配置，转换为列表格式
            if concurrent_config and not concurrent_queries:
                concurrent_queries = [concurrent_config]
            elif not concurrent_queries and not concurrent_config:
                self.logger.warning("未找到并发查询配置(concurrent_query或concurrent_queries)，跳过并发测试")
                return
            
            all_results = []
            
            # 处理每组并发查询配置
            for idx, config in enumerate(concurrent_queries, 1):
                concurrent_query = config.get("sql", "select * from d17_0 a, d17_1 b, d17_2 c where a.ts=b.ts and a.ts=c.ts")
                concurrency_levels = config.get("concurrency_levels", [10, 50, 100])
                query_name = config.get("name", f"并发查询_{idx}")
                query_description = config.get("description", f"并发测试组_{idx}")
                
                self.logger.info(f"=== 开始第{idx}组JMeter并发查询测试 ===")
                self.logger.info(f"测试名称: {query_name}")
                self.logger.info(f"测试SQL: {concurrent_query}")
                self.logger.info(f"并发级别: {concurrency_levels}")
                
                results = self.create_jmeter_query_test_plan(concurrent_query, concurrency_levels, query_name)
                
                if results:
                    self.logger.info(f"第{idx}组JMeter并发查询测试完成，共完成{len(results)}个并发级别的测试")
                    
                    # 为每个结果添加查询名称信息
                    for result in results:
                        result['query_name'] = query_name
                        result['query_description'] = query_description
                        result['group_index'] = idx
                    
                    all_results.extend(results)
                    
                    # 根据配置决定是否生成截图
                    if test_config.get("enable_screenshots", True):
                        self.logger.info(f"生成第{idx}组并发测试结果截图...")
                        
                        # 为每个结果创建截图，包含查询名称
                        for result in results:
                            concurrency = result['concurrency']
                            result_dir = result['result_dir']
                            safe_query_name = re.sub(r'[^\w\-_]', '_', query_name)  # 安全的文件名
                            
                            # 查找HTML报告并截图
                            for root, _, files in os.walk(result_dir):
                                for file in files:
                                    if file == "index.html":
                                        html_file = os.path.join(root, file)
                                        self.playwright.take_screenshot(
                                            target=html_file,
                                            output_file=f"jmeter_{safe_query_name}_concurrency_{concurrency}.png"
                                        )
                                        
                                    elif file == "test_summary.txt":
                                        summary_file = os.path.join(root, file)
                                        self.playwright.take_screenshot_text(
                                            text_file=summary_file,
                                            output_file=f"jmeter_{safe_query_name}_summary_concurrency_{concurrency}.png",
                                            title=f"{query_name} - {concurrency}并发测试摘要"
                                        )
                    else:
                        self.logger.info("根据配置跳过截图生成")
                else:
                    self.logger.warning(f"第{idx}组JMeter并发查询测试失败或无结果")
            
            if all_results:
                self.logger.info(f"所有并发查询测试完成，共完成{len(concurrent_queries)}组测试，{len(all_results)}个并发级别")
            else:
                self.logger.warning("所有JMeter并发查询测试失败或无结果")
                
        except Exception as e:
            self.logger.error(f"并发查询测试失败: {e}")

    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        if hasattr(self, 'jmeter_util') and self.jmeter_util:
            self.jmeter_util.cleanup()

    def run(self):
        # 1. 首先执行插入操作
        self.logger.info("=== 步骤1: 数据插入 ===")
        self.insert_with_load_json()
        
        # 2. 执行基础查询验证
        self.logger.info("=== 步骤2: 基础查询验证 ===")
        self.perform_basic_queries()
        
        # 3. 执行JMeter并发查询测试
        self.logger.info("=== 步骤3: JMeter并发查询测试 ===")
        self.perform_jmeter_concurrent_queries()

        # 生成性能报告
        result_file_name = self.run_log_dir + '/perf_report.txt'
        timestamp_start = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        timestamp_end = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        
        try:
        self.perf.taosBenchmark_insert_summary_result(self.result_filename, version="3.0")
        self.perf.get_process_exporter_info(self.prom_env_setting, 1, timestamp_start, timestamp_end)
        self.perf.get_node_exporter_info(self.prom_env_setting, 1, timestamp_start, timestamp_end)
        except Exception as e:
            self.logger.warning(f"性能报告生成失败: {e}")

        print(result_file_name)

        # 生成性能报告截图
        self._remote._logger.info("生成性能报告截图...")
        self.playwright.take_screenshot_text(
            text_file=result_file_name,
            output_file=f"{os.path.basename(result_file_name)}.png",
            title="性能测试报告"
        )

        self._remote._logger.info("收集测试截图...")
        
        # JMeter并发查询测试的截图已经在perform_jmeter_concurrent_queries()中生成了
        # 这里不需要重复生成截图

        self.playwright.collect_screenshots(self.run_log_dir)
        self.playwright.reset()
        self._remote._logger.info("测试截图收集完成！")