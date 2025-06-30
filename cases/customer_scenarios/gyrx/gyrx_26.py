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
import threading
import time


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
        self.json_file = os.path.join(self.env_root, "pocs/gyrx/test.json")
        self.json_log ={}
        
        # 并发控制参数
        self.keep_writing = True  # 控制是否继续写入数据
        self.write_interval = 1   # 写入间隔(秒)
        self.node_stop_duration = 30  # 节点停止时间(秒)
        self.test_duration = 60   # 测试持续时间(秒)
        self.inserted_count = 0   # 实际成功插入的记录数

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

    def execute_sql(self,sql):
        self.tdSql.execute(sql)
        self.json_log[sql] = '执行成功'


    def query_sql(self,sql,expected_count=None,expected_res=None,db_name=None):
        if db_name is None:
            db_name = self.dbname
        self.tdSql.execute(f'use {db_name}')
        self.tdSql.query(sql)
        print(self.tdSql.query_data)
        # 转换查询结果为JSON可序列化格式
        serializable_data = self.convert_to_json_serializable(self.tdSql.query_data)
        self.json_log[sql] = serializable_data


    def insert_with_python_connector(self):
        
        # Custom Write
        self.execute_sql(f'drop database if exists {self.dbname}')
        self.execute_sql(f'create database {self.dbname} replica 3 vgroups 3')
        self.execute_sql(f'use {self.dbname}')
        self.execute_sql('create table test (ts timestamp, v_int int ,v_double double)')
        



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
        self.result_filename = self.tdCom.threads_run_taosBenchmark(self._remote, self.taosBenchmark_iplist, self.json_data_list, json_filename_list, self.taosBenchmark_env_setting, self.run_log_dir)
        self.tdSql.execute(f'flush database {self.dbname}')

    def continuous_data_insert(self):
        """持续数据写入线程"""
        counter = 1
        self.logger.info("开始持续数据写入...")
        
        while self.keep_writing:
            try:
                current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                insert_sql = f'insert into test (ts,v_int,v_double) values ("{current_time}",{counter},{counter*1.1})'
                self.execute_sql(insert_sql)
                self.inserted_count += 1  # 成功插入后计数器加1
                self.logger.debug(f"插入数据: counter={counter}, 总插入数: {self.inserted_count}")
                counter += 1
                time.sleep(self.write_interval)
            except Exception as e:
                self.logger.error(f"数据写入失败: {e}")
                time.sleep(self.write_interval)  # 失败后也要等待，避免无限循环
        
        self.logger.info(f"数据写入线程结束，总共成功插入 {self.inserted_count} 条记录")

    def node_restart_operations(self):
        """节点启停操作线程"""
        self.logger.info("开始节点启停操作...")
        
        try:
            # 等待10秒让写入线程先运行一段时间
            time.sleep(10)
            
            # 获取taosd节点信息
            taosd_iplist = self.get_fqdn("taosd")
            if taosd_iplist:
                target_node = taosd_iplist[1]  # 选择第二个节点
                dnode_name = f"{target_node}:6030"  # 构造dnode名称
                
                self.logger.info(f"准备停止节点: {dnode_name}")
                
                # 停止节点
                self.logger.info(f"停止节点: {dnode_name}")
                self.envMgr.stopDnode(dnode_name)
                
                # 等待30秒
                self.logger.info(f"节点已停止，等待 {self.node_stop_duration} 秒...")
                time.sleep(self.node_stop_duration)
                
                # 启动节点
                self.logger.info(f"启动节点: {dnode_name}")
                self.envMgr.startDnode(dnode_name)
                time.sleep(5)  # 等待5秒让服务完全启动
                
                self.logger.info(f"节点 {dnode_name} 重启完成")
            
            # 等待测试时间结束
            remaining_time = self.test_duration - 10 - self.node_stop_duration - 5
            if remaining_time > 0:
                self.logger.info(f"继续写入数据，剩余时间: {remaining_time} 秒")
                time.sleep(remaining_time)
                
        except Exception as e:
            self.logger.error(f"节点启停操作失败: {e}")
        
        # 测试时间结束，停止写入
        self.keep_writing = False
        self.logger.info("节点启停操作线程结束")

    def concurrent_test(self):
        """执行并发测试：持续写入 + 节点启停"""
        self.logger.info(f"开始并发测试，持续时间: {self.test_duration}秒")
        
        # 创建并启动写入线程
        write_thread = threading.Thread(target=self.continuous_data_insert, name="DataWriteThread")
        write_thread.daemon = True
        write_thread.start()
        
        # 创建并启动节点启停线程
        restart_thread = threading.Thread(target=self.node_restart_operations, name="NodeRestartThread")
        restart_thread.daemon = True
        restart_thread.start()
        
        # 等待测试完成
        restart_thread.join()
        write_thread.join(timeout=5)  # 给写入线程5秒时间结束
        
        self.logger.info("并发测试完成")

    def verify_data_consistency(self):
        """验证数据一致性：比较实际插入数和数据库记录数"""
        self.logger.info("开始数据一致性验证...")
        
        try:
            # 查询数据库中的实际记录数
            self.tdSql.execute(f'use {self.dbname}')
            self.tdSql.query('select count(*) from test')
            db_count = self.tdSql.query_data[0][0]
            
            self.logger.info(f"实际插入条数: {self.inserted_count}")
            self.logger.info(f"数据库记录数: {db_count}")
            
            # 比较两个数值
            if self.inserted_count == db_count:
                self.logger.info("✅ 数据一致性验证通过：插入数与数据库记录数相等")
                self.json_log["数据一致性验证"] = "通过"
                self.json_log["实际插入数"] = self.inserted_count
                self.json_log["数据库记录数"] = db_count
                return True
            else:
                self.logger.error(f"❌ 数据一致性验证失败：插入数({self.inserted_count}) != 数据库记录数({db_count})")
                self.logger.error(f"丢失数据: {self.inserted_count - db_count} 条")
                self.json_log["数据一致性验证"] = "失败"
                self.json_log["实际插入数"] = self.inserted_count
                self.json_log["数据库记录数"] = db_count
                self.json_log["丢失数据"] = self.inserted_count - db_count
                return False
                
        except Exception as e:
            self.logger.error(f"数据一致性验证过程中出错: {e}")
            self.json_log["数据一致性验证"] = f"验证过程出错: {e}"
            return False


    def desc(self):
        pass

    def author(self):
        pass

    def tags(self):
        pass

    def cleanup(self):
        pass

    def run(self):
        # 初始化：创建数据库和表
        self.tdSql.execute("create mnode on dnode 2")
        self.tdSql.execute("create mnode on dnode 3")
        self.insert_with_python_connector()

        # 验证初始查询
        self.query_sql('select * from test')
        self.logger.info("初始数据验证完成")

        # 执行并发测试：持续写入 + 节点启停
        self.concurrent_test()

        # 数据一致性验证
        consistency_result = self.verify_data_consistency()

        # 验证节点状态
        self.logger.info("验证节点状态...")
        try:
            # 使用taos命令行工具查看节点状态
            cmd = "taos -s 'show dnodes'"
            self.logger.info(f"执行命令: {cmd}")
            result = os.system(cmd)
            if result == 0:
                self.logger.info("✅ show dnodes 命令执行成功")
                self.json_log["show_dnodes_status"] = "执行成功"
            else:
                self.logger.error(f"❌ show dnodes 命令执行失败，返回码: {result}")
                self.json_log["show_dnodes_status"] = f"执行失败，返回码: {result}"
        except Exception as e:
            self.logger.error(f"执行show dnodes时出错: {e}")
            self.json_log["show_dnodes_status"] = f"执行出错: {e}"
        
        # 测试结束后的验证查询
        self.query_sql('select count(*) from test')
        self.query_sql('select * from test order by ts desc limit 10')

        # 保存测试结果
        result_file_name = self.run_log_dir + '/perf_report.txt'
        self.tdCom.dump_json(f'{self.run_log_dir}/json_log.json', self.json_log)
        
        self.logger.info(f"测试完成，结果文件: {result_file_name}")
        print(result_file_name)
