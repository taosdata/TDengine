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
        
        # 基础配置参数
        self.replica = 3  # 强制设置为3副本
        self.dbname = "test_replica"
        self.stbname = "stb_replica"
        self.ctbname = "ctb_replica"
        self.tbname = "tb_replica"
        
        # 测试控制参数
        self.test_rounds = 3  # 测试轮次数
        self.write_batch_size = 100  # 每轮写入的记录数
        self.consistency_check_delay = 5  # 一致性检查延迟(秒)
        
        # 集群节点信息
        self.taosd_nodes = []
        self.current_leader = None
        self.test_results = {}
        self.json_log = {}
        
        # 数据计数器
        self.total_inserted = 0
        self.round_counter = 0
        
        # 获取集群节点列表
        self.taosd_iplist = self.get_fqdn("taosd")
        if len(self.taosd_iplist) < 3:
            self.logger.error(f"需要至少3个taosd节点，当前只有{len(self.taosd_iplist)}个")
            raise Exception("集群节点数量不足")
        
        self.logger.info(f"检测到 {len(self.taosd_iplist)} 个taosd节点: {self.taosd_iplist}")

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

    def execute_sql(self, sql, log_success=False):
        """执行SQL语句并记录日志"""
        try:
            self.tdSql.execute(sql)
            if log_success:
                self.logger.info(f"SQL执行成功: {sql}")
            return True
        except Exception as e:
            self.logger.error(f"SQL执行失败: {sql}, 错误: {e}")
            return False

    def query_sql(self, sql, log_result=True):
        """执行查询SQL并返回结果"""
        try:
            self.tdSql.query(sql)
            if log_result:
                serializable_data = self.convert_to_json_serializable(self.tdSql.query_data)
                self.json_log[sql] = {
                    "row_count": self.tdSql.query_row,
                    "query_data": serializable_data
                }
            self.logger.info(f"查询成功: {sql}, 返回{self.tdSql.query_row}行")
            return self.tdSql.query_data
        except Exception as e:
            self.logger.error(f"查询失败: {sql}, 错误: {e}")
            self.json_log[sql] = f'查询失败: {e}'
            return None

    def setup_cluster_and_database(self):
        """初始化3节点集群和数据库"""
        self.logger.info("=== 开始初始化3节点集群 ===")
        
        # 先检查现有mnode状态
        self.logger.info("检查当前mnode状态...")
        self.query_sql("show mnodes")
        
        # 创建mnode节点 - 第1个节点默认是mnode，只需创建第2、3个
        self.logger.info("dnode 1 默认为mnode，只需创建dnode 2和3的mnode")
        for i in range(2, 4):  # 只创建 dnode 2, 3
            try:
                self.logger.info(f"尝试创建mnode on dnode {i}...")
                # 直接执行SQL，不使用execute_sql方法，避免重试机制
                self.tdSql.execute(f"create mnode on dnode {i}")
                self.logger.info(f"✅ mnode {i} 创建成功")
                self.json_log[f"create mnode on dnode {i}"] = '创建成功'
                time.sleep(2)
            except Exception as e:
                if "already exists" in str(e) or "Mnode already exists" in str(e):
                    self.logger.info(f"✅ mnode {i} 已存在，跳过创建")
                    self.json_log[f"create mnode on dnode {i}"] = '已存在'
                else:
                    self.logger.error(f"❌ 创建mnode {i}失败: {e}")
                    self.json_log[f"create mnode on dnode {i}"] = f'创建失败: {e}'
        
        time.sleep(10)  # 等待mnode节点完全启动
        
        # 再次检查mnode状态
        self.logger.info("验证mnode最终状态...")
        self.query_sql("show mnodes")
        
        # 创建数据库和表
        self.execute_sql(f'drop database if exists {self.dbname}')
        self.execute_sql(f'create database {self.dbname} replica {self.replica} vgroups 3')
        self.execute_sql(f'use {self.dbname}')
        
        # 创建超级表 - 避免使用关键字作为列名
        self.execute_sql(f'create table {self.stbname} (ts timestamp, id int, val double, name varchar(50)) tags (location int)')
        
        # 创建子表
        self.execute_sql(f'create table {self.ctbname} using {self.stbname} tags (1)')
        
        # 创建普通表 - 避免使用关键字作为列名
        self.execute_sql(f'create table {self.tbname} (ts timestamp, id int, val double, name varchar(50))')
        
        self.logger.info("✅ 集群和数据库初始化完成")

    def get_current_leader_info(self, show_details=True):
        """获取当前leader信息"""
        try:
            # 查询vgroup信息
            vgroup_data = self.query_sql("show vgroups", log_result=False)
            if vgroup_data:
                leader_info = {}
                leader_summary = []
                
                # 根据实际的show vgroups返回格式解析
                # 格式: [vgroup_id, db_name, tables, v1_dnode, v1_status, v1_applied/committed, 
                #        v2_dnode, v2_status, v2_applied/committed, v3_dnode, v3_status, v3_applied/committed, ...]
                
                for row in vgroup_data:
                    if len(row) >= 11:  # 确保有足够的列
                        vgid = row[0]
                        
                        # 查找leader：检查v1_status, v2_status, v3_status
                        leader_dnode = None
                        
                        # v1节点检查 (索引3=dnode, 索引4=status)
                        if len(row) > 4 and row[4] == 'leader':
                            leader_dnode = row[3]
                        # v2节点检查 (索引6=dnode, 索引7=status)  
                        elif len(row) > 7 and row[7] == 'leader':
                            leader_dnode = row[6]
                        # v3节点检查 (索引9=dnode, 索引10=status)
                        elif len(row) > 10 and row[10] == 'leader':
                            leader_dnode = row[9]
                        
                        if leader_dnode:
                            leader_info[f"vgroup_{vgid}"] = f"dnode_{leader_dnode}"
                            leader_summary.append(f"VG{vgid}→DN{leader_dnode}")
                        else:
                            # 如果没找到leader，记录为未知
                            leader_info[f"vgroup_{vgid}"] = "no_leader"
                            leader_summary.append(f"VG{vgid}→未知")
                
                if show_details:
                    print(f"🔍 当前Leader分布: {' | '.join(leader_summary)}")
                
                self.json_log["当前leader信息"] = leader_info
                return leader_info
            
            return {}
        except Exception as e:
            if show_details:
                self.logger.error(f"获取leader信息失败: {e}")
            return {}

    def trigger_leader_switch_by_stopping_node(self, target_dnode):
        """通过停止当前leader节点来触发leader切换"""
        self.logger.info(f"=== 通过停止节点触发leader切换测试 ===")
        
        try:
            # 获取当前vgroup信息
            vgroup_data = self.query_sql("show vgroups", log_result=False)
            if not vgroup_data:
                self.logger.error("无法获取vgroup信息")
                return False
            
            # 记录当前leader信息
            current_leaders = {}
            nodes_to_stop = set()
            
            for row in vgroup_data:
                if len(row) >= 4:
                    vgid = row[0]
                    current_leader = row[2]
                    current_leaders[vgid] = current_leader
                    
                    # 如果当前leader不是目标节点，就停止当前leader
                    if current_leader != target_dnode:
                        nodes_to_stop.add(current_leader)
            
            self.logger.info(f"当前leader分布: {current_leaders}")
            self.logger.info(f"需要停止的节点: {nodes_to_stop}")
            
            # 如果没有需要停止的节点，说明目标节点已经是所有vgroup的leader
            if not nodes_to_stop:
                self.logger.info(f"dnode {target_dnode} 已经是所有vgroup的leader，无需切换")
                return True
            
            # 停止节点来触发leader切换
            stopped_nodes = []
            taosd_iplist = self.get_fqdn("taosd")
            
            for node_id in nodes_to_stop:
                try:
                    if node_id <= len(taosd_iplist):
                        node_ip = taosd_iplist[node_id - 1]  # node_id从1开始，数组从0开始
                        dnode_name = f"{node_ip}:6030"
                        
                        self.logger.info(f"停止节点 dnode {node_id} ({dnode_name})")
                        self.envMgr.stopDnode(dnode_name)
                        stopped_nodes.append((node_id, dnode_name))
                        time.sleep(5)  # 等待节点停止
                except Exception as e:
                    self.logger.error(f"停止dnode {node_id}失败: {e}")
            
            # 等待leader选举完成
            self.logger.info("等待leader重新选举...")
            time.sleep(15)
            
            # 验证新的leader分布
            new_leader_info = self.get_current_leader_info()
            self.logger.info("leader切换后的状态检查完成")
            
            # 重新启动停止的节点
            for node_id, dnode_name in stopped_nodes:
                try:
                    self.logger.info(f"重新启动节点 dnode {node_id} ({dnode_name})")
                    self.envMgr.startDnode(dnode_name)
                    time.sleep(5)  # 等待节点启动
                except Exception as e:
                    self.logger.error(f"启动dnode {node_id}失败: {e}")
            
            # 等待集群稳定
            self.logger.info("等待集群稳定...")
            time.sleep(10)
            
            # 最终状态检查
            final_leader_info = self.get_current_leader_info()
            self.json_log[f"节点重启后leader信息"] = final_leader_info
            
            return True
            
        except Exception as e:
            self.logger.error(f"leader切换测试过程中出错: {e}")
            return False

    def insert_test_data(self, batch_name, record_count=100, show_details=True):
        """插入测试数据"""
        if show_details:
            print(f"📝 数据写入: {batch_name} ({record_count}条)")
        
        success_count = 0
        base_timestamp = int(time.time() * 1000)  # 毫秒级时间戳
        
        try:
            # 构建批量插入SQL语句
            stb_values = []
            tb_values = []
            
            for i in range(record_count):
                ts = base_timestamp + i
                id_val = self.total_inserted + i + 1  # 使用累计计数作为ID
                val_data = round(random.uniform(1.0, 100.0), 2)
                name_val = f"data_{batch_name}_{i}"
                
                stb_values.append(f"({ts}, {id_val}, {val_data}, '{name_val}')")
                tb_values.append(f"({ts}, {id_val}, {val_data}, '{name_val}')")
            
            # 批量插入到超级表
            if stb_values:
                stb_sql = f"insert into {self.ctbname} values " + ",".join(stb_values)
                self.tdSql.execute(stb_sql)
                success_count = record_count
            
            # 批量插入到普通表
            if tb_values:
                tb_sql = f"insert into {self.tbname} values " + ",".join(tb_values)
                self.tdSql.execute(tb_sql)
                
        except Exception as e:
            if show_details:
                print(f"  ❌ 批量插入失败: {e}")
                # 如果批量插入失败，尝试逐条插入
                print(f"  🔄 尝试逐条插入...")
                success_count = 0
                for i in range(record_count):
                    try:
                        ts = base_timestamp + i
                        id_val = self.total_inserted + i + 1
                        val_data = round(random.uniform(1.0, 100.0), 2)
                        name_val = f"data_{batch_name}_{i}"
                        
                        # 逐条插入
                        stb_sql = f"insert into {self.ctbname} values ({ts}, {id_val}, {val_data}, '{name_val}')"
                        self.tdSql.execute(stb_sql)
                        success_count += 1
                        
                        tb_sql = f"insert into {self.tbname} values ({ts}, {id_val}, {val_data}, '{name_val}')"
                        self.tdSql.execute(tb_sql)
                        
                    except Exception as inner_e:
                        if show_details:
                            print(f"    ❌ 第{i}条数据插入失败: {inner_e}")
                            break
        
        self.total_inserted += success_count
        if show_details:
            print(f"✅ 写入完成: 成功{success_count}条，累计{self.total_inserted}条")
        
        return success_count

    def verify_data_consistency(self, round_id):
        """验证各个节点的数据一致性"""
        # 等待数据同步
        time.sleep(self.consistency_check_delay)
        
        try:
            # 查询超级表数据
            stb_data = self.query_sql(f"select count(*) from {self.ctbname}", log_result=False)
            stb_count = stb_data[0][0] if stb_data else 0
            
            # 查询普通表数据
            tb_data = self.query_sql(f"select count(*) from {self.tbname}", log_result=False)
            tb_count = tb_data[0][0] if tb_data else 0
            
            # 一致性检查
            is_consistent = (stb_count > 0 and tb_count > 0 and stb_count == tb_count)
            status = "✅ 通过" if is_consistent else "❌ 失败"
            
            print(f"  📊 超级表: {stb_count}条 | 普通表: {tb_count}条 | 预期: {self.total_inserted}条")
            print(f"  📈 一致性检查: {status}")
            
            consistency_results = {
                "round": round_id,
                "super_table_count": stb_count,
                "normal_table_count": tb_count,
                "expected_total": self.total_inserted,
                "consistency_check": "通过" if is_consistent else "失败"
            }
            
            self.json_log[f"第{round_id}轮一致性验证"] = consistency_results
            return consistency_results
            
        except Exception as e:
            print(f"  ❌ 一致性验证失败: {e}")
            error_result = {
                "round": round_id,
                "error": str(e),
                "consistency_check": "验证失败"
            }
            self.json_log[f"第{round_id}轮一致性验证"] = error_result
            return error_result

    def run_full_test_cycle(self):
        """运行完整的测试周期：数据写入 -> 节点故障模拟 -> 一致性验证"""
        print(f"\n🚀 开始3轮节点故障恢复测试")
        
        for round_id in range(1, self.test_rounds + 1):
            self.round_counter = round_id
            print(f"\n{'='*60}")
            print(f"🔄 第 {round_id}/{self.test_rounds} 轮测试")
            print(f"{'='*60}")
            
            # 步骤1: 获取当前leader信息
            print(f"📋 步骤1: 检查集群状态")
            
            # 调试：显示原始vgroup数据（仅第一轮）
            if round_id == 1:
                print(f"  🔍 调试信息：原始vgroup数据")
                raw_vgroup_data = self.query_sql("show vgroups", log_result=False)
                if raw_vgroup_data:
                    for i, row in enumerate(raw_vgroup_data):
                        print(f"    行{i+1}: {row}")
                        
            initial_leaders = self.get_current_leader_info()
            
            # 步骤2: 插入初始测试数据
            print(f"📋 步骤2: 写入初始数据")
            inserted_count = self.insert_test_data(f"round{round_id}_initial", self.write_batch_size)
            
            # 步骤3: 模拟节点故障
            print(f"📋 步骤3: 模拟节点故障")
            
            # 选择要停止的节点（轮流停止不同节点）
            nodes_to_test = [1, 2, 3]  # 第1轮停dnode1，第2轮停dnode2，第3轮停dnode3
            target_stop_node = nodes_to_test[(round_id - 1) % len(nodes_to_test)]
            print(f"  🎯 本轮将停止: dnode {target_stop_node}")
            
            fault_test_success = self.simulate_node_fault_and_recovery(target_stop_node)
            
            # 步骤4: 验证故障恢复后的状态
            print(f"📋 步骤4: 验证恢复状态")
            recovery_leaders = self.get_current_leader_info()
            
            # 步骤5: 在故障恢复后插入数据测试集群可用性
            print(f"📋 步骤5: 验证集群可用性")
            extra_inserted = self.insert_test_data(f"round{round_id}_recovery", 50)
            
            # 步骤6: 验证数据一致性
            print(f"📋 步骤6: 数据一致性验证")
            consistency_result = self.verify_data_consistency(round_id)
            
            # 记录本轮测试结果
            round_result = {
                "round": round_id,
                "test_type": "节点故障恢复测试",
                "target_stop_node": target_stop_node,
                "initial_leaders": initial_leaders,
                "fault_test_success": fault_test_success,
                "recovery_leaders": recovery_leaders,
                "initial_insert_count": inserted_count,
                "recovery_insert_count": extra_inserted,
                "consistency_result": consistency_result
            }
            
            self.test_results[f"round_{round_id}"] = round_result
            print(f"✅ 第 {round_id} 轮测试完成")
            
            # 轮次间间隔
            if round_id < self.test_rounds:
                print(f"⏳ 等待10秒后进行下一轮测试...")
                time.sleep(10)

    def simulate_node_fault_and_recovery(self, target_node):
        """模拟节点故障和恢复"""
        print(f"  🔧 目标节点: dnode {target_node}")
        
        try:
            taosd_iplist = self.get_fqdn("taosd")
            if target_node > len(taosd_iplist):
                print(f"  ❌ 节点{target_node}超出范围")
                return False
            
            node_ip = taosd_iplist[target_node - 1]
            dnode_name = f"{node_ip}:6030"
            
            # 子步骤3.1: 停止目标节点
            print(f"  🛑 停止节点 dnode {target_node}")
            self.envMgr.stopDnode(dnode_name)
            
            # 子步骤3.2: 等待leader重新选举
            print(f"  ⏳ 等待leader重新选举(15秒)...")
            time.sleep(15)
            
            # 子步骤3.3: 检查故障状态下的集群
            print(f"  🔍 检查故障状态")
            fault_leaders = self.get_current_leader_info(show_details=False)
            self.json_log[f"节点{target_node}故障时的leader信息"] = fault_leaders
            
            # 子步骤3.4: 在故障状态下测试写入
            print(f"  📝 故障下写入测试(20条)")
            fault_insert_count = self.insert_test_data(f"fault_node{target_node}", 20, show_details=False)
            print(f"  ✅ 故障下成功写入{fault_insert_count}条")
            
            # 子步骤3.5: 恢复节点
            print(f"  🔄 恢复节点 dnode {target_node}")
            self.envMgr.startDnode(dnode_name)
            
            # 子步骤3.6: 等待集群完全恢复
            print(f"  ⏳ 等待集群恢复(20秒)...")
            time.sleep(20)
            
            # 子步骤3.7: 验证恢复后的状态
            print(f"  🔍 验证恢复状态")
            recovery_leaders = self.get_current_leader_info(show_details=False)
            self.json_log[f"节点{target_node}恢复后的leader信息"] = recovery_leaders
            
            print(f"  ✅ 节点{target_node}故障恢复完成")
            return True
            
        except Exception as e:
            print(f"  ❌ 故障恢复失败: {e}")
            return False

    def generate_final_report(self):
        """生成最终测试报告"""
        self.logger.info("=== 生成最终测试报告 ===")
        
        # 最终数据统计
        final_stb_data = self.query_sql(f"select count(*) from {self.ctbname}", log_result=False)
        final_tb_data = self.query_sql(f"select count(*) from {self.tbname}", log_result=False)
        
        final_stb_count = final_stb_data[0][0] if final_stb_data else 0
        final_tb_count = final_tb_data[0][0] if final_tb_data else 0
        
        # 查询show vgroups最终状态
        final_vgroups = self.query_sql("show vgroups", log_result=False)
        
        # 查询show mnodes状态
        final_mnodes = self.query_sql("show mnodes", log_result=False)
        
        # 生成测试总结
        test_summary = {
            "测试配置": {
                "集群节点数": len(self.taosd_iplist),
                "数据库副本数": self.replica,
                "测试轮次": self.test_rounds,
                "每轮写入量": self.write_batch_size
            },
            "测试结果": self.test_results,
            "最终数据统计": {
                "超级表最终记录数": final_stb_count,
                "普通表最终记录数": final_tb_count,
                "预期总记录数": self.total_inserted,
                "数据一致性": "通过" if final_stb_count > 0 and final_tb_count > 0 else "失败"
            },
            "集群最终状态": {
                "vgroups信息": self.convert_to_json_serializable(final_vgroups) if final_vgroups else [],
                "mnodes信息": self.convert_to_json_serializable(final_mnodes) if final_mnodes else []
            }
        }
        
        self.json_log["测试总结报告"] = test_summary
        
        # 打印关键信息
        self.logger.info("=== 测试总结 ===")
        self.logger.info(f"完成 {self.test_rounds} 轮测试")
        self.logger.info(f"超级表最终记录数: {final_stb_count}")
        self.logger.info(f"普通表最终记录数: {final_tb_count}")
        self.logger.info(f"预期总记录数: {self.total_inserted}")
        
        if final_stb_count > 0 and final_tb_count > 0:
            self.logger.info("✅ 多副本强一致性测试通过")
        else:
            self.logger.error("❌ 多副本强一致性测试失败")

    def desc(self):
        return "3节点集群多副本强一致性测试：依次切换leader并验证数据一致性"

    def author(self):
        return "TDengine QA Team"

    def tags(self):
        return ["cluster", "replica", "consistency", "leader_switch"]

    def cleanup(self):
        try:
            self.execute_sql(f'drop database if exists {self.dbname}')
        except:
            pass

    def run(self):
        try:
            # 1. 初始化集群和数据库
            self.setup_cluster_and_database()
            
            # 2. 初始状态检查
            self.logger.info("=== 初始状态检查 ===")
            self.query_sql("show dnodes")
            self.query_sql("show mnodes") 
            self.query_sql("show vgroups")
            
            # 3. 运行完整测试周期
            self.run_full_test_cycle()
            
            # 4. 生成最终报告
            self.generate_final_report()
            
            # 5. 保存测试结果
            result_file = f'{self.run_log_dir}/json_log.json'
            self.tdCom.dump_json(result_file, self.json_log)
            
            self.logger.info(f"测试完成，详细结果保存到: {result_file}")
            
            # 打印关键JSON路径和内容
            print(f"\n=== JSON 文件路径信息 ===")
            print(f"测试结果文件: {result_file}")
            
            print(f"\n=== 关键测试结果 ===")
            if "测试总结报告" in self.json_log:
                import json
                summary = self.json_log["测试总结报告"]
                print("测试总结:")
                print(json.dumps(summary, indent=2, ensure_ascii=False))
            
        except Exception as e:
            self.logger.error(f"测试过程中发生错误: {e}")
            self.json_log["测试错误"] = str(e)
            raise e
