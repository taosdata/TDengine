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
import copy
import json
from taostest.util.common import TDCom
import datetime
from taostest import TDCase
from taostest.performance.result_reduction import Perf_Base_func
from taostest.util.remote import Remote
import time


class Demo(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        self.perf = Perf_Base_func(self._remote._logger, self.run_log_dir)
        
        # Prometheus环境设置
        try:
            self.prom_env_setting = self.get_component_by_name("prometheus")
        except Exception as e:
            self.logger.warning(f"Prometheus组件未配置或初始化失败: {e}")
            self.prom_env_setting = None
        
        # 基础配置
        self.env_root = os.path.join(os.environ["TEST_ROOT"], "env")
        self.json_file = os.path.join(self.env_root, "pocs/gyrx/case_18.json")
        
        # taosBenchmark配置
        self.taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
        self.taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")
        
        # 测试结果存储
        self.test_results = []
        
        # 性能测试配置
        self.test_scenarios = [
            # 场景1: 默认配置 + 不同thread_count
            {
                "name": "默认配置测试",
                "description": "使用默认interlace_rows=0，测试不同thread_count",
                "thread_counts": [10, 50, 100],  # 原始配置是10，然后测试50和100
                "interlace_rows": 0
            },
            # 场景2: interlace_rows=1 + 不同thread_count
            {
                "name": "交错写入测试", 
                "description": "使用interlace_rows=1，测试不同thread_count",
                "thread_counts": [10, 50, 100],
                "interlace_rows": 1
            }
        ]

    def load_json_config(self):
        """加载case_18.json配置"""
        try:
            with open(self.json_file, 'r', encoding='utf-8') as f:
                json_config = json.load(f)
            
            # 更新主机名为当前环境的主机名
            current_host = self.get_fqdn("taosd")[0]
            json_config["host"] = current_host
            
            # 确保所有必要字段存在
            if "test_log" not in json_config:
                json_config["test_log"] = "/root/testlog/"
            
            # 确保数据库配置为 drop: yes (避免冲突)
            if "databases" in json_config:
                for db in json_config["databases"]:
                    if "dbinfo" in db:
                        db["dbinfo"]["drop"] = "yes"
            
            self.logger.info(f"成功加载JSON配置文件: {self.json_file}")
            self.logger.info(f"更新主机名为: {current_host}")
            self.logger.info(f"测试日志目录: {json_config.get('test_log')}")
            return json_config
        except Exception as e:
            self.logger.error(f"加载JSON配置文件失败: {e}")
            raise

    def create_test_config(self, base_config, thread_count, interlace_rows):
        """创建测试配置"""
        # 深拷贝基础配置
        test_config = copy.deepcopy(base_config)
        
        # 更新线程数
        test_config["thread_count"] = thread_count
        
        # 更新超级表的interlace_rows
        if "databases" in test_config and len(test_config["databases"]) > 0:
            for db in test_config["databases"]:
                if "super_tables" in db and len(db["super_tables"]) > 0:
                    for stb in db["super_tables"]:
                        stb["interlace_rows"] = interlace_rows
        
        return test_config

    def run_performance_test(self, config, test_name, test_description):
        """运行单个性能测试"""
        self.logger.info(f"=== 开始性能测试: {test_name} ===")
        self.logger.info(f"测试描述: {test_description}")
        self.logger.info(f"配置详情: thread_count={config.get('thread_count')}, "
                        f"interlace_rows={self.get_interlace_rows_from_config(config)}")
        
        try:
            # 记录开始时间
            start_time = datetime.datetime.now()
            
            # 生成配置文件
            json_filename = f"case_18_{test_name.replace(' ', '_')}.json"
            json_filename_list = [json_filename]
            
            self.logger.info(f"生成JSON配置文件: {json_filename}")
            self.logger.info(f"配置内容检查: thread_count={config.get('thread_count')}, test_log={config.get('test_log')}")
            
            # 生成本地JSON文件
            self.tdCom.genBenchmarkJson(self.run_log_dir, json_filename, config)
            
            # 设置数据
            json_data_list = [config]
            
            # 上传文件并执行taosBenchmark
            self.tdCom.put_file(
                self._remote, 
                self.taosBenchmark_iplist, 
                json_data_list, 
                json_filename_list, 
                self.run_log_dir
            )
            
            result_filename = self.tdCom.threads_run_taosBenchmark(
                self._remote, 
                self.taosBenchmark_iplist, 
                json_data_list, 
                json_filename_list, 
                self.taosBenchmark_env_setting, 
                self.run_log_dir
            )
            
            # 记录结束时间
            end_time = datetime.datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            self.logger.info(f"性能测试完成: {test_name}, 耗时: {duration:.2f}秒")
            self.logger.info(f"结果文件: {result_filename}")
            
            # 存储测试结果
            test_result = {
                'name': test_name,
                'description': test_description,
                'config': config,
                'start_time': start_time,
                'end_time': end_time,
                'duration': duration,
                'result_files': result_filename,
                'thread_count': config.get('thread_count'),
                'interlace_rows': self.get_interlace_rows_from_config(config)
            }
            
            self.test_results.append(test_result)
            
            return test_result
            
        except Exception as e:
            self.logger.error(f"性能测试失败: {test_name}, 错误: {e}")
            return None

    def get_interlace_rows_from_config(self, config):
        """从配置中提取interlace_rows值"""
        try:
            if "databases" in config and len(config["databases"]) > 0:
                for db in config["databases"]:
                    if "super_tables" in db and len(db["super_tables"]) > 0:
                        return db["super_tables"][0].get("interlace_rows", 0)
            return 0
        except:
            return 0

    def run_all_performance_tests(self):
        """运行所有性能测试场景"""
        self.logger.info("=== 开始执行性能测试场景 ===")
        
        # 加载基础配置
        base_config = self.load_json_config()
        
        for scenario_idx, scenario in enumerate(self.test_scenarios, 1):
            self.logger.info(f"\n=== 场景 {scenario_idx}: {scenario['name']} ===")
            self.logger.info(f"场景描述: {scenario['description']}")
            
            # 获取场景配置
            interlace_rows = scenario['interlace_rows']
            thread_counts = scenario['thread_counts']
            
            for thread_count in thread_counts:
                # 创建测试配置
                test_config = self.create_test_config(base_config, thread_count, interlace_rows)
                
                # 构造测试名称
                test_name = f"Scenario{scenario_idx}_threads{thread_count}_interlace{interlace_rows}"
                test_description = f"{scenario['name']} - 线程数:{thread_count}, 交错行数:{interlace_rows}"
                
                # 运行测试
                result = self.run_performance_test(test_config, test_name, test_description)
                
                if result:
                    self.logger.info(f"✅ 测试成功: {test_name}")
            else:
                    self.logger.error(f"❌ 测试失败: {test_name}")
                
                # 在测试之间添加短暂延迟
                time.sleep(3)

    def generate_performance_summary(self):
        """生成性能测试总结"""
        if not self.test_results:
            self.logger.warning("没有测试结果可供总结")
            return
        
        self.logger.info("\n" + "="*80)
        self.logger.info("Case 18 性能测试总结报告")
        self.logger.info("="*80)
        
        # 按场景分组显示结果
        scenario1_results = [r for r in self.test_results if 'Scenario1' in r['name']]
        scenario2_results = [r for r in self.test_results if 'Scenario2' in r['name']]
        
        if scenario1_results:
            self.logger.info("\n【场景1: 默认配置测试 (interlace_rows=0)】")
            self.logger.info("-" * 60)
            for result in scenario1_results:
                self.logger.info(f"线程数: {result['thread_count']:3d} | "
                               f"耗时: {result['duration']:6.2f}秒 | "
                               f"状态: 成功")
        
        if scenario2_results:
            self.logger.info("\n【场景2: 交错写入测试 (interlace_rows=1)】")
            self.logger.info("-" * 60)
            for result in scenario2_results:
                self.logger.info(f"线程数: {result['thread_count']:3d} | "
                               f"耗时: {result['duration']:6.2f}秒 | "
                               f"状态: 成功")
        
        # 性能分析
        self.logger.info("\n【性能分析】")
        self.logger.info("-" * 40)
        
        if scenario1_results and scenario2_results:
            # 比较不同interlace_rows的性能
            for thread_count in [10, 50, 100]:
                s1_result = next((r for r in scenario1_results if r['thread_count'] == thread_count), None)
                s2_result = next((r for r in scenario2_results if r['thread_count'] == thread_count), None)
                
                if s1_result and s2_result:
                    diff_pct = ((s2_result['duration'] - s1_result['duration']) / s1_result['duration']) * 100
                    self.logger.info(f"线程数 {thread_count:3d}: "
                                   f"默认={s1_result['duration']:6.2f}s, "
                                   f"交错={s2_result['duration']:6.2f}s, "
                                   f"差异={diff_pct:+5.1f}%")
        
        self.logger.info("="*80)

    def save_results_to_file(self):
        """保存测试结果到文件"""
        try:
            results_file = os.path.join(self.run_log_dir, "case18_performance_results.json")
            
            # 转换datetime对象为字符串以便JSON序列化
            serializable_results = []
            for result in self.test_results:
                serializable_result = result.copy()
                serializable_result['start_time'] = result['start_time'].isoformat()
                serializable_result['end_time'] = result['end_time'].isoformat()
                serializable_results.append(serializable_result)
            
            with open(results_file, 'w', encoding='utf-8') as f:
                json.dump(serializable_results, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"测试结果已保存到: {results_file}")
            
            # 同时生成简单的总结文件
            summary_file = os.path.join(self.run_log_dir, "case18_performance_summary.txt")
            with open(summary_file, 'w', encoding='utf-8') as f:
                f.write("Case 18 性能测试总结\n")
                f.write("="*50 + "\n\n")
                
                scenario1_results = [r for r in self.test_results if 'Scenario1' in r['name']]
                scenario2_results = [r for r in self.test_results if 'Scenario2' in r['name']]
                
                if scenario1_results:
                    f.write("场景1: 默认配置测试 (interlace_rows=0)\n")
                    f.write("-" * 40 + "\n")
                    for result in scenario1_results:
                        f.write(f"线程数: {result['thread_count']:3d} | 耗时: {result['duration']:6.2f}秒\n")
                    f.write("\n")
                
                if scenario2_results:
                    f.write("场景2: 交错写入测试 (interlace_rows=1)\n")
                    f.write("-" * 40 + "\n")
                    for result in scenario2_results:
                        f.write(f"线程数: {result['thread_count']:3d} | 耗时: {result['duration']:6.2f}秒\n")
                    f.write("\n")
                
                if scenario1_results and scenario2_results:
                    f.write("性能对比分析\n")
                    f.write("-" * 20 + "\n")
                    for thread_count in [10, 50, 100]:
                        s1_result = next((r for r in scenario1_results if r['thread_count'] == thread_count), None)
                        s2_result = next((r for r in scenario2_results if r['thread_count'] == thread_count), None)
                        
                        if s1_result and s2_result:
                            diff_pct = ((s2_result['duration'] - s1_result['duration']) / s1_result['duration']) * 100
                            f.write(f"线程数 {thread_count:3d}: 默认={s1_result['duration']:6.2f}s, 交错={s2_result['duration']:6.2f}s, 差异={diff_pct:+5.1f}%\n")
            
            self.logger.info(f"总结报告已保存到: {summary_file}")
                        
        except Exception as e:
            self.logger.error(f"保存测试结果失败: {e}")

    def desc(self):
        return "Case 18 性能测试：使用不同thread_count和interlace_rows配置进行写入性能测试"

    def author(self):
        return "TAOS"

    def tags(self):
        return "性能测试,thread_count,interlace_rows,写入测试"

    def cleanup(self):
        pass

    def run(self):
        """主执行流程"""
        try:
            self.logger.info("=== Case 18 性能测试开始 ===")
            
            # 1. 运行所有性能测试场景
            self.logger.info("=== 步骤1: 执行性能测试 ===")
            self.run_all_performance_tests()
            
            # 2. 生成性能测试总结
            self.logger.info("=== 步骤2: 生成测试总结 ===")
            self.generate_performance_summary()
            
            # 3. 保存测试结果
            self.logger.info("=== 步骤3: 保存测试结果 ===")
            self.save_results_to_file()
            
            # 4. 生成性能报告
            self.logger.info("=== 步骤4: 生成性能报告 ===")
            
            # 为每次测试执行生成性能报告
            if self.test_results and len(self.test_results) > 0:
                self.logger.info("为每次测试执行生成性能报告...")
                
                for idx, test_result in enumerate(self.test_results, 1):
                    try:
                        result_file_name = os.path.join(self.run_log_dir, f'case18_perf_report_{idx}_{test_result["name"]}.txt')
                        timestamp_start = test_result['start_time'].strftime('%Y-%m-%d %H:%M:%S.%f')
                        timestamp_end = test_result['end_time'].strftime('%Y-%m-%d %H:%M:%S.%f')
                        
                        self.logger.info(f"生成第{idx}次测试性能报告: {test_result['name']}")
                        
                        # 创建性能报告内容
                        report_content = []
                        report_content.append(f"Case 18 性能测试报告 - 测试{idx}")
                        report_content.append("=" * 60)
                        report_content.append(f"测试名称: {test_result['name']}")
                        report_content.append(f"测试描述: {test_result['description']}")
                        report_content.append(f"线程数: {test_result['thread_count']}")
                        report_content.append(f"交错行数: {test_result['interlace_rows']}")
                        report_content.append(f"开始时间: {test_result['start_time']}")
                        report_content.append(f"结束时间: {test_result['end_time']}")
                        report_content.append(f"测试耗时: {test_result['duration']:.2f} 秒")
                        report_content.append("-" * 60)
                        
                        # 生成taosBenchmark插入汇总结果
                        if test_result.get('result_files'):
                            try:
                                self.perf.taosBenchmark_insert_summary_result(test_result['result_files'], version="3.0")
                                report_content.append("taosBenchmark 汇总结果已生成")
                                
                                # 读取性能报告文件内容
                                perf_report_file = os.path.join(self.run_log_dir, 'perf_report.txt')
                                if os.path.exists(perf_report_file):
                                    report_content.append("")
                                    report_content.append("taosBenchmark 性能汇总详情:")
                                    report_content.append("-" * 40)
                                    try:
                                        with open(perf_report_file, 'r', encoding='utf-8') as f:
                                            perf_lines = f.readlines()
                                            # 添加性能报告内容
                                            for line in perf_lines:
                                                report_content.append(line.rstrip())
                                    except Exception as read_e:
                                        report_content.append(f"读取性能报告失败: {read_e}")
                                else:
                                    report_content.append("性能报告文件未找到")
                                    
                            except Exception as bench_e:
                                report_content.append(f"taosBenchmark 汇总结果生成失败: {bench_e}")
                            
                        # 生成进程监控信息
                        try:
                            if hasattr(self, 'prom_env_setting') and self.prom_env_setting:
                                self.perf.get_process_exporter_info(self.prom_env_setting, 1, timestamp_start, timestamp_end)
                                self.perf.get_node_exporter_info(self.prom_env_setting, 1, timestamp_start, timestamp_end)
                                report_content.append("")
                                report_content.append("Prometheus 监控信息已生成")
                                
                                # 查找并读取Prometheus相关的输出文件
                                prom_files = []
                                for root, dirs, files in os.walk(self.run_log_dir):
                                    for file in files:
                                        if 'prometheus' in file.lower() or 'process' in file.lower() or 'node' in file.lower():
                                            prom_files.append(os.path.join(root, file))
                                
                                if prom_files:
                                    report_content.append("Prometheus 监控详情:")
                                    report_content.append("-" * 40)
                                    for prom_file in prom_files[:3]:  # 只显示前3个文件，避免过长
                                        try:
                                            report_content.append(f"文件: {os.path.basename(prom_file)}")
                                            with open(prom_file, 'r', encoding='utf-8') as f:
                                                lines = f.readlines()
                                                # 只显示前20行，避免内容过长
                                                for line in lines[:20]:
                                                    report_content.append(f"  {line.rstrip()}")
                                                if len(lines) > 20:
                                                    report_content.append(f"  ... (还有 {len(lines)-20} 行)")
                                            report_content.append("")
                                        except Exception as read_e:
                                            report_content.append(f"读取监控文件失败: {read_e}")
                                else:
                                    report_content.append("Prometheus 监控文件未找到")
                            else:
                                report_content.append("Prometheus 未配置，跳过监控信息生成")
                        except Exception as prom_e:
                            report_content.append(f"Prometheus监控信息生成失败: {prom_e}")
                        
                        report_content.append("=" * 60)
                        
                        # 写入报告文件
                        with open(result_file_name, 'w', encoding='utf-8') as f:
                            f.write('\n'.join(report_content))
                        
                        self.logger.info(f"第{idx}次测试性能报告生成成功: {result_file_name}")
                        print(f"测试{idx} [{test_result['name']}] 性能报告: {result_file_name}")
                            
                    except Exception as e:
                        self.logger.warning(f"第{idx}次测试性能报告生成失败: {e}")
                
                # 生成综合性能报告
                try:
                    result_file_name = os.path.join(self.run_log_dir, 'case18_perf_report_summary.txt')
                    timestamp_start = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                    timestamp_end = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                    
                    self.logger.info("生成综合性能报告...")
                    
                    # 创建综合报告内容
                    summary_content = []
                    summary_content.append("Case 18 综合性能测试报告")
                    summary_content.append("=" * 80)
                    summary_content.append(f"测试时间: {datetime.datetime.now()}")
                    summary_content.append(f"总测试数量: {len(self.test_results)}")
                    summary_content.append("")
                    
                    # 按场景分组显示结果
                    scenario1_results = [r for r in self.test_results if 'Scenario1' in r['name']]
                    scenario2_results = [r for r in self.test_results if 'Scenario2' in r['name']]
                    
                    if scenario1_results:
                        summary_content.append("场景1: 默认配置测试 (interlace_rows=0)")
                        summary_content.append("-" * 60)
                        for result in scenario1_results:
                            summary_content.append(f"线程数: {result['thread_count']:3d} | "
                                                  f"耗时: {result['duration']:6.2f}秒 | "
                                                  f"状态: 成功")
                        summary_content.append("")
                    
                    if scenario2_results:
                        summary_content.append("场景2: 交错写入测试 (interlace_rows=1)")
                        summary_content.append("-" * 60)
                        for result in scenario2_results:
                            summary_content.append(f"线程数: {result['thread_count']:3d} | "
                                                  f"耗时: {result['duration']:6.2f}秒 | "
                                                  f"状态: 成功")
                        summary_content.append("")
                    
                    # 性能分析
                    if scenario1_results and scenario2_results:
                        summary_content.append("性能对比分析")
                        summary_content.append("-" * 40)
                        for thread_count in [10, 50, 100]:
                            s1_result = next((r for r in scenario1_results if r['thread_count'] == thread_count), None)
                            s2_result = next((r for r in scenario2_results if r['thread_count'] == thread_count), None)
                            
                            if s1_result and s2_result:
                                diff_pct = ((s2_result['duration'] - s1_result['duration']) / s1_result['duration']) * 100
                                summary_content.append(f"线程数 {thread_count:3d}: "
                                                      f"默认={s1_result['duration']:6.2f}s, "
                                                      f"交错={s2_result['duration']:6.2f}s, "
                                                      f"差异={diff_pct:+5.1f}%")
                        summary_content.append("")
                    
                    # 使用最后一个测试的结果文件作为综合报告基础
                    last_result = self.test_results[-1]
                    if last_result.get('result_files'):
                        try:
                            self.perf.taosBenchmark_insert_summary_result(last_result['result_files'], version="3.0")
                            summary_content.append("taosBenchmark 综合汇总结果已生成")
                        except Exception as bench_e:
                            summary_content.append(f"taosBenchmark 综合汇总结果生成失败: {bench_e}")
                    
                    # 添加监控信息
                    try:
                        if hasattr(self, 'prom_env_setting') and self.prom_env_setting:
                            self.perf.get_process_exporter_info(self.prom_env_setting, 1, timestamp_start, timestamp_end)
                            self.perf.get_node_exporter_info(self.prom_env_setting, 1, timestamp_start, timestamp_end)
                            summary_content.append("Prometheus 综合监控信息已生成")
                        else:
                            summary_content.append("Prometheus 未配置，跳过监控信息生成")
                    except Exception as prom_e:
                        summary_content.append(f"综合报告Prometheus监控信息生成失败: {prom_e}")
                    
                    summary_content.append("=" * 80)
                    
                    # 写入综合报告文件
                    with open(result_file_name, 'w', encoding='utf-8') as f:
                        f.write('\n'.join(summary_content))
                    
                    self.logger.info("综合性能报告生成成功")
                    print(f"Case 18 综合性能报告: {result_file_name}")
                    
                except Exception as e:
                    self.logger.warning(f"综合性能报告生成失败: {e}")
                    else:
                self.logger.warning("没有测试结果，跳过性能报告生成")
            
            self.logger.info("=== Case 18 性能测试完成 ===")
                
        except Exception as e:
            self.logger.error(f"Case 18 性能测试执行失败: {e}")
            raise