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
import time
import threading
import subprocess
import json
import datetime
from collections import defaultdict
from taostest.util.common import TDCom
from taostest import TDCase
from taostest.util.remote import Remote


class Demo(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self._remote: Remote = Remote(self.logger)
        
        # 基础配置
        self.env_root = os.path.join(os.environ["TEST_ROOT"], "env")
        self.json_file = os.path.join(self.env_root, "pocs/gyrx/test.json")
        self.consumer_dir = "/root/sub"
        self.consumer_bin = os.path.join(self.consumer_dir, "bin/start.sh")
        self.consumer_stop = os.path.join(self.consumer_dir, "bin/mystop.sh")
        self.data_file = os.path.join(self.consumer_dir, "data.txt")
        
        # taosBenchmark配置
        self.taosBenchmark_iplist = self.get_fqdn("taosBenchmark")
        self.taosBenchmark_env_setting = self.get_component_by_name("taosBenchmark")

        # 控制变量
        self.insert_started = False
        self.topic_created = False
        self.consumer_started = False
        self.should_stop = False
        
        # 统计变量
        self.json_log = {}
        self.insert_count = 0
        self.stats_per_second = defaultdict(int)
        
        # 消费控制参数
        self.max_consumption_records = 5000000  # 最大消费500万条记录

    def set_max_consumption_records(self, max_records):
        """设置最大消费记录数"""
        self.max_consumption_records = max_records
        self.logger.info(f"设置最大消费记录数为: {max_records:,} 条")

    def load_json_config(self):
        """加载test.json配置"""
        try:
            with open(self.json_file, 'r', encoding='utf-8') as f:
                json_config = json.load(f)
            
            # 更新主机名为当前环境的主机名
            current_host = self.get_fqdn("taosd")[0]
            json_config["host"] = current_host
            
            self.logger.info(f"成功加载JSON配置文件: {self.json_file}")
            self.logger.info(f"更新主机名为: {current_host}")
            self.logger.info(f"当前最大消费记录数设置: {self.max_consumption_records:,} 条")
            return json_config
        except Exception as e:
            self.logger.error(f"加载JSON配置文件失败: {e}")
            raise

    def execute_sql(self, sql):
        """执行SQL语句"""
        try:
        self.tdSql.execute(sql)
        self.json_log[sql] = '执行成功'
            self.logger.info(f"SQL执行成功: {sql}")
        except Exception as e:
            self.json_log[sql] = f'执行失败: {str(e)}'
            self.logger.error(f"SQL执行失败: {sql}, 错误: {e}")
            raise

    def start_data_insert(self, json_config):
        """启动数据插入线程"""
        def insert_worker():
            try:
                self.logger.info("开始执行数据插入...")
                self.insert_started = True
                
                # 生成JSON文件名
                json_filename = "gyrx_21_insert.json"
                json_filename_list = [json_filename]
                
                # 先生成本地JSON文件
                self.tdCom.genBenchmarkJson(self.run_log_dir, json_filename, json_config)
                
                # 设置数据
                json_data_list = [json_config]
                
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
                
                self.logger.info(f"数据插入完成，结果文件: {result_filename}")
                
            except Exception as e:
                self.logger.error(f"数据插入失败: {e}")
                raise
        
        # 启动插入线程
        insert_thread = threading.Thread(target=insert_worker)
        insert_thread.daemon = True
        insert_thread.start()
        
        return insert_thread

    def create_topic_after_delay(self, dbname, topic_name="subscribedemo", delay=5):
        """延迟创建topic"""
        def topic_worker():
            # 等待插入开始
            while not self.insert_started:
                time.sleep(0.1)
            
            # 延迟指定时间
            self.logger.info(f"数据插入已开始，{delay}秒后创建topic...")
            time.sleep(delay)
            
            try:
                # 创建topic
                create_topic_sql = f"CREATE TOPIC {topic_name} AS SELECT * FROM {dbname}.stb"
                self.execute_sql(create_topic_sql)
                self.topic_created = True
                self.logger.info(f"Topic '{topic_name}' 创建成功")
                
            except Exception as e:
                self.logger.error(f"创建topic失败: {e}")
                raise
        
        # 启动topic创建线程
        topic_thread = threading.Thread(target=topic_worker)
        topic_thread.daemon = True
        topic_thread.start()
        
        return topic_thread

    def start_consumer(self):
        """启动消费程序"""
        def consumer_worker():
            # 等待topic创建
            while not self.topic_created:
                time.sleep(0.1)
            
            try:
                self.logger.info("启动消费程序...")
                
                # 清理之前的数据文件
                if os.path.exists(self.data_file):
                    os.remove(self.data_file)
                    self.logger.info(f"清理旧的数据文件: {self.data_file}")
                
                # 启动消费程序 - 使用nohup确保后台运行
                cmd = f"cd {self.consumer_dir} && nohup bash {self.consumer_bin} > consumer.log 2>&1 &"
                result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                
                self.consumer_started = True
                self.logger.info(f"消费程序启动命令执行完成: {cmd}")
                self.logger.info(f"启动结果: stdout={result.stdout}, stderr={result.stderr}")
                
                # 等待一下让消费程序完全启动
                time.sleep(10)
                
                # 动态监控消费进度（不依赖进程对象）
                self.monitor_consumption_progress_without_process()
                
            except Exception as e:
                self.logger.error(f"启动消费程序失败: {e}")
                raise
        
        # 启动消费线程
        consumer_thread = threading.Thread(target=consumer_worker)
        consumer_thread.daemon = True
        consumer_thread.start()
        
        return consumer_thread

    def monitor_consumption_progress(self, process):
        """监控消费进度"""
        last_count = 0
        no_progress_count = 0
        max_no_progress = 12  # 最多120秒无进展就停止（每次检查10秒）
        check_count = 0
        
        self.logger.info(f"开始监控消费进度... (目标: {self.max_consumption_records:,} 条记录)")
        
        while True:
            # 等待10秒
            time.sleep(10)
            check_count += 1
            
            # 检查进程是否还在运行
            poll_result = process.poll()
            if poll_result is not None:
                self.logger.info(f"消费程序进程已结束 (返回码: {poll_result})")
                # 进程结束后再检查一次消费数量
                final_count = self.get_current_consumption_count()
                self.logger.info(f"进程结束时消费数量: {final_count:,} 条")
                break
            
            # 检查当前消费数量
            current_count = self.get_current_consumption_count()
            self.logger.debug(f"第{check_count}次检查，当前消费数量: {current_count}")
            
            # 检查是否达到目标记录数
            if current_count >= self.max_consumption_records:
                self.logger.info(f"✅ 已达到目标消费数量: {current_count:,} 条 (目标: {self.max_consumption_records:,} 条)")
                self.logger.info("停止消费程序...")
                break
            
            if current_count > last_count:
                # 有进展，重置无进展计数器
                progress_pct = (current_count / self.max_consumption_records) * 100
                self.logger.info(f"消费进度: {current_count:,} 条 (新增 {current_count - last_count:,} 条) [{progress_pct:.1f}%]")
                last_count = current_count
                no_progress_count = 0
            else:
                # 无进展，增加计数器
                no_progress_count += 1
                progress_pct = (current_count / self.max_consumption_records) * 100
                self.logger.info(f"消费进度: {current_count:,} 条 (无新增，等待 {no_progress_count}/{max_no_progress}) [{progress_pct:.1f}%]")
                
                # 如果连续无进展时间过长，停止消费
                if no_progress_count >= max_no_progress:
                    self.logger.info("消费已完成或无新数据，停止消费程序")
                    break
                    
            # 检查是否消费程序异常（很长时间都没有任何消费）
            if check_count > 30 and current_count == 0:  # 5分钟还没有开始消费
                self.logger.warning("消费程序可能异常，5分钟内没有开始消费数据")
                break
        
        # 停止消费程序
        self.stop_consumer()

    def monitor_consumption_progress_without_process(self):
        """监控消费进度（不依赖进程对象）"""
        last_count = 0
        no_progress_count = 0
        max_no_progress = 12  # 最多120秒无进展就停止（每次检查10秒）
        check_count = 0
        
        self.logger.info(f"开始监控消费进度... (目标: {self.max_consumption_records:,} 条记录)")
        
        while True:
            # 等待10秒
            time.sleep(10)
            check_count += 1
            
            # 检查消费程序是否还在运行
            is_running = self.check_consumer_process_running()
            
            # 检查当前消费数量
            current_count = self.get_current_consumption_count()
            self.logger.info(f"第{check_count}次检查，消费数量: {current_count:,}，进程运行: {is_running}")
            
            # 检查是否达到目标记录数
            if current_count >= self.max_consumption_records:
                self.logger.info(f"✅ 已达到目标消费数量: {current_count:,} 条 (目标: {self.max_consumption_records:,} 条)")
                self.logger.info("停止消费程序...")
                break
            
            if current_count > last_count:
                # 有进展，重置无进展计数器
                progress_pct = (current_count / self.max_consumption_records) * 100
                self.logger.info(f"消费进度: {current_count:,} 条 (新增 {current_count - last_count:,} 条) [{progress_pct:.1f}%]")
                last_count = current_count
                no_progress_count = 0
            else:
                # 无进展，增加计数器
                no_progress_count += 1
                progress_pct = (current_count / self.max_consumption_records) * 100
                self.logger.info(f"消费进度: {current_count:,} 条 (无新增，等待 {no_progress_count}/{max_no_progress}) [{progress_pct:.1f}%]")
                
                # 如果消费程序已停止且无进展，提前退出
                if not is_running and no_progress_count >= 3:
                    self.logger.info("消费程序已停止且无新数据，结束监控")
                    break
                
                # 如果连续无进展时间过长，停止消费
                if no_progress_count >= max_no_progress:
                    self.logger.info("消费已完成或无新数据，停止消费程序")
                    break
                    
            # 检查是否消费程序异常（很长时间都没有任何消费）
            if check_count > 6 and current_count == 0:  # 1分钟还没有开始消费
                self.logger.warning("消费程序可能异常，1分钟内没有开始消费数据")
                if not is_running:
                    self.logger.error("消费程序进程已停止，无法继续消费")
                    break
        
        # 停止消费程序
        self.stop_consumer()

    def check_consumer_process_running(self):
        """检查消费程序进程是否还在运行"""
        try:
            result = subprocess.run(
                "pgrep -f 'subscribeDemo-java'", 
                shell=True, 
                capture_output=True, 
                text=True
            )
            return result.returncode == 0 and result.stdout.strip()
        except Exception as e:
            self.logger.warning(f"检查消费程序进程失败: {e}")
            return False

    def get_current_consumption_count(self):
        """获取当前消费数量"""
        try:
            if not os.path.exists(self.data_file):
                return 0
            
            with open(self.data_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # 统计非空行数
            count = sum(1 for line in lines if line.strip())
            return count
            
        except Exception as e:
            self.logger.warning(f"读取消费文件失败: {e}")
            return 0

    def get_database_record_count(self, dbname):
        """获取数据库中的总记录数"""
        try:
            self.tdSql.query(f"SELECT COUNT(*) FROM {dbname}.stb")
            result = self.tdSql.queryData
            if result and len(result) > 0:
                return result[0][0]
            return 0
        except Exception as e:
            self.logger.warning(f"获取数据库记录数失败: {e}")
            return 0

    def stop_consumer(self):
        """停止消费程序"""
        try:
            self.logger.info("停止消费程序...")
            cmd = f"cd {self.consumer_dir} && bash {self.consumer_stop}"
            subprocess.run(cmd, shell=True, timeout=10)
            self.logger.info("消费程序已停止")
        except Exception as e:
            self.logger.error(f"停止消费程序失败: {e}")

    def analyze_consumption_data(self):
        """分析消费数据，统计每秒写入条数"""
        try:
            if not os.path.exists(self.data_file):
                self.logger.warning(f"数据文件不存在: {self.data_file}")
                return {}
            
            self.logger.info(f"开始分析消费数据文件: {self.data_file}")
            
            # 读取数据文件
            with open(self.data_file, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            self.logger.info(f"数据文件总行数: {len(lines)}")
            
            # 统计每秒的数据条数
            second_counts = defaultdict(int)
            total_records = 0
            
            for line in lines:
                if line.strip():
                    total_records += 1
                    
                    # 解析时间戳 (假设数据格式包含时间戳)
                    try:
                        # 从行中提取时间戳 (根据实际数据格式调整)
                        parts = line.split('\t')
                        if len(parts) > 0:
                            # 假设第一列是时间戳
                            timestamp_str = parts[0]
                            
                            # 尝试解析时间戳
                            if timestamp_str.isdigit():
                                # 毫秒时间戳
                                timestamp = int(timestamp_str)
                                if timestamp > 1000000000000:  # 毫秒时间戳
                                    dt = datetime.datetime.fromtimestamp(timestamp / 1000)
                                else:  # 秒时间戳
                                    dt = datetime.datetime.fromtimestamp(timestamp)
                            else:
                                # 尝试解析ISO格式时间戳
                                dt = datetime.datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                            
                            # 按秒统计
                            second_key = dt.strftime('%Y-%m-%d %H:%M:%S')
                            second_counts[second_key] += 1
                            
                    except Exception as e:
                        # 如果解析时间戳失败，按当前时间统计
                        current_second = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        second_counts[current_second] += 1
            
            # 输出统计结果
            self.logger.info(f"消费数据分析完成:")
            self.logger.info(f"总记录数: {total_records}")
            self.logger.info(f"统计时间段: {len(second_counts)} 秒")
            
            # 显示每秒统计
            for second, count in sorted(second_counts.items()):
                self.logger.info(f"  {second}: {count} 条/秒")
            
            # 计算平均每秒写入条数
            if second_counts:
                avg_per_second = total_records / len(second_counts)
                self.logger.info(f"平均每秒写入条数: {avg_per_second:.2f}")
            
            return {
                'total_records': total_records,
                'time_periods': len(second_counts),
                'per_second_stats': dict(second_counts),
                'avg_per_second': avg_per_second if second_counts else 0
            }
            
        except Exception as e:
            self.logger.error(f"分析消费数据失败: {e}")
            return {}

    def desc(self):
        return "测试TMQ消息队列：读取test.json写入数据，延迟5秒创建topic，启动消费程序，统计每秒写入条数"

    def author(self):
        return "TAOS"

    def tags(self):
        return "TMQ,消息队列,消费者,性能统计"

    def cleanup(self):
        """清理资源"""
        try:
            # 停止消费程序
            self.stop_consumer()
            
            # 清理topic
            if self.topic_created:
                try:
                    self.execute_sql("DROP TOPIC IF EXISTS subscribedemo")
                except:
                    pass
            
            # 清理数据库
            try:
                self.execute_sql("DROP DATABASE IF EXISTS test")
            except:
                pass
                
        except Exception as e:
            self.logger.error(f"清理资源失败: {e}")

    def run(self):
        """主执行流程"""
        try:
            self.logger.info("开始执行TMQ消息队列测试...")
            
            # 1. 读取test.json配置
            json_config = self.load_json_config()
            dbname = json_config['databases'][0]['dbinfo']['name']
            
            # 2. 启动数据插入线程
            insert_thread = self.start_data_insert(json_config)
            
            # 3. 启动延迟创建topic线程
            topic_thread = self.create_topic_after_delay(dbname)
            
            # 4. 启动消费程序线程
            consumer_thread = self.start_consumer()
            
            # 5. 等待所有线程完成
            self.logger.info("等待所有线程完成...")
            
            # 等待插入完成
            insert_thread.join(timeout=120)  # 最多等待2分钟
            
            # 等待topic创建
            topic_thread.join(timeout=30)
            
            # 等待消费完成（增加超时时间，因为1000万数据需要更长时间）
            consumer_thread.join(timeout=600)  # 等待最多10分钟
            
            # 6. 分析消费数据
            time.sleep(5)  # 等待数据写入完成
            stats = self.analyze_consumption_data()
            
            # 7. 获取数据库总记录数进行对比
            db_count = self.get_database_record_count(dbname)
            
            # 8. 输出最终结果
            self.logger.info("=== 测试执行完成 ===")
            self.logger.info(f"插入状态: {'已开始' if self.insert_started else '未开始'}")
            self.logger.info(f"Topic状态: {'已创建' if self.topic_created else '未创建'}")
            self.logger.info(f"消费状态: {'已启动' if self.consumer_started else '未启动'}")
            self.logger.info(f"数据库总记录数: {db_count}")
            
            if stats:
                self.logger.info(f"消费统计: 总计 {stats['total_records']:,} 条记录")
                self.logger.info(f"平均每秒: {stats['avg_per_second']:.2f} 条")
                
                # 计算相对于目标数量的完成率
                target_completion_rate = (stats['total_records'] / self.max_consumption_records) * 100
                self.logger.info(f"目标完成率: {target_completion_rate:.2f}% ({stats['total_records']:,}/{self.max_consumption_records:,})")
                
                # 计算相对于数据库总数的完成率
                if db_count > 0:
                    db_completion_rate = (stats['total_records'] / db_count) * 100
                    self.logger.info(f"数据库完成率: {db_completion_rate:.2f}% ({stats['total_records']:,}/{db_count:,})")
                
                # 判断消费状态
                if stats['total_records'] >= self.max_consumption_records:
                    self.logger.info("✅ 已达到目标消费数量！")
                elif target_completion_rate >= 95:  # 允许一些误差
                    self.logger.info("✅ 基本达到目标消费数量！")
                else:
                    remaining = self.max_consumption_records - stats['total_records']
                    self.logger.warning(f"⚠️ 未达到目标！还需消费 {remaining:,} 条记录")
            
            # 打印执行日志
            print("=== SQL执行日志 ===")
            for sql, result in self.json_log.items():
                print(f"{sql}: {result}")
            
            return stats
            
        except Exception as e:
            self.logger.error(f"测试执行失败: {e}")
            raise
        finally:
            # 执行清理
            self.cleanup()
            print("cleanup")
