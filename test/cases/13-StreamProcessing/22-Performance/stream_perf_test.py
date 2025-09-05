import json
import subprocess
import threading
import psutil
import time
import taos
import shutil
import argparse
import os
import signal
import sys
import datetime
import itertools

"""TDengine New Stream Computing Performance Test
TDengine 新数据流计算性能测试工具

Purpose/用途:
    TDengine 流计算性能测试工具，支持多种测试模式和流计算类型。
    提供完整的数据生成、流计算创建、性能监控、延迟检测等功能。
    适用于流计算性能基准测试、系统调优、压力测试等场景。

Catalog/目录:
    - Performance Testing / 性能测试
    - Stream Computing / 流计算  
    - Real-time Analytics / 实时分析
    - Benchmarking / 基准测试

Features/功能:
    ✨ 流计算性能测试
        - 支持时间窗口、滑动窗口、会话窗口、计数窗口、事件窗口、状态窗口、定时触发等7种窗口类型
        - 支持超级表和子表数据源，支持按子表名和tag分组
        - 支持聚合查询和投影查询两种计算模式
        - 支持单流和多流并发测试
        
    📊 实时性能监控
        - 系统资源监控：CPU、内存、磁盘I/O实时监控
        - 流计算延迟监控：源表与目标表数据延迟检测
        - 动态阈值告警：基于检查间隔的智能延迟分级
        - 详细性能报告：图表化展示和问题分析建议
        
    🗄️ 数据管理
        - 自动数据生成：支持可配置的表数量、记录数、乱序率
        - 数据备份恢复：支持完整的数据备份和快速恢复
        - 历史数据测试：基于已有数据进行流计算性能测试
        - 增量数据写入：模拟实时数据流场景
        
    🏗️ 部署架构
        - 单节点模式：适用于开发测试和小规模验证
        - 集群模式：支持3节点集群的高可用测试
        - 自动环境准备：自动配置TDengine集群环境
        - 灵活参数调优：支持vgroups、调试级别等参数调整
        
Test Scenarios/测试场景:
    🚀 基础流计算测试
        - 创建测试数据并执行实时流计算
        - 持续数据写入，观察流计算延迟变化
        - 适用于验证流计算基本功能和性能
        
    📈 压力测试
        - 大数据量下的流计算性能测试
        - 多流并发处理能力测试  
        - 系统资源使用情况分析
        
    🔍 延迟测试
        - 流计算实时性验证
        - 延迟分布统计和分析
        - 延迟阈值告警测试
        
    🏋️ 历史数据测试
        - 基于已有大数据集的流计算测试
        - 避免重复数据生成，快速验证不同流SQL
        - 适用于算法验证和性能对比

Window Types/窗口类型:
    📊 时间窗口 (INTERVAL SLIDING WINDOW)
        - 固定时间间隔的计算
        - 适用于连续数据的平滑统计
        
    📊 滑动窗口 (SLIDING WINDOW)
        - 固定时间间隔的滑动计算
        - 适用于连续数据的平滑统计
        
    🔗 会话窗口 (SESSION WINDOW)  
        - 基于数据间隔的动态窗口
        - 适用于用户会话分析
        
    📝 计数窗口 (COUNT WINDOW)
        - 基于记录数量的固定窗口
        - 适用于批量数据处理
        
    ⚡ 事件窗口 (EVENT WINDOW)
        - 基于事件条件的动态窗口
        - 适用于异常检测和模式识别
        
    📍 状态窗口 (STATE WINDOW)
        - 基于状态变化的窗口划分
        - 适用于状态机分析
        
    ⏰ 定时触发 (PERIOD TRIGGER)
        - 定时执行的计算触发
        - 适用于定期报表生成

Query Types/查询类型:
    📊 聚合查询 (AGG): avg(), max(), min() 等统计函数
    📋 投影查询 (SELECT): 字段选择和投影操作

Data Sources/数据源:
    🏢 超级表 (STB): stream_from.stb - 适用于大规模数据查询
    📄 子表 (TB): stream_from.ctb0_X - 适用于单表性能测试

Partitioning/分组方式:
    📂 无分组 (NONE): 全表统计
    🏷️ 按子表名 (BY_TBNAME): partition by tbname
    🔖 按标签 (BY_TAG): partition by tag

Performance Monitoring/性能监控:
    💻 系统资源: CPU使用率、内存占用、磁盘I/O
    ⏱️ 延迟监控: 源表与目标表的时间差分析
    📈 实时统计: 流计算处理速度和效率
    🚨 智能告警: 延迟阈值告警和性能建议

Requirements/系统要求:
    TDengine: v3.3.7.0+
    Python: 3.7+
    依赖包: taos, psutil, json

Authors/作者:
    Guo Xiangyang / 郭向阳

Labels/标签: 
    performance, stream, testing

History/历史:
    - 2025-08-15 Initial commit
                 首次提交

Usage Examples/使用示例:

    # 快速开始 - 基础流计算测试
    python3 stream_perf_test.py --stream-perf-test-dir /home/stream_perf_test_dir -m 1 --table-count 1000 --time 30
    
    # 创建并备份测试数据  
    python3 stream_perf_test.py --stream-perf-test-dir /home/stream_perf_test_dir --create-data --table-count 5000 --histroy-rows 1000
    
    # 恢复数据并测试特定流计算
    python3 stream_perf_test.py --stream-perf-test-dir /home/stream_perf_test_dir -m 3 --sql-type intervalsliding_stb_partition_by_tbname --time 60
    
    # 多流并发测试
    python3 stream_perf_test.py --stream-perf-test-dir /home/stream_perf_test_dir -m 1 --sql-type intervalsliding_stb --stream-num 100 --time 30
    
    # 延迟监控测试
    python3 stream_perf_test.py --stream-perf-test-dir /home/stream_perf_test_dir -m 1 --check-stream-delay --delay-check-interval 5 --time 60
    
    # 集群模式测试
    python3 stream_perf_test.py --stream-perf-test-dir /home/stream_perf_test_dir -m 1 --deployment-mode cluster --vgroups 20 --time 30
    
    # 压力控制测试
    python3 stream_perf_test.py --stream-perf-test-dir /home/stream_perf_test_dir -m 1 --real-time-batch-sleep 10 --real-time-batch-rows 500


"""

# ========== 全局颜色定义 ==========
class Colors:
    """终端颜色常量定义"""
    # 基础颜色
    RED = '\033[91m'        # 红色 - 错误、严重警告
    GREEN = '\033[92m'      # 绿色 - 成功、正常状态
    YELLOW = '\033[93m'     # 黄色 - 警告、注意
    BLUE = '\033[94m'       # 蓝色 - 信息、详情
    PURPLE = '\033[95m'     # 紫色 - 标题、重要信息
    CYAN = '\033[96m'       # 青色 - 时间戳、数据
    WHITE = '\033[97m'      # 白色 - 普通文本
    
    # 特殊格式
    BOLD = '\033[1m'        # 粗体
    UNDERLINE = '\033[4m'   # 下划线
    BLINK = '\033[5m'       # 闪烁
    REVERSE = '\033[7m'     # 反色
    
    # 背景色
    BG_RED = '\033[41m'     # 红色背景
    BG_GREEN = '\033[42m'   # 绿色背景
    BG_YELLOW = '\033[43m'  # 黄色背景
    BG_BLUE = '\033[44m'    # 蓝色背景
    
    # 结束符
    END = '\033[0m'         # 重置所有格式
    
    @staticmethod
    def supports_color():
        """检测终端是否支持颜色输出"""
        return (
            hasattr(os.sys.stdout, 'isatty') and os.sys.stdout.isatty() and
            os.environ.get('TERM') != 'dumb' and
            os.environ.get('NO_COLOR') is None
        )
    
    @staticmethod
    def get_colors():
        """获取颜色对象，如果不支持颜色则返回空字符串"""
        if Colors.supports_color():
            return Colors
        else:
            # 创建一个所有颜色都为空字符串的类
            class NoColors:
                RED = GREEN = YELLOW = BLUE = PURPLE = CYAN = WHITE = ''
                BOLD = UNDERLINE = BLINK = REVERSE = ''
                BG_RED = BG_GREEN = BG_YELLOW = BG_BLUE = ''
                END = ''
            return NoColors


# ========== 颜色打印辅助函数 ==========
def print_success(message):
    """打印成功消息"""
    c = Colors.get_colors()
    print(f"{c.GREEN}✓ {message}{c.END}")

def print_warning(message):
    """打印警告消息"""
    c = Colors.get_colors()
    print(f"{c.YELLOW}⚠ {message}{c.END}")

def print_error(message):
    """打印错误消息"""
    c = Colors.get_colors()
    print(f"{c.RED}✗ {message}{c.END}")

def print_info(message):
    """打印信息消息"""
    c = Colors.get_colors()
    print(f"{c.BLUE}ℹ {message}{c.END}")

def print_title(message):
    """打印标题消息"""
    c = Colors.get_colors()
    print(f"{c.BOLD}{c.PURPLE}{message}{c.END}")

def print_subtitle(message):
    """打印副标题消息"""
    c = Colors.get_colors()
    print(f"{c.CYAN}{message}{c.END}")

def print_timestamp(message):
    """打印带时间戳的消息"""
    c = Colors.get_colors()
    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"{c.CYAN}[{timestamp}]{c.END} {message}")

def print_separator(char='-', length=80, color=None):
    """打印分隔线"""
    c = Colors.get_colors()
    if color:
        print(f"{getattr(c, color.upper(), '')}{char * length}{c.END}")
    else:
        print(char * length)
        
        
class MonitorSystemLoad:

    def __init__(self, name_pattern, count, perf_file='/tmp/perf.log', use_signal=True, interval=1, deployment_mode='cluster', warm_up_time=0) -> None:
        """初始化系统负载监控
        
        Args:
            name_pattern: 进程名模式,例如 'taosd.*dnode1/conf'
            count: 监控次数
            perf_file: 性能数据输出文件
            interval: 性能采集间隔(秒),默认1秒
            deployment_mode: 部署模式 'single' 或 'cluster'
            warm_up_time: 预热时间(秒),在此期间收集数据但不打印,默认0秒
        """
        self.name_pattern = name_pattern  # 保存进程名模式
        self.count = count
        self.perf_file = perf_file
        self.interval = interval
        self.deployment_mode = deployment_mode
        self.warm_up_time = warm_up_time
        
        # 添加唯一标识符
        import uuid
        self.instance_id = str(uuid.uuid4())[:8]
        self.thread_name = f"MonitorSystemLoad-{self.instance_id}"
        
        print(f"调试: MonitorSystemLoad 初始化，实例ID = {self.instance_id}，perf_file = {perf_file}")
        print(f"调试: 预热等待时间 = {warm_up_time}秒")        
        
        self.stop_monitoring = False
        self._should_stop = False
        self._is_running = False
        self._force_stop = False
        self._stop_event = threading.Event()
        
        # 根据部署模式确定需要监控的节点
        if deployment_mode == 'single':
            self.monitor_nodes = ['dnode1']
            print(f"单节点模式: 仅监控 dnode1")
        else:
            self.monitor_nodes = ['dnode1', 'dnode2', 'dnode3']
            print(f"集群模式: 监控 dnode1, dnode2, dnode3")
        
        # 为每个dnode创建对应的性能文件句柄
        self.perf_files = {}
        for dnode in self.monitor_nodes:
            # 基于传入的perf_file路径生成每个节点的文件路径
            base_dir = os.path.dirname(perf_file)
            base_name = os.path.splitext(os.path.basename(perf_file))[0]
            file_path = os.path.join(base_dir, f"{base_name}-{dnode}.log")
            
            try:
                # 确保目录存在
                os.makedirs(base_dir, exist_ok=True)
                self.perf_files[dnode] = open(file_path, 'w+')
                print(f"创建性能日志文件: {file_path}")
            except Exception as e:
                print(f"创建日志文件失败 {file_path}: {str(e)}")
                
        # 创建汇总日志文件
        try:
            base_dir = os.path.dirname(perf_file)
            base_name = os.path.splitext(os.path.basename(perf_file))[0]
            all_file = os.path.join(base_dir, f"{base_name}-all.log")
            
            # 确保目录存在
            os.makedirs(base_dir, exist_ok=True)
            self.perf_files['all'] = open(all_file, 'w+')
            print(f"创建汇总日志文件: {all_file}")
        except Exception as e:
            print(f"创建汇总日志文件失败: {str(e)}")
            
        # 获取进程ID
        self.pids = self.get_pids_by_pattern()
        self.processes = {}
        
        # 修复CPU监控问题：正确初始化进程对象并进行第一次CPU采样
        for dnode, pid in self.pids.items():
            if pid:
                try:
                    process = psutil.Process(pid)
                    process.cpu_percent()
                    self.processes[dnode] = process
                    print(f"初始化进程对象: {dnode}, PID: {pid}")
                except Exception as e:
                    print(f"初始化进程对象失败 {dnode} (PID: {pid}): {str(e)}")
                    self.processes[dnode] = None
            else:
                self.processes[dnode] = None
                
        self.stop_monitoring = False
        self._should_stop = False
        if use_signal and threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, self.signal_handler)
            signal.signal(signal.SIGTERM, self.signal_handler)

    def __del__(self):
        """确保所有文件都被正确关闭"""
        try:
            if hasattr(self, '_is_running') and self._is_running:
                print(f"调试: MonitorSystemLoad实例 {getattr(self, 'instance_id', 'unknown')} 正在析构，强制停止")
                self.stop()
        except:
            pass
        
        # 关闭文件句柄
        if hasattr(self, 'perf_files'):
            for f in self.perf_files.values():
                try:
                    if not f.closed:
                        f.close()
                except:
                    pass
            
    def stop(self):
        """提供外部停止监控的方法"""
        print(f"调试: 停止监控实例 {getattr(self, 'instance_id', 'unknown')}")
        self.stop_monitoring = True
        self._should_stop = True
        self._force_stop = True 
        self._is_running = False
        self._stop_event.set() 
        
        print("\n停止性能监控...")
        # 等待所有文件写入完成
        if hasattr(self, 'perf_files'):
            for f in self.perf_files.values():
                try:
                    if not f.closed:
                        f.flush()
                except:
                    pass
            

    def _should_continue_monitoring(self):
        """统一的停止检查方法"""
        return (not self.stop_monitoring and 
                not self._should_stop and 
                not self._force_stop and 
                not self._stop_event.is_set())
        
    def signal_handler(self, signum, frame):
        """处理中断信号"""
        print("\n收到中断信号，正在停止监控...")
        self.stop_monitoring = True
        time.sleep(1)
        # 关闭所有文件
        for f in self.perf_files.values():
            try:
                f.close()
            except:
                pass
        # 退出监控但不影响taosd进程
        print("\n监控已停止，taosd进程继续运行")
        os._exit(0)
            
    def get_pid_by_name(self, name):
        for proc in psutil.process_iter(['pid', 'name']):
            if proc.info['name'] == name:
                return proc.info['pid']
        return None
    
    def write_metrics(self, dnode, status, timestamp=None, print_to_console=True):
        """写入性能指标
        
        Args:
            dnode: 节点名称
            status: 性能数据
            timestamp: 时间戳(可选)
            print_to_console: 是否打印到控制台
        """
        # 写入单个节点的日志文件
        self.perf_files[dnode].write(status + '\n')
        
        # 同时写入汇总日志文件
        self.perf_files['all'].write(status + '\n')
        
        # 根据参数决定是否输出到控制台
        if print_to_console:
            print(status)

    def get_pids_by_pattern(self):
        """根据进程名模式获取所有匹配的进程ID"""
        pids = {}
        # 使用 ps 命令获取详细进程信息
        result = subprocess.run(
            'ps -ef | grep taosd | grep -v grep', 
            shell=True, 
            capture_output=True, 
            text=True
        )
        
        for line in result.stdout.splitlines():
            if self.name_pattern in line:
                parts = line.split()
                pid = int(parts[1])
                # 从配置文件路径中提取dnode信息
                cfg_path = next((p for p in parts if '/conf/taos.cfg' in p), None)
                if cfg_path:
                    # 从路径中提取dnode名称
                    dnode = next((part for part in cfg_path.split('/') if part.startswith('dnode')), None)
                    if dnode and dnode in self.monitor_nodes:
                        pids[dnode] = pid
                        print(f"找到 {dnode} 进程, PID: {pid}, 配置文件: {cfg_path}")
        
        if not pids:
            print(f"警告: 未找到匹配模式 '{self.name_pattern}' 的进程")
        else:
            print(f"共找到 {len(pids)} 个taosd进程")
        
        # 无论是否找到进程,都初始化所有dnode的文件句柄
        for dnode in self.monitor_nodes:
            if dnode not in self.perf_files:
                file_path = f"{os.path.splitext(self.perf_file)[0]}-{dnode}.log"
                try:
                    self.perf_files[dnode] = open(file_path, 'w+')
                    print(f"创建性能日志文件: {file_path}")
                except Exception as e:
                    print(f"创建日志文件失败 {file_path}: {str(e)}")
        
        return pids
    
    def write_zero_metrics(self, dnode, timestamp, is_warm_up=False, instance_id=None):
        """写入零值指标
        
        Args:
            dnode: 节点名称
            timestamp: 时间戳
            is_warm_up: 是否为预热期
        """
        status = (
            f"{timestamp} [{dnode}] "
            f"CPU: 0.0%, "
            f"Memory: 0.00MB (0.00%), "
            f"Read: 0.00MB (0), "
            f"Write: 0.00MB (0)"
        )
        
        if is_warm_up:
            status += " [预热期数据]"
        if instance_id:
            status += f" (实例ID: {instance_id})"
        
        try:
            if hasattr(self, 'perf_files') and dnode in self.perf_files:
                if not self.perf_files[dnode].closed:
                    self.perf_files[dnode].write(status + '\n')
        except:
            pass
        
        # 预热期不打印到控制台
        if not is_warm_up:
            print(status)
        
    def get_proc_status_old(self):
        """监控所有匹配进程的状态"""
        try:
            processes = {
                dnode: psutil.Process(pid) if pid else None
                for dnode, pid in self.pids.items()
            }
            
            while self.count > 0 and not self.stop_monitoring:
                try:
                    sys_load = psutil.getloadavg()
                    timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                    
                    # 记录系统整体负载
                    load_info = f"\n{timestamp} System Load: {sys_load[0]:.2f}\n"
                    for dnode in self.perf_files.keys():
                        self.perf_files[dnode].write(load_info)
                    print(load_info)
                    
                    # 记录每个节点的状态
                    for dnode in ['dnode1', 'dnode2', 'dnode3']:
                        process = processes.get(dnode)
                        try:
                            if process and process.is_running():
                                cpu_percent = process.cpu_percent(interval=self.interval)
                                memory_info = process.memory_info()
                                memory_percent = process.memory_percent()
                                io_counters = process.io_counters()

                                status = (
                                    f"{timestamp} [{dnode}] "
                                    f"CPU: {cpu_percent:.1f}%, "
                                    f"Memory: {memory_info.rss/1048576.0:.2f}MB ({memory_percent:.2f}%), "
                                    f"Read: {io_counters.read_bytes/1048576.0:.2f}MB ({io_counters.read_count}), "
                                    f"Write: {io_counters.write_bytes/1048576.0:.2f}MB ({io_counters.write_count})"
                                )
                                self.write_metrics(dnode, status)
                            else:
                                # 进程不存在时写入零值
                                self.write_zero_metrics(dnode, timestamp)
                                
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            # 进程已终止或无法访问时写入零值
                            self.write_zero_metrics(dnode, timestamp)
                            processes[dnode] = None
                        except Exception as e:
                            print(f"监控 {dnode} 出错: {str(e)}")
                            self.write_zero_metrics(dnode, timestamp)
                    
                    # 添加分隔线
                    separator = "-" * 80 + "\n"
                    for f in self.perf_files.values():
                        f.write(separator)
                        f.flush()
                    print(separator.strip())
                        
                    time.sleep(self.interval)
                    self.count -= 1
                    
                    if self.stop_monitoring:
                        print("正在完成最后的监控记录...")
                        break
                    
                except Exception as e:
                    print(f"监控出错: {str(e)}")
                    if not self.stop_monitoring:  # 只有在非主动停止时才跳出
                            break
                
            print("\n监控已停止")
        
        finally:
            # 关闭所有文件
            for f in self.perf_files.values():
                try:
                    f.close()
                except:
                    pass

    def get_proc_status(self):
        """监控所有匹配进程的状态"""
        # 标记实例正在运行
        self._is_running = True
        instance_id = getattr(self, 'instance_id', 'unknown')
        
        print(f"调试: 监控线程开始运行，实例ID = {instance_id}")
        
        try:
            # 计算预热期间的数据点数量
            warm_up_cycles = int(self.warm_up_time / self.interval) if self.warm_up_time > 0 else 0
            
            if warm_up_cycles > 0:
                print(f"\n=== 开始性能监控预热期 ===")
                print(f"预热时间: {self.warm_up_time}秒 ({warm_up_cycles}个周期)")
                print(f"预热期间收集数据但不打印，等待系统稳定...")
                
                # 写入预热说明到日志文件
                for f in self.perf_files.values():
                    f.write(f"=== 性能监控预热期开始 ===\n")
                    f.write(f"预热时间: {self.warm_up_time}秒\n")
                    f.write(f"预热期间的数据仅用于系统稳定，不作为性能评估依据\n")
                    f.write(f"{'='*60}\n")
            
            cycle_count = 0
            
            while self.count > 0 and self._should_continue_monitoring():
                start_time = time.time()
                cycle_count += 1
                
                # 在每个循环开始时检查停止标志
                if not self._should_continue_monitoring():
                    print(f"调试: 监控实例 {instance_id} 收到停止信号，第{cycle_count}次循环")
                    break
                
                sys_load = psutil.getloadavg()
                timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                
                # 判断是否在预热期
                is_warm_up = cycle_count <= warm_up_cycles
                
                # 记录系统负载
                load_info = f"\n{timestamp} System Load: {sys_load[0]:.2f}\n"
                if is_warm_up:
                    load_info += f" [预热期 {cycle_count}/{warm_up_cycles}] (实例ID: {instance_id})"
                else:
                    load_info += f" [正式监控] (实例ID: {instance_id})"
                load_info += "\n"
                
                # 写入文件（包括预热期数据）
                for f in self.perf_files.values():
                    try:
                        if not f.closed:
                            f.write(load_info)
                    except:
                        pass
                                
                # 控制台输出（预热期静默）
                if not is_warm_up:
                    print(load_info.strip()) 
                elif cycle_count == 1:
                    print(f"预热开始: {timestamp}(实例ID: {instance_id})")
                elif cycle_count % 10 == 0:  # 每10个周期显示一次预热进度
                    print(f"预热进度: {cycle_count}/{warm_up_cycles} ({(cycle_count/warm_up_cycles)*100:.1f}%)(实例ID: {instance_id})")
                
                current_pids = self.get_pids_by_pattern()
                
                # 更新进程对象：检查PID是否变化，如果变化则重新初始化
                for dnode in self.monitor_nodes:
                    current_pid = current_pids.get(dnode)
                    existing_process = self.processes.get(dnode)
                    
                    # 检查是否需要更新进程对象
                    need_update = False
                    if current_pid is None:
                        # 进程不存在了
                        if existing_process is not None:
                            need_update = True
                            self.processes[dnode] = None
                    elif existing_process is None:
                        # 新发现的进程
                        need_update = True
                    elif existing_process.pid != current_pid:
                        # PID变化了，进程重启了
                        need_update = True
                    
                    if need_update and current_pid:
                        try:
                            new_process = psutil.Process(current_pid)
                            new_process.cpu_percent()
                            self.processes[dnode] = new_process
                            print(f"调试: 更新进程对象 {dnode}, 新PID: {current_pid}")
                        except Exception as e:
                            print(f"调试: 初始化新进程对象失败 {dnode} (PID: {current_pid}): {str(e)}")
                            self.processes[dnode] = None
                            
                # 收集进程指标
                for dnode in self.monitor_nodes:
                    try:
                        process = self.processes.get(dnode)
                        
                        if process and process.is_running():
                            try:
                                cpu_percent = process.cpu_percent(interval=None)
                                memory_info = process.memory_info()
                                memory_percent = process.memory_percent()
                                io_counters = process.io_counters()

                                status = (
                                    f"{timestamp} [{dnode}] "
                                    f"CPU: {cpu_percent:.1f}%, "
                                    f"Memory: {memory_info.rss/1048576.0:.2f}MB ({memory_percent:.2f}%), "
                                    f"Read: {io_counters.read_bytes/1048576.0:.2f}MB ({io_counters.read_count}), "
                                    f"Write: {io_counters.write_bytes/1048576.0:.2f}MB ({io_counters.write_count})"
                                )
                                
                                if is_warm_up:
                                    status += f" [预热期数据] (实例ID: {instance_id})"
                                else:
                                    status += f" (实例ID: {instance_id})"
                                
                                # 写入日志文件
                                self.write_metrics(dnode, status, print_to_console=(not is_warm_up))
                                # ✅ 调试输出CPU采样信息
                                if cycle_count <= 3 or (cycle_count % 20 == 0):  # 前3次或每20次打印调试信息
                                    print(f"调试: {dnode} CPU采样 - 周期{cycle_count}, CPU: {cpu_percent:.1f}%")
                                    
                            except (psutil.NoSuchProcess, psutil.AccessDenied) as e:
                                print(f"调试: 进程{dnode}访问失败: {str(e)}")
                                self.write_zero_metrics(dnode, timestamp, is_warm_up, instance_id)
                                self.processes[dnode] = None  # 清理无效的进程对象
                        else:
                            # 如果配置要求监控此节点但找不到进程
                            if dnode in self.monitor_nodes:
                                self.write_zero_metrics(dnode, timestamp, is_warm_up, instance_id)
                            
                    except Exception as e:
                        if not is_warm_up:
                            print(f"监控 {dnode} 出错: {str(e)} (实例ID: {instance_id})")
                        self.write_zero_metrics(dnode, timestamp, is_warm_up, instance_id)
                
                # 添加分隔线
                separator = f"---------------- (实例ID: {instance_id}) ----------------\n"
                for f in self.perf_files.values():
                    try:
                        if not f.closed:
                            f.write(separator)
                            f.flush()
                    except:
                        pass
                
                if not is_warm_up:
                    print(separator.strip())
                    
                # 预热期结束提示
                if is_warm_up and cycle_count == warm_up_cycles:
                    print(f"\n=== 预热期结束，开始正式性能监控 (实例ID: {instance_id}) ===")
                    
                    # 写入预热结束标记
                    for f in self.perf_files.values():
                        try:
                            if not f.closed:
                                f.write(f"\n=== 预热期结束，正式监控开始 (实例ID: {instance_id}) ===\n")
                                f.write(f"{'='*60}\n")
                                f.flush()
                                print(f"调试: 已写入预热期结束标记到性能文件")
                        except Exception as e:
                            print(f"调试: 写入预热期结束标记失败: {str(e)}")
                            pass
                
                # 精确控制间隔时间
                elapsed = time.time() - start_time
                if elapsed < self.interval:
                    remaining_time = self.interval - elapsed
                    
                    # 使用Event.wait()替代sleep，能立即响应停止信号
                    if self._stop_event.wait(timeout=remaining_time):
                        print(f"调试: 监控实例 {instance_id} 在wait期间收到停止信号")
                        break
                
                self.count -= 1
                
                # 最终检查是否需要停止
                if not self._should_continue_monitoring():
                    print(f"调试: 监控实例 {instance_id} 正常结束循环")
                    break
                
        except Exception as e:
            print(f"监控出错 (实例ID: {instance_id}): {str(e)}")  
        finally:
            self._is_running = False
            print(f"调试: 监控线程正在清理资源 (实例ID: {instance_id})...")
            
            # ✅ 确保所有文件句柄都被关闭
            if hasattr(self, 'perf_files'):
                for f in self.perf_files.values():
                    try:
                        if not f.closed:
                            f.close()
                    except:
                        pass
                        
            print(f"调试: 监控线程已结束 (实例ID: {instance_id})")
                   

def do_monitor(runtime, perf_file, deployment_mode='cluster', warm_up_time=0):
    """监控线程函数"""
    try:
        # 不在子线程中使用信号处理
        loader = MonitorSystemLoad(
            'taosd -c', 
            runtime, 
            perf_file, 
            use_signal=False,
            deployment_mode=deployment_mode,
            warm_up_time=warm_up_time 
        )
        loader.get_proc_status()
    except Exception as e:
        print(f"监控线程出错: {str(e)}")
    finally:
        print("监控线程结束")

def get_table_list(cursor):
    cursor.execute('use stream_from')

    sql = "select table_name from information_schema.ins_tables where db_name = 'stream_from' and stable_name='stb' order by table_name"
    cursor.execute(sql)

    res = cursor.fetchall()
    return res

def do_multi_insert(index, total, host, user, passwd, conf, tz):
    conn = taos.connect(
        host=host, user=user, password=passwd, config=conf, timezone=tz
    )

    cursor = conn.cursor()
    cursor.execute('use stream_from')

    start_ts = 1609430400000
    step = 5

    cursor.execute("create stable if not exists stb_result(wstart timestamp, minx float, maxx float, countx bigint) tags(gid bigint unsigned)")

    list = get_table_list(cursor)

    list = list[index*total: (index+1)*total]

    print("there are %d tables" % len(list))

    for index, n in enumerate(list):
        cursor.execute(f"create table if not exists {n[0]}_1 using stb_result tags(1)")
        count = 1
        while True:
            sql = (f"select cast({start_ts + step * 1000 * (count - 1)} as timestamp), min(c1), max(c2), count(c3) from stream_from.{n[0]} "
                   f"where ts >= {start_ts + step * 1000 * (count - 1)} and ts < {start_ts + step * 1000 * count}")
            cursor.execute(sql)

            res = cursor.fetchall()
            if res[0][3] == 0:
                break

            insert = f"insert into {n[0]}_1 values ({start_ts + step * 1000 * (count - 1)}, {res[0][1]}, {res[0][2]}, {res[0][3]})"
            cursor.execute(insert)
            count += 1
    conn.close()


'''
class StreamSQLTemplates_bak_for_phase1:
    """流计算 SQL 模板集合"""
    
    s2_2 = """
    create stream s2_2 trigger at_once 
        ignore expired 0 ignore update 0 into stream_to.stb2_2 
        as select _wstart as wstart,
        avg(c0), avg(c1),avg(c2), avg(c3),
        max(c0), max(c1), max(c2), max(c3),
        min(c0), min(c1), min(c2), min(c3)
        from stream_from.stb partition by tbname interval(15s);
    """
    
    s2_3 = """
    create stream s2_3 trigger window_close 
        ignore expired 0 ignore update 0 into stream_to.stb2_3 
        as select _wstart as wstart,
        avg(c0), avg(c1),avg(c2), avg(c3),
        max(c0), max(c1), max(c2), max(c3),
        min(c0), min(c1), min(c2), min(c3)
        from stream_from.stb partition by tbname interval(15s);
    """
    
    s2_4 = """
    create stream s2_4 trigger max_delay 5s 
        ignore expired 0 ignore update 0 into stream_to.stb2_4 
        as select _wstart as wstart,
        avg(c0), avg(c1),avg(c2), avg(c3),
        max(c0), max(c1), max(c2), max(c3),
        min(c0), min(c1), min(c2), min(c3)
        from stream_from.stb partition by tbname interval(15s);
    """
    
    s2_5 = """
    create stream s2_5 trigger FORCE_WINDOW_CLOSE into stream_to.stb2_5 
        as select _wstart as wstart,
        avg(c0), avg(c1),avg(c2), avg(c3),
        max(c0), max(c1), max(c2), max(c3),
        min(c0), min(c1), min(c2), min(c3)
        from stream_from.stb partition by tbname interval(15s);
    """
    
    s2_6 = """
    create stream s2_6 trigger CONTINUOUS_WINDOW_CLOSE 
        ignore expired 0 ignore update 0 into stream_to.stb2_6 
        as select _wstart as wstart,
        avg(c0), avg(c1),avg(c2), avg(c3),
        max(c0), max(c1), max(c2), max(c3),
        min(c0), min(c1), min(c2), min(c3)
        from stream_from.stb partition by tbname interval(15s);
    """
    
    s2_7 = """
    create stream stream_from.s2_7 INTERVAL(15s) SLIDING(15s)
            from stream_from.stb 
            partition by tbname 
            into stream_to.stb2_7
            as select _twstart ts, avg(c0), avg(c1), avg(c2), avg(c3),
            max(c0), max(c1), max(c2), max(c3),
            min(c0), min(c1), min(c2), min(c3)
            from %%tbname where ts >= _twstart and ts < _twend;
    """
    
    s2_8 = """
    create stream stream_from.s2_8 INTERVAL(15s) SLIDING(15s)
            from stream_from.stb 
            partition by tbname 
            into stream_to.stb2_8
            as select _twstart ts, avg(c0), avg(c1), avg(c2), avg(c3),
            max(c0), max(c1), max(c2), max(c3),
            min(c0), min(c1), min(c2), min(c3)
            from %%trows ;
    """
    
    s2_9 = """
    create stream stream_from.s2_9 INTERVAL(15s) SLIDING(15s)
            from stream_from.stb partition by tbname 
            STREAM_OPTIONS(MAX_DELAY(5s)) 
            into stream_to.stb2_9
            as select _twstart ts, avg(c0), avg(c1), avg(c2), avg(c3),
            max(c0), max(c1), max(c2), max(c3),
            min(c0), min(c1), min(c2), min(c3)
            from %%tbname where ts >= _twstart and ts < _twend;
    """
    
    s2_10 = """
    create stream stream_from.s2_10 INTERVAL(15s) SLIDING(15s) 
            from stream_from.stb 
            STREAM_OPTIONS(MAX_DELAY(5s))
            into stream_to.stb2_10
            as select _twstart ts, avg(c0), avg(c1), avg(c2), avg(c3),
            max(c0), max(c1), max(c2), max(c3),
            min(c0), min(c1), min(c2), min(c3)
            from %%trows ;
    """
    
    s2_11 = """
    create stream stream_from.s2_11 period(15s) 
            from stream_from.stb partition by tbname  
            into stream_to.stb2_11
            as select cast(_tlocaltime/1000000 as timestamp) ts, avg(c0), avg(c1), avg(c2), avg(c3),
            max(c0), max(c1), max(c2), max(c3),
            min(c0), min(c1), min(c2), min(c3)
            from %%tbname ;
    """    
    
    s2_12 = """
    create stream stream_from.s2_12 session(ts,10a)
            from stream_from.stb partition by tbname 
            into stream_to.stb2_12
            as select _twstart ts, avg(c0), avg(c1), avg(c2), avg(c3),
            max(c0), max(c1), max(c2), max(c3),
            min(c0), min(c1), min(c2), min(c3)
            from %%tbname where ts >= _twstart and ts <= _twend;
    """
    
    s2_13 = """
    create stream stream_from.s2_13 COUNT_WINDOW(1000)
            from stream_from.stb partition by tbname 
            into stream_to.stb2_13
            as select _twstart ts, avg(c0), avg(c1), avg(c2), avg(c3),
            max(c0), max(c1), max(c2), max(c3),
            min(c0), min(c1), min(c2), min(c3)
            from %%tbname where ts >= _twstart and ts <= _twend;
    """
    
    s2_14 = """
    create stream stream_from.s2_14 EVENT_WINDOW(START WITH c0 > -10000000 END WITH c0 < 10000000) 
            from stream_from.stb partition by tbname 
            into stream_to.stb2_14
            as select _twstart ts, avg(c0), avg(c1), avg(c2), avg(c3),
            max(c0), max(c1), max(c2), max(c3),
            min(c0), min(c1), min(c2), min(c3)
            from %%tbname where ts >= _twstart and ts <= _twend;
    """
    
    s2_15 = """
    create stream stream_from.s2_15 STATE_WINDOW(c0) 
            from stream_from.stb partition by tbname 
            into stream_to.stb2_15
            as select _twstart ts, avg(c0), avg(c1), avg(c2), avg(c3),
            max(c0), max(c1), max(c2), max(c3),
            min(c0), min(c1), min(c2), min(c3)
            from %%tbname where ts >= _twstart and ts <= _twend;
    """
    
    @classmethod
    def get_sql(cls, sql_type):
        """
        获取指定类型的 SQL 模板
        Args:
            sql_type: SQL 类型标识符 (case_id)
        Returns:
            对应的 SQL 模板
        """
        sql_map = {
            's2_2': cls.s2_2,
            's2_3': cls.s2_3,
            's2_4': cls.s2_4,
            's2_5': cls.s2_5,
            's2_6': cls.s2_6,
            's2_7': cls.s2_7,
            's2_8': cls.s2_8,
            's2_9': cls.s2_9,
            's2_10': cls.s2_10,
            's2_11': cls.s2_11,
            's2_12': cls.s2_12,
            's2_13': cls.s2_13,
            's2_14': cls.s2_14,
            's2_15': cls.s2_15,
        }
        
        # 定义批量 SQL 组合
        batch_map = {
            'all': {
                's2_7': cls.s2_7,
                's2_8': cls.s2_8,
                's2_9': cls.s2_9,
                's2_10': cls.s2_10,
                's2_11': cls.s2_11,
                's2_12': cls.s2_12,
                's2_13': cls.s2_13,
                's2_14': cls.s2_14,
                's2_15': cls.s2_15,
            }
        }
        # 如果请求批量 SQL
        if sql_type in batch_map:
            return batch_map[sql_type]
        
        # 返回单个 SQL，默认返回 s2_7
        return sql_map.get(sql_type, cls.s2_7)  
'''


class StreamSQLTemplates:
    """流计算 SQL 模板集合 - 重构版本"""
    
    def __init__(self):
        # 定义默认的聚合函数组合
        self.default_agg_columns = [
                "avg(c0), avg(c1), avg(c2), avg(c3)",
                "max(c0), max(c1), max(c2), max(c3)", 
                "min(c0), min(c1), min(c2), min(c3)"
        ]
        
        # 定义默认的投影列
        self.default_select_columns = ["c0", "c1", "c2", "c3"]
        
    def get_select_stream(self, **kwargs):
        """查询所有流信息"""
        return "select * from information_schema.ins_streams;"
    
    def _build_columns(self, agg_or_select='agg', custom_columns=None):
        """构建查询列
        
        Args:
            agg_or_select: 'agg' 表示聚合查询, 'select' 表示投影查询
            custom_columns: 自定义列，如果提供则使用自定义列
            
        Returns:
            str: 构建好的列字符串
        """
        if custom_columns:
            if isinstance(custom_columns, list):
                return ", ".join(custom_columns)
            return custom_columns
            
        if agg_or_select == 'agg':
            return ",\n        ".join(self.default_agg_columns)
        else:  
            return ", ".join(self.default_select_columns)
    
    def _build_from_clause(self, tbname_or_trows_or_sourcetable='sourcetable', is_period=False, is_sliding=False):
        """构建 FROM 子句
        
        Args:
            tbname_or_trows_or_sourcetable: FROM 子句类型
                - 'tbname': %%tbname where ts >= _twstart and ts <= _twend  
                - 'trows': %%trows
                - 'sourcetable': 明确指定源表名 where ts >= _twstart and ts <= _twend
            is_period: 是否为 period 类型的流计算
            is_sliding: 是否为 sliding 类型的流计算
            
        Returns:
            str: FROM 子句字符串
        """
        if tbname_or_trows_or_sourcetable == 'trows':
            return "%%trows "
        elif tbname_or_trows_or_sourcetable == 'sourcetable':
            # 返回占位符，在具体模板中会被替换为实际的表名
            if is_sliding:
                return "SOURCE_TABLE_PLACEHOLDER where ts >= _tprev_ts and ts <= _tnext_ts"
            elif is_period:
                return "SOURCE_TABLE_PLACEHOLDER where ts >= cast(_tprev_localtime/1000000 as timestamp) and ts <= cast(_tnext_localtime/1000000 as timestamp)"
            else:
                return "SOURCE_TABLE_PLACEHOLDER where ts >= _twstart and ts <= _twend"
        else:  # 'tbname' and 纳秒
            if is_sliding:
                return "%%tbname where ts >= _tprev_ts and ts <= _tnext_ts"
            elif is_period:
                return "%%tbname where ts >= cast(_tprev_localtime/1000000 as timestamp) and ts <= cast(_tnext_localtime/1000000 as timestamp)"
            else:
                return "%%tbname where ts >= _twstart and ts <= _twend"

    
    def _build_from_source_and_clause(self, source_type='stb', stream_index=None, tbname_or_trows_or_sourcetable='sourcetable', is_period=False, is_sliding=False):
        """构建数据源和FROM子句的组合
        
        Args:
            source_type: 数据源类型 ('stb', 'tb')
            stream_index: 流的索引编号
            tbname_or_trows_or_sourcetable: FROM子句类型
            is_period: 是否为 period 类型的流计算
            is_sliding: 是否为 sliding 类型的流计算
                
        Returns:
            tuple: (from_source, from_clause)
        """
        from_source = self._build_from_source(source_type, stream_index)
        
        if tbname_or_trows_or_sourcetable == 'sourcetable':
            # 对于sourcetable模式，FROM子句直接使用具体的表名
            if source_type == 'tb':
                # 子表情况：使用具体的子表名
                if stream_index is not None:
                    table_name = f"stream_from.ctb0_{stream_index}"
                else:
                    table_name = "stream_from.ctb0_0"
            else:  # 'stb'
                # 超级表情况：使用超级表名
                table_name = "stream_from.stb"
                
            # period 类型使用不同的时间变量
            if is_sliding:
                from_clause = f"{table_name} where ts >= _tprev_ts and ts <= _tnext_ts"
            elif is_period:
                from_clause = f"{table_name} where ts >= cast(_tprev_localtime/1000000 as timestamp) and ts <= cast(_tnext_localtime/1000000 as timestamp)"
            else:
                from_clause = f"{table_name} where ts >= _twstart and ts <= _twend"
        else:
            # 其他模式使用原有逻辑
            from_clause = self._build_from_clause(tbname_or_trows_or_sourcetable, is_period, is_sliding)
            
        return from_source, from_clause        
    
    def _build_partition_clause(self, partition_type='none'):
        """构建分区子句
        
        Args:
            partition_type: 分区类型
                - 'none': 不分组
                - 'tbname': 按子表名分组  
                - 'tag': 按tag分组
                
        Returns:
            str: 分区子句字符串
        """
        if partition_type == 'tbname':
            return "partition by tbname"
        elif partition_type == 'tag':
            return "partition by t0"  # 假设第一个tag字段名为t0
        else:  # 'none'
            return ""
    
    def _build_from_source(self, source_type='stb', stream_index=None):
        """构建数据源
        
        Args:
            source_type: 数据源类型
                - 'stb': 超级表 stream_from.stb
                - 'tb': 子表 stream_from.ctb0_X
            stream_index: 流的索引编号（用于多流时选择不同子表）
                
        Returns:
            str: 数据源字符串
        """
        if source_type == 'tb':
            # 如果有流索引，使用对应的子表；否则使用默认的ctb0_0
            if stream_index is not None:
                # 子表编号从1开始
                table_index = stream_index 
                #print(f"调试: 生成子表名 stream_from.ctb0_{table_index}")
                return f"stream_from.ctb0_{table_index}"
            else:
                #print(f"调试: 使用默认子表 stream_from.ctb0_0")
                return "stream_from.ctb0_0"  # 默认子表
        else:  # 'stb'
            return "stream_from.stb"
    
    def _generate_stream_name(self, base_type, source_type, partition_type, agg_or_select='agg', tbname_or_trows_or_sourcetable='sourcetable', stream_index=None):
        """生成流名称
        
        Args:
            base_type: 基础类型 (如 's2_7', 'intervalsliding')
            source_type: 数据源类型 ('stb', 'tb') 
            partition_type: 分区类型 ('none', 'tbname', 'tag')
            agg_or_select: 查询类型 ('agg', 'select')
            tbname_or_trows_or_sourcetable: FROM子句类型 ('tbname', 'trows', 'sourcetable')
            stream_index: 流索引编号（可选）
            
        Returns:
            str: 生成的流名称
        """
        # 构建名称组件
        source_part = "stb" if source_type == "stb" else "tb"
        
        if partition_type == 'none':
            partition_part = ""
        elif partition_type == 'tbname':
            partition_part = "_partition_by_tbname" 
        elif partition_type == 'tag':
            partition_part = "_partition_by_tag"
        else:
            partition_part = ""
            
        # 查询类型部分
        query_part = f"_{agg_or_select}"
        
        # FROM子句类型部分
        if tbname_or_trows_or_sourcetable == 'sourcetable':
            if source_type == 'stb':
                # 超级表情况：加上 stb 后缀
                from_part = "_sourcetable_stb"
            else:  # 'tb'
                # 子表情况：加上具体的子表名
                if stream_index is not None:
                    from_part = f"_sourcetable_ctb0_{stream_index}"
                else:
                    from_part = "_sourcetable_ctb0_0"
        else:
            from_part = f"_{tbname_or_trows_or_sourcetable}"
        
        # 基础名称组合
        stream_name = f"stream_from.{base_type}_{source_part}{partition_part}{query_part}{from_part}"
        
        # 如果有流索引，添加到末尾
        if stream_index is not None and not (tbname_or_trows_or_sourcetable == 'sourcetable' and source_type == 'tb'):
            stream_name += f"_{stream_index}"
            
        return stream_name
    
    def _generate_target_table(self, base_type, source_type, partition_type, agg_or_select='agg', tbname_or_trows_or_sourcetable='sourcetable', stream_index=None):
        """生成目标表名称"""
        source_part = "stb" if source_type == "stb" else "tb"
        
        if partition_type == 'none':
            partition_part = ""
        elif partition_type == 'tbname':
            partition_part = "_partition_by_tbname"
        elif partition_type == 'tag':
            partition_part = "_partition_by_tag"
        else:
            partition_part = ""
        
        # 查询类型部分
        query_part = f"_{agg_or_select}"
        
        # FROM子句类型部分
        if tbname_or_trows_or_sourcetable == 'sourcetable':
            if source_type == 'stb':
                # 超级表情况：加上 stb 后缀
                from_part = "_sourcetable_stb"
            else:  # 'tb'
                # 子表情况：加上具体的子表名
                if stream_index is not None:
                    from_part = f"_sourcetable_ctb0_{stream_index}"
                else:
                    from_part = "_sourcetable_ctb0_0"
        else:
            from_part = f"_{tbname_or_trows_or_sourcetable}"
        
        # 基础名称组合
        target_table = f"stream_to.{base_type}_{source_part}{partition_part}{query_part}{from_part}"
        
        # 如果有流索引，添加到末尾
        if stream_index is not None and not (tbname_or_trows_or_sourcetable == 'sourcetable' and source_type == 'tb'):
            target_table += f"_{stream_index}"
            
        return target_table
    
        
    # ========== 通用模板生成方法 ==========
    def get_intervalsliding_template(self, source_type='stb', partition_type='tbname', 
                           agg_or_select='agg', tbname_or_trows_or_sourcetable='sourcetable', custom_columns=None, stream_index=None):
        """生成滑动窗口模板
        
        Args:
            source_type: 'stb'(超级表) 或 'tb'(子表)
            partition_type: 'none'(不分组), 'tbname'(按子表名), 'tag'(按tag)
            agg_or_select: 'agg' 或 'select'
            tbname_or_trows_or_sourcetable: 'tbname' 或 'trows' 或 'sourcetable'
            custom_columns: 自定义列
        """
        columns = self._build_columns(agg_or_select, custom_columns)
        from_source, from_clause = self._build_from_source_and_clause(source_type, stream_index, tbname_or_trows_or_sourcetable, is_period=False)
        partition_clause = self._build_partition_clause(partition_type)
        
        stream_name = self._generate_stream_name('intervalsliding', source_type, partition_type, agg_or_select, tbname_or_trows_or_sourcetable, stream_index)
        target_table = self._generate_target_table('intervalsliding', source_type, partition_type, agg_or_select, tbname_or_trows_or_sourcetable, stream_index)
        
        # 如果有流索引，在流名称和目标表中添加索引
        if stream_index is not None:
            stream_name += f"_{stream_index}"
            target_table += f"_{stream_index}"
        
        # 构建完整的SQL
        partition_line = f"\n            {partition_clause}" if partition_clause else ""
        
        return f"""
    create stream {stream_name} INTERVAL(15s) SLIDING(15s)
            from {from_source}{partition_line}
            into {target_table}
            as select _twstart ts, {columns}
            from {from_clause};
    """
    
    def get_session_template(self, source_type='stb', partition_type='tbname',
                           agg_or_select='agg', tbname_or_trows_or_sourcetable='sourcetable', custom_columns=None, stream_index=None):
        """生成会话窗口模板"""
        columns = self._build_columns(agg_or_select, custom_columns)
        from_source, from_clause = self._build_from_source_and_clause(source_type, stream_index, tbname_or_trows_or_sourcetable, is_period=False)
        partition_clause = self._build_partition_clause(partition_type)
         
        stream_name = self._generate_stream_name('session', source_type, partition_type, agg_or_select, tbname_or_trows_or_sourcetable, stream_index)
        target_table = self._generate_target_table('session', source_type, partition_type, agg_or_select, tbname_or_trows_or_sourcetable, stream_index)
        
        # 如果有流索引，在流名称和目标表中添加索引
        if stream_index is not None:
            stream_name += f"_{stream_index}"
            target_table += f"_{stream_index}"
        
        partition_line = f"\n            {partition_clause}" if partition_clause else ""
        
        return f"""
    create stream {stream_name} session(ts,10a)
            from {from_source}{partition_line}
            into {target_table}
            as select _twstart ts, {columns}
            from {from_clause};
    """
    
    def get_count_template(self, source_type='stb', partition_type='tbname',
                         agg_or_select='agg', tbname_or_trows_or_sourcetable='sourcetable', custom_columns=None, stream_index=None):
        """生成计数窗口模板"""
        columns = self._build_columns(agg_or_select, custom_columns)
        from_source, from_clause = self._build_from_source_and_clause(source_type, stream_index, tbname_or_trows_or_sourcetable, is_period=False)
        partition_clause = self._build_partition_clause(partition_type)
        
        stream_name = self._generate_stream_name('count', source_type, partition_type, agg_or_select, tbname_or_trows_or_sourcetable, stream_index)
        target_table = self._generate_target_table('count', source_type, partition_type, agg_or_select, tbname_or_trows_or_sourcetable, stream_index)
        
        # 如果有流索引，在流名称和目标表中添加索引
        if stream_index is not None:
            stream_name += f"_{stream_index}"
            target_table += f"_{stream_index}"
        
        partition_line = f"\n            {partition_clause}" if partition_clause else ""
        
        return f"""
    create stream {stream_name} COUNT_WINDOW(1000)
            from {from_source}{partition_line}
            into {target_table}
            as select _twstart ts, {columns}
            from {from_clause};
    """
    
    def get_event_template(self, source_type='stb', partition_type='tbname',
                         agg_or_select='agg', tbname_or_trows_or_sourcetable='sourcetable', custom_columns=None, stream_index=None):
        """生成事件窗口模板"""
        columns = self._build_columns(agg_or_select, custom_columns)
        from_source, from_clause = self._build_from_source_and_clause(source_type, stream_index, tbname_or_trows_or_sourcetable, is_period=False)
        partition_clause = self._build_partition_clause(partition_type)
        
        stream_name = self._generate_stream_name('event', source_type, partition_type, agg_or_select, tbname_or_trows_or_sourcetable, stream_index)
        target_table = self._generate_target_table('event', source_type, partition_type, agg_or_select, tbname_or_trows_or_sourcetable, stream_index)
        
        # 如果有流索引，在流名称和目标表中添加索引
        if stream_index is not None:
            stream_name += f"_{stream_index}"
            target_table += f"_{stream_index}"
        
        partition_line = f"\n            {partition_clause}" if partition_clause else ""
        
        return f"""
    create stream {stream_name} EVENT_WINDOW(START WITH c0 > -10000000 END WITH c0 < 10000000)
            from {from_source}{partition_line}
            into {target_table}
            as select _twstart ts, {columns}
            from {from_clause};
    """
    
    def get_state_template(self, source_type='stb', partition_type='tbname',
                         agg_or_select='agg', tbname_or_trows_or_sourcetable='sourcetable', custom_columns=None, stream_index=None):
        """生成状态窗口模板"""
        columns = self._build_columns(agg_or_select, custom_columns)
        from_source, from_clause = self._build_from_source_and_clause(source_type, stream_index, tbname_or_trows_or_sourcetable, is_period=False)
        partition_clause = self._build_partition_clause(partition_type)
         
        stream_name = self._generate_stream_name('state', source_type, partition_type, agg_or_select, tbname_or_trows_or_sourcetable, stream_index)
        target_table = self._generate_target_table('state', source_type, partition_type, agg_or_select, tbname_or_trows_or_sourcetable, stream_index)
        
        # 如果有流索引，在流名称和目标表中添加索引
        if stream_index is not None:
            stream_name += f"_{stream_index}"
            target_table += f"_{stream_index}"
        
        partition_line = f"\n            {partition_clause}" if partition_clause else ""
        
        return f"""
    create stream {stream_name} STATE_WINDOW(c0)
            from {from_source}{partition_line}
            into {target_table}
            as select _twstart ts, {columns}
            from {from_clause};
    """
    
    def get_period_template(self, source_type='stb', partition_type='tbname',
                          agg_or_select='agg', tbname_or_trows_or_sourcetable='sourcetable', custom_columns=None, stream_index=None):
        """生成定时触发模板 """
        columns = self._build_columns(agg_or_select, custom_columns)
        # from_source, from_clause = self._build_from_source_and_clause(source_type, stream_index, tbname_or_trows_or_sourcetable)
        partition_clause = self._build_partition_clause(partition_type)
        
        # period 类型特殊处理：使用专门的时间变量
        from_source = self._build_from_source(source_type, stream_index)
        
        # period 类型的 FROM 子句处理
        if tbname_or_trows_or_sourcetable == 'trows':
            # period + trows 的组合：直接使用 %%trows（不支持时间条件）
            from_clause = "%%trows"
        elif tbname_or_trows_or_sourcetable == 'sourcetable':
            # period + sourcetable：使用具体表名 + period 专用时间变量
            if source_type == 'tb':
                if stream_index is not None:
                    table_name = f"stream_from.ctb0_{stream_index}"
                else:
                    table_name = "stream_from.ctb0_0"
            else:  # 'stb'
                table_name = "stream_from.stb"
            from_clause = f"{table_name} where ts >= cast(_tprev_localtime/1000000 as timestamp) and ts <= cast(_tnext_localtime/1000000 as timestamp)"
        else:  # 'tbname'
            # period + tbname：使用 %%tbname + period 专用时间变量
            from_clause = "%%tbname where ts >= cast(_tprev_localtime/1000000 as timestamp) and ts <= cast(_tnext_localtime/1000000 as timestamp)"
         
        stream_name = self._generate_stream_name('period', source_type, partition_type, agg_or_select, tbname_or_trows_or_sourcetable, stream_index)
        target_table = self._generate_target_table('period', source_type, partition_type, agg_or_select, tbname_or_trows_or_sourcetable, stream_index)
        
        # 如果有流索引，在流名称和目标表中添加索引
        if stream_index is not None:
            stream_name += f"_{stream_index}"
            target_table += f"_{stream_index}"
        
        partition_line = f"\n            {partition_clause}" if partition_clause else ""
        
        return f"""
    create stream {stream_name} period(15s)
            from {from_source}{partition_line}
            into {target_table}
            as select cast(_tlocaltime/1000000 as timestamp) ts, {columns}
            from {from_clause};
    """


    def get_sliding_template(self, source_type='stb', partition_type='tbname',
                           agg_or_select='agg', tbname_or_trows_or_sourcetable='sourcetable', custom_columns=None, stream_index=None):
        """生成 sliding 窗口模板 - 新增窗口类型"""
        columns = self._build_columns(agg_or_select, custom_columns)
        partition_clause = self._build_partition_clause(partition_type)
        
        # sliding 类型特殊处理：使用专门的时间变量
        from_source = self._build_from_source(source_type, stream_index)
        
        # sliding 类型的 FROM 子句处理
        if tbname_or_trows_or_sourcetable == 'trows':
            # sliding + trows 的组合：直接使用 %%trows（不支持时间条件）
            from_clause = "%%trows"
        elif tbname_or_trows_or_sourcetable == 'sourcetable':
            # sliding + sourcetable：使用具体表名 + sliding 专用时间变量
            if source_type == 'tb':
                if stream_index is not None:
                    table_name = f"stream_from.ctb0_{stream_index}"
                else:
                    table_name = "stream_from.ctb0_0"
            else:  # 'stb'
                table_name = "stream_from.stb"
            from_clause = f"{table_name} where ts >= _tprev_ts and ts <= _tnext_ts"
        else:  # 'tbname'
            # sliding + tbname：使用 %%tbname + sliding 专用时间变量
            from_clause = "%%tbname where ts >= _tprev_ts and ts <= _tnext_ts"
         
        stream_name = self._generate_stream_name('sliding', source_type, partition_type, agg_or_select, tbname_or_trows_or_sourcetable, stream_index)
        target_table = self._generate_target_table('sliding', source_type, partition_type, agg_or_select, tbname_or_trows_or_sourcetable, stream_index)
        
        partition_line = f"\n            {partition_clause}" if partition_clause else ""
        
        return f"""
    create stream {stream_name} sliding(15s)
            from {from_source}{partition_line}
            into {target_table}
            as select _tcurrent_ts ts, {columns}
            from {from_clause};
    """
       
    # ========== 组合生成方法 ==========
    def get_intervalsliding_group_detailed(self, **kwargs):
        """获取详细的滑动窗口组合 (4种组合)"""
        return {
            'intervalsliding_stb': self.get_intervalsliding_template(source_type='stb', partition_type='none', **kwargs),
            'intervalsliding_stb_partition_by_tbname': self.get_intervalsliding_template(source_type='stb', partition_type='tbname', **kwargs),
            'intervalsliding_stb_partition_by_tag': self.get_intervalsliding_template(source_type='stb', partition_type='tag', **kwargs),
            'intervalsliding_tb': self.get_intervalsliding_template(source_type='tb', partition_type='none', **kwargs)
        }

    def get_sliding_group_detailed(self, **kwargs):
        """获取详细的 sliding 窗口组合"""
        return {
            'sliding_stb': self.get_sliding_template(source_type='stb', partition_type='none', **kwargs),
            'sliding_stb_partition_by_tbname': self.get_sliding_template(source_type='stb', partition_type='tbname', **kwargs),
            'sliding_stb_partition_by_tag': self.get_sliding_template(source_type='stb', partition_type='tag', **kwargs),
            'sliding_tb': self.get_sliding_template(source_type='tb', partition_type='none', **kwargs)
        }
            
    def get_session_group_detailed(self, **kwargs):
        """获取详细的会话窗口组合"""
        return {
            'session_stb': self.get_session_template(source_type='stb', partition_type='none', **kwargs),
            'session_stb_partition_by_tbname': self.get_session_template(source_type='stb', partition_type='tbname', **kwargs),
            'session_stb_partition_by_tag': self.get_session_template(source_type='stb', partition_type='tag', **kwargs),
            'session_tb': self.get_session_template(source_type='tb', partition_type='none', **kwargs)
        }
    
    def get_count_group_detailed(self, **kwargs):
        """获取详细的计数窗口组合"""
        return {
            'count_stb': self.get_count_template(source_type='stb', partition_type='none', **kwargs),
            'count_stb_partition_by_tbname': self.get_count_template(source_type='stb', partition_type='tbname', **kwargs),
            'count_stb_partition_by_tag': self.get_count_template(source_type='stb', partition_type='tag', **kwargs),
            'count_tb': self.get_count_template(source_type='tb', partition_type='none', **kwargs)
        }
    
    def get_event_group_detailed(self, **kwargs):
        """获取详细的事件窗口组合"""
        return {
            'event_stb': self.get_event_template(source_type='stb', partition_type='none', **kwargs),
            'event_stb_partition_by_tbname': self.get_event_template(source_type='stb', partition_type='tbname', **kwargs),
            'event_stb_partition_by_tag': self.get_event_template(source_type='stb', partition_type='tag', **kwargs),
            'event_tb': self.get_event_template(source_type='tb', partition_type='none', **kwargs)
        }
    
    def get_state_group_detailed(self, **kwargs):
        """获取详细的状态窗口组合"""
        return {
            'state_stb': self.get_state_template(source_type='stb', partition_type='none', **kwargs),
            'state_stb_partition_by_tbname': self.get_state_template(source_type='stb', partition_type='tbname', **kwargs),
            'state_stb_partition_by_tag': self.get_state_template(source_type='stb', partition_type='tag', **kwargs),
            'state_tb': self.get_state_template(source_type='tb', partition_type='none', **kwargs)
        }
    
    def get_period_group_detailed(self, **kwargs):
        """获取详细的定时触发组合"""
        # period 不支持 tbname_or_trows_or_sourcetable 参数
        filtered_kwargs = {k: v for k, v in kwargs.items() if k != 'tbname_or_trows_or_sourcetable'}
        return {
            'period_stb': self.get_period_template(source_type='stb', partition_type='none', **filtered_kwargs),
            'period_stb_partition_by_tbname': self.get_period_template(source_type='stb', partition_type='tbname', **filtered_kwargs),
            'period_stb_partition_by_tag': self.get_period_template(source_type='stb', partition_type='tag', **filtered_kwargs),
            'period_tb': self.get_period_template(source_type='tb', partition_type='none', **filtered_kwargs)
        }
        
    def get_tbname_agg_group(self, **kwargs):
        """获取 tbname + agg 组合的所有窗口类型
        
        固定参数: tbname_or_trows_or_sourcetable='tbname', agg_or_select='agg'
        忽略命令行传入的这两个参数
        """
        # 固定参数，忽略传入的参数
        fixed_kwargs = {k: v for k, v in kwargs.items() if k not in ['tbname_or_trows_or_sourcetable', 'agg_or_select']}
        fixed_kwargs.update({
            'tbname_or_trows_or_sourcetable': 'tbname',
            'agg_or_select': 'agg'
        })
        
        return {
            # intervalsliding 窗口 - 所有组合
            'intervalsliding_stb_tbname_agg': self.get_intervalsliding_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'intervalsliding_stb_partition_by_tbname_agg': self.get_intervalsliding_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'intervalsliding_stb_partition_by_tag_agg': self.get_intervalsliding_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'intervalsliding_tb_agg': self.get_intervalsliding_template(source_type='tb', partition_type='none', **fixed_kwargs),
            
            # sliding 窗口 - 所有组合
            'sliding_stb_tbname_agg': self.get_sliding_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'sliding_stb_partition_by_tbname_agg': self.get_sliding_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'sliding_stb_partition_by_tag_agg': self.get_sliding_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'sliding_tb_agg': self.get_sliding_template(source_type='tb', partition_type='none', **fixed_kwargs),
                        
            # 会话窗口 - 所有组合
            'session_stb_tbname_agg': self.get_session_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'session_stb_partition_by_tbname_agg': self.get_session_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'session_stb_partition_by_tag_agg': self.get_session_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'session_tb_agg': self.get_session_template(source_type='tb', partition_type='none', **fixed_kwargs),
            
            # 计数窗口 - 所有组合
            'count_stb_tbname_agg': self.get_count_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'count_stb_partition_by_tbname_agg': self.get_count_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'count_stb_partition_by_tag_agg': self.get_count_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'count_tb_agg': self.get_count_template(source_type='tb', partition_type='none', **fixed_kwargs),
            
            # 事件窗口 - 所有组合
            'event_stb_tbname_agg': self.get_event_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'event_stb_partition_by_tbname_agg': self.get_event_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'event_stb_partition_by_tag_agg': self.get_event_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'event_tb_agg': self.get_event_template(source_type='tb', partition_type='none', **fixed_kwargs),
            
            # 状态窗口 - 所有组合
            'state_stb_tbname_agg': self.get_state_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'state_stb_partition_by_tbname_agg': self.get_state_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'state_stb_partition_by_tag_agg': self.get_state_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'state_tb_agg': self.get_state_template(source_type='tb', partition_type='none', **fixed_kwargs),
            
            # 定时触发 - 所有组合
            'period_stb_tbname_agg': self.get_period_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'period_stb_partition_by_tbname_agg': self.get_period_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'period_stb_partition_by_tag_agg': self.get_period_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'period_tb_agg': self.get_period_template(source_type='tb', partition_type='none', **fixed_kwargs),
        }

    def get_tbname_select_group(self, **kwargs):
        """获取 tbname + select 组合的所有窗口类型
        
        固定参数: tbname_or_trows_or_sourcetable='tbname', agg_or_select='select'
        忽略命令行传入的这两个参数
        """
        # 固定参数，忽略传入的参数
        fixed_kwargs = {k: v for k, v in kwargs.items() if k not in ['tbname_or_trows_or_sourcetable', 'agg_or_select']}
        fixed_kwargs.update({
            'tbname_or_trows_or_sourcetable': 'tbname',
            'agg_or_select': 'select'
        })
        
        return {
            # intervalsliding窗口 - 所有组合
            'intervalsliding_stb_tbname_select': self.get_intervalsliding_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'intervalsliding_stb_partition_by_tbname_select': self.get_intervalsliding_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'intervalsliding_stb_partition_by_tag_select': self.get_intervalsliding_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'intervalsliding_tb_select': self.get_intervalsliding_template(source_type='tb', partition_type='none', **fixed_kwargs),
            
            # sliding 窗口 - 所有组合
            'sliding_stb_tbname_select': self.get_sliding_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'sliding_stb_partition_by_tbname_select': self.get_sliding_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'sliding_stb_partition_by_tag_select': self.get_sliding_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'sliding_tb_select': self.get_sliding_template(source_type='tb', partition_type='none', **fixed_kwargs),
                        
            # 会话窗口 - 所有组合
            'session_stb_tbname_select': self.get_session_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'session_stb_partition_by_tbname_select': self.get_session_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'session_stb_partition_by_tag_select': self.get_session_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'session_tb_select': self.get_session_template(source_type='tb', partition_type='none', **fixed_kwargs),
            
            # 计数窗口 - 所有组合
            'count_stb_tbname_select': self.get_count_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'count_stb_partition_by_tbname_select': self.get_count_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'count_stb_partition_by_tag_select': self.get_count_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'count_tb_select': self.get_count_template(source_type='tb', partition_type='none', **fixed_kwargs),
            
            # 事件窗口 - 所有组合
            'event_stb_tbname_select': self.get_event_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'event_stb_partition_by_tbname_select': self.get_event_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'event_stb_partition_by_tag_select': self.get_event_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'event_tb_select': self.get_event_template(source_type='tb', partition_type='none', **fixed_kwargs),
            
            # 状态窗口 - 所有组合
            'state_stb_tbname_select': self.get_state_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'state_stb_partition_by_tbname_select': self.get_state_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'state_stb_partition_by_tag_select': self.get_state_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'state_tb_select': self.get_state_template(source_type='tb', partition_type='none', **fixed_kwargs),
            
            # 定时触发 - 所有组合
            'period_stb_tbname_select': self.get_period_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'period_stb_partition_by_tbname_select': self.get_period_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'period_stb_partition_by_tag_select': self.get_period_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'period_tb_select': self.get_period_template(source_type='tb', partition_type='none', **fixed_kwargs),
        }

    def get_trows_agg_group(self, **kwargs):
        """获取 trows + agg 组合的所有窗口类型
        
        固定参数: tbname_or_trows_or_sourcetable='trows', agg_or_select='agg'
        忽略命令行传入的这两个参数
        """
        # 固定参数，忽略传入的参数
        fixed_kwargs = {k: v for k, v in kwargs.items() if k not in ['tbname_or_trows_or_sourcetable', 'agg_or_select']}
        fixed_kwargs.update({
            'tbname_or_trows_or_sourcetable': 'trows',
            'agg_or_select': 'agg'
        })
        
        return {
            # intervalsliding 窗口 - 所有组合
            'intervalsliding_stb_trows_agg': self.get_intervalsliding_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'intervalsliding_stb_partition_by_tbname_trows_agg': self.get_intervalsliding_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'intervalsliding_stb_partition_by_tag_trows_agg': self.get_intervalsliding_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'intervalsliding_tb_trows_agg': self.get_intervalsliding_template(source_type='tb', partition_type='none', **fixed_kwargs),
            
            # sliding 窗口 - 所有组合
            'sliding_stb_trows_agg': self.get_sliding_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'sliding_stb_partition_by_tbname_trows_agg': self.get_sliding_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'sliding_stb_partition_by_tag_trows_agg': self.get_sliding_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'sliding_tb_trows_agg': self.get_sliding_template(source_type='tb', partition_type='none', **fixed_kwargs),
                     
            # 会话窗口 - 所有组合
            'session_stb_trows_agg': self.get_session_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'session_stb_partition_by_tbname_trows_agg': self.get_session_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'session_stb_partition_by_tag_trows_agg': self.get_session_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'session_tb_trows_agg': self.get_session_template(source_type='tb', partition_type='none', **fixed_kwargs),
            
            # 计数窗口 - 所有组合
            'count_stb_trows_agg': self.get_count_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'count_stb_partition_by_tbname_trows_agg': self.get_count_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'count_stb_partition_by_tag_trows_agg': self.get_count_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'count_tb_trows_agg': self.get_count_template(source_type='tb', partition_type='none', **fixed_kwargs),
            
            # 事件窗口 - 所有组合
            'event_stb_trows_agg': self.get_event_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'event_stb_partition_by_tbname_trows_agg': self.get_event_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'event_stb_partition_by_tag_trows_agg': self.get_event_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'event_tb_trows_agg': self.get_event_template(source_type='tb', partition_type='none', **fixed_kwargs),
            
            # 状态窗口 - 所有组合
            'state_stb_trows_agg': self.get_state_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'state_stb_partition_by_tbname_trows_agg': self.get_state_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'state_stb_partition_by_tag_trows_agg': self.get_state_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'state_tb_trows_agg': self.get_state_template(source_type='tb', partition_type='none', **fixed_kwargs),
            
            # 定时触发 - 所有组合 (注意: period不支持trows，所以这里实际上仍然使用tbname)
            'period_stb_trows_agg': self.get_period_template(source_type='stb', partition_type='none', **{k: v for k, v in fixed_kwargs.items() if k != 'tbname_or_trows_or_sourcetable'}),
            'period_stb_partition_by_tbname_trows_agg': self.get_period_template(source_type='stb', partition_type='tbname', **{k: v for k, v in fixed_kwargs.items() if k != 'tbname_or_trows_or_sourcetable'}),
            'period_stb_partition_by_tag_trows_agg': self.get_period_template(source_type='stb', partition_type='tag', **{k: v for k, v in fixed_kwargs.items() if k != 'tbname_or_trows_or_sourcetable'}),
            'period_tb_trows_agg': self.get_period_template(source_type='tb', partition_type='none', **{k: v for k, v in fixed_kwargs.items() if k != 'tbname_or_trows_or_sourcetable'}),
        }

    def get_trows_select_group(self, **kwargs):
        """获取 trows + select 组合的所有窗口类型
        
        固定参数: tbname_or_trows_or_sourcetable='trows', agg_or_select='select'
        忽略命令行传入的这两个参数
        """
        # 固定参数，忽略传入的参数
        fixed_kwargs = {k: v for k, v in kwargs.items() if k not in ['tbname_or_trows_or_sourcetable', 'agg_or_select']}
        fixed_kwargs.update({
            'tbname_or_trows_or_sourcetable': 'trows',
            'agg_or_select': 'select'
        })
        
        return {
            # intervalsliding 窗口 - 所有组合
            'intervalsliding_stb_trows_select': self.get_intervalsliding_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'intervalsliding_stb_partition_by_tbname_trows_select': self.get_intervalsliding_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'intervalsliding_stb_partition_by_tag_trows_select': self.get_intervalsliding_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'intervalsliding_tb_trows_select': self.get_intervalsliding_template(source_type='tb', partition_type='none', **fixed_kwargs),
             
            # sliding 窗口 - 所有组合
            'sliding_stb_trows_select': self.get_sliding_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'sliding_stb_partition_by_tbname_trows_select': self.get_sliding_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'sliding_stb_partition_by_tag_trows_select': self.get_sliding_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'sliding_tb_trows_select': self.get_sliding_template(source_type='tb', partition_type='none', **fixed_kwargs),
                       
            # 会话窗口 - 所有组合
            'session_stb_trows_select': self.get_session_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'session_stb_partition_by_tbname_trows_select': self.get_session_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'session_stb_partition_by_tag_trows_select': self.get_session_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'session_tb_trows_select': self.get_session_template(source_type='tb', partition_type='none', **fixed_kwargs),
            
            # 计数窗口 - 所有组合
            'count_stb_trows_select': self.get_count_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'count_stb_partition_by_tbname_trows_select': self.get_count_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'count_stb_partition_by_tag_trows_select': self.get_count_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'count_tb_trows_select': self.get_count_template(source_type='tb', partition_type='none', **fixed_kwargs),
            
            # 事件窗口 - 所有组合
            'event_stb_trows_select': self.get_event_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'event_stb_partition_by_tbname_trows_select': self.get_event_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'event_stb_partition_by_tag_trows_select': self.get_event_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'event_tb_trows_select': self.get_event_template(source_type='tb', partition_type='none', **fixed_kwargs),
            
            # 状态窗口 - 所有组合
            'state_stb_trows_select': self.get_state_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'state_stb_partition_by_tbname_trows_select': self.get_state_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'state_stb_partition_by_tag_trows_select': self.get_state_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'state_tb_trows_select': self.get_state_template(source_type='tb', partition_type='none', **fixed_kwargs),
            
            # 定时触发 - 所有组合 (注意: period不支持trows，所以这里实际上仍然使用tbname)
            'period_stb_trows_select': self.get_period_template(source_type='stb', partition_type='none', **{k: v for k, v in fixed_kwargs.items() if k != 'tbname_or_trows_or_sourcetable'}),
            'period_stb_partition_by_tbname_trows_select': self.get_period_template(source_type='stb', partition_type='tbname', **{k: v for k, v in fixed_kwargs.items() if k != 'tbname_or_trows_or_sourcetable'}),
            'period_stb_partition_by_tag_trows_select': self.get_period_template(source_type='stb', partition_type='tag', **{k: v for k, v in fixed_kwargs.items() if k != 'tbname_or_trows_or_sourcetable'}),
            'period_tb_trows_select': self.get_period_template(source_type='tb', partition_type='none', **{k: v for k, v in fixed_kwargs.items() if k != 'tbname_or_trows_or_sourcetable'}),
        }
        
    def get_sourcetable_agg_group(self, **kwargs):
        """获取 sourcetable + agg 组合的所有窗口类型
        
        固定参数: tbname_or_trows_or_sourcetable='sourcetable', agg_or_select='agg'
        忽略命令行传入的这两个参数
        """
        # 固定参数，忽略传入的参数
        fixed_kwargs = {k: v for k, v in kwargs.items() if k not in ['tbname_or_trows_or_sourcetable', 'agg_or_select']}
        fixed_kwargs.update({
            'tbname_or_trows_or_sourcetable': 'sourcetable',
            'agg_or_select': 'agg'
        })
        
        return {
            # intervalsliding 窗口 - 所有组合
            'intervalsliding_stb_sourcetable_agg': self.get_intervalsliding_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'intervalsliding_stb_partition_by_tbname_sourcetable_agg': self.get_intervalsliding_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'intervalsliding_stb_partition_by_tag_sourcetable_agg': self.get_intervalsliding_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'intervalsliding_tb_sourcetable_agg': self.get_intervalsliding_template(source_type='tb', partition_type='none', **fixed_kwargs),
            
            # sliding 窗口 - 所有组合
            'sliding_stb_sourcetable_agg': self.get_sliding_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'sliding_stb_partition_by_tbname_sourcetable_agg': self.get_sliding_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'sliding_stb_partition_by_tag_sourcetable_agg': self.get_sliding_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'sliding_tb_sourcetable_agg': self.get_sliding_template(source_type='tb', partition_type='none', **fixed_kwargs),
                        
            # 会话窗口 - 所有组合
            'session_stb_sourcetable_agg': self.get_session_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'session_stb_partition_by_tbname_sourcetable_agg': self.get_session_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'session_stb_partition_by_tag_sourcetable_agg': self.get_session_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'session_tb_sourcetable_agg': self.get_session_template(source_type='tb', partition_type='none', **fixed_kwargs),
            
            # 计数窗口 - 所有组合
            'count_stb_sourcetable_agg': self.get_count_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'count_stb_partition_by_tbname_sourcetable_agg': self.get_count_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'count_stb_partition_by_tag_sourcetable_agg': self.get_count_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'count_tb_sourcetable_agg': self.get_count_template(source_type='tb', partition_type='none', **fixed_kwargs),
            
            # 事件窗口 - 所有组合
            'event_stb_sourcetable_agg': self.get_event_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'event_stb_partition_by_tbname_sourcetable_agg': self.get_event_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'event_stb_partition_by_tag_sourcetable_agg': self.get_event_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'event_tb_sourcetable_agg': self.get_event_template(source_type='tb', partition_type='none', **fixed_kwargs),
            
            # 状态窗口 - 所有组合
            'state_stb_sourcetable_agg': self.get_state_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'state_stb_partition_by_tbname_sourcetable_agg': self.get_state_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'state_stb_partition_by_tag_sourcetable_agg': self.get_state_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'state_tb_sourcetable_agg': self.get_state_template(source_type='tb', partition_type='none', **fixed_kwargs),
            
            # 定时触发 - 所有组合 (注意: period支持sourcetable)
            'period_stb_sourcetable_agg': self.get_period_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'period_stb_partition_by_tbname_sourcetable_agg': self.get_period_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'period_stb_partition_by_tag_sourcetable_agg': self.get_period_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'period_tb_sourcetable_agg': self.get_period_template(source_type='tb', partition_type='none', **fixed_kwargs),
        }


    def get_sourcetable_select_group(self, **kwargs):
        """获取 sourcetable + select 组合的所有窗口类型
        
        固定参数: tbname_or_trows_or_sourcetable='sourcetable', agg_or_select='select'
        忽略命令行传入的这两个参数
        """
        # 固定参数，忽略传入的参数
        fixed_kwargs = {k: v for k, v in kwargs.items() if k not in ['tbname_or_trows_or_sourcetable', 'agg_or_select']}
        fixed_kwargs.update({
            'tbname_or_trows_or_sourcetable': 'sourcetable',
            'agg_or_select': 'select'
        })
        
        return {
            # intervalsliding 窗口 - 所有组合
            'intervalsliding_stb_sourcetable_select': self.get_intervalsliding_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'intervalsliding_stb_partition_by_tbname_sourcetable_select': self.get_intervalsliding_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'intervalsliding_stb_partition_by_tag_sourcetable_select': self.get_intervalsliding_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'intervalsliding_tb_sourcetable_select': self.get_intervalsliding_template(source_type='tb', partition_type='none', **fixed_kwargs),
            
            # sliding 窗口 - 所有组合
            'sliding_stb_sourcetable_select': self.get_sliding_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'sliding_stb_partition_by_tbname_sourcetable_select': self.get_sliding_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'sliding_stb_partition_by_tag_sourcetable_select': self.get_sliding_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'sliding_tb_sourcetable_select': self.get_sliding_template(source_type='tb', partition_type='none', **fixed_kwargs),
                        
            # 会话窗口 - 所有组合
            'session_stb_sourcetable_select': self.get_session_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'session_stb_partition_by_tbname_sourcetable_select': self.get_session_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'session_stb_partition_by_tag_sourcetable_select': self.get_session_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'session_tb_sourcetable_select': self.get_session_template(source_type='tb', partition_type='none', **fixed_kwargs),
            
            # 计数窗口 - 所有组合
            'count_stb_sourcetable_select': self.get_count_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'count_stb_partition_by_tbname_sourcetable_select': self.get_count_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'count_stb_partition_by_tag_sourcetable_select': self.get_count_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'count_tb_sourcetable_select': self.get_count_template(source_type='tb', partition_type='none', **fixed_kwargs),
            
            # 事件窗口 - 所有组合
            'event_stb_sourcetable_select': self.get_event_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'event_stb_partition_by_tbname_sourcetable_select': self.get_event_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'event_stb_partition_by_tag_sourcetable_select': self.get_event_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'event_tb_sourcetable_select': self.get_event_template(source_type='tb', partition_type='none', **fixed_kwargs),
            
            # 状态窗口 - 所有组合
            'state_stb_sourcetable_select': self.get_state_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'state_stb_partition_by_tbname_sourcetable_select': self.get_state_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'state_stb_partition_by_tag_sourcetable_select': self.get_state_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'state_tb_sourcetable_select': self.get_state_template(source_type='tb', partition_type='none', **fixed_kwargs),
            
            # 定时触发 - 所有组合 (注意: period支持sourcetable)
            'period_stb_sourcetable_select': self.get_period_template(source_type='stb', partition_type='none', **fixed_kwargs),
            'period_stb_partition_by_tbname_sourcetable_select': self.get_period_template(source_type='stb', partition_type='tbname', **fixed_kwargs),
            'period_stb_partition_by_tag_sourcetable_select': self.get_period_template(source_type='stb', partition_type='tag', **fixed_kwargs),
            'period_tb_sourcetable_select': self.get_period_template(source_type='tb', partition_type='none', **fixed_kwargs),
        }

        
    def generate_multiple_streams(self, base_sql, stream_num=1):
        """根据基础SQL模板生成多个编号的流
        
        Args:
            base_sql: 基础SQL模板
            stream_num: 要生成的流数量
            
        Returns:
            dict: 包含多个流SQL的字典，key为流名称，value为SQL
        """
        if stream_num <= 1:
            return {'stream_1': base_sql}
        
        result = {}
        
        if isinstance(base_sql, str):
            for i in range(1, stream_num + 1):
                modified_sql = self._modify_sql_for_multiple_streams(base_sql, i)
                result[f'stream_{i}'] = modified_sql
        else:
            # 如果需要重新生成（用于支持不同子表的情况）
            # 这种情况下base_sql应该包含生成函数和参数信息
            # 暂时保持原有逻辑，这个分支主要用于向后兼容
            for i in range(1, stream_num + 1):
                modified_sql = self._modify_sql_for_multiple_streams(base_sql, i)
                result[f'stream_{i}'] = modified_sql
            
        return result

    def _modify_sql_for_multiple_streams(self, sql, stream_index):
        """修改SQL以支持多流创建
        
        Args:
            sql: 原始SQL
            stream_index: 流编号
            
        Returns:
            str: 修改后的SQL
        """
        import re
        
        # 1. 修改流名称，添加编号后缀
        # 匹配 "create stream xxx" 模式
        sql = re.sub(
            r'(create\s+stream\s+)([^\s]+)',
            rf'\1\2_{stream_index}',
            sql,
            flags=re.IGNORECASE
        )
        
        # 2. 修改目标表名称，添加编号后缀
        # 匹配 "into stream_to.xxx" 模式
        sql = re.sub(
            r'(into\s+)(stream_to\.)([^\s]+)',
            rf'\1\2\3_{stream_index}',
            sql,
            flags=re.IGNORECASE
        )
        
        return sql
        
    '''                    
    # ========== old流模板，仍然支持，作为第一轮的摸底测试，但不推荐使用了，使用上面的建流模版进行第二轮测试 (s2_2 到 s2_6) ==========
    def get_s2_2(self, agg_or_select='agg', custom_columns=None):
        """trigger at_once"""
        columns = self._build_columns(agg_or_select, custom_columns)
        return f"""
    create stream s2_2 trigger at_once 
        ignore expired 0 ignore update 0 into stream_to.stb2_2 
        as select _wstart as wstart,
        {columns}
        from stream_from.stb partition by tbname interval(15s);
    """
    
    def get_s2_3(self, agg_or_select='agg', custom_columns=None):
        """trigger window_close"""
        columns = self._build_columns(agg_or_select, custom_columns)
        return f"""
    create stream s2_3 trigger window_close 
        ignore expired 0 ignore update 0 into stream_to.stb2_3 
        as select _wstart as wstart,
        {columns}
        from stream_from.stb partition by tbname interval(15s);
    """
    
    def get_s2_4(self, agg_or_select='agg', custom_columns=None):
        """trigger max_delay 5s"""
        columns = self._build_columns(agg_or_select, custom_columns)
        return f"""
    create stream s2_4 trigger max_delay 5s 
        ignore expired 0 ignore update 0 into stream_to.stb2_4 
        as select _wstart as wstart,
        {columns}
        from stream_from.stb partition by tbname interval(15s);
    """
    
    def get_s2_5(self, agg_or_select='agg', custom_columns=None):
        """trigger FORCE_WINDOW_CLOSE"""
        columns = self._build_columns(agg_or_select, custom_columns)
        return f"""
    create stream s2_5 trigger FORCE_WINDOW_CLOSE into stream_to.stb2_5 
        as select _wstart as wstart,
        {columns}
        from stream_from.stb partition by tbname interval(15s);
    """
    
    def get_s2_6(self, agg_or_select='agg', custom_columns=None):
        """trigger CONTINUOUS_WINDOW_CLOSE"""
        columns = self._build_columns(agg_or_select, custom_columns)
        return f"""
    create stream s2_6 trigger CONTINUOUS_WINDOW_CLOSE 
        ignore expired 0 ignore update 0 into stream_to.stb2_6 
        as select _wstart as wstart,
        {columns}
        from stream_from.stb partition by tbname interval(15s);
    """
    
    # ========== new流模板，仍然支持，作为第一轮的摸底测试，但不推荐使用了，使用上面的建流模版进行第二轮测试  (s2_7 到 s2_15) ==========
    def get_s2_7(self, agg_or_select='agg', tbname_or_trows_or_sourcetable='tbname', custom_columns=None):
        """INTERVAL(15s) 窗口"""
        columns = self._build_columns(agg_or_select, custom_columns)
        from_clause = self._build_from_clause(tbname_or_trows_or_sourcetable)
        return f"""
    create stream stream_from.s2_7 INTERVAL(15s) SLIDING(15s)
            from stream_from.stb 
            partition by tbname 
            into stream_to.stb2_7
            as select _twstart ts, {columns}
            from {from_clause};
    """
    
    def get_s2_8(self, agg_or_select='agg', tbname_or_trows_or_sourcetable='trows', custom_columns=None):
        """INTERVAL with %%trows"""
        columns = self._build_columns(agg_or_select, custom_columns)
        from_clause = self._build_from_clause(tbname_or_trows_or_sourcetable)
        return f"""
    create stream stream_from.s2_8 INTERVAL(15s) SLIDING(15s)
            from stream_from.stb 
            partition by tbname 
            into stream_to.stb2_8
            as select _twstart ts, {columns}
            from {from_clause};
    """
    
    def get_s2_9(self, agg_or_select='agg', tbname_or_trows_or_sourcetable='tbname', custom_columns=None):
        """INTERVAL with MAX_DELAY"""
        columns = self._build_columns(agg_or_select, custom_columns)
        from_clause = self._build_from_clause(tbname_or_trows_or_sourcetable)
        return f"""
    create stream stream_from.s2_9 INTERVAL(15s) SLIDING(15s)
            from stream_from.stb partition by tbname 
            STREAM_OPTIONS(MAX_DELAY(5s)) 
            into stream_to.stb2_9
            as select _twstart ts, {columns}
            from {from_clause};
    """
    
    def get_s2_10(self, agg_or_select='agg', tbname_or_trows_or_sourcetable='trows', custom_columns=None):
        """INTERVAL with MAX_DELAY and %%trows"""
        columns = self._build_columns(agg_or_select, custom_columns)
        from_clause = self._build_from_clause(tbname_or_trows_or_sourcetable)
        return f"""
    create stream stream_from.s2_10 INTERVAL(15s) SLIDING(15s) 
            from stream_from.stb 
            STREAM_OPTIONS(MAX_DELAY(5s))
            into stream_to.stb2_10
            as select _twstart ts, {columns}
            from {from_clause};
    """
    
    def get_s2_11(self, agg_or_select='agg', custom_columns=None):
        """PERIOD(15s) 定时触发"""
        columns = self._build_columns(agg_or_select, custom_columns)
        return f"""
    create stream stream_from.s2_11 period(15s) 
            from stream_from.stb partition by tbname  
            into stream_to.stb2_11
            as select cast(_tlocaltime/1000000 as timestamp) ts, {columns}
            from %%tbname;
    """
    
    def get_s2_12(self, agg_or_select='agg', tbname_or_trows_or_sourcetable='tbname', custom_columns=None):
        """SESSION(ts,10a) 会话窗口"""
        columns = self._build_columns(agg_or_select, custom_columns)
        from_clause = self._build_from_clause(tbname_or_trows_or_sourcetable)
        return f"""
    create stream stream_from.s2_12 session(ts,10a)
            from stream_from.stb partition by tbname 
            into stream_to.stb2_12
            as select _twstart ts, {columns}
            from {from_clause};
    """
    
    def get_s2_13(self, agg_or_select='agg', tbname_or_trows_or_sourcetable='tbname', custom_columns=None):
        """COUNT_WINDOW(1000) 计数窗口"""
        columns = self._build_columns(agg_or_select, custom_columns)
        from_clause = self._build_from_clause(tbname_or_trows_or_sourcetable)
        return f"""
    create stream stream_from.s2_13 COUNT_WINDOW(1000)
            from stream_from.stb partition by tbname 
            into stream_to.stb2_13
            as select _twstart ts, {columns}
            from {from_clause};
    """
    
    def get_s2_14(self, agg_or_select='agg', tbname_or_trows_or_sourcetable='tbname', custom_columns=None):
        """EVENT_WINDOW 事件窗口"""
        columns = self._build_columns(agg_or_select, custom_columns)
        from_clause = self._build_from_clause(tbname_or_trows_or_sourcetable)
        return f"""
    create stream stream_from.s2_14 EVENT_WINDOW(START WITH c0 > -10000000 END WITH c0 < 10000000) 
            from stream_from.stb partition by tbname 
            into stream_to.stb2_14
            as select _twstart ts, {columns}
            from {from_clause};
    """
    
    def get_s2_15(self, agg_or_select='agg', tbname_or_trows_or_sourcetable='tbname', custom_columns=None):
        """STATE_WINDOW 状态窗口"""
        columns = self._build_columns(agg_or_select, custom_columns)
        from_clause = self._build_from_clause(tbname_or_trows_or_sourcetable)
        return f"""
    create stream stream_from.s2_15 STATE_WINDOW(c0) 
            from stream_from.stb partition by tbname 
            into stream_to.stb2_15
            as select _twstart ts, {columns}
            from {from_clause};
    """
    
    # ========== 分组和批量获取方法 ==========
    def get_intervalsliding_group(self, **kwargs):
        """获取滑动窗口组 (s2_7, s2_8, s2_9, s2_10)"""
        return {
            's2_7': self.get_s2_7(**kwargs),
            's2_8': self.get_s2_8(**kwargs),
            's2_9': self.get_s2_9(**kwargs),
            's2_10': self.get_s2_10(**kwargs)
        }
    
    def get_count_group(self, **kwargs):
        """获取计数窗口组 (s2_13)"""
        return {
            's2_13': self.get_s2_13(**kwargs)
        }
    
    def get_session_group(self, **kwargs):
        """获取会话窗口组 (s2_12)"""
        return {
            's2_12': self.get_s2_12(**kwargs)
        }
    
    def get_period_group(self, **kwargs):
        """获取定时触发组 (s2_11)"""
        # period 类型不支持 tbname_or_trows_or_sourcetable 参数
        filtered_kwargs = {k: v for k, v in kwargs.items() if k != 'tbname_or_trows_or_sourcetable'}
        return {
            's2_11': self.get_s2_11(**filtered_kwargs)
        }
    
    def get_event_group(self, **kwargs):
        """获取事件窗口组 (s2_14)"""
        return {
            's2_14': self.get_s2_14(**kwargs)
        }
    
    def get_state_group(self, **kwargs):
        """获取状态窗口组 (s2_15)"""
        return {
            's2_15': self.get_s2_15(**kwargs)
        }
    
    def get_basic_group(self, **kwargs):
        """获取基础触发组 (s2_2 到 s2_6)"""
        # 基础组不支持 tbname_or_trows_or_sourcetable 参数
        filtered_kwargs = {k: v for k, v in kwargs.items() if k != 'tbname_or_trows_or_sourcetable'}
        return {
            's2_2': self.get_s2_2(**filtered_kwargs),
            's2_3': self.get_s2_3(**filtered_kwargs),
            's2_4': self.get_s2_4(**filtered_kwargs),
            's2_5': self.get_s2_5(**filtered_kwargs),
            's2_6': self.get_s2_6(**filtered_kwargs)
        }
    
    def get_all_advanced(self, **kwargs):
        """获取所有高级流 (s2_7 到 s2_15)"""
        result = {}
        result.update(self.get_intervalsliding_group(**kwargs))
        result.update(self.get_period_group(**kwargs))
        result.update(self.get_session_group(**kwargs))
        result.update(self.get_count_group(**kwargs))
        result.update(self.get_event_group(**kwargs))
        result.update(self.get_state_group(**kwargs))
        return result
    
    def get_all(self, **kwargs):
        """获取所有流模板"""
        result = {}
        result.update(self.get_basic_group(**kwargs))
        result.update(self.get_all_advanced(**kwargs))
        return result
    
    # @classmethod
    # def get_sql_bak(cls, sql_type, **kwargs):
    #     """
    #     获取指定类型的 SQL 模板 - 主要入口方法
        
    #     Args:
    #         sql_type: SQL 类型标识符或分组名
    #         **kwargs: 可选参数
    #             - agg_or_select: 'agg'(默认) 或 'select'，控制聚合还是投影查询
    #             - tbname_or_trows_or_sourcetable: 'tbname'(默认) 或 'trows'，控制FROM子句
    #             - custom_columns: 自定义列，如果提供则使用自定义列
                
    #     Returns:
    #         str or dict: 单个SQL字符串或SQL字典
            
    #     Usage Examples:
    #         # 获取单个模板
    #         sql = StreamSQLTemplates.get_sql('s2_7')
            
    #         # 使用投影查询
    #         sql = StreamSQLTemplates.get_sql('s2_7', agg_or_select='select')
            
    #         # 使用trows
    #         sql = StreamSQLTemplates.get_sql('s2_8', tbname_or_trows_or_sourcetable='trows')
            
    #         # 自定义列
    #         sql = StreamSQLTemplates.get_sql('s2_7', custom_columns=['sum(c0)', 'count(*)'])
            
    #         # 获取分组
    #         sqls = StreamSQLTemplates.get_sql('intervalsliding')  # 滑动窗口组
    #         sqls = StreamSQLTemplates.get_sql('all')      # 所有模板
    #     """
    #     instance = cls()
        
    #     # 单个模板映射
    #     single_templates = {
    #         's2_2': instance.get_s2_2,
    #         's2_3': instance.get_s2_3,
    #         's2_4': instance.get_s2_4,
    #         's2_5': instance.get_s2_5,
    #         's2_6': instance.get_s2_6,
    #         's2_7': instance.get_s2_7,
    #         's2_8': instance.get_s2_8,
    #         's2_9': instance.get_s2_9,
    #         's2_10': instance.get_s2_10,
    #         's2_11': instance.get_s2_11,
    #         's2_12': instance.get_s2_12,
    #         's2_13': instance.get_s2_13,
    #         's2_14': instance.get_s2_14,
    #         's2_15': instance.get_s2_15,
    #     }
        
    #     # 分组模板映射
    #     group_templates = {
    #         'basic': instance.get_basic_group,
    #         'intervalsliding': instance.get_intervalsliding_group,
    #         'count': instance.get_count_group,
    #         'session': instance.get_session_group,
    #         'period': instance.get_period_group,
    #         'event': instance.get_event_group,
    #         'state': instance.get_state_group,
    #         'advanced': instance.get_all_advanced,
    #         'all': instance.get_all,
    #     }
        
    #     # 处理单个模板
    #     if sql_type in single_templates:
    #         method = single_templates[sql_type]
    #         # 为不同类型的模板过滤参数
    #         if sql_type in ['s2_2', 's2_3', 's2_4', 's2_5', 's2_6', 's2_11']:
    #             # 基础模板和period模板不支持 tbname_or_trows_or_sourcetable
    #             filtered_kwargs = {k: v for k, v in kwargs.items() if k != 'tbname_or_trows_or_sourcetable'}
    #             return method(**filtered_kwargs)
    #         else:
    #             return method(**kwargs)
        
    #     # 处理分组模板
    #     if sql_type in group_templates:
    #         return group_templates[sql_type](**kwargs)
        
    #     # 默认返回 s2_7
    #     print(f"警告: 未找到模板 '{sql_type}'，使用默认模板 's2_7'")
    #     return instance.get_s2_7(**kwargs)
    
    '''
    
    @classmethod
    def get_sql(cls, sql_type, stream_num=1, auto_combine=False, **kwargs):
        """
        获取指定类型的 SQL 模板 - 主要入口方法
        
        Args:
            sql_type: SQL 类型标识符或分组名
                单个模板: 'intervalsliding_stb', 'intervalsliding_stb_partition_by_tbname' 等
                组合模板: 'intervalsliding_detailed', 'session_detailed' 等
            stream_num: 流数量（仅对单个流类型有效）
            auto_combine: 是否自动生成6种参数组合
            **kwargs: 可选参数
                
        Returns:
            str or dict: 单个SQL字符串或SQL字典
            
        Usage Examples:
            # 获取单个详细模板
            sql = StreamSQLTemplates.get_sql('intervalsliding_stb_partition_by_tbname')
            
            # 获取详细组合
            sqls = StreamSQLTemplates.get_sql('intervalsliding_detailed')
            
            # 获取所有窗口类型的详细组合
            sqls = StreamSQLTemplates.get_sql('all_detailed')
        """
        instance = cls()
        
        # 处理特殊的查询语句
        if sql_type == 'select_stream':
            return instance.get_select_stream(**kwargs)
        
        if sql_type == 'tbname_agg':
            if stream_num > 1:
                print("警告: 固定参数组合模板不支持 --stream-num 参数，将忽略该参数")
            return instance.get_tbname_agg_group(**kwargs)
        elif sql_type == 'tbname_select':
            if stream_num > 1:
                print("警告: 固定参数组合模板不支持 --stream-num 参数，将忽略该参数")
            return instance.get_tbname_select_group(**kwargs)
        elif sql_type == 'trows_agg':
            if stream_num > 1:
                print("警告: 固定参数组合模板不支持 --stream-num 参数，将忽略该参数")
            return instance.get_trows_agg_group(**kwargs)
        elif sql_type == 'trows_select':
            if stream_num > 1:
                print("警告: 固定参数组合模板不支持 --stream-num 参数，将忽略该参数")
            return instance.get_trows_select_group(**kwargs)
        elif sql_type == 'sourcetable_agg':
            if stream_num > 1:
                print("警告: 固定参数组合模板不支持 --stream-num 参数，将忽略该参数")
            return instance.get_sourcetable_agg_group(**kwargs)
        elif sql_type == 'sourcetable_select':
            if stream_num > 1:
                print("警告: 固定参数组合模板不支持 --stream-num 参数，将忽略该参数")
            return instance.get_sourcetable_select_group(**kwargs)
        
        # 单个详细模板映射
        detailed_templates = {
            'intervalsliding_stb': lambda stream_index=None, **kw: instance.get_intervalsliding_template(source_type='stb', partition_type='none', stream_index=stream_index, **kw),
            'intervalsliding_stb_partition_by_tbname': lambda stream_index=None, **kw: instance.get_intervalsliding_template(source_type='stb', partition_type='tbname', stream_index=stream_index, **kw),
            'intervalsliding_stb_partition_by_tag': lambda stream_index=None, **kw: instance.get_intervalsliding_template(source_type='stb', partition_type='tag', stream_index=stream_index, **kw),
            'intervalsliding_tb': lambda stream_index=None, **kw: instance.get_intervalsliding_template(source_type='tb', partition_type='none', stream_index=stream_index, **kw),
            
            'sliding_stb': lambda stream_index=None, **kw: instance.get_sliding_template(source_type='stb', partition_type='none', stream_index=stream_index, **kw),
            'sliding_stb_partition_by_tbname': lambda stream_index=None, **kw: instance.get_sliding_template(source_type='stb', partition_type='tbname', stream_index=stream_index, **kw),
            'sliding_stb_partition_by_tag': lambda stream_index=None, **kw: instance.get_sliding_template(source_type='stb', partition_type='tag', stream_index=stream_index, **kw),
            'sliding_tb': lambda stream_index=None, **kw: instance.get_sliding_template(source_type='tb', partition_type='none', stream_index=stream_index, **kw),
                       
            'session_stb': lambda stream_index=None, **kw: instance.get_session_template(source_type='stb', partition_type='none', stream_index=stream_index, **kw),
            'session_stb_partition_by_tbname': lambda stream_index=None, **kw: instance.get_session_template(source_type='stb', partition_type='tbname', stream_index=stream_index, **kw),
            'session_stb_partition_by_tag': lambda stream_index=None, **kw: instance.get_session_template(source_type='stb', partition_type='tag', stream_index=stream_index, **kw),
            'session_tb': lambda stream_index=None, **kw: instance.get_session_template(source_type='tb', partition_type='none', stream_index=stream_index, **kw),
            
            'count_stb': lambda stream_index=None, **kw: instance.get_count_template(source_type='stb', partition_type='none', stream_index=stream_index, **kw),
            'count_stb_partition_by_tbname': lambda stream_index=None, **kw: instance.get_count_template(source_type='stb', partition_type='tbname', stream_index=stream_index, **kw),
            'count_stb_partition_by_tag': lambda stream_index=None, **kw: instance.get_count_template(source_type='stb', partition_type='tag', stream_index=stream_index, **kw),
            'count_tb': lambda stream_index=None, **kw: instance.get_count_template(source_type='tb', partition_type='none', stream_index=stream_index, **kw),
            
            'event_stb': lambda stream_index=None, **kw: instance.get_event_template(source_type='stb', partition_type='none', stream_index=stream_index, **kw),
            'event_stb_partition_by_tbname': lambda stream_index=None, **kw: instance.get_event_template(source_type='stb', partition_type='tbname', stream_index=stream_index, **kw),
            'event_stb_partition_by_tag': lambda stream_index=None, **kw: instance.get_event_template(source_type='stb', partition_type='tag', stream_index=stream_index, **kw),
            'event_tb': lambda stream_index=None, **kw: instance.get_event_template(source_type='tb', partition_type='none', stream_index=stream_index, **kw),
            
            'state_stb': lambda stream_index=None, **kw: instance.get_state_template(source_type='stb', partition_type='none', stream_index=stream_index, **kw),
            'state_stb_partition_by_tbname': lambda stream_index=None, **kw: instance.get_state_template(source_type='stb', partition_type='tbname', stream_index=stream_index, **kw),
            'state_stb_partition_by_tag': lambda stream_index=None, **kw: instance.get_state_template(source_type='stb', partition_type='tag', stream_index=stream_index, **kw),
            'state_tb': lambda stream_index=None, **kw: instance.get_state_template(source_type='tb', partition_type='none', stream_index=stream_index, **kw),
            
            'period_stb': lambda stream_index=None, **kw: instance.get_period_template(source_type='stb', partition_type='none', stream_index=stream_index, **kw),
            'period_stb_partition_by_tbname': lambda stream_index=None, **kw: instance.get_period_template(source_type='stb', partition_type='tbname', stream_index=stream_index, **kw),
            'period_stb_partition_by_tag': lambda stream_index=None, **kw: instance.get_period_template(source_type='stb', partition_type='tag', stream_index=stream_index, **kw),
            'period_tb': lambda stream_index=None, **kw: instance.get_period_template(source_type='tb', partition_type='none', stream_index=stream_index, **kw),
        }
        
        # 组合模板映射
        group_templates = {
            'intervalsliding_detailed': instance.get_intervalsliding_group_detailed,
            'sliding_detailed': instance.get_sliding_group_detailed, 
            'session_detailed': instance.get_session_group_detailed,
            'count_detailed': instance.get_count_group_detailed,
            'event_detailed': instance.get_event_group_detailed,
            'state_detailed': instance.get_state_group_detailed,
            'period_detailed': instance.get_period_group_detailed,
        }
        
        # 处理单个详细模板时
        if sql_type in detailed_templates:
            # 检查是否启用自动组合
            if auto_combine:
                print(f"自动生成 {sql_type} 的6种参数组合 (忽略 --agg-or-select 和 --tbname-or-trows-or-sourcetable 参数)")
                
                if stream_num > 1:
                    print("警告: --auto-combine 模式下 --stream-num 参数无效，将忽略")
                
                # 生成6种组合
                combinations = [
                    ('tbname', 'agg'),
                    ('tbname', 'select'),
                    ('trows', 'agg'),
                    ('trows', 'select'),
                    ('sourcetable', 'agg'),
                    ('sourcetable', 'select')
                ]
                
                result = {}
                for tbname_param, agg_param in combinations:
                    # 创建参数副本，覆盖指定参数
                    combo_kwargs = kwargs.copy()
                    combo_kwargs['tbname_or_trows_or_sourcetable'] = tbname_param
                    combo_kwargs['agg_or_select'] = agg_param
                    
                    # 生成组合名称
                    combo_name = f"{sql_type}_{agg_param}_{tbname_param}"
                    
                    # 生成SQL
                    combo_sql = detailed_templates[sql_type](**combo_kwargs)
                    result[combo_name] = combo_sql
                    
                    print(f"  生成组合: {combo_name}")
                
                return result
                
            else:
                # 原有逻辑：根据 stream_num 处理
                if stream_num > 1:
                    print(f"生成 {stream_num} 个 {sql_type} 类型的流")
                    result = {}
                    
                    for i in range(1, stream_num + 1):
                        stream_sql = detailed_templates[sql_type](stream_index=i, **kwargs)
                        result[f'stream_{i}'] = stream_sql
                        
                    return result
                else:
                    return detailed_templates[sql_type](**kwargs)
        
        # 处理组合模板（支持 stream_num）（不受 auto_combine 影响）
        if sql_type in group_templates:
            if auto_combine:
                print("警告: 组合模板不支持 --auto-combine 参数，将忽略该参数")
                
            base_sqls = group_templates[sql_type](**kwargs)
            
            # 如果stream_num > 1，为每个组合中的每个流生成多个副本
            if stream_num > 1:
                print(f"生成 {sql_type} 组合，每种流类型创建 {stream_num} 个流")
                result = {}
                
                for base_name, base_sql in base_sqls.items():
                    # 为每个基础流生成多个编号副本
                    multiple_streams = instance.generate_multiple_streams(base_sql, stream_num)
                    
                    # 重命名键值以避免冲突
                    for stream_key, stream_sql in multiple_streams.items():
                        # 例如: intervalsliding_stb_1, intervalsliding_stb_2, intervalsliding_stb_partition_by_tbname_1, etc.
                        new_key = f"{base_name}_{stream_key.split('_')[-1]}"  # 提取编号
                        result[new_key] = stream_sql
                
                return result
            else:
                return base_sqls
        
        # 处理特殊组合
        if sql_type == 'all_detailed':
            if auto_combine:
                print("警告: all_detailed 模板不支持 --auto-combine 参数，将忽略该参数")
                
            result = {}
            for group_type in ['intervalsliding_detailed', 'sliding_detailed', 'session_detailed', 'count_detailed', 
                            'event_detailed', 'state_detailed', 'period_detailed']:
                group_sqls = group_templates[group_type](**kwargs)
                
                # 如果stream_num > 1，为每个流生成多个副本
                if stream_num > 1:
                    for base_name, base_sql in group_sqls.items():
                        multiple_streams = instance.generate_multiple_streams(base_sql, stream_num)
                        
                        for stream_key, stream_sql in multiple_streams.items():
                            new_key = f"{base_name}_{stream_key.split('_')[-1]}"
                            result[new_key] = stream_sql
                else:
                    result.update(group_sqls)
                    
            if stream_num > 1:
                print(f"生成 all_detailed 组合，每种流类型创建 {stream_num} 个流")
                
            return result
        
        # 默认返回 select_stream
        print(f"警告: 未找到模板 '{sql_type}'，使用默认模板 'select_stream'")
        return instance.get_select_stream(**kwargs)

            
def format_delay_time(delay_ms):
    """将延迟毫秒数格式化为易读的时间格式
    
    Args:
        delay_ms: 延迟毫秒数
        
    Returns:
        str: 格式化后的时间字符串
    """
    if delay_ms is None:
        return "N/A"
    
    if delay_ms < 1000:  # 小于1秒，显示毫秒
        return f"{delay_ms}ms"
    elif delay_ms < 60000:  # 小于1分钟，显示秒
        seconds = delay_ms / 1000.0
        return f"{seconds:.1f}s"
    elif delay_ms < 3600000:  # 小于1小时，显示分钟和秒
        minutes = delay_ms // 60000
        seconds = (delay_ms % 60000) / 1000.0
        if seconds >= 1:
            return f"{minutes}m{seconds:.1f}s"
        else:
            return f"{minutes}m"
    else:  # 1小时以上，显示小时、分钟、秒
        hours = delay_ms // 3600000
        minutes = (delay_ms % 3600000) // 60000
        seconds = (delay_ms % 60000) / 1000.0
        
        parts = [f"{hours}h"]
        if minutes > 0:
            parts.append(f"{minutes}m")
        if seconds >= 1:
            parts.append(f"{seconds:.1f}s")
        
        return "".join(parts)
    
def extract_stream_creation_error(error_message):
    """从流创建错误信息中提取TDengine的具体错误
    
    Args:
        error_message: 完整的异常错误信息
        
    Returns:
        str: 提取的TDengine错误信息
    """
    #print(f"调试 extract_stream_creation_error: 输入参数类型={type(error_message)}, 长度={len(str(error_message))}")
    
    try:
        # 简单处理：直接返回错误信息，只做基本清理
        if not error_message:
            print(f"调试: 错误信息为空，返回默认消息")
            return ""
        
        # 转换为字符串
        error_str = str(error_message).strip()
        #print(f"调试: 转换后字符串长度: {len(error_str)}")
        
    
        if not error_str:
            print(f"调试: 转换后字符串为空，返回默认消息")
            return "错误信息为空"
        
        # 如果太长就截断
        if len(error_str) > 500:
            error_str = error_str[:500] + "..."
        
        # 清理换行符，保持单行显示
        error_str = error_str.replace('\n', ' ').replace('\r', ' ')
        
        # 清理HTML特殊字符
        error_str = error_str.replace('<', '&lt;').replace('>', '&gt;')
        
        return error_str
        
    except Exception as e:
        return f"错误信息处理失败: {str(e)}"

  
def extract_tdengine_error(error_message):
    """从错误信息中提取TDengine的具体错误
    
    Args:
        error_message: 完整的错误信息
        
    Returns:
        str: 提取的TDengine错误信息
    """
    try:
        # 简化处理：直接返回错误信息，只做基本的长度控制
        if not error_message:
            return ""
        
        # 转换为字符串（防止传入非字符串类型）
        error_str = str(error_message).strip()
        #print(f"调试: 转换后字符串长度: {len(error_str)}")
        print(f"调试: 原始错误信息前300个字符: {error_str[:300]}")
        
        # 如果错误信息太长，截断到合理长度
        max_length = 500
        if len(error_str) > max_length:
            error_str = error_str[:max_length] + "..."
        
        # 清理一些可能影响HTML显示的字符
        error_str = error_str.replace('<', '&lt;').replace('>', '&gt;')
        error_str = error_str.replace('\n', ' ').replace('\r', ' ')
        print(f"调试: 清理后错误信息长度: {len(error_str)}")
        print(f"调试: 最终返回的错误信息: {error_str[:200]}...")
        
        return error_str
        
    except Exception as e:
        # 如果处理出错，返回简单的错误信息
        return f"错误信息处理失败: {str(e)}"
    

def format_sql_for_display(sql_text):
    """格式化SQL用于显示，去掉多余的空白和换行
    
    Args:
        sql_text: 原始SQL文本
        
    Returns:
        str: 格式化后的SQL
    """
    try:
        import re
        
        # 1. 去掉前后的空白
        sql = sql_text.strip()
        
        # 2. 将多个连续的空白字符（包括换行、制表符、空格）替换为单个空格
        sql = re.sub(r'\s+', ' ', sql)
        
        # 3. 在主要的SQL关键字前后添加适当的换行，使结构更清晰
        # 但保持整体紧凑
        keywords = [
            'create stream', 'from', 'partition by', 'into', 'as select', 
            'where', 'interval\\(', 'sliding\\(', 'session\\(', 'period\\(', 
            'count_window\\(', 'event_window\\(', 'state_window\\('
        ]
        
        for keyword in keywords:
            # 在关键字前添加换行（除了第一个create stream）
            if keyword != 'create stream':
                sql = re.sub(f'\\s+({keyword})', r' \1', sql, flags=re.IGNORECASE)
        
        # 4. 特殊处理：在主要子句之间添加分隔
        # 但不添加实际换行，而是用 | 符号分隔增强可读性
        sql = re.sub(r'\s+(from\s+)', r' | \1', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\s+(partition\s+by)', r' | \1', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\s+(into\s+)', r' | \1', sql, flags=re.IGNORECASE)
        sql = re.sub(r'\s+(as\s+select)', r' | \1', sql, flags=re.IGNORECASE)
        
        return sql
        
    except Exception as e:
        # 如果格式化出错，返回简单清理后的版本
        return ' '.join(sql_text.split())



class StreamBatchTester:
    """流计算批量测试器 - 自动执行多种参数组合测试"""
    
    def __init__(self, base_args=None, specified_sql_types=None, filter_mode='all', single_template_mode='default'):
        """初始化批量测试器
        
        Args:
            base_args: 基础参数字典，包含不变的参数
            specified_sql_types: 指定的SQL类型列表，如果为None则使用默认的全部组合
            filter_mode: 过滤模式 ('all', 'skip-known-failures', 'only-known-failures')
        """
        self.base_args = base_args or {}
        self.test_results = []
        self.failed_tests = []
        self.current_test_index = 0
        self.total_tests = 0
        self.start_time = None
        self.specified_sql_types = specified_sql_types or []
        self.filter_mode = filter_mode
        self.single_template_mode = single_template_mode
        
        # 缓存已知失败测试列表，避免重复调用和打印
        self._known_failure_tests = None
        
        # 默认的固定参数
        self.fixed_params = {
            'mode': 1,  # 实时流计算测试
            'check_stream_delay': True,
            'stream_num': 1,
            'real_time_batch_sleep': 0,
            'monitor_interval': 30,
            'delay_check_interval': 30,
            'max_delay_threshold': 30000,
            'deployment_mode': 'single',
            'table_count': 2000,
            'histroy_rows': 1,
            'real_time_batch_rows': 200,
            'disorder_ratio': 0,
            'vgroups': 10,
            'debug_flag': 131,
            'num_of_log_lines': 500000
        }
        
        # 更新固定参数
        self.fixed_params.update(self.base_args)       
        
        # 批量测试中强制启用流延迟检查
        self.fixed_params['check_stream_delay'] = True
        
        print(f"批量测试器初始化完成:")
        if self.specified_sql_types:
            print(f"  指定SQL类型: {', '.join(self.specified_sql_types)}")
        else:
            print(f"  SQL类型: 全部组合")
            
        print(f"  过滤模式: {self.filter_mode}")
        if self.filter_mode != 'all':
            failure_count = len(self.get_known_failure_tests())
            print(f"  已知失败测试: {failure_count} 个")
            if self.filter_mode == 'skip-known-failures':
                print(f"  将跳过已知失败的测试场景，专注于成功场景")
            elif self.filter_mode == 'only-known-failures':
                print(f"  仅运行已知失败的测试场景，用于调试失败原因")
                
        print(f"  流延迟检查: {'启用' if self.fixed_params['check_stream_delay'] else '禁用'} (批量测试强制启用)")
        print(f"  延迟检查间隔: {self.fixed_params['delay_check_interval']}秒")
        print(f"  最大延迟阈值: {self.fixed_params['max_delay_threshold']}ms")
        print(f"  流数量: {self.fixed_params['stream_num']}") 


    def get_known_failure_tests(self):
        """获取已知失败的测试场景列表
        
        这里维护所有已知会失败的测试场景名称
        您可以直接在这里添加或删除失败的测试名称
        
        Returns:
            set: 已知失败的测试名称集合
        """
        # 使用缓存避免重复创建和打印
        if self._known_failure_tests is not None:
            return self._known_failure_tests
        
        # 在这里维护已知失败的测试名称列表
        known_failures = {
            # period 
            'period_stb_agg_tbname',
            'period_stb_select_tbname', 
            'period_stb_partition_by_tag_agg_tbname',
            'period_stb_partition_by_tag_select_tbname',
            'period_tb_agg_tbname',
            'period_tb_select_tbname',
            
            # count 
            'count_stb_agg_tbname',
            'count_stb_select_tbname',
            'count_stb_agg_trows', 
            'count_stb_select_trows',
            'count_stb_agg_sourcetable_stb',
            'count_stb_select_sourcetable_stb',
            'count_stb_partition_by_tag_agg_tbname',
            'count_stb_partition_by_tag_select_tbname',
            'count_stb_partition_by_tag_agg_trows', 
            'count_stb_partition_by_tag_select_trows',
            'count_stb_partition_by_tag_agg_sourcetable_stb',
            'count_stb_partition_by_tag_select_sourcetable_stb',
            'count_tb_agg_tbname',
            'count_tb_select_tbname',
            
            # event 
            'event_stb_agg_tbname',
            'event_stb_select_tbname',
            'event_stb_agg_trows',
            'event_stb_select_trows',
            'event_stb_agg_sourcetable_stb', 
            'event_stb_select_sourcetable_stb',
            'event_stb_partition_by_tag_agg_tbname',
            'event_stb_partition_by_tag_select_tbname',
            'event_stb_partition_by_tag_agg_trows',
            'event_stb_partition_by_tag_select_trows',
            'event_stb_partition_by_tag_agg_sourcetable_stb', 
            'event_stb_partition_by_tag_select_sourcetable_stb',
            'event_tb_agg_tbname',
            'event_tb_select_tbname',
            
            # state 
            'state_stb_agg_tbname',
            'state_stb_select_tbname',
            'state_stb_agg_trows',
            'state_stb_select_trows',
            'state_stb_agg_sourcetable_stb', 
            'state_stb_select_sourcetable_stb',
            'state_stb_partition_by_tag_agg_tbname',
            'state_stb_partition_by_tag_select_tbname',
            'state_stb_partition_by_tag_agg_trows',
            'state_stb_partition_by_tag_select_trows',
            'state_stb_partition_by_tag_agg_sourcetable_stb', 
            'state_stb_partition_by_tag_select_sourcetable_stb',
            'state_tb_agg_tbname',
            'state_tb_select_tbname',
            
            # intervalsliding
            'intervalsliding_stb_agg_tbname',
            'intervalsliding_stb_select_tbname',
            'intervalsliding_stb_partition_by_tag_agg_tbname',
            'intervalsliding_stb_partition_by_tag_select_tbname',
            'intervalsliding_tb_agg_tbname',
            'intervalsliding_tb_select_tbname',
            
            # session            
            'session_stb_agg_tbname',
            'session_stb_select_tbname',
            'session_stb_partition_by_tag_agg_tbname',
            'session_stb_partition_by_tag_select_tbname',
            'session_tb_agg_tbname',
            'session_tb_select_tbname',
            
            # sliding
            'sliding_stb_agg_tbname',
            'sliding_stb_select_tbname',
            'sliding_stb_partition_by_tag_agg_tbname',
            'sliding_stb_partition_by_tag_select_tbname',
            'sliding_tb_agg_tbname',
            'sliding_tb_select_tbname',
            
        }
        
        # 缓存结果并只打印一次
        self._known_failure_tests = known_failures
        print(f"维护的已知失败测试列表包含 {len(known_failures)} 个场景")
        return self._known_failure_tests
    
    def extract_stream_names_from_sql(self, sql_templates):
        """从SQL模板中提取流名称
        
        Args:
            sql_templates: SQL模板字符串或字典
            
        Returns:
            set: 流名称集合
        """
        import re
        stream_names = set()
        
        try:
            if isinstance(sql_templates, dict):
                for sql_template in sql_templates.values():
                    match = re.search(r'create\s+stream\s+([^\s]+)', sql_template, re.IGNORECASE)
                    if match:
                        full_stream_name = match.group(1)
                        # 去掉数据库前缀，只保留流名称部分
                        if '.' in full_stream_name:
                            stream_name = full_stream_name.split('.')[-1]
                        else:
                            stream_name = full_stream_name
                        stream_names.add(stream_name)
                        print(f"调试: 从SQL中提取流名称: {full_stream_name} -> {stream_name}")
            else:
                match = re.search(r'create\s+stream\s+([^\s]+)', sql_templates, re.IGNORECASE)
                if match:
                    full_stream_name = match.group(1)
                    # 去掉数据库前缀，只保留流名称部分
                    if '.' in full_stream_name:
                        stream_name = full_stream_name.split('.')[-1]
                    else:
                        stream_name = full_stream_name
                    stream_names.add(stream_name)
                    print(f"调试: 从SQL中提取流名称: {full_stream_name} -> {stream_name}")
        except Exception as e:
            print(f"提取流名称时出错: {str(e)}")
        
        return stream_names

    def should_skip_test_by_stream_names(self, stream_names):
        """根据流名称判断是否跳过测试
        
        Args:
            stream_names: 当前测试要创建的流名称集合
            
        Returns:
            tuple: (是否跳过, 原因)
        """
        if self.filter_mode == 'all':
            return False, ""
        
        print(f"调试过滤: 当前流名称集合: {stream_names}")
        print(f"调试过滤: 过滤模式: {self.filter_mode}")
        
        known_failures = self.get_known_failure_tests()
        print(f"调试过滤: 已知失败流数量: {len(known_failures)}")
        
        failed_streams = stream_names.intersection(known_failures)
        success_streams = stream_names - known_failures
        
        print(f"调试过滤: 匹配到的失败流: {failed_streams}")
        print(f"调试过滤: 成功流: {success_streams}")
        
        if self.filter_mode == 'skip-known-failures':
            # 如果包含已知失败的流，跳过
            if failed_streams:
                return True, f"包含已知失败流: {', '.join(failed_streams)}"
            return False, ""
            
        elif self.filter_mode == 'only-known-failures':
            # 如果不包含已知失败的流，跳过
            if not failed_streams:
                return True, f"不包含已知失败流，当前流: {', '.join(stream_names)}"
            return False, ""
            
        return False, ""
        
    def get_test_combinations(self):
        """获取所有测试组合
        
        Returns:
            list: 包含所有参数组合的列表
        """
        if self.specified_sql_types:
            # 如果指定了SQL类型，使用指定的类型进行组合
            return self.get_combinations_for_specified_types()
        else:
            # 使用默认的全部组合
            return self.get_default_combinations()
    
    def get_default_combinations(self):
        """获取默认的全部测试组合 """
        # 定义变化的参数
        variable_params = {
            'tbname_or_trows_or_sourcetable': ['tbname', 'trows', 'sourcetable'],
            'agg_or_select': ['agg', 'select'],
            'sql_type': [
                # 单个详细模板 - 间隔滑动窗口
                'intervalsliding_stb', 'intervalsliding_stb_partition_by_tbname', 
                'intervalsliding_stb_partition_by_tag', 'intervalsliding_tb',
                # 单个详细模板 - 滑动窗口  
                'sliding_stb', 'sliding_stb_partition_by_tbname',
                'sliding_stb_partition_by_tag', 'sliding_tb',
                # 单个详细模板 - 会话窗口
                'session_stb', 'session_stb_partition_by_tbname',
                'session_stb_partition_by_tag', 'session_tb',
                # 单个详细模板 - 计数窗口
                'count_stb', 'count_stb_partition_by_tbname',
                'count_stb_partition_by_tag', 'count_tb',
                # 单个详细模板 - 事件窗口
                'event_stb', 'event_stb_partition_by_tbname',
                'event_stb_partition_by_tag', 'event_tb',
                # 单个详细模板 - 状态窗口
                'state_stb', 'state_stb_partition_by_tbname',
                'state_stb_partition_by_tag', 'state_tb',
                # 单个详细模板 - 定时触发
                'period_stb', 'period_stb_partition_by_tbname',
                'period_stb_partition_by_tag', 'period_tb'
            ]
        }
        
        # 生成所有组合
        combinations = []
        keys = list(variable_params.keys())
        values = list(variable_params.values())
        
        for combination in itertools.product(*values):
            param_dict = dict(zip(keys, combination))
            combinations.append(param_dict)
            
        return combinations
    
    def get_combinations_for_specified_types(self):
        """为指定的SQL类型生成测试组合"""
        combinations = []
        
        # 解析指定的SQL类型并生成对应的组合
        for sql_type in self.specified_sql_types:
            type_combinations = self.get_combinations_for_single_type(sql_type)
            combinations.extend(type_combinations)
        
        return combinations
    
    def get_combinations_for_single_type(self, sql_type):
        """为单个SQL类型生成测试组合"""
        combinations = []
        
        # 固定参数组合模板 - 这些忽略命令行的 tbname_or_trows_or_sourcetable 和 agg_or_select
        fixed_param_templates = {
            'tbname_agg': [
                ('tbname', 'agg', 'intervalsliding_stb'), ('tbname', 'agg', 'intervalsliding_stb_partition_by_tbname'),
                ('tbname', 'agg', 'intervalsliding_stb_partition_by_tag'), ('tbname', 'agg', 'intervalsliding_tb'),
                ('tbname', 'agg', 'sliding_stb'), ('tbname', 'agg', 'sliding_stb_partition_by_tbname'),
                ('tbname', 'agg', 'sliding_stb_partition_by_tag'), ('tbname', 'agg', 'sliding_tb'),
                ('tbname', 'agg', 'session_stb'), ('tbname', 'agg', 'session_stb_partition_by_tbname'),
                ('tbname', 'agg', 'session_stb_partition_by_tag'), ('tbname', 'agg', 'session_tb'),
                ('tbname', 'agg', 'count_stb'), ('tbname', 'agg', 'count_stb_partition_by_tbname'),
                ('tbname', 'agg', 'count_stb_partition_by_tag'), ('tbname', 'agg', 'count_tb'),
                ('tbname', 'agg', 'event_stb'), ('tbname', 'agg', 'event_stb_partition_by_tbname'),
                ('tbname', 'agg', 'event_stb_partition_by_tag'), ('tbname', 'agg', 'event_tb'),
                ('tbname', 'agg', 'state_stb'), ('tbname', 'agg', 'state_stb_partition_by_tbname'),
                ('tbname', 'agg', 'state_stb_partition_by_tag'), ('tbname', 'agg', 'state_tb'),
                ('tbname', 'agg', 'period_stb'), ('tbname', 'agg', 'period_stb_partition_by_tbname'),
                ('tbname', 'agg', 'period_stb_partition_by_tag'), ('tbname', 'agg', 'period_tb')
            ],
            'tbname_select': [
                ('tbname', 'select', 'intervalsliding_stb'), ('tbname', 'select', 'intervalsliding_stb_partition_by_tbname'),
                ('tbname', 'select', 'intervalsliding_stb_partition_by_tag'), ('tbname', 'select', 'intervalsliding_tb'),
                ('tbname', 'select', 'sliding_stb'), ('tbname', 'select', 'sliding_stb_partition_by_tbname'),
                ('tbname', 'select', 'sliding_stb_partition_by_tag'), ('tbname', 'select', 'sliding_tb'),
                ('tbname', 'select', 'session_stb'), ('tbname', 'select', 'session_stb_partition_by_tbname'),
                ('tbname', 'select', 'session_stb_partition_by_tag'), ('tbname', 'select', 'session_tb'),
                ('tbname', 'select', 'count_stb'), ('tbname', 'select', 'count_stb_partition_by_tbname'),
                ('tbname', 'select', 'count_stb_partition_by_tag'), ('tbname', 'select', 'count_tb'),
                ('tbname', 'select', 'event_stb'), ('tbname', 'select', 'event_stb_partition_by_tbname'),
                ('tbname', 'select', 'event_stb_partition_by_tag'), ('tbname', 'select', 'event_tb'),
                ('tbname', 'select', 'state_stb'), ('tbname', 'select', 'state_stb_partition_by_tbname'),
                ('tbname', 'select', 'state_stb_partition_by_tag'), ('tbname', 'select', 'state_tb'),
                ('tbname', 'select', 'period_stb'), ('tbname', 'select', 'period_stb_partition_by_tbname'),
                ('tbname', 'select', 'period_stb_partition_by_tag'), ('tbname', 'select', 'period_tb')
            ],
            'trows_agg': [
                ('trows', 'agg', 'intervalsliding_stb'), ('trows', 'agg', 'intervalsliding_stb_partition_by_tbname'),
                ('trows', 'agg', 'intervalsliding_stb_partition_by_tag'), ('trows', 'agg', 'intervalsliding_tb'),
                ('trows', 'agg', 'sliding_stb'), ('trows', 'agg', 'sliding_stb_partition_by_tbname'),
                ('trows', 'agg', 'sliding_stb_partition_by_tag'), ('trows', 'agg', 'sliding_tb'),
                ('trows', 'agg', 'session_stb'), ('trows', 'agg', 'session_stb_partition_by_tbname'),
                ('trows', 'agg', 'session_stb_partition_by_tag'), ('trows', 'agg', 'session_tb'),
                ('trows', 'agg', 'count_stb'), ('trows', 'agg', 'count_stb_partition_by_tbname'),
                ('trows', 'agg', 'count_stb_partition_by_tag'), ('trows', 'agg', 'count_tb'),
                ('trows', 'agg', 'event_stb'), ('trows', 'agg', 'event_stb_partition_by_tbname'),
                ('trows', 'agg', 'event_stb_partition_by_tag'), ('trows', 'agg', 'event_tb'),
                ('trows', 'agg', 'state_stb'), ('trows', 'agg', 'state_stb_partition_by_tbname'),
                ('trows', 'agg', 'state_stb_partition_by_tag'), ('trows', 'agg', 'state_tb'),
                ('trows', 'agg', 'period_stb'), ('trows', 'agg', 'period_stb_partition_by_tbname'),
                ('trows', 'agg', 'period_stb_partition_by_tag'), ('trows', 'agg', 'period_tb')
            ],
            'trows_select': [
                ('trows', 'select', 'intervalsliding_stb'), ('trows', 'select', 'intervalsliding_stb_partition_by_tbname'),
                ('trows', 'select', 'intervalsliding_stb_partition_by_tag'), ('trows', 'select', 'intervalsliding_tb'),
                ('trows', 'select', 'sliding_stb'), ('trows', 'select', 'sliding_stb_partition_by_tbname'),
                ('trows', 'select', 'sliding_stb_partition_by_tag'), ('trows', 'select', 'sliding_tb'),
                ('trows', 'select', 'session_stb'), ('trows', 'select', 'session_stb_partition_by_tbname'),
                ('trows', 'select', 'session_stb_partition_by_tag'), ('trows', 'select', 'session_tb'),
                ('trows', 'select', 'count_stb'), ('trows', 'select', 'count_stb_partition_by_tbname'),
                ('trows', 'select', 'count_stb_partition_by_tag'), ('trows', 'select', 'count_tb'),
                ('trows', 'select', 'event_stb'), ('trows', 'select', 'event_stb_partition_by_tbname'),
                ('trows', 'select', 'event_stb_partition_by_tag'), ('trows', 'select', 'event_tb'),
                ('trows', 'select', 'state_stb'), ('trows', 'select', 'state_stb_partition_by_tbname'),
                ('trows', 'select', 'state_stb_partition_by_tag'), ('trows', 'select', 'state_tb'),
                ('trows', 'select', 'period_stb'), ('trows', 'select', 'period_stb_partition_by_tbname'),
                ('trows', 'select', 'period_stb_partition_by_tag'), ('trows', 'select', 'period_tb')
            ],
            'sourcetable_agg': [
                ('sourcetable', 'agg', 'intervalsliding_stb'), ('sourcetable', 'agg', 'intervalsliding_stb_partition_by_tbname'),
                ('sourcetable', 'agg', 'intervalsliding_stb_partition_by_tag'), ('sourcetable', 'agg', 'intervalsliding_tb'),
                ('sourcetable', 'agg', 'sliding_stb'), ('sourcetable', 'agg', 'sliding_stb_partition_by_tbname'),
                ('sourcetable', 'agg', 'sliding_stb_partition_by_tag'), ('sourcetable', 'agg', 'sliding_tb'),
                ('sourcetable', 'agg', 'session_stb'), ('sourcetable', 'agg', 'session_stb_partition_by_tbname'),
                ('sourcetable', 'agg', 'session_stb_partition_by_tag'), ('sourcetable', 'agg', 'session_tb'),
                ('sourcetable', 'agg', 'count_stb'), ('sourcetable', 'agg', 'count_stb_partition_by_tbname'),
                ('sourcetable', 'agg', 'count_stb_partition_by_tag'), ('sourcetable', 'agg', 'count_tb'),
                ('sourcetable', 'agg', 'event_stb'), ('sourcetable', 'agg', 'event_stb_partition_by_tbname'),
                ('sourcetable', 'agg', 'event_stb_partition_by_tag'), ('sourcetable', 'agg', 'event_tb'),
                ('sourcetable', 'agg', 'state_stb'), ('sourcetable', 'agg', 'state_stb_partition_by_tbname'),
                ('sourcetable', 'agg', 'state_stb_partition_by_tag'), ('sourcetable', 'agg', 'state_tb'),
                ('sourcetable', 'agg', 'period_stb'), ('sourcetable', 'agg', 'period_stb_partition_by_tbname'),
                ('sourcetable', 'agg', 'period_stb_partition_by_tag'), ('sourcetable', 'agg', 'period_tb')
            ],
            'sourcetable_select': [
                ('sourcetable', 'select', 'intervalsliding_stb'), ('sourcetable', 'select', 'intervalsliding_stb_partition_by_tbname'),
                ('sourcetable', 'select', 'intervalsliding_stb_partition_by_tag'), ('sourcetable', 'select', 'intervalsliding_tb'),
                ('sourcetable', 'select', 'sliding_stb'), ('sourcetable', 'select', 'sliding_stb_partition_by_tbname'),
                ('sourcetable', 'select', 'sliding_stb_partition_by_tag'), ('sourcetable', 'select', 'sliding_tb'),
                ('sourcetable', 'select', 'session_stb'), ('sourcetable', 'select', 'session_stb_partition_by_tbname'),
                ('sourcetable', 'select', 'session_stb_partition_by_tag'), ('sourcetable', 'select', 'session_tb'),
                ('sourcetable', 'select', 'count_stb'), ('sourcetable', 'select', 'count_stb_partition_by_tbname'),
                ('sourcetable', 'select', 'count_stb_partition_by_tag'), ('sourcetable', 'select', 'count_tb'),
                ('sourcetable', 'select', 'event_stb'), ('sourcetable', 'select', 'event_stb_partition_by_tbname'),
                ('sourcetable', 'select', 'event_stb_partition_by_tag'), ('sourcetable', 'select', 'event_tb'),
                ('sourcetable', 'select', 'state_stb'), ('sourcetable', 'select', 'state_stb_partition_by_tbname'),
                ('sourcetable', 'select', 'state_stb_partition_by_tag'), ('sourcetable', 'select', 'state_tb'),
                ('sourcetable', 'select', 'period_stb'), ('sourcetable', 'select', 'period_stb_partition_by_tbname'),
                ('sourcetable', 'select', 'period_stb_partition_by_tag'), ('sourcetable', 'select', 'period_tb')
            ]
        }
        
        # 组合模板 - 每组包含4种组合，使用所有的 tbname_or_trows_or_sourcetable 和 agg_or_select 组合
        detailed_templates = {
            'intervalsliding_detailed': [
                'intervalsliding_stb', 'intervalsliding_stb_partition_by_tbname',
                'intervalsliding_stb_partition_by_tag', 'intervalsliding_tb'
            ],
            'sliding_detailed': [
                'sliding_stb', 'sliding_stb_partition_by_tbname',
                'sliding_stb_partition_by_tag', 'sliding_tb'
            ],
            'session_detailed': [
                'session_stb', 'session_stb_partition_by_tbname',
                'session_stb_partition_by_tag', 'session_tb'
            ],
            'count_detailed': [
                'count_stb', 'count_stb_partition_by_tbname',
                'count_stb_partition_by_tag', 'count_tb'
            ],
            'event_detailed': [
                'event_stb', 'event_stb_partition_by_tbname',
                'event_stb_partition_by_tag', 'event_tb'
            ],
            'state_detailed': [
                'state_stb', 'state_stb_partition_by_tbname',
                'state_stb_partition_by_tag', 'state_tb'
            ],
            'period_detailed': [
                'period_stb', 'period_stb_partition_by_tbname',
                'period_stb_partition_by_tag', 'period_tb'
            ]
        }
        
        # 检查是否是固定参数组合模板
        if sql_type in fixed_param_templates:
            for tbname_param, agg_param, actual_sql_type in fixed_param_templates[sql_type]:
                combinations.append({
                    'sql_type': actual_sql_type,
                    'tbname_or_trows_or_sourcetable': tbname_param,
                    'agg_or_select': agg_param
                })
            print(f"  {sql_type}: 生成 {len(fixed_param_templates[sql_type])} 种组合")
            
        # 检查是否是组合模板
        elif sql_type in detailed_templates:
            for actual_sql_type in detailed_templates[sql_type]:
                # 对每个sql_type，生成所有 tbname_or_trows_or_sourcetable 和 agg_or_select 的组合
                for tbname_param in ['tbname', 'trows', 'sourcetable']:
                    for agg_param in ['agg', 'select']:
                        combinations.append({
                            'sql_type': actual_sql_type,
                            'tbname_or_trows_or_sourcetable': tbname_param,
                            'agg_or_select': agg_param
                        })
            print(f"  {sql_type}: 生成 {len(detailed_templates[sql_type]) * 3 * 2} 种组合 (4个模板 × 3个from类型 × 2个查询类型)")
            
        # 检查是否是all_detailed特殊模板
        elif sql_type == 'all_detailed':
            all_sql_types = []
            for template_list in detailed_templates.values():
                all_sql_types.extend(template_list)
            
            for actual_sql_type in all_sql_types:
                for tbname_param in ['tbname', 'trows', 'sourcetable']:
                    for agg_param in ['agg', 'select']:
                        combinations.append({
                            'sql_type': actual_sql_type,
                            'tbname_or_trows_or_sourcetable': tbname_param,
                            'agg_or_select': agg_param
                        })
            print(f"  {sql_type}: 生成 {len(all_sql_types) * 3 * 2} 种组合 (28个模板 × 3个from类型 × 2个查询类型)")
            
        # 检查是否是单个模板
        else:
            # # 单个SQL类型，生成所有参数组合
            # for tbname_param in ['tbname', 'trows', 'sourcetable']:
            #     for agg_param in ['agg', 'select']:
            #         combinations.append({
            #             'sql_type': sql_type,
            #             'tbname_or_trows_or_sourcetable': tbname_param,
            #             'agg_or_select': agg_param
            #         })
            # print(f"  {sql_type}: 生成 6 种组合 (3个from类型 × 2个查询类型)")
            
            # 单个SQL类型的新逻辑：默认只生成一个测试（使用默认参数）
            # 用户可以通过添加其他参数来控制行为
            # 检查批量测试模式设置
            if hasattr(self, 'single_template_mode') and self.single_template_mode == 'all-combinations':
                # 生成所有参数组合
                for tbname_param in ['tbname', 'trows', 'sourcetable']:
                    for agg_param in ['agg', 'select']:
                        combinations.append({
                            'sql_type': sql_type,
                            'tbname_or_trows_or_sourcetable': tbname_param,
                            'agg_or_select': agg_param
                        })
                print(f"  {sql_type}: 生成 6 种组合 (3个from类型 × 2个查询类型)")
            else:
                # 只生成默认组合
                combinations.append({
                    'sql_type': sql_type,
                    'tbname_or_trows_or_sourcetable': 'sourcetable',
                    'agg_or_select': 'agg'
                })
                print(f"  {sql_type}: 生成 1 种组合 (默认参数)")
                print(f"    提示: 使用 --batch-single-template-mode all-combinations 可测试所有6种组合")
        
        return combinations
    
    def filter_combinations(self, combinations):
        """过滤测试组合
        
        由于需要先生成SQL才能获得流名称，这里只做基本的有效性检查
        实际的失败过滤将在execute_single_test中进行
        
        Args:
            combinations: 原始组合列表
            
        Returns:
            list: 过滤后的有效组合列表
        """
        valid_combinations = []
        for combo in combinations:
            if self.is_valid_combination(combo):
                valid_combinations.append(combo)
        
        # 打印过滤统计
        total_original = len(combinations)
        total_filtered = len(valid_combinations)
        
        print(f"\n=== 测试组合基本过滤统计 ===")
        print(f"原始组合数: {total_original}")
        print(f"有效组合数: {total_filtered}")
        
        if self.filter_mode != 'all':
            print(f"过滤模式: {self.filter_mode}")
            print(f"注意: 已知失败流的过滤将在实际创建流时进行")
        
        return valid_combinations
    
    def is_valid_combination(self, combo):
        """检查组合是否有效
        
        Args:
            combo: 参数组合字典
            
        Returns:
            bool: 是否为有效组合
        """
        sql_type = combo['sql_type']
        tbname_param = combo['tbname_or_trows_or_sourcetable']
        
        # Period 类型的特殊处理
        if sql_type.startswith('period_'):
            pass
            
        # 可以在这里添加更多过滤规则
        # 例如：某些组合可能不支持或没有意义
        
        return True
    
    def create_batch_result_dir(self):
        """创建批量测试结果目录"""
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        result_dir = f"/tmp/stream_batch_test_{timestamp}"
        os.makedirs(result_dir, exist_ok=True)
        
        # 创建子目录
        for subdir in ['logs', 'performance', 'reports', 'configs']:
            os.makedirs(os.path.join(result_dir, subdir), exist_ok=True)
            
        return result_dir
    
    def generate_test_config(self, combination, test_index, result_dir):
        """为单个测试生成配置
        
        Args:
            combination: 参数组合
            test_index: 测试编号
            result_dir: 结果目录
            
        Returns:
            dict: 完整的测试配置
        """
        # 合并固定参数和变化参数
        config = self.fixed_params.copy()
        config.update(combination)
        
        # 强制确保流延迟检查在批量测试中启用
        config['check_stream_delay'] = True
    
        # 设置输出文件路径
        test_name = f"test_{test_index:03d}_{combination['sql_type']}_{combination['agg_or_select']}_{combination['tbname_or_trows_or_sourcetable']}"
        config['test_name'] = test_name
        config['perf_file'] = os.path.join(result_dir, 'performance', f'{test_name}_perf.log')
        config['delay_log_file'] = os.path.join(result_dir, 'logs', f'{test_name}_delay.log')
        config['test_log_file'] = os.path.join(result_dir, 'logs', f'{test_name}_test.log')
        
        # 调试输出：验证延迟检查配置
        print(f"调试: 为测试 {test_name} 设置性能文件: {config['perf_file']}")
        print(f"调试: 为测试 {test_name} 设置延迟日志文件: {config['delay_log_file']}")
        print(f"  check_stream_delay: {config.get('check_stream_delay', 'NOT_SET')}")
        print(f"  delay_check_interval: {config.get('delay_check_interval', 'NOT_SET')}")
        print(f"  max_delay_threshold: {config.get('max_delay_threshold', 'NOT_SET')}")
        print(f"  stream_num: {config.get('stream_num', 'NOT_SET')}") 
        print(f"  monitor_warm_up_time: {config.get('monitor_warm_up_time', 'NOT_SET')}") 
        
        if not config['check_stream_delay']:
            # 强制启用
            config['check_stream_delay'] = True
            print(f"  ✓ 已强制启用流延迟检查")
        
        return config
    
    def execute_single_test(self, config):
        """执行单个测试
        
        Args:
            config: 测试配置字典
            
        Returns:
            dict: 测试结果
        """
        test_name = config['test_name']
        start_time = time.time()
        
        print(f"\n{'='*80}")
        print(f"开始测试: {test_name}")
        print(f"测试编号: {self.current_test_index}/{self.total_tests}")
        print(f"SQL类型: {config['sql_type']}")
        print(f"查询类型: {config['agg_or_select']}")
        print(f"FROM类型: {config['tbname_or_trows_or_sourcetable']}")
        print(f"运行时间: {config.get('time', 5)}分钟")
        print(f"流数量: {config.get('stream_num', 1)}") 
        print(f"预热时间: {config.get('monitor_warm_up_time', 'AUTO')}秒")
        print(f"{'='*80}")
        
        result = {
            'test_name': test_name,
            'config': config,
            'start_time': datetime.datetime.now().isoformat(),
            'status': 'RUNNING',
            'duration': 0,
            'error': None
        }
        
        # 检查是否需要跳过（在实际创建流之前）
        if self.filter_mode != 'all':
            try:
                # 生成SQL模板
                sql_templates = StreamSQLTemplates.get_sql(
                    config['sql_type'],
                    stream_num=config.get('stream_num', 1),
                    agg_or_select=config['agg_or_select'],
                    tbname_or_trows_or_sourcetable=config['tbname_or_trows_or_sourcetable']
                )
                
                # 提取流名称
                stream_names = self.extract_stream_names_from_sql(sql_templates)
                
                # 检查是否应该跳过
                should_skip, skip_reason = self.should_skip_test_by_stream_names(stream_names)
                
                if should_skip:
                    print(f"⏭️  跳过测试: {skip_reason}")
                    
                    # 记录跳过结果
                    end_time = time.time()
                    duration = end_time - start_time
                    
                    result.update({
                        'status': 'SKIPPED',
                        'end_time': datetime.datetime.now().isoformat(),
                        'duration': duration,
                        'skip_reason': skip_reason,
                        'stream_names': list(stream_names)
                    })
                    
                    return result
                else:
                    print(f"✅ 测试通过过滤检查，将要创建的流: {', '.join(stream_names)}")
                    
            except Exception as e:
                print(f"⚠️  过滤检查时出错: {str(e)}，继续执行测试")
        
        # 继续执行原有的测试逻辑
        log_file = None
        original_stdout = sys.stdout
        
        try:
            # 强制清理环境
            print("清理测试环境...")
            self.cleanup_environment()
            
            # 等待环境清理完成
            time.sleep(3)
        
            # 额外的线程清理检查
            #print("检查并清理可能的残留线程...")
            import threading
            active_threads = threading.active_count()
            print(f"当前活跃线程数: {active_threads}")
            
            # 强制垃圾回收
            import gc
            gc.collect()
            time.sleep(2)
        
            # 验证环境清理是否成功
            result_check = subprocess.run('ps -ef | grep taosd | grep -v grep', 
                                        shell=True, capture_output=True, text=True)
            if result_check.stdout:
                print("⚠️  发现残留taosd进程，强制清理...")
                subprocess.run('pkill -9 taosd', shell=True)
                time.sleep(3)
            
            # 创建StreamStarter实例
            starter = StreamStarter(
                runtime=config.get('time', 5),
                perf_file=config['perf_file'],
                table_count=config['table_count'],
                histroy_rows=config['histroy_rows'],
                real_time_batch_rows=config['real_time_batch_rows'],
                real_time_batch_sleep=config['real_time_batch_sleep'],
                disorder_ratio=config['disorder_ratio'],
                vgroups=config['vgroups'],
                sql_type=config['sql_type'],
                stream_num=config.get('stream_num', 1),
                stream_perf_test_dir=config.get('stream_perf_test_dir', '/home/stream_perf_test_dir'),
                monitor_interval=config['monitor_interval'],
                deployment_mode=config['deployment_mode'],
                debug_flag=config['debug_flag'],
                num_of_log_lines=config['num_of_log_lines'],
                agg_or_select=config['agg_or_select'],
                tbname_or_trows_or_sourcetable=config['tbname_or_trows_or_sourcetable'],
                check_stream_delay=config['check_stream_delay'],
                max_delay_threshold=config['max_delay_threshold'],
                delay_check_interval=config['delay_check_interval'],
                delay_log_file=config['delay_log_file'],
                monitor_warm_up_time=config.get('monitor_warm_up_time') 
            )
            
            # 打开日志文件并设置输出重定向
            log_file = open(config['test_log_file'], 'w')
            
            # 同时输出到控制台和文件
            class TeeOutput:
                def __init__(self, *files):
                    self.files = files
                def write(self, text):
                    for f in self.files:
                        try:
                            f.write(text)
                            f.flush()
                        except (ValueError, OSError) as e:
                            pass
                def flush(self):
                    for f in self.files:
                        try:
                            f.flush()
                        except (ValueError, OSError):
                            pass
            
            sys.stdout = TeeOutput(original_stdout, log_file)
            
            # 执行测试
            print(f"开始执行流计算测试: {test_name}")
            print(f"延迟监控状态: {'启用' if config['check_stream_delay'] else '禁用'}")
            print(f"性能监控文件: {config['perf_file']}")
            print(f"流数量: {config.get('stream_num', 1)}") 
            
            # ✅ 验证预热时间是否正确传递
            if hasattr(starter, 'monitor_warm_up_time'):
                print(f"预热时间配置: {starter.monitor_warm_up_time}秒")
            else:
                print(f"警告: 预热时间参数未设置")
            
            
            # 核心测试逻辑 - 使用内部异常处理
            test_error = None
            try:
                starter.do_test_stream_with_realtime_data()
            except Exception as inner_e:
                # 捕获具体的流创建或执行错误
                error_message = str(inner_e)
                
                # 检查是否是流创建相关的错误
                if any(keyword in error_message.lower() for keyword in ['create stream', 'cursor.execute', '%%tbname']):
                    test_error = extract_stream_creation_error(error_message)
                else:
                    test_error = extract_tdengine_error(error_message)
                
            # 正常完成或有错误，都先安全地关闭文件和恢复输出
            sys.stdout = original_stdout
            if log_file:
                log_file.close()
                log_file = None
                
            # 如果有错误，现在抛出
            if test_error:
                raise Exception(test_error)
                
            end_time = time.time()
            duration = end_time - start_time
            
            result.update({
                'status': 'SUCCESS',
                'end_time': datetime.datetime.now().isoformat(),
                'duration': duration
            })
            
            print(f"测试 {test_name} 完成，耗时: {duration:.2f}秒")
            
        except Exception as e:
            end_time = time.time()
            duration = end_time - start_time
            
            # 使用异常消息作为错误信息
            error_message = str(e)
            
            # 确保错误信息不为空
            if not error_message or error_message.strip() == "":
                error_message = "未知错误: 测试执行失败但无具体错误信息"
            
            result.update({
                'status': 'FAILED',
                'end_time': datetime.datetime.now().isoformat(),
                'duration': duration,
                'error': error_message
            })
            
            print(f"测试 {test_name} 失败: {error_message}")
            
            self.failed_tests.append(result)
            
        finally:
            # 安全地恢复标准输出和关闭文件
            if sys.stdout != original_stdout:
                sys.stdout = original_stdout
            
            if log_file and not log_file.closed:
                try:
                    log_file.close()
                except:
                    pass
            
            # 强制清理环境，为下一个测试做准备
            print("测试后全面清理环境...")
            try:
                # 1. 强制停止所有taosd进程
                print("  → 停止所有taosd进程")
                subprocess.run('pkill -9 taosd', shell=True)
                time.sleep(2)
                
                # 2. 清理可能的监控线程和文件句柄
                print("  → 清理系统资源")
                import gc
                gc.collect()
                
                # 3. 清理临时文件
                print("  → 清理临时文件")
                subprocess.run('rm -f /tmp/stream_from*.json', shell=True)
                subprocess.run('rm -f /tmp/taosBenchmark_result.log', shell=True)
                
                # 4. 检查活跃线程数
                import threading
                active_threads = threading.active_count()
                print(f"  → 当前活跃线程数: {active_threads}")
                
                # 5. 验证清理结果
                check_result = subprocess.run('ps -ef | grep taosd | grep -v grep', 
                                            shell=True, capture_output=True, text=True)
                if check_result.stdout:
                    print(f"  ⚠️  警告: 仍有taosd进程运行，可能影响下个测试")
                else:
                    print(f"  ✅ 环境清理完成")
                    
            except Exception as cleanup_e:
                print(f"  ⚠️  清理过程出错: {str(cleanup_e)}")
            
            # 5. 为下一个测试留出缓冲时间
            if self.current_test_index < self.total_tests:
                print("  → 等待3秒后开始下一个测试...")
                time.sleep(3)
            
        return result
            
            
    def cleanup_environment(self):
        """清理测试环境"""
        try:
            print("执行增强环境清理...")
            
            # 1. 强制停止所有taosd进程
            print("  → 强制停止taosd进程")
            subprocess.run('pkill -9 taosd', shell=True)
            time.sleep(2)
            # 强制停止所有活跃的监控线程
            stopped_threads = []
            for thread in threading.enumerate():
                if thread.name in ["MonitorSystemLoad", "TaosdMonitor", "StreamDelayMonitor", "DelayedStreamCreation", "ContinuousDataWriter"]:
                    try:
                        print(f"    发现活跃监控线程: {thread.name}")
                        if hasattr(thread, '_target') and hasattr(thread._target, '__self__'):
                            monitor_obj = thread._target.__self__
                            if hasattr(monitor_obj, 'stop'):
                                print(f"    调用 {thread.name} 的stop()方法")
                                monitor_obj.stop()
                            elif hasattr(monitor_obj, '_stop_event'):
                                print(f"    设置 {thread.name} 的_stop_event")
                                monitor_obj._stop_event.set()
                            elif hasattr(monitor_obj, '_should_stop'):
                                print(f"    设置 {thread.name} 的_should_stop标志")
                                monitor_obj._should_stop = True
                        
                        # 通用的停止标志设置
                        if hasattr(thread, '_should_stop'):
                            thread._should_stop = True
                            print(f"    已设置线程 {thread.name} 的停止标志")
                        
                        stopped_threads.append(thread.name)
                    except Exception as e:
                        print(f"    设置线程停止标志失败: {str(e)}")
            
            # 等待监控线程自然结束
            if stopped_threads:
                print(f"  → 等待 {len(stopped_threads)} 个监控线程结束...")
                start_wait = time.time()
                max_wait = 5  # 最多等待5秒
                
                while time.time() - start_wait < max_wait:
                    active_monitor_threads = []
                    for thread in threading.enumerate():
                        if thread.name in ["MonitorSystemLoad", "TaosdMonitor", "StreamDelayMonitor", "DelayedStreamCreation", "ContinuousDataWriter"]:
                            active_monitor_threads.append(thread.name)
                    
                    if not active_monitor_threads:
                        print(f"  ✅ 所有监控线程已停止")
                        break
                        
                    print(f"    等待中，剩余活跃线程: {', '.join(active_monitor_threads)}")
                    time.sleep(1)
                else:
                    print(f"  ⚠️  超时：仍有监控线程未结束，强制继续清理")
            
            # 强制垃圾回收，清理可能的资源
            import gc
            gc.collect()
                   
            # 多次尝试停止进程
            max_attempts = 5
            for attempt in range(max_attempts):
                # 首先尝试正常停止
                subprocess.run('pkill -15 -f "^taosd.*-c.*conf"', shell=True)
                time.sleep(1)
                
                # 检查是否还有进程
                result = subprocess.run('pgrep -f "^taosd.*-c.*conf"', shell=True, capture_output=True)
                if result.returncode != 0:  # 没有找到进程
                    print("  ✅ 所有taosd进程已停止")
                    break
                
                # 强制停止
                subprocess.run('pkill -9 -f "^taosd.*-c.*conf"', shell=True)
                time.sleep(1)
                
                # 再次检查
                result = subprocess.run('pgrep -f "^taosd.*-c.*conf"', shell=True, capture_output=True)
                if result.returncode != 0:
                    print("  ✅ 所有taosd进程已停止")
                    break
                else:
                    print(f"  ⚠️  第{attempt+1}次尝试: 仍有taosd进程运行，继续清理...")
        
            # ✅ 2. 清理可能的监控线程和资源
            print("  → 强制清理监控资源")
            
            # 强制垃圾回收
            gc.collect()
            
            # 等待一段时间让所有线程结束
            time.sleep(2)
            
            # ✅ 3. 清理临时文件和socket文件
            print("  → 清理临时文件")
            temp_patterns = [
                '/tmp/stream_from*.json',
                '/tmp/taosBenchmark_result.log',
                '/tmp/testlog/*',
            ]
            
            # 单独处理可能有问题的文件模式
            socket_patterns = [
                '/tmp/*.sock*',
                '/tmp/taos*.sock'
            ]
            
            for pattern in temp_patterns:
                try:
                    subprocess.run(f'rm -f {pattern}', shell=True)
                except:
                    pass
                    
            # 小心处理socket文件，避免删除重要文件
            for pattern in socket_patterns:
                try:
                    # 使用find命令更安全地删除
                    subprocess.run(f'find /tmp -name "{pattern.split("/")[-1]}" -type f -delete 2>/dev/null', shell=True)
                except:
                    pass
            
            # 4. 最终验证
            print("  → 验证清理结果")
            result = subprocess.run('ps -ef | grep "taosd.*-c.*conf" | grep -v grep', 
                                    shell=True, capture_output=True, text=True)
            if result.stdout:
                print(f"  ⚠️  警告: 发现残留进程:")
                for line in result.stdout.strip().split('\n'):
                    print(f"       {line}")
                # 最后一次强制清理
                subprocess.run('pkill -9 -f "^taosd.*-c.*conf"', shell=True)
                time.sleep(1)
            else:
                print(f"  ✅ 环境清理验证通过")
            
            # 5. 检查和报告活跃线程状态
            print("  → 检查活跃线程状态")
            active_monitor_threads = []
            for thread in threading.enumerate():
                if thread.name in ["MonitorSystemLoad", "TaosdMonitor", "StreamDelayMonitor", "DelayedStreamCreation", "ContinuousDataWriter"]:
                    active_monitor_threads.append(thread.name)
            
            if active_monitor_threads:
                print(f"  ⚠️  警告: 仍有活跃的监控线程: {', '.join(active_monitor_threads)}")
                print(f"    这些线程在下次测试时会自动停止")
            else:
                print(f"  ✅ 所有监控线程已清理")
                
            total_active_threads = threading.active_count()
            print(f"  → 当前总活跃线程数: {total_active_threads}")
            
            print("增强环境清理完成")
            
        except Exception as e:
            print(f"增强环境清理时出错: {str(e)}")
    
    def save_test_config(self, config, result_dir):
        """保存测试配置到文件"""
        config_file = os.path.join(result_dir, 'configs', f"{config['test_name']}_config.json")
        
        # 创建可序列化的配置副本
        serializable_config = {}
        for key, value in config.items():
            if isinstance(value, (str, int, float, bool, list, dict, type(None))):
                serializable_config[key] = value
            else:
                serializable_config[key] = str(value)
        
        with open(config_file, 'w') as f:
            json.dump(serializable_config, f, indent=2)
    
    def generate_progress_report(self, result_dir):
        """生成进度报告"""
        if not self.test_results:
            return
            
        report_file = os.path.join(result_dir, 'reports', 'progress_report.txt')
        
        with open(report_file, 'w') as f:
            f.write(f"批量流计算测试进度报告\n")
            f.write(f"{'='*60}\n")
            f.write(f"开始时间: {self.start_time}\n")
            f.write(f"当前时间: {datetime.datetime.now().isoformat()}\n")
            f.write(f"总测试数: {self.total_tests}\n")
            f.write(f"已完成: {len(self.test_results)}\n")
            f.write(f"成功: {len([r for r in self.test_results if r['status'] == 'SUCCESS'])}\n")
            f.write(f"失败: {len([r for r in self.test_results if r['status'] == 'FAILED'])}\n")
            f.write(f"进度: {(len(self.test_results)/self.total_tests)*100:.1f}%\n")
            f.write(f"\n最近完成的测试:\n")
            
            for result in self.test_results[-5:]:  # 显示最近5个
                status_icon = "✓" if result['status'] == 'SUCCESS' else "✗"
                f.write(f"  {status_icon} {result['test_name']} - {result.get('duration', 0):.1f}s\n")
                
    
    def generate_final_report_bak(self, result_dir):
        """生成最终测试报告"""
        report_file = os.path.join(result_dir, 'reports', 'final_report.html')
        
        success_count = len([r for r in self.test_results if r['status'] == 'SUCCESS'])
        failed_count = len([r for r in self.test_results if r['status'] == 'FAILED'])
        total_duration = sum(r.get('duration', 0) for r in self.test_results)
        
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>流计算批量测试报告</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .header {{ background-color: #f0f0f0; padding: 20px; border-radius: 5px; }}
        .summary {{ display: flex; justify-content: space-around; margin: 20px 0; }}
        .metric {{ text-align: center; padding: 10px; background-color: #e9e9e9; border-radius: 5px; }}
        .success {{ color: green; }}
        .failed {{ color: red; }}
        table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
        th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
        th {{ background-color: #f2f2f2; }}
        .status-success {{ background-color: #d4edda; }}
        .status-failed {{ background-color: #f8d7da; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>TDengine 流计算批量测试报告</h1>
        <p>生成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p>测试开始: {self.start_time}</p>
        <p>测试结束: {datetime.datetime.now().isoformat()}</p>
    </div>
    
    <div class="summary">
        <div class="metric">
            <h3>总测试数</h3>
            <h2>{self.total_tests}</h2>
        </div>
        <div class="metric success">
            <h3>成功</h3>
            <h2>{success_count}</h2>
        </div>
        <div class="metric failed">
            <h3>失败</h3>
            <h2>{failed_count}</h2>
        </div>
        <div class="metric">
            <h3>成功率</h3>
            <h2>{(success_count/self.total_tests*100):.1f}%</h2>
        </div>
        <div class="metric">
            <h3>总耗时</h3>
            <h2>{total_duration/3600:.1f}小时</h2>
        </div>
    </div>
    
    <h2>详细测试结果</h2>
    <table>
        <tr>
            <th>测试名称</th>
            <th>SQL类型</th>
            <th>查询类型</th>
            <th>FROM类型</th>
            <th>状态</th>
            <th>耗时(秒)</th>
            <th>错误信息</th>
        </tr>
"""
        
        for result in self.test_results:
            config = result['config']
            status_class = 'status-success' if result['status'] == 'SUCCESS' else 'status-failed'
            error_msg = result.get('error', '')[:100] if result.get('error') else ''
            
            html_content += f"""
        <tr class="{status_class}">
            <td>{result['test_name']}</td>
            <td>{config['sql_type']}</td>
            <td>{config['agg_or_select']}</td>
            <td>{config['tbname_or_trows_or_sourcetable']}</td>
            <td>{result['status']}</td>
            <td>{result.get('duration', 0):.1f}</td>
            <td>{error_msg}</td>
        </tr>
"""
        
        html_content += """
    </table>
</body>
</html>
"""
        
        with open(report_file, 'w') as f:
            f.write(html_content)
        
        print(f"最终报告已生成: {report_file}")


    def generate_final_report(self, result_dir):
        """生成最终测试报告"""
        report_file = os.path.join(result_dir, 'reports', 'final_report.html')
        
        success_count = len([r for r in self.test_results if r['status'] == 'SUCCESS'])
        failed_count = len([r for r in self.test_results if r['status'] == 'FAILED'])
        skipped_count = len([r for r in self.test_results if r['status'] == 'SKIPPED'])
        total_duration = sum(r.get('duration', 0) for r in self.test_results)
        current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 检查是否有测试结果
        if not self.test_results:
            print("警告: 没有测试结果可以生成报告")
            # 生成一个简单的空报告
            report_file = os.path.join(result_dir, 'reports', 'final_report.html')
            try:
                os.makedirs(os.path.dirname(report_file), exist_ok=True)
                with open(report_file, 'w', encoding='utf-8') as f:
                    f.write(f"""
<!DOCTYPE html>
<html>
<head>
    <title>流计算批量测试报告 - 无测试结果</title>
    <meta charset="UTF-8">
</head>
<body>
    <h1>流计算批量测试报告</h1>
    <p>生成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    <p><strong>警告:</strong> 没有测试结果可以显示。</p>
    <p>可能的原因:</p>
    <ul>
        <li>过滤设置导致所有测试都被跳过</li>
        <li>测试配置错误</li>
        <li>测试提前终止</li>
    </ul>
    <p>请检查过滤模式和SQL类型设置。</p>
</body>
</html>
""")
                print(f"空报告已生成: {report_file}")
            except Exception as e:
                print(f"生成空报告失败: {str(e)}")
            return
        
        report_file = os.path.join(result_dir, 'reports', 'final_report.html')
        
        # 防止除零错误
        total_tests = len(self.test_results)
        if total_tests == 0:
            success_rate = 0
            avg_duration = 0
            efficiency = 0
        else:
            success_rate = (success_count / total_tests) * 100
            avg_duration = total_duration / total_tests / 60
            efficiency = total_tests / (total_duration / 3600) if total_duration > 0 else 0
        
        # 获取系统信息
        def get_system_info():
            """获取系统CPU核数和总内存"""
            try:
                # 获取CPU核数
                cpu_count = None
                try:
                    result = subprocess.run('lscpu | grep "^CPU(s):"', shell=True, 
                                        capture_output=True, text=True)
                    if result.returncode == 0:
                        cpu_line = result.stdout.strip()
                        # 解析 "CPU(s):                             16" 格式
                        cpu_count = cpu_line.split(':')[1].strip()
                        print(f"调试: 获取到CPU核数: {cpu_count}")
                except Exception as e:
                    print(f"调试: 获取CPU核数失败: {str(e)}")
                
                # 获取总内存
                total_memory_gb = None
                try:
                    result = subprocess.run('free -m | grep "^Mem:"', shell=True, 
                                        capture_output=True, text=True)
                    if result.returncode == 0:
                        mem_line = result.stdout.strip()
                        # 解析 "Mem:           15944        1234        5678        ..." 格式
                        total_memory_mb = int(mem_line.split()[1])
                        total_memory_gb = f"{total_memory_mb/1024:.1f}GB"
                        print(f"调试: 获取到总内存: {total_memory_gb}")
                except Exception as e:
                    print(f"调试: 获取总内存失败: {str(e)}")
                    
                return cpu_count, total_memory_gb
            except Exception as e:
                print(f"调试: 获取系统信息失败: {str(e)}")
                return None, None
        
        system_cpu_count, system_total_memory = get_system_info()
        
        html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>流计算批量测试详细报告</title>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; margin: 20px; background-color: #f5f5f5; }}
        .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 10px; margin-bottom: 20px; }}
        .summary {{ display: flex; justify-content: space-around; margin: 20px 0; flex-wrap: wrap; }}
        .metric {{ text-align: center; padding: 20px; background-color: white; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); min-width: 150px; margin: 5px; }}
        .metric h3 {{ margin: 0 0 10px 0; color: #666; font-size: 14px; }}
        .metric h2 {{ margin: 0; font-size: 24px; }}
        .success {{ color: #28a745; }}
        .failed {{ color: #dc3545; }}
        .warning {{ color: #ffc107; }}
        .info {{ color: #17a2b8; }}
        
        /* 表格样式 */
        .table-container {{ background: white; border-radius: 10px; overflow: hidden; box-shadow: 0 2px 10px rgba(0,0,0,0.1); margin: 20px 0; }}
        table {{ width: 100%; border-collapse: collapse; }}
        th {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 15px 8px; text-align: left; font-weight: 600; font-size: 12px; }}
        td {{ border: none; padding: 12px 8px; text-align: left; border-bottom: 1px solid #eee; font-size: 11px; }}
        tr:hover {{ background-color: #f8f9fa; }}
        .status-success {{ background-color: #d4edda !important; }}
        .status-failed {{ background-color: #f8d7da !important; }}
        .status-skipped {{ background-color: #f8f9fa !important; }}
        
        /* 状态标签 */
        .status-badge {{ padding: 4px 8px; border-radius: 15px; font-size: 10px; font-weight: bold; text-transform: uppercase; }}
        .badge-success {{ background-color: #28a745; color: white; }}
        .badge-failed {{ background-color: #dc3545; color: white; }}
        .badge-skipped {{ background-color: #6c757d; color: white; }} 
        
        /* 性能指标样式 */
        .perf-good {{ color: #28a745; font-weight: bold; }}
        .perf-warning {{ color: #ffc107; font-weight: bold; }}
        .perf-danger {{ color: #dc3545; font-weight: bold; }}
        
        /* SQL展示样式 */
        .sql-preview {{ 
            font-family: 'Courier New', monospace; 
            font-size: 10px; 
            background-color: #f8f9fa; 
            padding: 5px; 
            border-radius: 3px; 
            max-width: 300px; 
            overflow: hidden; 
            text-overflow: ellipsis; 
            white-space: nowrap;
            cursor: pointer;
            border: 1px solid #dee2e6;
        }}
        .sql-preview:hover {{ background-color: #e9ecef; }}
        
        /* 延迟指标样式 */
        .delay-excellent {{ color: #28a745; font-weight: bold; }}
        .delay-good {{ color: #20c997; font-weight: bold; }}
        .delay-normal {{ color: #ffc107; font-weight: bold; }}
        .delay-warning {{ color: #fd7e14; font-weight: bold; }}
        .delay-danger {{ color: #dc3545; font-weight: bold; }}
        .delay-critical {{ color: #6f42c1; font-weight: bold; }}
        
        /* 工具提示 */
        .tooltip {{ position: relative; display: inline-block; }}
        .tooltip .tooltiptext {{ 
            visibility: hidden; 
            width: 400px; 
            background-color: #333; 
            color: #fff; 
            text-align: left; 
            border-radius: 6px; 
            padding: 10px; 
            position: absolute; 
            z-index: 1; 
            bottom: 125%; 
            left: 50%; 
            margin-left: -200px; 
            opacity: 0; 
            transition: opacity 0.3s; 
            font-family: 'Courier New', monospace; 
            font-size: 10px; 
            white-space: pre-wrap; 
        }}
        .tooltip:hover .tooltiptext {{ visibility: visible; opacity: 1; }}
        
        /* 筛选和搜索 */
        .controls {{ margin: 20px 0; padding: 15px; background: white; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
        .filter-group {{ display: inline-block; margin-right: 20px; }}
        .filter-group label {{ font-weight: bold; margin-right: 5px; }}
        .filter-group select, .filter-group input {{ padding: 5px; border: 1px solid #ddd; border-radius: 4px; }}
        
        /* 响应式设计 */
        @media (max-width: 1200px) {{
            th, td {{ font-size: 10px; padding: 8px 4px; }}
            .sql-preview {{ max-width: 200px; }}
        }}
    </style>
    <script>
        function filterTable() {{
            const statusFilter = document.getElementById('statusFilter').value;
            const sqlTypeFilter = document.getElementById('sqlTypeFilter').value;
            const searchTerm = document.getElementById('searchInput').value.toLowerCase();
            
            const table = document.getElementById('resultsTable');
            const rows = table.getElementsByTagName('tr');
            
            for (let i = 1; i < rows.length; i++) {{
                const row = rows[i];
                const status = row.cells[4].textContent;
                const sqlType = row.cells[1].textContent;
                const testName = row.cells[0].textContent.toLowerCase();
                
                let showRow = true;
                
                if (statusFilter && !status.includes(statusFilter)) showRow = false;
                if (sqlTypeFilter && sqlType !== sqlTypeFilter) showRow = false;
                if (searchTerm && !testName.includes(searchTerm)) showRow = false;
                
                row.style.display = showRow ? '' : 'none';
            }}
        }}
        
        function sortTable(columnIndex) {{
            const table = document.getElementById('resultsTable');
            const rows = Array.from(table.rows).slice(1);
            const isNumeric = columnIndex === 5 || columnIndex === 7 || columnIndex === 8; // 耗时、CPU、内存列
            
            rows.sort((a, b) => {{
                const aVal = a.cells[columnIndex].textContent;
                const bVal = b.cells[columnIndex].textContent;
                
                if (isNumeric) {{
                    return parseFloat(aVal) - parseFloat(bVal);
                }} else {{
                    return aVal.localeCompare(bVal);
                }}
            }});
            
            rows.forEach(row => table.appendChild(row));
        }}
    </script>
</head>
<body>
    <div class="header">
        <h1>🚀 TDengine 流计算批量测试详细报告</h1>
        <p>📅 生成时间: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p>⏰ 测试周期: {self.start_time} → {datetime.datetime.now().isoformat()}</p>
        <p>🏗️ 测试配置: {self.fixed_params.get('deployment_mode', 'single')}模式 | 
           {self.fixed_params.get('table_count', 1000)}张自表 | 
           {self.fixed_params.get('real_time_batch_rows', 200)}条/轮 | 
           每轮写入间隔{self.fixed_params.get('real_time_batch_sleep', 0)}秒</p>
        <p>💻 测试环境: CPU核数{system_cpu_count or '未知'} | 总内存{system_total_memory or '未知'}</p>
    </div>
    
    <div class="summary">
        <div class="metric">
            <h3>📊 总测试数</h3>
            <h2>{self.total_tests}</h2>
        </div>
        <div class="metric success">
            <h3>✅ 成功</h3>
            <h2>{success_count}</h2>
            <small>{(success_count/self.total_tests*100):.1f}%</small>
        </div>
        <div class="metric failed">
            <h3>❌ 创建流语句失败，非流计算失败</h3>
            <h2>{failed_count}</h2>
            <small>{(failed_count/self.total_tests*100):.1f}%</small>
        </div>
        <div class="metric skipped">
            <h3>⏭️ 跳过(已知失败)</h3>
            <h2>{skipped_count}</h2>
            <small>{(skipped_count/total_tests*100):.1f}%</small>
        </div>
        <div class="metric info">
            <h3>⏱️ 总耗时</h3>
            <h2>{total_duration/3600:.1f}h</h2>
            <small>平均{total_duration/self.total_tests/60:.1f}分钟/测试</small>
        </div>
        <div class="metric info">
            <h3>💪 效率</h3>
            <h2>{self.total_tests/(total_duration/3600):.1f}</h2>
            <small>测试场景个数/小时</small>
        </div>
    </div>
    
    <div class="controls">
        <div class="filter-group">
            <label for="statusFilter">状态筛选:</label>
            <select id="statusFilter" onchange="filterTable()">
                <option value="">全部</option>
                <option value="SUCCESS">成功</option>
                <option value="FAILED">失败</option>
                <option value="SKIPPED">跳过</option>
            </select>
        </div>
        <div class="filter-group">
            <label for="sqlTypeFilter">SQL类型筛选:</label>
            <select id="sqlTypeFilter" onchange="filterTable()">
                <option value="">全部</option>
"""
        
        # 获取所有SQL类型用于筛选
        sql_types = set()
        for result in self.test_results:
            sql_types.add(result['config']['sql_type'])
        
        for sql_type in sorted(sql_types):
            html_content += f'                <option value="{sql_type}">{sql_type}</option>\n'
        
        html_content += f"""
            </select>
        </div>
        <div class="filter-group">
            <label for="searchInput">搜索测试名称:</label>
            <input type="text" id="searchInput" placeholder="输入关键词..." onkeyup="filterTable()">
        </div>
    </div>
    
    <div class="table-container">
        <h2 style="margin: 0; padding: 20px; border-bottom: 1px solid #eee;">📋 详细测试结果</h2>
        <table id="resultsTable">
            <tr>
                <th onclick="sortTable(0)" style="cursor: pointer;">📝 测试名称</th>
                <th onclick="sortTable(1)" style="cursor: pointer;">🔧 SQL类型</th>
                <th onclick="sortTable(2)" style="cursor: pointer;">📊 查询类型</th>
                <th onclick="sortTable(3)" style="cursor: pointer;">📂 FROM类型</th>
                <th onclick="sortTable(4)" style="cursor: pointer;">🎯 状态</th>
                <th onclick="sortTable(5)" style="cursor: pointer;">⏱️ 耗时(秒)</th>
                <th>🔍 SQL预览</th>
                <th onclick="sortTable(7)" style="cursor: pointer;">💻 CPU峰值范围(%)</th>
                <th onclick="sortTable(8)" style="cursor: pointer;">🧠 内存峰值范围(MB)</th>
                <th onclick="sortTable(9)" style="cursor: pointer;">📊 CPU平均值(%)</th>
                <th onclick="sortTable(10)" style="cursor: pointer;">📈 内存平均值(MB)</th>
                <th>📈 延迟统计</th>
                <th>❌ 错误信息</th>
            </tr>
"""
        
        for result_index, result in enumerate(self.test_results):
            config = result['config']
            
            if result['status'] == 'SUCCESS':
                status_class = 'status-success'
                status_badge = 'badge-success'
            elif result['status'] == 'FAILED':
                status_class = 'status-failed'
                status_badge = 'badge-failed'
            elif result['status'] == 'SKIPPED':
                status_class = 'status-skipped'
                status_badge = 'badge-skipped'
            else:
                status_class = ''
                status_badge = ''
            
            print(f"调试: 处理第 {result_index + 1}/{len(self.test_results)} 个测试结果: {result['test_name']}")
            
            # 获取SQL预览
            sql_preview = "N/A"
            sql_full = "无SQL信息"
            try:
                # 从StreamSQLTemplates获取SQL
                sql_obj = StreamSQLTemplates.get_sql(
                    config['sql_type'],
                    agg_or_select=config.get('agg_or_select', 'agg'),
                    tbname_or_trows_or_sourcetable=config.get('tbname_or_trows_or_sourcetable', 'sourcetable')
                )
                
                if isinstance(sql_obj, dict):
                    # 如果是字典，取第一个SQL作为预览
                    first_key = list(sql_obj.keys())[0]
                    sql_full = sql_obj[first_key]
                    sql_preview = f"批量({len(sql_obj)}个) - {first_key}"
                else:
                    sql_full = str(sql_obj)
                    # 格式化SQL用于显示
                    formatted_sql = format_sql_for_display(sql_full)
                    
                    # 提取流名称作为预览
                    import re
                    match = re.search(r'create\s+stream\s+([^\s|]+)', formatted_sql, re.IGNORECASE)
                    if match:
                        stream_name = match.group(1)
                        # 创建简洁的预览，显示流名称和主要子句
                        preview_parts = []
                        preview_parts.append(f"CREATE STREAM {stream_name}")
                        
                        # 提取窗口类型
                        if 'interval(' in formatted_sql.lower():
                            interval_match = re.search(r'interval\([^)]+\)', formatted_sql, re.IGNORECASE)
                            if interval_match:
                                preview_parts.append(interval_match.group(0).upper())
                        elif 'sliding(' in formatted_sql.lower():
                            sliding_match = re.search(r'sliding\([^)]+\)', formatted_sql, re.IGNORECASE)
                            if sliding_match:
                                preview_parts.append(sliding_match.group(0).upper())
                        elif 'session(' in formatted_sql.lower():
                            session_match = re.search(r'session\([^)]+\)', formatted_sql, re.IGNORECASE)
                            if session_match:
                                preview_parts.append(session_match.group(0).upper())
                        elif 'period(' in formatted_sql.lower():
                            period_match = re.search(r'period\([^)]+\)', formatted_sql, re.IGNORECASE)
                            if period_match:
                                preview_parts.append(period_match.group(0).upper())
                        elif 'count_window(' in formatted_sql.lower():
                            count_match = re.search(r'count_window\([^)]+\)', formatted_sql, re.IGNORECASE)
                            if count_match:
                                preview_parts.append(count_match.group(0).upper())
                        
                        sql_preview = " ".join(preview_parts)
                    else:
                        # 如果无法提取流名称，显示前50个字符
                        sql_preview = formatted_sql[:50] + "..." if len(formatted_sql) > 50 else formatted_sql
                    
                    # 使用格式化后的SQL作为完整SQL
                    sql_full = formatted_sql
                        
                        
            except Exception as e:
                sql_preview = f"获取失败: {str(e)[:30]}"
                sql_full = f"SQL获取错误: {str(e)}"
            
            # 清理SQL中的特殊字符
            sql_full_escaped = sql_full.replace('"', '&quot;').replace("'", '&#39;').replace('\n', '\\n').replace('\r', '\\r')
            
            # 读取性能数据
            cpu_peak, memory_peak = "N/A", "N/A"
            cpu_range_info = ""
            memory_range_info = ""
            cpu_peak_value = None
            memory_peak_value = None
            cpu_avg_value = None
            memory_avg_value = None
            memory_avg_percentage = None
            cpu_avg_info = ""
            memory_avg_info = ""
            
            
            # 只有测试成功时才尝试读取性能数据
            if result['status'] == 'SUCCESS':
                try:
                    perf_file = config.get('perf_file', '')
                    print(f"调试: 第 {result_index + 1} 个测试，尝试读取性能文件: {perf_file}")
                    
                    # 检查各种可能的性能文件路径
                    possible_files = []
                    if perf_file:
                        # 原始文件路径
                        possible_files.append(perf_file)
                        # 去掉扩展名加-all.log
                        base_name = os.path.splitext(perf_file)[0]
                        possible_files.append(f"{base_name}-all.log")
                        # 直接加-all.log
                        possible_files.append(f"{perf_file}-all.log")
                    
                    # 添加一些常见的性能文件路径
                    possible_files.extend([
                        '/tmp/perf-taosd-all.log',
                        '/tmp/perf-stream-test-all.log', 
                        '/tmp/perf-all.log'
                    ])
                    
                    print(f"调试: 第 {result_index + 1} 个测试，检查文件路径: {possible_files}")
                    
                    # 尝试读取第一个存在的文件
                    perf_content = None
                    used_file = None
                    for file_path in possible_files:
                        if os.path.exists(file_path):
                            print(f"调试: 第 {result_index + 1} 个测试，找到性能文件: {file_path}")
                            try:
                                with open(file_path, 'r') as f:
                                    perf_content = f.read()
                                    used_file = file_path
                                    break
                            except Exception as e:
                                print(f"调试: 第 {result_index + 1} 个测试，读取文件 {file_path} 失败: {str(e)}")
                                continue
                    
                    if perf_content:
                        print(f"调试: 第 {result_index + 1} 个测试，成功读取性能文件 {used_file}, 内容长度: {len(perf_content)}")
                        
                        # 解析性能数据
                        cpu_values = []
                        memory_values = []
                        memory_percentages = []
                        
                        lines = perf_content.split('\n')
                        print(f"调试: 第 {result_index + 1} 个测试，文件总行数: {len(lines)}")
                        
                        valid_lines_count = 0
                        warm_up_lines_count = 0  
                        formal_monitoring_started = False 
                        warm_up_end_detected = False 
                    
                        for line_num, line in enumerate(lines):
                            line = line.strip()
                            if not line:
                                continue
                            
                            # 检查是否是预热期结束标记
                            if '=== 预热期结束，正式监控开始 ===' in line or '正式监控开始' in line:
                                formal_monitoring_started = True
                                warm_up_end_detected = True
                                print(f"调试: 第 {result_index + 1} 个测试，在第{line_num+1}行发现正式监控开始标记")
                                continue
                                
                            # 查找包含CPU和Memory的行
                            # 格式: 2025-09-01 17:34:18 [dnode1] CPU: 271.4%, Memory: 787.64MB (1.23%), Read: 0.00MB (7492031), Write: 665.98MB (13546146)
                            if 'CPU:' in line and 'Memory:' in line and '%' in line and 'MB' in line:
                                
                                # 检查是否是预热期数据
                                is_warm_up_data = '[预热期数据]' in line
                                
                                if is_warm_up_data:
                                    warm_up_lines_count += 1
                                    # 跳过预热期数据，不统计
                                    continue
                                
                                # 新增逻辑：如果设置了预热时间但没有检测到预热期结束标记
                                # 则根据时间戳判断或者放宽统计条件
                                if config.get('monitor_warm_up_time', 0) > 0:
                                    if not warm_up_end_detected and not formal_monitoring_started:
                                        # 根据行数判断：前面的行可能是预热期数据
                                        warm_up_cycles = config.get('monitor_warm_up_time', 0) // config.get('monitor_interval', 5)
                                        # 可能是标记格式问题，尝试更宽松的匹配
                                        if warm_up_lines_count > 0:
                                            # 已经开始有预热期数据，继续等待结束标记
                                            continue
                                        elif line_num < 10:
                                            # 前10行可能还在预热期
                                            continue
                                        else:
                                            # 超过10行后，如果没有预热期标记，可能是正式数据
                                            formal_monitoring_started = True
                                            print(f"调试: 第 {result_index + 1} 个测试，未找到明确的预热期结束标记，从第{line_num+1}行开始统计")
                                
                                # 统计正式监控期的数据
                                valid_lines_count += 1
                                
                                try:
                                    import re
                                    
                                    # 提取CPU使用率
                                    cpu_match = re.search(r'CPU:\s*([\d.]+)%', line)
                                    if cpu_match:
                                        cpu_value = float(cpu_match.group(1))
                                        cpu_values.append(cpu_value)
                                        #print(f"调试: 提取CPU值: {cpu_value}")
                                    
                                    # 提取内存使用量(MB)
                                    memory_match = re.search(r'Memory:\s*([\d.]+)MB\s*\(([\d.]+)%\)', line)
                                    if memory_match:
                                        memory_value = float(memory_match.group(1))
                                        memory_percentage = float(memory_match.group(2))
                                        memory_values.append(memory_value)
                                        memory_percentages.append(memory_percentage)
                                        #print(f"调试: 提取内存值: {memory_value}")
                                        
                                except Exception as e:
                                    print(f"调试: 第 {result_index + 1} 个测试，解析第{line_num+1}行出错: {str(e)}")
                                    continue
                        
                        print(f"调试: 第 {result_index + 1} 个测试，预热期数据行数: {warm_up_lines_count}")
                        print(f"调试: 第 {result_index + 1} 个测试，正式监控期数据行数: {valid_lines_count}")
                        print(f"调试: 第 {result_index + 1} 个测试，预热期结束检测: {warm_up_end_detected}")
                        print(f"调试: 收集到CPU峰值: {max(cpu_values) if cpu_values else 'N/A'}")
                        print(f"调试: 收集到内存峰值: {max(memory_values) if memory_values else 'N/A'}")
                        
                        if cpu_values:
                            cpu_peak_value = max(cpu_values)
                            cpu_min_value = min(cpu_values)
                            cpu_avg_value = sum(cpu_values) / len(cpu_values) 
                            
                            # 构建CPU范围信息，包含系统核数
                            cpu_core_info = f"(CPU(s):{system_cpu_count})" if system_cpu_count else ""
                            cpu_range_info = f"{cpu_min_value:.1f}% -> {cpu_peak_value:.1f}%{cpu_core_info}"
                            cpu_peak = f"{cpu_peak_value:.1f}"
                            cpu_avg_info = f"avg:{cpu_avg_value:.1f}%"
                            
                            print(f"调试: 第 {result_index + 1} 个测试，正式监控期CPU峰值: {cpu_peak}%, 平均值: {cpu_avg_value:.1f}%")
                            print(f"调试: 第 {result_index + 1} 个测试，正式监控期CPU范围: {cpu_range_info}")
                            
                        else:
                            print(f"调试: 第 {result_index + 1} 个测试，未收集到CPU数据")
                            cpu_range_info = "无正式监控期数据"
                            cpu_avg_info = ""
                        
                        if memory_values and memory_percentages:
                            memory_peak_value = max(memory_values)
                            memory_min_value = min(memory_values)
                            memory_avg_value = sum(memory_values) / len(memory_values)  # 新增：计算平均值
                            memory_avg_percentage = sum(memory_percentages) / len(memory_percentages)  # 新增：平均百分比
                            
                            memory_peak_percentage = memory_percentages[memory_values.index(memory_peak_value)]
                            memory_min_percentage = memory_percentages[memory_values.index(memory_min_value)]
                            
                            memory_peak = f"{memory_peak_value:.0f}"
                            
                            # 构建内存范围信息，包含总内存和百分比
                            memory_total_info = f"({system_total_memory})" if system_total_memory else ""
                            memory_range_info = f"{memory_min_value:.0f}MB({memory_min_percentage:.1f}%) -> {memory_peak_value:.0f}MB({memory_peak_percentage:.1f}%){memory_total_info}"
                            # 新增：内存平均值信息
                            memory_avg_info = f"avg:{memory_avg_value:.0f}MB({memory_avg_percentage:.1f}%){memory_total_info}"
                        
                            print(f"调试: 第 {result_index + 1} 个测试，正式监控期内存峰值: {memory_peak}MB, 平均值: {memory_avg_value:.0f}MB({memory_avg_percentage:.1f}%)")
                            print(f"调试: 第 {result_index + 1} 个测试，正式监控期内存范围: {memory_range_info}")
                            
                        else:
                            print(f"调试: 第 {result_index + 1} 个测试，未收集到内存数据")
                            memory_range_info = "无正式监控期数据"
                            memory_avg_value = None
                            memory_avg_percentage = None
                            memory_avg_info = ""
                            
                        # 数据质量检查
                        if valid_lines_count == 0:
                            print(f"警告: 第 {result_index + 1} 个测试，未找到正式监控期数据")
                            for line_num, line in enumerate(lines):
                                line = line.strip()
                                if not line:
                                    continue
                                    
                                if 'CPU:' in line and 'Memory:' in line and '%' in line and 'MB' in line:
                                    valid_lines_count += 1
                                    
                                    try:
                                        import re
                                        
                                        cpu_match = re.search(r'CPU:\s*([\d.]+)%', line)
                                        if cpu_match:
                                            cpu_value = float(cpu_match.group(1))
                                            cpu_values.append(cpu_value)
                                        
                                        memory_match = re.search(r'Memory:\s*([\d.]+)MB\s*\(([\d.]+)%\)', line)
                                        if memory_match:
                                            memory_value = float(memory_match.group(1))
                                            memory_percentage = float(memory_match.group(2))
                                            memory_values.append(memory_value)
                                            memory_percentages.append(memory_percentage)
                                            
                                    except Exception as e:
                                        continue
                            
                            print(f"调试: 第 {result_index + 1} 个测试，兜底处理后收集到数据行数: {valid_lines_count}")
                        elif valid_lines_count < 5:
                            print(f"警告: 第 {result_index + 1} 个测试，正式监控期数据点过少({valid_lines_count}个)，可能影响统计准确性")
                            
                    else:
                        print(f"调试: 第 {result_index + 1} 个测试，未找到任何可用的性能文件")
                        cpu_range_info = "性能文件不存在"
                        memory_range_info = "性能文件不存在"
                        cpu_avg_value = None
                        memory_avg_value = None
                        memory_avg_percentage = None
                        cpu_avg_info = ""
                        memory_avg_info = ""
                        
                except Exception as e:
                    print(f"调试: 第 {result_index + 1} 个测试，读取性能数据时出错: {str(e)}")
                    cpu_range_info = f"读取失败: {str(e)}"
                    memory_range_info = f"读取失败: {str(e)}"
                    cpu_avg_value = None
                    memory_avg_value = None
                    memory_avg_percentage = None
                    cpu_avg_info = ""
                    memory_avg_info = ""
            else:
                print(f"调试: 第 {result_index + 1} 个测试失败，跳过性能数据读取")
                cpu_range_info = "N/A"
                memory_range_info = "N/A"
                cpu_avg_value = None
                memory_avg_value = None
                memory_avg_percentage = None
                cpu_avg_info = ""
                memory_avg_info = ""
            
            
            # CPU和内存的颜色样式
            cpu_class = ""
            if cpu_peak != "N/A" and cpu_peak_value is not None:
                try:
                    # 计算实际的CPU使用率百分比
                    if system_cpu_count:
                        try:
                            total_cpu_cores = int(system_cpu_count)
                            # 计算实际CPU使用率：当前使用率 / 总核数 
                            actual_cpu_usage_percent = cpu_peak_value / total_cpu_cores
                            print(f"调试: CPU峰值颜色计算 - 峰值: {cpu_peak_value}%, 总核数: {total_cpu_cores}, 实际使用率: {actual_cpu_usage_percent:.1f}%")
                            
                            # 基于实际使用率判断颜色
                            if actual_cpu_usage_percent < 30:      # 实际使用率低于30%
                                cpu_class = "perf-good"
                                print(f"调试: CPU峰值颜色判断 -> 绿色 (实际使用率 {actual_cpu_usage_percent:.1f}% < 30%)")
                            elif actual_cpu_usage_percent < 70:   # 实际使用率30%-70%
                                cpu_class = "perf-warning"
                                print(f"调试: CPU峰值颜色判断 -> 黄色 (实际使用率 {actual_cpu_usage_percent:.1f}% 在30%-70%)")
                            else:                                  # 实际使用率超过70%
                                cpu_class = "perf-danger"
                                print(f"调试: CPU峰值颜色判断 -> 红色 (实际使用率 {actual_cpu_usage_percent:.1f}% > 70%)")
                                
                        except (ValueError, TypeError) as e:
                            print(f"调试: 第 {result_index + 1} 个测试，解析CPU核数失败: {e}")
                            if cpu_peak_value < 500:
                                cpu_class = "perf-good"
                            elif cpu_peak_value < 1000:
                                cpu_class = "perf-warning"
                            else:
                                cpu_class = "perf-danger"
                    else:
                        print(f"调试: 未获取到CPU核数，使用原始值判断")
                        # 没有获取到CPU核数，使用更宽松的判断标准
                        # 假设是多核系统，适当放宽标准
                        if cpu_peak_value < 500:      # 假设至少16核，500%以下为良好
                            cpu_class = "perf-good"
                        elif cpu_peak_value < 1000:   # 500%-1000%为警告
                            cpu_class = "perf-warning"
                        else:                  # 超过1000%为危险
                            cpu_class = "perf-danger"
                            
                except Exception as e:
                    print(f"调试:第 {result_index + 1} 个测试，CPU颜色判断出错: {e}")
                    cpu_class = ""
            else:
                print(f"调试: CPU数据无效，跳过颜色判断 - cpu_peak: {cpu_peak}, cpu_peak_value: {cpu_peak_value}")
            
            
            # ========== CPU平均值的颜色样式（新增独立判断） ==========
            cpu_avg_class = ""
            if cpu_avg_value is not None:  # 使用平均值进行独立的颜色判断
                try:
                    if system_cpu_count:
                        try:
                            total_cpu_cores = int(system_cpu_count)
                            # 使用平均值计算实际CPU使用率
                            actual_cpu_avg_usage_percent = cpu_avg_value / total_cpu_cores
                            print(f"调试: CPU平均值颜色计算 - 平均值: {cpu_avg_value:.1f}%, 总核数: {total_cpu_cores}, 实际平均使用率: {actual_cpu_avg_usage_percent:.1f}%")
                            
                            # 基于平均使用率判断颜色
                            if actual_cpu_avg_usage_percent < 30:
                                cpu_avg_class = "perf-good"
                                print(f"调试: CPU平均值颜色判断 -> 绿色 (平均使用率 {actual_cpu_avg_usage_percent:.1f}% < 30%)")
                            elif actual_cpu_avg_usage_percent < 70:
                                cpu_avg_class = "perf-warning"
                                print(f"调试: CPU平均值颜色判断 -> 黄色 (平均使用率 {actual_cpu_avg_usage_percent:.1f}% 在30%-70%)")
                            else:
                                cpu_avg_class = "perf-danger"
                                print(f"调试: CPU平均值颜色判断 -> 红色 (平均使用率 {actual_cpu_avg_usage_percent:.1f}% > 70%)")
                                
                        except (ValueError, TypeError) as e:
                            print(f"调试: 第 {result_index + 1} 个测试，解析CPU核数失败: {e}")
                            # 使用平均值的备用判断
                            if cpu_avg_value < 500:
                                cpu_avg_class = "perf-good"
                            elif cpu_avg_value < 1000:
                                cpu_avg_class = "perf-warning"
                            else:
                                cpu_avg_class = "perf-danger"
                    else:
                        print(f"调试: 未获取到CPU核数，使用平均值判断")
                        if cpu_avg_value < 500:
                            cpu_avg_class = "perf-good"
                        elif cpu_avg_value < 1000:
                            cpu_avg_class = "perf-warning"
                        else:
                            cpu_avg_class = "perf-danger"
                            
                except Exception as e:
                    print(f"调试:第 {result_index + 1} 个测试，CPU平均值颜色判断出错: {e}")
                    cpu_avg_class = ""
            else:
                print(f"调试: CPU平均值数据无效，跳过颜色判断 - cpu_avg_value: {cpu_avg_value}")
            
            
            memory_class = ""
            if memory_range_info and "MB" in memory_range_info:
                try:
                    import re
                    # 提取内存峰值和百分比，匹配 "96MB(0.1%) -> 787MB(1.2%)(62.5GB)" 中的 787MB(1.2%) 部分
                    match = re.search(r'(\d+)MB\(([\d.]+)%\)', memory_range_info.split('->')[-1])
                    if match:
                        memory_peak_mb = float(match.group(1))
                        memory_peak_percentage = float(match.group(2))
                        
                        print(f"调试: 第 {result_index + 1} 个测试内存判断 - 峰值: {memory_peak_mb}MB, 占用率: {memory_peak_percentage}%")
                        
                        # 基于内存使用率的判断
                        if memory_peak_percentage < 25.0:         # 使用率 < 25%
                            memory_class = "perf-good"
                            judgment_reason = f"内存使用良好 (使用率{memory_peak_percentage:.1f}%, 绝对值{memory_peak_mb:.0f}MB)"
                        elif memory_peak_percentage < 50.0:      # 使用率 25%-50%
                            memory_class = "perf-warning" 
                            judgment_reason = f"内存使用一般 (使用率{memory_peak_percentage:.1f}%, 绝对值{memory_peak_mb:.0f}MB)"
                        else:                                     # 使用率 > 50%
                            memory_class = "perf-danger"
                            judgment_reason = f"内存使用过高 (使用率{memory_peak_percentage:.1f}%, 绝对值{memory_peak_mb:.0f}MB)"
                                                    
                        print(f"调试: 第 {result_index + 1} 个测试内存颜色判断 -> {memory_class} ({judgment_reason})")
                        
                        # 特殊情况处理
                        # 如果系统内存很大(>32GB)但使用率很低，即使绝对值大也认为是好的
                        if system_total_memory:
                            try:
                                total_memory_gb = float(system_total_memory.replace('GB', ''))
                                if total_memory_gb > 32 and memory_peak_percentage < 10.0:
                                    memory_class = "perf-good"
                                    print(f"调试: 大内存系统特殊处理 -> 绿色 (系统{total_memory_gb}GB, 使用率仅{memory_peak_percentage:.1f}%)")
                            except Exception as e:
                                print(f"调试: 第 {result_index + 1} 个测试解析系统总内存失败: {str(e)}")
                        
                        # 极端情况警告
                        if memory_peak_mb > 49152:  # > 48GB
                            print(f"调试: ⚠️  内存使用量过大警告: {memory_peak_mb:.0f}MB，建议检查是否有内存泄漏")
                            
                    else:
                        print(f"调试: 第 {result_index + 1} 个测试无法从内存范围信息中解析峰值和百分比")
                        print(f"调试: 内存范围信息内容: {memory_range_info}")
                        
                except Exception as e:
                    print(f"调试:第 {result_index + 1} 个测试内存颜色判断出错: {str(e)}")
                    import traceback
                    print(f"调试: 异常详情: {traceback.format_exc()}")
                    memory_class = ""
                    
            else:
                print(f"调试: 第 {result_index + 1} 个测试内存数据无效，跳过颜色判断 - memory_range_info: {memory_range_info}")
            
            
            # ========== 内存平均值的颜色样式（新增独立判断） ==========
            memory_avg_class = ""
            if memory_avg_value is not None and memory_avg_percentage is not None:  
                try:
                    print(f"调试: 第 {result_index + 1} 个测试内存平均值判断 - 平均值: {memory_avg_value:.0f}MB, 平均占用率: {memory_avg_percentage:.1f}%")
                    
                    # 基于内存平均使用率的判断
                    if memory_avg_percentage < 25.0:
                        memory_avg_class = "perf-good"
                        judgment_reason = f"内存使用良好 (平均使用率{memory_avg_percentage:.1f}%, 平均值{memory_avg_value:.0f}MB)"
                    elif memory_avg_percentage < 50.0:
                        memory_avg_class = "perf-warning" 
                        judgment_reason = f"内存使用一般 (平均使用率{memory_avg_percentage:.1f}%, 平均值{memory_avg_value:.0f}MB)"
                    else:
                        memory_avg_class = "perf-danger"
                        judgment_reason = f"内存使用过高 (平均使用率{memory_avg_percentage:.1f}%, 平均值{memory_avg_value:.0f}MB)"
                                                
                    print(f"调试: 第 {result_index + 1} 个测试内存平均值颜色判断 -> {memory_avg_class} ({judgment_reason})")
                    
                    # 特殊情况处理
                    if system_total_memory:
                        try:
                            total_memory_gb = float(system_total_memory.replace('GB', ''))
                            if total_memory_gb > 32 and memory_avg_percentage < 10.0:
                                memory_avg_class = "perf-good"
                                print(f"调试: 大内存系统特殊处理 -> 绿色 (系统{total_memory_gb}GB, 平均使用率仅{memory_avg_percentage:.1f}%)")
                        except Exception as e:
                            print(f"调试: 第 {result_index + 1} 个测试解析系统总内存失败: {str(e)}")
                    
                    # 极端情况警告
                    if memory_avg_value > 49152:  # > 48GB
                        print(f"调试: ⚠️  内存平均使用量过大警告: {memory_avg_value:.0f}MB，建议检查是否有内存泄漏")
                        
                except Exception as e:
                    print(f"调试:第 {result_index + 1} 个测试内存平均值颜色判断出错: {str(e)}")
                    memory_avg_class = ""
                    
            else:
                print(f"调试: 第 {result_index + 1} 个测试内存平均值数据无效，跳过颜色判断 - memory_avg_value: {memory_avg_value}, memory_avg_percentage: {memory_avg_percentage}")
            
            # 读取延迟统计
            delay_stats = "N/A"
            delay_stats_html = "N/A"
            delay_class = ""
            
            if result['status'] == 'SUCCESS':
                try:
                    delay_file = config.get('delay_log_file', '')
                    print(f"调试: 第 {result_index + 1} 个测试，尝试读取延迟文件: {delay_file}")
                    
                    # 验证这是否是当前测试的文件（通过测试名称匹配）
                    test_name = result.get('test_name', '')
                    if delay_file and test_name:
                        # 检查延迟文件路径是否包含当前测试名称
                        if test_name.replace('test_', '').replace('_', '') not in delay_file.replace('_', '').replace('-', ''):
                            print(f"调试: 第 {result_index + 1} 个测试，延迟文件路径与测试名称不匹配")
                            print(f"  测试名称: {test_name}")
                            print(f"  延迟文件: {delay_file}")
                            # 尝试根据测试名称重新构建正确的延迟文件路径
                            result_dir_from_perf = os.path.dirname(os.path.dirname(config.get('perf_file', '')))
                            if result_dir_from_perf:
                                corrected_delay_file = os.path.join(result_dir_from_perf, 'logs', f'{test_name}_delay.log')
                                if os.path.exists(corrected_delay_file):
                                    delay_file = corrected_delay_file
                                    print(f"调试: 第 {result_index + 1} 个测试，使用修正的延迟文件: {delay_file}")
                    
                    # 检查文件是否存在
                    if not delay_file:
                        print(f"调试: 第 {result_index + 1} 个测试，延迟文件路径为空")
                        delay_stats = "配置错误：延迟文件路径为空"
                        delay_stats_html = "配置错误：延迟文件路径为空"
                    elif not os.path.exists(delay_file):
                        print(f"调试: 第 {result_index + 1} 个测试，延迟文件不存在: {delay_file}")
                        delay_stats = "延迟文件不存在"
                        delay_stats_html = "延迟文件不存在"
                        
                        # 尝试查找可能的延迟文件
                        possible_delay_files = []
                        delay_dir = os.path.dirname(delay_file)
                        if os.path.exists(delay_dir):
                            try:
                                files = os.listdir(delay_dir)
                                # 查找包含当前测试名称的延迟文件
                                test_specific_files = [f for f in files if 'delay' in f and test_name in f]
                                if test_specific_files:
                                    possible_delay_files.extend([os.path.join(delay_dir, f) for f in test_specific_files])
                                    print(f"调试: 第 {result_index + 1} 个测试，找到可能的延迟文件: {test_specific_files}")
                            except Exception as e:
                                print(f"调试: 第 {result_index + 1} 个测试，读取目录失败: {str(e)}")
                        
                        # 尝试使用找到的延迟文件
                        for possible_file in possible_delay_files:
                            if os.path.exists(possible_file):
                                delay_file = possible_file
                                print(f"调试: 第 {result_index + 1} 个测试，使用找到的延迟文件: {possible_file}")
                                break
                    
                    # 读取延迟文件
                    if delay_file and os.path.exists(delay_file):
                        print(f"调试: 第 {result_index + 1} 个测试，开始读取延迟文件: {delay_file}")
                        
                        with open(delay_file, 'r') as f:
                            content = f.read()
                            
                            if len(content) > 0:
                                print(f"调试: 第 {result_index + 1} 个测试，延迟文件内容前200字符: {content[:200]}")
                            
                            # 统计延迟分布
                            delay_counts = {
                                '优秀': content.count('延迟等级: 优秀'),
                                '良好': content.count('延迟等级: 良好'), 
                                '正常': content.count('延迟等级: 正常'),
                                '轻微延迟': content.count('延迟等级: 轻微延迟'),
                                '明显延迟': content.count('延迟等级: 明显延迟'),
                                '严重延迟': content.count('延迟等级: 严重延迟')
                            }
                            
                            print(f"调试: 第 {result_index + 1} 个测试，延迟分布统计: {delay_counts}")
                            
                            # 构建延迟统计字符串
                            total_checks = sum(delay_counts.values())
                            print(f"调试: 第 {result_index + 1} 个测试，总检查次数: {total_checks}")
                            
                            if total_checks > 0:
                                # 定义延迟级别对应的CSS类
                                delay_level_classes = {
                                    '优秀': 'delay-excellent',
                                    '良好': 'delay-good',
                                    '正常': 'delay-normal',
                                    '轻微延迟': 'delay-warning',
                                    '明显延迟': 'delay-danger',
                                    '严重延迟': 'delay-critical'
                                }
                            
                                # 构建纯文本版本（用于导出等）
                                delay_parts = []
                                # 构建HTML版本（每个级别有自己的颜色）
                                delay_parts_html = []
                                
                                for level, count in delay_counts.items():
                                    if count > 0:
                                        percentage = count / total_checks * 100
                                        text_part = f"{level}:{count}({percentage:.0f}%)"
                                        delay_parts.append(text_part)
                                        
                                        # HTML版本：每个级别用对应的CSS类包装
                                        css_class = delay_level_classes.get(level, 'delay-normal')
                                        html_part = f'<span class="{css_class}">{level}:{count}({percentage:.0f}%)</span>'
                                        delay_parts_html.append(html_part)
                                
                                # 纯文本版本
                                delay_stats = " | ".join(delay_parts) if delay_parts else "无有效延迟数据"
                                
                                # HTML版本（多颜色显示）
                                delay_stats_html = " | ".join(delay_parts_html) if delay_parts_html else "无有效延迟数据"
                                
                                print(f"调试: 第 {result_index + 1} 个测试，生成的延迟统计: {delay_stats}")
                                print(f"调试: 第 {result_index + 1} 个测试，生成的HTML延迟统计: {delay_stats_html}")
                                
                                # 根据主要延迟级别设置颜色
                                if delay_counts['优秀'] > total_checks * 0.8:
                                    delay_class = "delay-excellent"
                                elif delay_counts['良好'] + delay_counts['优秀'] > total_checks * 0.7:
                                    delay_class = "delay-good"
                                elif delay_counts['正常'] + delay_counts['良好'] + delay_counts['优秀'] > total_checks * 0.6:
                                    delay_class = "delay-normal"
                                elif delay_counts['轻微延迟'] > 0:
                                    delay_class = "delay-warning"
                                elif delay_counts['明显延迟'] > 0:
                                    delay_class = "delay-danger"
                                else:
                                    delay_class = "delay-critical"
                                    
                                print(f"调试: 第 {result_index + 1} 个测试，延迟颜色类: {delay_class}")
                            else:
                                print(f"调试: 第 {result_index + 1} 个测试，总检查次数为0，延迟监控可能未正常工作")
                                delay_stats = "延迟监控未生成有效数据"
                                delay_stats_html = "延迟监控未生成有效数据"
                    else:
                        print(f"调试: 第 {result_index + 1} 个测试，最终未找到有效的延迟文件")
                        delay_stats = "N/A"
                        delay_stats_html = "N/A"
                            
                except Exception as e:
                    print(f"调试: 第 {result_index + 1} 个测试，读取延迟统计时出错: {str(e)}")
                    delay_stats = f"读取失败: {str(e)}"
                    delay_stats_html = f"读取失败: {str(e)}"
            else:
                print(f"调试: 第 {result_index + 1} 个测试状态为 {result['status']}，跳过延迟数据读取")
                delay_stats = "N/A"
                delay_stats_html = "N/A"
            
             # 调试：打印每个结果的错误信息
            print(f"调试HTML报告 - 测试 {result['test_name']}:")
            print(f"  状态: {result['status']}")
            print(f"  错误字段存在: {'error' in result}")
            if 'error' in result:
                error_content = result.get('error', '')
                print(f"  错误内容: {str(error_content)[:100]}..." if error_content else "  错误内容: 空")
                
            # 错误信息处理
            error_msg = result.get('error', '')
            print(f"调试: 原始error_msg = {repr(error_msg)}")
            
            if error_msg:
                # 如果错误信息太长，截断但保留关键信息
                if len(error_msg) > 300:
                    error_msg = error_msg[:300] + "..."
                error_msg = error_msg.replace('<', '&lt;').replace('>', '&gt;')
                print(f"调试: HTML转义后error_msg = {repr(error_msg[:300])}")
            else:
                error_msg = ""
                print(f"调试: error_msg为空")
                
            # 处理跳过原因显示
            if result['status'] == 'SKIPPED':
                skip_reason = result.get('skip_reason', '未知原因')
                error_msg = f"跳过原因: {skip_reason}"
 
            cpu_peak_display = f"{cpu_peak_value:.1f}" if cpu_peak_value is not None else "N/A"
            memory_peak_display = f"{memory_peak_value:.0f}" if memory_peak_value else "N/A"
            # 修改平均值显示，包含完整的单位和百分比信息
            if cpu_avg_value is not None:
                cpu_avg_display = f"{cpu_avg_value:.1f}%{cpu_core_info}"
            else:
                cpu_avg_display = "N/A"
                
            if memory_avg_value is not None and memory_avg_percentage is not None:
                memory_avg_display = f"{memory_avg_value:.0f}MB({memory_avg_percentage:.1f}%){memory_total_info}"
            else:
                memory_avg_display = "N/A"
                   
            
            html_content += f"""
            <tr class="{status_class}">
                <td><strong>{result['test_name']}</strong></td>
                <td>{config['sql_type']}</td>
                <td>{config['agg_or_select']}</td>
                <td>{config['tbname_or_trows_or_sourcetable']}</td>
                <td><span class="status-badge {status_badge}">{result['status']}</span></td>
                <td>{result.get('duration', 0):.1f}</td>
                <td>
                    <div class="tooltip">
                        <div class="sql-preview">{sql_preview}</div>
                        <span class="tooltiptext">{sql_full_escaped}</span>
                    </div>
                </td>
                <td class="{cpu_class}">
                    {cpu_range_info if cpu_range_info else 'N/A'}
                </td>
                <td class="{memory_class}">
                    {memory_range_info if memory_range_info else 'N/A'}
                </td>
                <td class="{cpu_class}">
                    {cpu_avg_display}
                </td>
                <td class="{memory_class}">
                    {memory_avg_display}
                </td>
                <td style="font-size: 10px;">{delay_stats_html}</td>
                <td style="color: #dc3545; font-size: 10px;">{error_msg}</td>
            </tr>
"""
        
        print(f"调试: HTML行生成完成，错误列内容: {error_msg[:300]}...")
        
        
        html_content += f"""
        </table>
    </div>
    
    <div style="margin-top: 30px; padding: 20px; background: white; border-radius: 10px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);">
        <h3>📊 使用说明</h3>
        <ul>
            <li><strong>状态说明</strong>: 
                <span class="badge-success">成功</span> - 流创建成功并正常运行
                <span class="badge-failed">失败</span> - 流创建失败，通常为SQL语法或功能限制
                <span class="badge-skipped">跳过</span> - 已知失败场景，被过滤器跳过测试
            </li>
            <li><strong>SQL预览</strong>: 鼠标悬停查看完整SQL语句</li>
            <li><strong>性能指标</strong>: 
                CPU/内存 - <span class="perf-good">绿色(优秀)</span> 
                <span class="perf-warning">黄色(一般)</span> 
                <span class="perf-danger">红色(较高)</span>
                <br>显示格式: 资源消耗范围(最小->最大) + 系统信息
                <br><strong>CPU颜色判断</strong>: 基于实际CPU使用率 (显示值÷总核数) 
                <br>　　绿色: 实际使用率 < 30% | 黄色: 30%-70% | 红色: > 70%
                <br><strong>内存颜色判断</strong>: 综合考虑使用率和绝对值
                <br>　　绿色: 使用率<25% | 黄色: 使用率25%-50% | 红色: 使用率>50%
                <br>　　特殊: 大内存系统(>32GB)且使用率<10%时优先判为优秀
            </li>
            <li><strong>性能范围说明</strong>:
                <br>CPU峰值范围: 显示测试期间的CPU使用率范围(最小->最大)，括号内为系统CPU核数
                <br>内存峰值范围: 显示内存使用量范围(最小->最大)和占系统总内存百分比，括号内为系统总内存
                <br>CPU平均值: 显示测试期间的CPU平均值
                <br>内存平均值: 显示内存使用平均值
            </li>
            <li><strong>延迟等级</strong> (基于最大延迟阈值{self.fixed_params.get('max_delay_threshold', 30000)}ms进行分级):                  
                <br>• 延迟时间 = 源数据表的Last（Ts）- 流生成数据表的Last（Ts）
                <br>• 延迟倍数 = 延迟时间 / 最大延迟阈值 
                <br><span class="delay-excellent">🟢 优秀 (< 0.1倍间隔)</span>: 延迟 < {self.fixed_params.get('max_delay_threshold', 30000) * 0.1}秒，流计算非常及时
                <br><span class="delay-good">🟢 良好 (0.1-0.5倍间隔)</span>: 延迟 {self.fixed_params.get('max_delay_threshold', 30000) * 0.1}-{self.fixed_params.get('max_delay_threshold', 30000) * 0.5}秒，流计算及时
                <br><span class="delay-normal">🟡 正常 (0.5-1倍间隔)</span>: 延迟 {self.fixed_params.get('max_delay_threshold', 30000) * 0.5}-{self.fixed_params.get('max_delay_threshold', 30000)}秒，在检查间隔内
                <br><span class="delay-warning">🟡 轻微延迟 (1-6倍间隔)</span>: 延迟 {self.fixed_params.get('max_delay_threshold', 30000)}-{self.fixed_params.get('max_delay_threshold', 30000) * 6}秒，略有滞后
                <br><span class="delay-danger">🟠 明显延迟 (6-30倍间隔)</span>: 延迟 {self.fixed_params.get('max_delay_threshold', 30000) * 6}-{self.fixed_params.get('max_delay_threshold', 30000) * 30}秒，需要关注
                <br><span class="delay-critical">🔴 严重延迟 (> 30倍间隔)</span>: 延迟 > {self.fixed_params.get('max_delay_threshold', 30000) * 30}秒，需要优化
            </li>
            <li><strong>交互功能</strong>: 点击表头排序，使用筛选器快速查找</li>
        </ul>
    </div>
    
    <div style="margin-top: 20px; text-align: center; color: #666; font-size: 12px;">
        <p>📁 详细日志文件位置: {result_dir}</p>
        <p>🏃‍♂️ TDengine 流计算性能测试工具 | 生成时间: {current_time}</p>
    </div>
</body>
</html>
"""
        
        with open(report_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        print(f"详细HTML报告已生成: {report_file}")


        
    def run_batch_tests(self, test_time_minutes=5, max_parallel=1, start_from=0, test_limit=None):
        """运行批量测试
        
        Args:
            test_time_minutes: 每个测试的运行时间（分钟）
            max_parallel: 最大并行测试数（当前只支持1）
            start_from: 从第几个测试开始
            test_limit: 限制测试数量
        """
        print("开始批量流计算测试...")
        
        # 获取所有测试组合
        combinations = self.get_test_combinations()
        combinations = self.filter_combinations(combinations)
        
        # 检查过滤后是否还有测试
        if not combinations:
            print(f"\n❌ 错误: 过滤后没有有效的测试组合!")
            print(f"当前过滤模式: {self.filter_mode}")
            print(f"指定的SQL类型: {self.specified_sql_types}")
            print(f"\n💡 建议:")
            print(f"  1. 检查 --batch-filter-mode 参数设置")
            print(f"  2. 检查 --batch-sql-types 参数是否正确")
            print(f"  3. 尝试使用 --batch-filter-mode all 运行所有测试")
            print(f"  4. 使用 --batch-filter-mode skip-known-failures 运行成功的测试")
            
            # 仍然创建结果目录和生成报告
            result_dir = self.create_batch_result_dir()
            print(f"结果目录已创建: {result_dir}")
            self.start_time = datetime.datetime.now().isoformat()
            self.total_tests = 0
            self.generate_final_report(result_dir)
            return
        
        
        # 应用启动位置和限制
        if test_limit:
            combinations = combinations[start_from:start_from + test_limit]
        else:
            combinations = combinations[start_from:]
        
        self.total_tests = len(combinations)
        self.start_time = datetime.datetime.now().isoformat()
        
        print(f"总测试组合数: {self.total_tests}")
        print(f"每个测试运行时间: {test_time_minutes}分钟")
        
        # 防止除零错误
        if self.total_tests > 0:
            print(f"预计总耗时: {(self.total_tests * test_time_minutes / 60):.1f}小时")
        else:
            print(f"预计总耗时: 0小时 (没有测试要执行)")
        
        print(f"\n批量测试固定配置:")
        print(f"  流延迟检查: {'启用' if self.fixed_params.get('check_stream_delay', False) else '禁用'}")
        if self.fixed_params.get('check_stream_delay', False):
            print(f"  延迟检查间隔: {self.fixed_params.get('delay_check_interval', 10)}秒")
            print(f"  最大延迟阈值: {self.fixed_params.get('max_delay_threshold', 30000)}ms")
        print(f"  子表数量: {self.fixed_params.get('table_count', 1000)}")
        print(f"  每轮写入记录数: {self.fixed_params.get('real_time_batch_rows', 200)}")
        print(f"  数据写入间隔: {self.fixed_params.get('real_time_batch_sleep', 0)}秒")
        print(f"  部署模式: {self.fixed_params.get('deployment_mode', 'single')}")
        
        if start_from > 0:
            print(f"从第 {start_from + 1} 个测试开始")
        if test_limit:
            print(f"限制测试数量: {test_limit}")
        
        # 创建结果目录
        result_dir = self.create_batch_result_dir()
        print(f"测试结果将保存到: {result_dir}")
        
        # 如果没有测试要执行，直接生成报告并返回
        if self.total_tests == 0:
            print(f"\n⚠️ 没有测试需要执行，直接生成报告")
            self.generate_final_report(result_dir)
            return
        
        # 确认开始测试
        if self.total_tests > 10:
            response = input(f"\n将要执行 {self.total_tests} 个测试，预计耗时 {(self.total_tests * test_time_minutes / 60):.1f} 小时，是否继续？(y/N): ")
            if response.lower() != 'y':
                print("测试已取消")
                return
        
        # 执行测试
        for i, combination in enumerate(combinations, 1):
            self.current_test_index = start_from + i
            
            # 生成测试配置
            config = self.generate_test_config(combination, self.current_test_index, result_dir)
            config['time'] = test_time_minutes
            
            # 保存测试配置
            self.save_test_config(config, result_dir)
            
            # 执行测试
            result = self.execute_single_test(config)
            self.test_results.append(result)
            
            # 生成进度报告
            self.generate_progress_report(result_dir)
            
            # 显示进度
            remaining_tests = self.total_tests - len(self.test_results)
            estimated_remaining_time = remaining_tests * test_time_minutes
            
            # 统计各种状态
            success_count = len([r for r in self.test_results if r['status'] == 'SUCCESS'])
            failed_count = len([r for r in self.test_results if r['status'] == 'FAILED'])
            skipped_count = len([r for r in self.test_results if r['status'] == 'SKIPPED'])
            
            print(f"\n进度总结:")
            print(f"  已完成: {len(self.test_results)}/{self.total_tests}")
            print(f"  成功: {success_count}")
            print(f"  失败: {failed_count}")
            if skipped_count > 0:
                print(f"  跳过: {skipped_count}")
            print(f"  剩余估计时间: {estimated_remaining_time:.0f}分钟 ({estimated_remaining_time/60:.1f}小时)")
            
            # 在测试之间添加短暂休息
            if i < len(combinations):
                print("等待2秒后开始下一个测试...")
                time.sleep(2)
        
        # 生成最终报告
        self.generate_final_report(result_dir)
        
        print(f"\n{'='*80}")
        print("批量测试完成!")
        print(f"总测试数: {self.total_tests}")
        
        if self.total_tests > 0:
            success_count = len([r for r in self.test_results if r['status'] == 'SUCCESS'])
            failed_count = len([r for r in self.test_results if r['status'] == 'FAILED'])
            skipped_count = len([r for r in self.test_results if r['status'] == 'SKIPPED'])
            
            print(f"成功: {success_count}")
            print(f"失败: {failed_count}")
            if skipped_count > 0:
                print(f"跳过: {skipped_count}")
            print(f"成功率: {(success_count/self.total_tests*100):.1f}%")
            
            if self.filter_mode != 'all':
                print(f"过滤模式: {self.filter_mode}")
                if skipped_count > 0:
                    print(f"过滤效果: 成功跳过 {skipped_count} 个测试")
        else:
            print(f"没有执行任何测试")
            
        print(f"结果目录: {result_dir}")
        print(f"{'='*80}")

        
        
class StreamStarter:
    def __init__(self, runtime=None, perf_file=None, table_count=500, 
                histroy_rows=1, real_time_batch_rows=200, disorder_ratio=0, vgroups=10,
                stream_sql=None, sql_type='select_stream', stream_num=1, stream_perf_test_dir=None, monitor_interval=1,
                create_data=False, restore_data=False, deployment_mode='single',
                debug_flag=131, num_of_log_lines=500000, 
                agg_or_select='agg', tbname_or_trows_or_sourcetable='sourcetable', custom_columns=None,
                check_stream_delay=False, max_delay_threshold=30000, delay_check_interval=10,
                real_time_batch_sleep=0, delay_log_file=None, auto_combine=False, monitor_warm_up_time=None) -> None:
        
        self.stream_perf_test_dir = stream_perf_test_dir if stream_perf_test_dir else '/home/taos_stream_cluster'        
        self.table_count = table_count      
        self.histroy_rows = histroy_rows      
        self.real_time_batch_rows = real_time_batch_rows    
        self.real_time_batch_sleep = real_time_batch_sleep  
        self.disorder_ratio = disorder_ratio 
        self.vgroups = vgroups 
        self.monitor_interval = monitor_interval
        self.taosd_processes = [] 
        self.create_data = create_data
        self.restore_data = restore_data
        self.backup_dir = os.path.join(self.stream_perf_test_dir, 'data_bak')
        self.deployment_mode = deployment_mode
        self.debug_flag = debug_flag
        self.num_of_log_lines = num_of_log_lines
        self.auto_combine = auto_combine
        self.monitor_warm_up_time = monitor_warm_up_time
        
        self.sql_type = sql_type
        self.stream_num = stream_num
        self.agg_or_select = agg_or_select
        self.tbname_or_trows_or_sourcetable = tbname_or_trows_or_sourcetable
        self.custom_columns = custom_columns
        print(f"调试信息: tbname_or_trows_or_sourcetable = {tbname_or_trows_or_sourcetable}")
        print(f"调试信息: sql_type = {sql_type}")
        print(f"调试信息: agg_or_select = {agg_or_select}")
        print(f"调试信息:数据写入间隔: {real_time_batch_sleep}秒")
        print(f"调试信息: auto_combine = {auto_combine}")
        self.stream_sql = stream_sql if stream_sql else StreamSQLTemplates.get_sql(
            sql_type, 
            stream_num=stream_num, 
            auto_combine=auto_combine, 
            agg_or_select=agg_or_select,
            tbname_or_trows_or_sourcetable=tbname_or_trows_or_sourcetable,
            custom_columns=custom_columns
        )
        #print(f"生成的SQL:\n{self.stream_sql}")
        
        self.check_stream_delay = check_stream_delay
        self.max_delay_threshold = max_delay_threshold  # 毫秒
        self.delay_check_interval = delay_check_interval  # 秒
        
        # 延迟日志文件路径设置
        if delay_log_file:
            # 如果外部指定了延迟日志文件路径，使用指定的路径
            self.delay_log_file = delay_log_file
            print(f"调试: 使用外部指定的延迟日志文件: {self.delay_log_file}")
        else:
            # 如果没有指定，使用默认路径
            self.delay_log_file = f"{os.path.splitext(perf_file)[0]}-stream-delay.log"
            print(f"调试: 使用默认延迟日志文件: {self.delay_log_file}")
        
        print(f"流延迟检查: {'启用' if check_stream_delay else '禁用'}")
        if check_stream_delay:
            print(f"最大延迟阈值: {max_delay_threshold}ms")
            print(f"延迟检查间隔: {delay_check_interval}秒")
            print(f"延迟日志文件: {self.delay_log_file}")
        
        # 根据部署模式调整实例配置
        if self.deployment_mode == 'single':
            # 单节点模式只使用第一个实例
            self.instances = [
                {
                    'name': 'dnode1',
                    'host': 'localhost',
                    'port': 6030,
                    'user': 'root',
                    'passwd': 'taosdata',
                    'data_dir': f'{self.stream_perf_test_dir}/dnode1/data',
                    'log_dir': f'{self.stream_perf_test_dir}/dnode1/log',
                }
            ]
            
            # 单节点模式的数据库配置
            self.db_config = {
                'stream_from': {
                    'name': 'stream_from',
                    'vgroups': self.vgroups
                },
                'stream_to': {
                    'name': 'stream_to', 
                    'vgroups': self.vgroups
                }
            }
        else:
            # 定义3个实例的配置
            self.instances = [
                {
                    'name': 'dnode1',
                    'host': 'localhost',
                    'port': 6030,
                    'user': 'root',
                    'passwd': 'taosdata',
                    'data_dir': f'{self.stream_perf_test_dir}/dnode1/data',
                    'log_dir': f'{self.stream_perf_test_dir}/dnode1/log',
                },
                {
                    'name': 'dnode2',
                    'host': 'localhost',
                    'port': 7030,
                    'user': 'root',
                    'passwd': 'taosdata',
                    'data_dir': f'{self.stream_perf_test_dir}/dnode2/data',
                    'log_dir': f'{self.stream_perf_test_dir}/dnode2/log',
                },
                {
                    'name': 'dnode3',
                    'host': 'localhost',
                    'port': 8030,
                    'user': 'root',
                    'passwd': 'taosdata',
                    'data_dir': f'{self.stream_perf_test_dir}/dnode3/data/',
                    'log_dir': f'{self.stream_perf_test_dir}/dnode3/log',
                }
            ]
            
            self.db_config = {
                'stream_from': {
                    'name': 'stream_from',
                    'vgroups': self.vgroups,
                    'dnodes': '1'  # 默认在dnode1上
                },
                'stream_to': {
                    'name': 'stream_to', 
                    'vgroups': self.vgroups,
                    'dnodes': '2'  # 默认在dnode2上
                }
            }
            
        self.sql = None
        self.host = self.instances[0]['host']
        self.user = self.instances[0]['user']
        self.passwd = self.instances[0]['passwd']
        self.conf = f"{self.stream_perf_test_dir}/dnode1/conf/taos.cfg"
        self.tz = 'Asia/Shanghai'
        # 设置运行时间和性能文件路径
        self.runtime = runtime if runtime else 600  # 默认运行10分钟
        self.perf_file = perf_file if perf_file else '/tmp/perf.log'
        

    def get_connection(self):
        """获取数据库连接
        
        Returns:
            tuple: (connection, cursor)
        """
        try:
            conn = taos.connect(
                host=self.host,
                user=self.user,
                password=self.passwd,
                config=self.conf,
                timezone=self.tz
            )
            cursor = conn.cursor()
            return conn, cursor
        except Exception as e:
            print(f"数据库连接失败: {str(e)}")
            raise
        
    def stop_taosd(self):
        """停止所有taosd进程"""
        try:
            # 先尝试正常停止
            subprocess.run('pkill taosd', shell=True)
            time.sleep(10)
            
            # 检查是否还有进程存在
            result = subprocess.run('ps -ef | grep "taosd -c" | grep -v grep', 
                                shell=True, capture_output=True, text=True)
            if result.stdout:
                print("发现顽固进程，强制停止...")
                for line in result.stdout.splitlines():
                    try:
                        pid = int(line.split()[1])
                        subprocess.run(f'kill -9 {pid}', shell=True)
                        print(f"强制终止进程 PID: {pid}")
                    except:
                        continue
                
            print("所有taosd进程已停止")
            
        except Exception as e:
            print(f"停止进程出错: {str(e)}")
            raise

    def check_taosd_status(self):
        """检查taosd进程状态"""
        try:
            result = subprocess.run('ps -ef | grep "taosd -c" | grep -v grep', 
                                shell=True, capture_output=True, text=True)
            if result.stdout:
                print("\n当前运行的taosd进程:")
                for line in result.stdout.splitlines():
                    print(line)
                return True
            else:
                print("警告: 未发现运行中的taosd进程")
                return False
                
        except Exception as e:
            print(f"检查进程状态出错: {str(e)}")
            return False

        
    def create_database(self, db_name, vgroups=None, dnodes=None):
        """创建数据库
        
        Args:
            db_name: 数据库名称
            vgroups: vgroups数量,如果不指定则使用配置中的值
            dnodes: 指定数据库所在的dnode,如果不指定则使用配置中的值
        """
        try:
            # 获取数据库配置
            db_config = self.db_config.get(db_name, {})
            vgroups = vgroups or db_config.get('vgroups', self.vgroups)
            dnodes = dnodes or db_config.get('dnodes', '1')
            
            # 创建数据库
            conn, cursor = self.get_connection()
            create_db_sql = f"create database {db_name} vgroups {vgroups}"
            if dnodes:
                create_db_sql += f" dnodes '{dnodes}'"
                
            print(f"\n创建数据库: {create_db_sql}")
            cursor.execute(create_db_sql)
            
            # 关闭连接
            cursor.close()
            conn.close()
            
            print(f"数据库 {db_name} 创建成功")
            return True
            
        except Exception as e:
            print(f"创建数据库 {db_name} 失败: {str(e)}")
            return False
        
    def start_taosd_processes(self):
        """启动所有 taosd 进程 - 支持单节点和集群模式"""
        try:
            mode_desc = "单节点" if self.deployment_mode == 'single' else "集群"
            print(f"\n=== 开始启动 {mode_desc} taosd 进程 ===")
            
            # 根据部署模式确定要启动的节点
            nodes_to_start = []
            if self.deployment_mode == 'single':
                nodes_to_start = ['dnode1']
            else:
                nodes_to_start = ['dnode1', 'dnode2', 'dnode3']
            
            for dnode in nodes_to_start:
                cfg_file = os.path.join(self.stream_perf_test_dir, dnode, 'conf', 'taos.cfg')
                cmd = f'nohup taosd -c {cfg_file} > /dev/null 2>&1 &'
                print(f"执行启动命令: {cmd}")
                
                # 执行启动命令
                result = subprocess.run(cmd, shell=True)
                if result.returncode == 0:
                    print(f"已执行 {dnode} 的启动命令")
                else:
                    print(f"警告: {dnode} 启动命令执行失败")
                    
                # 验证进程是否启动
                time.sleep(2)
                check_cmd = f"pgrep -f 'taosd -c {cfg_file}'"
                if subprocess.run(check_cmd, shell=True, stdout=subprocess.PIPE).stdout:
                    print(f"{dnode} 进程已成功启动")
                else:
                    print(f"警告: {dnode} 进程可能未正常启动")
            
            # 等待服务完全启动
            wait_time = 3 if self.deployment_mode == 'single' else 10
            print(f"\n等待服务启动 ({wait_time}秒)...")
            time.sleep(wait_time)             
            self.check_taosd_status()
            
            # 配置集群（如果是集群模式）
            if self.deployment_mode == 'cluster':
                try:
                    # 连接到第一个节点
                    conn = taos.connect(
                        host=self.host,
                        user=self.user,
                        password=self.passwd,
                        config=self.conf
                    )
                    cursor = conn.cursor()
                    
                    print("\n=== 检查集群配置 ===")
                    
                    # 查询并显示节点状态
                    try:
                        # 查询 dnodes 信息
                        print("\nDNode 信息:")
                        cursor.execute("show dnodes")
                        result = cursor.fetchall()
                        for row in result:
                            print(f"ID: {row[0]}, endpoint: {row[1]}, status: {row[4]}")
                        
                        # 查询 mnodes 信息
                        print("\nMNode 信息:")
                        cursor.execute("show mnodes")
                        result = cursor.fetchall()
                        if result:
                            for row in result:
                                print(f"ID: {row[0]}, endpoint: {row[1]}, role: {row[2]}, status: {row[3]}")
                        else:
                            print("当前系统中没有额外的mnode")
                        
                        # 查询 snodes 信息
                        print("\nSNode 信息:")
                        cursor.execute("show snodes")
                        result = cursor.fetchall()
                        if result:
                            for row in result:
                                print(f"ID: {row[0]}, endpoint: {row[1]}, create_time: {row[2]}")
                        else:
                            print("当前系统中没有snode")
                            
                    except Exception as e:
                        print(f"查询节点信息失败: {str(e)}")
                    
                    # 关闭连接
                    cursor.close()
                    conn.close()
                    
                except Exception as e:
                    print(f"检查集群配置失败: {str(e)}")
                    
            print(f"\n{mode_desc}模式 taosd 进程启动完成")
            return True
            
        except Exception as e:
            print(f"启动 taosd 进程时出错: {str(e)}")
            return False
        return True
        
    def backup_cluster_data(self):
        """备份集群数据 - 支持单节点和集群模式"""
        try:
            mode_desc = "单节点" if self.deployment_mode == 'single' else "集群"
            print(f"开始备份{mode_desc}数据...")
            
            # 先停止所有 taosd 进程
            print("停止所有 taosd 进程...")
            subprocess.run('pkill -15 taosd', shell=True)
            time.sleep(5)  # 等待进程完全停止
            
            # 检查进程是否完全停止
            if subprocess.run('pgrep -x taosd', shell=True, stdout=subprocess.PIPE).stdout:
                print("等待 taosd 进程停止...")
                time.sleep(5)
                # 再次检查，如果还有进程则强制终止
                if subprocess.run('pgrep -x taosd', shell=True, stdout=subprocess.PIPE).stdout:
                    print("强制终止 taosd 进程...")
                    subprocess.run('pkill -9 taosd', shell=True)
                    time.sleep(2)
                    
            # 创建备份目录
            if os.path.exists(self.backup_dir):
                print(f"清理已存在的备份目录: {self.backup_dir}")
                shutil.rmtree(self.backup_dir)
            os.makedirs(self.backup_dir)
            print(f"创建备份目录: {self.backup_dir}")
            
            # 根据部署模式确定需要备份的节点
            nodes_to_backup = []
            if self.deployment_mode == 'single':
                nodes_to_backup = ['dnode1']
                print("单节点模式: 仅备份 dnode1 数据")
            else:
                nodes_to_backup = ['dnode1', 'dnode2', 'dnode3']
                print("集群模式: 备份 dnode1, dnode2, dnode3 数据")
                
            # 只备份data目录，不备份log和conf
            for dnode in nodes_to_backup:
                src_data_dir = os.path.join(self.stream_perf_test_dir, dnode, 'data')
                dst_data_dir = os.path.join(self.backup_dir, dnode, 'data')
                
                if os.path.exists(src_data_dir):
                    print(f"\n备份 {dnode}/data 目录...")
                    try:
                        # 确保目标目录存在
                        os.makedirs(os.path.dirname(dst_data_dir), exist_ok=True)
                        
                        # 使用 rsync 排除临时文件和socket文件
                        cmd = f'rsync -av --exclude="*.sock*" --exclude="*.tmp" --exclude="*.lock" {src_data_dir}/ {dst_data_dir}/'
                        result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
                        
                        if result.returncode == 0:
                            print(f"完成备份: {dst_data_dir}")
                            
                            # 显示备份数据的统计信息
                            try:
                                # 获取备份目录大小
                                du_result = subprocess.run(f'du -sh {dst_data_dir}', shell=True, 
                                                        capture_output=True, text=True)
                                if du_result.returncode == 0:
                                    size_info = du_result.stdout.strip().split()[0]
                                    print(f"  备份大小: {size_info}")
                                    
                                # 统计文件数量
                                find_result = subprocess.run(f'find {dst_data_dir} -type f | wc -l', 
                                                            shell=True, capture_output=True, text=True)
                                if find_result.returncode == 0:
                                    file_count = find_result.stdout.strip()
                                    print(f"  文件数量: {file_count}")
                            except Exception as e:
                                print(f"  统计备份信息时出错: {str(e)}")
                        else:
                            # rsync 失败，尝试使用 cp 命令
                            print(f"rsync 失败，使用 cp 命令备份...")
                            cmd = f'cp -r {src_data_dir} {dst_data_dir}'
                            subprocess.run(cmd, shell=True, check=True)
                            print(f"完成备份: {dst_data_dir}")
                            
                    except subprocess.CalledProcessError as e:
                        print(f"备份 {dnode}/data 失败: {str(e)}")
                        return False
                    except Exception as e:
                        print(f"备份 {dnode}/data 时出错: {str(e)}")
                        return False
                else:
                    print(f"警告: {dnode}/data 目录不存在，跳过备份")
            
            # 创建备份信息文件
            backup_info_file = os.path.join(self.backup_dir, 'backup_info.txt')
            try:
                with open(backup_info_file, 'w') as f:
                    f.write(f"TDengine 数据备份信息\n")
                    f.write(f"=" * 50 + "\n")
                    f.write(f"备份时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"部署模式: {self.deployment_mode}\n")
                    f.write(f"备份节点: {', '.join(nodes_to_backup)}\n")
                    f.write(f"备份内容: 仅数据目录 (data)\n")
                    f.write(f"子表数量: {self.table_count}\n")
                    f.write(f"每表记录数: {self.histroy_rows}\n")
                    f.write(f"数据乱序率: {self.disorder_ratio}\n")
                    f.write(f"vgroups数: {self.vgroups}\n")
                    f.write(f"=" * 50 + "\n")
                    
                    # 记录每个节点的备份状态
                    for dnode in nodes_to_backup:
                        dst_data_dir = os.path.join(self.backup_dir, dnode, 'data')
                        if os.path.exists(dst_data_dir):
                            f.write(f"{dnode}: 备份成功\n")
                        else:
                            f.write(f"{dnode}: 备份失败\n")
                            
                print(f"备份信息已保存到: {backup_info_file}")
            except Exception as e:
                print(f"创建备份信息文件失败: {str(e)}")
            
            print(f"\n=== {mode_desc}数据备份完成! ===")
            print(f"备份目录: {self.backup_dir}")
            return True
            
            # # 直接复制整个节点目录
            # for dnode in nodes_to_backup:
            #     src_dir = os.path.join(self.stream_perf_test_dir, dnode, 'data')
            #     dst_dir = os.path.join(self.backup_dir, dnode, 'data')
                
            #     print(f"\n备份 {dnode} 目录...")
            #     try:
            #         # 使用 rsync 排除 socket 文件
            #         cmd = f'rsync -av --exclude="*.sock*" {src_dir}/ {dst_dir}/'
            #         subprocess.run(cmd, shell=True, check=True)
            #         print(f"完成备份: {dst_dir}")
            #     except subprocess.CalledProcessError:
            #         # 如果 rsync 失败,使用 cp 命令
            #         print(f"rsync 失败,使用 cp 命令备份...")
            #         shutil.copytree(src_dir, dst_dir, symlinks=True, 
            #                     ignore=shutil.ignore_patterns('*.sock*'))
            #         print(f"完成备份: {dst_dir}")
            
            # print("\n=== 集群数据备份完成! ===")
            # print(f"备份目录: {self.backup_dir}")
            # return True
    
        except Exception as e:
            print(f"备份数据时出错: {str(e)}")
            return False
        finally:
            # 重新启动 taosd 进程
            if not self.start_taosd_processes():
                print("警告: taosd 进程启动失败")
        
    def restore_cluster_data(self):
        """从备份恢复集群数据 - 支持单节点和集群模式"""
        try:
            if not os.path.exists(self.backup_dir):
                raise Exception(f"错误: 备份目录不存在: {self.backup_dir}")
                
            # 读取备份信息
            backup_info_file = os.path.join(self.backup_dir, 'backup_info.txt')
            backup_mode = None
            if os.path.exists(backup_info_file):
                try:
                    with open(backup_info_file, 'r') as f:
                        content = f.read()
                        # 从备份信息中提取部署模式
                        for line in content.split('\n'):
                            if line.startswith('部署模式:'):
                                backup_mode = line.split(':')[1].strip()
                                break
                except Exception as e:
                    print(f"读取备份信息失败: {str(e)}")
            
            current_mode_desc = "单节点" if self.deployment_mode == 'single' else "集群"
            backup_mode_desc = "单节点" if backup_mode == 'single' else "集群" if backup_mode == 'cluster' else "未知"
            
            print(f"开始恢复数据...")
            print(f"当前模式: {current_mode_desc}")
            if backup_mode:
                print(f"备份模式: {backup_mode_desc}")
                if backup_mode != self.deployment_mode:
                    print(f"警告: 当前模式({self.deployment_mode})与备份模式({backup_mode})不匹配!")
                    response = input("是否继续恢复? (y/N): ")
                    if response.lower() != 'y':
                        print("恢复操作已取消")
                        return False
            
            # 停止现有taosd进程
            subprocess.run('pkill -15 taosd', shell=True)
            time.sleep(5)
            # 确保进程完全停止
            while subprocess.run('pgrep -x taosd', shell=True, stdout=subprocess.PIPE).stdout:
                print("等待 taosd 进程停止...")
                time.sleep(2)
                subprocess.run('pkill -9 taosd', shell=True)
                
            # 根据当前部署模式确定需要恢复的节点
            nodes_to_restore = []
            if self.deployment_mode == 'single':
                nodes_to_restore = ['dnode1']
                print("单节点模式: 仅恢复 dnode1 数据")
            else:
                nodes_to_restore = ['dnode1', 'dnode2', 'dnode3']
                print("集群模式: 恢复 dnode1, dnode2, dnode3 数据")
                
            # 恢复每个节点的数据
            restored_count = 0
            for dnode in nodes_to_restore:
                backup_data_dir = os.path.join(self.backup_dir, dnode, 'data')
                cluster_data_dir = os.path.join(self.stream_perf_test_dir, dnode, 'data')
                
                if not os.path.exists(backup_data_dir):
                    print(f"警告: 备份目录中未找到 {dnode}/data")
                    # 对于集群模式，如果某些节点的备份不存在，跳过但继续
                    if self.deployment_mode == 'cluster':
                        continue
                    else:
                        # 对于单节点模式，如果dnode1的备份不存在，则失败
                        if dnode == 'dnode1':
                            raise Exception(f"单节点模式下必须的 {dnode}/data 备份不存在")
                        continue
                        
                print(f"\n还原 {dnode}/data ...")
                
                # 显示备份目录信息
                try:
                    du_result = subprocess.run(f'du -sh {backup_data_dir}', shell=True, 
                                            capture_output=True, text=True)
                    if du_result.returncode == 0:
                        backup_size = du_result.stdout.strip().split()[0]
                        print(f"备份数据大小: {backup_size}")
                except:
                    pass
                
                # 清理现有数据目录
                if os.path.exists(cluster_data_dir):
                    print(f"清理现有数据目录: {cluster_data_dir}")
                    shutil.rmtree(cluster_data_dir)
                
                # 确保父目录存在
                os.makedirs(os.path.dirname(cluster_data_dir), exist_ok=True)
                
                # 恢复数据目录
                try:
                    shutil.copytree(backup_data_dir, cluster_data_dir, symlinks=True)
                    print(f"完成还原: {cluster_data_dir}")
                    restored_count += 1
                    
                    # 显示恢复后的信息
                    try:
                        du_result = subprocess.run(f'du -sh {cluster_data_dir}', shell=True, 
                                                capture_output=True, text=True)
                        if du_result.returncode == 0:
                            restored_size = du_result.stdout.strip().split()[0]
                            print(f"恢复数据大小: {restored_size}")
                    except:
                        pass
                        
                except Exception as e:
                    print(f"还原 {dnode}/data 时出错: {str(e)}")
                    return False
            
            if restored_count == 0:
                print("错误: 没有成功恢复任何节点的数据")
                return False
                
            print(f"\n=== 数据还原完成! ===")
            print(f"成功恢复 {restored_count} 个节点的数据")
            
            # 重启 taosd 进程
            return self.start_taosd_processes()
            
        except Exception as e:
            print(f"\n还原数据时出错: {str(e)}")
            return False
        
    def do_test_stream_with_restored_data(self):
        """恢复数据后测试指定的流计算SQL"""
        
        loader = None
        monitor_thread = None
        delay_monitor_thread = None
        conn = None
        cursor = None
    
        try:
            print("=== 恢复数据后测试流计算 ===")
            
            # 先恢复数据
            print("1. 恢复历史数据...")
            if not self.restore_cluster_data():
                raise Exception("恢复集群数据失败")
            
            print("2. 创建目标数据库...")
            # 创建stream_to数据库
            if not self.create_database('stream_to'):
                raise Exception("创建stream_to数据库失败")
            
            print("3. 开始流计算测试...")
            print(f"SQL类型: {self.sql_type}")
            print(f"流数量: {self.stream_num}")
            print(f"自定义SQL: {'是' if self.stream_sql else '否'}")
            
            # 获取数据库连接
            conn, cursor = self.get_connection()
            
            
            # 连接到源数据库
            print("连接到源数据库 stream_from...")
            cursor.execute('use stream_from')
            
            # 检查源数据状态
            print("检查源数据状态...")
            cursor.execute("select count(*) from information_schema.ins_tables where db_name='stream_from'")
            table_count = cursor.fetchall()[0][0]
            
            cursor.execute("select count(*) from stream_from.stb")
            record_count = cursor.fetchall()[0][0]
            
            print(f"源数据统计: {table_count} 张表, {record_count:,} 条记录")
            
            # 获取 SQL 模板并创建流
            sql_templates = self.stream_sql 
            
            # 判断是否为批量执行
            if isinstance(sql_templates, dict):
                print("\n=== 开始批量创建流 ===")
                total_streams = len(sql_templates)
                success_count = 0
                failed_count = 0
                
                for index, (sql_name, sql_template) in enumerate(sql_templates.items(), 1):
                    try:
                        print(f"\n[{index}/{total_streams}] 创建流: {sql_name}")
                        
                        # 提取实际的流名称（从SQL中解析）
                        import re
                        # 匹配 create stream 后面的流名称
                        match = re.search(r'create\s+stream\s+([^\s]+)', sql_template, re.IGNORECASE)
                        actual_stream_name = match.group(1) if match else sql_name
                        
                        print(f"  SQL流名称: {actual_stream_name}")
                        # 显示完整的流SQL语句
                        print(f"  流SQL语句:")
                        # 格式化SQL显示，去掉多余的空行和缩进
                        formatted_sql = '\n'.join([
                            '    ' + line.strip() 
                            for line in sql_template.strip().split('\n') 
                            if line.strip()
                        ])
                        print(formatted_sql)
                        
                        print(f"  执行创建...")
                        cursor.execute(sql_template)
                        success_count += 1
                        print(f"  ✓ 创建成功")
                        
                    except Exception as e:
                        failed_count += 1
                    
                        # 提取TDengine的具体错误信息
                        error_message = str(e)
                        tdengine_error = extract_stream_creation_error(error_message)
                        
                        print(f"执行错误: {tdengine_error}")
                
                # 显示批量创建结果摘要
                print(f"\n=== 批量创建结果摘要 ===")
                print(f"总流数: {total_streams}")
                print(f"成功: {success_count}")
                print(f"失败: {failed_count}")
                print(f"成功率: {(success_count/total_streams*100):.1f}%")
                
                if failed_count > 0:
                    print(f"⚠️  {failed_count} 个流创建失败，请检查错误信息")
                else:
                    print("所有流创建成功！")
                    
            else:
                # 单个流的创建
                print("\n=== 开始创建流 ===")
                
                # 提取实际的流名称
                import re
                match = re.search(r'create\s+stream\s+([^\s]+)', sql_templates, re.IGNORECASE)
                actual_stream_name = match.group(1) if match else "未知流名称"
                
                print(f"流名称: {actual_stream_name}")
                print("执行流计算SQL:")
                print("-" * 60)
                print(sql_templates)
                print("-" * 60)
                
                try:
                    start_time = time.time()
                    cursor.execute(sql_templates)
                    create_time = time.time() - start_time
                    
                    print(f"✓ 流 {actual_stream_name} 创建完成! 耗时: {create_time:.2f}秒")
                    
                except Exception as e:
                    # 提取TDengine的具体错误信息
                    error_message = str(e)
                    tdengine_error = extract_stream_creation_error(error_message)
                    
                    print(f"执行错误: {tdengine_error}")
                    raise
            
            # 启动系统监控
            print("\n开始监控系统资源使用情况...")
        
            # 计算预热时间
            if hasattr(self, 'monitor_warm_up_time') and self.monitor_warm_up_time is not None:
                warm_up_time = self.monitor_warm_up_time
                print(f"使用用户设置的预热时间: {warm_up_time}秒")
            else:
                warm_up_time = 60 if self.runtime >= 2 else 0
                if warm_up_time > 0:
                    print(f"使用自动计算的预热时间: {warm_up_time}秒")
                    
            loader = MonitorSystemLoad(
                name_pattern='taosd -c', 
                count=self.runtime * 60,
                perf_file='/tmp/perf-stream-test.log',
                interval=self.monitor_interval,
                deployment_mode=self.deployment_mode,
                warm_up_time=warm_up_time
            )
            
            # 在新线程中运行监控
            monitor_thread = threading.Thread(
                target=loader.get_proc_status,
                name="StreamTestMonitor"
            )
            monitor_thread.daemon = True
            monitor_thread.start()
            
            # 启动流延迟监控
            delay_monitor_thread = self.start_stream_delay_monitor()
            
            # 等待流计算完成
            print(f"等待流计算完成... (监控时间: {self.runtime}分钟)")
            
            # 定期检查流计算进度
            check_interval = 30  # 每30秒检查一次
            total_checks = (self.runtime * 60) // check_interval
            
            for i in range(total_checks):
                time.sleep(check_interval)
                
                try:
                    # 检查目标表数据量
                    cursor.execute("use stream_to")
                    
                    # 查询目标表数量和记录数
                    cursor.execute("select count(*) from information_schema.ins_tables where db_name='stream_to'")
                    target_table_count = cursor.fetchall()[0][0]
                    
                    if target_table_count > 0:
                        # 检查是否有数据写入
                        cursor.execute("show tables")
                        tables = cursor.fetchall()
                        
                        total_target_records = 0
                        for table in tables:
                            table_name = table[0]
                            try:
                                cursor.execute(f"select count(*) from stream_to.{table_name}")
                                count = cursor.fetchall()[0][0]
                                total_target_records += count
                            except:
                                continue
                        
                        progress = (i + 1) / total_checks * 100
                        print(f"进度: {progress:.1f}% - 目标表: {target_table_count}, 目标记录: {total_target_records:,}")
                    else:
                        progress = (i + 1) / total_checks * 100
                        print(f"进度: {progress:.1f}% - 等待流计算开始...")
                        
                    cursor.execute('use stream_from')  # 切换回源数据库
                    
                except Exception as e:
                    print(f"检查进度时出错: {str(e)}")
                    
                # 检查是否达到运行时间限制
                if i >= total_checks - 1:
                    print(f"\n已达到运行时间限制 ({self.runtime} 分钟)")
                    break
            
            print("\n流计算测试完成!")
            
            # 显示最终结果统计
            print("\n=== 流计算结果统计 ===")
            try:
                cursor.execute("use stream_to")
                cursor.execute("select count(*) from information_schema.ins_tables where db_name='stream_to'")
                final_table_count = cursor.fetchall()[0][0]
                
                cursor.execute("show tables")
                tables = cursor.fetchall()
                
                final_total_records = 0
                table_details = []
                for table in tables:
                    table_name = table[0]
                    try:
                        cursor.execute(f"select count(*) from stream_to.{table_name}")
                        count = cursor.fetchall()[0][0]
                        final_total_records += count
                        table_details.append(f"表 {table_name}: {count:,} 条记录")
                    except Exception as e:
                        table_details.append(f"表 {table_name}: 查询失败 - {str(e)}")
                
                print(f"总计: {final_table_count} 张目标表, {final_total_records:,} 条结果记录")
                
                # 显示前几张表的详情
                for detail in table_details[:5]:
                    print(f"  {detail}")
                if len(table_details) > 5:
                    print(f"  ... 还有 {len(table_details) - 5} 张表")
                
                # 计算处理效率
                if record_count > 0:
                    processing_ratio = final_total_records / record_count * 100
                    print(f"处理效率: {processing_ratio:.2f}% ({final_total_records:,}/{record_count:,})")
                
            except Exception as e:
                print(f"统计结果时出错: {str(e)}")
                
            print("\n=== 流计算测试完成 ===")
            print("性能监控数据已保存到: /tmp/perf-stream-test-*.log")
            print("可以使用以下命令查看监控数据:")
            print("cat /tmp/perf-stream-test-all.log")
            
        except Exception as e:
            print(f"流计算测试失败: {str(e)}")
            raise
        finally:
            # 安全地关闭数据库连接
            try:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
            except Exception as e:
                print(f"关闭数据库连接时出错: {str(e)}")
            
            # 安全地停止监控
            try:
                if loader:
                    print("主动停止系统监控...")
                    loader.stop()
            except Exception as e:
                print(f"停止系统监控时出错: {str(e)}")
            
            # 安全地等待监控线程结束
            try:
                if monitor_thread:
                    print("等待监控数据收集完成...")
                    monitor_thread.join(timeout=15)
                    
                    if monitor_thread.is_alive():
                        print("监控线程未在预期时间内结束，强制继续...")
                    else:
                        print("监控线程已正常结束")
            except Exception as e:
                print(f"等待监控线程时出错: {str(e)}")
                
            # 安全地等待延迟监控线程结束
            try:
                if delay_monitor_thread:
                    print("等待延迟监控完成...")
                    delay_monitor_thread.join(timeout=10)
                    if delay_monitor_thread.is_alive():
                        print("延迟监控线程未在预期时间内结束，强制继续...")
                    else:
                        print("延迟监控线程已正常结束")
            except Exception as e:
                print(f"等待延迟监控线程时出错: {str(e)}")
            
            # 打印最终报告
            try:
                if self.check_stream_delay:
                    print(f"\n流延迟监控报告已保存到: {self.delay_log_file}")
                    self.print_final_delay_summary()
            except Exception as e:
                print(f"打印最终报告时出错: {str(e)}")

        
    def create_test_data(self):
        """创建并备份测试数据"""
        try:
            print("开始生成测试数据...")
            self.prepare_env()
            self.prepare_source_from_data()
            # 运行数据生成
            proc = subprocess.Popen('taosBenchmark --f /tmp/stream_from.json',
                                    stdout=subprocess.PIPE, shell=True, text=True)
            
            # 等待数据写入完成
            conn = taos.connect(host=self.host, user=self.user, 
                                password=self.passwd, config=self.conf)
            cursor = conn.cursor()
            
            if not self.wait_for_data_ready(cursor,self.table_count, self.histroy_rows):
                print("数据生成失败")
                return False
                
            # 备份数据
            if self.backup_cluster_data():
                print("测试数据已生成并备份")
                return True
                
            print("测试数据创建完成")
            return True
            
        except Exception as e:
            print(f"创建测试数据时出错: {str(e)}")
            return False
        
    def prepare_env(self):
        """
        清理环境并启动TDengine服务
        支持单节点(single)和集群(cluster)两种模式
        """
        try:
            print_title(f"\n=== 开始准备环境 (模式: {self.deployment_mode}) ===")
            # 停止已存在的taosd进程
            print_info("停止现有taosd进程")
            self.stop_taosd()
            
            # 检查并处理集群根目录
            if os.path.exists(self.stream_perf_test_dir):
                print_info(f"清理已存在的集群目录: {self.stream_perf_test_dir}")
                subprocess.run(f'rm -rf {self.stream_perf_test_dir}/*', shell=True)
            else:
                print_info(f"创建新的集群目录: {self.stream_perf_test_dir}")
                subprocess.run(f'mkdir -p {self.stream_perf_test_dir}', shell=True)
            
                
            for instance in self.instances:
                # 创建必要的目录
                for dir_type in ['data', 'log', 'conf']:
                    dir_path = f"{self.stream_perf_test_dir}/{instance['name']}/{dir_type}"
                    subprocess.run(f'mkdir -p {dir_path}', shell=True)
                
                # 清理数据目录
                data_dir = f"{self.stream_perf_test_dir}/{instance['name']}/data"
                subprocess.run(f'rm -rf {data_dir}', shell=True)
                print(f"创建目录: {dir_path}")
                
                # 根据部署模式生成不同的配置文件
                if self.deployment_mode == 'single':
                    # 单节点配置生成配置文件
                    cfg_content = f"""
firstEP         localhost:6030
fqdn            localhost
serverPort      {instance['port']}
supportVnodes   100
dataDir         {instance['data_dir']}
logDir          {instance['log_dir']}
asyncLog        1
debugFlag       {self.debug_flag}
numOfLogLines   {self.num_of_log_lines}
"""
                else:
                    # 集群配置生成配置文件
                    cfg_content = f"""
firstEP         localhost:6030
secondEP        localhost:7030
fqdn            localhost
serverPort      {instance['port']}
supportVnodes   50
dataDir         {instance['data_dir']}
logDir          {instance['log_dir']}
asyncLog        1
debugFlag       {self.debug_flag}
numOfLogLines   {self.num_of_log_lines}
"""
                cfg_file = f"{self.stream_perf_test_dir}/{instance['name']}/conf/taos.cfg"
                
                # 使用 EOF 方式写入配置文件
                subprocess.run(f"""
cat << 'EOF' > {cfg_file}
{cfg_content}
EOF
""", shell=True)
            print_success("环境准备完成，配置文件已生成")
            
            
            # 启动所有taosd实例
            self.taosd_processes = []  
            for instance in self.instances:
                cfg_file = f"{self.stream_perf_test_dir}/{instance['name']}/conf/taos.cfg"
                cmd = f'nohup taosd -c {cfg_file} > /dev/null 2>&1 &'
                
                try:
                    process = subprocess.Popen(cmd, shell=True)
                    self.taosd_processes.append({
                        'name': instance,
                        'pid': process.pid,
                        'cfg': cfg_file
                    })
                    print(f"启动taosd进程: {instance}, PID: {process.pid}")
                except Exception as e:
                    print(f"启动 {instance} 失败: {str(e)}")
                    raise
            
            # 等待服务完全启动
            wait_time = 3 if self.deployment_mode == 'single' else 10
            print(f"等待服务启动 ({wait_time}秒)...")
            time.sleep(wait_time)             
            self.check_taosd_status()
            
            # 配置集群
            try:
                # 连接到第一个节点
                conn = taos.connect(
                    host=self.host,
                    user=self.user,
                    password=self.passwd,
                    config=self.conf
                )
                cursor = conn.cursor()
                
                if self.deployment_mode == 'cluster':
                    # 集群模式配置
                    print("\n=== 开始配置集群 ===")
                    cluster_cmds = [
                        'create dnode "localhost:7030"',
                        'create dnode "localhost:8030"',
                        'create mnode on dnode 2',
                        'create mnode on dnode 3',
                        'create snode on dnode 3',
                        
                        'create snode on dnode 2',
                        'create snode on dnode 1'
                    ]
                
                    for cmd in cluster_cmds:
                        try:
                            cursor.execute(cmd)
                            time.sleep(1)
                            print(f"执行成功: {cmd}")
                        except Exception as e:
                            print(f"执行失败: {cmd}")
                            print(f"错误信息: {str(e)}")
                
                    # 查询并显示集群状态
                    print("\n集群节点信息:")
                    print("-" * 50)
                    
                else:
                    # 单节点模式配置
                    print("\n=== 配置单节点环境 ===")
                    try:
                        cursor.execute('create snode on dnode 1')
                        print("执行成功: create snode on dnode 1")
                        time.sleep(1)
                    except Exception as e:
                        print(f"创建snode失败: {str(e)}")
                
                # 查询并显示节点状态（单节点和集群都显示）
                print(f"\n=== {self.deployment_mode.upper()}模式节点信息 ===")
                print("-" * 60)
                
                try:
                    # 查询 dnodes 信息
                    print("\nDNode 信息:")
                    cursor.execute("show dnodes")
                    result = cursor.fetchall()
                    for row in result:
                        print(f"ID: {row[0]}, endpoint: {row[1]}, status: {row[4]}")
                    
                    # 查询 mnodes 信息
                    print("\nMNode 信息:")
                    cursor.execute("show mnodes")
                    result = cursor.fetchall()
                    if result:
                        for row in result:
                            print(f"ID: {row[0]}, endpoint: {row[1]}, role: {row[2]}, status: {row[3]}")
                    else:
                        print("当前系统中没有额外的mnode")
                    
                    # 查询 snodes 信息
                    print("\nSNode 信息:")
                    cursor.execute("show snodes")
                    result = cursor.fetchall()
                    if result:
                        for row in result:
                            print(f"ID: {row[0]}, endpoint: {row[1]}, create_time: {row[2]}")
                    else:
                        print("当前系统中没有snode")
                        
                except Exception as e:
                    print(f"查询节点信息失败: {str(e)}")
                
                print("-" * 50)
                
                # 关闭连接
                cursor.close()
                conn.close()
                
                print(f"{self.deployment_mode.upper()}模式配置完成")
                
            except Exception as e:
                print_error(f"节点配置失败: {str(e)}")
                raise
                
        except Exception as e:
            print_error(f"环境准备失败: {str(e)}")
            raise
        
    def prepare_source_from_data(self) -> dict:
        json_data = {
            "filetype": "insert",
            "cfgdir": f"{self.stream_perf_test_dir}/dnode1/conf",
            "host": "localhost",
            "port": 6030,
            "rest_port": 6041,
            "user": "root",
            "password": "taosdata",
            "thread_count": 50,
            "create_table_thread_count": 5,
            "result_file": "/tmp/taosBenchmark_result.log",
            "confirm_parameter_prompt": "no",
            "insert_interval": 10,
            "num_of_records_per_req": 1000,
            "max_sql_len": 102400,
            "databases": [
                {
                    "dbinfo": {
                        "name": "stream_from",
                        "drop": "yes",
                        "replica": 1,
                        "duration": 10,
                        "precision": "ms",
                        "keep": 3650,
                        "minRows": 100,
                        "maxRows": 4096,
                        "comp": 2,
                        "dnodes": "1",
                        "vgroups": self.vgroups,
                        "stt_trigger": 2,
                        "WAL_RETENTION_PERIOD": 86400
                    },
                    "super_tables": [
                        {
                            "name": "stb",
                            "child_table_exists": "yes",
                            "childtable_count": self.table_count,
                            "childtable_prefix": "ctb0_",
                            "escape_character": "no",
                            "auto_create_table": "yes",
                            "batch_create_tbl_num": 1000,
                            "data_source": "rand",
                            "insert_mode": "taosc",
                            "interlace_rows": 1,
                            "tcp_transfer": "no",
                            "insert_rows": self.histroy_rows,
                            "partial_col_num": 0,
                            "childtable_limit": 0,
                            "childtable_offset": 0,
                            "rows_per_tbl": 0,
                            "max_sql_len": 1024000,
                            "disorder_ratio": self.disorder_ratio,
                            "disorder_range": 1000,
                            "keep_trying": -1,
                            "timestamp_step": 50,
                            "trying_interval": 10,
                            "start_timestamp": "2025-06-01 00:00:00",
                            "sample_format": "csv",
                            "sample_file": "./sample.csv",
                            "tags_file": "",
                            "columns": [
                                {
                                    "type": "INT",
                                    "count": 1
                                },
                                {
                                    "type": "BIGINT",
                                    "count": 1
                                },
                                {
                                    "type": "DOUBLE",
                                    "count": 1
                                },
                                {
                                    "type": "FLOAT",
                                    "count": 1
                                }
                            ],
                            "tags": [
                                {
                                    "type": "INT",
                                    "count": 1
                                },
                                {
                                    "type": "VARCHAR",
                                    "count": 1,
                                    "len": 32
                                }
                            ]
                        }
                    ]
                }
            ],
            "prepare_rand": 10000,
            "chinese": "no",
            "test_log": "/tmp/testlog/"
        }

        with open('/tmp/stream_from.json', 'w+') as f:
            json.dump(json_data, f, indent=4)
            

    def insert_source_from_data(self) -> dict:
        json_data = {
            "filetype": "insert",
            "cfgdir": f"{self.stream_perf_test_dir}/dnode1/conf",
            "host": "localhost",
            "port": 6030,
            "rest_port": 6041,
            "user": "root",
            "password": "taosdata",
            "thread_count": 50,
            "create_table_thread_count": 5,
            "result_file": "/tmp/taosBenchmark_result.log",
            "confirm_parameter_prompt": "no",
            "insert_interval": 1,
            "num_of_records_per_req": 1000,
            "max_sql_len": 102400,
            "databases": [
                {
                    "dbinfo": {
                        "name": "stream_from",
                        "drop": "no",
                        "replica": 1,
                        "duration": 10,
                        "precision": "ms",
                        "keep": 3650,
                        "minRows": 100,
                        "maxRows": 4096,
                        "comp": 2,
                        "dnodes": "1",
                        "vgroups": self.vgroups,
                        "stt_trigger": 2,
                        "WAL_RETENTION_PERIOD": 86400
                    },
                    "super_tables": [
                        {
                            "name": "stb",
                            "child_table_exists": "yes",
                            "childtable_count": self.table_count,
                            "childtable_prefix": "ctb0_",
                            "escape_character": "no",
                            "auto_create_table": "yes",
                            "batch_create_tbl_num": 1000,
                            "data_source": "rand",
                            "insert_mode": "taosc",
                            "interlace_rows": 1,
                            "tcp_transfer": "no",
                            "insert_rows": self.real_time_batch_rows,
                            "partial_col_num": 0,
                            "childtable_limit": 0,
                            "childtable_offset": 0,
                            "rows_per_tbl": 0,
                            "max_sql_len": 1024000,
                            "disorder_ratio": self.disorder_ratio,
                            "disorder_range": 1000,
                            "keep_trying": -1,
                            "timestamp_step": 50,
                            "trying_interval": 10,
                            "start_timestamp": "2025-06-01 00:00:00",
                            "sample_format": "csv",
                            "sample_file": "./sample.csv",
                            "tags_file": "",
                            "columns": [
                                {
                                    "type": "INT",
                                    "count": 1
                                },
                                {
                                    "type": "BIGINT",
                                    "count": 1
                                },
                                {
                                    "type": "DOUBLE",
                                    "count": 1
                                },
                                {
                                    "type": "FLOAT",
                                    "count": 1
                                }
                            ],
                            "tags": [
                                {
                                    "type": "INT",
                                    "count": 1
                                },
                                {
                                    "type": "VARCHAR",
                                    "count": 1,
                                    "len": 32
                                }
                            ]
                        }
                    ]
                }
            ],
            "prepare_rand": 10000,
            "chinese": "no",
            "test_log": "/tmp/testlog/"
        }

        with open('/tmp/stream_from_insertdata.json', 'w+') as f:
            json.dump(json_data, f, indent=4)
            
    def update_insert_config(self):
        """
        更新数据插入配置
        Args:
            real_time_batch_rows: 下一轮要插入的数据行数
        """
        try:
            print("\n=== 更新数据插入配置 ===")
            #print(f"当前SQL类型: {self.sql_type}")
            
            # 获取最新时间戳
            conn = taos.connect(
                host=self.host,
                user=self.user,
                password=self.passwd,
                config=self.conf
            )
            cursor = conn.cursor()
            
            try:
                # 根据 SQL 类型决定时间戳更新方式
                # 检查是否为 period 类型的流计算
                is_period_type = (
                    self.sql_type.startswith('period_') or 
                    'period_' in self.sql_type or
                    self.sql_type == 's2_5' or 
                    self.sql_type == 's2_11' or
                    (isinstance(self.stream_sql, dict) and 
                    any('period_' in key for key in self.stream_sql.keys())) or
                    (isinstance(self.stream_sql, str) and 
                    'period(' in self.stream_sql.lower())
                )
                
                if is_period_type:
                    next_start_time = time.strftime("%Y-%m-%d %H:%M:%S")
                    print(f"检测到 PERIOD 类型流计算，使用当前时间作为起始时间: {next_start_time}")
                    print(f"PERIOD 类型说明: 使用 cast(_tprev_localtime/1000000 as timestamp) 和 cast(_tnext_localtime/1000000 as timestamp) 作为时间范围变量")
                
                else:
                    # 查询最新时间戳
                    cursor.execute("select last(ts) from stream_from.stb")
                    last_ts = cursor.fetchall()[0][0]
                
                    if not last_ts:
                        raise Exception("未能获取到最新时间戳")
                        
                    # 将时间戳转换为字符串格式，并加上1秒
                    next_start_time = (last_ts + datetime.timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
                    print(f"当前最新时间戳: {last_ts}")
                    print(f"更新起始时间为: {next_start_time}")
                
                # 读取现有配置
                config_file = '/tmp/stream_from_insertdata.json'
                with open(config_file, 'r') as f:
                    content = f.read()
                
                # 使用正则表达式替换时间戳
                import re
                new_content = re.sub(
                    r'"start_timestamp":\s*"[^"]*"',
                    f'"start_timestamp": "{next_start_time}"',
                    content,
                    count=1  # 只替换第一次出现的时间戳
                )
                
                # 格式化写入以确保 JSON 格式正确
                with open(config_file, 'w') as f:
                    f.write(new_content)
                
                print("配置时间戳已更新")
                return True
                
            except Exception as e:
                print(f"更新配置时出错: {str(e)}")
                return False
            finally:
                cursor.close()
                conn.close()
                
        except Exception as e:
            print(f"执行更新配置时出错: {str(e)}")
            return False

    def wait_for_data_ready(self, cursor, expected_tables, expected_records):
        """等待数据写入完成   
        Args:
            cursor: 数据库游标
            expected_tables: 预期的子表数量
            expected_records: 每个子表的记录数
            
        Returns:
            bool: 数据是否准备就绪
        """
        max_wait_time = 3600  # 最大等待时间(秒)
        check_interval = 2   # 检查间隔(秒)
        start_time = time.time()
        
        while True:
            try:
                # 检查子表数量
                cursor.execute("select count(*) from information_schema.ins_tables "
                            "where db_name='stream_from'")
                table_count = cursor.fetchall()[0][0]
                
                if table_count < expected_tables:
                    print(f"\r等待子表创建完成... 当前: {table_count}/{expected_tables}", end='')
                    if time.time() - start_time > max_wait_time:
                        print(f"\n等待超时! 子表数量不足: {table_count}/{expected_tables}")
                        return False
                    time.sleep(check_interval)
                    continue
                
                # 使用超级表查询总记录数
                cursor.execute("select count(*) from stream_from.stb")
                total_records = cursor.fetchall()[0][0]
                expected_total = expected_tables * expected_records
                
                if total_records < expected_total:
                    print(f"\r等待数据写入完成... 当前: {total_records}/{expected_total}", end='')
                    
                    # 如果接近超时，检查每个子表的记录数
                    if time.time() - start_time > max_wait_time - 30:  # 留出30秒用于详细检查
                        print("\n即将超时，检查各子表数据情况:")
                        insufficient_tables = []
                        
                        for i in range(expected_tables):
                            cursor.execute(f"select count(*) from stream_from.ctb0_{i}")
                            count = cursor.fetchall()[0][0]
                            if count < expected_records:
                                insufficient_tables.append({
                                    'table': f'ctb0_{i}',
                                    'current': count,
                                    'expected': expected_records,
                                    'missing': expected_records - count
                                })
                        
                        if insufficient_tables:
                            print("\n以下子表数据不足:")
                            for table in insufficient_tables:
                                print(f"表 {table['table']}: "
                                    f"当前 {table['current']}/{table['expected']}, "
                                    f"缺少 {table['missing']} 条记录")
                        return False
                    
                    time.sleep(check_interval)
                    continue
                
                print(f"\n数据准备就绪! 共 {table_count} 张子表，{total_records} 条记录")
                return True
                
            except Exception as e:
                print(f"\n检查数据时出错: {str(e)}")
                if time.time() - start_time > max_wait_time:
                    print("等待超时!")
                    return False
                time.sleep(check_interval)
                
    def _parse_target_table_from_sql(self, sql):
        """从流SQL中解析目标表名
        
        Args:
            sql: 流的SQL语句
            
        Returns:
            str: 目标表名，如果解析失败则返回None
            
        Example SQL:
            create stream qdb.s18  interval(58s) intervalsliding(11m, 4m)  
                    from qdb.v1   stream_options(watermark(43m)) 
                    into qdb.st18 
                    as select _twstart ts, c2 as c2_val, c1 as c1_val 
                    from stream_trigger;
            
            应该返回: qdb.st18
        """
        try:
            import re
            
            # 清理SQL：移除多余的空白字符和换行符
            cleaned_sql = ' '.join(sql.split())
            
            # 使用正则表达式匹配 into 关键字后面的表名
            # 模式说明:
            # \binto\s+ : 匹配单词边界的into，后跟一个或多个空白字符
            # ([^\s]+)  : 捕获一个或多个非空白字符（表名）
            # \s+as\s+  : 匹配空白字符 + as + 空白字符
            pattern = r'\binto\s+([^\s]+)\s+as\s+'
            
            match = re.search(pattern, cleaned_sql, re.IGNORECASE)
            
            if match:
                target_table = match.group(1)
                # 移除可能的分号或其他标点符号
                target_table = target_table.rstrip(';').rstrip(',')
                #print(f"从SQL中解析出目标表: {target_table}")
                return target_table
            else:
                # 如果第一个模式没匹配到，尝试更宽松的模式
                # 有些SQL可能格式不同，尝试只匹配into后面的第一个标识符
                pattern2 = r'\binto\s+([^\s\n]+)'
                match2 = re.search(pattern2, cleaned_sql, re.IGNORECASE)
                
                if match2:
                    target_table = match2.group(1)
                    target_table = target_table.rstrip(';').rstrip(',')
                    print(f"使用备用模式解析出目标表: {target_table}")
                    return target_table
                else:
                    print(f"无法从SQL中解析目标表: {cleaned_sql[:200]}...")
                    return None
                    
        except Exception as e:
            print(f"解析SQL时出错: {str(e)}")
            return None
                
    def get_stream_target_tables(self):
        """获取流计算的目标表列表
        
        Returns:
            list: 目标表名列表
        """
        try:
            conn, cursor = self.get_connection()
            
            # 查询所有流信息
            cursor.execute("select stream_name, sql from information_schema.ins_streams")
            streams = cursor.fetchall()
            print(f"从 information_schema.ins_streams 查询到 {len(streams)} 个流")
        
            
            target_tables = []
            for stream in streams:
                stream_name = stream[0]
                sql = stream[1]
            
                # 使用正则表达式解析SQL中的目标表
                target_table = self._parse_target_table_from_sql(sql)
                
                if target_table:
                    target_tables.append({
                        'stream_name': stream_name,
                        'target_table': target_table,
                        'target_db': target_table.split('.')[0] if '.' in target_table else 'stream_to',
                        'target_table_name': target_table.split('.')[-1],
                        'sql': sql  # 保存原始SQL用于调试
                    })
                    #print(f"  流: {stream_name} -> 目标表: {target_table}")
                else:
                    print(f"  警告: 无法解析流 {stream_name} 的目标表，SQL: {sql[:100]}...")
            
            cursor.close()
            conn.close()
            
            if len(target_tables) == 0:
                print("警告: 未找到任何流或无法解析目标表")
            else:
                print(f"成功解析 {len(target_tables)} 个流的目标表")
                
            return target_tables
            
        except Exception as e:
            print(f"获取流目标表失败: {str(e)}")
            return []

    def check_stream_computation_delay(self):
        """检查流计算延迟
        
        Returns:
            dict: 延迟检查结果
        """
        try:
            conn, cursor = self.get_connection()
            
            # 检查数据库是否存在
            try:
                cursor.execute("show databases")
                databases = cursor.fetchall()
                db_names = [db[0] for db in databases]
                #print(f"发现数据库: {db_names}")
                
                if 'stream_from' not in db_names:
                    print("错误: 源数据库 stream_from 不存在")
                    cursor.close()
                    conn.close()
                    return None
            except Exception as e:
                print(f"查询数据库列表失败: {str(e)}")
                cursor.close()
                conn.close()
                return None
        
            # 获取源表最新时间戳
            try:
                cursor.execute("select last(ts) from stream_from.stb")
                source_result = cursor.fetchall()
                #print(f"源表查询结果: {source_result}")
            except Exception as e:
                print(f"查询源表失败: {str(e)}")
                cursor.close()
                conn.close()
                return None
            
            if not source_result or not source_result[0][0]:
                print("警告: 无法获取源表最新时间戳")
                cursor.close()
                conn.close()
                return None
                
            source_last_ts = source_result[0][0]
            source_ts_ms = int(source_last_ts.timestamp() * 1000)
            print(f"源表最新时间戳: {source_last_ts} ({source_ts_ms}ms)")
            
            # 检查是否有流存在
            print("查询流的详细信息...")
            try:
                cursor.execute("select stream_name, sql from information_schema.ins_streams")
                streams_info = cursor.fetchall()
                print(f"发现 {len(streams_info)} 个流:")
                for stream in streams_info:
                    print(f"  流名称: {stream[0]}")
                    #print(f"  SQL: {stream[1][:100]}...")  # 只显示前100个字符
            except Exception as e:
                print(f"查询流详细信息失败: {str(e)}")
            
            # 获取流目标表
            target_tables = self.get_stream_target_tables()
            if not target_tables:
                print("警告: 未找到流目标表")
                cursor.close()
                conn.close()
                return None
            
            delay_results = {
                'check_time': time.strftime('%Y-%m-%d %H:%M:%S'),
                'source_last_ts': source_last_ts,
                'source_ts_ms': source_ts_ms,
                'streams': []
            }
            
            # 检查每个流的延迟
            for table_info in target_tables:
                try:
                    target_table = table_info['target_table']
                    stream_name = table_info['stream_name']
                    
                    #print(f"检查流 {stream_name} 的目标表 {target_table}")
                    
                    # 先检查目标表是否存在
                    try:
                        cursor.execute(f"describe {target_table}")
                        #print(f"目标表 {target_table} 结构正常")
                    except Exception as e:
                        print(f"目标表 {target_table} 不存在或无法访问")
                        
                        stream_result = {
                            'stream_name': stream_name,
                            'target_table': target_table,
                            'error': f"目标表不存在: {str(e)}",
                            'status': 'ERROR'
                        }
                        delay_results['streams'].append(stream_result)
                        continue
                    
                    # 查询目标表最新时间戳
                    cursor.execute(f"select last(ts) from {target_table}")
                    target_result = cursor.fetchall()
                    #print(f"目标表 {target_table} 查询结果: {target_result}")
                    
                    if target_result and target_result[0][0]:
                        target_last_ts = target_result[0][0]
                        target_ts_ms = int(target_last_ts.timestamp() * 1000)
                        
                        # 计算延迟(毫秒)
                        delay_ms = source_ts_ms - target_ts_ms
                        
                        stream_result = {
                            'stream_name': stream_name,
                            'target_table': target_table,
                            'target_last_ts': target_last_ts,
                            'delay_ms': delay_ms,
                            'delay_seconds': delay_ms / 1000.0,
                            'is_lagging': delay_ms > self.max_delay_threshold,
                            'status': 'LAGGING' if delay_ms > self.max_delay_threshold else 'OK'
                        }
                        
                        print(f"流 {stream_name} 延迟: {delay_ms}ms ({delay_ms/1000.0:.2f}秒)")
                        
                    else:
                        # 目标表无数据
                        stream_result = {
                            'stream_name': stream_name,
                            'target_table': target_table,
                            'target_last_ts': None,
                            'delay_ms': None,
                            'delay_seconds': None,
                            'is_lagging': True,
                            'status': 'NO_DATA'
                        }
                    
                    delay_results['streams'].append(stream_result)
                    
                except Exception as e:
                    error_msg = f"检查流 {stream_name} 延迟时出错: {str(e)}"
                    print(error_msg)
                    stream_result = {
                        'stream_name': stream_name,
                        'target_table': target_table,
                        'error': str(e),
                        'status': 'ERROR'
                    }
                    delay_results['streams'].append(stream_result)
            
            cursor.close()
            conn.close()
            
            return delay_results
            
        except Exception as e:
            print(f"检查流计算延迟时出错: {str(e)}")
            return None

    def log_stream_delay_results(self, delay_results):
        """记录流延迟检查结果到日志文件"""
        if not delay_results:
            return
            
        try:           
            # 基于 max_delay_threshold 计算动态阈值
            base_threshold_ms = self.max_delay_threshold
            
            # 定义倍数阈值
            excellent_threshold = base_threshold_ms * 0.1    # 0.1倍检查间隔 - 优秀
            good_threshold = base_threshold_ms * 0.5         # 0.5倍检查间隔 - 良好  
            normal_threshold = base_threshold_ms * 1.0       # 1倍检查间隔 - 正常
            mild_delay_threshold = base_threshold_ms * 6.0   # 6倍检查间隔 - 轻微延迟
            obvious_delay_threshold = base_threshold_ms * 30.0  # 30倍检查间隔 - 明显延迟
            
            with open(self.delay_log_file, 'a') as f:
                # 写入检查时间和源表信息
                f.write(f"\n{'='*80}\n")
                f.write(f"检查时间: {delay_results['check_time']}\n")
                f.write(f"源表最新时间: {delay_results['source_last_ts']}\n")
                f.write(f"源表时间戳(ms): {delay_results['source_ts_ms']}\n")
                f.write(f"延迟阈值: {format_delay_time(self.max_delay_threshold)}\n")
                f.write(f"检查频率: {self.delay_check_interval}秒\n")
                f.write(f"-" * 80 + "\n")
                
                # 写入每个流的延迟信息
                for stream in delay_results['streams']:
                    f.write(f"流名称: {stream['stream_name']}\n")
                    f.write(f"目标表: {stream['target_table']}\n")
                    f.write(f"状态: {stream['status']}\n")
                    
                    if stream['status'] == 'OK' or stream['status'] == 'LAGGING':
                        f.write(f"目标表最新时间: {stream['target_last_ts']}\n")
                        delay_ms = stream['delay_ms']
                        f.write(f"延迟: {format_delay_time(delay_ms)}\n")
                        
                        # 计算延迟倍数并添加中文延迟等级判断和记录
                        delay_multiplier = delay_ms / base_threshold_ms
                        
                        # 添加中文延迟等级判断和记录
                        if delay_ms < excellent_threshold:  # 优秀
                            delay_level = "优秀"
                            delay_desc = f"(< 0.1倍间隔, {delay_multiplier:.2f}倍)"
                        elif delay_ms < good_threshold:  # 良好
                            delay_level = "良好"
                            delay_desc = f"(0.1-0.5倍间隔, {delay_multiplier:.2f}倍)"
                        elif delay_ms < normal_threshold:  # 正常
                            delay_level = "正常"
                            delay_desc = f"(0.5-1倍间隔, {delay_multiplier:.2f}倍)"
                        elif delay_ms < mild_delay_threshold:  # 轻微延迟
                            delay_level = "轻微延迟"
                            delay_desc = f"(1-6倍间隔, {delay_multiplier:.2f}倍)"
                        elif delay_ms < obvious_delay_threshold:  # 明显延迟
                            delay_level = "明显延迟"
                            delay_desc = f"(6-30倍间隔, {delay_multiplier:.2f}倍)"
                        else:  # 严重延迟
                            delay_level = "严重延迟"
                            delay_desc = f"(> 30倍间隔, {delay_multiplier:.2f}倍)"
                        
                        # 写入中文延迟等级
                        f.write(f"延迟等级: {delay_level}\n")
                        f.write(f"延迟倍数: {delay_desc}\n")
                        
                        if stream['is_lagging']:
                            f.write(f"警告: 延迟超过配置阈值 {format_delay_time(self.max_delay_threshold)}!\n")
                            
                    elif stream['status'] == 'NO_DATA':
                        f.write(f"警告: 目标表无数据\n")
                        f.write(f"延迟等级: 严重延迟\n")  # 无数据也算严重延迟
                        f.write(f"延迟倍数: (无数据状态)\n")
                        
                    elif stream['status'] == 'ERROR':
                        f.write(f"错误: {stream.get('error', '未知错误')}\n")
                        f.write(f"延迟等级: 严重延迟\n")  # 错误也算严重延迟
                        f.write(f"延迟倍数: (错误状态)\n")
                    
                    f.write(f"-" * 40 + "\n")
                    
        except Exception as e:
            print(f"写入延迟日志失败: {str(e)}")

    def print_stream_delay_summary(self, delay_results):
        """打印流延迟检查摘要"""
        if not delay_results:
            return
        
        c = Colors.get_colors()
        
        # 基于 delay_check_interval 计算动态阈值
        base_threshold_ms = self.max_delay_threshold
        
        # 定义倍数阈值
        excellent_threshold = base_threshold_ms * 0.1    # 0.1倍检查间隔 - 优秀
        good_threshold = base_threshold_ms * 0.5         # 0.5倍检查间隔 - 良好  
        normal_threshold = base_threshold_ms * 1.0       # 1倍检查间隔 - 正常
        mild_delay_threshold = base_threshold_ms * 6.0   # 6倍检查间隔 - 轻微延迟
        obvious_delay_threshold = base_threshold_ms * 30.0  # 30倍检查间隔 - 明显延迟
            
        print(f"\n=== 流计算延迟检查 ({delay_results['check_time']}) ===")
        print(f"源表最新时间: {delay_results['source_last_ts']}")
        print(f"配置信息: 子表数量({self.table_count}) | 每轮插入记录数({self.real_time_batch_rows}) | vgroups({self.vgroups}) | 数据乱序({self.disorder_ratio})")
        print(f"延迟判断基准: 最大延迟阈值 {format_delay_time(self.max_delay_threshold)} | 检查频率 {self.delay_check_interval}s | 部署模式({self.deployment_mode}) | SQL类型({self.sql_type})")
            
        ok_count = 0
        lagging_count = 0
        no_data_count = 0
        error_count = 0
        
        # 按延迟级别分类统计
        excellent_count = 0
        good_count = 0
        normal_count = 0
        mild_delay_count = 0
        obvious_delay_count = 0
        severe_delay_count = 0
    
        for stream in delay_results['streams']:
            status = stream['status']
            stream_name = stream['stream_name']
            
            if status == 'OK':
                ok_count += 1
                target_time = stream['target_last_ts']
                delay_ms = stream['delay_ms']
                # 根据延迟时间选择图标和描述
                if delay_ms < excellent_threshold:  #优秀
                    excellent_count += 1
                    status_icon = "🟢" if Colors.supports_color() else "✓"
                    delay_desc = f"优秀(<{format_delay_time(excellent_threshold)})"
                    color = c.GREEN
                elif delay_ms < good_threshold:  # 良好
                    good_count += 1
                    status_icon = "🟢" if Colors.supports_color() else "✓"
                    delay_desc = f"良好(<{format_delay_time(good_threshold)})"
                    color = c.GREEN
                else:  # delay_ms < normal_threshold (在阈值内) - 正常
                    normal_count += 1
                    status_icon = "🟡" if Colors.supports_color() else "✓"
                    delay_desc = f"正常(<{format_delay_time(normal_threshold)})"
                    color = c.YELLOW
                    
                print(f"{color}{status_icon} {stream_name}: 延迟 {format_delay_time(delay_ms)} - {delay_desc}{c.END}")
                print(f"  {c.BLUE}目标表最新时间: {target_time}{c.END}")
            
            elif status == 'LAGGING':
                lagging_count += 1
                target_time = stream['target_last_ts']
                delay_ms = stream['delay_ms']
            
                # 根据延迟程度选择颜色和图标
                if delay_ms < mild_delay_threshold:  # 轻微延迟
                    mild_delay_count += 1
                    status_icon = "🟡" if Colors.supports_color() else "⚠"
                    color = c.YELLOW
                    delay_desc = f"轻微延迟(<{format_delay_time(mild_delay_threshold)})"
                elif delay_ms < obvious_delay_threshold:  # 明显延迟
                    obvious_delay_count += 1
                    status_icon = "🟠" if Colors.supports_color() else "⚠"
                    color = c.YELLOW
                    delay_desc = f"明显延迟(<{format_delay_time(obvious_delay_threshold)})"
                else:  # 严重延迟
                    severe_delay_count += 1
                    status_icon = "🔴" if Colors.supports_color() else "⚠"
                    color = c.RED
                    delay_desc = f"严重延迟(>{format_delay_time(obvious_delay_threshold)})"
                    
                print(f"{color}{c.BOLD}{status_icon} {stream_name}: 延迟 {format_delay_time(delay_ms)} - {delay_desc}!{c.END}")
                print(f"  {c.BLUE}目标表最新时间: {target_time}{c.END}")
                
            elif status == 'NO_DATA':
                no_data_count += 1
                severe_delay_count += 1
                status_icon = "❌" if Colors.supports_color() else "✗"
                print(f"{c.RED}{status_icon} {stream_name}: 目标表未生成数据{c.END}")
                
            elif status == 'ERROR':
                error_count += 1
                severe_delay_count += 1
                status_icon = "💥" if Colors.supports_color() else "✗"
                print(f"{c.RED}{c.BOLD}{status_icon} {stream_name}: 目标表不存在或检查出错{c.END}")
                error_msg = stream.get('error', '未知错误')
                print(f"  {c.RED}错误信息: {error_msg}{c.END}")
 
    
        # 计算总数和百分比
        total_streams = len(delay_results['streams'])
        
        def get_percentage(count, total):
            return f"{(count/total*100):.1f}%" if total > 0 else "0.0%"
           
        # 摘要信息带颜色和图标
        summary_parts = []
        # 正常流
        if ok_count > 0:
            ok_icon = "🟢" if Colors.supports_color() else ""
            ok_percent = get_percentage(ok_count, total_streams)
            summary_parts.append(f"{c.GREEN}{ok_icon}正常({ok_count}/{ok_percent}){c.END}")
            
        # 延迟流
        if lagging_count > 0:
            lag_icon = "🟡" if Colors.supports_color() else ""
            lag_percent = get_percentage(lagging_count, total_streams)
            summary_parts.append(f"{c.YELLOW}{lag_icon}延迟({lagging_count}/{lag_percent}){c.END}")
            
        # 未生成数据的流     
        if no_data_count > 0:
            no_data_icon = "❌" if Colors.supports_color() else ""
            no_data_percent = get_percentage(no_data_count, total_streams)
            summary_parts.append(f"{c.RED}{no_data_icon}未生成数据({no_data_count}/{no_data_percent}){c.END}")
            
        # 错误状态的流    
        if error_count > 0:
            error_icon = "💥" if Colors.supports_color() else ""
            error_percent = get_percentage(error_count, total_streams)
            summary_parts.append(f"{c.RED}{error_icon}表不存在({error_count}/{error_percent}){c.END}")
        
        print(f"\n{c.BOLD}📊 摘要 (总流数: {total_streams}): {' '.join(summary_parts)}{c.END}")

        # 详细分布统计
        if ok_count > 0 or lagging_count > 0:
            print(f"\n{c.CYAN}📈 延迟分布详情:{c.END}")
            
            # 正常状态分布
            if ok_count > 0:
                print(f"  {c.GREEN}正常状态分布:{c.END}")
                if excellent_count > 0:
                    excellent_percent = get_percentage(excellent_count, total_streams)
                    print(f"    🟢 优秀: {excellent_count} ({excellent_percent})")
                if good_count > 0:
                    good_percent = get_percentage(good_count, total_streams)
                    print(f"    🟢 良好: {good_count} ({good_percent})")
                if normal_count > 0:
                    normal_percent = get_percentage(normal_count, total_streams)
                    print(f"    🟡 正常: {normal_count} ({normal_percent})")
            
            # 延迟状态分布
            if lagging_count > 0:
                print(f"  {c.YELLOW}延迟状态分布:{c.END}")
                if mild_delay_count > 0:
                    mild_percent = get_percentage(mild_delay_count, total_streams)
                    print(f"    🟡 轻微延迟: {mild_delay_count} ({mild_percent})")
                if obvious_delay_count > 0:
                    obvious_percent = get_percentage(obvious_delay_count, total_streams)
                    print(f"    🟠 明显延迟: {obvious_delay_count} ({obvious_percent})")
                if severe_delay_count > 0:
                    severe_percent = get_percentage(severe_delay_count, total_streams)
                    print(f"    🔴 严重延迟: {severe_delay_count} ({severe_percent})")
    
        # 问题流统计
        if no_data_count > 0 or error_count > 0:
            print(f"\n{c.RED}⚠️ 问题流统计:{c.END}")
            if no_data_count > 0:
                no_data_percent = get_percentage(no_data_count, total_streams)
                print(f"  ❌ 目标表未生成数据: {no_data_count} ({no_data_percent}) ")
            if error_count > 0:
                error_percent = get_percentage(error_count, total_streams)
                print(f"  💥 目标表不存在: {error_count} ({error_percent}) ")
           
        # 显示阈值参考信息
        print(f"\n{c.CYAN}延迟等级参考 (基于检查间隔 {format_delay_time(self.max_delay_threshold)}):{c.END}")
        print(f"  🟢 优秀: < {format_delay_time(excellent_threshold)} (0.1倍间隔)")
        print(f"  🟢 良好: {format_delay_time(excellent_threshold)} - {format_delay_time(good_threshold)} (0.1-0.5倍间隔)")
        print(f"  🟡 正常: {format_delay_time(good_threshold)} - {format_delay_time(normal_threshold)} (0.5-1倍间隔)")
        print(f"  🟡 轻微延迟: {format_delay_time(normal_threshold)} - {format_delay_time(mild_delay_threshold)} (1-6倍间隔)")
        print(f"  🟠 明显延迟: {format_delay_time(mild_delay_threshold)} - {format_delay_time(obvious_delay_threshold)} (6-30倍间隔)")
        print(f"  🔴 严重延迟: > {format_delay_time(obvious_delay_threshold)} (>30倍间隔)")
        print(f"\n{c.CYAN}监控频率: 每 {self.delay_check_interval}秒 检查一次延迟状态{c.END}")
        
        # 状态评估
        healthy_count = ok_count  # 只有正常状态才算健康
        problem_count = lagging_count + no_data_count + error_count # 延迟、无数据、错误都是问题
        
        # 警告和建议信息
        if problem_count > 0:
            if no_data_count > 0 or error_count > 0:
                # 有严重问题（无数据或错误）
                critical_count = no_data_count + error_count
                critical_percent = get_percentage(critical_count, total_streams)
                warning_icon = "🚨" if Colors.supports_color() else "⚠"
                print_error(f"{warning_icon} 严重: {critical_count} 个流存在严重问题 ({critical_percent})!")
                
            if lagging_count > 0:
                # 有延迟问题
                lag_percent = get_percentage(lagging_count, total_streams)
                warning_icon = "⚠️" if Colors.supports_color() else "⚠"
                print_warning(f"{warning_icon} 延迟: {lagging_count} 个流计算出现延迟 ({lag_percent})!")
            
        # 整体状态评估
        if error_count > 0 or no_data_count > 0:
            # 存在严重问题
            advice_icon = "🚨" if Colors.supports_color() else "💡"
            problem_ratio = (error_count + no_data_count) / total_streams * 100
            print_error(f"{advice_icon} 告警: {problem_ratio:.1f}% 的流存在严重问题 - 需要立即检查")
        elif lagging_count > healthy_count:
            # 延迟问题较多
            advice_icon = "💡" if Colors.supports_color() else "💡"
            lag_ratio = lagging_count / total_streams * 100
            print_error(f"{advice_icon} 警告: {lag_ratio:.1f}% 的流出现延迟 - 建议优化性能")
        elif lagging_count > 0:
            # 少量延迟
            tip_icon = "💡" if Colors.supports_color() else "💡"
            lag_ratio = lagging_count / total_streams * 100
            print_warning(f"{tip_icon} 提示: {lag_ratio:.1f}% 的流出现延迟 - 持续关注")
        elif healthy_count == total_streams:
            # 全部正常
            success_icon = "✅" if Colors.supports_color() else "✓"
            print_success(f"{success_icon} 状态优秀: 所有流计算都正常运行 (100%)")
            
        # 调优建议
        if problem_count > 0:
            print(f"\n{c.CYAN}💡 问题分析与建议:{c.END}")
            
            if error_count > 0:
                print(f"  🔴 目标表不存在问题:")
                print(f"    - {error_count} 个流的目标表无法访问")
                print(f"    - 可能原因: 流创建失败、目标表还未生成")
                
            if no_data_count > 0:
                print(f"  ❌ 目标表无数据问题:")
                print(f"    - {no_data_count} 个流的目标表存在但无数据")
                print(f"    - 可能原因: 流计算逻辑问题、源数据不匹配、流未启动")
                print(f"    - 建议: 检查流状态、验证源数据、检查流计算逻辑")
                
            if severe_delay_count > 0:
                severe_ratio = severe_delay_count / total_streams * 100
                print(f"  🔴 严重延迟问题:")
                print(f"    - {severe_delay_count} 个流存在严重延迟 ({severe_ratio:.1f}%)")
                print(f"    - 建议: 立即检查系统资源、优化流计算逻辑")
                
            if obvious_delay_count > 0:
                obvious_ratio = obvious_delay_count / total_streams * 100
                print(f"  🟠 明显延迟问题:")
                print(f"    - {obvious_delay_count} 个流存在明显延迟 ({obvious_ratio:.1f}%)")
                print(f"    - 建议: 检查系统负载、考虑增加资源")
                
            print(f"  📊 系统调优建议:")
            if problem_count > total_streams * 0.5:
                print(f"    - 超过一半的流存在问题，建议全面检查系统配置")
            print(f"    - 当前写入间隔: {self.real_time_batch_sleep}s，可适当增加以减轻压力")
            print(f"    - 可调整 --delay-check-interval 参数改变监控频率")
            
                        

    def start_stream_delay_monitor(self):
        """启动流延迟监控线程"""
        if not self.check_stream_delay:
            return None
            
        def delay_monitor():
            """延迟监控线程函数"""
            print(f"启动流延迟监控 (间隔: {self.delay_check_interval}秒)")
            print(f"延迟日志文件: {self.delay_log_file}")
            
            # 确保延迟日志文件的目录存在
            delay_log_dir = os.path.dirname(self.delay_log_file)
            if not os.path.exists(delay_log_dir):
                try:
                    os.makedirs(delay_log_dir, exist_ok=True)
                    print(f"创建延迟日志目录: {delay_log_dir}")
                except Exception as e:
                    print(f"创建延迟日志目录失败: {str(e)}")
                    return
            
            # 初始化日志文件
            try:
                with open(self.delay_log_file, 'w') as f:
                    f.write(f"TDengine 流计算延迟监控日志\n")
                    f.write(f"开始时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                    f.write(f"最大延迟阈值: {self.max_delay_threshold}ms\n")
                    f.write(f"检查间隔: {self.delay_check_interval}秒\n")
                    f.write(f"数据写入间隔: {self.real_time_batch_sleep}秒\n") 
                    f.write(f"每轮写入记录数: {self.real_time_batch_rows}\n")
                    f.write(f"子表数量: {self.table_count}\n")
                    f.write(f"运行时间: {self.runtime}分钟\n")
                    f.write(f"SQL类型: {self.sql_type}\n")
                    f.write(f"="*80 + "\n")
            except Exception as e:
                print(f"初始化延迟日志文件失败: {str(e)}")
            
            start_time = time.time()
            check_count = 0
            
            # 等待一段时间让流创建完成
            initial_wait = 15  # 等待15秒
            print(f"等待 {initial_wait} 秒让流计算启动...")
            time.sleep(initial_wait)
        
            while time.time() - start_time < self.runtime * 60:
                check_count += 1
                try:
                    print(f"\n--- 第 {check_count} 次延迟检查 ---")
                    
                    # 记录检查开始
                    with open(self.delay_log_file, 'a') as f:
                        f.write(f"\n第 {check_count} 次检查开始: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
                    
                    # 执行延迟检查
                    delay_results = self.check_stream_computation_delay()
                    
                    if delay_results:
                        # 记录到日志文件
                        self.log_stream_delay_results(delay_results)
                        
                        # 打印摘要
                        self.print_stream_delay_summary(delay_results)
                    else:
                        error_msg = f"第 {check_count} 次检查失败: 无法获取延迟结果"
                        print(error_msg)
                        with open(self.delay_log_file, 'a') as f:
                            f.write(f"{error_msg}\n")
                    
                    # 等待下次检查
                    print(f"等待 {self.delay_check_interval} 秒后进行下次检查...")
                    time.sleep(self.delay_check_interval)
                    
                except Exception as e:
                    error_msg = f"第 {check_count} 次延迟监控出错: {str(e)}"
                    print(error_msg)
                    with open(self.delay_log_file, 'a') as f:
                        f.write(f"{error_msg}\n")
                    time.sleep(self.delay_check_interval)
            
            final_msg = f"流延迟监控结束，共执行了 {check_count} 次检查"
            print(final_msg)
            try:
                with open(self.delay_log_file, 'a') as f:
                    f.write(f"\n{final_msg}\n")
                    f.write(f"结束时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            except Exception as e:
                print(f"写入延迟日志结束信息失败: {str(e)}")
        
        # 创建并启动监控线程
        monitor_thread = threading.Thread(target=delay_monitor, name="StreamDelayMonitor")
        monitor_thread.daemon = True
        monitor_thread.start()
        
        return monitor_thread
            
    def do_test_stream_with_realtime_data_ooo(self):
        self.prepare_env()
        self.prepare_source_from_data()
        self.insert_source_from_data()
        
        conn = taos.connect(
            host=self.host,
            user=self.user,
            password=self.passwd,
            config=self.conf
        )
        cursor = conn.cursor()

        try:
            # 运行source_from的数据生成
            subprocess.Popen('taosBenchmark --f /tmp/stream_from.json', 
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True)
            
            # 创建stream_to数据库
            if not self.create_database('stream_to'):
                raise Exception("创建stream_to数据库失败")
            
            time.sleep(5)
            print("数据库已创建,等待数据写入...")
            
            # 等待数据准备就绪
            if not self.wait_for_data_ready(cursor, self.table_count, self.histroy_rows):
                print("数据准备失败，退出测试")
                return
            
            
            # ✅ 计算预热时间和流创建延迟
            if hasattr(self, 'monitor_warm_up_time') and self.monitor_warm_up_time is not None:
                warm_up_time = self.monitor_warm_up_time
                stream_creation_delay = self.monitor_warm_up_time  # 流创建延迟等于预热时间
                print(f"使用用户设置的预热时间: {warm_up_time}秒")
                print(f"✨ 流创建延迟: {stream_creation_delay}秒 (将在预热结束后创建流)")
            else:
                warm_up_time = 60 if self.runtime >= 2 else 0
                stream_creation_delay = warm_up_time  # 流创建延迟等于预热时间
                if warm_up_time > 0:
                    print(f"使用自动计算的预热时间: {warm_up_time}秒")
                    print(f"✨ 流创建延迟: {stream_creation_delay}秒 (将在预热结束后创建流)")
                else:
                    print(f"运行时间较短({self.runtime}分钟)，跳过预热，立即创建流")
                    
                    
            # 获取新连接执行流式查询
            conn, cursor = self.get_connection()
            
            print("开始连接数据库")
            cursor.execute('use stream_from')
            
            # # 执行流式查询
            # print(f"执行流式查询SQL:\n{self.stream_sql}")
            # cursor.execute(self.stream_sql)
            
            # 获取 SQL 模板
            sql_templates = self.stream_sql 
            
            # 判断是否为批量执行
            if isinstance(sql_templates, dict):
                print("\n=== 开始批量创建流 ===")
                total_streams = len(sql_templates)
                success_count = 0
                failed_count = 0
                failed_errors = [] 
                
                for index, (sql_name, sql_template) in enumerate(sql_templates.items(), 1):
                    try:
                        print(f"\n[{index}/{total_streams}] 创建流: {sql_name}")
                        
                        # 提取实际的流名称（从SQL中解析）
                        import re
                        match = re.search(r'create\s+stream\s+([^\s]+)', sql_template, re.IGNORECASE)
                        actual_stream_name = match.group(1) if match else sql_name
                        
                        print(f"  SQL流名称: {actual_stream_name}")
                        # 显示完整的流SQL语句
                        print(f"  流SQL语句:")
                        # 格式化SQL显示，去掉多余的空行和缩进
                        formatted_sql = '\n'.join([
                            '    ' + line.strip() 
                            for line in sql_template.strip().split('\n') 
                            if line.strip()
                        ])
                        print(formatted_sql)
                        
                        print(f"  执行创建...")
                        cursor.execute(sql_template)
                        success_count += 1
                        print(f"  ✓ 创建成功")
                        
                    except Exception as e:
                        failed_count += 1
                        
                        # 提取TDengine的具体错误信息
                        error_message = str(e)
                        tdengine_error = extract_stream_creation_error(error_message)
                        
                        print(f"执行错误: {tdengine_error}")
                        failed_errors.append(f"{sql_name}: {tdengine_error}")
                
                # 显示批量创建结果摘要
                print(f"\n=== 批量创建结果摘要 ===")
                print(f"总流数: {total_streams}")
                print(f"成功: {success_count}")
                print(f"失败: {failed_count}")
                print(f"成功率: {(success_count/total_streams*100):.1f}%")
            
                if failed_count > 0:
                    # 构建详细的错误信息
                    error_summary = f"流创建失败统计: {failed_count}/{total_streams} 个流失败"
                    if len(failed_errors) > 0:
                        # 只保留前5个错误详情，避免信息过长
                        error_details = "; ".join(failed_errors[:5])
                        if len(failed_errors) > 5:
                            error_details += f"; 还有{len(failed_errors)-5}个错误..."
                        error_summary += f" | 错误详情: {error_details}"
                    print(f"⚠️  {failed_count} 个流创建失败，请检查错误信息")
                    
                    if success_count == 0:
                        # 如果所有流都失败了，抛出异常
                        raise Exception(f"所有 {total_streams} 个流创建都失败了")
                    else:
                        # 如果部分失败，记录警告但继续执行
                        print(f"⚠️  注意: {failed_count}/{total_streams} 个流创建失败，将使用 {success_count} 个成功的流继续测试")
                else:
                    print("所有流创建成功！")
                
            else:
                # 单个流的创建
                print("\n=== 开始创建流 ===")
                
                # 提取实际的流名称
                import re
                match = re.search(r'create\s+stream\s+([^\s]+)', sql_templates, re.IGNORECASE)
                actual_stream_name = match.group(1) if match else "未知流名称"
                
                print(f"流名称: {actual_stream_name}")
                print("流SQL语句:")
                print("-" * 80)
                print(sql_templates)
                print("-" * 80)
                
                try:
                    start_time = time.time()
                    cursor.execute(sql_templates)
                    create_time = time.time() - start_time
                    
                    print(f"✓ 流 {actual_stream_name} 创建完成! 耗时: {create_time:.2f}秒")
                    
                except Exception as e:
                    # 提取TDengine的具体错误信息
                    error_message = str(e)
                    tdengine_error = extract_stream_creation_error(error_message)
                    
                    print(f"realtime_data提取TDengine执行错误2: {tdengine_error}")
                    raise Exception(tdengine_error)
            
            print("流式查询已创建,开始监控系统负载")
            cursor.close()
            conn.close()
            
            # 监控系统负载 - 同时监控三个节点
            print(f"调试: 使用性能文件路径: {self.perf_file}")
            
            # 优先使用用户设置的值
            if hasattr(self, 'monitor_warm_up_time') and self.monitor_warm_up_time is not None:
                # 如果用户明确设置了预热时间，使用用户设置的值
                warm_up_time = self.monitor_warm_up_time
                print(f"使用用户设置的预热时间: {warm_up_time}秒")
            else:
                # 如果用户未设置，使用自动计算逻辑
                warm_up_time = 60 if self.runtime >= 2 else 0
                if warm_up_time > 0:
                    print(f"使用自动计算的预热时间: {warm_up_time}秒 (运行时间>=2分钟)")
                else:
                    print(f"运行时间较短({self.runtime}分钟)，跳过预热直接开始监控")
            
            if warm_up_time > 0:
                print(f"\n=== 启动性能监控 (包含{warm_up_time}秒预热期) ===")
                print(f"监控阶段划分:")
                print(f"  📊 预热期 (0-{warm_up_time}秒): 纯数据写入 + 系统稳定")
                print(f"  🔄 流计算期 ({warm_up_time}秒-{self.runtime*60}秒): 数据写入 + 流计算")
                print(f"总运行时间: {self.runtime}分钟")
            else:
                print(f"运行时间较短({self.runtime}分钟)，跳过预热直接开始监控")
                
            loader = MonitorSystemLoad(
                name_pattern='taosd -c', 
                count=self.runtime * 60,
                perf_file=self.perf_file,  # 基础文件名,会自动添加dnode编号
                interval=self.monitor_interval,
                deployment_mode=self.deployment_mode,
                warm_up_time=warm_up_time
            )
                        
            # 在新线程中运行监控
            monitor_thread = threading.Thread(
                target=loader.get_proc_status,
                name="TaosdMonitor"
            )
            monitor_thread.daemon = True
            monitor_thread.start()
            print("开始监控taosd进程资源使用情况...")
            
            # 启动流延迟监控
            delay_monitor_thread = self.start_stream_delay_monitor()    
          
            try:            
                # 循环执行写入和计算
                cycle_count = 0
                start_time = time.time()
                
                while True:
                    cycle_count += 1
                    print(f"\n=== 开始第 {cycle_count} 轮写入和计算 ===")
                    
                    if cycle_count > 1:
                        # 从第二轮开始，先写入新数据
                        print(f"\n写入新一批测试数据 (每轮 {self.real_time_batch_rows} 条记录)...")
                        
                        # 应用写入间隔控制
                        if self.real_time_batch_sleep > 0:
                            print(f"等待 {self.real_time_batch_sleep} 秒后开始写入数据...")
                            time.sleep(self.real_time_batch_sleep)
                        
                        write_start_time = time.time()
                        
                        if not self.update_insert_config():
                            raise Exception("更新写入配置失败")
                        
                        cmd = "taosBenchmark -f /tmp/stream_from_insertdata.json"
                        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True)
                        
                        write_end_time = time.time()
                        write_duration = write_end_time - write_start_time
                        
                        if result.returncode != 0:
                            print(f"写入数据失败: {result.stderr}")
                            raise Exception("写入新数据失败")
                        else:
                            total_records = self.table_count * self.real_time_batch_rows
                            write_speed = total_records / write_duration if write_duration > 0 else 0
                            print(f"数据写入完成: {total_records} 条记录, 耗时 {write_duration:.2f}秒, 速度 {write_speed:.0f} 条/秒")
                            
                            # 如果设置了写入间隔，显示写入控制信息
                            if self.real_time_batch_sleep > 0:
                                print(f"写入间隔控制: {self.real_time_batch_sleep}秒 (用于控制流计算压力)")
                                           
                    # 检查是否达到运行时间限制
                    if time.time() - start_time >= self.runtime * 60:
                        print(f"\n\n已达到运行时间限制 ({self.runtime} 分钟)，停止执行")
                        break
                        
                    print(f"\n=== 第 {cycle_count} 轮处理完成 ===")
             
            except Exception as e:
                print(f"查询写入操作出错: {str(e)}")
            finally:
                cursor.close()
                conn.close()
                print("查询写入操作完成")
        
                # 主动停止监控
                print("主动停止系统监控...")
                loader.stop()
                
            # 等待监控线程结束
            print("等待监控数据收集完成...")
            monitor_thread.join(timeout=15)  # 最多等待15秒
                
            if monitor_thread.is_alive():
                print("监控线程未在预期时间内结束，强制继续...")
            else:
                print("监控线程已正常结束")
        
            if delay_monitor_thread:
                print("等待延迟监控完成...")
                delay_monitor_thread.join(timeout=10)  # 最多等待10秒
                if delay_monitor_thread.is_alive():
                    print("延迟监控线程未在预期时间内结束，强制继续...")
                else:
                    print("延迟监控线程已正常结束")
                
            # 打印最终报告
            if self.check_stream_delay:
                print(f"\n流延迟监控报告已保存到: {self.delay_log_file}")
                self.print_final_delay_summary()
                
                            
            try:
                loader.get_proc_status()
            except KeyboardInterrupt:
                print("\n监控被中断")
            finally:
                # 检查taosd进程
                result = subprocess.run('ps -ef | grep taosd | grep -v grep', 
                                    shell=True, capture_output=True, text=True)
                if result.stdout:
                    print("\ntaosd进程仍在运行")
                    print("如需停止taosd进程，请手动执行: pkill taosd")
                    
                    
        
            print("\n流计算测试完成!")
            
            # 如果有部分流创建失败，在测试完成时报告
            if hasattr(self, 'partial_stream_creation_errors'):
                print(f"\n⚠️  测试完成，但存在流创建问题:")
                print(f"    {self.partial_stream_creation_errors}")
                # 作为警告而不是错误，不中断测试但记录问题
                # 可以选择是否抛出异常
                # raise Exception(self.partial_stream_creation_errors)
                
        except Exception as e:
            print(f"流计算测试失败: {str(e)}")      
            raise      


    def do_test_stream_with_realtime_data(self):
        self.prepare_env()
        self.prepare_source_from_data()
        self.insert_source_from_data()
        
        conn = taos.connect(
            host=self.host,
            user=self.user,
            password=self.passwd,
            config=self.conf
        )
        cursor = conn.cursor()

        try:
            # ✅ 启动初始数据生成进程
            print("启动初始数据生成...")
            data_process = subprocess.Popen('taosBenchmark --f /tmp/stream_from.json', 
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True)
            
            # 创建stream_to数据库
            if not self.create_database('stream_to'):
                raise Exception("创建stream_to数据库失败")
            
            time.sleep(5)
            print("数据库已创建,等待数据写入...")
            
            # 等待数据准备就绪
            if not self.wait_for_data_ready(cursor, self.table_count, self.histroy_rows):
                print("数据准备失败，退出测试")
                return
            
            # ✅ 计算预热时间和流创建延迟
            if hasattr(self, 'monitor_warm_up_time') and self.monitor_warm_up_time is not None:
                warm_up_time = self.monitor_warm_up_time
                stream_creation_delay = self.monitor_warm_up_time
                print(f"使用用户设置的预热时间: {warm_up_time}秒")
                print(f"✨ 流创建延迟: {stream_creation_delay}秒 (将在预热结束后创建流)")
            else:
                warm_up_time = 60 if self.runtime >= 2 else 0
                stream_creation_delay = warm_up_time
                if warm_up_time > 0:
                    print(f"使用自动计算的预热时间: {warm_up_time}秒")
                    print(f"✨ 流创建延迟: {stream_creation_delay}秒 (将在预热结束后创建流)")
                else:
                    print(f"运行时间较短({self.runtime}分钟)，跳过预热，立即创建流")
            
            # ✅ 启动系统监控 (包含预热期)
            print(f"调试: 使用性能文件路径: {self.perf_file}")
            
            if warm_up_time > 0:
                print(f"\n=== 启动性能监控 (包含{warm_up_time}秒预热期) ===")
                print(f"监控阶段划分:")
                print(f"  📊 预热期 (0-{warm_up_time}秒): 持续数据写入 + 系统稳定 + 不创建流")
                print(f"  🔄 流计算期 ({warm_up_time}秒-{self.runtime*60}秒): 创建流 + 数据写入 + 流计算")
                print(f"总运行时间: {self.runtime}分钟")
                print(f"⚠️  预热期目的: 观察纯数据写入性能基线，与流计算期对比")
            else:
                print(f"运行时间较短({self.runtime}分钟)，跳过预热直接开始监控")
                
            loader = MonitorSystemLoad(
                name_pattern='taosd -c', 
                count=self.runtime * 60,
                perf_file=self.perf_file,
                interval=self.monitor_interval,
                deployment_mode=self.deployment_mode,
                warm_up_time=warm_up_time
            )
                        
            # 在新线程中运行监控
            monitor_thread = threading.Thread(
                target=loader.get_proc_status,
                name="TaosdMonitor"
            )
            monitor_thread.daemon = True
            monitor_thread.start()
            print("✅ 系统资源监控已启动...")
            
            # ✅ 用于标记流创建是否成功的变量
            stream_creation_success = False
            stream_creation_error = None
            
            # ✅ 延迟创建流的线程
            stream_creation_thread = None
            if stream_creation_delay > 0:
                def delayed_stream_creation():
                    """延迟创建流的函数"""
                    nonlocal stream_creation_success, stream_creation_error
                    
                    print(f"\n⏰ 等待 {stream_creation_delay} 秒后创建流...")
                    print(f"📊 预热期: 持续数据写入阶段 ({stream_creation_delay}秒)")
                    print(f"   在此期间持续写入数据观察纯写入性能基线")
                    time.sleep(stream_creation_delay)
                    
                    print(f"\n🔄 预热期结束，开始创建流...")
                    print(f"=" * 80)
                    
                    try:
                        # 获取新的数据库连接用于创建流
                        stream_conn, stream_cursor = self.get_connection()
                        stream_cursor.execute('use stream_from')
                        
                        # 获取 SQL 模板并创建流
                        sql_templates = self.stream_sql 
                        
                        # 判断是否为批量执行
                        if isinstance(sql_templates, dict):
                            print("\n=== 开始批量创建流 ===")
                            total_streams = len(sql_templates)
                            success_count = 0
                            failed_count = 0
                            failed_errors = [] 
                            
                            for index, (sql_name, sql_template) in enumerate(sql_templates.items(), 1):
                                try:
                                    print(f"\n[{index}/{total_streams}] 创建流: {sql_name}")
                                    
                                    # 提取实际的流名称（从SQL中解析）
                                    import re
                                    match = re.search(r'create\s+stream\s+([^\s]+)', sql_template, re.IGNORECASE)
                                    actual_stream_name = match.group(1) if match else sql_name
                                    
                                    print(f"  SQL流名称: {actual_stream_name}")
                                    # 显示完整的流SQL语句
                                    print(f"  流SQL语句:")
                                    # 格式化SQL显示，去掉多余的空行和缩进
                                    formatted_sql = '\n'.join([
                                        '    ' + line.strip() 
                                        for line in sql_template.strip().split('\n') 
                                        if line.strip()
                                    ])
                                    print(formatted_sql)
                                    
                                    print(f"  执行创建...")
                                    stream_cursor.execute(sql_template)
                                    success_count += 1
                                    print(f"  ✓ 创建成功")
                                    
                                except Exception as e:
                                    failed_count += 1
                                    
                                    # 提取TDengine的具体错误信息
                                    error_message = str(e)
                                    tdengine_error = extract_stream_creation_error(error_message)
                                    
                                    print(f"执行错误: {tdengine_error}")
                                    failed_errors.append(f"{sql_name}: {tdengine_error}")
                            
                            # 显示批量创建结果摘要
                            print(f"\n=== 批量创建结果摘要 ===")
                            print(f"总流数: {total_streams}")
                            print(f"成功: {success_count}")
                            print(f"失败: {failed_count}")
                            print(f"成功率: {(success_count/total_streams*100):.1f}%")
                        
                            if failed_count > 0:
                                error_summary = f"流创建失败统计: {failed_count}/{total_streams} 个流失败"
                                if len(failed_errors) > 0:
                                    error_details = "; ".join(failed_errors[:5])
                                    if len(failed_errors) > 5:
                                        error_details += f"; 还有{len(failed_errors)-5}个错误..."
                                    error_summary += f" | 错误详情: {error_details}"
                                print(f"⚠️  {failed_count} 个流创建失败，请检查错误信息")
                                
                                if success_count == 0:
                                    # ✅ 如果所有流都失败了，设置错误标记并返回
                                    stream_creation_error = f"所有 {total_streams} 个流创建都失败了"
                                    stream_creation_success = False
                                    print(f"❌ 所有流创建失败，停止测试")
                                    return
                                else:
                                    # 部分失败，仍然算作成功，继续测试
                                    stream_creation_success = True
                                    print(f"⚠️  注意: {failed_count}/{total_streams} 个流创建失败，将使用 {success_count} 个成功的流继续测试")
                            else:
                                stream_creation_success = True
                                print("✅ 所有流创建成功！")
                            
                        else:
                            # 单个流的创建
                            print("\n=== 开始创建流 ===")
                            
                            # 提取实际的流名称
                            import re
                            match = re.search(r'create\s+stream\s+([^\s]+)', sql_templates, re.IGNORECASE)
                            actual_stream_name = match.group(1) if match else "未知流名称"
                            
                            print(f"流名称: {actual_stream_name}")
                            print("流SQL语句:")
                            print("-" * 80)
                            print(sql_templates)
                            print("-" * 80)
                            
                            try:
                                start_time = time.time()
                                stream_cursor.execute(sql_templates)
                                create_time = time.time() - start_time
                                
                                print(f"✅ 流 {actual_stream_name} 创建完成! 耗时: {create_time:.2f}秒")
                                stream_creation_success = True
                                
                            except Exception as e:
                                # ✅ 单个流创建失败，设置错误标记
                                error_message = str(e)
                                tdengine_error = extract_stream_creation_error(error_message)
                                
                                print(f"❌ 流创建失败: {tdengine_error}")
                                stream_creation_error = tdengine_error
                                stream_creation_success = False
                                return
                        
                        if stream_creation_success:
                            print(f"\n🔄 流计算期开始 - 数据写入 + 流计算")
                            print(f"=" * 80)
                        
                        # 关闭流创建连接
                        stream_cursor.close()
                        stream_conn.close()
                        
                    except Exception as e:
                        print(f"❌ 延迟创建流时出错: {str(e)}")
                        stream_creation_error = str(e)
                        stream_creation_success = False
                
                # 创建并启动延迟流创建线程
                stream_creation_thread = threading.Thread(
                    target=delayed_stream_creation,
                    name="DelayedStreamCreation"
                )
                stream_creation_thread.daemon = True
                stream_creation_thread.start()
                print(f"✅ 延迟流创建线程已启动 (将在{stream_creation_delay}秒后执行)")
                
            else:
                # 立即创建流 (原有逻辑保持不变)
                print("\n=== 立即创建流 ===")
    
                try:
                    # 获取新的数据库连接用于创建流
                    stream_conn, stream_cursor = self.get_connection()
                    stream_cursor.execute('use stream_from')
                    
                    # 获取 SQL 模板并创建流
                    sql_templates = self.stream_sql 
                    
                    # 判断是否为批量执行
                    if isinstance(sql_templates, dict):
                        print("\n=== 开始批量创建流 ===")
                        total_streams = len(sql_templates)
                        success_count = 0
                        failed_count = 0
                        failed_errors = [] 
                        
                        for index, (sql_name, sql_template) in enumerate(sql_templates.items(), 1):
                            try:
                                print(f"\n[{index}/{total_streams}] 创建流: {sql_name}")
                                
                                # 提取实际的流名称（从SQL中解析）
                                import re
                                match = re.search(r'create\s+stream\s+([^\s]+)', sql_template, re.IGNORECASE)
                                actual_stream_name = match.group(1) if match else sql_name
                                
                                print(f"  SQL流名称: {actual_stream_name}")
                                # 显示完整的流SQL语句
                                print(f"  流SQL语句:")
                                # 格式化SQL显示，去掉多余的空行和缩进
                                formatted_sql = '\n'.join([
                                    '    ' + line.strip() 
                                    for line in sql_template.strip().split('\n') 
                                    if line.strip()
                                ])
                                print(formatted_sql)
                                
                                print(f"  执行创建...")
                                stream_cursor.execute(sql_template)
                                success_count += 1
                                print(f"  ✓ 创建成功")
                                
                            except Exception as e:
                                failed_count += 1
                                
                                # 提取TDengine的具体错误信息
                                error_message = str(e)
                                tdengine_error = extract_stream_creation_error(error_message)
                                
                                print(f"❌ 执行错误: {tdengine_error}")
                                failed_errors.append(f"{sql_name}: {tdengine_error}")
                        
                        # 显示批量创建结果摘要
                        print(f"\n=== 批量创建结果摘要 ===")
                        print(f"总流数: {total_streams}")
                        print(f"成功: {success_count}")
                        print(f"失败: {failed_count}")
                        print(f"成功率: {(success_count/total_streams*100):.1f}%")
                    
                        if failed_count > 0:
                            error_summary = f"流创建失败统计: {failed_count}/{total_streams} 个流失败"
                            if len(failed_errors) > 0:
                                error_details = "; ".join(failed_errors[:5])
                                if len(failed_errors) > 5:
                                    error_details += f"; 还有{len(failed_errors)-5}个错误..."
                                error_summary += f" | 错误详情: {error_details}"
                            print(f"⚠️  {failed_count} 个流创建失败，请检查错误信息")
                            
                            if success_count == 0:
                                # 如果所有流都失败了，设置错误标记并抛出异常
                                stream_creation_error = f"所有 {total_streams} 个流创建都失败了"
                                stream_creation_success = False
                                print(f"❌ 所有流创建失败，停止测试")
                                raise Exception(stream_creation_error)
                            else:
                                # 部分失败，仍然算作成功，继续测试
                                stream_creation_success = True
                                print(f"⚠️  注意: {failed_count}/{total_streams} 个流创建失败，将使用 {success_count} 个成功的流继续测试")
                        else:
                            stream_creation_success = True
                            print("✅ 所有流创建成功！")
                        
                    else:
                        # 单个流的创建
                        print("\n=== 开始创建单个流 ===")
                        
                        # 提取实际的流名称
                        import re
                        match = re.search(r'create\s+stream\s+([^\s]+)', sql_templates, re.IGNORECASE)
                        actual_stream_name = match.group(1) if match else "未知流名称"
                        
                        print(f"流名称: {actual_stream_name}")
                        print("流SQL语句:")
                        print("-" * 80)
                        print(sql_templates)
                        print("-" * 80)
                        
                        try:
                            start_time = time.time()
                            stream_cursor.execute(sql_templates)
                            create_time = time.time() - start_time
                            
                            print(f"✅ 流 {actual_stream_name} 创建完成! 耗时: {create_time:.2f}秒")
                            stream_creation_success = True
                            
                        except Exception as e:
                            # 单个流创建失败，设置错误标记并抛出异常
                            error_message = str(e)
                            tdengine_error = extract_stream_creation_error(error_message)
                            
                            print(f"❌ 流创建失败: {tdengine_error}")
                            stream_creation_error = tdengine_error
                            stream_creation_success = False
                            raise Exception(tdengine_error)
                    
                    if stream_creation_success:
                        print(f"\n🔄 流计算开始 - 数据写入 + 流计算")
                        print(f"=" * 80)
                    
                    # 关闭流创建连接
                    stream_cursor.close()
                    stream_conn.close()
                    
                except Exception as e:
                    print(f"❌ 立即创建流时出错: {str(e)}")
                    stream_creation_error = str(e)
                    stream_creation_success = False
                    # 对于立即创建流的情况，如果失败就直接抛出异常
                    raise Exception(f"流创建失败: {str(e)}")
            
            # ✅ 立即启动数据写入线程（预热期数据写入）
            write_thread = None
            write_stop_flag = threading.Event()
            
            def continuous_data_writing():
                """持续数据写入线程函数"""
                write_cycle = 0
                while not write_stop_flag.is_set():
                    write_cycle += 1
                    current_time = time.time()
                    
                    # 判断当前阶段
                    if stream_creation_delay > 0:
                        if current_time - test_start_time <= stream_creation_delay:
                            stage = "预热期"
                            stage_desc = "纯数据写入负载 (观察基线性能)"
                        else:
                            stage = "流计算期"
                            stage_desc = "数据写入 + 流计算负载"
                    else:
                        stage = "正常期"
                        stage_desc = "数据写入 + 流计算"
                    
                    print(f"\n=== 第 {write_cycle} 轮数据写入 - {stage} ===")
                    print(f"📝 {stage_desc}")
                    
                    # 应用写入间隔控制
                    if self.real_time_batch_sleep > 0:
                        print(f"等待 {self.real_time_batch_sleep} 秒后开始写入数据...")
                        if write_stop_flag.wait(self.real_time_batch_sleep):
                            break  # 如果收到停止信号，退出
                    
                    write_start_time = time.time()
                    
                    try:
                        # 更新写入配置
                        if not self.update_insert_config():
                            print("❌ 更新写入配置失败")
                            break
                        
                        # 执行数据写入
                        cmd = "taosBenchmark -f /tmp/stream_from_insertdata.json"
                        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True)
                        
                        write_end_time = time.time()
                        write_duration = write_end_time - write_start_time
                        
                        if result.returncode != 0:
                            print(f"❌ 写入数据失败: {result.stderr}")
                            break
                        else:
                            total_records = self.table_count * self.real_time_batch_rows
                            write_speed = total_records / write_duration if write_duration > 0 else 0
                            print(f"✅ 数据写入完成: {total_records} 条记录, 耗时 {write_duration:.2f}秒, 速度 {write_speed:.0f} 条/秒")
                            
                            # 显示写入控制信息
                            if self.real_time_batch_sleep > 0:
                                print(f"⏳ 写入间隔控制: {self.real_time_batch_sleep}秒")
                    
                    except Exception as e:
                        print(f"❌ 数据写入出错: {str(e)}")
                        break
                    
                    # 检查是否应该停止
                    if write_stop_flag.is_set():
                        break
            
            # 启动数据写入线程
            test_start_time = time.time()
            write_thread = threading.Thread(target=continuous_data_writing, name="ContinuousDataWriter")
            write_thread.daemon = True
            write_thread.start()
            print("✅ 持续数据写入线程已启动...")
            
            # ✅ 等待流创建完成（如果是延迟创建）
            if stream_creation_thread:
                print("等待流创建完成...")
                # 等待足够的时间让流创建完成
                max_wait_time = stream_creation_delay + 60  # 预热时间 + 60秒缓冲时间
                stream_creation_thread.join(timeout=max_wait_time)
                
                # 检查流创建结果
                if not stream_creation_success:
                    if stream_creation_error:
                        print(f"❌ 流创建失败，停止测试: {stream_creation_error}")
                        raise Exception(f"流创建失败: {stream_creation_error}")
                    else:
                        print("❌ 流创建状态未知，停止测试")
                        raise Exception("流创建状态未知")
                
                print("✅ 流创建完成，继续数据写入和流计算测试")
            
            # ✅ 只有在流创建成功后才启动延迟监控
            delay_monitor_thread = None
            if stream_creation_success and self.check_stream_delay:
                print("流创建成功，启动延迟监控...")
                delay_monitor_thread = self.start_stream_delay_monitor()
            elif not stream_creation_success:
                print("⚠️  流创建失败，跳过延迟监控")
            elif not self.check_stream_delay:
                print("⚠️  延迟监控未启用")
            else:
                print("⚠️  未知状态，跳过延迟监控")
        
            try:            
                # ✅ 等待测试完成
                total_test_time = self.runtime * 60
                print(f"\n=== 开始等待测试完成 (总时间: {self.runtime}分钟) ===")
                
                time.sleep(total_test_time)
                
                print(f"\n✅ 已达到运行时间限制 ({self.runtime} 分钟)，停止测试")
            
            except Exception as e:
                print(f"测试执行出错: {str(e)}")
            finally:
                # 停止数据写入线程
                if write_thread and write_thread.is_alive():
                    print("停止数据写入线程...")
                    write_stop_flag.set()
                    write_thread.join(timeout=10)
                    if write_thread.is_alive():
                        print("数据写入线程未在预期时间内结束")
                    else:
                        print("数据写入线程已停止")
                
                print("测试主体操作完成")
        
                # 主动停止监控
                print("主动停止系统监控...")
                loader.stop()
                
            # 等待监控线程结束
            print("等待监控数据收集完成...")
            monitor_thread.join(timeout=15)  # 最多等待15秒
                
            if monitor_thread.is_alive():
                print("监控线程未在预期时间内结束，强制继续...")
            else:
                print("监控线程已正常结束")
        
            if delay_monitor_thread:
                print("等待延迟监控完成...")
                delay_monitor_thread.join(timeout=10)  # 最多等待10秒
                if delay_monitor_thread.is_alive():
                    print("延迟监控线程未在预期时间内结束，强制继续...")
                else:
                    print("延迟监控线程已正常结束")
                
            # ✅ 增强的最终报告
            if self.check_stream_delay and stream_creation_success:
                print(f"\n流延迟监控报告已保存到: {self.delay_log_file}")
                self.print_final_delay_summary_enhanced(stream_creation_delay)
            else:
                self.print_final_test_summary_enhanced(stream_creation_delay)
                            
            print("\n流计算测试完成!")
            
        except Exception as e:
            print(f"流计算测试失败: {str(e)}")      
            raise       

    def print_final_delay_summary_enhanced(self, stream_creation_delay):
        """增强的最终延迟测试摘要，包含阶段划分信息"""
        try:
            c = Colors.get_colors()
            
            print_title("\n=== 流计算延迟测试总结 (增强版) ===")
            
            # ✅ 显示测试阶段信息
            if stream_creation_delay > 0:
                print(f"🔄 测试阶段划分:")
                print(f"  📊 预热期 (0-{stream_creation_delay}秒): 持续数据写入 + 系统稳定 + 不创建流")
                print(f"      - 系统资源监控: 启动")
                print(f"      - 数据写入: 启动 (观察纯写入性能基线)")
                print(f"      - 流计算: 未启动")
                print(f"      - 延迟监控: 未开始")
                print(f"  🔄 流计算期 ({stream_creation_delay}-{self.runtime*60}秒): 创建流 + 数据写入 + 流计算")
                print(f"      - 系统资源监控: 继续")
                print(f"      - 数据写入: 继续")
                print(f"      - 流计算: 启动")
                print(f"      - 延迟监控: 开始")
                print(f"  📏 总运行时间: {self.runtime*60}秒 ({self.runtime}分钟)")
                
                # 计算实际流计算时间
                actual_stream_time = self.runtime * 60 - stream_creation_delay
                print(f"  ⏱️  实际流计算时间: {actual_stream_time}秒 ({actual_stream_time/60:.1f}分钟)")
                print(f"  🔍 性能对比: 预热期为纯写入基线，流计算期为写入+流计算负载")
            else:
                print(f"🔄 测试模式: 立即启动流计算 (无预热延迟)")
                print(f"  📏 总运行时间: {self.runtime*60}秒 ({self.runtime}分钟)")
                print(f"  ⏱️  流计算时间: {self.runtime*60}秒 ({self.runtime}分钟)")
            
            # ... 其余代码保持不变 ...
            
        except Exception as e:
            print(f"生成增强摘要时出错: {str(e)}")

    def print_final_test_summary_enhanced(self, stream_creation_delay):
        """增强的最终测试摘要（无延迟监控时使用）"""
        try:
            c = Colors.get_colors()
            
            print_title("\n=== 流计算性能测试总结 ===")
            
            # 显示测试阶段信息
            if stream_creation_delay > 0:
                print(f"🔄 测试阶段划分:")
                print(f"  📊 预热期 (0-{stream_creation_delay}秒): 纯数据写入阶段")
                print(f"  🔄 流计算期 ({stream_creation_delay}-{self.runtime*60}秒): 数据写入 + 流计算阶段")
                actual_stream_time = self.runtime * 60 - stream_creation_delay
                print(f"  ⏱️  实际流计算时间: {actual_stream_time}秒 ({actual_stream_time/60:.1f}分钟)")
            else:
                print(f"🔄 测试模式: 立即启动流计算")
                print(f"  ⏱️  流计算时间: {self.runtime*60}秒 ({self.runtime}分钟)")
            
            print(f"\n✅ 测试已完成!")
            print(f"📄 性能监控数据: /tmp/perf-taosd-*.log")
            print(f"🔍 建议启用 --check-stream-delay 参数获得延迟分析")
            
        except Exception as e:
            print(f"生成测试摘要时出错: {str(e)}")
    

    def print_final_delay_summary(self):
        """打印最终的延迟测试摘要"""
        try:
            c = Colors.get_colors()
            
            print_title("\n=== 流计算延迟测试总结 ===")
            print(f"测试配置:")
            print(f"  子表数量: {self.table_count}")
            print(f"  每轮插入记录数: {self.real_time_batch_rows}")
            print(f"  数据写入间隔: {self.real_time_batch_sleep}秒")
            print(f"  延迟检查间隔: {self.delay_check_interval}秒")
            print(f"  最大延迟阈值: {format_delay_time(self.max_delay_threshold)}")
            print(f"  运行时间: {self.runtime}分钟")
            print(f"  SQL类型: {self.sql_type}")
            print(f"  流数量: {self.stream_num}")
            
            # 分析写入压力
            total_records_per_round = self.table_count * self.real_time_batch_rows
            estimated_rounds = (self.runtime * 60) / max(self.delay_check_interval, 1)
            total_estimated_records = total_records_per_round * estimated_rounds
            
            print(f"\n写入压力分析:")
            print(f"  每轮总记录数: {total_records_per_round:,}")
            print(f"  预计轮次: {estimated_rounds:.0f}")
            print(f"  预计总记录数: {total_estimated_records:,.0f}")
            
            if self.real_time_batch_sleep > 0:
                effective_write_interval = self.delay_check_interval + self.real_time_batch_sleep
                print(f"  实际写入间隔: {effective_write_interval}秒 (检查间隔 + 写入等待)")
                write_rate = total_records_per_round / effective_write_interval
                print(f"  平均写入速率: {write_rate:.0f} 条/秒")
            else:
                print(f"  写入模式: 无间隔控制 (最大速度写入)")
            
            print(f"\n优化建议:")
            if self.real_time_batch_sleep == 0:
                print_warning("  - 当前无写入间隔控制，如果延迟较大可以尝试增加 --real-time-batch-sleep 参数")
            else:
                print_info(f"  - 当前写入间隔: {self.real_time_batch_sleep}秒")
                print_info("  - 如果延迟仍然较大，可以进一步增加写入间隔")
                print_info("  - 如果延迟较小，可以减少写入间隔以增加测试压力")
            
            print(f"  - 可以调整 --real-time-batch-rows 参数改变每轮写入量")
            print(f"  - 可以调整 --delay-check-interval 参数改变监控频率")
            
            print(f"\n延迟监控日志: {self.delay_log_file}")
            print(f"性能监控日志: /tmp/perf-taosd-*.log")
            
        except Exception as e:
            print(f"生成最终摘要时出错: {str(e)}")
            
            
    def do_start_bak(self):
        self.prepare_env()
        self.prepare_source_from_data()
        
        conn = taos.connect(
            host=self.host,
            user=self.user,
            password=self.passwd,
            config=self.conf
        )
        cursor = conn.cursor()

        try:
            # 运行source_from的数据生成
            subprocess.Popen('taosBenchmark --f /tmp/stream_from.json', 
                            stdout=subprocess.PIPE, shell=True, text=True)
            
            # 创建stream_to数据库
            if not self.create_database('stream_to'):
                raise Exception("创建stream_to数据库失败")
            
            time.sleep(5)
            print("数据库已创建,等待数据写入...")
            
            # 等待数据准备就绪
            if not self.wait_for_data_ready(cursor, self.table_count, self.histroy_rows):
                print("数据准备失败，退出测试")
                return
            
            # 获取新连接执行流式查询
            conn, cursor = self.get_connection()
            
            print("开始连接数据库")
            cursor.execute('use stream_from')
            
            # 执行流式查询
            print(f"执行流式查询SQL:\n{self.stream_sql}")
            cursor.execute(self.stream_sql)
            
            print("流式查询已创建,开始监控系统负载")
            cursor.close()
            conn.close()
            
            # 监控系统负载 - 同时监控三个节点
            loader = MonitorSystemLoad(
                name_pattern='taosd -c', 
                count=self.runtime * 60,
                perf_file='/tmp/perf-taosd.log',  # 基础文件名,会自动添加dnode编号
                interval=self.monitor_interval
            )
        
            try:
                loader.get_proc_status()
            except KeyboardInterrupt:
                print("\n监控被中断")
            finally:
                # 检查taosd进程
                result = subprocess.run('ps -ef | grep taosd | grep -v grep', 
                                    shell=True, capture_output=True, text=True)
                if result.stdout:
                    print("\ntaosd进程仍在运行")
                    print("如需停止taosd进程，请手动执行: pkill taosd")
                
        except Exception as e:
            print(f"执行错误: {str(e)}")            
                                    
    def format_timestamp(self, ts):
        """格式化时间戳为可读字符串
        Args:
            ts: 毫秒级时间戳
        Returns:
            str: 格式化后的时间字符串 (YYYY-MM-DD HH:mm:ss)
        """
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts/1000))

         
    def do_query_then_insert(self):
        self.prepare_env()
        self.prepare_source_from_data()
        self.insert_source_from_data()
        
        conn = taos.connect(
            host=self.host,
            user=self.user,
            password=self.passwd,
            config=self.conf
        )
        cursor = conn.cursor()

        try:
            # 运行source_from的数据生成
            subprocess.Popen('taosBenchmark --f /tmp/stream_from.json', 
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True)
        except subprocess.CalledProcessError as e:
            print(f"Error running Bash command: {e}")

        # 创建stream_to数据库
        if not self.create_database('stream_to'):
            raise Exception("创建stream_to数据库失败")
        
        time.sleep(5)
        print("数据库已创建,等待数据写入...")
        # 等待数据准备就绪
        if not self.wait_for_data_ready(cursor, self.table_count, self.histroy_rows):
            print("数据准备失败，退出测试")
            return
        
        try:
            # 启动性能监控线程
            if hasattr(self, 'monitor_warm_up_time') and self.monitor_warm_up_time is not None:
                warm_up_time = self.monitor_warm_up_time
            else:
                warm_up_time = 60 if self.runtime >= 2 else 0
        
            loader = MonitorSystemLoad(
                name_pattern='taosd -c', 
                count=self.runtime * 60,
                perf_file='/tmp/perf-taosd-query.log',
                interval=self.monitor_interval,
                deployment_mode=self.deployment_mode,
                warm_up_time=warm_up_time  
            )
            
            # 在新线程中运行监控
            monitor_thread = threading.Thread(
                target=loader.get_proc_status,
                name="TaosdMonitor"
            )
            monitor_thread.daemon = True
            monitor_thread.start()
            print("开始监控taosd进程资源使用情况...")

            # 数据库连接和查询操作
            conn = taos.connect(
                host=self.host, 
                user=self.user, 
                password=self.passwd, 
                config=self.conf, 
                timezone=self.tz
            )
            cursor = conn.cursor()
            cursor.execute('use stream_to')
            
            print("开始执行查询和写入操作...")

            cursor.execute("create stable if not exists stream_to.stb_result(wstart timestamp, avg_c0 float, avg_c1 float, avg_c2 float,avg_c3 float, max_c0 float, max_c1 float, max_c2 float,max_c3 float, min_c0 float, min_c1 float, min_c2 float,min_c3 float) tags(gid bigint unsigned)")

            try:
                t = threading.Thread(target=do_monitor, args=(self.runtime * 60, self.perf_file))
                t.daemon = True 
                t.start()
            except Exception as e:
                print("Error: unable to start thread, %s" % e)
            finally:
                print("Execution completed")

            print("start to query")

            list = get_table_list(cursor)
            print("there are %d tables" % len(list))

            try:
                # 循环执行写入和计算
                cycle_count = 0
                start_time = time.time()
                
                while True:
                    cycle_count += 1
                    print(f"\n=== 开始第 {cycle_count} 轮写入和计算 ===")
                    
                    if cycle_count > 1:
                        # 从第二轮开始，先写入新数据
                        print(f"\n写入新一批测试数据 (每轮 {self.real_time_batch_rows} 条记录)...")
                        
                        # 应用写入间隔控制
                        if self.real_time_batch_sleep > 0:
                            print(f"等待 {self.real_time_batch_sleep} 秒后开始写入数据...")
                            time.sleep(self.real_time_batch_sleep)
                
                        if not self.update_insert_config():
                            raise Exception("写入新数据失败")
                        
                        cmd = "taosBenchmark -f /tmp/stream_from_insertdata.json"
                        if subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True).returncode != 0:
                            raise Exception("写入新数据失败")
                
                        if self.real_time_batch_sleep > 0:
                            print(f"数据写入完成，写入间隔控制: {self.real_time_batch_sleep}秒")
                
                    for index, table in enumerate(list):
                        table_name = table[0]
                        print(f"\n开始处理表 {table_name} ({index+1}/{len(list)})")
                        window_count = 0
                        
                        count_sql = f"select count(*) from stream_from.{table_name}"
                        cursor.execute(count_sql)
                        count_res = cursor.fetchall()
                        if count_res and count_res[0][0] >= 0:
                            print(f"表 {table_name} 包含 {count_res[0][0]} 条记录")
                            cursor.execute(f"create table if not exists stream_to.{table_name}_1 using stream_to.stb_result tags(1)")
                            
                            # 查询表的时间范围
                            range_sql = f"select first(ts), last(ts) from stream_from.{table_name}"
                            print(f"查询时间范围SQL: {range_sql}")
                            cursor.execute(range_sql)
                            time_range = cursor.fetchall()
                            
                            if time_range and len(time_range) > 0 and time_range[0][0]:
                                start_ts = int(time_range[0][0].timestamp() * 1000)
                                end_ts = int(time_range[0][1].timestamp() * 1000)
                                step = 15 * 1000  # 15秒
                                
                                # 计算总时间窗口数
                                total_windows = ((end_ts - start_ts) // step) + 1
                                print(f"数据时间范围: {self.format_timestamp(start_ts)} -> {self.format_timestamp(end_ts)}")
                                print(f"预计处理 {total_windows} 个时间窗口")
                                
                                # 使用列表保存所有时间窗口
                                time_windows = []
                                current_ts = start_ts
                                while current_ts < end_ts:
                                    next_ts = min(current_ts + step, end_ts)
                                    time_windows.append((current_ts, next_ts))
                                    current_ts = next_ts
                            
                                for window_idx, (window_start, window_end) in enumerate(time_windows, 1):                                
                                    window_sql = (f"select cast({current_ts} as timestamp), "
                                        f"avg(c0), avg(c1), avg(c2), avg(c3), "
                                        f"max(c0), max(c1), max(c2), max(c3), "
                                        f"min(c0), min(c1), min(c2), min(c3) "
                                        f"from stream_from.{table_name} "
                                        f"where ts >= {window_start} and ts < {window_end}")
                                    
                                    #print(f"执行SQL查询: {window_sql}")
                                    cursor.execute(window_sql)
                                    window_data = cursor.fetchall()
                                    
                                    if window_data and len(window_data) > 0:
                                        # 写入数据
                                        insert_sql = f"insert into stream_to.{table_name}_1 values ({current_ts}, {window_data[0][1]}, {window_data[0][2]}, {window_data[0][3]}, {window_data[0][4]}, {window_data[0][5]}, {window_data[0][6]}, {window_data[0][7]}, {window_data[0][8]}, {window_data[0][9]}, {window_data[0][10]}, {window_data[0][11]}, {window_data[0][12]})"
                                        
                                        cursor.execute(insert_sql)
                                        window_count += 1
                                        
                                        # 显示进度
                                        print(f"\r进度: {(window_count/total_windows)*100:.2f}% - "
                                            f"窗口 [{window_count}/{total_windows}]: "
                                            f"{self.format_timestamp(current_ts)} -> "
                                            f"{self.format_timestamp(window_end)}", end='')
                                    else:
                                        print(f" stream_from.{table_name} 没有查询到数据，时间范围: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_ts/1000))} -> {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(query_end/1000))}")
                                        break
                                    
                                    # 移动到下一个时间窗口
                                    current_ts = window_end
                                    
                                print(f"表 {table_name} 处理完成, 共写入 {window_count} 个时间窗口的数据")
                                
                            else:
                                print(f"表 {table_name} 无数据记录，跳过处理")
                
                    # 检查是否达到运行时间限制
                    if time.time() - start_time >= self.runtime * 60:
                        print(f"\n\n已达到运行时间限制 ({self.runtime} 分钟)，停止执行")
                        break
                        
                    print(f"\n=== 第 {cycle_count} 轮处理完成 ===")
                        
            except Exception as e:
                print(f"查询写入操作出错: {str(e)}")
            finally:
                cursor.close()
                conn.close()
                print("查询写入操作完成")
        
                # 主动停止监控
                print("主动停止系统监控...")
                loader.stop()
                
            # 等待监控线程结束
            print("等待监控数据收集完成...")
            monitor_thread.join(timeout=15)  # 最多等待15秒
    
            if monitor_thread.is_alive():
                print("监控线程未在预期时间内结束，程序将继续...")
            else:
                print("监控线程已正常结束")
            
        except KeyboardInterrupt:
            print("\n收到中断信号")
            print("停止监控和查询操作...")
        except Exception as e:
            print(f"执行出错: {str(e)}")
        finally:
            print("\n执行完成")
            print("监控数据已保存到: /tmp/perf-taosd-query-*.log")
            print("可以使用以下命令查看监控数据:")
            print("cat /tmp/perf-taosd-query-all.log")
                     
    def do_query_then_insert_bak(self):
        self.prepare_env()
        self.prepare_source_from_data()
        
        conn = taos.connect(
            host=self.host,
            user=self.user,
            password=self.passwd,
            config=self.conf
        )
        cursor = conn.cursor()

        try:
            # 运行source_from的数据生成
            subprocess.Popen('taosBenchmark --f /tmp/stream_from.json', 
                            stdout=subprocess.PIPE, shell=True, text=True)
        except subprocess.CalledProcessError as e:
            print(f"Error running Bash command: {e}")

        # 创建stream_to数据库
        if not self.create_database('stream_to'):
            raise Exception("创建stream_to数据库失败")
        
        time.sleep(5)
        print("数据库已创建,等待数据写入...")
        # 等待数据准备就绪
        if not self.wait_for_data_ready(cursor, self.table_count, self.histroy_rows):
            print("数据准备失败，退出测试")
            return
        
        try:
            # 启动性能监控线程
            loader = MonitorSystemLoad(
                name_pattern='taosd -c', 
                count=self.runtime * 60,
                perf_file='/tmp/perf-taosd-query.log',
                interval=self.monitor_interval
            )
            
            # 在新线程中运行监控
            monitor_thread = threading.Thread(
                target=loader.get_proc_status,
                name="TaosdMonitor"
            )
            monitor_thread.daemon = True
            monitor_thread.start()
            print("开始监控taosd进程资源使用情况...")

            # 数据库连接和查询操作
            conn = taos.connect(
                host=self.host, 
                user=self.user, 
                password=self.passwd, 
                config=self.conf, 
                timezone=self.tz
            )
            cursor = conn.cursor()
            cursor.execute('use stream_to')
            
            print("开始执行查询和写入操作...")

            cursor.execute("create stable if not exists stream_to.stb_result(wstart timestamp, avg_c0 float, avg_c1 float, avg_c2 float,avg_c3 float, max_c0 float, max_c1 float, max_c2 float,max_c3 float, min_c0 float, min_c1 float, min_c2 float,min_c3 float) tags(gid bigint unsigned)")

            try:
                t = threading.Thread(target=do_monitor, args=(self.runtime * 60, self.perf_file))
                t.daemon = True 
                t.start()
            except Exception as e:
                print("Error: unable to start thread, %s" % e)
            finally:
                print("Execution completed")

            print("start to query")

            list = get_table_list(cursor)
            print("there are %d tables" % len(list))

            try:
                for index, table in enumerate(list):
                    table_name = table[0]
                    print(f"\n开始处理表 {table_name} ({index+1}/{len(list)})")
                    window_count = 0
                    
                    count_sql = f"select count(*) from stream_from.{table_name}"
                    cursor.execute(count_sql)
                    count_res = cursor.fetchall()
                    if count_res and count_res[0][0] >= 0:
                        print(f"表 {table_name} 包含 {count_res[0][0]} 条记录")
                        cursor.execute(f"create table if not exists stream_to.{table_name}_1 using stream_to.stb_result tags(1)")
                        
                        # 查询表的时间范围
                        range_sql = f"select first(ts), last(ts) from stream_from.{table_name}"
                        print(f"查询时间范围SQL: {range_sql}")
                        cursor.execute(range_sql)
                        time_range = cursor.fetchall()
                        
                        if time_range and len(time_range) > 0 and time_range[0][0]:
                            start_ts = int(time_range[0][0].timestamp() * 1000)
                            end_ts = int(time_range[0][1].timestamp() * 1000)
                            step = 15 * 1000  # 15秒
                            
                            # 计算总时间窗口数
                            total_windows = ((end_ts - start_ts) // step) + 1
                            print(f"数据时间范围: {self.format_timestamp(start_ts)} -> {self.format_timestamp(end_ts)}")
                            print(f"预计处理 {total_windows} 个时间窗口")
                            
                            # 使用列表保存所有时间窗口
                            time_windows = []
                            current_ts = start_ts
                            while current_ts < end_ts:
                                next_ts = min(current_ts + step, end_ts)
                                time_windows.append((current_ts, next_ts))
                                current_ts = next_ts
                        
                            for window_idx, (window_start, window_end) in enumerate(time_windows, 1):                                
                                window_sql = (f"select cast({current_ts} as timestamp), "
                                    f"avg(c0), avg(c1), avg(c2), avg(c3), "
                                    f"max(c0), max(c1), max(c2), max(c3), "
                                    f"min(c0), min(c1), min(c2), min(c3) "
                                    f"from stream_from.{table_name} "
                                    f"where ts >= {window_start} and ts < {window_end}")
                                
                                #print(f"执行SQL查询: {window_sql}")
                                cursor.execute(window_sql)
                                window_data = cursor.fetchall()
                                
                                if window_data and len(window_data) > 0:
                                    # 写入数据
                                    insert_sql = f"insert into stream_to.{table_name}_1 values ({current_ts}, {window_data[0][1]}, {window_data[0][2]}, {window_data[0][3]}, {window_data[0][4]}, {window_data[0][5]}, {window_data[0][6]}, {window_data[0][7]}, {window_data[0][8]}, {window_data[0][9]}, {window_data[0][10]}, {window_data[0][11]}, {window_data[0][12]})"
                                    
                                    cursor.execute(insert_sql)
                                    window_count += 1
                                    
                                    # 显示进度
                                    print(f"\r进度: {(window_count/total_windows)*100:.2f}% - "
                                        f"窗口 [{window_count}/{total_windows}]: "
                                        f"{self.format_timestamp(current_ts)} -> "
                                        f"{self.format_timestamp(window_end)}", end='')
                                else:
                                    print(f" stream_from.{table_name} 没有查询到数据，时间范围: {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_ts/1000))} -> {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(query_end/1000))}")
                                    break
                                
                                # 移动到下一个时间窗口
                                current_ts = window_end
                                
                            print(f"表 {table_name} 处理完成, 共写入 {window_count} 个时间窗口的数据")
                            
                        else:
                            print(f"表 {table_name} 无数据记录，跳过处理")
                        
            except Exception as e:
                print(f"查询写入操作出错: {str(e)}")
            finally:
                cursor.close()
                conn.close()
                print("查询写入操作完成")
                
            # 等待监控线程结束
            print("等待监控数据收集完成...")
            monitor_thread.join()
            
        except KeyboardInterrupt:
            print("\n收到中断信号")
            print("停止监控和查询操作...")
        except Exception as e:
            print(f"执行出错: {str(e)}")
        finally:
            print("\n执行完成")
            print("监控数据已保存到: /tmp/perf-taosd-query-*.log")
            print("可以使用以下命令查看监控数据:")
            print("cat /tmp/perf-taosd-query-all.log")
        
    def do_query_then_insert_no_monitor(self):
        self.prepare_env()
        self.prepare_source_from_data()

        try:
            # 运行source_from的数据生成
            subprocess.Popen('taosBenchmark --f /tmp/stream_from.json', 
                            stdout=subprocess.PIPE, shell=True, text=True)
        except subprocess.CalledProcessError as e:
            print(f"Error running Bash command: {e}")

        # 创建stream_to数据库
        if not self.create_database('stream_to'):
            raise Exception("创建stream_to数据库失败")
        
        time.sleep(10)
        print("数据库已创建,等待数据写入...")

        conn = taos.connect(
            host=self.host, user=self.user, password=self.passwd, config=self.conf, timezone=self.tz
        )

        cursor = conn.cursor()
        cursor.execute('use stream_to')

        start_ts = 1748707200000
        step = 300 # 将步长改为300秒

        cursor.execute("create stable if not exists stream_to.stb_result(wstart timestamp, avg_c0 float, avg_c1 float, avg_c2 float,avg_c3 float, max_c0 float, max_c1 float, max_c2 float,max_c3 float, min_c0 float, min_c1 float, min_c2 float,min_c3 float) tags(gid bigint unsigned)")

        try:
            t = threading.Thread(target=do_monitor, args=(self.runtime * 60, self.perf_file))
            t.daemon = True 
            t.start()
        except Exception as e:
            print("Error: unable to start thread, %s" % e)
        finally:
            print("Execution completed")

        print("start to query")

        list = get_table_list(cursor)
        print("there are %d tables" % len(list))

        for index, n in enumerate(list):
            cursor.execute(f"create table if not exists stream_to.{n[0]}_1 using stream_to.stb_result tags(1)")
            count = 1
            while True:
                sql = (f"select cast({start_ts + step * 1000 * (count - 1)} as timestamp), "
                        f"avg(c0), avg(c1), avg(c2), avg(c3), "
                        f"max(c0), max(c1), max(c2), max(c3), "
                        f"min(c0), min(c1), min(c2), min(c3) "
                        f"from stream_from.{n[0]} "
                        f"where ts >= {start_ts + step * 1000 * (count - 1)} "
                        f"and ts < {start_ts + step * 1000 * count}")
                print(f"执行SQL查询: {sql}")
                cursor.execute(sql)

                res = cursor.fetchall()
                if not res or len(res) == 0:  # 检查是否有结果
                    print(f"没有查询到数据，时间范围: {start_ts + step * 1000 * (count - 1)} -> {start_ts + step * 1000 * count}")
                    break

                insert = f"insert into stream_to.{n[0]}_1 values ({start_ts + step * 1000 * (count - 1)}, {res[0][1]}, {res[0][2]}, {res[0][3]}, {res[0][4]}, {res[0][5]}, {res[0][6]}, {res[0][7]}, {res[0][8]}, {res[0][9]}, {res[0][10]}, {res[0][11]}, {res[0][12]})"
                print(f"Inserting: {insert}")
                cursor.execute(insert)
                count += 1
        conn.close()

    def multi_insert(self):
        self.prepare_env()
        self.prepare_source_from_data()

        try:
            subprocess.Popen('taosBenchmark --f /tmp/stream_from.json', stdout=subprocess.PIPE, shell=True, text=True)
            subprocess.Popen('taosBenchmark --f /tmp/stream_to.json', stdout=subprocess.PIPE, shell=True, text=True)
        except subprocess.CalledProcessError as e:
            print(f"Error running Bash command: {e}")

        time.sleep(10)

        for n in range(5):
            try:
                print(f"start query_insert thread {n}")
                t = threading.Thread(target=do_multi_insert, args=(n, 100, self.host, self.user, self.passwd, self.conf, self.tz))
                t.start()
            except Exception as e:
                print("Error: unable to start thread, %s" % e)

        loader = MonitorSystemLoad('taosd', self.runtime * 60, self.perf_file)
        loader.get_proc_status()

    
def main():
    def signal_handler(signum, frame):
        """主程序的信号处理器"""
        print("\n收到中断信号")
        print("正在停止监控,但保持taosd进程运行...")
        return  # 直接返回，不执行任何停止操作

    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # 使用自定义格式化器来改善帮助信息显示
    class CustomHelpFormatter(argparse.RawDescriptionHelpFormatter):
        def __init__(self, prog):
            super().__init__(prog, max_help_position=50, width=150)
            
        def _format_action_invocation(self, action):
            # 保持原有的调用格式
            return super()._format_action_invocation(action)
        
        def _format_usage(self, usage, actions, groups, prefix):
            """重写 usage 格式化，支持多行显示"""
            if prefix is None:
                prefix = 'usage: '
            
            # 获取程序名
            prog = usage or self._prog
            
            # 构建参数列表
            parts = [prog]
            
            # 分组显示参数
            optional_parts = []
            positional_parts = []
            
            for action in actions:
                if action.option_strings:
                    # 可选参数
                    if action.option_strings:
                        opt_str = '/'.join(action.option_strings)
                        if action.nargs != 0:
                            opt_str += f' {action.dest.upper()}'
                        optional_parts.append(f'[{opt_str}]')
                else:
                    # 位置参数
                    positional_parts.append(action.dest.upper())
            
            # 组合所有部分
            all_parts = parts + positional_parts + optional_parts
            
            # 按行分组显示
            lines = [prefix + prog]
            current_line = ''
            max_line_length = 80
            
            for part in optional_parts:
                if len(current_line + ' ' + part) > max_line_length:
                    if current_line:
                        lines.append(' ' * len(prefix) + current_line)
                        current_line = part
                    else:
                        lines.append(' ' * len(prefix) + part)
                else:
                    current_line = current_line + ' ' + part if current_line else part
            
            if current_line:
                lines.append(' ' * len(prefix) + current_line)
                
            return '\n'.join(lines) + '\n\n'
        
        def _fill_text(self, text, width, indent):
            # 保持文本的原始格式，不进行自动换行
            return ''.join(indent + line for line in text.splitlines(keepends=True))
        
        def _split_lines(self, text, width):
            # 按照 \n 分割行，保持手动换行
            if '\n' in text:
                return text.splitlines()
            else:
                # 对于没有换行符的文本，使用默认处理
                return argparse.HelpFormatter._split_lines(self, text, width)
    
    parser = argparse.ArgumentParser(
        prog=' python3 stream_perf_test.py -m 1 --stream-perf-test-dir /home/stream_perf_test_dir',
        description='TDengine Stream Perf Test',
        formatter_class=CustomHelpFormatter,
        epilog="""
上面参数可以进行搭配组合进行测试
        """
    )
    
    # 基础运行参数
    basic_group = parser.add_argument_group('基础参数，控制测试模式、运行时间等核心参数')
    # basic_group.add_argument('-m', '--mode', type=int, default=0,
    #                         help='运行模式:\n'
    #                             '  1: do_test_stream_with_realtime_data (写入数据并执行实时数据流计算)\n'
    #                             '  2: do_query_then_insert (持续查询写入)\n'
    #                             '  3: multi_insert (多线程插入)')
    basic_group.add_argument('-m', '--mode', type=int, default=0,
                            # help='运行模式:\n'
                            #     '  1: do_test_stream_with_realtime_data (写入数据并执行实时数据流计算)\n'
                            #     '  2: do_query_then_insert (持续查询写入)(和流计算无关)\n'
                            #     '  3: do_test_stream_with_restored_data (恢复历史数据并测试指定流计算)')
                            help='''运行模式 (必选):
  1 = do_test_stream_with_realtime_data: 创建数据并执行实时流计算测试
      ├─ 自动生成测试数据
      ├─ 创建并启动流计算  
      ├─ 模拟持续实时写入数据
      └─ 全程性能监控
      
  2 = do_test_stream_with_restored_data: 加载历史数据并执行流计算测试
      ├─ 从备份恢复历史数据
      ├─ 测试指定的流计算SQL  
      └─ 避免重复数据生成，节省时间
      
  3 = do_query_then_insert: 持续查询写入(和流计算无关)
      ├─ 持续写入数据
      ├─ 持续执行聚合查询
      └─ 用于对比流计算与查询性能差异
      
默认: 0 (显示帮助)\n''')
    basic_group.add_argument('-t', '--time', type=int, default=10,
                            help='运行时间(分钟), 默认10分钟\n')
    basic_group.add_argument('-f', '--file', type=str, default='/tmp/perf.log',
                            help='性能数据输出文件路径, 默认/tmp/perf.log')
    
    # 数据相关参数
    data_group = parser.add_argument_group('数据参数, 控制数据规模和写入速度')
    data_group.add_argument('--table-count', type=int, default=500,
                            help='子表数量, 默认500, 测试大数据时需调大此参数')
    data_group.add_argument('--histroy-rows', type=int, default=1,
                            help='每个子表插入历史数据数, 默认1')
    data_group.add_argument('--real-time-batch-rows', type=int, default=200,
                            help='后续每个子表每轮插入实时数据数, 默认200【timestamp_step = 50】')
    data_group.add_argument('--real-time-batch-sleep', type=float, default=0,
                            help='数据写入间隔时间(秒), 默认0秒\n'
                                '    控制每轮数据写入之间的等待时间\n'
                                '    用于调节写入压力，观察流计算延迟变化\n'
                                '    建议值: 0(无控制), 1-10(轻度控制), 10+(重度控制)\n'
                                '    例如: --real-time-batch-sleep 5')
    data_group.add_argument('--disorder-ratio', type=int, default=0,
                            help='数据乱序率, 默认0无乱序, 乱序过多影响流计算速度')
    data_group.add_argument('--vgroups', type=int, default=10,
                            help='''vgroups数量, 默认10\n
示例用法:%(prog)s --table-count 10000 --histroy-rows 1000 \n --real-time-batch-rows 500 --real-time-batch-sleep 5 --disorder-ratio 1 --vgroups 20 --sql-type intervalsliding_stb --time 60\n\n''')
    
    # SQL相关参数
    sql_group = parser.add_argument_group('流计算SQL配置')
    
    # old for phase 1
    # sql_group.add_argument('--sql-type', type=str, default='s2_7',
    #                     choices=['s2_2', 's2_3', 's2_4', 's2_5', 's2_6', 
    #                              's2_7', 's2_8', 's2_9', 's2_10', 's2_11', 
    #                              's2_12', 's2_13', 's2_14', 's2_15', 'all'],
    #                      help='实时流计算SQL类型:\n'
    #                           '  s2_2: trigger at_once\n'
    #                           '  s2_3: trigger window_close\n'
    #                           '  s2_4: trigger max_delay 5s\n'
    #                           '  s2_5: trigger FORCE_WINDOW_CLOSE\n'
    #                           '  s2_6: trigger CONTINUOUS_WINDOW_CLOSE\n'
    #                           '  s2_7: INTERVAL(15s) 窗口\n'
    #                           '  s2_8: INTERVAL with %%trows\n'
    #                           '  s2_9: INTERVAL with MAX_DELAY\n'
    #                           '  s2_10: INTERVAL with MAX_DELAY and %%trows\n'
    #                           '  s2_11: PERIOD(15s) 定时触发\n'
    #                           '  s2_12: SESSION(ts,10a) 会话窗口\n'
    #                           '  s2_13: COUNT_WINDOW(1000) 计数窗口\n'
    #                           '  s2_14: EVENT_WINDOW 事件窗口\n'
    #                           '  s2_15: STATE_WINDOW 状态窗口\n'
    #                           '  all: 批量执行多种流类型')
    
    def validate_sql_type(value):
        """验证 SQL 类型参数"""
        valid_choices = [
            'select_stream', 'all_detailed',
            'tbname_agg', 'tbname_select', 'trows_agg', 'trows_select',
            'sourcetable_agg', 'sourcetable_select',
            'intervalsliding_stb', 'intervalsliding_stb_partition_by_tbname', 'intervalsliding_stb_partition_by_tag', 'intervalsliding_tb',
            'sliding_stb', 'sliding_stb_partition_by_tbname', 'sliding_stb_partition_by_tag', 'sliding_tb', 
            'session_stb', 'session_stb_partition_by_tbname', 'session_stb_partition_by_tag', 'session_tb',
            'count_stb', 'count_stb_partition_by_tbname', 'count_stb_partition_by_tag', 'count_tb',
            'event_stb', 'event_stb_partition_by_tbname', 'event_stb_partition_by_tag', 'event_tb',
            'state_stb', 'state_stb_partition_by_tbname', 'state_stb_partition_by_tag', 'state_tb',
            'period_stb', 'period_stb_partition_by_tbname', 'period_stb_partition_by_tag', 'period_tb',
            'intervalsliding_detailed', 'sliding_detailed', 'session_detailed', 'count_detailed',
            'event_detailed', 'state_detailed', 'period_detailed'
        ]
        
        if value not in valid_choices:
            # 按类别格式化错误信息
            error_msg = f"invalid choice: '{value}'\n\n有效选择项:\n"
            error_msg += "说明: stb(超级表), tb(子表), by_tbname(tbname分组), by_tag(tag分组,第一列tag)\n"
            error_msg += "查询类型: select_stream 查询所有流信息\n"
            error_msg += "固定参数组合: tbname_agg, tbname_select, trows_agg, trows_select, sourcetable_agg, sourcetable_select,\n"
            error_msg += "时间窗口: intervalsliding_stb, intervalsliding_stb_partition_by_tbname, intervalsliding_stb_partition_by_tag, intervalsliding_tb\n"
            error_msg += "滑动窗口: sliding_stb, sliding_stb_partition_by_tbname, sliding_stb_partition_by_tag, sliding_tb\n"
            error_msg += "会话窗口: session_stb, session_stb_partition_by_tbname, session_stb_partition_by_tag, session_tb\n"
            error_msg += "计数窗口: count_stb, count_stb_partition_by_tbname, count_stb_partition_by_tag, count_tb\n"
            error_msg += "事件窗口: event_stb, event_stb_partition_by_tbname, event_stb_partition_by_tag, event_tb\n"
            error_msg += "状态窗口: state_stb, state_stb_partition_by_tbname, state_stb_partition_by_tag, state_tb\n"
            error_msg += "定时触发: period_stb, period_stb_partition_by_tbname, period_stb_partition_by_tag, period_tb\n"
            error_msg += "组合模板: intervalsliding_detailed, session_detailed, count_detailed, event_detailed, state_detailed, period_detailed, all_detailed"
            
            raise argparse.ArgumentTypeError(error_msg)
        return value

    # 使用时不设置 choices，而是使用 type 参数进行验证
    sql_group.add_argument('--sql-type', type=validate_sql_type, default='select_stream',
                        help='''实时流计算SQL类型:
    说明:
        stb(超级表), tb(子表), by_tbname(tbname分组), by_tag(tag分组,第一列tag)
    查询类型:
        select_stream: 查询所有流信息 (默认查询information_schema.ins_streams的数据)
    固定参数组合模板(忽略 --tbname-or-trows-or-sourcetable 和 --agg-or-select 参数):
        tbname_agg:    所有窗口类型 + tbname + 聚合查询
        tbname_select: 所有窗口类型 + tbname + 投影查询  
        trows_agg:     所有窗口类型 + trows +  聚合查询
        trows_select:  所有窗口类型 + trows +  投影查询
        sourcetable_agg:   所有窗口类型 + sourcetable + 聚合查询
        sourcetable_select: 所有窗口类型 + sourcetable + 投影查询
    时间窗口模板(INTERVAL(15s) SLIDING(15s)):
        intervalsliding_stb, intervalsliding_stb_partition_by_tbname, intervalsliding_stb_partition_by_tag, intervalsliding_tb
    滑动窗口模板(SLIDING(15s)):
        sliding_stb, sliding_stb_partition_by_tbname, sliding_stb_partition_by_tag, sliding_tb
    会话窗口模板(SESSION(ts,10a)):
        session_stb, session_stb_partition_by_tbname, session_stb_partition_by_tag, session_tb
    计数窗口模板(COUNT_WINDOW(1000)):
        count_stb, count_stb_partition_by_tbname, count_stb_partition_by_tag, count_tb
    事件窗口模板(EVENT_WINDOW(START WITH c0 > -10000000 END WITH c0 < 10000000)):
        event_stb, event_stb_partition_by_tbname, event_stb_partition_by_tag, event_tb
    状态窗口模板(STATE_WINDOW(c0)):
        state_stb, state_stb_partition_by_tbname, state_stb_partition_by_tag, state_tb
    定时触发模板(PERIOD(15s)):
        period_stb, period_stb_partition_by_tbname, period_stb_partition_by_tag, period_tb
    组合模板(每组包含上述4个组合,all包含上述所有组合):
        intervalsliding_detailed, sliding_detailed, session_detailed, count_detailed, event_detailed, 
        state_detailed, period_detailed, all_detailed''')
    
    sql_group.add_argument('--stream-num', type=int, default=1,
                        help='流数量: 当 sql-type 为非组合模版时，创建指定数量的相同流\n'
                                '    并发流数量, 默认1个流\n'
                                '    设置为N时，会创建N个编号为1到N的相同流，如果是从子表获取数据则从第一个子表到第N个子表\n'
                                '    需要和 --sql-type 搭配使用，适合测试多个流的压力\n'
                                '    示例: --sql-type intervalsliding_stb --stream-num 100')
    
    sql_group.add_argument('--stream-sql', type=str,
                        help='自定义流计算SQL (优先级最高)\n'
                                '    示例: "create stream test_stream..."')
    
    sql_group.add_argument('--agg-or-select', type=str, default='agg',
                        choices=['agg', 'select'],
                        help='查询类型:\n'
                            '    agg: 聚合查询 (avg,max,min)\n'
                            '         avg(c0), avg(c1), avg(c2), avg(c3), max(c0), max(c1), max(c2), max(c3), min(c0), min(c1), min(c2), min(c3)\n'
                            '    select: 投影查询 (c0,c1,c2,c3)')
    sql_group.add_argument('--custom-columns', type=str,
                        help='自定义查询列 (逗号分隔):\n'
                                '    示例: "sum(c0),count(*),avg(c1)"\n'
                                '          "c0,c1,c2" (投影查询)\n'
                                '    注意: 会覆盖 --agg-or-select 设置')
    sql_group.add_argument('--tbname-or-trows-or-sourcetable', type=str, default='sourcetable',
                        choices=['tbname', 'trows', 'sourcetable'],
                        help='FROM子句类型，默认sourcetable:\n'
                            '    sourcetable - 超级表: from stream_from.stb where ts >= _twstart and ts <= _twend\n'
                            '    sourcetable - 子表: from stream_from.ctb0_N where ts >= _twstart and ts <= _twend\n'
                            '    tbname: from %%tbname where ts >= _twstart and ts <= _twend\n'
                            '    trows:  from %%trows ')
    
    sql_group.add_argument('--auto-combine', action='store_true',
                    help='''自动生成参数组合，创建6个流(忽略 --agg-or-select 和 --tbname-or-trows-or-sourcetable 参数):
  
功能: 对指定的单个流类型，自动生成所有参数组合
  ├─ tbname + agg:         使用 %%tbname + 聚合查询
  ├─ tbname + select:      使用 %%tbname + 投影查询  
  ├─ trows + agg:          使用 %%trows + 聚合查询
  ├─ trows + select:       使用 %%trows + 投影查询
  ├─ sourcetable + agg:    使用具体表名 + 聚合查询
  └─ sourcetable + select: 使用具体表名 + 投影查询

适用范围: 仅对单个流类型有效 (如 intervalsliding_stb)
不适用于: 组合模板 (*_detailed) 和固定参数模板 (tbname_agg等)

示例用法:
  --sql-type intervalsliding_stb --auto-combine    # 生成6个组合
  --sql-type session_stb --auto-combine           # 生成6个组合
  
注意: 使用此参数时会忽略 --agg-or-select 和 --tbname-or-trows-or-sourcetable 设置''')
    
    # todo
    sql_group.add_argument('--sql-file', type=str,
                        help='从文件读取流式查询SQL，todo\n'
                            '''    示例: --sql-file ./my_stream.sql\n
示例用法1:%(prog)s --table-count 10000 \n --real-time-batch-rows 500 --real-time-batch-sleep 5 --sql-type intervalsliding_stb --time 60\n
示例用法2:%(prog)s --table-count 10000 \n --real-time-batch-rows 500 --real-time-batch-sleep 5 --sql-type intervalsliding_stb --auto-combine --time 60\n
示例用法3:%(prog)s --table-count 10000 \n --real-time-batch-rows 500 --real-time-batch-sleep 5 --sql-type intervalsliding_detailed --time 60\n
示例用法4:%(prog)s --table-count 10000 \n --real-time-batch-rows 500 --real-time-batch-sleep 5 --sql-type tbname_agg --time 60\n
示例用法5:%(prog)s --table-count 10000 \n --real-time-batch-rows 500 --real-time-batch-sleep 5 --sql-type intervalsliding_detailed --tbname-or-trows-or-sourcetable trows\n
示例用法6:%(prog)s --table-count 10000 \n --real-time-batch-rows 500 --real-time-batch-sleep 5 --sql-type intervalsliding_detailed --agg-or-select select\n
示例用法7:%(prog)s --table-count 10000 \n --real-time-batch-rows 500 --real-time-batch-sleep 5 --sql-type intervalsliding_stb --stream-num 100\n\n''')
    
    # 流性能监控参数
    stream_monitor_group = parser.add_argument_group('流性能监控和流计算延迟检测')
    stream_monitor_group.add_argument('--check-stream-delay', action='store_true',
                                    help='''启用流计算延迟监控,默认不开启,加上此参数后会自动监控流计算延迟:
  
功能: 实时监测源表与目标表的数据延迟
  ├─ 智能延迟分级: 优秀/良好/正常/轻微/明显/严重
  ├─ 动态阈值计算: 基于检查间隔自动调整  
  ├─ 问题诊断: 自动识别无数据/表不存在等问题
  └─ 优化建议: 提供具体的性能调优建议

输出: 详细延迟报告保存为 /tmp/*-stream-delay.log''')
    stream_monitor_group.add_argument('--max-delay-threshold', type=int, default=30000,
                                    help='最大允许延迟时间(毫秒), 默认30000ms(30秒)\n'
                                        '超过此阈值认为流计算跟不上数据写入速度，可以针对具体的流和间隔调整')
    stream_monitor_group.add_argument('--delay-check-interval', type=int, default=10,
                                    help='''延迟检查间隔(秒), 默认10秒, 目前是粗粒度的检查生成目标表超级表的last(ts), 而非每个生成子表的last(ts)\n
示例用法:%(prog)s --check-stream-delay \n--max-delay-threshold 60000 --delay-check-interval 5 --sql-type intervalsliding_stb \n\n''')
 
    # 批量测试参数
    batch_group = parser.add_argument_group('批量测试，自动执行多种参数组合')
    batch_group.add_argument('--batch-test', action='store_true',
                            help='''启用批量测试模式:
执行流程:
  1. 自动生成所有有效的参数组合 (约168种，其中成功102种，失败66种)
  2. 为每个组合依次执行完整的测试流程
  3. 每次测试前自动清理环境和重启服务
  4. 生成详细的HTML测试报告和性能数据
  5. 支持测试中断后从指定位置继续

特点:
  ├─ 全自动化: 无需人工干预，自动处理所有组合
  ├─ 环境隔离: 每次测试前完全清理环境，避免干扰
  ├─ 详细报告: HTML格式报告，包含成功率、耗时等统计
  ├─ 流延迟监控: 批量测试自动启用流延迟检查 
  └─ 断点续传: 支持从指定测试编号开始执行

注意: 批量测试会覆盖其他相关参数设置，并强制启用流延迟监控''')
    
    batch_group.add_argument('--batch-sql-types', type=str, nargs='+',
                        help='''指定批量测试的SQL类型，支持多个类型空格分隔:

单个窗口类型模板:
  intervalsliding_stb, intervalsliding_stb_partition_by_tbname, intervalsliding_stb_partition_by_tag, intervalsliding_tb
  sliding_stb, sliding_stb_partition_by_tbname, sliding_stb_partition_by_tag, sliding_tb
  session_stb, session_stb_partition_by_tbname, session_stb_partition_by_tag, session_tb
  count_stb, count_stb_partition_by_tbname, count_stb_partition_by_tag, count_tb
  event_stb, event_stb_partition_by_tbname, event_stb_partition_by_tag, event_tb
  state_stb, state_stb_partition_by_tbname, state_stb_partition_by_tag, state_tb
  period_stb, period_stb_partition_by_tbname, period_stb_partition_by_tag, period_tb

固定参数组合模板(所有窗口类型):
  tbname_agg:        所有窗口类型 + tbname + 聚合查询 (28种组合)
  tbname_select:     所有窗口类型 + tbname + 投影查询 (28种组合)
  trows_agg:         所有窗口类型 + trows + 聚合查询 (28种组合)
  trows_select:      所有窗口类型 + trows + 投影查询 (28种组合)
  sourcetable_agg:   所有窗口类型 + sourcetable + 聚合查询 (28种组合)
  sourcetable_select: 所有窗口类型 + sourcetable + 投影查询 (28种组合)

组合模板(每组4种组合):
  intervalsliding_detailed: 时间窗口的24种组合
  sliding_detailed:        滑动窗口的24种组合  
  session_detailed:        会话窗口的24种组合
  count_detailed:          计数窗口的24种组合
  event_detailed:          事件窗口的24种组合
  state_detailed:          状态窗口的24种组合
  period_detailed:         定时触发的24种组合

特殊类型:
  all_detailed:            所有窗口类型的detailed组合 (168种)

示例用法:
  --batch-sql-types tbname_agg                    # 只测试tbname+agg的所有窗口类型
  --batch-sql-types intervalsliding_detailed      # 只测试时间窗口的24种组合
  --batch-sql-types tbname_agg trows_agg          # 测试tbname_agg和trows_agg
  --batch-sql-types sliding_stb session_stb       # 测试指定的两个单个模板
  
默认: 如果不指定此参数，则测试所有有效组合(168种)''')
    
    batch_group.add_argument('--batch-single-template-mode', type=str, 
                        choices=['default', 'all-combinations'], 
                        default='default',
                        help='''批量测试中单个模板的处理方式:
    default: 只测试默认参数组合 (sourcetable + agg)
    all-combinations: 测试所有6种参数组合 (3个from类型 × 2个查询类型)
    
示例:
    --batch-sql-types intervalsliding_stb --batch-single-template-mode default
        只测试 1 个组合: intervalsliding_stb + sourcetable + agg        
    --batch-sql-types intervalsliding_stb --batch-single-template-mode all-combinations  
        测试 6 个组合: intervalsliding_stb 的所有参数组合
        
注意: 详细模板 (*_detailed) 和固定参数模板 (tbname_agg等) 不受此参数影响''')
    
    batch_group.add_argument('--batch-test-time', type=int, default=5,
                            help='批量测试中每个测试的运行时间(分钟), 默认5分钟\n'
                                '总耗时 = 组合数 × 每个测试时间\n'
                                '例如168个组合×5分钟 = 14小时')
    
    batch_group.add_argument('--batch-start-from', type=int, default=0,
                            help='从第几个测试开始(从0开始计数), 默认0\n'
                                '用于断点续传，例如 --batch-start-from 50')
    
    batch_group.add_argument('--batch-filter-mode', type=str, choices=['all', 'skip-known-failures', 'only-known-failures'], default='all',
                        help='''批量测试过滤模式，控制是否运行已知失败的测试场景:
    all: 运行所有168个测试场景 (默认)
    skip-known-failures: 跳过已知会失败的66个场景，只运行102个成功场景
    only-known-failures: 只运行已知会失败的66个场景，用于调试失败原因
  
说明: 基于历史测试数据统计，某些参数组合由于TDengine限制会固定失败
      使用此参数可以避免浪费时间在已知问题上，专注测试有效场景''')
    
    batch_group.add_argument('--batch-test-limit', type=int,
                            help='''限制测试数量，用于测试部分组合\n
示例用法1:测试所有组合 (168种)%(prog)s --monitor-interval 10 \n--deployment-mode single --batch-test --batch-test-time 3\n
示例用法2:测试特定的固定参数组合%(prog)s --monitor-interval 10 \n--deployment-mode single --batch-test --batch-sql-types tbname_agg \n
示例用法3:测试特定的固定参数组合%(prog)s --monitor-interval 10 \n--deployment-mode single --batch-test --batch-sql-types tbname_agg trows_agg \n
示例用法4:测试特定窗口类型的详细组合%(prog)s --monitor-interval 10 \n--deployment-mode single --batch-test --batch-sql-types intervalsliding_detailed --batch-test-time 3\n
示例用法5:测试特定窗口类型的详细组合%(prog)s --monitor-interval 10 \n--deployment-mode single --batch-test --batch-sql-types sliding_detailed session_detailed --batch-test-time 3\n
示例用法6:测试指定的单个模板%(prog)s --monitor-interval 10 \n--deployment-mode single --batch-test --batch-sql-types intervalsliding_stb sliding_stb \n
示例用法7:混合测试模式%(prog)s --monitor-interval 10 \n--deployment-mode single --batch-test --batch-sql-types tbname_agg intervalsliding_detailed sliding_stb \n
示例用法8:断点续传%(prog)s --monitor-interval 10 \n--deployment-mode single --batch-test --batch-start-from 50 --batch-test-limit 20 --batch-test-time 3\n
示例用法9:限制测试数量%(prog)s --monitor-interval 10 \n--deployment-mode single --batch-test --batch-sql-types all_detailed --batch-test-limit 10\n
示例用法10:只运行成功的测试%(prog)s --monitor-interval 10 \n--deployment-mode single --batch-test --batch-filter-mode skip-known-failures \n
示例用法11:只运行失败的测试%(prog)s --monitor-interval 10 \n--deployment-mode single --batch-test --batch-filter-mode only-known-failures \n
示例用法12:成功/失败的组合%(prog)s --monitor-interval 10 \n--deployment-mode single --batch-test --batch-sql-types intervalsliding_detailed --batch-filter-mode skip-known-failures\n\n''')
     

   
    # 系统配置参数
    system_group = parser.add_argument_group('系统配置，配置TDengine部署架构和系统参数')
    system_group.add_argument('--stream-perf-test-dir', type=str, 
                            default='/home/stream_perf_test_dir',
                            help='单机或者集群测试根目录, 默认/home/stream_perf_test_dir, 尽量配置此参数避免误删目录外的文件\n'
                                 '存储配置文件、数据文件、日志文件\n'
                                 '确保目录有足够空间和读写权限')
    system_group.add_argument('--monitor-interval', type=int, default=5,
                            help='性能数据采集间隔(秒), 默认5秒\n'
                                  '监控CPU、内存、磁盘IO使用情况\n这个是监控taosd占用资源的，delay-check-interval是检查流数据生成的\n'
                                  '建议值: 1(详细), 5(标准), 10(粗略)')
    system_group.add_argument('--deployment-mode', type=str, default='single',
                            choices=['single', 'cluster'],
                            help='部署模式:默认single\n'
                                '    single:  单节点模式，dnode+mnode+snode 都在一个节点\n'
                                '    cluster: 三节点集群模式，3个dnode，3个mnode，源数据库vgroups都在dnode1，目标数据库vgroups都在dnode2，snode独立在dnode3')
    system_group.add_argument('--debug-flag', type=int, default=131,
                            help='TDengine调试级别, 默认131\n'
                                '    常用值: 131(默认), 135, 143')
    system_group.add_argument('--monitor-warm-up-time', type=int, default=0,
                            help='性能监控预热时间(秒), 默认0秒\n'
                             '    在此期间收集数据但不打印到控制台，等待系统稳定\n'
                             '    当运行时间 >= 2分钟时，自动设置为60秒\n'
                             '    当运行时间 < 2分钟时，自动设置为0秒\n'
                             '    也可手动指定: --monitor-warm-up-time 120')
    system_group.add_argument('--num-of-log-lines', type=int, default=5000000,
                            help='''TDengine日志文件最大行数, 默认5000000,最大值不能超过2000000000\n
示例用法:%(prog)s --monitor-interval 10 \n--deployment-mode single --debug-flag 135 --num-of-log-lines 1000\n\n''')
    
    # 数据管理参数
    data_mgmt_group = parser.add_argument_group('数据管理')
    data_mgmt_group.add_argument('--create-data', action='store_true',
                                help='''创建测试数据并自动备份:  
执行流程:
  1. 准备TDengine环境 (启动服务、配置单节点或者集群)
  2. 创建指定规模的测试数据  
  3. 自动备份到 --stream-perf-test-dir/data_bak
  4. 保留环境供后续测试历史数据流计算使用
建议: 测试多组历史数据流计算前先执行此操作\n
示例用法:创建并备份测试数据%(prog)s --create-data 
--deployment-mode single --table-count 500 --histroy-rows 10000000\n\n''')
    
    data_mgmt_group.add_argument('--restore-data', action='store_true',
                                help='''从备份恢复测试数据: 
执行流程:
  1. 检查备份数据完整性
  2. 停止现有TDengine服务
  3. 恢复数据文件
  4. 重启服务并验证
适用: 快速恢复到已知数据状态，避免重复数据生成，节省时间\n如果测试历史数据进行流计算，建议用-m 2模式\n
示例用法:恢复数据并测试%(prog)s --restore-data --deployment-mode single''')
    
    args = parser.parse_args()
    
    # 打印运行参数
    print("运行参数:")
    print(f"运行模式: {args.mode}")
    print(f"运行时间: {args.time}分钟")
    print(f"性能文件: {args.file}")
    print(f"子表数量: {args.table_count}")
    print(f"初始插入记录数: {args.histroy_rows}")
    print(f"后续每轮插入记录数: {args.real_time_batch_rows}")
    print(f"数据写入间隔: {args.real_time_batch_sleep}秒")
    print(f"数据乱序: {args.disorder_ratio}")
    print(f"vgroups数: {args.vgroups}")
    print(f"流类型: {args.sql_type}")
    print(f"流数量: {args.stream_num}")
    print(f"集群目录: {args.stream_perf_test_dir}")
    print(f"性能数据采集间隔: {args.monitor_interval}秒")
    print(f"部署模式: {args.deployment_mode}")
    print(f"调试标志: {args.debug_flag}")
    print(f"日志行数: {args.num_of_log_lines}")
    
    print(f"流延迟检查: {'启用' if args.check_stream_delay else '禁用'}")
    if args.check_stream_delay:
        print(f"最大延迟阈值: {args.max_delay_threshold}ms")
        print(f"延迟检查间隔: {args.delay_check_interval}秒")
    
    # 处理SQL参数
    stream_sql = None
    if args.sql_file:
        try:
            with open(args.sql_file, 'r') as f:
                stream_sql = f.read().strip()
                print(f"从文件加载SQL: {args.sql_file}")
        except Exception as e:
            print(f"读取SQL文件失败: {e}")
            return
    elif args.stream_sql:
        stream_sql = args.stream_sql
        print("使用命令行指定SQL")
    else:
        print("使用默认SQL")
        
    custom_columns = None
    if args.custom_columns:
        custom_columns = [col.strip() for col in args.custom_columns.split(',')]
    
    # 创建StreamStarter实例
    try:
        starter = StreamStarter(
            runtime=args.time,
            perf_file=args.file,
            table_count=args.table_count,
            histroy_rows=args.histroy_rows,
            real_time_batch_rows=args.real_time_batch_rows,
            real_time_batch_sleep=args.real_time_batch_sleep,
            disorder_ratio=args.disorder_ratio,
            vgroups=args.vgroups,
            stream_sql=stream_sql,
            sql_type=args.sql_type,
            stream_num=args.stream_num, 
            stream_perf_test_dir=args.stream_perf_test_dir,
            monitor_interval=args.monitor_interval,
            deployment_mode=args.deployment_mode,
            debug_flag=args.debug_flag, 
            num_of_log_lines=args.num_of_log_lines,            
            agg_or_select=args.agg_or_select,
            tbname_or_trows_or_sourcetable=args.tbname_or_trows_or_sourcetable,
            custom_columns=custom_columns,
            check_stream_delay=args.check_stream_delay,
            max_delay_threshold=args.max_delay_threshold,
            delay_check_interval=args.delay_check_interval,
            auto_combine=args.auto_combine,
            monitor_warm_up_time=args.monitor_warm_up_time
        )
        
        # 处理批量测试
        if args.batch_test:
            print("启动批量测试模式...")
            
            # 准备基础参数
            base_args = {
                'time': args.batch_test_time,
                'stream_perf_test_dir': args.stream_perf_test_dir,
                'table_count': args.table_count,
                'histroy_rows': args.histroy_rows,
                'real_time_batch_rows': args.real_time_batch_rows,
                'real_time_batch_sleep': args.real_time_batch_sleep,
                'disorder_ratio': args.disorder_ratio,
                'vgroups': args.vgroups,
                'monitor_interval': args.monitor_interval,
                'deployment_mode': args.deployment_mode,
                'debug_flag': args.debug_flag,
                'num_of_log_lines': args.num_of_log_lines,
                'max_delay_threshold': args.max_delay_threshold,
                'delay_check_interval': args.delay_check_interval,
                'stream_num': args.stream_num,
                'monitor_warm_up_time': args.monitor_warm_up_time  
            }
        
            print(f"批量测试基础配置:")
            print(f"  每个测试运行时间: {args.batch_test_time}分钟")
            print(f"  子表数量: {args.table_count}")
            print(f"  每轮写入记录数: {args.real_time_batch_rows}")
            print(f"  数据写入间隔: {args.real_time_batch_sleep}秒")
            print(f"  部署模式: {args.deployment_mode}")
            print(f"  过滤模式: {args.batch_filter_mode}")
            print(f"  流数量: {args.stream_num}") 
            print(f"  监控预热时间: {args.monitor_warm_up_time}秒") 
            
            # 处理SQL类型参数
            if args.batch_sql_types:
                print(f"  指定SQL类型: {', '.join(args.batch_sql_types)}")
                # 验证指定的SQL类型是否有效
                valid_types = [
                    'tbname_agg', 'tbname_select', 'trows_agg', 'trows_select', 
                    'sourcetable_agg', 'sourcetable_select',
                    'intervalsliding_detailed', 'sliding_detailed', 'session_detailed', 
                    'count_detailed', 'event_detailed', 'state_detailed', 'period_detailed',
                    'all_detailed',
                    'intervalsliding_stb', 'intervalsliding_stb_partition_by_tbname', 
                    'intervalsliding_stb_partition_by_tag', 'intervalsliding_tb',
                    'sliding_stb', 'sliding_stb_partition_by_tbname',
                    'sliding_stb_partition_by_tag', 'sliding_tb',
                    'session_stb', 'session_stb_partition_by_tbname',
                    'session_stb_partition_by_tag', 'session_tb',
                    'count_stb', 'count_stb_partition_by_tbname',
                    'count_stb_partition_by_tag', 'count_tb',
                    'event_stb', 'event_stb_partition_by_tbname',
                    'event_stb_partition_by_tag', 'event_tb',
                    'state_stb', 'state_stb_partition_by_tbname',
                    'state_stb_partition_by_tag', 'state_tb',
                    'period_stb', 'period_stb_partition_by_tbname',
                    'period_stb_partition_by_tag', 'period_tb'
                ]
                
                invalid_types = [t for t in args.batch_sql_types if t not in valid_types]
                if invalid_types:
                    print(f"错误: 无效的SQL类型: {', '.join(invalid_types)}")
                    print(f"有效类型请参考 --help 中的 --batch-sql-types 说明")
                    return
            else:
                print(f"  SQL类型: 全部组合 (默认)")
            
            # 创建批量测试器
            batch_tester = StreamBatchTester(
                base_args, 
                args.batch_sql_types,
                filter_mode=args.batch_filter_mode,
                single_template_mode=args.batch_single_template_mode
            )
            
            # 执行批量测试
            batch_tester.run_batch_tests(
                test_time_minutes=args.batch_test_time,
                start_from=args.batch_start_from,
                test_limit=args.batch_test_limit
            )
            
            return
        
        if args.create_data:
            print("\n=== 开始创建测试数据 ===")
            print(f"子表数量: {args.table_count}")
            print(f"每表记录数: {args.histroy_rows}")
            print(f"数据乱序率: {args.disorder_ratio}")
            print(f"vgroups数: {args.vgroups}\n")
            
            if starter.create_test_data():
                print("\n测试数据创建完成!")
            return
            
        if args.restore_data:
            print("\n=== 开始恢复测试数据 ===")
            if starter.restore_cluster_data():
                print("\n数据恢复完成!")
                
                # 如果指定了SQL类型且不是默认值，提示可以进行流计算测试
                if args.sql_type != 'select_stream' or args.stream_sql or args.sql_file:
                    print(f"\n提示: 数据已恢复，可以使用以下命令测试流计算:")
                    print(f"python3 {sys.argv[0]} -m 2 --sql-type {args.sql_type} --time {args.time}")
            return
        
        print("\n开始执行...")
        if args.mode == 1:
            print("执行模式: do_test_stream_with_realtime_data")
            starter.do_test_stream_with_realtime_data()
        elif args.mode == 3:
            print("执行模式: do_query_then_insert")
            starter.do_query_then_insert()
        elif args.mode == 2:
            print("执行模式: do_test_stream_with_restored_data (恢复数据并测试指定流计算)")
            starter.do_test_stream_with_restored_data()
        else:
            print(f"错误: 不支持的执行模式 {args.mode}")
            
    except KeyboardInterrupt:
        print("\n程序退出")
        # 检查taosd进程状态
        result = subprocess.run('ps -ef | grep "taosd -c" | grep -v grep', 
                                shell=True, capture_output=True, text=True)
        if result.stdout:
            print("\ntaosd进程仍在运行:")
            print(result.stdout)
        else:
            print("\n警告: 未找到运行中的taosd进程")
    except Exception as e:
        print(f"\n程序执行出错: {str(e)}")
    finally:
        print("\n如需手动停止taosd进程，请执行:")
        print("pkill taosd")

if __name__ == "__main__":
    main()
