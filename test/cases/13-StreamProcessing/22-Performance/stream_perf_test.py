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
        - 支持滑动窗口、会话窗口、计数窗口、事件窗口、状态窗口、定时触发等6种窗口类型
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
    python3 stream_perf_test.py --stream-perf-test-dir /home/stream_perf_test_dir -m 3 --sql-type sliding_stb_partition_by_tbname --time 60
    
    # 多流并发测试
    python3 stream_perf_test.py --stream-perf-test-dir /home/stream_perf_test_dir -m 1 --sql-type sliding_stb --stream-num 100 --time 30
    
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

    def __init__(self, name_pattern, count, perf_file='/tmp/perf.log', use_signal=True, interval=1, deployment_mode='cluster') -> None:
        """初始化系统负载监控
        
        Args:
            name_pattern: 进程名模式,例如 'taosd.*dnode1/conf'
            count: 监控次数
            perf_file: 性能数据输出文件
            interval: 性能采集间隔(秒),默认1秒
            deployment_mode: 部署模式 'single' 或 'cluster'
        """
        self.name_pattern = name_pattern  # 保存进程名模式
        self.count = count
        self.perf_file = perf_file
        self.interval = interval
        self.deployment_mode = deployment_mode
        
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
            file_path = f"{os.path.splitext(perf_file)[0]}-{dnode}.log"
            try:
                self.perf_files[dnode] = open(file_path, 'w+')
                print(f"创建性能日志文件: {file_path}")
            except Exception as e:
                print(f"创建日志文件失败 {file_path}: {str(e)}")
                
        # 创建汇总日志文件
        try:
            all_file = f"{os.path.splitext(perf_file)[0]}-all.log"
            self.perf_files['all'] = open(all_file, 'w+')
            print(f"创建汇总日志文件: {all_file}")
        except Exception as e:
            print(f"创建汇总日志文件失败: {str(e)}")
            
        # 获取进程ID
        self.pids = self.get_pids_by_pattern()
        self.processes = {
            dnode: psutil.Process(pid) if pid else None
            for dnode, pid in self.pids.items()
        }
        for process in self.processes.values():
            if process:
                process.cpu_percent()
        self.stop_monitoring = False
        self._should_stop = False
        if use_signal and threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, self.signal_handler)
            signal.signal(signal.SIGTERM, self.signal_handler)

    def __del__(self):
        """确保所有文件都被正确关闭"""
        for f in self.perf_files.values():
            try:
                f.close()
            except:
                pass
            
    def stop(self):
        """提供外部停止监控的方法"""
        self.stop_monitoring = True
        self._should_stop = True
        print("\n停止性能监控...")
        # 等待所有文件写入完成
        for f in self.perf_files.values():
            try:
                f.flush()
            except:
                pass
            
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
    
    def write_metrics(self, dnode, status, timestamp=None):
        """写入性能指标
        
        Args:
            dnode: 节点名称
            status: 性能数据
            timestamp: 时间戳(可选)
        """
        # 写入单个节点的日志文件
        self.perf_files[dnode].write(status + '\n')
        
        # 同时写入汇总日志文件
        self.perf_files['all'].write(status + '\n')
        
        # 输出到控制台
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
    
    def write_zero_metrics(self, dnode, timestamp):
        """写入零值指标
        
        Args:
            dnode: 节点名称
            timestamp: 时间戳
        """
        status = (
            f"{timestamp} [{dnode}] "
            f"CPU: 0.0%, "
            f"Memory: 0.00MB (0.00%), "
            f"Read: 0.00MB (0), "
            f"Write: 0.00MB (0)"
        )
        self.perf_files[dnode].write(status + '\n')
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
        try:
            while self.count > 0 and not self.stop_monitoring and not self._should_stop:
                start_time = time.time()
                
                sys_load = psutil.getloadavg()
                timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                
                # 记录系统负载
                load_info = f"\n{timestamp} System Load: {sys_load[0]:.2f}\n"
                for f in self.perf_files.values():
                    f.write(load_info)
                print(load_info)
                
                # 收集进程指标
                for dnode in self.monitor_nodes:
                    process = self.processes.get(dnode)
                    try:
                        if process and process.is_running():
                            # 直接获取CPU使用率，不使用interval参数
                            cpu_percent = process.cpu_percent()
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
                            if dnode in self.pids or self.deployment_mode == 'cluster':
                                self.write_zero_metrics(dnode, timestamp)
                            
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        if dnode in self.pids or self.deployment_mode == 'cluster':
                            self.write_zero_metrics(dnode, timestamp)
                        self.processes[dnode] = None
                    except Exception as e:
                        print(f"监控 {dnode} 出错: {str(e)}")
                        if dnode in self.pids or self.deployment_mode == 'cluster':
                            self.write_zero_metrics(dnode, timestamp)
                
                # 添加分隔线
                separator = "-" * 80 + "\n"
                for f in self.perf_files.values():
                    f.write(separator)
                    f.flush()
                print(separator.strip())
                
                # 精确控制间隔时间
                elapsed = time.time() - start_time
                if elapsed < self.interval:
                    time.sleep(self.interval - elapsed)
                
                self.count -= 1
                
                # 检查是否需要停止
                if self.stop_monitoring or self._should_stop:
                    print("监控提前结束")
                    break
                
                # 使用分段sleep，以便能够及时响应停止信号
                elapsed = time.time() - start_time
                remaining_time = self.interval - elapsed
                
                if remaining_time > 0:
                    # 将大的间隔分解为小的片段，以便及时响应停止信号
                    sleep_chunks = max(1, int(remaining_time))  # 每次最多sleep 1秒
                    chunk_size = remaining_time / sleep_chunks
                    
                    for _ in range(sleep_chunks):
                        if self.stop_monitoring or self._should_stop:
                            print("在sleep期间收到停止信号")
                            break
                        time.sleep(chunk_size)
                
        except Exception as e:
            print(f"监控出错: {str(e)}")  
        finally:
            print("监控线程正在清理资源...")
            # 关闭所有文件
            for f in self.perf_files.values():
                try:
                    f.close()
                except:
                    pass
            print("监控线程已结束")              

def do_monitor(runtime, perf_file, deployment_mode='cluster'):
    """监控线程函数"""
    try:
        # 不在子线程中使用信号处理
        loader = MonitorSystemLoad(
            'taosd -c', 
            runtime, 
            perf_file, 
            use_signal=False,
            deployment_mode=deployment_mode
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
    
    def _build_from_clause(self, tbname_or_trows='tbname'):
        """构建 FROM 子句
        
        Args:
            tbname_or_trows: 'tbname' 或 'trows'
            
        Returns:
            str: FROM 子句字符串
        """
        if tbname_or_trows == 'trows':
            return "%%trows "
        else:
            return "%%tbname where ts >= _twstart and ts <= _twend"
    
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
    
    def _generate_stream_name(self, base_type, source_type, partition_type):
        """生成流名称
        
        Args:
            base_type: 基础类型 (如 's2_7', 'sliding')
            source_type: 数据源类型 ('stb', 'tb') 
            partition_type: 分区类型 ('none', 'tbname', 'tag')
            
        Returns:
            str: 生成的流名称
        """
        # 构建名称组件
        source_part = "stb" if source_type == "stb" else "tb"
        
        if partition_type == 'none':
            partition_part = ""
        elif partition_type == 'tbname':
            partition_part = "_tbname" 
        elif partition_type == 'tag':
            partition_part = "_tag"
        else:
            partition_part = ""
            
        return f"stream_from.{base_type}_{source_part}{partition_part}"
    
    def _generate_target_table(self, base_type, source_type, partition_type):
        """生成目标表名称"""
        source_part = "stb" if source_type == "stb" else "tb"
        
        if partition_type == 'none':
            partition_part = ""
        elif partition_type == 'tbname':
            partition_part = "_tbname"
        elif partition_type == 'tag':
            partition_part = "_tag"
        else:
            partition_part = ""
            
        return f"stream_to.{base_type}_{source_part}{partition_part}"
    
        
    # ========== 通用模板生成方法 ==========
    def get_sliding_template(self, source_type='stb', partition_type='tbname', 
                           agg_or_select='agg', tbname_or_trows='tbname', custom_columns=None, stream_index=None):
        """生成滑动窗口模板
        
        Args:
            source_type: 'stb'(超级表) 或 'tb'(子表)
            partition_type: 'none'(不分组), 'tbname'(按子表名), 'tag'(按tag)
            agg_or_select: 'agg' 或 'select'
            tbname_or_trows: 'tbname' 或 'trows'
            custom_columns: 自定义列
        """
        columns = self._build_columns(agg_or_select, custom_columns)
        from_clause = self._build_from_clause(tbname_or_trows)
        partition_clause = self._build_partition_clause(partition_type)
        from_source = self._build_from_source(source_type, stream_index) 
        stream_name = self._generate_stream_name('sliding', source_type, partition_type)
        target_table = self._generate_target_table('sliding', source_type, partition_type)
        
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
                           agg_or_select='agg', tbname_or_trows='tbname', custom_columns=None, stream_index=None):
        """生成会话窗口模板"""
        columns = self._build_columns(agg_or_select, custom_columns)
        from_clause = self._build_from_clause(tbname_or_trows)
        partition_clause = self._build_partition_clause(partition_type)
        from_source = self._build_from_source(source_type, stream_index) 
        stream_name = self._generate_stream_name('session', source_type, partition_type)
        target_table = self._generate_target_table('session', source_type, partition_type)
        
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
                         agg_or_select='agg', tbname_or_trows='tbname', custom_columns=None, stream_index=None):
        """生成计数窗口模板"""
        columns = self._build_columns(agg_or_select, custom_columns)
        from_clause = self._build_from_clause(tbname_or_trows)
        partition_clause = self._build_partition_clause(partition_type)
        from_source = self._build_from_source(source_type, stream_index) 
        stream_name = self._generate_stream_name('count', source_type, partition_type)
        target_table = self._generate_target_table('count', source_type, partition_type)
        
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
                         agg_or_select='agg', tbname_or_trows='tbname', custom_columns=None, stream_index=None):
        """生成事件窗口模板"""
        columns = self._build_columns(agg_or_select, custom_columns)
        from_clause = self._build_from_clause(tbname_or_trows)
        partition_clause = self._build_partition_clause(partition_type)
        from_source = self._build_from_source(source_type, stream_index) 
        stream_name = self._generate_stream_name('event', source_type, partition_type)
        target_table = self._generate_target_table('event', source_type, partition_type)
        
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
                         agg_or_select='agg', tbname_or_trows='tbname', custom_columns=None, stream_index=None):
        """生成状态窗口模板"""
        columns = self._build_columns(agg_or_select, custom_columns)
        from_clause = self._build_from_clause(tbname_or_trows)
        partition_clause = self._build_partition_clause(partition_type)
        from_source = self._build_from_source(source_type, stream_index) 
        stream_name = self._generate_stream_name('state', source_type, partition_type)
        target_table = self._generate_target_table('state', source_type, partition_type)
        
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
                          agg_or_select='agg', tbname_or_trows='tbname', custom_columns=None, stream_index=None):
        """生成定时触发模板 (period不支持tbname_or_trows)"""
        columns = self._build_columns(agg_or_select, custom_columns)
        from_clause = self._build_from_clause(tbname_or_trows)
        partition_clause = self._build_partition_clause(partition_type)
        from_source = self._build_from_source(source_type, stream_index) 
        stream_name = self._generate_stream_name('period', source_type, partition_type)
        target_table = self._generate_target_table('period', source_type, partition_type)
        
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
    
    # ========== 组合生成方法 ==========
    def get_sliding_group_detailed(self, **kwargs):
        """获取详细的滑动窗口组合 (4种组合)"""
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
        # period 不支持 tbname_or_trows 参数
        filtered_kwargs = {k: v for k, v in kwargs.items() if k != 'tbname_or_trows'}
        return {
            'period_stb': self.get_period_template(source_type='stb', partition_type='none', **filtered_kwargs),
            'period_stb_partition_by_tbname': self.get_period_template(source_type='stb', partition_type='tbname', **filtered_kwargs),
            'period_stb_partition_by_tag': self.get_period_template(source_type='stb', partition_type='tag', **filtered_kwargs),
            'period_tb': self.get_period_template(source_type='tb', partition_type='none', **filtered_kwargs)
        }
        
    def get_tbname_agg_group(self, **kwargs):
        """获取 tbname + agg 组合的所有窗口类型
        
        固定参数: tbname_or_trows='tbname', agg_or_select='agg'
        忽略命令行传入的这两个参数
        """
        # 固定参数，忽略传入的参数
        fixed_kwargs = {k: v for k, v in kwargs.items() if k not in ['tbname_or_trows', 'agg_or_select']}
        fixed_kwargs.update({
            'tbname_or_trows': 'tbname',
            'agg_or_select': 'agg'
        })
        
        return {
            # 滑动窗口 - 所有组合
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
        
        固定参数: tbname_or_trows='tbname', agg_or_select='select'
        忽略命令行传入的这两个参数
        """
        # 固定参数，忽略传入的参数
        fixed_kwargs = {k: v for k, v in kwargs.items() if k not in ['tbname_or_trows', 'agg_or_select']}
        fixed_kwargs.update({
            'tbname_or_trows': 'tbname',
            'agg_or_select': 'select'
        })
        
        return {
            # 滑动窗口 - 所有组合
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
        
        固定参数: tbname_or_trows='trows', agg_or_select='agg'
        忽略命令行传入的这两个参数
        """
        # 固定参数，忽略传入的参数
        fixed_kwargs = {k: v for k, v in kwargs.items() if k not in ['tbname_or_trows', 'agg_or_select']}
        fixed_kwargs.update({
            'tbname_or_trows': 'trows',
            'agg_or_select': 'agg'
        })
        
        return {
            # 滑动窗口 - 所有组合
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
            'period_stb_trows_agg': self.get_period_template(source_type='stb', partition_type='none', **{k: v for k, v in fixed_kwargs.items() if k != 'tbname_or_trows'}),
            'period_stb_partition_by_tbname_trows_agg': self.get_period_template(source_type='stb', partition_type='tbname', **{k: v for k, v in fixed_kwargs.items() if k != 'tbname_or_trows'}),
            'period_stb_partition_by_tag_trows_agg': self.get_period_template(source_type='stb', partition_type='tag', **{k: v for k, v in fixed_kwargs.items() if k != 'tbname_or_trows'}),
            'period_tb_trows_agg': self.get_period_template(source_type='tb', partition_type='none', **{k: v for k, v in fixed_kwargs.items() if k != 'tbname_or_trows'}),
        }

    def get_trows_select_group(self, **kwargs):
        """获取 trows + select 组合的所有窗口类型
        
        固定参数: tbname_or_trows='trows', agg_or_select='select'
        忽略命令行传入的这两个参数
        """
        # 固定参数，忽略传入的参数
        fixed_kwargs = {k: v for k, v in kwargs.items() if k not in ['tbname_or_trows', 'agg_or_select']}
        fixed_kwargs.update({
            'tbname_or_trows': 'trows',
            'agg_or_select': 'select'
        })
        
        return {
            # 滑动窗口 - 所有组合
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
            'period_stb_trows_select': self.get_period_template(source_type='stb', partition_type='none', **{k: v for k, v in fixed_kwargs.items() if k != 'tbname_or_trows'}),
            'period_stb_partition_by_tbname_trows_select': self.get_period_template(source_type='stb', partition_type='tbname', **{k: v for k, v in fixed_kwargs.items() if k != 'tbname_or_trows'}),
            'period_stb_partition_by_tag_trows_select': self.get_period_template(source_type='stb', partition_type='tag', **{k: v for k, v in fixed_kwargs.items() if k != 'tbname_or_trows'}),
            'period_tb_trows_select': self.get_period_template(source_type='tb', partition_type='none', **{k: v for k, v in fixed_kwargs.items() if k != 'tbname_or_trows'}),
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
    def get_s2_7(self, agg_or_select='agg', tbname_or_trows='tbname', custom_columns=None):
        """INTERVAL(15s) 窗口"""
        columns = self._build_columns(agg_or_select, custom_columns)
        from_clause = self._build_from_clause(tbname_or_trows)
        return f"""
    create stream stream_from.s2_7 INTERVAL(15s) SLIDING(15s)
            from stream_from.stb 
            partition by tbname 
            into stream_to.stb2_7
            as select _twstart ts, {columns}
            from {from_clause};
    """
    
    def get_s2_8(self, agg_or_select='agg', tbname_or_trows='trows', custom_columns=None):
        """INTERVAL with %%trows"""
        columns = self._build_columns(agg_or_select, custom_columns)
        from_clause = self._build_from_clause(tbname_or_trows)
        return f"""
    create stream stream_from.s2_8 INTERVAL(15s) SLIDING(15s)
            from stream_from.stb 
            partition by tbname 
            into stream_to.stb2_8
            as select _twstart ts, {columns}
            from {from_clause};
    """
    
    def get_s2_9(self, agg_or_select='agg', tbname_or_trows='tbname', custom_columns=None):
        """INTERVAL with MAX_DELAY"""
        columns = self._build_columns(agg_or_select, custom_columns)
        from_clause = self._build_from_clause(tbname_or_trows)
        return f"""
    create stream stream_from.s2_9 INTERVAL(15s) SLIDING(15s)
            from stream_from.stb partition by tbname 
            STREAM_OPTIONS(MAX_DELAY(5s)) 
            into stream_to.stb2_9
            as select _twstart ts, {columns}
            from {from_clause};
    """
    
    def get_s2_10(self, agg_or_select='agg', tbname_or_trows='trows', custom_columns=None):
        """INTERVAL with MAX_DELAY and %%trows"""
        columns = self._build_columns(agg_or_select, custom_columns)
        from_clause = self._build_from_clause(tbname_or_trows)
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
    
    def get_s2_12(self, agg_or_select='agg', tbname_or_trows='tbname', custom_columns=None):
        """SESSION(ts,10a) 会话窗口"""
        columns = self._build_columns(agg_or_select, custom_columns)
        from_clause = self._build_from_clause(tbname_or_trows)
        return f"""
    create stream stream_from.s2_12 session(ts,10a)
            from stream_from.stb partition by tbname 
            into stream_to.stb2_12
            as select _twstart ts, {columns}
            from {from_clause};
    """
    
    def get_s2_13(self, agg_or_select='agg', tbname_or_trows='tbname', custom_columns=None):
        """COUNT_WINDOW(1000) 计数窗口"""
        columns = self._build_columns(agg_or_select, custom_columns)
        from_clause = self._build_from_clause(tbname_or_trows)
        return f"""
    create stream stream_from.s2_13 COUNT_WINDOW(1000)
            from stream_from.stb partition by tbname 
            into stream_to.stb2_13
            as select _twstart ts, {columns}
            from {from_clause};
    """
    
    def get_s2_14(self, agg_or_select='agg', tbname_or_trows='tbname', custom_columns=None):
        """EVENT_WINDOW 事件窗口"""
        columns = self._build_columns(agg_or_select, custom_columns)
        from_clause = self._build_from_clause(tbname_or_trows)
        return f"""
    create stream stream_from.s2_14 EVENT_WINDOW(START WITH c0 > -10000000 END WITH c0 < 10000000) 
            from stream_from.stb partition by tbname 
            into stream_to.stb2_14
            as select _twstart ts, {columns}
            from {from_clause};
    """
    
    def get_s2_15(self, agg_or_select='agg', tbname_or_trows='tbname', custom_columns=None):
        """STATE_WINDOW 状态窗口"""
        columns = self._build_columns(agg_or_select, custom_columns)
        from_clause = self._build_from_clause(tbname_or_trows)
        return f"""
    create stream stream_from.s2_15 STATE_WINDOW(c0) 
            from stream_from.stb partition by tbname 
            into stream_to.stb2_15
            as select _twstart ts, {columns}
            from {from_clause};
    """
    
    # ========== 分组和批量获取方法 ==========
    def get_sliding_group(self, **kwargs):
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
        # period 类型不支持 tbname_or_trows 参数
        filtered_kwargs = {k: v for k, v in kwargs.items() if k != 'tbname_or_trows'}
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
        # 基础组不支持 tbname_or_trows 参数
        filtered_kwargs = {k: v for k, v in kwargs.items() if k != 'tbname_or_trows'}
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
        result.update(self.get_sliding_group(**kwargs))
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
    #             - tbname_or_trows: 'tbname'(默认) 或 'trows'，控制FROM子句
    #             - custom_columns: 自定义列，如果提供则使用自定义列
                
    #     Returns:
    #         str or dict: 单个SQL字符串或SQL字典
            
    #     Usage Examples:
    #         # 获取单个模板
    #         sql = StreamSQLTemplates.get_sql('s2_7')
            
    #         # 使用投影查询
    #         sql = StreamSQLTemplates.get_sql('s2_7', agg_or_select='select')
            
    #         # 使用trows
    #         sql = StreamSQLTemplates.get_sql('s2_8', tbname_or_trows='trows')
            
    #         # 自定义列
    #         sql = StreamSQLTemplates.get_sql('s2_7', custom_columns=['sum(c0)', 'count(*)'])
            
    #         # 获取分组
    #         sqls = StreamSQLTemplates.get_sql('sliding')  # 滑动窗口组
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
    #         'sliding': instance.get_sliding_group,
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
    #             # 基础模板和period模板不支持 tbname_or_trows
    #             filtered_kwargs = {k: v for k, v in kwargs.items() if k != 'tbname_or_trows'}
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
    def get_sql(cls, sql_type, stream_num=1, **kwargs):
        """
        获取指定类型的 SQL 模板 - 主要入口方法
        
        Args:
            sql_type: SQL 类型标识符或分组名
                单个模板: 'sliding_stb', 'sliding_stb_partition_by_tbname' 等
                组合模板: 'sliding_detailed', 'session_detailed' 等
            stream_num: 流数量（仅对单个流类型有效）
            **kwargs: 可选参数
                
        Returns:
            str or dict: 单个SQL字符串或SQL字典
            
        Usage Examples:
            # 获取单个详细模板
            sql = StreamSQLTemplates.get_sql('sliding_stb_partition_by_tbname')
            
            # 获取详细组合
            sqls = StreamSQLTemplates.get_sql('sliding_detailed')
            
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
        
        # 单个详细模板映射
        detailed_templates = {
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
            'sliding_detailed': instance.get_sliding_group_detailed,
            'session_detailed': instance.get_session_group_detailed,
            'count_detailed': instance.get_count_group_detailed,
            'event_detailed': instance.get_event_group_detailed,
            'state_detailed': instance.get_state_group_detailed,
            'period_detailed': instance.get_period_group_detailed,
        }
        
        # # 处理单个详细模板
        # if sql_type in detailed_templates:
        #     return detailed_templates[sql_type](**kwargs)
        
        # # 处理组合模板
        # if sql_type in group_templates:
        #     return group_templates[sql_type](**kwargs)
        
        # # 处理单个详细模板
        # if sql_type in detailed_templates:
        #     base_sql = detailed_templates[sql_type](**kwargs)
            
        #     # 如果stream_num > 1，生成多个流
        #     if stream_num > 1:
        #         print(f"生成 {stream_num} 个 {sql_type} 类型的流")
        #         return instance.generate_multiple_streams(base_sql, stream_num)
        #     else:
        #         return base_sql
        
        # 处理单个详细模板时
        if sql_type in detailed_templates:
            # 如果stream_num > 1，需要为每个流生成不同的SQL（特别是tb类型）
            if stream_num > 1:
                print(f"生成 {stream_num} 个 {sql_type} 类型的流")
                result = {}
                
                for i in range(1, stream_num + 1):
                    # 为每个流传入stream_index参数
                    stream_sql = detailed_templates[sql_type](stream_index=i, **kwargs)
                    result[f'stream_{i}'] = stream_sql
                    
                return result
            else:
                return detailed_templates[sql_type](**kwargs)
        
        # # 处理组合模板
        # if sql_type in group_templates:
        #     if stream_num > 1:
        #         print("警告: 组合模板不支持 --stream-num 参数，将忽略该参数")
        #     return group_templates[sql_type](**kwargs)
        
        # # 处理特殊组合
        # if sql_type == 'all_detailed':
        #     if stream_num > 1:
        #         print("警告: all_detailed 模板不支持 --stream-num 参数，将忽略该参数")
        #     result = {}
        #     for group_type in ['sliding_detailed', 'session_detailed', 'count_detailed', 
        #                     'event_detailed', 'state_detailed', 'period_detailed']:
        #         result.update(group_templates[group_type](**kwargs))
        #     return result
        
        # 处理组合模板（支持 stream_num）
        if sql_type in group_templates:
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
                        # 例如: sliding_stb_1, sliding_stb_2, sliding_stb_partition_by_tbname_1, etc.
                        new_key = f"{base_name}_{stream_key.split('_')[-1]}"  # 提取编号
                        result[new_key] = stream_sql
                
                return result
            else:
                return base_sqls
        
        # 处理特殊组合
        if sql_type == 'all_detailed':
            result = {}
            for group_type in ['sliding_detailed', 'session_detailed', 'count_detailed', 
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
        
        
class StreamStarter:
    def __init__(self, runtime=None, perf_file=None, table_count=500, 
                histroy_rows=1, real_time_batch_rows=200, disorder_ratio=0, vgroups=10,
                stream_sql=None, sql_type='select_stream', stream_num=1, stream_perf_test_dir=None, monitor_interval=1,
                create_data=False, restore_data=False, deployment_mode='single',
                debug_flag=131, num_of_log_lines=500000, 
                agg_or_select='agg', tbname_or_trows='tbname', custom_columns=None,
                check_stream_delay=False, max_delay_threshold=30000, delay_check_interval=10,
                real_time_batch_sleep=0) -> None:
        
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
        
        self.sql_type = sql_type
        self.stream_num = stream_num
        self.agg_or_select = agg_or_select
        self.tbname_or_trows = tbname_or_trows
        self.custom_columns = custom_columns
        print(f"调试信息: tbname_or_trows = {tbname_or_trows}")
        print(f"调试信息: sql_type = {sql_type}")
        print(f"调试信息: agg_or_select = {agg_or_select}")
        print(f"调试信息:数据写入间隔: {real_time_batch_sleep}秒")
        self.stream_sql = stream_sql if stream_sql else StreamSQLTemplates.get_sql(
            sql_type, 
            stream_num=stream_num, 
            agg_or_select=agg_or_select,
            tbname_or_trows=tbname_or_trows,
            custom_columns=custom_columns
        )
        #print(f"生成的SQL:\n{self.stream_sql}")
        
        self.check_stream_delay = check_stream_delay
        self.max_delay_threshold = max_delay_threshold  # 毫秒
        self.delay_check_interval = delay_check_interval  # 秒
        self.delay_log_file = f"{os.path.splitext(perf_file)[0]}-stream-delay.log"
        
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
            wait_time = 5 if self.deployment_mode == 'single' else 10
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
                for sql_name, sql_template in sql_templates.items():
                    try:
                        print(f"\n创建流 {sql_name}:")
                        print(sql_template)
                        cursor.execute(sql_template)
                        print(f"流 {sql_name} 创建成功")
                    except Exception as e:
                        print(f"创建流 {sql_name} 失败: {str(e)}")
            else:
                # 单个流的创建
                print("\n=== 开始创建流 ===")
                print("执行流计算SQL:")
                print("-" * 60)
                print(sql_templates)
                print("-" * 60)
                
                start_time = time.time()
                cursor.execute(sql_templates)
                create_time = time.time() - start_time
                
                print(f"流创建完成! 耗时: {create_time:.2f}秒")
            
            # 启动系统监控
            print("\n开始监控系统资源使用情况...")
            loader = MonitorSystemLoad(
                name_pattern='taosd -c', 
                count=self.runtime * 60,
                perf_file='/tmp/perf-stream-test.log',
                interval=self.monitor_interval,
                deployment_mode=self.deployment_mode
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
            wait_time = 5 if self.deployment_mode == 'single' else 10
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
                if self.sql_type == 's2_5' or self.sql_type == 's2_11':
                    next_start_time = time.strftime("%Y-%m-%d %H:%M:%S")
                    print(f"流类型为{self.sql_type}，使用当前时间作为起始时间: {next_start_time}")
                
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
            create stream qdb.s18  interval(58s) sliding(11m, 4m)  
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
                        
                        # # 尝试查看目标数据库中的所有表
                        # target_db = table_info['target_db']
                        # try:
                        #     cursor.execute(f"show tables in {target_db}")
                        #     tables_in_db = cursor.fetchall()
                        #     print(f"数据库 {target_db} 中的表: {[t[0] for t in tables_in_db]}")
                        # except Exception as db_e:
                        #     print(f"无法查看数据库 {target_db}: {str(db_e)}")
                        
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
            with open(self.delay_log_file, 'a') as f:
                # 写入检查时间和源表信息
                f.write(f"\n{'='*80}\n")
                f.write(f"检查时间: {delay_results['check_time']}\n")
                f.write(f"源表最新时间: {delay_results['source_last_ts']}\n")
                f.write(f"源表时间戳(ms): {delay_results['source_ts_ms']}\n")
                f.write(f"延迟阈值: {format_delay_time(self.max_delay_threshold)}\n")
                f.write(f"-" * 80 + "\n")
                
                # 写入每个流的延迟信息
                for stream in delay_results['streams']:
                    f.write(f"流名称: {stream['stream_name']}\n")
                    f.write(f"目标表: {stream['target_table']}\n")
                    f.write(f"状态: {stream['status']}\n")
                    
                    if stream['status'] == 'OK' or stream['status'] == 'LAGGING':
                        f.write(f"目标表最新时间: {stream['target_last_ts']}\n")
                        f.write(f"延迟: {format_delay_time(stream['delay_ms'])}\n")
                        if stream['is_lagging']:
                            f.write(f"警告: 延迟超过阈值!\n")
                    elif stream['status'] == 'NO_DATA':
                        f.write(f"警告: 目标表无数据\n")
                    elif stream['status'] == 'ERROR':
                        f.write(f"错误: {stream.get('error', '未知错误')}\n")
                    
                    f.write(f"-" * 40 + "\n")
                    
        except Exception as e:
            print(f"写入延迟日志失败: {str(e)}")

    def print_stream_delay_summary(self, delay_results):
        """打印流延迟检查摘要"""
        if not delay_results:
            return
        
        c = Colors.get_colors()
        
        # 基于 delay_check_interval 计算动态阈值
        check_interval_ms = self.delay_check_interval * 1000  # 转换为毫秒
        
        # 定义倍数阈值
        excellent_threshold = check_interval_ms * 0.1    # 0.1倍检查间隔 - 优秀
        good_threshold = check_interval_ms * 0.5         # 0.5倍检查间隔 - 良好  
        normal_threshold = check_interval_ms * 1.0       # 1倍检查间隔 - 正常
        mild_delay_threshold = check_interval_ms * 6.0   # 6倍检查间隔 - 轻微延迟
        obvious_delay_threshold = check_interval_ms * 30.0  # 30倍检查间隔 - 明显延迟
            
        print(f"\n=== 流计算延迟检查 ({delay_results['check_time']}) ===")
        print(f"源表最新时间: {delay_results['source_last_ts']}")
        print(f"配置信息: 子表数量({self.table_count}) | 每轮插入记录数({self.real_time_batch_rows}) | vgroups({self.vgroups}) | 数据乱序({self.disorder_ratio})")
        print(f"延迟判断基准: 检查间隔 {self.delay_check_interval}s | 部署模式({self.deployment_mode}) | SQL类型({self.sql_type})")
            
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
                status_icon = "❌" if Colors.supports_color() else "✗"
                print(f"{c.RED}{status_icon} {stream_name}: 目标表未生成数据{c.END}")
                
            elif status == 'ERROR':
                error_count += 1
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
        print(f"\n{c.CYAN}延迟等级参考 (基于检查间隔 {self.delay_check_interval}s):{c.END}")
        print(f"  🟢 优秀: < {format_delay_time(excellent_threshold)} (0.1倍间隔)")
        print(f"  🟢 良好: {format_delay_time(excellent_threshold)} - {format_delay_time(good_threshold)} (0.1-0.5倍间隔)")
        print(f"  🟡 正常: {format_delay_time(good_threshold)} - {format_delay_time(normal_threshold)} (0.5-1倍间隔)")
        print(f"  🟡 轻微延迟: {format_delay_time(normal_threshold)} - {format_delay_time(mild_delay_threshold)} (1-6倍间隔)")
        print(f"  🟠 明显延迟: {format_delay_time(mild_delay_threshold)} - {format_delay_time(obvious_delay_threshold)} (6-30倍间隔)")
        print(f"  🔴 严重延迟: > {format_delay_time(obvious_delay_threshold)} (>30倍间隔)")
        
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
            with open(self.delay_log_file, 'a') as f:
                f.write(f"\n{final_msg}\n")
                f.write(f"结束时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        # 创建并启动监控线程
        monitor_thread = threading.Thread(target=delay_monitor, name="StreamDelayMonitor")
        monitor_thread.daemon = True
        monitor_thread.start()
        
        return monitor_thread
            
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
            
            # 获取新连接执行流式查询
            conn, cursor = self.get_connection()
            
            print("开始连接数据库")
            cursor.execute('use stream_from')
            
            # # 执行流式查询
            # print(f"执行流式查询SQL:\n{self.stream_sql}")
            # cursor.execute(self.stream_sql)
            
            # 获取 SQL 模板
            #sql_templates = StreamSQLTemplates.get_sql(self.sql_type)
            sql_templates = self.stream_sql 
            
            # 判断是否为批量执行
            if isinstance(sql_templates, dict):
                print("\n=== 开始批量创建流 ===")
                for sql_name, sql_template in sql_templates.items():
                    try:
                        print(f"\n创建流 {sql_name}:")
                        print(sql_template)
                        cursor.execute(sql_template)
                        print(f"流 {sql_name} 创建成功")
                    except Exception as e:
                        print(f"创建流 {sql_name} 失败: {str(e)}")
            else:
                # 单个流的创建
                print("\n=== 开始创建流 ===")
                print("执行流式查询SQL:")
                print(sql_templates)
                cursor.execute(sql_templates)
                print("流创建成功")
            
            print("流式查询已创建,开始监控系统负载")
            cursor.close()
            conn.close()
            
            # 监控系统负载 - 同时监控三个节点
            loader = MonitorSystemLoad(
                name_pattern='taosd -c', 
                count=self.runtime * 60,
                perf_file='/tmp/perf-taosd.log',  # 基础文件名,会自动添加dnode编号
                interval=self.monitor_interval,
                deployment_mode=self.deployment_mode 
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
                        
                        # cmd = "taosBenchmark -f /tmp/stream_from_insertdata.json"
                        # if subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True, text=True).returncode != 0:
                        #     raise Exception("写入新数据失败")
                        
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
                
        except Exception as e:
            print(f"执行错误: {str(e)}")            


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
            loader = MonitorSystemLoad(
                name_pattern='taosd -c', 
                count=self.runtime * 60,
                perf_file='/tmp/perf-taosd-query.log',
                interval=self.monitor_interval,
                deployment_mode=self.deployment_mode 
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
示例用法:%(prog)s --table-count 10000 --histroy-rows 1000 \n --real-time-batch-rows 500 --real-time-batch-sleep 5 --disorder-ratio 1 --vgroups 20 --sql-type sliding_stb --time 60\n\n''')
    
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
            'sliding_stb', 'sliding_stb_partition_by_tbname', 'sliding_stb_partition_by_tag', 'sliding_tb',
            'session_stb', 'session_stb_partition_by_tbname', 'session_stb_partition_by_tag', 'session_tb',
            'count_stb', 'count_stb_partition_by_tbname', 'count_stb_partition_by_tag', 'count_tb',
            'event_stb', 'event_stb_partition_by_tbname', 'event_stb_partition_by_tag', 'event_tb',
            'state_stb', 'state_stb_partition_by_tbname', 'state_stb_partition_by_tag', 'state_tb',
            'period_stb', 'period_stb_partition_by_tbname', 'period_stb_partition_by_tag', 'period_tb',
            'sliding_detailed', 'session_detailed', 'count_detailed',
            'event_detailed', 'state_detailed', 'period_detailed'
        ]
        
        if value not in valid_choices:
            # 按类别格式化错误信息
            error_msg = f"invalid choice: '{value}'\n\n有效选择项:\n"
            error_msg += "说明: stb(超级表), tb(子表), by_tbname(tbname分组), by_tag(tag分组,第一列tag)\n"
            error_msg += "查询类型: select_stream 查询所有流信息\n"
            error_msg += "固定参数组合: tbname_agg, tbname_select, trows_agg, trows_select\n"
            error_msg += "滑动窗口: sliding_stb, sliding_stb_partition_by_tbname, sliding_stb_partition_by_tag, sliding_tb\n"
            error_msg += "会话窗口: session_stb, session_stb_partition_by_tbname, session_stb_partition_by_tag, session_tb\n"
            error_msg += "计数窗口: count_stb, count_stb_partition_by_tbname, count_stb_partition_by_tag, count_tb\n"
            error_msg += "事件窗口: event_stb, event_stb_partition_by_tbname, event_stb_partition_by_tag, event_tb\n"
            error_msg += "状态窗口: state_stb, state_stb_partition_by_tbname, state_stb_partition_by_tag, state_tb\n"
            error_msg += "定时触发: period_stb, period_stb_partition_by_tbname, period_stb_partition_by_tag, period_tb\n"
            error_msg += "组合模板: sliding_detailed, session_detailed, count_detailed, event_detailed, state_detailed, period_detailed, all_detailed"
            
            raise argparse.ArgumentTypeError(error_msg)
        return value

    # 使用时不设置 choices，而是使用 type 参数进行验证
    sql_group.add_argument('--sql-type', type=validate_sql_type, default='select_stream',
                        help='''实时流计算SQL类型:
    说明:
        stb(超级表), tb(子表), by_tbname(tbname分组), by_tag(tag分组,第一列tag)
    查询类型:
        select_stream: 查询所有流信息 (默认查询information_schema.ins_streams的数据)
    固定参数组合模板(忽略 --tbname-or-trows 和 --agg-or-select 参数):
        tbname_agg:    所有窗口类型 + tbname + 聚合查询
        tbname_select: 所有窗口类型 + tbname + 投影查询  
        trows_agg:     所有窗口类型 + trows +  聚合查询
        trows_select:  所有窗口类型 + trows +  投影查询
    滑动窗口模板(INTERVAL(15s) SLIDING(15s)):
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
        sliding_detailed, session_detailed, count_detailed, event_detailed, 
        state_detailed, period_detailed, all_detailed''')
    
    sql_group.add_argument('--stream-num', type=int, default=1,
                        help='流数量: 当 sql-type 为非组合模版时，创建指定数量的相同流\n'
                                '    并发流数量, 默认1个流\n'
                                '    设置为N时，会创建N个编号为1到N的相同流，如果是从子表获取数据则从第一个子表到第N个子表\n'
                                '    需要和 --sql-type 搭配使用，适合测试多个流的压力\n'
                                '    示例: --sql-type sliding_stb --stream-num 100')
    
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
    sql_group.add_argument('--tbname-or-trows', type=str, default='tbname',
                        choices=['tbname', 'trows'],
                        help='FROM子句类型，默认%%tbname:\n'
                            '    tbname: from %%tbname where ts >= _twstart and ts <= _twend\n'
                            '    trows:  from %%trows ')
    
    # todo
    sql_group.add_argument('--sql-file', type=str,
                        help='从文件读取流式查询SQL，todo\n'
                            '''    示例: --sql-file ./my_stream.sql\n
示例用法1:%(prog)s --table-count 10000 \n --real-time-batch-rows 500 --real-time-batch-sleep 5 --sql-type sliding_stb --time 60\n
示例用法2:%(prog)s --table-count 10000 \n --real-time-batch-rows 500 --real-time-batch-sleep 5 --sql-type sliding_detailed --time 60\n
示例用法3:%(prog)s --table-count 10000 \n --real-time-batch-rows 500 --real-time-batch-sleep 5 --sql-type tbname_agg --time 60\n
示例用法4:%(prog)s --table-count 10000 \n --real-time-batch-rows 500 --real-time-batch-sleep 5 --sql-type sliding_detailed --tbname-or-trows trows\n
示例用法5:%(prog)s --table-count 10000 \n --real-time-batch-rows 500 --real-time-batch-sleep 5 --sql-type sliding_detailed --agg-or-select select\n
示例用法6:%(prog)s --table-count 10000 \n --real-time-batch-rows 500 --real-time-batch-sleep 5 --sql-type sliding_stb--stream-num 100\n\n''')
    
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
示例用法:%(prog)s --check-stream-delay \n--max-delay-threshold 60000 --delay-check-interval 5 --sql-type sliding_stb \n\n''')
    
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
示例用法:%(prog)s --create-data 
--deployment-mode single --table-count 500 --histroy-rows 10000000\n\n''')
    
    data_mgmt_group.add_argument('--restore-data', action='store_true',
                                help='''从备份恢复测试数据: 
执行流程:
  1. 检查备份数据完整性
  2. 停止现有TDengine服务
  3. 恢复数据文件
  4. 重启服务并验证
适用: 快速恢复到已知数据状态，避免重复数据生成，节省时间\n如果测试历史数据进行流计算，建议用-m 2模式\n
示例用法:%(prog)s --restore-data --deployment-mode single''')
    
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
            tbname_or_trows=args.tbname_or_trows,
            custom_columns=custom_columns,
            check_stream_delay=args.check_stream_delay,
            max_delay_threshold=args.max_delay_threshold,
            delay_check_interval=args.delay_check_interval 
        )
        
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
