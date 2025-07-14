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

"""TDengine Historical Data Stream Computing Performance Test
TDengine 历史数据流计算性能测试

Purpose/用途:
    Performance testing tool for TDengine historical data stream computing.
    用于测试 TDengine 历史数据流计算功能的性能测试工具。

Catalog/目录:
    - Performance:Stream Computing
    - 性能测试:历史数据流计算

Features/功能:
    - Multiple stream computing modes support
    - Load a large amount of historical data for calculation
    - System resource monitoring
    - Performance data collection
    
    - 支持多种流计算模式测试
    - 加载大量历史数据计算
    - 系统资源监控
    - 性能数据收集

Requirements/要求:
    Since: v3.3.7.0
    自版本: v3.3.7.0

Labels/标签: 
    performance, stream, testing

History/历史:
    - 2025-06-19 Initial commit
                 首次提交
    - 2025-06-20 Added resource monitoring
                 添加资源监控功能
    - 2025-06-21 Added multiple stream modes
                 增加多种流计算模式支持

Usage/用法:
    Basic Testing/基础测试:
        python3 stream_perf_1.py -m 2 --table-count 1000
    
    Force Trigger Mode/强制触发模式:
        python3 stream_perf_1.py -m 2 --sql-type s2_5
    
    Window Trigger Mode/窗口触发模式:
        python3 stream_perf_1.py -m 2 --sql-type s2_7

Parameters/参数:
    -m, --mode: Test mode / 测试模式
        1: Stream test / 写入数据并执行历史数据流计算
        11: Restore data and execute stream test / 恢复数据并执行历史数据流计算
        2: Query and insert test / 查询写入
        22: Restore data and execute query / 恢复数据并执行查询写入
    
    --table-count: Number of tables / 表数量
    --insert-rows: Number of rows per table / 每个表的行数
    --vgroups : Number of vgroups / vgroup数量
    --sql-type: Stream SQL type / 流式 SQL 类型
    --time: Runtime in minutes / 运行时间(分钟)
    --cluster-root : Cluster root directory / 集群根目录
    --monitor-interval: Monitoring interval in seconds / 监控间隔(秒)

Authors/作者:
    - Guo Xiangyang / 郭向阳
"""



class MonitorSystemLoad:

    def __init__(self, name_pattern, count, perf_file='/tmp/perf.log', use_signal=True, interval=1) -> None:
        """初始化系统负载监控
        
        Args:
            name_pattern: 进程名模式,例如 'taosd.*dnode1/conf'
            count: 监控次数
            perf_file: 性能数据输出文件
            interval: 性能采集间隔(秒),默认1秒
        """
        self.name_pattern = name_pattern  # 保存进程名模式
        self.count = count
        self.perf_file = perf_file
        self.interval = interval
        
        # 为每个dnode创建对应的性能文件句柄
        self.perf_files = {}
        for dnode in ['dnode1', 'dnode2', 'dnode3']:
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
        print("\n停止性能监控...")
        # 等待所有文件写入完成
        for f in self.perf_files.values():
            try:
                f.flush()
            except:
                pass
        time.sleep(5)
            
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
                    if dnode:
                        pids[dnode] = pid
                        print(f"找到 {dnode} 进程, PID: {pid}, 配置文件: {cfg_path}")
        
        if not pids:
            print(f"警告: 未找到匹配模式 '{self.name_pattern}' 的进程")
        else:
            print(f"共找到 {len(pids)} 个taosd进程")
        
        # 无论是否找到进程,都初始化所有dnode的文件句柄
        for dnode in ['dnode1', 'dnode2', 'dnode3']:
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
            while self.count > 0 and not self.stop_monitoring:
                start_time = time.time()
                
                sys_load = psutil.getloadavg()
                timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
                
                # 记录系统负载
                load_info = f"\n{timestamp} System Load: {sys_load[0]:.2f}\n"
                for f in self.perf_files.values():
                    f.write(load_info)
                print(load_info)
                
                # 收集进程指标
                for dnode in ['dnode1', 'dnode2', 'dnode3']:
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
                            self.write_zero_metrics(dnode, timestamp)
                            
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        self.write_zero_metrics(dnode, timestamp)
                        self.processes[dnode] = None
                    except Exception as e:
                        print(f"监控 {dnode} 出错: {str(e)}")
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
                
        except Exception as e:
            print(f"监控出错: {str(e)}")                

def do_monitor(runtime, perf_file):
    """监控线程函数"""
    try:
        # 不在子线程中使用信号处理
        loader = MonitorSystemLoad('taosd -c', runtime, perf_file, use_signal=False)
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

class StreamSQLTemplates:
    """流计算 SQL 模板集合"""
    
    s1_2 = """
    create stream s1_2 trigger at_once 
        fill_history 1 ignore expired 0 ignore update 0 into stream_to.stb1_2 
        as select _wstart as wstart,
        avg(c0), avg(c1),avg(c2), avg(c3),
        max(c0), max(c1), max(c2), max(c3),
        min(c0), min(c1), min(c2), min(c3)
        from stream_from.stb partition by tbname interval(300s);
    """
    
    s1_3 = """
    create stream s1_3 trigger window_close 
        fill_history 1 ignore expired 0 ignore update 0 into stream_to.stb1_3 
        as select _wstart as wstart,
        avg(c0), avg(c1),avg(c2), avg(c3),
        max(c0), max(c1), max(c2), max(c3),
        min(c0), min(c1), min(c2), min(c3)
        from stream_from.stb partition by tbname interval(300s);
    """
    
    s1_4 = """
    create stream s1_4 trigger continuous_window_close 
        fill_history 1 ignore expired 0 ignore update 0 into stream_to.stb1_4 
        as select _wstart as wstart,
        avg(c0), avg(c1),avg(c2), avg(c3),
        max(c0), max(c1), max(c2), max(c3),
        min(c0), min(c1), min(c2), min(c3)
        from stream_from.stb partition by tbname interval(300s);
    """
    
    s1_5 = """
    create stream stream_from.s1_5 INTERVAL(60s) SLIDING(60s)
        from stream_from.stb 
        stream_options(FILL_HISTORY('2025-01-01 00:00:00')) 
        into stream_to.stb1_5
        as select _twstart ts, avg(c0), avg(c1), avg(c2), avg(c3),
        max(c0), max(c1), max(c2), max(c3),
        min(c0), min(c1), min(c2), min(c3)
        from stream_from.stb partition by tbname interval(300s);
    """
    
    s1_6 = """
    create stream stream_from.s1_6 count_window(60) 
        from stream_from.stb partition by tbname 
        stream_options(FILL_HISTORY('2025-01-01 00:00:00')) 
        into stream_to.stb1_6
        as select _twstart ts, avg(c0), avg(c1), avg(c2), avg(c3),
        max(c0), max(c1), max(c2), max(c3),
        min(c0), min(c1), min(c2), min(c3)
        from stream_from.stb partition by tbname interval(300s);
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
            's1_2': cls.s1_2,
            's1_3': cls.s1_3,
            's1_4': cls.s1_4,
            's1_5': cls.s1_5,
            's1_6': cls.s1_6,
        }
        return sql_map.get(sql_type, cls.s1_2)  # 默认返回s1_2
    
class StreamStarter:
    def __init__(self, runtime=None, perf_file=None, table_count=500, 
                insert_rows=50000, disorder_ratio=0, vgroups=4,
                stream_sql=None, sql_type='s1_2', cluster_root=None, monitor_interval=1,
                create_data=False, restore_data=False) -> None:
        # 设置集群根目录,默认使用/home/taos_stream_cluster
        self.cluster_root = cluster_root if cluster_root else '/home/taos_stream_cluster'        
        self.table_count = table_count      # 子表数量
        self.insert_rows = insert_rows      # 插入记录数
        self.disorder_ratio = disorder_ratio # 数据乱序率
        self.vgroups = vgroups # vgroups数量
        self.monitor_interval = monitor_interval
        self.taosd_processes = [] 
        self.create_data = create_data
        self.restore_data = restore_data
        self.backup_dir = os.path.join(self.cluster_root, 'data_bak')
        # 定义3个实例的配置
        self.instances = [
            {
                'name': 'dnode1',
                'host': 'localhost',
                'port': 6030,
                'user': 'root',
                'passwd': 'taosdata',
                'data_dir': f'{self.cluster_root}/dnode1/data',
                'log_dir': f'{self.cluster_root}/dnode1/log',
            },
            {
                'name': 'dnode2',
                'host': 'localhost',
                'port': 7030,
                'user': 'root',
                'passwd': 'taosdata',
                'data_dir': f'{self.cluster_root}/dnode2/data',
                'log_dir': f'{self.cluster_root}/dnode2/log',
            },
            {
                'name': 'dnode3',
                'host': 'localhost',
                'port': 8030,
                'user': 'root',
                'passwd': 'taosdata',
                'data_dir': f'{self.cluster_root}/dnode3/data/',
                'log_dir': f'{self.cluster_root}/dnode3/log',
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
        self.conf = f"{self.cluster_root}/dnode1/conf/taos.cfg"
        self.tz = 'Asia/Shanghai'
        # 设置运行时间和性能文件路径
        self.runtime = runtime if runtime else 600  # 默认运行10分钟
        self.perf_file = perf_file if perf_file else '/tmp/perf.log'
        self.sql_type = sql_type
        self.stream_sql = stream_sql if stream_sql else StreamSQLTemplates.get_sql(sql_type)
        # 新增测试参数
        # # stream2
        # self.stream_sql = stream_sql if stream_sql else """
        # create stream s1_2 trigger at_once 
        #     fill_history 1 ignore expired 0 ignore update 0 into stream_to.stb 
        #     as select _wstart as wstart,
        #     avg(c0), avg(c1),avg(c2), avg(c3),
        #     max(c0), max(c1), max(c2), max(c3),
        #     min(c0), min(c1), min(c2), min(c3)
        #     from stream_from.stb partition by tbname interval(300s);
        # """
        # # stream3
        # self.stream_sql = stream_sql if stream_sql else """
        # create stream s1_3 trigger window_close 
        #     fill_history 1 ignore expired 0 ignore update 0 into stream_to.stb 
        #     as select _wstart as wstart,
        #     avg(c0), avg(c1),avg(c2), avg(c3),
        #     max(c0), max(c1), max(c2), max(c3),
        #     min(c0), min(c1), min(c2), min(c3)
        #     from stream_from.stb partition by tbname interval(300s);
        # """
        # stream4
        # self.stream_sql = stream_sql if stream_sql else """
        # create stream s1_4 trigger continuous_window_close 
        #     fill_history 1 ignore expired 0 ignore update 0 into stream_to.stb 
        #     as select _wstart as wstart,
        #     avg(c0), avg(c1),avg(c2), avg(c3),
        #     max(c0), max(c1), max(c2), max(c3),
        #     min(c0), min(c1), min(c2), min(c3)
        #     from stream_from.stb partition by tbname interval(300s);
        # """
        # # stream5
        # self.stream_sql = stream_sql if stream_sql else """
        # create stream stream_from.s1_5 INTERVAL(60s) SLIDING(60s)
        #     from stream_from.stb 
        #     stream_options(FILL_HISTORY('2025-01-01 00:00:00'))
        #     into stream_to.stb
        #     as select _twstart ts, avg(c0), avg(c1), avg(c2), avg(c3),
        #     max(c0), max(c1), max(c2), max(c3),
        #     min(c0), min(c1), min(c2), min(c3)
        #     from stream_from.stb  
        #     partition by tbname interval(300s);
        # """
        # stream6
        # self.stream_sql = stream_sql if stream_sql else """
        # create stream stream_from.s1_6 count_window(60) 
        #     from stream_from.stb 
        #     stream_options(FILL_HISTORY('2025-01-01 00:00:00'))
        #     into stream_to.stb
        #     as select _twstart ts, avg(c0), avg(c1), avg(c2), avg(c3),
        #     max(c0), max(c1), max(c2), max(c3),
        #     min(c0), min(c1), min(c2), min(c3)
        #     from stream_from.stb  
        #     partition by tbname interval(300s);
        # """
        # """
        #     create stream str1 trigger continuous_window_close 
        #     ignore expired 0 ignore update 0 
        #     into str1_dst 
        #     as select _wstart as wstart, min(c1), max(c2), count(c3) 
        #     from stream_from.stb interval(5s)
        # """

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
        """启动所有 taosd 进程"""
        try:
            print("\n=== 开始启动 taosd 进程 ===")
            for dnode in ['dnode1', 'dnode2', 'dnode3']:
                cfg_file = os.path.join(self.cluster_root, dnode, 'conf', 'taos.cfg')
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
            
            print("\n等待集群完全启动...")
            time.sleep(10)
            
            # 检查集群状态
            check_cluster_cmd = "taos -s 'show dnodes'"
            try:
                result = subprocess.run(check_cluster_cmd, shell=True, 
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    text=True)
                if result.returncode == 0:
                    print("\n集群状态:")
                    print(result.stdout)
                else:
                    print("警告: 无法获取集群状态")
            except Exception as e:
                print(f"检查集群状态时出错: {str(e)}")
                
        except Exception as e:
            print(f"启动 taosd 进程时出错: {str(e)}")
            return False
        return True
        
    def backup_cluster_data(self):
        """备份集群数据"""
        try:
            print("开始备份集群数据...")
            
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
            
            # 直接复制整个节点目录
            for dnode in ['dnode1', 'dnode2', 'dnode3']:
                src_dir = os.path.join(self.cluster_root, dnode)
                dst_dir = os.path.join(self.backup_dir, dnode)
                
                print(f"\n备份 {dnode} 目录...")
                try:
                    # 使用 rsync 排除 socket 文件
                    cmd = f'rsync -av --exclude="*.sock*" {src_dir}/ {dst_dir}/'
                    subprocess.run(cmd, shell=True, check=True)
                    print(f"完成备份: {dst_dir}")
                except subprocess.CalledProcessError:
                    # 如果 rsync 失败,使用 cp 命令
                    print(f"rsync 失败,使用 cp 命令备份...")
                    shutil.copytree(src_dir, dst_dir, symlinks=True, 
                                ignore=shutil.ignore_patterns('*.sock*'))
                    print(f"完成备份: {dst_dir}")
            
            print("\n=== 集群数据备份完成! ===")
            print(f"备份目录: {self.backup_dir}")
            return True
    
        except Exception as e:
            print(f"备份数据时出错: {str(e)}")
            return False
        finally:
            # 重新启动 taosd 进程
            if not self.start_taosd_processes():
                print("警告: taosd 进程启动失败")
        
    def restore_cluster_data(self):
        """从备份恢复集群数据"""
        try:
            if not os.path.exists(self.backup_dir):
                raise Exception(f"错误: 备份目录不存在: {self.backup_dir}")
                
            print("开始恢复集群数据...")
            
            # 停止现有taosd进程
            subprocess.run('pkill -15 taosd', shell=True)
            time.sleep(5)
            # 确保进程完全停止
            while subprocess.run('pgrep -x taosd', shell=True, stdout=subprocess.PIPE).stdout:
                print("等待 taosd 进程停止...")
                time.sleep(2)
                subprocess.run('pkill -9 taosd', shell=True)
            
            # 清理现有目录
            for dnode in ['dnode1', 'dnode2', 'dnode3']:
                cluster_dir = os.path.join(self.cluster_root, dnode)
                backup_dir = os.path.join(self.backup_dir, dnode)
                
                if not os.path.exists(backup_dir):
                    print(f"警告: 备份目录中未找到 {dnode}")
                    continue
                    
                print(f"\n还原 {dnode} 数据...")
                
                # 检查并显示备份目录结构
                print(f"备份目录结构 ({backup_dir}):")
                for root, dirs, files in os.walk(backup_dir):
                    print(f"- {os.path.relpath(root, backup_dir)}/")
                
                # 清理现有目录
                if os.path.exists(cluster_dir):
                    print(f"清理目录: {cluster_dir}")
                    shutil.rmtree(cluster_dir)
                
                # 复制整个目录
                try:
                    shutil.copytree(backup_dir, cluster_dir, symlinks=True)
                    print(f"完成还原: {cluster_dir}")
                except Exception as e:
                    print(f"还原 {dnode} 时出错: {str(e)}")
                    return False
            
            print("\n=== 集群数据还原完成! ===")
            
            # 重启 taosd 进程
            return self.start_taosd_processes()
            
        except Exception as e:
            print(f"\n还原数据时出错: {str(e)}")
            return False
        
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
            
            if not self.wait_for_data_ready(cursor,self.table_count, self.insert_rows):
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
        """
        try:
            # 停止已存在的taosd进程
            print("停止现有taosd进程")
            self.stop_taosd()
            
            # 检查并处理集群根目录
            if os.path.exists(self.cluster_root):
                print(f"清理已存在的集群目录: {self.cluster_root}")
                subprocess.run(f'rm -rf {self.cluster_root}/*', shell=True)
            else:
                print(f"创建新的集群目录: {self.cluster_root}")
                subprocess.run(f'mkdir -p {self.cluster_root}', shell=True)
            
                
            for instance in self.instances:
                # 创建必要的目录
                for dir_type in ['data', 'log', 'conf']:
                    dir_path = f"{self.cluster_root}/{instance['name']}/{dir_type}"
                    subprocess.run(f'mkdir -p {dir_path}', shell=True)
                
                # 清理数据目录
                data_dir = f"{self.cluster_root}/{instance['name']}/data"
                subprocess.run(f'rm -rf {data_dir}', shell=True)
                print(f"创建目录: {dir_path}")
                
                # 生成配置文件
                cfg_content = f"""
firstEP         localhost:6030
decondEP        localhost:7030
fqdn            localhost
serverPort      {instance['port']}
supportVnodes   50
dataDir         {instance['data_dir']}
logDir          {instance['log_dir']}
asyncLog        0
debugFlag       131
numOfLogLines   50000
"""
                cfg_file = f"{self.cluster_root}/{instance['name']}/conf/taos.cfg"
                
                # 使用 EOF 方式写入配置文件
                subprocess.run(f"""
cat << 'EOF' > {cfg_file}
{cfg_content}
EOF
""", shell=True)
            print("环境准备完成，配置文件已生成")
            
            
            # 启动所有taosd实例
            self.taosd_processes = []  
            for instance in self.instances:
                cfg_file = f"{self.cluster_root}/{instance['name']}/conf/taos.cfg"
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
            time.sleep(10)            
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
                
                # 执行集群配置命令
                cluster_cmds = [
                    'create dnode "localhost:7030"',
                    'create dnode "localhost:8030"',
                    'create mnode on dnode 2',
                    'create mnode on dnode 3',
                    'create snode on dnode 3'
                    # ,
                    # 'create snode on dnode 2',
                    # 'create snode on dnode 1'
                ]
                
                print("\n开始配置集群:")
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
                    for row in result:
                        print(f"ID: {row[0]}, endpoint: {row[1]}, role: {row[2]}, status: {row[3]}")
                    
                    # 查询 snodes 信息
                    print("\nSNode 信息:")
                    cursor.execute("show snodes")
                    result = cursor.fetchall()
                    for row in result:
                        print(f"ID: {row[0]}, endpoint: {row[1]}, create_time: {row[2]}")
                        
                except Exception as e:
                    print(f"查询集群信息失败: {str(e)}")
                
                print("-" * 50)
                
                # 关闭连接
                cursor.close()
                conn.close()
                
                print("集群配置完成")
                
            except Exception as e:
                print(f"集群配置失败: {str(e)}")
                raise
                
        except Exception as e:
            print(f"环境准备失败: {str(e)}")
            raise
        
    def prepare_source_from_data(self) -> dict:
        json_data = {
            "filetype": "insert",
            "cfgdir": f"{self.cluster_root}/dnode1/conf",
            "host": "localhost",
            "port": 6030,
            "rest_port": 6041,
            "user": "root",
            "password": "taosdata",
            "thread_count": 5,
            "create_table_thread_count": 5,
            "result_file": "/tmp/taosBenchmark_result.log",
            "confirm_parameter_prompt": "no",
            "insert_interval": 1000,
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
                        "stt_trigger": 1,
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
                            "interlace_rows": 400,
                            "tcp_transfer": "no",
                            "insert_rows": self.insert_rows,
                            "partial_col_num": 0,
                            "childtable_limit": 0,
                            "childtable_offset": 0,
                            "rows_per_tbl": 0,
                            "max_sql_len": 1024000,
                            "disorder_ratio": self.disorder_ratio,
                            "disorder_range": 1000,
                            "keep_trying": -1,
                            "timestamp_step": 1000,
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
                                    "len": 16
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
            
    def do_start(self):
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
            if not self.wait_for_data_ready(cursor, self.table_count, self.insert_rows):
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
            
    def do_start_restore_data(self):
        # 恢复数据
        if not self.restore_cluster_data():
            raise Exception("恢复集群数据失败")
        
        conn = taos.connect(
            host=self.host,
            user=self.user,
            password=self.passwd,
            config=self.conf
        )
        cursor = conn.cursor()

        # 创建stream_to数据库
        if not self.create_database('stream_to'):
            raise Exception("创建stream_to数据库失败")

        try:
            
            # 获取新连接执行流式查询
            conn, cursor = self.get_connection()
              
            #old            
            cursor.execute('drop snode on dnode 1')
            time.sleep(5)
            cursor.execute('create snode on dnode 3;')
            time.sleep(5)
            
            # #new         
            # cursor.execute('create snode on dnode 3;')
            # time.sleep(5)   
            # cursor.execute('drop snode on dnode 1')
            # time.sleep(5)
            
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
            
    def do_query_then_insert_restore_data(self):
        # 恢复数据
        if not self.restore_cluster_data():
            raise Exception("恢复集群数据失败")
        
        conn = taos.connect(
            host=self.host,
            user=self.user,
            password=self.passwd,
            config=self.conf
        )
        cursor = conn.cursor()

        # 创建stream_to数据库
        if not self.create_database('stream_to'):
            raise Exception("创建stream_to数据库失败")
        
        
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
                            step = 300 * 1000  # 5分钟间隔(毫秒)
                            
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
            
    def do_query_then_insert(self):
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
        if not self.wait_for_data_ready(cursor, self.table_count, self.insert_rows):
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
                            step = 300 * 1000  # 5分钟间隔(毫秒)
                            
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

    def do_query_then_insert_old(self):
        self.prepare_env()
        self.prepare_source_from_data()

        try:
            subprocess.Popen('taosBenchmark --f /tmp/stream_from.json', stdout=subprocess.PIPE, shell=True, text=True)
            subprocess.Popen('taosBenchmark --f /tmp/stream_to.json', stdout=subprocess.PIPE, shell=True, text=True)
        except subprocess.CalledProcessError as e:
            print(f"Error running Bash command: {e}")

        time.sleep(50)

        conn = taos.connect(
            host=self.host, user=self.user, password=self.passwd, config=self.conf, timezone=self.tz
        )

        cursor = conn.cursor()
        cursor.execute('use stream_from')

        start_ts = 1609430400000
        step = 5

        cursor.execute("create stable if not exists stb_result(wstart timestamp, minx float, maxx float, countx bigint) tags(gid bigint unsigned)")

        try:
            t = threading.Thread(target=do_monitor, args=(self.runtime * 60, self.perf_file))
            t.start()
        except Exception as e:
            print("Error: unable to start thread, %s" % e)

        print("start to query")

        list = get_table_list(cursor)
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
    
    parser = argparse.ArgumentParser(description='TDengine Stream Test')
    parser.add_argument('--create-data', action='store_true',
                        help='仅创建测试数据并备份')
    parser.add_argument('--restore-data', action='store_true',
                        help='从备份恢复测试数据')
    parser.add_argument('-m', '--mode', type=int, default=0,
                        help='1: do_start, 11: do_start_restore_data, 2: do_query_then_insert, 22: do_query_then_insert_restore_data 3: multi_insert')
    parser.add_argument('-t', '--time', type=int, default=10,
                        help='运行时间(分钟),默认10分钟')
    parser.add_argument('-f', '--file', type=str, default='/tmp/perf.log',
                        help='性能数据输出文件路径,默认/tmp/perf.log')
    parser.add_argument('--table-count', type=int, default=500,
                        help='子表数量,默认500')
    parser.add_argument('--insert-rows', type=int, default=500,
                        help='插入记录数,默认50000')
    parser.add_argument('--disorder-ratio', type=int, default=0,
                        help='数据乱序率,默认0')
    parser.add_argument('--vgroups', type=int, default=4,
                        help='vgroups,默认4')
    parser.add_argument('--sql-type', type=str, default='s1_2',
                       choices=['s1_2', 's1_3', 's1_4', 's1_5', 's1_6'],
                       help='实时流计算SQL-CASE-ID')
    parser.add_argument('--stream-sql', type=str,
                        help='自定义流计算SQL(优先级高于sql-type)')
    parser.add_argument('--sql-file', type=str,
                        help='从文件读取流式查询SQL')
    parser.add_argument('--cluster-root', type=str, default='/home/taos_stream_cluster',
                        help='集群根目录,默认/home/taos_stream_cluster')
    parser.add_argument('--monitor-interval', type=int, default=1,
                        help='性能数据采集间隔(秒),默认1秒')
    
    args = parser.parse_args()
    
    # 打印运行参数
    print("运行参数:")
    print(f"运行模式: {args.mode}")
    print(f"运行时间: {args.time}分钟")
    print(f"性能文件: {args.file}")
    print(f"子表数量: {args.table_count}")
    print(f"插入记录: {args.insert_rows}")
    print(f"数据乱序: {args.disorder_ratio}")
    print(f"vgroups数: {args.vgroups}")
    print(f"集群目录: {args.cluster_root}")
    print(f"性能数据采集间隔: {args.monitor_interval}秒")
    
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
    
    # 创建StreamStarter实例
    try:
        starter = StreamStarter(
            runtime=args.time,
            perf_file=args.file,
            table_count=args.table_count,
            insert_rows=args.insert_rows,
            disorder_ratio=args.disorder_ratio,
            vgroups=args.vgroups,
            stream_sql=stream_sql,
            sql_type=args.sql_type,
            cluster_root=args.cluster_root,
            monitor_interval=args.monitor_interval 
        )
        
        if args.create_data:
            print("\n=== 开始创建测试数据 ===")
            print(f"子表数量: {args.table_count}")
            print(f"每表记录数: {args.insert_rows}")
            print(f"数据乱序率: {args.disorder_ratio}")
            print(f"vgroups数: {args.vgroups}\n")
            
            if starter.create_test_data():
                print("\n测试数据创建完成!")
            return
            
        if args.restore_data:
            print("\n=== 开始恢复测试数据 ===")
            if starter.restore_cluster_data():
                print("\n数据恢复完成!")
            return
        
        print("\n开始执行...")
        if args.mode == 1:
            print("执行模式: do_start")
            starter.do_start()
        elif args.mode == 11:
            print("执行模式: do_start_restore_data")
            starter.do_start_restore_data()
        elif args.mode == 2:
            print("执行模式: do_query_then_insert")
            starter.do_query_then_insert()
        elif args.mode == 22:
            print("执行模式: do_query_then_insert_restore_data")
            starter.do_query_then_insert_restore_data()
        elif args.mode == 3:
            print("执行模式: multi_insert")
            starter.multi_insert()
            
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
