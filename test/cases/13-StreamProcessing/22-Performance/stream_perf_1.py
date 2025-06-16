import json
import subprocess
import threading
import psutil
import time
import taos
import argparse
import os
import signal


class MonitorSystemLoad:

    def __init__(self, name_pattern, count, perf_file='/tmp/perf.log') -> None:
        """初始化系统负载监控
        
        Args:
            name_pattern: 进程名模式,例如 'taosd.*dnode1/conf'
            count: 监控次数
            perf_file: 性能数据输出文件
        """
        self.name_pattern = name_pattern  # 保存进程名模式
        self.count = count
        self.perf_file = perf_file
        self.stop_monitoring = False
        
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
        self.summary_file = f"{os.path.splitext(perf_file)[0]}-all.log"
        try:
            self.perf_files['all'] = open(self.summary_file, 'w+')
            print(f"创建汇总日志文件: {self.summary_file}")
        except Exception as e:
            print(f"创建汇总日志文件失败: {str(e)}")
            
        # 获取进程ID
        self.pids = self.get_pids_by_pattern()
        self.stop_monitoring = False
        # 注册信号处理器
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
        time.sleep(1)
            
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
        
    def get_proc_status(self):
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
                                cpu_percent = process.cpu_percent(interval=1)
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
                    
                    time.sleep(1)
                    self.count -= 1
                    
                    if self.stop_monitoring:
                        break
                        
                    time.sleep(1)
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


def do_monitor():
    print("start monitor threads")
    loader = MonitorSystemLoad('taosd', 80000)
    loader.get_proc_status()

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

class StreamStarter:
    def __init__(self, runtime=None, perf_file=None, table_count=500, 
                 insert_rows=50000, disorder_ratio=0, vgroups=4,
                 stream_sql=None, cluster_root=None) -> None:
        # 设置集群根目录,默认使用/root/taos_stream_cluster
        self.cluster_root = cluster_root if cluster_root else '/root/taos_stream_cluster'        
        self.table_count = table_count      # 子表数量
        self.insert_rows = insert_rows      # 插入记录数
        self.disorder_ratio = disorder_ratio # 数据乱序率
        self.vgroups = vgroups # vgroups数量
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
        # 新增测试参数
        self.stream_sql = stream_sql if stream_sql else """
        create stream stream_from.s1_6 count_window(60) 
            from stream_from.stb 
            OPTIONS(FILL_HISTORY('2025-01-01 00:00:00')) 
            into stream_to.stb
            as select _twstart ts, avg(c0), avg(c1), avg(c2), avg(c3),
            max(c0), max(c1), max(c2), max(c3),
            min(c0), min(c1), min(c2), min(c3)
            from stream_from.stb  
            partition by tbname interval(300s);
        """
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
        
    def prepare_env(self):
        """
        清理环境并启动TDengine服务
        """
        try:
            # 停止已存在的taosd进程
            subprocess.run('pkill taosd', shell=True)
            print("停止现有taosd进程")
            
            # 等待进程完全停止
            time.sleep(10)
            
            # 检查是否还有taosd进程
            result = subprocess.run('ps -ef | grep taosd | grep -v grep', 
                                shell=True, capture_output=True, text=True)
            if result.stdout:
                print("发现顽固进程，强制停止...")
                # 获取所有taosd进程的PID
                pids = []
                for line in result.stdout.splitlines():
                    parts = line.split()
                    if len(parts) > 1:
                        pids.append(parts[1])
                
                # 强制杀死每个进程
                for pid in pids:
                    print(f"强制终止进程 PID: {pid}")
                    subprocess.run(f'kill -9 {pid}', shell=True)
                
                # 同时关闭screen会话
                subprocess.run('pkill -9 SCREEN', shell=True)
                
            time.sleep(10)
            
            # 最后确认
            result = subprocess.run('ps -ef | grep taosd | grep -v grep', 
                                shell=True, capture_output=True, text=True)
            if result.stdout:
                raise Exception("无法停止所有taosd进程，请手动处理")
            
            print("所有taosd进程已停止")
        
            
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
            for instance in self.instances:
                cfg_file = f"{self.cluster_root}/{instance['name']}/conf/taos.cfg"
                cmd = f'nohup taosd -c {cfg_file} > /dev/null 2>&1 &'
                subprocess.Popen(cmd, shell=True)
                print(f"启动taosd实例: {instance['name']}")
            
            # 等待服务完全启动
            time.sleep(15)
            
            # 检查所有实例是否正常运行
            result = subprocess.run('ps aux | grep taosd | grep -v grep', 
                                 shell=True, capture_output=True, text=True)
            if result.stdout.count('taosd') == len(self.instances):
                print(f"所有 {len(self.instances)} 个TDengine实例已成功启动")
            else:
                raise Exception("部分TDengine实例启动失败")
            
            # 等待服务完全就绪
            time.sleep(5)
            
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
                    'create snode on dnode 1',
                    'create snode on dnode 2',
                    'create snode on dnode 3'
                ]
                
                print("\n开始配置集群:")
                for cmd in cluster_cmds:
                    try:
                        cursor.execute(cmd)
                        time.sleep(3)
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
            

    def do_start(self):
        self.prepare_env()
        self.prepare_source_from_data()

        try:
            # 运行source_from的数据生成
            subprocess.Popen('taosBenchmark --f /tmp/stream_from.json', 
                           stdout=subprocess.PIPE, shell=True, text=True)
            
            # 创建stream_to数据库
            if not self.create_database('stream_to'):
                raise Exception("创建stream_to数据库失败")
            
            time.sleep(20)
            print("数据库已创建,等待数据写入...")
            
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
                perf_file='/tmp/perf-taosd.log'  # 基础文件名,会自动添加dnode编号
            )
            
            # def local_signal_handler(signum, frame):
            #     """处理本地的中断信号"""
            #     loader.stop()  # 停止监控
            #     print("监控已停止，taosd进程保持运行")
            
            # # 注册本地信号处理器
            # original_handler = signal.getsignal(signal.SIGINT)
            # signal.signal(signal.SIGINT, local_signal_handler)
            
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
            

    def do_query_then_insert(self):
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
        
def do_monitor(runtime=600, perf_file='/tmp/perf.log'):
    print("start monitor threads")
    loader = MonitorSystemLoad('taosd', runtime, perf_file)
    loader.get_proc_status()
    
def main():
    def signal_handler(signum, frame):
        """主程序的信号处理器"""
        print("\n正在优雅退出程序...")
        # 检查taosd进程是否存在
        result = subprocess.run('ps -ef | grep taosd | grep -v grep', 
                             shell=True, capture_output=True, text=True)
        if result.stdout:
            print("taosd进程仍在运行")
            print("如需停止taosd进程，请手动执行: pkill taosd")
        # 强制退出程序
        os._exit(0)

    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    parser = argparse.ArgumentParser(description='TDengine Stream Test')
    parser.add_argument('-m', '--mode', type=int, choices=[1,2,3], default=1,
                      help='1: do_start(默认), 2: do_query_then_insert, 3: multi_insert')
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
    parser.add_argument('--stream-sql', type=str,
                      help='流式查询SQL,如果不指定则使用默认查询')
    parser.add_argument('--sql-file', type=str,
                      help='从文件读取流式查询SQL')
    parser.add_argument('--cluster-root', type=str, default='/root/taos_stream_cluster',
                      help='集群根目录,默认/root/taos_stream_cluster')
    
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
            cluster_root=args.cluster_root
        )
        
        print("\n开始执行...")
        if args.mode == 1:
            print("执行模式: do_start")
            starter.do_start()
        elif args.mode == 2:
            print("执行模式: do_query_then_insert")
            starter.do_query_then_insert()
        elif args.mode == 3:
            print("执行模式: multi_insert")
            starter.multi_insert()
            
    except KeyboardInterrupt:
        print("\n程序退出")
                
    except Exception as e:
        print(f"\n程序执行出错: {str(e)}")
        raise

if __name__ == "__main__":
    main()
