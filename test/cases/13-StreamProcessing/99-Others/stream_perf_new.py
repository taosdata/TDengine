import json
import subprocess
import threading
import psutil
import time
import taos
import argparse


class MonitorSystemLoad:

    def __init__(self, name, count, perf_file='/tmp/perf.log') -> None:
        self.name = name  # 保存进程名
        self.count = count
        self.perf_file = perf_file
        self.pid = self.get_pid_by_name(name)  # 获取进程ID
        if self.pid is None:
            raise ValueError(f"找不到进程名为 {name} 的进程")

    def get_pid_by_name(self, name):
        for proc in psutil.process_iter(['pid', 'name']):
            if proc.info['name'] == name:
                return proc.info['pid']
        return None

    def get_proc_status(self):
        if self.pid is None:
            print(f"无法找到进程 {self.name}")
            return
            
        process = psutil.Process(self.pid)
        
        with open(self.perf_file, 'w+') as f:
            while self.count > 0:
                try:
                    cpu_percent = process.cpu_percent(interval=1)
                    memory_info = process.memory_info()
                    memory_percent = process.memory_percent()
                    io_counters = process.io_counters()
                    sys_load = psutil.getloadavg()

                    s = "load: %.2f, CPU:%.1f, Mem:%.2fMiB, %.2f%%, Read: %.2fMiB, %d, Write: %.2fMib, %d" % (
                        sys_load[0], cpu_percent, memory_info.rss / 1048576.0,
                        memory_percent, io_counters.read_bytes / 1048576.0, io_counters.read_count,
                        io_counters.write_bytes / 1048576.0, io_counters.write_count)

                    print(s)
                    f.write(s + '\n')
                    f.flush()

                    time.sleep(1)
                    self.count -= 1
                except psutil.NoSuchProcess:
                    print(f"进程 {self.name}(PID:{self.pid}) 已终止")
                    break
                except Exception as e:
                    print(f"监控出错: {str(e)}")
                    break


def do_monitor():
    print("start monitor threads")
    loader = MonitorSystemLoad('taosd', 80000)
    loader.get_proc_status()

def get_table_list(cursor):
    cursor.execute('use stream_test')

    sql = "select table_name from information_schema.ins_tables where db_name = 'stream_test' and stable_name='stb' order by table_name"
    cursor.execute(sql)

    res = cursor.fetchall()
    return res

def do_multi_insert(index, total, host, user, passwd, conf, tz):
    conn = taos.connect(
        host=host, user=user, password=passwd, config=conf, timezone=tz
    )

    cursor = conn.cursor()
    cursor.execute('use stream_test')

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
            sql = (f"select cast({start_ts + step * 1000 * (count - 1)} as timestamp), min(c1), max(c2), count(c3) from stream_test.{n[0]} "
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
                 insert_rows=50000, disorder_ratio=0, vgroups=10,
                 stream_sql=None) -> None:
        self.sql = None
        self.host = 'localhost'
        self.user = 'root'
        self.passwd = 'taosdata'
        self.conf = '/etc/taos/taos.cfg'
        self.tz = 'Asia/Shanghai'
        # 设置运行时间和性能文件路径
        self.runtime = runtime if runtime else 600  # 默认运行10分钟
        self.perf_file = perf_file if perf_file else '/tmp/perf.log'
        # 新增测试参数
        self.table_count = table_count      # 子表数量
        self.insert_rows = insert_rows      # 插入记录数
        self.disorder_ratio = disorder_ratio # 数据乱序率
        self.vgroups = vgroups # vgroups数量
        self.stream_sql = stream_sql if stream_sql else """
            create stream str1 trigger continuous_window_close 
            ignore expired 0 ignore update 0 
            into str1_dst 
            as select _wstart as wstart, min(c1), max(c2), count(c3) 
            from stream_test.stb interval(5s)
        """

    def prepare_env(self):
        """
        清理环境并启动TDengine服务
        """
        try:
            # 停止已存在的taosd进程
            subprocess.run('pkill taosd', shell=True)
            print("停止现有taosd进程")
            
            # 等待进程完全停止
            time.sleep(20)
            
            # 清理/var/lib/taos
            subprocess.run('rm -rf /var/lib/taos/vnode', shell=True)
            subprocess.run('rm -rf /var/lib/taos/dnode', shell=True)
            subprocess.run('rm -rf /var/lib/taos/mnode', shell=True)
            time.sleep(2)
            
            # 启动taosd服务
            subprocess.Popen('nohup taosd > /dev/null 2>&1 &', shell=True)
            print("启动新的taosd服务")
            
            # 等待服务完全启动
            time.sleep(5)
            
            # 检查服务是否正常运行
            result = subprocess.run('ps aux | grep taosd | grep -v grep', 
                                 shell=True, capture_output=True, text=True)
            if result.stdout:
                print("TDengine服务已成功启动")
            else:
                raise Exception("TDengine服务启动失败")
                
        except Exception as e:
            print(f"环境准备失败: {str(e)}")
            raise
        
    def prepare_data(self) -> dict:
        json_data = {
            "filetype": "insert",
            "cfgdir": "/etc/taos/cfg",
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
                        "name": "stream_test",
                        "drop": "yes",
                        "replica": 1,
                        "duration": 10,
                        "precision": "ms",
                        "keep": 3650,
                        "minRows": 100,
                        "maxRows": 4096,
                        "comp": 2,
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
                            "start_timestamp": "2021-01-01 00:00:00",
                            "sample_format": "csv",
                            "sample_file": "./sample.csv",
                            "tags_file": "",
                            "columns": [
                                {
                                    "type": "INT",
                                    "count": 1
                                },
                                {
                                    "type": "TINYINT",
                                    "count": 0
                                },
                                {
                                    "type": "DOUBLE",
                                    "count": 0
                                },
                                {
                                    "type": "VARCHAR",
                                    "count": 0,
                                    "len": 16
                                },
                                {
                                    "type": "NCHAR",
                                    "count": 0,
                                    "len": 4
                                },
                                {
                                    "type": "SMALLINT",
                                    "count": 0
                                },
                                {
                                    "type": "BIGINT",
                                    "count": 0
                                },
                                {
                                    "type": "UTINYINT",
                                    "count": 0
                                },
                                {
                                    "type": "USMALLINT",
                                    "count": 0
                                },
                                {
                                    "type": "UINT",
                                    "count": 0
                                },
                                {
                                    "type": "UBIGINT",
                                    "count": 0
                                },
                                {
                                    "type": "FLOAT",
                                    "count": 2
                                },
                                {
                                    "type": "BINARY",
                                    "count": 0,
                                    "len": 8
                                },
                                {
                                    "type": "BOOL",
                                    "count": 0
                                },
                                {
                                    "type": "TIMESTAMP",
                                    "count": 1
                                }
                            ],
                            "tags": [
                                {
                                    "type": "INT",
                                    "count": 0
                                },
                                {
                                    "type": "TINYINT",
                                    "count": 1
                                },
                                {
                                    "type": "DOUBLE",
                                    "count": 0
                                },
                                {
                                    "type": "VARCHAR",
                                    "count": 0,
                                    "len": 8
                                },
                                {
                                    "type": "NCHAR",
                                    "count": 0,
                                    "len": 16
                                },
                                {
                                    "type": "SMALLINT",
                                    "count": 0
                                },
                                {
                                    "type": "BIGINT",
                                    "count": 0
                                },
                                {
                                    "type": "UTINYINT",
                                    "count": 0
                                },
                                {
                                    "type": "USMALLINT",
                                    "count": 0
                                },
                                {
                                    "type": "UINT",
                                    "count": 0
                                },
                                {
                                    "type": "UBIGINT",
                                    "count": 0
                                },
                                {
                                    "type": "FLOAT",
                                    "count": 0
                                },
                                {
                                    "type": "BINARY",
                                    "count": 1,
                                    "len": 16
                                },
                                {
                                    "type": "BOOL",
                                    "count": 0
                                },
                                {
                                    "type": "TIMESTAMP",
                                    "count": 0
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

        with open('/tmp/stream.json', 'w+') as f:
            json.dump(json_data, f, indent=4)

    def do_start(self):
        self.prepare_env()
        self.prepare_data()

        try:
            subprocess.Popen('taosBenchmark --f /tmp/stream.json', stdout=subprocess.PIPE, shell=True, text=True)
        except subprocess.CalledProcessError as e:
            print(f"Error running Bash command: {e}")

        conn = taos.connect(
            host=self.host, user=self.user, password=self.passwd, config=self.conf, timezone=self.tz
        )

        time.sleep(20)

        print("start to connect db")
        cursor = conn.cursor()

        cursor.execute('use stream_test')

        # # sql = "create stream str1 trigger continuous_window_close ignore expired 0 ignore update 0 into str1_dst as select _wstart as wstart, min(c1),max(c2), count(c3), tbname  from stream_test.stb partition by tbname interval(5s)"
        # sql = "create stream str1 trigger continuous_window_close ignore expired 0 ignore update 0 into str1_dst as select _wstart as wstart, min(c1),max(c2), count(c3)  from stream_test.stb interval(5s)"
        # cursor.execute(sql)
        
        print(f"执行流式查询SQL:\n{self.stream_sql}")
        cursor.execute(self.stream_sql)

        print("create stream completed, start to monitor system load")
        conn.close()

        loader = MonitorSystemLoad('taosd', self.runtime * 60, self.perf_file)
        loader.get_proc_status()

    def do_query_then_insert(self):
        self.prepare_env()
        self.prepare_data()

        try:
            subprocess.Popen('taosBenchmark --f /tmp/stream.json', stdout=subprocess.PIPE, shell=True, text=True)
        except subprocess.CalledProcessError as e:
            print(f"Error running Bash command: {e}")

        time.sleep(50)

        conn = taos.connect(
            host=self.host, user=self.user, password=self.passwd, config=self.conf, timezone=self.tz
        )

        cursor = conn.cursor()
        cursor.execute('use stream_test')

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
                sql = (f"select cast({start_ts + step * 1000 * (count - 1)} as timestamp), min(c1), max(c2), count(c3) from stream_test.{n[0]} "
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
        self.prepare_data()

        try:
            subprocess.Popen('taosBenchmark --f /tmp/stream.json', stdout=subprocess.PIPE, shell=True, text=True)
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
    parser = argparse.ArgumentParser(description='TDengine Stream Test')
    parser.add_argument('-m', '--mode', type=int, choices=[1,2,3], 
                      help='1: do_start, 2: do_query_then_insert, 3: multi_insert')
    parser.add_argument('-t', '--time', type=int, default=10,
                      help='运行时间(分钟),默认10分钟')
    parser.add_argument('-f', '--file', type=str, default='/tmp/perf.log',
                      help='性能数据输出文件路径,默认/tmp/perf.log')
    parser.add_argument('--table-count', type=int, default=500,
                      help='子表数量,默认500')
    parser.add_argument('--insert-rows', type=int, default=50000,
                      help='插入记录数,默认50000')
    parser.add_argument('--disorder-ratio', type=int, default=0,
                      help='数据乱序率,默认0')
    parser.add_argument('--vgroups', type=int, default=10,
                      help='vgroups,默认10')
    parser.add_argument('--stream-sql', type=str,
                      help='流式查询SQL,如果不指定则使用默认查询')
    parser.add_argument('--sql-file', type=str,
                      help='从文件读取流式查询SQL')
    
    args = parser.parse_args()
    
    # if not args.mode:
    #     parser.print_help()
    #     return
        
    # starter = StreamStarter(args.time, args.file)
    
    # 处理SQL参数
    stream_sql = None
    if args.sql_file:
        try:
            with open(args.sql_file, 'r') as f:
                stream_sql = f.read().strip()
        except Exception as e:
            print(f"读取SQL文件失败: {e}")
            return
    elif args.stream_sql:
        stream_sql = args.stream_sql
    
    starter = StreamStarter(
        runtime=args.time,
        perf_file=args.file,
        table_count=args.table_count,
        insert_rows=args.insert_rows,
        disorder_ratio=args.disorder_ratio,
        vgroups=args.vgroups,
        stream_sql=stream_sql
    )
    
    if args.mode == 1:
        starter.do_start()
    elif args.mode == 2:
        starter.do_query_then_insert()
    elif args.mode == 3:
        starter.multi_insert()

if __name__ == "__main__":
    main()
