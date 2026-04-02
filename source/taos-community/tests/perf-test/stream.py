import json
import subprocess
import threading
import psutil
import time
import taos


class MonitorSystemLoad:

    def __init__(self, name, count) -> None:
        self.pid = self.get_pid_by_name(name)
        self.count = count

    def get_pid_by_name(self, name):
        for proc in psutil.process_iter(['pid', 'name']):
            if proc.info['name'] == name:
                return proc.info['pid']
        return None

    def get_proc_status(self):
        process = psutil.Process(self.pid)

        with open('/tmp/pref.txt', 'w+') as f:
            while True:
                cpu_percent = process.cpu_percent(interval=1)

                memory_info = process.memory_info()
                memory_percent = process.memory_percent()

                io_counters = process.io_counters()
                sys_load = psutil.getloadavg()

                s = "load: %.2f, CPU:%s, Mem:%.2fMiB, %.2f%%, Read: %.2fMiB, %d, Write: %.2fMib, %d" % (
                    sys_load[0], cpu_percent, memory_info.rss / 1048576.0,
                    memory_percent, io_counters.read_bytes / 1048576.0, io_counters.read_count,
                    io_counters.write_bytes / 1048576.0, io_counters.write_count)

                print(s)
                f.write(s + '\n')
                f.flush()

                time.sleep(1)

                self.count -= 1
                if self.count <= 0:
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
    def __init__(self) -> None:
        self.sql = None
        self.host='localhost'
        self.user = 'root'
        self.passwd = 'taosdata'
        self.conf = '/etc/taos/taos.cfg'
        self.tz = 'Asia/Shanghai'

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
                        "vgroups": 10,
                        "stt_trigger": 1,
                        "WAL_RETENTION_PERIOD": 86400
                    },
                    "super_tables": [
                        {
                            "name": "stb",
                            "child_table_exists": "yes",
                            "childtable_count": 500,
                            "childtable_prefix": "ctb0_",
                            "escape_character": "no",
                            "auto_create_table": "yes",
                            "batch_create_tbl_num": 1000,
                            "data_source": "rand",
                            "insert_mode": "taosc",
                            "interlace_rows": 400,
                            "tcp_transfer": "no",
                            "insert_rows": 50000,
                            "partial_col_num": 0,
                            "childtable_limit": 0,
                            "childtable_offset": 0,
                            "rows_per_tbl": 0,
                            "max_sql_len": 1024000,
                            "disorder_ratio": 30,
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

        # sql = "create stream str1 trigger continuous_window_close ignore expired 0 ignore update 0 into str1_dst as select _wstart as wstart, min(c1),max(c2), count(c3), tbname  from stream_test.stb partition by tbname interval(5s)"
        sql = "create stream str1 trigger continuous_window_close ignore expired 0 ignore update 0 into str1_dst as select _wstart as wstart, min(c1),max(c2), count(c3)  from stream_test.stb interval(5s)"
        cursor.execute(sql)
      

        print("create stream completed, start to monitor system load")
        conn.close()

        loader = MonitorSystemLoad('taosd', 600)
        loader.get_proc_status()

    def do_query_then_insert(self):
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
            t = threading.Thread(target=do_monitor)
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

        loader = MonitorSystemLoad('taosd', 80)
        loader.get_proc_status()

if __name__ == "__main__":
    StreamStarter().do_start()
    # StreamStarter().do_query_then_insert()
    # StreamStarter().multi_insert()