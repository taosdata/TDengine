import json
import subprocess

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

                s = "load: %.2f, CPU:%s, Mem:%.2f MiB(%.2f%%), Read: %.2fMiB(%d), Write: %.2fMib (%d)" % (
                    sys_load[0], cpu_percent, memory_info.rss / 1048576.0,
                    memory_percent, io_counters.read_bytes / 1048576.0, io_counters.read_count,
                    io_counters.write_bytes / 1048576.0, io_counters.write_count)

                print(s)
                f.write(s + '\n')

                time.sleep(1)

                self.count -= 1
                if self.count <= 0:
                    break


class StreamStarter:
    def __init__(self) -> None:
        self.sql = None
        self.host='127.0.0.1'
        self.user = 'root'
        self.passwd = 'taosdata'
        self.conf = '/etc/taos/taos.cfg'
        self.tz = 'Asia/Shanghai'

    def prepare_data(self) -> dict:
        json_data = {
            "filetype": "insert",
            "cfgdir": "/etc/taos/cfg",
            "host": "127.0.0.1",
            "port": 6030,
            "rest_port": 6041,
            "user": "root",
            "password": "taosdata",
            "thread_count": 5,
            "create_table_thread_count": 10,
            "result_file": "/tmp/taosBenchmark_result.log",
            "confirm_parameter_prompt": "no",
            "insert_interval": 1000,
            "num_of_records_per_req": 10000,
            "max_sql_len": 1024000,
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
                            "childtable_count": 50000,
                            "childtable_prefix": "ctb0_",
                            "escape_character": "no",
                            "auto_create_table": "yes",
                            "batch_create_tbl_num": 1000,
                            "data_source": "rand",
                            "insert_mode": "taosc",
                            "interlace_rows": 400,
                            "tcp_transfer": "no",
                            "insert_rows": 10000,
                            "partial_col_num": 0,
                            "childtable_limit": 0,
                            "childtable_offset": 0,
                            "rows_per_tbl": 0,
                            "max_sql_len": 1024000,
                            "disorder_ratio": 0,
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

        time.sleep(10)

        print("start to connect db")
        cursor = conn.cursor()

        cursor.execute('use stream_test')

        sql = "create stream str1 ignore update 0 into str1_dst as select _wstart as wstart, min(c1),max(c2), count(c3)  from stream_test.stb partition by cast(t1 as int) t1,tbname interval(5s)"
        cursor.execute(sql)

        print("create stream completed, start to monitor system load")
        conn.close()

        loader = MonitorSystemLoad('taosd', 80)
        loader.get_proc_status()


if __name__ == "__main__":
    StreamStarter().do_start()
