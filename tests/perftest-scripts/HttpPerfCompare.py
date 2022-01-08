from loguru import logger
import time
import os
import json

class HttpPerfCompard:
    def __init__(self):
        self.hostname = "vm85"
        self.taosc_port = 6030
        self.http_port = 6041
        self.database = "test"
        self.query_times = 1
        self.concurrent = 1
        self.column_count = 10
        self.tag_count = 10
        self.perfMonitorBin = '/home/ubuntu/perfMonitor' 
        self.taosBenchmarkBin = '/usr/local/bin/taosBenchmark'
        self.sleep_time = 20

        self.current_time = time.strftime("%Y-%m-%d-%H:%M:%S", time.localtime(time.time()))
        self.current_dir = os.path.dirname(os.path.realpath(__file__))
        self.log_file = os.path.join(self.current_dir, f'./performance.log')
        logger.add(self.log_file)
        logger.info(f'init env success, log will be export to {self.log_file}')
        self.sql_list = ['select last_row(*) from test.stb;',
                        'select * from test.stb limit 100000;',
                        'select count(*) from test.stb interval (1d);',
                        'select avg(c3), max(c4), min(c5) from test.stb interval (1d);',
                        'select count(*) from test.stb where t1 = "shanghai" interval (1h);',
                        'select avg(c3), max(c4), min(c5) from test.stb where t1 = "shanghai" interval (1d);',
                        'select avg(c3), max(c4), min(c5) from test.stb where ts > "2021-01-01 00:00:00" and ts < "2021-01-31 00:00:00" interval (1d);'
                        'select last(*) from test.stb;'
                    ]
#        self.sql_list = ['select * from test.stb limit 100000;']

    def initLog(self):
        self.exec_local_cmd(f'echo "" > {self.log_file}')

    def exec_local_cmd(self,shell_cmd):
        result = os.popen(shell_cmd).read().strip()
        return result

    def genQueryJsonFile(self, query_sql):
        json_file = os.path.join(self.current_dir, f'./query.json')
        jdict = {
            "filetype": "query",
            "cfgdir": "/etc/taos",
            "host": self.hostname,
            "port": self.taosc_port,
            "user": "root",
            "password": "taosdata",
            "confirm_parameter_prompt": "no",
            "databases": self.database,
            "query_times": self.query_times,
            "query_mode": "restful",
            "specified_table_query": {
                "concurrent": self.concurrent,
                "sqls": [
                {
                    "sql": query_sql,
                    "result": "./query_res0.txt"
                }
                ]
            }
        }
        with open(json_file, "w", encoding="utf-8") as f_w:
            f_w.write(json.dumps(jdict))

    def genInsertJsonFile(self, thread_count, table_count, row_count, batch_size):
        json_file = os.path.join(self.current_dir, f'./insert.json')
        jdict = {
            "filetype": "insert",
            "cfgdir": "/etc/taos",
            "host": self.hostname,
            "rest_host": self.hostname,
            "port": self.taosc_port,
            "rest_port": self.http_port,
            "user": "root",
            "password": "taosdata",
            "thread_count": thread_count,
            "thread_count_create_tbl": 1,
            "result_file": self.log_file,
            "databases": [{
                "dbinfo": {
                    "name": self.database,
                    "drop": "yes"
                },
                "super_tables": [{
                    "name": "stb",
                    "childtable_count": table_count,
                    "childtable_prefix": "stb_",
                    "batch_create_tbl_num": 1,
                    "insert_mode": "rand",
                    "insert_iface": "rest",
                    "insert_rows": row_count,
                    "insert_interval": 0,
                    "batch_rows": batch_size,
                    "max_sql_len": 1048576,
                    "timestamp_step": 3000,
                    "start_timestamp": "2021-01-01 00:00:00.000",
                    "tags_file": "",
                    "partical_col_num": 0,
                    "columns": [{"type": "INT", "count": self.column_count}],
                    "tags": [{"type": "BINARY", "len": 16, "count": self.tag_count}]
                }]
            }]
        }
        with open(json_file, "w", encoding="utf-8") as f_w:
            f_w.write(json.dumps(jdict))

    def runTest(self):
        self.initLog()
        self.genInsertJsonFile(32, 100, 100000, 1)
        logger.info('result of insert_perf with 32 threads and 1 batch_size:')
        self.exec_local_cmd(f'{self.perfMonitorBin} -f insert.json')
        time.sleep(self.sleep_time)
        self.genInsertJsonFile(32, 500, 1000000, 1000)
        logger.info('result of insert_perf with 32 threads and 1000 batch_size:')
        self.exec_local_cmd(f'{self.perfMonitorBin} -f insert.json')
        time.sleep(self.sleep_time)

        for query_sql in self.sql_list:
            self.genQueryJsonFile(query_sql)
            self.exec_local_cmd(f'{self.taosBenchmarkBin} -f query.json > tmp.log')
            res = self.exec_local_cmd('grep -Eo \'\<Spent.+s\>\' tmp.log |grep -v \'total queries\' |awk \'{sum+=$2}END{print "Average=",sum/NR,"s"}\'')
            logger.info(query_sql)
            logger.info(res)
            time.sleep(self.sleep_time)

if __name__ == '__main__':   
    runPerf = HttpPerfCompard()
    runPerf.runTest()

    


    
