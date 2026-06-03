import random
import taos
import sys
import time
import socket
import os
import threading
import signal

from util.log import *
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.common import *
from taos.tmq import Consumer

sys.path.append("./7-tmq")

insertJson = '''{
    "filetype": "insert",
    "cfgdir": "/etc/taos",
    "host": "localhost",
    "port": 6030,
    "user": "root",
    "password": "taosdata",
    "connection_pool_size": 10,
    "thread_count": 10,
    "create_table_thread_count": 10,
    "result_file": "./insert-2-2-1.txt",
    "confirm_parameter_prompt": "no",
    "num_of_records_per_req": 3600,
    "prepared_rand": 3600,
    "chinese": "no",
    "escape_character": "yes",
    "continue_if_fail": "no",
    "databases": [
        {
            "dbinfo": {
                "name": "tmq_alter_tag",
                "drop": "yes",
                "vgroups": 2,
                "precision": "ms",
        "buffer": 512,
        "cachemodel":"'both'",
        "stt_trigger": 1
            },
            "super_tables": [
                {
                    "name": "stb",
                    "child_table_exists": "no",
                    "childtable_count": 1000000,
                    "childtable_prefix": "d_",
                    "auto_create_table": "no",
                    "batch_create_tbl_num": 1000,
                    "data_source": "csv",
                    "insert_mode": "taosc",
                    "non_stop_mode": "no",
                    "line_protocol": "line",
                    "insert_rows": 10000,
                    "childtable_limit": 0,
                    "childtable_offset": 0,
                    "interlace_rows": 0,
                    "insert_interval": 0,
                    "partial_col_num": 0,
                    "timestamp_step": 1000,
                    "start_timestamp": "2024-11-01 00:00:00.000",
                    "sample_format": "csv",
                    "sample_file": "./td_double10000_juchi.csv",
                    "use_sample_ts": "no",
                    "tags_file": "",
                    "columns": [
                        {"type": "DOUBLE", "name": "val"},
                        { "type": "INT", "name": "quality"}
                    ],
                    "tags": [
                        {"type": "INT", "name": "id", "max": 1000000, "min": 1}
                    ]
                }
            ]
        }
    ]
}'''

class TDTestCase:
    updatecfgDict = {'debugFlag': 131, 'asynclog': 1}
    clientCfgDict = {'debugFlag': 131, 'asynclog': 1}
    updatecfgDict["clientCfg"] = clientCfgDict

    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        self.tableNum = 1000000
        self.stopAlter = False  # 停止标志
        self.pauseAlter = threading.Event()  # 暂停/恢复事件
        self.pauseAlter.set()  # 初始状态为运行
        tdLog.debug(f"start to excute {__file__}")
        tdSql.init(conn.cursor())
        self.conn = conn
        self.tdSqlList = []
        self.alterThread = []
        
        # 注册信号处理器
        signal.signal(signal.SIGUSR1, self.signalHandler)  # 暂停/恢复 alter 线程
        signal.signal(signal.SIGUSR2, self.signalHandler)  # 停止 alter 线程
        signal.signal(signal.SIGINT, self.signalHandler)   # Ctrl+C 停止所有
        
        tdLog.info(f"Signal handlers registered. PID: {os.getpid()}")
        tdLog.info(f"Send SIGUSR1 to pause/resume alter threads: kill -SIGUSR1 {os.getpid()}")
        tdLog.info(f"Send SIGUSR2 to stop alter threads: kill -SIGUSR2 {os.getpid()}")

    def signalHandler(self, signum, frame):
        """信号处理器"""
        if signum == signal.SIGUSR1:
            # SIGUSR1: 切换暂停/恢复状态
            if self.pauseAlter.is_set():
                self.pauseAlter.clear()
                tdLog.info("Received SIGUSR1: Pausing alter threads...")
            else:
                self.pauseAlter.set()
                tdLog.info("Received SIGUSR1: Resuming alter threads...")
        elif signum == signal.SIGUSR2:
            # SIGUSR2: 停止 alter 线程
            tdLog.info("Received SIGUSR2: Stopping alter threads...")
            self.stopAlter = True
            self.pauseAlter.set()  # 确保线程不会卡在等待状态
        elif signum == signal.SIGINT:
            # SIGINT (Ctrl+C): 停止所有
            tdLog.info("Received SIGINT: Stopping all threads...")
            self.stopAlter = True
            self.pauseAlter.set()

    def consume(self, **paras):
        group = paras["group"]
        consumer_dict = {
            "group.id": group,
            "td.connect.user": "root",
            "td.connect.pass": "taosdata",
            "auto.offset.reset": "earliest",
        }
        consumer = Consumer(consumer_dict)

        try:
            consumer.subscribe(["t0"])
        except TmqError:
            tdLog.exit(f"subscribe error")

        cnt = 0
        times = 0
        try:
            while True:
                res = consumer.poll(10)
                if not res:
                    break
                val = res.value()
                if val is None:
                    continue
                for block in val:
                    cnt += len(block.fetchall())
                    if cnt // 5000000 > times:
                        times = cnt // 5000000
                        print(f"{group} Consumed {cnt} records so far...")
        finally:
            consumer.close()

    def insertData(self):
        with open('tmq_alter.json', 'w') as file:
            file.write(insertJson)

        tdLog.info("start to insert data: taosBenchmark -f tmq_alter.json")
        if os.system("taosBenchmark -f tmq_alter.json") != 0:
            tdLog.exit("taosBenchmark -f tmq_alter.json")

        tdLog.info("test tmq_alter ......")
    
    def alterTag(self, **kwargs):
        threadId = threading.current_thread().ident
        startTable = kwargs["start"]
        endTable = kwargs["end"]
        tdSqlTmp = kwargs["tdSql"]
        alterSql = ''
        tableIndex = startTable
        tdSqlTmp.execute(f'use tmq_alter_tag')

        while not self.stopAlter:
            # 等待恢复信号（如果被暂停）
            self.pauseAlter.wait()
            
            if self.stopAlter:
                break
                
            tagVal = random.randint(1, self.tableNum)
            alterSql += f'alter table d_{tableIndex} set tag id = {tagVal};'
            tableIndex += 1
            
            if tableIndex % 10 == 0:
                if tableIndex % 10000 == 0:
                    print(f"[Thread {threadId}] Executing batch alter for tables {tableIndex - 10}/{endTable} to {tableIndex - 1}/{endTable}")
                try:
                    tdSqlTmp.execute(alterSql)
                except Exception as e:
                    tdLog.warning(f"[Thread {threadId}] Alter failed: {e}")
                alterSql = ''
                
            if tableIndex >= endTable:
                tableIndex = startTable
                print(f"[Thread {threadId}] Restart alter table tag from {startTable}")
                alterSql = ''
        
        tdLog.info(f"[Thread {threadId}] Alter thread stopped")

    def countTableNum(self):
        while True:
            tdSql.execute(f'use tmq_alter_tag')
            tdSql.query(f'select count(*) from information_schema.ins_tables where stable_name = "stb"')
            if tdSql.getData(0, 0) == self.tableNum:
                time.sleep(10)
                break
            time.sleep(3)
            print(f"Waiting for table count to reach {self.tableNum}, current count is {tdSql.getData(0, 0)}")
            continue

    def run(self):
        tdLog.info(f"Test started. PID: {os.getpid()}")
        
        insertThread = threading.Thread(target=self.insertData, name="InsertThread")
        insertThread.start()

        self.countTableNum()
        time.sleep(5)  # 确保所有表都创建完成

        tdSql.execute(f'use tmq_alter_tag')
        tdSql.execute(f'create topic t0 as stable tmq_alter_tag.stb where id > 300000')

        consumeThread1 = threading.Thread(target=self.consume, kwargs={"group": "g1"}, name="ConsumeThread-g1")
        consumeThread1.start()
        consumeThread2 = threading.Thread(target=self.consume, kwargs={"group": "g2"}, name="ConsumeThread-g2")
        consumeThread2.start()

        time.sleep(5)  # 确保消费者线程已经开始消费

        alterCnt = 10
        batchAlterNum = self.tableNum // alterCnt
        
        for i in range(alterCnt):
            tdSqlTmp = TDSql()
            tdSqlTmp.init(self.conn.cursor())
            self.tdSqlList.append(tdSqlTmp)
            thread = threading.Thread(
                target=self.alterTag, 
                kwargs={"start": i * batchAlterNum, "end": (i + 1) * batchAlterNum, "tdSql": tdSqlTmp},
                name=f"AlterThread-{i}"
            )
            self.alterThread.append(thread)
            thread.start()
            tdLog.info(f"Started AlterThread-{i}")

        # 等待插入线程完成
        insertThread.join()
        tdLog.info("Insert thread completed")

        # 等待消费线程完成
        consumeThread1.join()
        tdLog.info("Consumer thread g1 completed")
        consumeThread2.join()
        tdLog.info("Consumer thread g2 completed")

        # 停止 alter 线程
        tdLog.info("Stopping alter threads...")
        self.stopAlter = True
        self.pauseAlter.set()  # 确保线程不会卡在等待状态
        
        for i, thread in enumerate(self.alterThread):
            thread.join(timeout=10)
            if thread.is_alive():
                tdLog.warning(f"Alter thread {i} did not finish in time")
            else:
                tdLog.info(f"Alter thread {i} completed")

    def stop(self):
        self.stopAlter = True
        self.pauseAlter.set()
        for tdSqlTmp in self.tdSqlList:
            tdSqlTmp.close()
        tdLog.success(f"{__file__} successfully executed")

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())