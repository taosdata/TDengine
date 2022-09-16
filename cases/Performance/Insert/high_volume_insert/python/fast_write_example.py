# install dependencies:
# recommend python >= 3.8
# pip3 install faster-fifo
#

import logging
import math
import sys
import time
import os
from multiprocessing import Process
from taostest import TDCase
from faster_fifo import Queue
from .mockdatasource import MockDataSource
from queue import Empty
from typing import List
from taostest.util.common import TDCom

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(asctime)s [%(name)s] - %(message)s")

# read_processes = []
# write_processes = []

class InsertTest(TDCase):
    def init(self):
        self.tdCom = TDCom(self.tdSql)
        self.taosd_setting = self.tdCom.get_components_setting(self.env_setting["settings"], "taosd")
        self.fqdn = self.taosd_setting["fqdn"][0]
        self.read_processes = []
        self.write_processes = []
        self.task_queues = []
        self.monitor_process = None
        self.fast_write_param_list = self.envMgr.fast_write_param_list.replace('[', "").replace(']', "").split(",")
        
    def author(self):
        pass
    def desc(self):
        pass
    def tags(self):
        pass

    def get_connection(self):
        """
        If variable TDENGINE_FIRST_EP is provided then it will be used. If not, firstEP in /etc/taos/taos.cfg will be used.
        You can also override the default username and password by supply variable TDENGINE_USER and TDENGINE_PASSWORD
        """
        import taos
        firstEP = self.taosd_setting["spec"]["config"]["firstEP"]
        if firstEP:
            host, port = firstEP.split(":")
        else:
            host, port = None, 0
        return taos.connect(host=host, port=int(port), user="root", password="taosdata")


    # ANCHOR: read

    def run_read_task(self, task_id: int, task_queues: List[Queue]):
        table_count_per_task = TABLE_COUNT // READ_TASK_COUNT
        data_source = MockDataSource(f"tb{task_id}", table_count_per_task)
        try:
            for batch in data_source:
                for table_id, rows in batch:
                    # hash data to different queue
                    i = table_id % len(task_queues)
                    # block putting forever when the queue is full
                    task_queues[i].put_many(rows, block=True, timeout=-1)
        except KeyboardInterrupt:
            pass


    def kill_all_using_process(self):
        self.envMgr._remote.cmd2("127.0.0.1", ["ps -ef | grep fast_write_example.py | grep python | grep -v grep | awk \'{{print $2}}\' | xargs kill -9"])

    # ANCHOR_END: read

    # ANCHOR: write
    def run_write_task(self, task_id: int, queue: Queue):
        from .sql_writer import SQLWriter
        log = logging.getLogger(f"WriteTask-{task_id}")
        writer = SQLWriter(self.get_connection)
        lines = None
        try:
            while True:
                try:
                    # get as many as possible
                    lines = queue.get_many(block=False, max_messages_to_get=MAX_BATCH_SIZE)
                    writer.process_lines(lines)
                except Empty:
                    time.sleep(0.01)
        except KeyboardInterrupt:
            pass
        except BaseException as e:
            log.debug(f"lines={lines}")
            raise e


    # ANCHOR_END: write

    def set_global_config(self):
        global RUN_TIME
        global READ_TASK_COUNT
        global WRITE_TASK_COUNT
        global TABLE_COUNT
        global QUEUE_SIZE
        global MAX_BATCH_SIZE
        RUN_TIME = self.tdCom.trans_time_to_s(str(self.fast_write_param_list[0])) if self.fast_write_param_list[0] != "None" else "1m"
        READ_TASK_COUNT = int(self.fast_write_param_list[1]) if self.fast_write_param_list[1] != "None" else 2
        WRITE_TASK_COUNT = int(self.fast_write_param_list[2]) if self.fast_write_param_list[2] != "None" else 2
        TABLE_COUNT = int(self.fast_write_param_list[3]) if self.fast_write_param_list[3] != "None" else 1000
        QUEUE_SIZE = int(self.fast_write_param_list[4]) if self.fast_write_param_list[4] != "None" else 1000000
        MAX_BATCH_SIZE = int(self.fast_write_param_list[5]) if self.fast_write_param_list[5] != "None" else 3000
        

    # ANCHOR: monitor
    def run_monitor_process(self):
        log = logging.getLogger("DataBaseMonitor")
        conn = self.get_connection()
        conn.execute("DROP DATABASE IF EXISTS test")
        conn.execute("CREATE DATABASE test")
        conn.execute("CREATE STABLE test.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) "
                    "TAGS (location BINARY(64), groupId INT)")

        def get_count():
            res = conn.query("SELECT count(*) FROM test.meters")
            rows = res.fetch_all()
            return rows[0][0] if rows else 0
        start_time = 0
        end_time = 30
        last_count = 0
        while True:
            time.sleep(10)
            start_time += 10
            if start_time >= RUN_TIME:
                self.kill_all_using_process()
            count = get_count()
            log.info(f"count={count} speed={(count - last_count) / 10}")
            with open(f'{self.run_log_dir}/fast_write.log', 'a') as f:
                f.write(f"count={count} speed={(count - last_count) / 10}\n")
            last_count = count


    # ANCHOR_END: monitor
    # ANCHOR: main
    def run(self):
        self.set_global_config()
        logging.info(f"READ_TASK_COUNT={READ_TASK_COUNT}, WRITE_TASK_COUNT={WRITE_TASK_COUNT}, "
                    f"TABLE_COUNT={TABLE_COUNT}, QUEUE_SIZE={QUEUE_SIZE}, MAX_BATCH_SIZE={MAX_BATCH_SIZE}")

        self.monitor_process = Process(target=self.run_monitor_process)
        self.monitor_process.start()
        time.sleep(3)  # waiting for database ready.

        self.task_queues: List[Queue] = []
        # create task queues
        for i in range(WRITE_TASK_COUNT):
            queue = Queue(max_size_bytes=QUEUE_SIZE)
            self.task_queues.append(queue)

        # create write processes
        for i in range(WRITE_TASK_COUNT):
            p = Process(target=self.run_write_task, args=(i, self.task_queues[i]))
            p.start()
            logging.debug(f"WriteTask-{i} started with pid {p.pid}")
            self.write_processes.append(p)

        # create read processes
        for i in range(READ_TASK_COUNT):
            queues = self.assign_queues(i, self.task_queues)
            p = Process(target=self.run_read_task, args=(i, queues))
            p.start()
            logging.debug(f"ReadTask-{i} started with pid {p.pid}")
            self.read_processes.append(p)


        try:
            self.monitor_process.join()

        except KeyboardInterrupt:
            self.monitor_process.terminate()
            [p.terminate() for p in self.read_processes]
            [p.terminate() for p in self.write_processes]
            [q.close() for q in self.task_queues]


    def assign_queues(self, read_task_id, task_queues):
        """
        Compute target queues for a specific read task.
        """
        ratio = WRITE_TASK_COUNT / READ_TASK_COUNT
        from_index = math.floor(read_task_id * ratio)
        end_index = math.ceil((read_task_id + 1) * ratio)
        return task_queues[from_index:end_index]


    # if __name__ == '__main__':
    #     main()
    # ANCHOR_END: main