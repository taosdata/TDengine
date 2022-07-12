# install dependencies:
# recommend python >= 3.8
# pip3 install faster-fifo
#

import logging
import sys
import time
from multiprocessing import Process
from faster_fifo import Queue
from queue import Empty
from typing import List

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(asctime)s [%(name)s] - %(message)s")

READ_TASK_COUNT = 1
WRITE_TASK_COUNT = 1
TABLE_COUNT = 1000
QUEUE_SIZE = 1000000
MAX_BATCH_SIZE = 3000

read_processes = []
write_processes = []


# ANCHOR: MockDataSource
class MockDataSource:
    location = ["LosAngeles", "SanDiego", "Hollywood", "Compton", "San Francisco"]
    current = [8.8, 10.7, 9.9, 8.9, 9.4]
    voltage = [119, 116, 111, 113, 118]
    phase = [0.32, 0.34, 0.33, 0.329, 0.141]
    max_rows_per_table = 10 ** 9

    def __init__(self, tb_name_prefix, table_count):
        self.table_name_prefix = tb_name_prefix
        self.table_count = table_count
        self.start_ms = round(time.time() * 1000) - self.max_rows_per_table * 100

    def __iter__(self):
        self.row = 0
        self.table_id = -1
        return self

    def __next__(self):
        """
        next 100 rows of current table
        """
        self.table_id += 1
        if self.table_id == self.table_count:
            self.table_id = 0
        if self.row >= self.max_rows_per_table:
            raise StopIteration
        rows = []

        while len(rows) < 100:
            self.row += 1
            ts = self.start_ms + 100 * self.row
            group_id = self.table_id % 5 if self.table_id % 5 == 0 else self.table_id % 5 + 1
            tb_name = self.table_name_prefix + '_' + str(self.table_id)
            ri = self.row % 5
            rows.append(
                f"{tb_name},{ts},{self.current[ri]},{self.voltage[ri]},{self.phase[ri]},{self.location[ri]},{group_id}")
        return self.table_id, rows


# ANCHOR_END: MockDataSource

# ANCHOR: read
def run_read_task(task_id: int, task_queues: List[Queue]):
    table_count_per_task = TABLE_COUNT // READ_TASK_COUNT
    data_source = MockDataSource(f"tb{task_id}", table_count_per_task)
    try:
        for table_id, rows in data_source:
            i = table_id % len(task_queues)
            task_queues[i].put_many(rows, block=True, timeout=-1)
    except KeyboardInterrupt:
        pass


# ANCHOR_END: read

# ANCHOR: write
def run_write_task(task_id: int, queue: Queue):
    from sql_writer import SQLWriter
    log = logging.getLogger(f"WriteTask-{task_id}")
    writer = SQLWriter()
    lines = None
    try:
        while True:
            try:
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
def set_global_config():
    argc = len(sys.argv)
    if argc > 1:
        global READ_TASK_COUNT
        READ_TASK_COUNT = int(sys.argv[1])
    if argc > 2:
        global WRITE_TASK_COUNT
        WRITE_TASK_COUNT = int(sys.argv[2])
    if argc > 3:
        global TABLE_COUNT
        TABLE_COUNT = int(sys.argv[3])
    if argc > 4:
        global QUEUE_SIZE
        QUEUE_SIZE = int(sys.argv[4])
    if argc > 5:
        global MAX_BATCH_SIZE
        MAX_BATCH_SIZE = int(sys.argv[5])


# ANCHOR: main
def run_monitor_process():
    import taos
    log = logging.getLogger("DataBaseMonitor")
    conn = taos.connect(host="localhost", user="root", password="taosdata", port=6030)
    conn.execute("DROP DATABASE IF EXISTS test")
    conn.execute("CREATE DATABASE test")
    conn.execute("CREATE STABLE test.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) "
                 "TAGS (location BINARY(64), groupId INT)")

    def get_count():
        res = conn.query("SELECT count(*) FROM test.meters")
        rows = res.fetch_all()
        return rows[0][0] if rows else 0

    last_count = 0
    while True:
        time.sleep(10)
        count = get_count()
        log.info(f"count={count} speed={(count - last_count) / 10}")
        last_count = count


def main():
    set_global_config()
    logging.info(f"READ_TASK_COUNT={READ_TASK_COUNT}, WRITE_TASK_COUNT={WRITE_TASK_COUNT}, "
                 f"TABLE_COUNT={TABLE_COUNT}, QUEUE_SIZE={QUEUE_SIZE}, MAX_BATCH_SIZE={MAX_BATCH_SIZE}")

    monitor_process = Process(target=run_monitor_process)
    monitor_process.start()
    time.sleep(3)

    task_queues: List[Queue] = []
    # create task queues
    for i in range(WRITE_TASK_COUNT):
        queue = Queue(max_size_bytes=QUEUE_SIZE)
        task_queues.append(queue)
    # create write processes
    for i in range(WRITE_TASK_COUNT):
        p = Process(target=run_write_task, args=(i, task_queues[i]))
        p.start()
        logging.debug(f"WriteTask-{i} started with pid {p.pid}")
        write_processes.append(p)
    # create read processes
    for i in range(READ_TASK_COUNT):
        p = Process(target=run_read_task, args=(i, task_queues))
        p.start()
        logging.debug(f"ReadTask-{i} started with pid {p.pid}")
        read_processes.append(p)

    try:
        monitor_process.join()
    except KeyboardInterrupt:
        monitor_process.terminate()
        [p.terminate() for p in read_processes]
        [p.terminate() for p in write_processes]
        [q.close() for q in task_queues]


if __name__ == '__main__':
    main()
# ANCHOR_END: main
