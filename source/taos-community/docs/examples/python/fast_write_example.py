# install dependencies:
# recommend python >= 3.8
#

import logging
import math
import multiprocessing
import sys
import time
import os
from multiprocessing import Process, Queue
from mockdatasource import MockDataSource
from queue import Empty
from typing import List

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(asctime)s [%(name)s] - %(message)s")

READ_TASK_COUNT = 1
WRITE_TASK_COUNT = 1
TABLE_COUNT = 1000
QUEUE_SIZE = 1000000
MAX_BATCH_SIZE = 3000

_DONE_MESSAGE = '__DONE__'


def get_connection():
    """
    If variable TDENGINE_FIRST_EP is provided then it will be used. If not, firstEP in /etc/taos/taos.cfg will be used.
    You can also override the default username and password by supply variable TDENGINE_USER and TDENGINE_PASSWORD
    """
    import taos
    firstEP = os.environ.get("TDENGINE_FIRST_EP")
    if firstEP:
        host, port = firstEP.split(":")
    else:
        host, port = None, 0
    user = os.environ.get("TDENGINE_USER", "root")
    password = os.environ.get("TDENGINE_PASSWORD", "taosdata")
    return taos.connect(host=host, port=int(port), user=user, password=password)


# ANCHOR: read

def run_read_task(task_id: int, task_queues: List[Queue], infinity):
    table_count_per_task = TABLE_COUNT // READ_TASK_COUNT
    data_source = MockDataSource(f"tb{task_id}", table_count_per_task, infinity)
    try:
        for batch in data_source:
            if isinstance(batch, tuple):
                batch = [batch]
            for table_id, rows in batch:
                # hash data to different queue
                i = table_id % len(task_queues)
                # block putting forever when the queue is full
                for row in rows:
                    task_queues[i].put(row)
        if not infinity:
            for queue in task_queues:
                queue.put(_DONE_MESSAGE)
    except KeyboardInterrupt:
        pass
    finally:
        logging.info('read task over')


# ANCHOR_END: read


# ANCHOR: write
def run_write_task(task_id: int, queue: Queue, done_queue: Queue):
    from sql_writer import SQLWriter
    log = logging.getLogger(f"WriteTask-{task_id}")
    writer = SQLWriter(get_connection)
    lines = None
    try:
        while True:
            over = False
            lines = []
            for _ in range(MAX_BATCH_SIZE):
                try:
                    line = queue.get_nowait()
                    if line == _DONE_MESSAGE:
                        over = True
                        break
                    if line:
                        lines.append(line)
                except Empty:
                    time.sleep(0.1)
            if len(lines) > 0:
                writer.process_lines(lines)
            if over:
                done_queue.put(_DONE_MESSAGE)
                break
    except KeyboardInterrupt:
        pass
    except BaseException as e:
        log.debug(f"lines={lines}")
        raise e
    finally:
        writer.close()
        log.debug('write task over')


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


# ANCHOR: monitor
def run_monitor_process(done_queue: Queue):
    log = logging.getLogger("DataBaseMonitor")
    conn = None
    try:
        conn = get_connection()

        def get_count():
            res = conn.query("SELECT count(*) FROM test.meters")
            rows = res.fetch_all()
            return rows[0][0] if rows else 0

        last_count = 0
        while True:
            try:
                done = done_queue.get_nowait()
                if done == _DONE_MESSAGE:
                    break
            except Empty:
                pass
            time.sleep(10)
            count = get_count()
            log.info(f"count={count} speed={(count - last_count) / 10}")
            last_count = count
    finally:
        conn.close()


# ANCHOR_END: monitor
# ANCHOR: main
def main(infinity):
    set_global_config()
    logging.info(f"READ_TASK_COUNT={READ_TASK_COUNT}, WRITE_TASK_COUNT={WRITE_TASK_COUNT}, "
                 f"TABLE_COUNT={TABLE_COUNT}, QUEUE_SIZE={QUEUE_SIZE}, MAX_BATCH_SIZE={MAX_BATCH_SIZE}")

    conn = get_connection()
    conn.execute("DROP DATABASE IF EXISTS test")
    conn.execute("CREATE DATABASE IF NOT EXISTS test keep 36500")
    conn.execute("CREATE STABLE IF NOT EXISTS test.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) "
                 "TAGS (location BINARY(64), groupId INT)")
    conn.close()

    done_queue = Queue()
    monitor_process = Process(target=run_monitor_process, args=(done_queue,))
    monitor_process.start()
    logging.debug(f"monitor task started with pid {monitor_process.pid}")

    task_queues: List[Queue] = []
    write_processes = []
    read_processes = []

    # create task queues
    for i in range(WRITE_TASK_COUNT):
        queue = Queue()
        task_queues.append(queue)

    # create write processes
    for i in range(WRITE_TASK_COUNT):
        p = Process(target=run_write_task, args=(i, task_queues[i], done_queue))
        p.start()
        logging.debug(f"WriteTask-{i} started with pid {p.pid}")
        write_processes.append(p)

    # create read processes
    for i in range(READ_TASK_COUNT):
        queues = assign_queues(i, task_queues)
        p = Process(target=run_read_task, args=(i, queues, infinity))
        p.start()
        logging.debug(f"ReadTask-{i} started with pid {p.pid}")
        read_processes.append(p)

    try:
        monitor_process.join()
        for p in read_processes:
            p.join()
        for p in write_processes:
            p.join()
        time.sleep(1)
        return
    except KeyboardInterrupt:
        monitor_process.terminate()
        [p.terminate() for p in read_processes]
        [p.terminate() for p in write_processes]
        [q.close() for q in task_queues]


def assign_queues(read_task_id, task_queues):
    """
    Compute target queues for a specific read task.
    """
    ratio = WRITE_TASK_COUNT / READ_TASK_COUNT
    from_index = math.floor(read_task_id * ratio)
    end_index = math.ceil((read_task_id + 1) * ratio)
    return task_queues[from_index:end_index]


if __name__ == '__main__':
    multiprocessing.set_start_method('spawn')
    main(False)
# ANCHOR_END: main
