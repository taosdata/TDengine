import logging
import sys
from threading import Thread
import time
from multiprocessing import Queue, Process
from queue import Empty
from typing import List


logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(asctime)s [%(name)s] - %(message)s")

READ_TASK_COUNT = 1
WRITE_TASK_COUNT = 1
TABLE_COUNT = 1000
MAX_BATCH_SIZE = 3000

read_processes = []
write_processes = []


def get_connection():
    import taos
    return taos.connect(host="localhost", user="root", password="taosdata", port=6030)

def get_max_sql_length(conn):
    rows = conn.query("SHOW variables").fetch_all()
    for r in rows:
        name = r[0]
        if name == "maxSQLLength":
            return int(r[1])

# ANCHOR: DataBaseMonitor
class DataBaseMonitor:
    """
    Start a thread.
    Prepare database and stable.
    Statistic writing speed and print it every 10 seconds.
    """
    def __init__(self):
        self.process = Process(target=self.run)
        self.process.start()

    def prepare_database(self, conn):
        conn.execute("DROP DATABASE IF EXISTS test")
        conn.execute("CREATE DATABASE test")
        conn.execute("CREATE STABLE test.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT)")

    def get_count(self, conn):
        res = conn.query("SELECT count(*) FROM test.meters")
        rows = res.fetch_all()
        return rows[0][0] if rows else 0

    def run(self):
        log = logging.getLogger("DataBaseMonitor")
        conn = get_connection()
        self.prepare_database(conn)
        last_count = 0
        while True:
            time.sleep(10)
            count = self.get_count(conn)
            log.info(f"count={count} speed={(count - last_count) / 10}")
            last_count = count

    def join(self):
        self.process.join()

    def stop(self):
        self.process.terminate()

# ANCHOR_END: DataBaseMonitor

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
        self.table_id += 1
        if self.table_id == self.table_count:
            self.table_id = 0
            self.row += 1
        if self.row < self.max_rows_per_table:
            ts = self.start_ms + 100 * self.row
            group_id = self.table_id % 5 if self.table_id % 5 == 0 else self.table_id % 5 + 1
            tb_name = self.table_name_prefix + '_' + str(self.table_id)
            ri = self.row % 5
            return self.table_id, f"{tb_name},{ts},{self.current[ri]},{self.voltage[ri]},{self.phase[ri]},{self.location[ri]},{group_id}"
        else:
            raise StopIteration


# ANCHOR_END: MockDataSource

# ANCHOR: SQLWriter
class SQLWriter:
    log = logging.getLogger("SQLWriter")

    def __init__(self):
        self._buffered_count = 0
        self._tb_values = {}
        self._tb_tags = {}
        self._conn = get_connection()
        self._max_sql_lenght = get_max_sql_length(self._conn)
        self._conn.execute("USE test")
        
    def process_line(self, line: str):
        """
        :param line: tbName,ts,current,voltage,phase,location,groupId
        """
        self._buffered_count += 1
        ps = line.split(",")
        table_name = ps[0]
        value = '(' + ",".join(ps[1:-2]) + ') '
        if table_name in self._tb_values:
            self._tb_values[table_name] += value
        else:
            self._tb_values[table_name] = value

        if table_name not in self._tb_tags:
            location = ps[-2]
            group_id = ps[-1]
            tag_value = f"('{location}',{group_id})"
            self._tb_tags[table_name] = tag_value

        if self._buffered_count == MAX_BATCH_SIZE:
            self.flush()

    def flush(self):
        """
        Assemble INSERT statement and execute it.
        When the sql length grows close to MAX_SQL_LENGTH, the sql will be executed immediately, and a new INSERT statement will be created.
        In case of "Table does not exit" exception, tables in the sql will be created and the sql will be re-executed.
        """
        sql = "INSERT INTO "
        sql_len = len(sql)
        buf = []
        for tb_name, values in self._tb_values.items():
            q = tb_name + " VALUES " + values
            if sql_len + len(q) >= self._max_sql_lenght:
                sql += " ".join(buf)
                self.execute_sql(sql)
                sql = "INSERT INTO "
                sql_len = len(sql)
                buf = []
            buf.append(q)
            sql_len += len(q)
        sql += " ".join(buf)
        self.execute_sql(sql)
        self._tb_values.clear()
        self._buffered_count = 0

    def execute_sql(self, sql):
        import taos
        try:
            self._conn.execute(sql)
        except taos.Error as e:
            error_code = e.errno & 0xffff
            # Table does not exit
            if error_code == 0x362 or error_code == 0x218:
                self.create_tables()
            else:
                raise e

    def create_tables(self):
        sql = "CREATE TABLE "
        for tb in self._tb_values.keys():
            tag_values = self._tb_tags[tb]
            sql += "IF NOT EXISTS " + tb + " USING meters TAGS " + tag_values + " "
        self._conn.execute(sql)

    @property
    def buffered_count(self):
        return self._buffered_count


# ANCHOR_END: SQLWriter

# ANCHOR: read
def run_read_task(task_id: int, task_queues: List[Queue]):
    table_count_per_task = TABLE_COUNT // READ_TASK_COUNT
    data_source = MockDataSource(f"tb{task_id}", table_count_per_task)
    try:
        for table_id, line in data_source:
            i = table_id % len(task_queues)
            task_queues[i].put(line, block=True)
    except KeyboardInterrupt:
        pass


# ANCHOR_END: read

# ANCHOR: write
def run_write_task(task_id: int, queue: Queue):
    log = logging.getLogger(f"WriteTask-{task_id}")
    writer = SQLWriter()
    try:
        while True:
            try:
                line = queue.get(block=False)
                writer.process_line(line)
            except Empty:
                if writer.buffered_count > 0:
                    writer.flush()
                else:
                    time.sleep(0.01)
    except KeyboardInterrupt:
        pass
    except BaseException as e:
        msg = f"line={line}, buffer_count={writer.buffered_count}"
        log.debug(msg)
        raise e


# ANCHOR_END: write

def set_global_config():
    argc = len(sys.argv)
    if argc > 1:
        global READ_TASK_COUNT
        READ_TASK_COUNT = sys.argv[1]
    if argc > 2:
        global WRITE_TASK_COUNT
        WRITE_TASK_COUNT = sys.argv[2]
    if argc > 3:
        global TABLE_COUNT
        TABLE_COUNT = sys.argv[3]
    if argc > 4:
        global MAX_BATCH_SIZE
        MAX_BATCH_SIZE = sys.argv[4]


# ANCHOR: main
def main():
    set_global_config()
    logging.info(f"READ_TASK_COUNT={READ_TASK_COUNT}, WRITE_TASK_COUNT={WRITE_TASK_COUNT}, TABLE_COUNT={TABLE_COUNT}, MAX_BATCH_SIZE={MAX_BATCH_SIZE}")

    database_monitor = DataBaseMonitor()
    time.sleep(3) # wait for database ready

    task_queues: List[Queue] = []

    for i in range(WRITE_TASK_COUNT):
        queue = Queue(maxsize=10000000)
        task_queues.append(queue)
        p = Process(target=run_write_task, args=(i, queue))
        p.start()
        logging.debug(f"WriteTask {i} started with pid {p.pid}")
        write_processes.append(p)

    for i in range(READ_TASK_COUNT):
        p = Process(target=run_read_task, args=(i, task_queues))
        p.start()
        logging.debug(f"ReadTask {i} started with pid {p.pid}")
        read_processes.append(p)

    try:
        database_monitor.join()
    except KeyboardInterrupt:
        database_monitor.stop()
        [p.terminate() for p in read_processes]
        [p.terminate() for p in write_processes]


# ANCHOR_END: main

if __name__ == '__main__':
    main()
