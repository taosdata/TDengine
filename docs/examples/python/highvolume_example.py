import sys
import logging
import time

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(asctime)s [%(threadName)s] - %(message)s")


class DataBaseMonitor:
    def init(self):
        pass

    def prepare_database(self):
        pass

    def get_count(self):
        return 1

    def stat_and_print(self):
        last_count = 0
        while True:
            time.sleep(10000)
            count = self.get_count()
            logging.info(f"count={count} speed={(count - last_count) / 10}")
            last_count = count


class ReadTask:
    def __init__(self):
        self.log = logging.getLogger("ReadTask")
        self.log.info("test")


class MockDataSource:
    def __init__(self):
        pass


class WriteTask:
    def __init__(self):
        self.log = logging.getLogger("WriteTask")
        self.log.info("test")


class SQLWriter:
    def __init__(self):
        pass


def main():
    argc = len(sys.argv)
    read_task_count = sys.argv[1] if argc > 1 else 1
    write_task_count = sys.argv[2] if argc > 2 else 3
    table_count = sys.argv[3] if argc > 3 else 1000
    max_batch_size = sys.argv[4] if argc > 4 else 3000
    logging.info(f"read_task_count={read_task_count}, write_task_count={write_task_count}, table_count={table_count}, max_batch_size={max_batch_size}")

    database_monitor = DataBaseMonitor()
    database_monitor.prepare_database()

    for i in range(write_task_count):
        pass

    for i in range(read_task_count):
        pass

    database_monitor.stat_and_print()


if __name__ == '__main__':
    main()
