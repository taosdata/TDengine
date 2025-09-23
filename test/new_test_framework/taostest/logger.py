import logging
import sys
import threading
from logging import handlers
from ..utils.log import tdLog


class ThreadLogger():
    def __init__(self, filePath: str, queue, logLevel="debug"):
        self._filePath = filePath
        self._queue = queue
        self.t = None
        self.level = logging.INFO
        if not logLevel is None:
            if logLevel == "debug":
                self.level = logging.DEBUG
            elif logLevel == "info":
                self.level = logging.INFO
            elif logLevel == "warn":
                self.level = logging.WARN
            elif logLevel == "error":
                self.level = logging.ERROR
            elif logLevel == "fatal":
                self.level = logging.FATAL
            elif logLevel == "off":
                self.level = logging.OFF
            elif logLevel == "all":
                self.level = logging.ALL

    def work_thread(self):
        date_fmt = '%Y-%m-%d %H:%M:%S'
        formatter = "%(asctime)s %(levelname)s: %(message)s"
        format_str = logging.Formatter(formatter, date_fmt)

        file_handler = handlers.WatchedFileHandler(filename=self._filePath, encoding='utf-8', mode='w')
        file_handler.setFormatter(format_str)
        file_handler.setLevel(self.level)

        stdout_handler = logging.StreamHandler(sys.stdout)
        stdout_handler.setFormatter(format_str)
        stdout_handler.setLevel(self.level)

        logger = get_basic_logger()
        logger.addHandler(file_handler)
        logger.addHandler(stdout_handler)
        logger.setLevel(self.level)
        # wait queue data
        while (True):
            try:
                m = self._queue.get(block=True)
                level = m[0]
                msg = m[1]
                args = m[2]
                if level == "debug":
                    logger.debug(msg, *args)
                elif level == "info":
                    logger.info(msg, *args)
                elif level == "warning":
                    logger.warning(msg, *args)
                elif level == "error":
                    logger.error(msg, *args)
                elif level == "critical":
                    logger.critical(msg, *args)
                elif level == "exception":
                    logger.exception(msg, *args)
                elif level == "terminate":
                    logger.debug(msg, *args)
                    break
            except:
                pass

    def start(self):
        # start log thread
        self.t = threading.Thread(target=self.work_thread, args=())
        self.t.start()


def get_basic_logger(name="test"):
    logger = logging.getLogger(name)
    return logger


class Logger():
    def __init__(self, queue):
        self._queue = queue

    def debug(self, msg, *args):
        self._queue.put(("debug", msg, args))

    def info(self, msg, *args):
        self._queue.put(("info", msg, args))

    def warning(self, msg, *args):
        self._queue.put(("warning", msg, args))

    def error(self, msg, *args):
        self._queue.put(("error", msg, args))

    def critical(self, msg, *args):
        self._queue.put(("critical", msg, args))

    def terminate(self, msg, *args):
        self._queue.put(("terminate", msg, args))

    def exception(self, msg, *args):
        self._queue.put(("exception", msg, args))
