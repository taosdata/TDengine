import threading
import random
import logging
import os
import sys
from typing import Optional
import time , datetime
from datetime import datetime
import taos


class CrashGenError(taos.error.ProgrammingError):
    INVALID_EMPTY_RESULT    = 0x991
    INVALID_MULTIPLE_RESULT = 0x992
    DB_CONNECTION_NOT_OPEN  = 0x993
    # def __init__(self, msg=None, errno=None):
    #     self.msg = msg
    #     self.errno = errno

    # def __str__(self):
    #     return self.msg
    pass


class LoggingFilter(logging.Filter):
    def filter(self, record: logging.LogRecord):
        if (record.levelno >= logging.INFO):
            return True  # info or above always log

        # Commenting out below to adjust...

        # if msg.startswith("[TRD]"):
        #     return False
        return True


class MyLoggingAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        shortTid = threading.get_ident() % 10000
        return "[{:04d}] {}".format(shortTid, msg), kwargs
        # return '[%s] %s' % (self.extra['connid'], msg), kwargs


class Logging:
    logger = None # type: Optional[MyLoggingAdapter]

    @classmethod
    def _get_datetime(cls):
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-1]

    @classmethod
    def getLogger(cls):
        return cls.logger

    @classmethod
    def clsInit(cls, debugMode: bool):
        if cls.logger:
            return
        
        # Logging Stuff
        # global misc.logger
        _logger = logging.getLogger('CrashGen')  # real logger
        _logger.addFilter(LoggingFilter())
        ch = logging.StreamHandler(sys.stdout) # Ref: https://stackoverflow.com/questions/14058453/making-python-loggers-output-all-messages-to-stdout-in-addition-to-log-file
        _logger.addHandler(ch)

        # Logging adapter, to be used as a logger
        # print("setting logger variable")
        # global logger
        cls.logger = MyLoggingAdapter(_logger, {})
        cls.logger.setLevel(logging.DEBUG if debugMode else logging.INFO)  # default seems to be INFO

    @classmethod
    def info(cls, msg):
        cls.logger.info("[time]: " + cls._get_datetime() +" [msg]: "+ msg)

    @classmethod
    def debug(cls, msg):
        cls.logger.debug("[time]: " + cls._get_datetime() +" [msg]: "+ msg)

    @classmethod
    def warning(cls, msg):
        cls.logger.warning("[time]: " + cls._get_datetime() +" [msg]: "+ msg)

    @classmethod
    def error(cls, msg):
        cls.logger.error("[time]: " + cls._get_datetime() +" [msg]: "+ msg)

class Status:
    STATUS_EMPTY    = 99
    STATUS_STARTING = 1
    STATUS_RUNNING  = 2
    STATUS_STOPPING = 3
    STATUS_STOPPED  = 4

    def __init__(self, status):
        self.set(status)

    def __repr__(self):
        return "[Status: v={}]".format(self._status)

    def set(self, status: int):
        self._status = status

    def get(self):
        return self._status

    def isEmpty(self):
        ''' Empty/Undefined '''
        return self._status == Status.STATUS_EMPTY

    def isStarting(self):
        return self._status == Status.STATUS_STARTING

    def isRunning(self):
        # return self._thread and self._thread.is_alive()
        return self._status == Status.STATUS_RUNNING

    def isStopping(self):
        return self._status == Status.STATUS_STOPPING

    def isStopped(self):
        return self._status == Status.STATUS_STOPPED

    def isStable(self):
        return self.isRunning() or self.isStopped()

    def isActive(self):
        return self.isStarting() or self.isRunning() or self.isStopping()

# Deterministic random number generator
class Dice():
    seeded = False  # static, uninitialized

    @classmethod
    def seed(cls, s):  # static
        if (cls.seeded):
            raise RuntimeError(
                "Cannot seed the random generator more than once")
        cls.verifyRNG()
        random.seed(s)
        cls.seeded = True  # TODO: protect against multi-threading

    @classmethod
    def verifyRNG(cls):  # Verify that the RNG is determinstic
        random.seed(0)
        x1 = random.randrange(0, 1000)
        x2 = random.randrange(0, 1000)
        x3 = random.randrange(0, 1000)
        if (x1 != 864 or x2 != 394 or x3 != 776):
            raise RuntimeError("System RNG is not deterministic")

    @classmethod
    def throw(cls, stop):  # get 0 to stop-1
        return cls.throwRange(0, stop)

    @classmethod
    def throwRange(cls, start, stop):  # up to stop-1
        if (not cls.seeded):
            raise RuntimeError("Cannot throw dice before seeding it")
        return random.randrange(start, stop)

    @classmethod
    def choice(cls, cList):
        return random.choice(cList)

class Helper:
    @classmethod
    def convertErrno(cls, errno):
        return errno if (errno > 0) else 0x80000000 + errno

    @classmethod
    def getFriendlyPath(cls, path): # returns .../xxx/yyy
        ht1 = os.path.split(path)
        ht2 = os.path.split(ht1[0])
        return ".../" + ht2[1] + '/' + ht1[1]


class Progress:
    STEP_BOUNDARY = 0
    BEGIN_THREAD_STEP = 1
    END_THREAD_STEP   = 2
    SERVICE_HEART_BEAT= 3
    SERVICE_RECONNECT_START     = 4
    SERVICE_RECONNECT_SUCCESS   = 5
    SERVICE_RECONNECT_FAILURE   = 6
    SERVICE_START_NAP           = 7
    CREATE_TABLE_ATTEMPT        = 8
    QUERY_GROUP_BY              = 9
    CONCURRENT_INSERTION        = 10
    ACCEPTABLE_ERROR            = 11

    tokens = {
        STEP_BOUNDARY:      '.',
        BEGIN_THREAD_STEP:  ' [',
        END_THREAD_STEP:    ']',
        SERVICE_HEART_BEAT: '.Y.',
        SERVICE_RECONNECT_START:    '<r.',
        SERVICE_RECONNECT_SUCCESS:  '.r>',
        SERVICE_RECONNECT_FAILURE:  '.xr>',
        SERVICE_START_NAP:           '_zz',
        CREATE_TABLE_ATTEMPT:       'c',
        QUERY_GROUP_BY:             'g',
        CONCURRENT_INSERTION:       'x',
        ACCEPTABLE_ERROR:           '_',
    }

    @classmethod
    def emit(cls, token):
        print(cls.tokens[token], end="", flush=True)

    @classmethod
    def emitStr(cls, str):
        print('({})'.format(str), end="", flush=True)
