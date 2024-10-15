from __future__ import annotations

import sys
import os
import datetime
import time
import threading
import requests
from requests.auth import HTTPBasicAuth
from taos.error import SchemalessError

import taos
import taosws
import taosrest
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.log import *
from util.types import TDSmlProtocolType, TDSmlTimestampType

import traceback
# from .service_manager import TdeInstance

from .config import Config
from .misc import Logging, CrashGenError, Helper
from .types import QueryResult

class DbConn:
    TYPE_NATIVE = "native-c"
    TYPE_REST =   "rest-api"
    TYPE_WS =    "ws"
    TYPE_INVALID = "invalid"

    # class variables
    lastSqlFromThreads : dict[int, str] = {} # stored by thread id, obtained from threading.current_thread().ident%10000
    spendThreads : dict[int, float] = {} # stored by thread id, obtained from threading.current_thread().ident%10000
    current_time : dict[int, float] = {}  # save current time 
    @classmethod
    def saveSqlForCurrentThread(cls, sql: str):
        '''
        Let us save the last SQL statement on a per-thread basis, so that when later we
        run into a dead-lock situation, we can pick out the deadlocked thread, and use
        that information to find what what SQL statement is stuck.
        '''

        th = threading.current_thread()
        shortTid = th.native_id % 10000 #type: ignore
        cls.lastSqlFromThreads[shortTid] = sql # Save this for later
        cls.record_save_sql_time()

    @classmethod
    def fetchSqlForThread(cls, shortTid : int) -> str :

        print("=======================")
        if shortTid not in cls.lastSqlFromThreads:
            raise CrashGenError("No last-attempted-SQL found for thread id: {}".format(shortTid))
        return cls.lastSqlFromThreads[shortTid]

    @classmethod
    def get_save_sql_time(cls, shortTid : int):
        '''
        Let us save the last SQL statement on a per-thread basis, so that when later we
        run into a dead-lock situation, we can pick out the deadlocked thread, and use
        that information to find what what SQL statement is stuck.
        '''
        return cls.current_time[shortTid]

    @classmethod
    def record_save_sql_time(cls):
        '''
        Let us save the last SQL statement on a per-thread basis, so that when later we
        run into a dead-lock situation, we can pick out the deadlocked thread, and use
        that information to find what what SQL statement is stuck.
        '''
        th = threading.current_thread()
        shortTid = th.native_id % 10000 #type: ignore
        cls.current_time[shortTid] = float(time.time()) # Save this for later

    @classmethod
    def sql_exec_spend(cls, cost: float):
        '''
        Let us save the last SQL statement on a per-thread basis, so that when later we
        run into a dead-lock situation, we can pick out the deadlocked thread, and use
        that information to find what what SQL statement is stuck.
        '''
        th = threading.current_thread()
        shortTid = th.native_id % 10000 #type: ignore
        cls.spendThreads[shortTid] = cost # Save this for later

    @classmethod
    def get_time_cost(cls) ->float:
        th = threading.current_thread()
        shortTid = th.native_id % 10000 #type: ignore
        return cls.spendThreads.get(shortTid)

    @classmethod
    def create(cls, connType, dbTarget, dbName=""):
        if connType == cls.TYPE_NATIVE:
            return DbConnNative(dbTarget)
        elif connType == cls.TYPE_REST:
            return DbConnRest(dbTarget)
        elif connType == cls.TYPE_WS:
            return DbConnWS(dbTarget, dbName)
        else:
            raise RuntimeError(
                "Unexpected connection type: {}".format(connType))

    @classmethod
    def createNative(cls, dbTarget) -> DbConn:
        return cls.create(cls.TYPE_NATIVE, dbTarget)


    @classmethod
    def createRest(cls, dbTarget) -> DbConn:
        return cls.create(cls.TYPE_REST, dbTarget)

    @classmethod
    def createWs(cls, dbTarget, dbName="") -> DbConn:
        return cls.create(cls.TYPE_WS, dbTarget, dbName)

    def __init__(self, dbTarget):
        self.isOpen = False
        self._type = self.TYPE_INVALID
        self._lastSql = None
        self._dbTarget = dbTarget

    def __repr__(self):
        return "[DbConn: type={}, target={}]".format(self._type, self._dbTarget)

    def getLastSql(self):
        return self._lastSql

    def open(self):
        if (self.isOpen):
            raise RuntimeError("Cannot re-open an existing DB connection")

        # below implemented by child classes
        self.openByType()

        Logging.debug("[DB] data connection opened: {}".format(self))
        self.isOpen = True

    def close(self):
        raise RuntimeError("Unexpected execution, should be overriden")

    def queryScalar(self, sql) -> int:
        return self._queryAny(sql)

    def queryString(self, sql) -> str:
        return self._queryAny(sql)

    def _queryAny(self, sql):  # actual query result as an int
        if (not self.isOpen):
            raise RuntimeError("Cannot query database until connection is open")
        nRows = self.query(sql)
        if nRows != 1:
            raise CrashGenError(
                "Unexpected result for query: {}, rows = {}".format(sql, nRows), 
                (CrashGenError.INVALID_EMPTY_RESULT if nRows==0 else CrashGenError.INVALID_MULTIPLE_RESULT)
            )
        if self.getResultRows() != 1 or self.getResultCols() != 1:
            raise RuntimeError("Unexpected result set for query: {}".format(sql))
        return self.getQueryResult()[0][0]

    def use(self, dbName):
        self.execute("use {}".format(dbName))

    def existsDatabase(self, dbName: str):
        ''' Check if a certain database exists '''
        self.query("select * from information_schema.ins_databases")
        dbs = [v[0] for v in self.getQueryResult()] # ref: https://stackoverflow.com/questions/643823/python-list-transformation
        # ret2 = dbName in dbs
        # print("dbs = {}, str = {}, ret2={}, type2={}".format(dbs, dbName,ret2, type(dbName)))
        return dbName in dbs # TODO: super weird type mangling seen, once here

    def existsSuperTable(self, dbName, stName):
        self.query(f"show {dbName}.stables")
        sts = [v[0] for v in self.getQueryResult()]
        return stName in sts

    def hasTables(self, dbName):
        return self.query(f"show {dbName}.tables") > 0

    def execute(self, sql):
        ''' Return the number of rows affected'''
        raise RuntimeError("Unexpected execution, should be overriden")

    def safeExecute(self, sql):
        '''Safely execute any SQL query, returning True/False upon success/failure'''
        try:
            self.execute(sql)
            return True # ignore num of results, return success
        except taos.error.Error as err:
            return False # failed, for whatever TAOS reason
        # Not possile to reach here, non-TAOS exception would have been thrown

    def query(self, sql) -> int: # return num rows returned
        ''' Return the number of rows affected'''
        raise RuntimeError("Unexpected execution, should be overriden")

    def openByType(self):
        raise RuntimeError("Unexpected execution, should be overriden")

    def getQueryResult(self) -> QueryResult :
        raise RuntimeError("Unexpected execution, should be overriden")

    def getResultRows(self):
        raise RuntimeError("Unexpected execution, should be overriden")

    def getResultCols(self):
        raise RuntimeError("Unexpected execution, should be overriden")

    def influxdbLineInsert(self, line, ts_type=None, dbName=""):
        if self._type != self.TYPE_NATIVE:
            return
        raise RuntimeError("Unexpected execution, should be overriden")

# Sample: curl -u root:taosdata -d "select * from information_schema.ins_databases" localhost:6020/rest/sql


class DbConnRest(DbConn):
    # Class variables
    _lock = threading.Lock()
    # _connInfoDisplayed = False # TODO: find another way to display this
    totalConnections = 0 # Not private
    totalRequests = 0
    time_cost = -1
    REST_PORT_INCREMENT = 11

    def __init__(self, dbTarget):
        super().__init__(dbTarget)
        self._type = self.TYPE_WS
        self._conn = None
        self._restPort = dbTarget.port + self.REST_PORT_INCREMENT
        self._result = None

    @classmethod
    def resetTotalRequests(cls):
        with cls._lock: # force single threading for opening DB connections. # TODO: whaaat??!!!
            cls.totalRequests = 0

    def openByType(self):  # Open connection
        # global gContainer
        # tInst = tInst or gContainer.defTdeInstance # set up in ClientManager, type: TdeInstance
        # cfgPath = self.getBuildPath() + "/test/cfg"
        # cfgPath  = tInst.getCfgDir()
        # hostAddr = tInst.getHostAddr()

        cls = self.__class__ # Get the class, to access class variables
        with cls._lock: # force single threading for opening DB connections. # TODO: whaaat??!!!
            dbTarget = self._dbTarget
            # if not cls._connInfoDisplayed:
            #     cls._connInfoDisplayed = True # updating CLASS variable
            Logging.debug("Initiating TAOS native connection to {}".format(dbTarget))
            # Make the connection
            # self._conn = taos.connect(host=hostAddr, config=cfgPath)  # TODO: make configurable
            # self._cursor = self._conn.cursor()
            # Record the count in the class
            self._tdSql = MyTDSql(dbTarget.hostAddr, dbTarget.cfgPath, self.TYPE_REST, self._restPort) # making DB connection
            cls.totalConnections += 1

        self._tdSql.execute('reset query cache')
        # self._cursor.execute('use db') # do this at the beginning of every

        # Open connection
        # self._tdSql = MyTDSql()
        # self._tdSql.init(self._cursor)

    def close(self):
        if (not self.isOpen):
            raise RuntimeError("Cannot clean up database until connection is open")
        self._tdSql.close()
        # Decrement the class wide counter
        cls = self.__class__ # Get the class, to access class variables
        with cls._lock:
            cls.totalConnections -= 1

        Logging.debug("[DB] Database connection closed")
        self.isOpen = False

    def execute(self, sql):
        if (not self.isOpen):
            traceback.print_stack()
            raise CrashGenError(
                "Cannot exec SQL unless db connection is open", CrashGenError.DB_CONNECTION_NOT_OPEN)
        Logging.debug("[SQL] Executing SQL: {}".format(sql))
        self._lastSql = sql
        time_cost = -1
        nRows = 0
        time_start = time.time()
        self.saveSqlForCurrentThread(sql) # Save in global structure too. #TODO: combine with above
        try:
            nRows= self._tdSql.execute(sql)
        except Exception as e:
            self.sql_exec_spend(-2)
        finally:
            time_cost =  time.time() - time_start
            self.sql_exec_spend(time_cost)

        cls = self.__class__
        cls.totalRequests += 1
        Logging.debug(
            "[SQL] Execution Result, nRows = {}, SQL = {}".format(
                nRows, sql))
        return nRows

    def query(self, sql):  # return rows affected
        if (not self.isOpen):
            traceback.print_stack()
            raise CrashGenError(
                "Cannot query database until connection is open, restarting?", CrashGenError.DB_CONNECTION_NOT_OPEN)
        Logging.debug("[SQL] Executing SQL: {}".format(sql))
        self._lastSql = sql
        self.saveSqlForCurrentThread(sql) # Save in global structure too. #TODO: combine with above
        nRows = self._tdSql.query(sql)
        cls = self.__class__
        cls.totalRequests += 1
        Logging.debug(
            "[SQL] Query Result, nRows = {}, SQL = {}".format(
                nRows, sql))
        return nRows
        # results are in: return self._tdSql.queryResult

    def getQueryResult(self):
        return self._tdSql.queryResult

    def getResultRows(self):
        return self._tdSql.queryRows

    def getResultCols(self):
        return self._tdSql.queryCols

    def influxdbLineInsert(self, line, ts_type=None, dbName=""):
        Logging.debug("Not supported by rest-api")
        return

    # Duplicate code from TDMySQL, TODO: merge all this into DbConnNative

# class DbConnRest.bak(DbConn):
#     REST_PORT_INCREMENT = 11

#     def __init__(self, dbTarget: DbTarget):
#         super().__init__(dbTarget)
#         self._type = self.TYPE_REST
#         restPort = dbTarget.port + 11
#         self._url = "http://{}:{}/rest/sql".format(
#             dbTarget.hostAddr, dbTarget.port + self.REST_PORT_INCREMENT)
#         self._result = None

#     def openByType(self):  # Open connection
#         pass  # do nothing, always open

#     def close(self):
#         if (not self.isOpen):
#             raise RuntimeError("Cannot clean up database until connection is open")
#         # Do nothing for REST
#         Logging.debug("[DB] REST Database connection closed")
#         self.isOpen = False

#     def _doSql(self, sql):
#         self._lastSql = sql # remember this, last SQL attempted
#         self.saveSqlForCurrentThread(sql) # Save in global structure too. #TODO: combine with above
#         time_cost = -1
#         time_start = time.time()
#         try:
#             r = requests.post(self._url,
#                 data = sql,
#                 auth = HTTPBasicAuth('root', 'taosdata'))
#         except:
#             print("REST API Failure (TODO: more info here)")
#             self.sql_exec_spend(-2)
#             raise
#         finally:
#             time_cost = time.time()- time_start
#             self.sql_exec_spend(time_cost)
#         rj = r.json()
#         print("------rj-------", rj)

#         if ('code' not in rj):  # error without code
#             raise RuntimeError("REST error return without code")

#         returnCode = rj['code']  # May need to massage this in the future
#         if returnCode != 0:  # better be this
#             raise RuntimeError(f"sql: {sql} - Unexpected REST return code: {returnCode}, full response: {r.text}")  # Properly raise an exception with a message
#             return returnCode
#             raise RuntimeError(
#                 "sql: {} - Unexpected REST return: {} ".format(
#                     sql, r.text))

#         nRows = rj['rows'] if ('rows' in rj) else 0
#         self._result = rj
#         return nRows

#     def execute(self, sql):
#         if (not self.isOpen):
#             raise RuntimeError(
#                 "Cannot execute database commands until connection is open")
#         Logging.debug("[SQL-REST] Executing SQL: {}".format(sql))
#         nRows = self._doSql(sql)
#         Logging.debug(
#             "[SQL-REST] Execution Result, nRows = {}, SQL = {}".format(nRows, sql))
#         return nRows

#     def query(self, sql):  # return rows affected
#         return self.execute(sql)

#     def getQueryResult(self):
#         # print("----self._result", self._result)
#         return self._result['data']

#     def getResultRows(self):
#         # print(self._result)
#         raise RuntimeError("TBD") # TODO: finish here to support -v under -c rest
#         # return self._tdSql.queryRows

#     def getResultCols(self):
#         # print(self._result)
#         raise RuntimeError("TBD")

#     # Duplicate code from TDMySQL, TODO: merge all this into DbConnNative


class DbConnWS(DbConn):
    # Class variables
    _lock = threading.Lock()
    # _connInfoDisplayed = False # TODO: find another way to display this
    totalConnections = 0 # Not private
    totalRequests = 0
    time_cost = -1
    WS_PORT_INCREMENT = 11

    def __init__(self, dbTarget: DbTarget, dbName=""):
        super().__init__(dbTarget)
        self._type = self.TYPE_WS
        self._conn = None
        self._wsPort = dbTarget.port + self.WS_PORT_INCREMENT
        self._result = None
        self._dbName = dbName

    @classmethod
    def resetTotalRequests(cls):
        with cls._lock: # force single threading for opening DB connections. # TODO: whaaat??!!!
            cls.totalRequests = 0

    def openByType(self):  # Open connection
        # global gContainer
        # tInst = tInst or gContainer.defTdeInstance # set up in ClientManager, type: TdeInstance
        # cfgPath = self.getBuildPath() + "/test/cfg"
        # cfgPath  = tInst.getCfgDir()
        # hostAddr = tInst.getHostAddr()

        cls = self.__class__ # Get the class, to access class variables
        with cls._lock: # force single threading for opening DB connections. # TODO: whaaat??!!!
            dbTarget = self._dbTarget
            # if not cls._connInfoDisplayed:
            #     cls._connInfoDisplayed = True # updating CLASS variable
            Logging.debug("Initiating TAOS native connection to {}".format(dbTarget))
            # Make the connection
            # self._conn = taos.connect(host=hostAddr, config=cfgPath)  # TODO: make configurable
            # self._cursor = self._conn.cursor()
            # Record the count in the class
            self._tdSql = MyTDSql(dbTarget.hostAddr, dbTarget.cfgPath, self.TYPE_WS, self._wsPort, self._dbName) # making DB connection
            cls.totalConnections += 1

        self._tdSql.execute('reset query cache')
        # self._cursor.execute('use db') # do this at the beginning of every

        # Open connection
        # self._tdSql = MyTDSql()
        # self._tdSql.init(self._cursor)

    def close(self):
        if (not self.isOpen):
            raise RuntimeError("Cannot clean up database until connection is open")
        self._tdSql.close()
        # Decrement the class wide counter
        cls = self.__class__ # Get the class, to access class variables
        with cls._lock:
            cls.totalConnections -= 1

        Logging.debug("[DB] Database connection closed")
        self.isOpen = False

    def execute(self, sql):
        if (not self.isOpen):
            traceback.print_stack()
            raise CrashGenError(
                "Cannot exec SQL unless db connection is open", CrashGenError.DB_CONNECTION_NOT_OPEN)
        Logging.debug("[SQL] Executing SQL: {}".format(sql))
        self._lastSql = sql
        time_cost = -1
        nRows = 0
        time_start = time.time()
        self.saveSqlForCurrentThread(sql) # Save in global structure too. #TODO: combine with above
        try:
            nRows= self._tdSql.execute(sql)
        except Exception as e:
            self.sql_exec_spend(-2)
        finally:
            time_cost =  time.time() - time_start
            self.sql_exec_spend(time_cost)

        cls = self.__class__
        cls.totalRequests += 1
        Logging.debug(
            "[SQL] Execution Result, nRows = {}, SQL = {}".format(
                nRows, sql))
        return nRows

    def query(self, sql):  # return rows affected
        if (not self.isOpen):
            traceback.print_stack()
            raise CrashGenError(
                "Cannot query database until connection is open, restarting?", CrashGenError.DB_CONNECTION_NOT_OPEN)
        Logging.debug("[SQL] Executing SQL: {}".format(sql))
        self._lastSql = sql
        self.saveSqlForCurrentThread(sql) # Save in global structure too. #TODO: combine with above
        nRows = self._tdSql.query(sql)
        cls = self.__class__
        cls.totalRequests += 1
        Logging.debug(
            "[SQL] Query Result, nRows = {}, SQL = {}".format(
                nRows, sql))
        return nRows
        # results are in: return self._tdSql.queryResult

    def getQueryResult(self):
        return self._tdSql.queryResult

    def getResultRows(self):
        return self._tdSql.queryRows

    def getResultCols(self):
        return self._tdSql.queryCols

    def influxdbLineInsert(self, line, ts_type=None, dbName=""):
        return self._tdSql.influxdbLineInsertWs(line, ts_type, dbName)

    def stmtStatement(self, sql):
        stmt = self._tdSql._conn.statement()
        stmt.prepare(sql)
        return stmt

class MyTDSql:
    # Class variables
    _clsLock = threading.Lock() # class wide locking
    longestQuery = '' # type: str
    longestQueryTime = 0.0 # seconds
    lqStartTime = 0.0
    # lqEndTime = 0.0 # Not needed, as we have the two above already

    def __init__(self, hostAddr, cfgPath, connType='native-c', port=6030, dbName=""):
        self.url = f"http://{hostAddr}:{port}"
        # Make the DB connection
        if connType == 'native-c':
            self._conn = taos.connect(host=hostAddr, config=cfgPath)
        elif connType == 'rest-api':
            self._conn = taosrest.connect(url=self.url)
        else:
            self._conn = taosws.connect(host=hostAddr, port=port, database=dbName)
        self._cursor = self._conn.cursor()
        self.cfgPath = cfgPath
        self.queryRows = 0
        self.queryCols = 0
        self.affectedRows = 0
        self.line = str()
        self._hostAddr = hostAddr
        self._port = port

    # def init(self, cursor, log=True):
    #     self.cursor = cursor
        # if (log):
        #     caller = inspect.getframeinfo(inspect.stack()[1][0])
        #     self.cursor.log(caller.filename + ".sql")

    def close(self):
        self._cursor.close() # can we double close?
        self._conn.close() # TODO: very important, cursor close does NOT close DB connection!
        self._cursor.close()

    def _execInternal(self, sql):
        startTime = time.time()
        # Logging.debug("Executing SQL: " + sql)
        # ret = None # TODO: use strong type here
        # try: # Let's not capture the error, and let taos.error.ProgrammingError pass through
        ret = self._cursor.execute(sql)
        # except taos.error.ProgrammingError as err:
        #     Logging.warning("Taos SQL execution error: {}, SQL: {}".format(err.msg, sql))
        #     raise CrashGenError(err.msg)

        # print("\nSQL success: {}".format(sql))
        queryTime =  time.time() - startTime
        # Record the query time
        cls = self.__class__
        if queryTime > (cls.longestQueryTime + 0.01) :
            with cls._clsLock:
                cls.longestQuery = sql
                cls.longestQueryTime = queryTime
                cls.lqStartTime = startTime

        # Now write to the shadow database
        if Config.isSet('use_shadow_db'):
            if sql[:11] == "INSERT INTO":
                if sql[:16] == "INSERT INTO db_0":
                    sql2 = "INSERT INTO db_s" + sql[16:]
                    self._cursor.execute(sql2)
                else:
                    raise CrashGenError("Did not find db_0 in INSERT statement: {}".format(sql))
            else: # not an insert statement
                pass

            if sql[:12] == "CREATE TABLE":
                if sql[:17] == "CREATE TABLE db_0":
                    sql2 = sql.replace('db_0', 'db_s')
                    self._cursor.execute(sql2)
                else:
                    raise CrashGenError("Did not find db_0 in CREATE TABLE statement: {}".format(sql))
            else: # not an insert statement
                pass
        return ret

    def query(self, sql):
        self.sql = sql
        # print(self.sql)
        self.recordSql(self.sql)
        try:
            self._execInternal(sql)
            self.queryResult = self._cursor.fetchall()
            self.queryRows = len(self.queryResult)
            self.queryCols = len(self._cursor.description)
        except Exception as e:
            # caller = inspect.getframeinfo(inspect.stack()[1][0])
            # args = (caller.filename, caller.lineno, sql, repr(e))
            # tdLog.exit("%s(%d) failed: sql:%s, %s" % args)
            raise
        return self.queryRows

    def execute(self, sql):
        self.sql = sql
        # print(self.sql)
        self.recordSql(self.sql)
        try:
            self.affectedRows = self._execInternal(sql)
        except Exception as e:
            # caller = inspect.getframeinfo(inspect.stack()[1][0])
            # args = (caller.filename, caller.lineno, sql, repr(e))
            # tdLog.exit("%s(%d) failed: sql:%s, %s" % args)
            raise
        return self.affectedRows

    def recordSql(self, sql):
        sql_file = os.path.join(os.path.dirname(self.cfgPath), "log/sql.txt")
        with open(sql_file, 'a') as f:
            if sql.endswith(";"):
                f.write(f'{sql}\n')
            else:
                f.write(f'{sql};\n')

    def recordSmlLine(self, line):
        line_file = os.path.join(os.path.dirname(self.cfgPath), "log/sml_line.txt")
        with open(line_file, 'a') as f:
            f.write(f'{line}\n')

    def influxdbLineInsertNative(self, line, ts_type=None, dbName=""):
        precision = None if ts_type is None else ts_type
        try:
            self._conn.execute(f'use {dbName}')
            self._conn.schemaless_insert(line, TDSmlProtocolType.LINE.value, precision)
            self.recordSmlLine(line)
            # Logging.info(f"Inserted influxDb Line: {line}")
        except SchemalessError as e:
            Logging.error(f"SchemalessError: {e}-{line}")
            raise

    def influxdbLineInsertWs(self, line, ts_type=None, dbName=""):
        precision = None if ts_type is None else ts_type
        try:
            # TODO refactor
            # TODO add database to source conn, not redefine here
            self._conn = taosws.connect(host=self._hostAddr, port=self._port, database=dbName)
            self._conn.schemaless_insert(line, taosws.PySchemalessProtocol.Line, precision, 1, 1)
            self.recordSmlLine(line)
            # Logging.info(f"Inserted influxDb Line: {line}")
        except SchemalessError as e:
            Logging.error(f"SchemalessError: {e}-{line}")
            raise
        finally:
            self._conn.close()

    def openTsdbTelnetLineInsert(self, line, ts_type=None):
        # TODO finish
        precision = None if ts_type is None else ts_type
        self._conn.schemaless_insert([line], TDSmlProtocolType.TELNET.value, precision)

class DbTarget:
    def __init__(self, cfgPath, hostAddr, port):
        self.cfgPath  = cfgPath
        self.hostAddr = hostAddr
        self.port     = port

    def __repr__(self):
        return "[DbTarget: cfgPath={}, host={}:{}]".format(
            Helper.getFriendlyPath(self.cfgPath), self.hostAddr, self.port)

    def getEp(self):
        return "{}:{}".format(self.hostAddr, self.port)

class DbConnNative(DbConn):
    # Class variables
    _lock = threading.Lock()
    # _connInfoDisplayed = False # TODO: find another way to display this
    totalConnections = 0 # Not private
    totalRequests = 0
    time_cost = -1

    def __init__(self, dbTarget):
        super().__init__(dbTarget)
        self._type = self.TYPE_NATIVE
        self._conn = None
        # self._cursor = None

    @classmethod
    def resetTotalRequests(cls):
        with cls._lock: # force single threading for opening DB connections. # TODO: whaaat??!!!
            cls.totalRequests = 0

    def openByType(self):  # Open connection
        # global gContainer
        # tInst = tInst or gContainer.defTdeInstance # set up in ClientManager, type: TdeInstance
        # cfgPath = self.getBuildPath() + "/test/cfg"
        # cfgPath  = tInst.getCfgDir()
        # hostAddr = tInst.getHostAddr()

        cls = self.__class__ # Get the class, to access class variables
        with cls._lock: # force single threading for opening DB connections. # TODO: whaaat??!!!
            dbTarget = self._dbTarget
            # if not cls._connInfoDisplayed:
            #     cls._connInfoDisplayed = True # updating CLASS variable
            Logging.debug("Initiating TAOS native connection to {}".format(dbTarget))
            # Make the connection
            # self._conn = taos.connect(host=hostAddr, config=cfgPath)  # TODO: make configurable
            # self._cursor = self._conn.cursor()
            # Record the count in the class
            self._tdSql = MyTDSql(dbTarget.hostAddr, dbTarget.cfgPath) # making DB connection
            cls.totalConnections += 1

        self._tdSql.execute('reset query cache')
        # self._cursor.execute('use db') # do this at the beginning of every

        # Open connection
        # self._tdSql = MyTDSql()
        # self._tdSql.init(self._cursor)

    def close(self):
        if (not self.isOpen):
            raise RuntimeError("Cannot clean up database until connection is open")
        self._tdSql.close()
        # Decrement the class wide counter
        cls = self.__class__ # Get the class, to access class variables
        with cls._lock:
            cls.totalConnections -= 1

        Logging.debug("[DB] Database connection closed")
        self.isOpen = False

    def execute(self, sql):
        if (not self.isOpen):
            traceback.print_stack()
            raise CrashGenError(
                "Cannot exec SQL unless db connection is open", CrashGenError.DB_CONNECTION_NOT_OPEN)
        Logging.debug("[SQL] Executing SQL: {}".format(sql))
        self._lastSql = sql
        time_cost = -1
        nRows = 0
        time_start = time.time()
        self.saveSqlForCurrentThread(sql) # Save in global structure too. #TODO: combine with above
        try:
            nRows= self._tdSql.execute(sql)
        except Exception as e:
            self.sql_exec_spend(-2)
        finally:
            time_cost =  time.time() - time_start
            self.sql_exec_spend(time_cost)

        cls = self.__class__
        cls.totalRequests += 1
        Logging.debug(
            "[SQL] Execution Result, nRows = {}, SQL = {}".format(
                nRows, sql))
        return nRows

    def query(self, sql):  # return rows affected
        if (not self.isOpen):
            traceback.print_stack()
            raise CrashGenError(
                "Cannot query database until connection is open, restarting?", CrashGenError.DB_CONNECTION_NOT_OPEN)
        Logging.debug("[SQL] Executing SQL: {}".format(sql))
        self._lastSql = sql
        self.saveSqlForCurrentThread(sql) # Save in global structure too. #TODO: combine with above
        nRows = self._tdSql.query(sql)
        cls = self.__class__
        cls.totalRequests += 1
        Logging.debug(
            "[SQL] Query Result, nRows = {}, SQL = {}".format(
                nRows, sql))
        return nRows
        # results are in: return self._tdSql.queryResult

    def getQueryResult(self):
        return self._tdSql.queryResult

    def getResultRows(self):
        return self._tdSql.queryRows

    def getResultCols(self):
        return self._tdSql.queryCols

    def influxdbLineInsert(self, line, ts_type=None, dbName=""):
        return self._tdSql.influxdbLineInsertNative(line, ts_type, dbName)

    def stmtStatement(self, sql):
        return self._tdSql._conn.statement(sql)



class DbManager():
    ''' This is a wrapper around DbConn(), to make it easier to use.

        TODO: rename this to DbConnManager
    '''
    def __init__(self, cType, dbTarget):
        # self.tableNumQueue = LinearQueue() # TODO: delete?
        # self.openDbServerConnection()
        if cType == 'native':
            self._dbConn = DbConn.createNative(dbTarget)
        elif cType == 'rest':
            self._dbConn = DbConn.createRest(dbTarget)
        elif cType == 'ws':
            self._dbConn = DbConn.createWs(dbTarget)
        else:
            raise RuntimeError("Unexpected connection type: {}".format(cType))
        try:
            self._dbConn.open()  # may throw taos.error.ProgrammingError: disconnected
            Logging.debug("DbManager opened DB connection...")
        except taos.error.ProgrammingError as err:
            # print("Error type: {}, msg: {}, value: {}".format(type(err), err.msg, err))
            if (err.msg == 'client disconnected'):  # cannot open DB connection
                print(
                    "Cannot establish DB connection, please re-run script without parameter, and follow the instructions.")
                sys.exit(2)
            else:
                print("Failed to connect to DB, errno = {}, msg: {}"
                    .format(Helper.convertErrno(err.errno), err.msg))
                raise
        except BaseException:
            print("[=] Unexpected exception")
            raise

        # Do this after dbConn is in proper shape
        # Moved to Database()
        # self._stateMachine = StateMechine(self._dbConn)

    def __del__(self):
        ''' Release the underlying DB connection upon deletion of DbManager '''
        self.cleanUp()

    def getDbConn(self) -> DbConn :
        if self._dbConn is None:
            raise CrashGenError("Unexpected empty DbConn")
        return self._dbConn

    def cleanUp(self):
        if self._dbConn:
            self._dbConn.close()
            self._dbConn = None
            Logging.debug("DbManager closed DB connection...")
