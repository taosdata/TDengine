from __future__ import annotations

import sys
import time
import threading
import requests
from requests.auth import HTTPBasicAuth

import taos
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.log import *

from .misc import Logging, CrashGenError, Helper, Dice
import os
import datetime
# from .service_manager import TdeInstance

class DbConn:
    TYPE_NATIVE = "native-c"
    TYPE_REST =   "rest-api"
    TYPE_INVALID = "invalid"

    @classmethod
    def create(cls, connType, dbTarget):
        if connType == cls.TYPE_NATIVE:
            return DbConnNative(dbTarget)
        elif connType == cls.TYPE_REST:
            return DbConnRest(dbTarget)
        else:
            raise RuntimeError(
                "Unexpected connection type: {}".format(connType))

    @classmethod
    def createNative(cls, dbTarget) -> DbConn:
        return cls.create(cls.TYPE_NATIVE, dbTarget)

    @classmethod
    def createRest(cls, dbTarget) -> DbConn:
        return cls.create(cls.TYPE_REST, dbTarget)

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
            raise taos.error.ProgrammingError(
                "Unexpected result for query: {}, rows = {}".format(sql, nRows), 
                (0x991 if nRows==0 else 0x992)
            )
        if self.getResultRows() != 1 or self.getResultCols() != 1:
            raise RuntimeError("Unexpected result set for query: {}".format(sql))
        return self.getQueryResult()[0][0]

    def use(self, dbName):
        self.execute("use {}".format(dbName))

    def existsDatabase(self, dbName: str):
        ''' Check if a certain database exists '''
        self.query("show databases")
        dbs = [v[0] for v in self.getQueryResult()] # ref: https://stackoverflow.com/questions/643823/python-list-transformation
        # ret2 = dbName in dbs
        # print("dbs = {}, str = {}, ret2={}, type2={}".format(dbs, dbName,ret2, type(dbName)))
        return dbName in dbs # TODO: super weird type mangling seen, once here

    def hasTables(self):
        return self.query("show tables") > 0

    def execute(self, sql):
        ''' Return the number of rows affected'''
        raise RuntimeError("Unexpected execution, should be overriden")

    def safeExecute(self, sql):
        '''Safely execute any SQL query, returning True/False upon success/failure'''
        try:
            self.execute(sql)
            return True # ignore num of results, return success
        except taos.error.ProgrammingError as err:
            return False # failed, for whatever TAOS reason
        # Not possile to reach here, non-TAOS exception would have been thrown

    def query(self, sql) -> int: # return num rows returned
        ''' Return the number of rows affected'''
        raise RuntimeError("Unexpected execution, should be overriden")

    def openByType(self):
        raise RuntimeError("Unexpected execution, should be overriden")

    def getQueryResult(self):
        raise RuntimeError("Unexpected execution, should be overriden")

    def getResultRows(self):
        raise RuntimeError("Unexpected execution, should be overriden")

    def getResultCols(self):
        raise RuntimeError("Unexpected execution, should be overriden")

# Sample: curl -u root:taosdata -d "show databases" localhost:6020/rest/sql


class DbConnRest(DbConn):
    REST_PORT_INCREMENT = 11

    def __init__(self, dbTarget: DbTarget):
        super().__init__(dbTarget)
        self._type = self.TYPE_REST
        restPort = dbTarget.port + 11
        self._url = "http://{}:{}/rest/sql".format(
            dbTarget.hostAddr, dbTarget.port + self.REST_PORT_INCREMENT)
        self._result = None

    def openByType(self):  # Open connection        
        pass  # do nothing, always open

    def close(self):
        if (not self.isOpen):
            raise RuntimeError("Cannot clean up database until connection is open")
        # Do nothing for REST
        Logging.debug("[DB] REST Database connection closed")
        self.isOpen = False

    def _doSql(self, sql):
        self._lastSql = sql # remember this, last SQL attempted
        try:
            r = requests.post(self._url, 
                data = sql,
                auth = HTTPBasicAuth('root', 'taosdata'))         
        except:
            print("REST API Failure (TODO: more info here)")
            raise
        rj = r.json()
        # Sanity check for the "Json Result"
        if ('status' not in rj):
            raise RuntimeError("No status in REST response")

        if rj['status'] == 'error':  # clearly reported error
            if ('code' not in rj):  # error without code
                raise RuntimeError("REST error return without code")
            errno = rj['code']  # May need to massage this in the future
            # print("Raising programming error with REST return: {}".format(rj))
            raise taos.error.ProgrammingError(
                rj['desc'], errno)  # todo: check existance of 'desc'

        if rj['status'] != 'succ':  # better be this
            raise RuntimeError(
                "Unexpected REST return status: {}".format(
                    rj['status']))

        nRows = rj['rows'] if ('rows' in rj) else 0
        self._result = rj
        return nRows

    def execute(self, sql):
        if (not self.isOpen):
            raise RuntimeError(
                "Cannot execute database commands until connection is open")
        Logging.debug("[SQL-REST] Executing SQL: {}".format(sql))
        nRows = self._doSql(sql)
        Logging.debug(
            "[SQL-REST] Execution Result, nRows = {}, SQL = {}".format(nRows, sql))
        return nRows

    def query(self, sql):  # return rows affected
        return self.execute(sql)

    def getQueryResult(self):
        return self._result['data']

    def getResultRows(self):
        print(self._result)
        raise RuntimeError("TBD") # TODO: finish here to support -v under -c rest
        # return self._tdSql.queryRows

    def getResultCols(self):
        print(self._result)
        raise RuntimeError("TBD")

    # Duplicate code from TDMySQL, TODO: merge all this into DbConnNative


class MyTDSql:
    # Class variables
    _clsLock = threading.Lock() # class wide locking
    longestQuery = None # type: str
    longestQueryTime = 0.0 # seconds
    lqStartTime = 0.0
    # lqEndTime = 0.0 # Not needed, as we have the two above already

    def __init__(self, hostAddr, cfgPath):
        # Make the DB connection
        self._conn = taos.connect(host=hostAddr, config=cfgPath) 
        self._cursor = self._conn.cursor()

        self.queryRows = 0
        self.queryCols = 0
        self.affectedRows = 0

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
        ret = self._cursor.execute(sql)
        # print("\nSQL success: {}".format(sql))
        queryTime =  time.time() - startTime
        # Record the query time
        cls = self.__class__
        if queryTime > (cls.longestQueryTime + 0.01) :
            with cls._clsLock:
                cls.longestQuery = sql
                cls.longestQueryTime = queryTime
                cls.lqStartTime = startTime
        return ret

    def query(self, sql):
        self.sql = sql
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
        try:
            self.affectedRows = self._execInternal(sql)
        except Exception as e:
            # caller = inspect.getframeinfo(inspect.stack()[1][0])
            # args = (caller.filename, caller.lineno, sql, repr(e))
            # tdLog.exit("%s(%d) failed: sql:%s, %s" % args)
            raise
        return self.affectedRows

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

    def __init__(self, dbTarget):
        super().__init__(dbTarget)
        self._type = self.TYPE_NATIVE
        self._conn = None
        # self._cursor = None        

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
            raise RuntimeError("Cannot execute database commands until connection is open")
        Logging.debug("[SQL] Executing SQL: {}".format(sql))
        self._lastSql = sql
        nRows = self._tdSql.execute(sql)
        Logging.debug(
            "[SQL] Execution Result, nRows = {}, SQL = {}".format(
                nRows, sql))
        return nRows

    def query(self, sql):  # return rows affected
        if (not self.isOpen):
            raise RuntimeError(
                "Cannot query database until connection is open")
        Logging.debug("[SQL] Executing SQL: {}".format(sql))
        self._lastSql = sql
        nRows = self._tdSql.query(sql)
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


class DbManager():
    ''' This is a wrapper around DbConn(), to make it easier to use. 

        TODO: rename this to DbConnManager
    '''
    def __init__(self, cType, dbTarget):
        # self.tableNumQueue = LinearQueue() # TODO: delete?
        # self.openDbServerConnection()
        self._dbConn = DbConn.createNative(dbTarget) if (
            cType == 'native') else DbConn.createRest(dbTarget)
        try:
            self._dbConn.open()  # may throw taos.error.ProgrammingError: disconnected
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

    def getDbConn(self):
        return self._dbConn

    # TODO: not used any more, to delete
    def pickAndAllocateTable(self):  # pick any table, and "use" it
        return self.tableNumQueue.pickAndAllocate()

    # TODO: Not used any more, to delete
    def addTable(self):
        with self._lock:
            tIndex = self.tableNumQueue.push()
        return tIndex

    # Not used any more, to delete
    def releaseTable(self, i):  # return the table back, so others can use it
        self.tableNumQueue.release(i)    

    # TODO: not used any more, delete
    def getTableNameToDelete(self):
        tblNum = self.tableNumQueue.pop()  # TODO: race condition!
        if (not tblNum):  # maybe false
            return False

        return "table_{}".format(tblNum)

    def cleanUp(self):
        self._dbConn.close()

