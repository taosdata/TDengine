# -----!/usr/bin/python3.7
###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-
# For type hinting before definition, ref:
# https://stackoverflow.com/questions/33533148/how-do-i-specify-that-the-return-type-of-a-method-is-the-same-as-the-class-itsel
from __future__ import annotations
import taos
from util.sql import *
from util.cases import *
from util.dnodes import *
from util.log import *
from queue import Queue, Empty
from typing import IO
from typing import Set
from typing import Dict
from typing import List
from requests.auth import HTTPBasicAuth
import textwrap
import datetime
import logging
import time
import random
import threading
import requests
import copy
import argparse
import getopt

import sys
import os
import io
import signal
import traceback
import resource
from guppy import hpy
import gc

try:
    import psutil
except:
    print("Psutil module needed, please install: sudo pip3 install psutil")
    sys.exit(-1)

# Require Python 3
if sys.version_info[0] < 3:
    raise Exception("Must be using Python 3")

# Global variables, tried to keep a small number.

# Command-line/Environment Configurations, will set a bit later
# ConfigNameSpace = argparse.Namespace
gConfig = argparse.Namespace()  # Dummy value, will be replaced later
gSvcMgr = None # TODO: refactor this hack, use dep injection
logger = None # type: Logger

def runThread(wt: WorkerThread):
    wt.run()

class CrashGenError(Exception):
    def __init__(self, msg=None, errno=None):
        self.msg = msg
        self.errno = errno

    def __str__(self):
        return self.msg


class WorkerThread:
    def __init__(self, pool: ThreadPool, tid, tc: ThreadCoordinator,
                 # te: TaskExecutor,
                 ):  # note: main thread context!
        # self._curStep = -1
        self._pool = pool
        self._tid = tid
        self._tc = tc  # type: ThreadCoordinator
        # self.threadIdent = threading.get_ident()
        self._thread = threading.Thread(target=runThread, args=(self,))
        self._stepGate = threading.Event()

        # Let us have a DB connection of our own
        if (gConfig.per_thread_db_connection):  # type: ignore
            # print("connector_type = {}".format(gConfig.connector_type))
            if gConfig.connector_type == 'native':
                self._dbConn = DbConn.createNative() 
            elif gConfig.connector_type == 'rest':
                self._dbConn = DbConn.createRest() 
            elif gConfig.connector_type == 'mixed':
                if Dice.throw(2) == 0: # 1/2 chance
                    self._dbConn = DbConn.createNative() 
                else:
                    self._dbConn = DbConn.createRest() 
            else:
                raise RuntimeError("Unexpected connector type: {}".format(gConfig.connector_type))

        # self._dbInUse = False  # if "use db" was executed already

    def logDebug(self, msg):
        logger.debug("    TRD[{}] {}".format(self._tid, msg))

    def logInfo(self, msg):
        logger.info("    TRD[{}] {}".format(self._tid, msg))

    # def dbInUse(self):
    #     return self._dbInUse

    # def useDb(self):
    #     if (not self._dbInUse):
    #         self.execSql("use db")
    #     self._dbInUse = True

    def getTaskExecutor(self):
        return self._tc.getTaskExecutor()

    def start(self):
        self._thread.start()  # AFTER the thread is recorded

    def run(self):
        # initialization after thread starts, in the thread context
        # self.isSleeping = False
        logger.info("Starting to run thread: {}".format(self._tid))

        if (gConfig.per_thread_db_connection):  # type: ignore
            logger.debug("Worker thread openning database connection")
            self._dbConn.open()

        self._doTaskLoop()

        # clean up
        if (gConfig.per_thread_db_connection):  # type: ignore
            if self._dbConn.isOpen: #sometimes it is not open
                self._dbConn.close()
            else:
                logger.warning("Cleaning up worker thread, dbConn already closed")

    def _doTaskLoop(self):
        # while self._curStep < self._pool.maxSteps:
        # tc = ThreadCoordinator(None)
        while True:
            tc = self._tc  # Thread Coordinator, the overall master
            try:
                tc.crossStepBarrier()  # shared barrier first, INCLUDING the last one
            except threading.BrokenBarrierError as err: # main thread timed out
                print("_bto", end="")
                logger.debug("[TRD] Worker thread exiting due to main thread barrier time-out")
                break

            logger.debug("[TRD] Worker thread [{}] exited barrier...".format(self._tid))
            self.crossStepGate()   # then per-thread gate, after being tapped
            logger.debug("[TRD] Worker thread [{}] exited step gate...".format(self._tid))
            if not self._tc.isRunning():
                print("_wts", end="")
                logger.debug("[TRD] Thread Coordinator not running any more, worker thread now stopping...")
                break

            # Before we fetch the task and run it, let's ensure we properly "use" the database (not needed any more)
            try:
                if (gConfig.per_thread_db_connection):  # most likely TRUE
                    if not self._dbConn.isOpen:  # might have been closed during server auto-restart
                        self._dbConn.open()
                # self.useDb() # might encounter exceptions. TODO: catch
            except taos.error.ProgrammingError as err:
                errno = Helper.convertErrno(err.errno)
                if errno in [0x383, 0x386, 0x00B, 0x014]  : # invalid database, dropping, Unable to establish connection, Database not ready
                    # ignore
                    dummy = 0
                else:
                    print("\nCaught programming error. errno=0x{:X}, msg={} ".format(errno, err.msg))
                    raise

            # Fetch a task from the Thread Coordinator
            logger.debug( "[TRD] Worker thread [{}] about to fetch task".format(self._tid))
            task = tc.fetchTask()

            # Execute such a task
            logger.debug("[TRD] Worker thread [{}] about to execute task: {}".format(
                    self._tid, task.__class__.__name__))
            task.execute(self)
            tc.saveExecutedTask(task)
            logger.debug("[TRD] Worker thread [{}] finished executing task".format(self._tid))

            # self._dbInUse = False  # there may be changes between steps
        # print("_wtd", end=None) # worker thread died

    def verifyThreadSelf(self):  # ensure we are called by this own thread
        if (threading.get_ident() != self._thread.ident):
            raise RuntimeError("Unexpectly called from other threads")

    def verifyThreadMain(self):  # ensure we are called by the main thread
        if (threading.get_ident() != threading.main_thread().ident):
            raise RuntimeError("Unexpectly called from other threads")

    def verifyThreadAlive(self):
        if (not self._thread.is_alive()):
            raise RuntimeError("Unexpected dead thread")

    # A gate is different from a barrier in that a thread needs to be "tapped"
    def crossStepGate(self):
        self.verifyThreadAlive()
        self.verifyThreadSelf()  # only allowed by ourselves

        # Wait again at the "gate", waiting to be "tapped"
        logger.debug(
            "[TRD] Worker thread {} about to cross the step gate".format(
                self._tid))
        self._stepGate.wait()
        self._stepGate.clear()

        # self._curStep += 1  # off to a new step...

    def tapStepGate(self):  # give it a tap, release the thread waiting there
        # self.verifyThreadAlive()
        self.verifyThreadMain()  # only allowed for main thread

        if self._thread.is_alive():
            logger.debug("[TRD] Tapping worker thread {}".format(self._tid))
            self._stepGate.set()  # wake up!
            time.sleep(0)  # let the released thread run a bit
        else:
            print("_tad", end="") # Thread already dead

    def execSql(self, sql):  # TODO: expose DbConn directly
        return self.getDbConn().execute(sql)

    def querySql(self, sql):  # TODO: expose DbConn directly
        return self.getDbConn().query(sql)

    def getQueryResult(self):
        return self.getDbConn().getQueryResult()

    def getDbConn(self) -> DbConn :
        if (gConfig.per_thread_db_connection):
            return self._dbConn
        else:
            return self._tc.getDbManager().getDbConn()

    # def querySql(self, sql): # not "execute", since we are out side the DB context
    #     if ( gConfig.per_thread_db_connection ):
    #         return self._dbConn.query(sql)
    #     else:
    #         return self._tc.getDbState().getDbConn().query(sql)

# The coordinator of all worker threads, mostly running in main thread


class ThreadCoordinator:
    WORKER_THREAD_TIMEOUT = 60 # one minute

    def __init__(self, pool: ThreadPool, dbManager: DbManager):
        self._curStep = -1  # first step is 0
        self._pool = pool
        # self._wd = wd
        self._te = None  # prepare for every new step
        self._dbManager = dbManager
        self._executedTasks: List[Task] = []  # in a given step
        self._lock = threading.RLock()  # sync access for a few things

        self._stepBarrier = threading.Barrier(
            self._pool.numThreads + 1)  # one barrier for all threads
        self._execStats = ExecutionStats()
        self._runStatus = MainExec.STATUS_RUNNING
        self._initDbs()

    def getTaskExecutor(self):
        return self._te

    def getDbManager(self) -> DbManager:
        return self._dbManager

    def crossStepBarrier(self, timeout=None):
        self._stepBarrier.wait(timeout) 

    def requestToStop(self):
        self._runStatus = MainExec.STATUS_STOPPING
        self._execStats.registerFailure("User Interruption")

    def _runShouldEnd(self, transitionFailed, hasAbortedTask, workerTimeout):
        maxSteps = gConfig.max_steps  # type: ignore
        if self._curStep >= (maxSteps - 1): # maxStep==10, last curStep should be 9
            return True
        if self._runStatus != MainExec.STATUS_RUNNING:
            return True
        if transitionFailed:
            return True
        if hasAbortedTask:
            return True
        if workerTimeout:
            return True
        return False

    def _hasAbortedTask(self): # from execution of previous step
        for task in self._executedTasks:
            if task.isAborted():
                # print("Task aborted: {}".format(task))
                # hasAbortedTask = True
                return True
        return False

    def _releaseAllWorkerThreads(self, transitionFailed):
        self._curStep += 1  # we are about to get into next step. TODO: race condition here!
        # Now not all threads had time to go to sleep
        logger.debug(
            "--\r\n\n--> Step {} starts with main thread waking up".format(self._curStep))

        # A new TE for the new step
        self._te = None # set to empty first, to signal worker thread to stop
        if not transitionFailed:  # only if not failed
            self._te = TaskExecutor(self._curStep)

        logger.debug("[TRD] Main thread waking up at step {}, tapping worker threads".format(
                self._curStep))  # Now not all threads had time to go to sleep
        # Worker threads will wake up at this point, and each execute it's own task
        self.tapAllThreads() # release all worker thread from their "gates"

    def _syncAtBarrier(self):
         # Now main thread (that's us) is ready to enter a step
        # let other threads go past the pool barrier, but wait at the
        # thread gate
        logger.debug("[TRD] Main thread about to cross the barrier")
        self.crossStepBarrier(timeout=self.WORKER_THREAD_TIMEOUT)
        self._stepBarrier.reset()  # Other worker threads should now be at the "gate"
        logger.debug("[TRD] Main thread finished crossing the barrier")

    def _doTransition(self):
        transitionFailed = False
        try:
            for x in self._dbs:
                db = x # type: Database
                sm = db.getStateMachine()
                logger.debug("[STT] starting transitions for DB: {}".format(db.getName()))
                # at end of step, transiton the DB state
                tasksForDb = db.filterTasks(self._executedTasks)
                sm.transition(tasksForDb, self.getDbManager().getDbConn())
                logger.debug("[STT] transition ended for DB: {}".format(db.getName()))

            # Due to limitation (or maybe not) of the TD Python library,
            # we cannot share connections across threads
            # Here we are in main thread, we cannot operate the connections created in workers
            # Moving below to task loop
            # if sm.hasDatabase():
            #     for t in self._pool.threadList:
            #         logger.debug("[DB] use db for all worker threads")
            #         t.useDb()
                    # t.execSql("use db") # main thread executing "use
                    # db" on behalf of every worker thread

        except taos.error.ProgrammingError as err:
            if (err.msg == 'network unavailable'):  # broken DB connection
                logger.info("DB connection broken, execution failed")
                traceback.print_stack()
                transitionFailed = True
                self._te = None  # Not running any more
                self._execStats.registerFailure("Broken DB Connection")
                # continue # don't do that, need to tap all threads at
                # end, and maybe signal them to stop
            else:
                raise
        # return transitionFailed # Why did we have this??!!

        self.resetExecutedTasks()  # clear the tasks after we are done
        # Get ready for next step
        logger.debug("<-- Step {} finished, trasition failed = {}".format(self._curStep, transitionFailed))
        return transitionFailed

    def run(self):
        self._pool.createAndStartThreads(self)

        # Coordinate all threads step by step
        self._curStep = -1  # not started yet
        
        self._execStats.startExec()  # start the stop watch
        transitionFailed = False
        hasAbortedTask = False
        workerTimeout = False
        while not self._runShouldEnd(transitionFailed, hasAbortedTask, workerTimeout):
            if not gConfig.debug: # print this only if we are not in debug mode                
                print(".", end="", flush=True)
            # if (self._curStep % 2) == 0: # print memory usage once every 10 steps
            #     memUsage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            #     print("[m:{}]".format(memUsage), end="", flush=True) # print memory usage
            # if (self._curStep % 10) == 3: 
            #     h = hpy()
            #     print("\n")        
            #     print(h.heap())
            
                        
            try:
                self._syncAtBarrier() # For now just cross the barrier
            except threading.BrokenBarrierError as err:
                logger.info("Main loop aborted, caused by worker thread time-out")
                self._execStats.registerFailure("Aborted due to worker thread timeout")
                print("\n\nWorker Thread time-out detected, important thread info:")
                ts = ThreadStacks()
                ts.print(filterInternal=True)
                workerTimeout = True
                break

            # At this point, all threads should be pass the overall "barrier" and before the per-thread "gate"
            # We use this period to do house keeping work, when all worker
            # threads are QUIET.
            hasAbortedTask = self._hasAbortedTask() # from previous step
            if hasAbortedTask: 
                logger.info("Aborted task encountered, exiting test program")
                self._execStats.registerFailure("Aborted Task Encountered")
                break # do transition only if tasks are error free

            # Ending previous step
            try:
                transitionFailed = self._doTransition() # To start, we end step -1 first
            except taos.error.ProgrammingError as err:
                transitionFailed = True
                errno2 = Helper.convertErrno(err.errno)  # correct error scheme
                errMsg = "Transition failed: errno=0x{:X}, msg: {}".format(errno2, err)
                logger.info(errMsg)
                traceback.print_exc()
                self._execStats.registerFailure(errMsg)

            # Then we move on to the next step
            self._releaseAllWorkerThreads(transitionFailed)                    

        if hasAbortedTask or transitionFailed : # abnormal ending, workers waiting at "gate"
            logger.debug("Abnormal ending of main thraed")
        elif workerTimeout:
            logger.debug("Abnormal ending of main thread, due to worker timeout")
        else: # regular ending, workers waiting at "barrier"
            logger.debug("Regular ending, main thread waiting for all worker threads to stop...")
            self._syncAtBarrier()

        self._te = None  # No more executor, time to end
        logger.debug("Main thread tapping all threads one last time...")
        self.tapAllThreads()  # Let the threads run one last time

        logger.debug("\r\n\n--> Main thread ready to finish up...")
        logger.debug("Main thread joining all threads")
        self._pool.joinAll()  # Get all threads to finish
        logger.info("\nAll worker threads finished")
        self._execStats.endExec()

    def cleanup(self): # free resources
        self._pool.cleanup()

        self._pool = None
        self._te = None  
        self._dbManager = None
        self._executedTasks = None
        self._lock = None
        self._stepBarrier = None
        self._execStats = None
        self._runStatus = None


    def printStats(self):
        self._execStats.printStats()

    def isFailed(self):
        return self._execStats.isFailed()

    def getExecStats(self):
        return self._execStats

    def tapAllThreads(self):  # in a deterministic manner
        wakeSeq = []
        for i in range(self._pool.numThreads):  # generate a random sequence
            if Dice.throw(2) == 1:
                wakeSeq.append(i)
            else:
                wakeSeq.insert(0, i)
        logger.debug(
            "[TRD] Main thread waking up worker threads: {}".format(
                str(wakeSeq)))
        # TODO: set dice seed to a deterministic value
        for i in wakeSeq:
            # TODO: maybe a bit too deep?!
            self._pool.threadList[i].tapStepGate()
            time.sleep(0)  # yield

    def isRunning(self):
        return self._te is not None

    def _initDbs(self):
        ''' Initialize multiple databases, invoked at __ini__() time '''
        self._dbs = [] # type: List[Database]
        dbc = self.getDbManager().getDbConn()
        if gConfig.max_dbs == 0:
            self._dbs.append(Database(0, dbc))
        else:
            for i in range(gConfig.max_dbs):
                self._dbs.append(Database(i, dbc))

    def pickDatabase(self):
        idxDb = 0
        if gConfig.max_dbs != 0 :
            idxDb = Dice.throw(gConfig.max_dbs) # 0 to N-1
        db = self._dbs[idxDb] # type: Database
        return db

    def fetchTask(self) -> Task:
        ''' The thread coordinator (that's us) is responsible for fetching a task
            to be executed next.
        '''
        if (not self.isRunning()):  # no task
            raise RuntimeError("Cannot fetch task when not running")

        # pick a task type for current state
        db = self.pickDatabase()
        taskType = db.getStateMachine().pickTaskType() # type: Task
        return taskType(self._execStats, db)  # create a task from it

    def resetExecutedTasks(self):
        self._executedTasks = []  # should be under single thread

    def saveExecutedTask(self, task):
        with self._lock:
            self._executedTasks.append(task)

# We define a class to run a number of threads in locking steps.

class Helper:
    @classmethod
    def convertErrno(cls, errno):
        return errno if (errno > 0) else 0x80000000 + errno

class ThreadPool:
    def __init__(self, numThreads, maxSteps):
        self.numThreads = numThreads
        self.maxSteps = maxSteps
        # Internal class variables
        self.curStep = 0
        self.threadList = []  # type: List[WorkerThread]

    # starting to run all the threads, in locking steps
    def createAndStartThreads(self, tc: ThreadCoordinator):
        for tid in range(0, self.numThreads):  # Create the threads
            workerThread = WorkerThread(self, tid, tc)
            self.threadList.append(workerThread)
            workerThread.start()  # start, but should block immediately before step 0

    def joinAll(self):
        for workerThread in self.threadList:
            logger.debug("Joining thread...")
            workerThread._thread.join()

    def cleanup(self):
        self.threadList = None # maybe clean up each?

# A queue of continguous POSITIVE integers, used by DbManager to generate continuous numbers
# for new table names


class LinearQueue():
    def __init__(self):
        self.firstIndex = 1  # 1st ever element
        self.lastIndex = 0
        self._lock = threading.RLock()  # our functions may call each other
        self.inUse = set()  # the indexes that are in use right now

    def toText(self):
        return "[{}..{}], in use: {}".format(
            self.firstIndex, self.lastIndex, self.inUse)

    # Push (add new element, largest) to the tail, and mark it in use
    def push(self):
        with self._lock:
            # if ( self.isEmpty() ):
            #     self.lastIndex = self.firstIndex
            #     return self.firstIndex
            # Otherwise we have something
            self.lastIndex += 1
            self.allocate(self.lastIndex)
            # self.inUse.add(self.lastIndex) # mark it in use immediately
            return self.lastIndex

    def pop(self):
        with self._lock:
            if (self.isEmpty()):
                # raise RuntimeError("Cannot pop an empty queue")
                return False  # TODO: None?

            index = self.firstIndex
            if (index in self.inUse):
                return False

            self.firstIndex += 1
            return index

    def isEmpty(self):
        return self.firstIndex > self.lastIndex

    def popIfNotEmpty(self):
        with self._lock:
            if (self.isEmpty()):
                return 0
            return self.pop()

    def allocate(self, i):
        with self._lock:
            # logger.debug("LQ allocating item {}".format(i))
            if (i in self.inUse):
                raise RuntimeError(
                    "Cannot re-use same index in queue: {}".format(i))
            self.inUse.add(i)

    def release(self, i):
        with self._lock:
            # logger.debug("LQ releasing item {}".format(i))
            self.inUse.remove(i)  # KeyError possible, TODO: why?

    def size(self):
        return self.lastIndex + 1 - self.firstIndex

    def pickAndAllocate(self):
        if (self.isEmpty()):
            return None
        with self._lock:
            cnt = 0  # counting the interations
            while True:
                cnt += 1
                if (cnt > self.size() * 10):  # 10x iteration already
                    # raise RuntimeError("Failed to allocate LinearQueue element")
                    return None
                ret = Dice.throwRange(self.firstIndex, self.lastIndex + 1)
                if (ret not in self.inUse):
                    self.allocate(ret)
                    return ret


class DbConn:
    TYPE_NATIVE = "native-c"
    TYPE_REST =   "rest-api"
    TYPE_INVALID = "invalid"

    @classmethod
    def create(cls, connType):
        if connType == cls.TYPE_NATIVE:
            return DbConnNative()
        elif connType == cls.TYPE_REST:
            return DbConnRest()
        else:
            raise RuntimeError(
                "Unexpected connection type: {}".format(connType))

    @classmethod
    def createNative(cls):
        return cls.create(cls.TYPE_NATIVE)

    @classmethod
    def createRest(cls):
        return cls.create(cls.TYPE_REST)

    def __init__(self):
        self.isOpen = False
        self._type = self.TYPE_INVALID
        self._lastSql = None

    def getLastSql(self):
        return self._lastSql

    def open(self):
        if (self.isOpen):
            raise RuntimeError("Cannot re-open an existing DB connection")

        # below implemented by child classes
        self.openByType()

        logger.debug("[DB] data connection opened, type = {}".format(self._type))
        self.isOpen = True

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
    def __init__(self):
        super().__init__()
        self._type = self.TYPE_REST
        self._url = "http://localhost:6041/rest/sql"  # fixed for now
        self._result = None

    def openByType(self):  # Open connection
        pass  # do nothing, always open

    def close(self):
        if (not self.isOpen):
            raise RuntimeError("Cannot clean up database until connection is open")
        # Do nothing for REST
        logger.debug("[DB] REST Database connection closed")
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
        logger.debug("[SQL-REST] Executing SQL: {}".format(sql))
        nRows = self._doSql(sql)
        logger.debug(
            "[SQL-REST] Execution Result, nRows = {}, SQL = {}".format(nRows, sql))
        return nRows

    def query(self, sql):  # return rows affected
        return self.execute(sql)

    def getQueryResult(self):
        return self._result['data']

    def getResultRows(self):
        print(self._result)
        raise RuntimeError("TBD")
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


class DbConnNative(DbConn):
    # Class variables
    _lock = threading.Lock()
    _connInfoDisplayed = False
    totalConnections = 0 # Not private

    def __init__(self):
        super().__init__()
        self._type = self.TYPE_NATIVE
        self._conn = None
        # self._cursor = None        

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))
        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("communit")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        buildPath = None
        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        if buildPath == None:
            raise RuntimeError("Failed to determine buildPath, selfPath={}, projPath={}"
                .format(selfPath, projPath))
        return buildPath

    
    def openByType(self):  # Open connection
        cfgPath = self.getBuildPath() + "/test/cfg"
        hostAddr = "127.0.0.1"

        cls = self.__class__ # Get the class, to access class variables
        with cls._lock: # force single threading for opening DB connections. # TODO: whaaat??!!!
            if not cls._connInfoDisplayed:
                cls._connInfoDisplayed = True # updating CLASS variable
                logger.info("Initiating TAOS native connection to {}, using config at {}".format(hostAddr, cfgPath))                    
            # Make the connection         
            # self._conn = taos.connect(host=hostAddr, config=cfgPath)  # TODO: make configurable
            # self._cursor = self._conn.cursor()
            # Record the count in the class
            self._tdSql = MyTDSql(hostAddr, cfgPath) # making DB connection
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

        logger.debug("[DB] Database connection closed")
        self.isOpen = False

    def execute(self, sql):
        if (not self.isOpen):
            raise RuntimeError("Cannot execute database commands until connection is open")
        logger.debug("[SQL] Executing SQL: {}".format(sql))
        self._lastSql = sql
        nRows = self._tdSql.execute(sql)
        logger.debug(
            "[SQL] Execution Result, nRows = {}, SQL = {}".format(
                nRows, sql))
        return nRows

    def query(self, sql):  # return rows affected
        if (not self.isOpen):
            raise RuntimeError(
                "Cannot query database until connection is open")
        logger.debug("[SQL] Executing SQL: {}".format(sql))
        self._lastSql = sql
        nRows = self._tdSql.query(sql)
        logger.debug(
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


class AnyState:
    STATE_INVALID = -1
    STATE_EMPTY = 0  # nothing there, no even a DB
    STATE_DB_ONLY = 1  # we have a DB, but nothing else
    STATE_TABLE_ONLY = 2  # we have a table, but totally empty
    STATE_HAS_DATA = 3  # we have some data in the table
    _stateNames = ["Invalid", "Empty", "DB_Only", "Table_Only", "Has_Data"]

    STATE_VAL_IDX = 0
    CAN_CREATE_DB = 1
    # For below, if we can "drop the DB", but strictly speaking 
    # only "under normal circumstances", as we may override it with the -b option
    CAN_DROP_DB = 2  
    CAN_CREATE_FIXED_SUPER_TABLE = 3
    CAN_DROP_FIXED_SUPER_TABLE = 4
    CAN_ADD_DATA = 5
    CAN_READ_DATA = 6

    def __init__(self):
        self._info = self.getInfo()

    def __str__(self):
        # -1 hack to accomodate the STATE_INVALID case
        return self._stateNames[self._info[self.STATE_VAL_IDX] + 1]

    # Each sub state tells us the "info", about itself, so we can determine
    # on things like canDropDB()
    def getInfo(self):
        raise RuntimeError("Must be overriden by child classes")

    def equals(self, other):
        if isinstance(other, int):
            return self.getValIndex() == other
        elif isinstance(other, AnyState):
            return self.getValIndex() == other.getValIndex()
        else:
            raise RuntimeError(
                "Unexpected comparison, type = {}".format(
                    type(other)))

    def verifyTasksToState(self, tasks, newState):
        raise RuntimeError("Must be overriden by child classes")

    def getValIndex(self):
        return self._info[self.STATE_VAL_IDX]

    def getValue(self):
        return self._info[self.STATE_VAL_IDX]

    def canCreateDb(self):
        return self._info[self.CAN_CREATE_DB]

    def canDropDb(self):
        # If user requests to run up to a number of DBs,
        # we'd then not do drop_db operations any more
        if gConfig.max_dbs > 0 : 
            return False
        return self._info[self.CAN_DROP_DB]

    def canCreateFixedSuperTable(self):
        return self._info[self.CAN_CREATE_FIXED_SUPER_TABLE]

    def canDropFixedSuperTable(self):
        return self._info[self.CAN_DROP_FIXED_SUPER_TABLE]

    def canAddData(self):
        return self._info[self.CAN_ADD_DATA]

    def canReadData(self):
        return self._info[self.CAN_READ_DATA]

    def assertAtMostOneSuccess(self, tasks, cls):
        sCnt = 0
        for task in tasks:
            if not isinstance(task, cls):
                continue
            if task.isSuccess():
                # task.logDebug("Task success found")
                sCnt += 1
                if (sCnt >= 2):
                    raise RuntimeError(
                        "Unexpected more than 1 success with task: {}".format(cls))

    def assertIfExistThenSuccess(self, tasks, cls):
        sCnt = 0
        exists = False
        for task in tasks:
            if not isinstance(task, cls):
                continue
            exists = True  # we have a valid instance
            if task.isSuccess():
                sCnt += 1
        if (exists and sCnt <= 0):
            raise RuntimeError("Unexpected zero success for task type: {}, from tasks: {}"
                .format(cls, tasks))

    def assertNoTask(self, tasks, cls):
        for task in tasks:
            if isinstance(task, cls):
                raise CrashGenError(
                    "This task: {}, is not expected to be present, given the success/failure of others".format(cls.__name__))

    def assertNoSuccess(self, tasks, cls):
        for task in tasks:
            if isinstance(task, cls):
                if task.isSuccess():
                    raise RuntimeError(
                        "Unexpected successful task: {}".format(cls))

    def hasSuccess(self, tasks, cls):
        for task in tasks:
            if not isinstance(task, cls):
                continue
            if task.isSuccess():
                return True
        return False

    def hasTask(self, tasks, cls):
        for task in tasks:
            if isinstance(task, cls):
                return True
        return False


class StateInvalid(AnyState):
    def getInfo(self):
        return [
            self.STATE_INVALID,
            False, False,  # can create/drop Db
            False, False,  # can create/drop fixed table
            False, False,  # can insert/read data with fixed table
        ]

    # def verifyTasksToState(self, tasks, newState):


class StateEmpty(AnyState):
    def getInfo(self):
        return [
            self.STATE_EMPTY,
            True, False,  # can create/drop Db
            False, False,  # can create/drop fixed table
            False, False,  # can insert/read data with fixed table
        ]

    def verifyTasksToState(self, tasks, newState):
        if (self.hasSuccess(tasks, TaskCreateDb)
                ):  # at EMPTY, if there's succes in creating DB
            if (not self.hasTask(tasks, TaskDropDb)):  # and no drop_db tasks
                # we must have at most one. TODO: compare numbers
                self.assertAtMostOneSuccess(tasks, TaskCreateDb)


class StateDbOnly(AnyState):
    def getInfo(self):
        return [
            self.STATE_DB_ONLY,
            False, True,
            True, False,
            False, False,
        ]

    def verifyTasksToState(self, tasks, newState):
        if (not self.hasTask(tasks, TaskCreateDb)):
            # only if we don't create any more
            self.assertAtMostOneSuccess(tasks, TaskDropDb)

        # TODO: restore the below, the problem exists, although unlikely in real-world
        # if (gSvcMgr!=None) and gSvcMgr.isRestarting():     
        # if (gSvcMgr == None) or (not gSvcMgr.isRestarting()) : 
        #     self.assertIfExistThenSuccess(tasks, TaskDropDb)       


class StateSuperTableOnly(AnyState):
    def getInfo(self):
        return [
            self.STATE_TABLE_ONLY,
            False, True,
            False, True,
            True, True,
        ]

    def verifyTasksToState(self, tasks, newState):
        if (self.hasSuccess(tasks, TaskDropSuperTable)
                ):  # we are able to drop the table
            #self.assertAtMostOneSuccess(tasks, TaskDropSuperTable)
            # we must have had recreted it
            self.hasSuccess(tasks, TaskCreateSuperTable)

            # self._state = self.STATE_DB_ONLY
        # elif ( self.hasSuccess(tasks, AddFixedDataTask) ): # no success dropping the table, but added data
        #     self.assertNoTask(tasks, DropFixedTableTask) # not true in massively parrallel cases
            # self._state = self.STATE_HAS_DATA
        # elif ( self.hasSuccess(tasks, ReadFixedDataTask) ): # no success in prev cases, but was able to read data
            # self.assertNoTask(tasks, DropFixedTableTask)
            # self.assertNoTask(tasks, AddFixedDataTask)
            # self._state = self.STATE_TABLE_ONLY # no change
        # else: # did not drop table, did not insert data, did not read successfully, that is impossible
        #     raise RuntimeError("Unexpected no-success scenarios")
        # TODO: need to revamp!!


class StateHasData(AnyState):
    def getInfo(self):
        return [
            self.STATE_HAS_DATA,
            False, True,
            False, True,
            True, True,
        ]

    def verifyTasksToState(self, tasks, newState):
        if (newState.equals(AnyState.STATE_EMPTY)):
            self.hasSuccess(tasks, TaskDropDb)
            if (not self.hasTask(tasks, TaskCreateDb)):
                self.assertAtMostOneSuccess(tasks, TaskDropDb)  # TODO: dicy
        elif (newState.equals(AnyState.STATE_DB_ONLY)):  # in DB only
            if (not self.hasTask(tasks, TaskCreateDb)
                ):  # without a create_db task
                # we must have drop_db task
                self.assertNoTask(tasks, TaskDropDb)
            self.hasSuccess(tasks, TaskDropSuperTable)
            # self.assertAtMostOneSuccess(tasks, DropFixedSuperTableTask) # TODO: dicy
        # elif ( newState.equals(AnyState.STATE_TABLE_ONLY) ): # data deleted
            # self.assertNoTask(tasks, TaskDropDb)
            # self.assertNoTask(tasks, TaskDropSuperTable)
            # self.assertNoTask(tasks, TaskAddData)
            # self.hasSuccess(tasks, DeleteDataTasks)
        else:  # should be STATE_HAS_DATA
            if (not self.hasTask(tasks, TaskCreateDb)
                ):  # only if we didn't create one
                # we shouldn't have dropped it
                self.assertNoTask(tasks, TaskDropDb)
            if (not self.hasTask(tasks, TaskCreateSuperTable)
                    ):  # if we didn't create the table
                # we should not have a task that drops it
                self.assertNoTask(tasks, TaskDropSuperTable)
            # self.assertIfExistThenSuccess(tasks, ReadFixedDataTask)


class StateMechine:
    def __init__(self, db: Database): 
        self._db = db
        # transitition target probabilities, indexed with value of STATE_EMPTY, STATE_DB_ONLY, etc.
        self._stateWeights = [1, 2, 10, 40]

    def init(self, dbc: DbConn): # late initailization, don't save the dbConn
        self._curState = self._findCurrentState(dbc)  # starting state
        logger.debug("Found Starting State: {}".format(self._curState))

    # TODO: seems no lnoger used, remove?
    def getCurrentState(self):
        return self._curState

    def hasDatabase(self):
        return self._curState.canDropDb()  # ha, can drop DB means it has one

    # May be slow, use cautionsly...
    def getTaskTypes(self):  # those that can run (directly/indirectly) from the current state
        def typesToStrings(types):
            ss = []
            for t in types:
                ss.append(t.__name__)
            return ss

        allTaskClasses = StateTransitionTask.__subclasses__()  # all state transition tasks
        firstTaskTypes = []
        for tc in allTaskClasses:
            # t = tc(self) # create task object
            if tc.canBeginFrom(self._curState):
                firstTaskTypes.append(tc)
        # now we have all the tasks that can begin directly from the current
        # state, let's figure out the INDIRECT ones
        taskTypes = firstTaskTypes.copy()  # have to have these
        for task1 in firstTaskTypes:  # each task type gathered so far
            endState = task1.getEndState()  # figure the end state
            if endState is None:  # does not change end state
                continue  # no use, do nothing
            for tc in allTaskClasses:  # what task can further begin from there?
                if tc.canBeginFrom(endState) and (tc not in firstTaskTypes):
                    taskTypes.append(tc)  # gather it

        if len(taskTypes) <= 0:
            raise RuntimeError(
                "No suitable task types found for state: {}".format(
                    self._curState))
        logger.debug(
            "[OPS] Tasks found for state {}: {}".format(
                self._curState,
                typesToStrings(taskTypes)))
        return taskTypes

    def _findCurrentState(self, dbc: DbConn):
        ts = time.time()  # we use this to debug how fast/slow it is to do the various queries to find the current DB state
        dbName =self._db.getName()
        if not dbc.existsDatabase(dbName): # dbc.hasDatabases():  # no database?!
            logger.debug( "[STT] empty database found, between {} and {}".format(ts, time.time()))
            return StateEmpty()
        # did not do this when openning connection, and this is NOT the worker
        # thread, which does this on their own
        dbc.use(dbName)
        if not dbc.hasTables():  # no tables
            logger.debug("[STT] DB_ONLY found, between {} and {}".format(ts, time.time()))
            return StateDbOnly()

        sTable = self._db.getFixedSuperTable()
        if sTable.hasRegTables(dbc, dbName):  # no regular tables
            logger.debug("[STT] SUPER_TABLE_ONLY found, between {} and {}".format(ts, time.time()))
            return StateSuperTableOnly()
        else:  # has actual tables
            logger.debug("[STT] HAS_DATA found, between {} and {}".format(ts, time.time()))
            return StateHasData()

    # We transition the system to a new state by examining the current state itself
    def transition(self, tasks, dbc: DbConn):
        if (len(tasks) == 0):  # before 1st step, or otherwise empty
            logger.debug("[STT] Starting State: {}".format(self._curState))
            return  # do nothing

        # this should show up in the server log, separating steps
        dbc.execute("show dnodes")

        # Generic Checks, first based on the start state
        if self._curState.canCreateDb():
            self._curState.assertIfExistThenSuccess(tasks, TaskCreateDb)
            # self.assertAtMostOneSuccess(tasks, CreateDbTask) # not really, in
            # case of multiple creation and drops

        if self._curState.canDropDb():
            if gSvcMgr == None: # only if we are running as client-only
                self._curState.assertIfExistThenSuccess(tasks, TaskDropDb)
            # self.assertAtMostOneSuccess(tasks, DropDbTask) # not really in
            # case of drop-create-drop

        # if self._state.canCreateFixedTable():
            # self.assertIfExistThenSuccess(tasks, CreateFixedTableTask) # Not true, DB may be dropped
            # self.assertAtMostOneSuccess(tasks, CreateFixedTableTask) # not
            # really, in case of create-drop-create

        # if self._state.canDropFixedTable():
            # self.assertIfExistThenSuccess(tasks, DropFixedTableTask) # Not True, the whole DB may be dropped
            # self.assertAtMostOneSuccess(tasks, DropFixedTableTask) # not
            # really in case of drop-create-drop

        # if self._state.canAddData():
        # self.assertIfExistThenSuccess(tasks, AddFixedDataTask)  # not true
        # actually

        # if self._state.canReadData():
            # Nothing for sure

        newState = self._findCurrentState(dbc)
        logger.debug("[STT] New DB state determined: {}".format(newState))
        # can old state move to new state through the tasks?
        self._curState.verifyTasksToState(tasks, newState)
        self._curState = newState

    def pickTaskType(self):
        # all the task types we can choose from at curent state
        taskTypes = self.getTaskTypes()
        weights = []
        for tt in taskTypes:
            endState = tt.getEndState()
            if endState is not None:
                # TODO: change to a method
                weights.append(self._stateWeights[endState.getValIndex()])
            else:
                # read data task, default to 10: TODO: change to a constant
                weights.append(10)
        i = self._weighted_choice_sub(weights)
        # logger.debug(" (weighted random:{}/{}) ".format(i, len(taskTypes)))
        return taskTypes[i]

    # ref:
    # https://eli.thegreenplace.net/2010/01/22/weighted-random-generation-in-python/
    def _weighted_choice_sub(self, weights):
        # TODO: use our dice to ensure it being determinstic?
        rnd = random.random() * sum(weights)
        for i, w in enumerate(weights):
            rnd -= w
            if rnd < 0:
                return i

class Database:
    ''' We use this to represent an actual TDengine database inside a service instance,
        possibly in a cluster environment.

        For now we use it to manage state transitions in that database
    '''
    _clsLock = threading.Lock() # class wide lock
    _lastInt = 101  # next one is initial integer
    _lastTick = 0
    _lastLaggingTick = 0 # lagging tick, for unsequenced insersions

    def __init__(self, dbNum: int, dbc: DbConn): # TODO: remove dbc
        self._dbNum = dbNum # we assign a number to databases, for our testing purpose
        self._stateMachine = StateMechine(self)
        self._stateMachine.init(dbc)
          
        self._lock = threading.RLock()

    def getStateMachine(self) -> StateMechine:
        return self._stateMachine

    def getDbNum(self):
        return self._dbNum

    def getName(self):
        return "db_{}".format(self._dbNum)

    def filterTasks(self, inTasks: List[Task]): # Pick out those belonging to us
        outTasks = []
        for task in inTasks:
            if task.getDb().isSame(self):
                outTasks.append(task)
        return outTasks

    def isSame(self, other):
        return self._dbNum == other._dbNum

    def exists(self, dbc: DbConn):
        return dbc.existsDatabase(self.getName())

    @classmethod
    def getFixedSuperTableName(cls):
        return "fs_table"

    @classmethod
    def getFixedSuperTable(cls) -> TdSuperTable:
        return TdSuperTable(cls.getFixedSuperTableName())

    # We aim to create a starting time tick, such that, whenever we run our test here once
    # We should be able to safely create 100,000 records, which will not have any repeated time stamp
    # when we re-run the test in 3 minutes (180 seconds), basically we should expand time duration
    # by a factor of 500.
    # TODO: what if it goes beyond 10 years into the future
    # TODO: fix the error as result of above: "tsdb timestamp is out of range"
    @classmethod
    def setupLastTick(cls):
        t1 = datetime.datetime(2020, 6, 1)
        t2 = datetime.datetime.now()
        # maybe a very large number, takes 69 years to exceed Python int range
        elSec = int(t2.timestamp() - t1.timestamp())
        elSec2 = (elSec % (8 * 12 * 30 * 24 * 60 * 60 / 500)) * \
            500  # a number representing seconds within 10 years
        # print("elSec = {}".format(elSec))
        t3 = datetime.datetime(2012, 1, 1)  # default "keep" is 10 years
        t4 = datetime.datetime.fromtimestamp(
            t3.timestamp() + elSec2)  # see explanation above
        logger.info("Setting up TICKS to start from: {}".format(t4))
        return t4

    @classmethod
    def getNextTick(cls):        
        with cls._clsLock:  # prevent duplicate tick
            if cls._lastLaggingTick==0:
                # 10k at 1/20 chance, should be enough to avoid overlaps
                cls._lastLaggingTick = cls.setupLastTick() + datetime.timedelta(0, -10000)                 
            if cls._lastTick==0: # should be quite a bit into the future
                cls._lastTick = cls.setupLastTick()  

            if Dice.throw(20) == 0:  # 1 in 20 chance, return lagging tick
                cls._lastLaggingTick += datetime.timedelta(0, 1) # Go back in time 100 seconds
                return cls._lastLaggingTick 
            else:  # regular
                # add one second to it
                cls._lastTick += datetime.timedelta(0, 1)
                return cls._lastTick

    def getNextInt(self):
        with self._lock:
            self._lastInt += 1
            return self._lastInt

    def getNextBinary(self):
        return "Beijing_Shanghai_Los_Angeles_New_York_San_Francisco_Chicago_Beijing_Shanghai_Los_Angeles_New_York_San_Francisco_Chicago_{}".format(
            self.getNextInt())

    def getNextFloat(self):
        ret = 0.9 + self.getNextInt()
        # print("Float obtained: {}".format(ret))
        return ret


class DbManager():
    ''' This is a wrapper around DbConn(), to make it easier to use. 

        TODO: rename this to DbConnManager
    '''
    def __init__(self):
        self.tableNumQueue = LinearQueue() # TODO: delete?
        # self.openDbServerConnection()
        self._dbConn = DbConn.createNative() if (
            gConfig.connector_type == 'native') else DbConn.createRest()
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

class TaskExecutor():
    class BoundedList:
        def __init__(self, size=10):
            self._size = size
            self._list = []
            self._lock = threading.Lock()

        def add(self, n: int):
            with self._lock:
                if not self._list:  # empty
                    self._list.append(n)
                    return
                # now we should insert
                nItems = len(self._list)
                insPos = 0
                for i in range(nItems):
                    insPos = i
                    if n <= self._list[i]:  # smaller than this item, time to insert
                        break  # found the insertion point
                    insPos += 1  # insert to the right

                if insPos == 0:  # except for the 1st item, # TODO: elimiate first item as gating item
                    return  # do nothing

                # print("Inserting at postion {}, value: {}".format(insPos, n))
                self._list.insert(insPos, n)  # insert

                newLen = len(self._list)
                if newLen <= self._size:
                    return  # do nothing
                elif newLen == (self._size + 1):
                    del self._list[0]  # remove the first item
                else:
                    raise RuntimeError("Corrupt Bounded List")

        def __str__(self):
            return repr(self._list)

    _boundedList = BoundedList()

    def __init__(self, curStep):
        self._curStep = curStep

    @classmethod
    def getBoundedList(cls):
        return cls._boundedList

    def getCurStep(self):
        return self._curStep

    def execute(self, task: Task, wt: WorkerThread):  # execute a task on a thread
        task.execute(wt)

    def recordDataMark(self, n: int):
        # print("[{}]".format(n), end="", flush=True)
        self._boundedList.add(n)

    # def logInfo(self, msg):
    #     logger.info("    T[{}.x]: ".format(self._curStep) + msg)

    # def logDebug(self, msg):
    #     logger.debug("    T[{}.x]: ".format(self._curStep) + msg)


class Task():
    ''' A generic "Task" to be executed. For now we decide that there is no
        need to embed a DB connection here, we use whatever the Worker Thread has
        instead. But a task is always associated with a DB
    '''
    taskSn = 100

    @classmethod
    def allocTaskNum(cls):
        Task.taskSn += 1  # IMPORTANT: cannot use cls.taskSn, since each sub class will have a copy
        # logger.debug("Allocating taskSN: {}".format(Task.taskSn))
        return Task.taskSn

    def __init__(self, execStats: ExecutionStats, db: Database):
        self._workerThread = None
        self._err = None # type: Exception
        self._aborted = False
        self._curStep = None
        self._numRows = None  # Number of rows affected

        # Assign an incremental task serial number
        self._taskNum = self.allocTaskNum()
        # logger.debug("Creating new task {}...".format(self._taskNum))

        self._execStats = execStats
        self._db = db # A task is always associated/for a specific DB

    def isSuccess(self):
        return self._err is None

    def isAborted(self):
        return self._aborted

    def clone(self):  # TODO: why do we need this again?
        newTask = self.__class__(self._execStats, self._db)
        return newTask

    def getDb(self):
        return self._db

    def logDebug(self, msg):
        self._workerThread.logDebug(
            "Step[{}.{}] {}".format(
                self._curStep, self._taskNum, msg))

    def logInfo(self, msg):
        self._workerThread.logInfo(
            "Step[{}.{}] {}".format(
                self._curStep, self._taskNum, msg))

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        raise RuntimeError(
            "To be implemeted by child classes, class name: {}".format(
                self.__class__.__name__))

    def _isErrAcceptable(self, errno, msg):
        if errno in [
                0x05,  # TSDB_CODE_RPC_NOT_READY
                0x0B,  # Unable to establish connection, more details in TD-1648
                # 0x200, # invalid SQL， TODO: re-examine with TD-934
                0x217, # "db not selected", client side defined error code
                0x218, # "Table does not exist" client side defined error code
                0x360, 0x362, 
                0x369, # tag already exists
                0x36A, 0x36B, 0x36D,
                0x381, 
                0x380, # "db not selected"
                0x383,
                0x386,  # DB is being dropped?!
                0x503,
                0x510,  # vnode not in ready state
                0x14,   # db not ready, errno changed
                0x600,
                1000  # REST catch-all error
            ]: 
            return True # These are the ALWAYS-ACCEPTABLE ones
        elif (errno in [ 0x0B ]) and gConfig.auto_start_service:
            return True # We may get "network unavilable" when restarting service
        elif errno == 0x200 : # invalid SQL, we need to div in a bit more
            if msg.find("invalid column name") != -1:
                return True 
            elif msg.find("tags number not matched") != -1: # mismatched tags after modification
                return True
            elif msg.find("duplicated column names") != -1: # also alter table tag issues
                return True
        elif (gSvcMgr!=None) and gSvcMgr.isRestarting():
            logger.info("Ignoring error when service is restarting: errno = {}, msg = {}".format(errno, msg))
            return True
        
        return False # Not an acceptable error


    def execute(self, wt: WorkerThread):
        wt.verifyThreadSelf()
        self._workerThread = wt  # type: ignore

        te = wt.getTaskExecutor()
        self._curStep = te.getCurStep()
        self.logDebug(
            "[-] executing task {}...".format(self.__class__.__name__))

        self._err = None # TODO: type hint mess up?
        self._execStats.beginTaskType(self.__class__.__name__)  # mark beginning
        errno2 = None

        # Now pick a database, and stick with it for the duration of the task execution
        dbName = self._db.getName()
        try:
            self._executeInternal(te, wt)  # TODO: no return value?
        except taos.error.ProgrammingError as err:
            errno2 = Helper.convertErrno(err.errno)
            if (gConfig.continue_on_exception):  # user choose to continue
                self.logDebug("[=] Continue after TAOS exception: errno=0x{:X}, msg: {}, SQL: {}".format(
                        errno2, err, wt.getDbConn().getLastSql()))
                self._err = err
            elif self._isErrAcceptable(errno2, err.__str__()):
                self.logDebug("[=] Acceptable Taos library exception: errno=0x{:X}, msg: {}, SQL: {}".format(
                        errno2, err, wt.getDbConn().getLastSql()))
                print("_", end="", flush=True)
                self._err = err
            else: # not an acceptable error
                errMsg = "[=] Unexpected Taos library exception ({}): errno=0x{:X}, msg: {}, SQL: {}".format(
                    self.__class__.__name__,
                    errno2, err, wt.getDbConn().getLastSql())
                self.logDebug(errMsg)
                if gConfig.debug:
                    # raise # so that we see full stack
                    traceback.print_exc()
                print(
                    "\n\n----------------------------\nProgram ABORTED Due to Unexpected TAOS Error: \n\n{}\n".format(errMsg) +
                    "----------------------------\n")
                # sys.exit(-1)
                self._err = err
                self._aborted = True
        except Exception as e:
            self.logInfo("Non-TAOS exception encountered")
            self._err = e
            self._aborted = True
            traceback.print_exc()
        except BaseException as e:
            self.logInfo("Python base exception encountered")
            self._err = e
            self._aborted = True
            traceback.print_exc()
        except BaseException: # TODO: what is this again??!!
            self.logDebug(
                "[=] Unexpected exception, SQL: {}".format(
                    wt.getDbConn().getLastSql()))
            raise
        self._execStats.endTaskType(self.__class__.__name__, self.isSuccess())

        self.logDebug("[X] task execution completed, {}, status: {}".format(
            self.__class__.__name__, "Success" if self.isSuccess() else "Failure"))
        # TODO: merge with above.
        self._execStats.incExecCount(self.__class__.__name__, self.isSuccess(), errno2)

    # TODO: refactor away, just provide the dbConn
    def execWtSql(self, wt: WorkerThread, sql):  # execute an SQL on the worker thread
        """ Haha """
        return wt.execSql(sql)

    def queryWtSql(self, wt: WorkerThread, sql):  # execute an SQL on the worker thread
        return wt.querySql(sql)

    def getQueryResult(self, wt: WorkerThread):  # execute an SQL on the worker thread
        return wt.getQueryResult()


class ExecutionStats:
    def __init__(self):
        # total/success times for a task
        self._execTimes: Dict[str, [int, int]] = {}
        self._tasksInProgress = 0
        self._lock = threading.Lock()
        self._firstTaskStartTime = None
        self._execStartTime = None
        self._errors = {}
        self._elapsedTime = 0.0  # total elapsed time
        self._accRunTime = 0.0  # accumulated run time

        self._failed = False
        self._failureReason = None

    def __str__(self):
        return "[ExecStats: _failed={}, _failureReason={}".format(
            self._failed, self._failureReason)

    def isFailed(self):
        return self._failed

    def startExec(self):
        self._execStartTime = time.time()

    def endExec(self):
        self._elapsedTime = time.time() - self._execStartTime

    def incExecCount(self, klassName, isSuccess, eno=None):  # TODO: add a lock here
        if klassName not in self._execTimes:
            self._execTimes[klassName] = [0, 0]
        t = self._execTimes[klassName]  # tuple for the data
        t[0] += 1  # index 0 has the "total" execution times
        if isSuccess:
            t[1] += 1  # index 1 has the "success" execution times
        if eno != None:             
            if klassName not in self._errors:
                self._errors[klassName] = {}
            errors = self._errors[klassName]
            errors[eno] = errors[eno]+1 if eno in errors else 1

    def beginTaskType(self, klassName):
        with self._lock:
            if self._tasksInProgress == 0:  # starting a new round
                self._firstTaskStartTime = time.time()  # I am now the first task
            self._tasksInProgress += 1

    def endTaskType(self, klassName, isSuccess):
        with self._lock:
            self._tasksInProgress -= 1
            if self._tasksInProgress == 0:  # all tasks have stopped
                self._accRunTime += (time.time() - self._firstTaskStartTime)
                self._firstTaskStartTime = None

    def registerFailure(self, reason):
        self._failed = True
        self._failureReason = reason

    def printStats(self):
        logger.info(
            "----------------------------------------------------------------------")
        logger.info(
            "| Crash_Gen test {}, with the following stats:". format(
                "FAILED (reason: {})".format(
                    self._failureReason) if self._failed else "SUCCEEDED"))
        logger.info("| Task Execution Times (success/total):")
        execTimesAny = 0
        for k, n in self._execTimes.items():
            execTimesAny += n[0]
            errStr = None
            if k in self._errors:
                errors = self._errors[k]
                # print("errors = {}".format(errors))
                errStrs = ["0x{:X}:{}".format(eno, n) for (eno, n) in errors.items()]
                # print("error strings = {}".format(errStrs))
                errStr = ", ".join(errStrs) 
            logger.info("|    {0:<24}: {1}/{2} (Errors: {3})".format(k, n[1], n[0], errStr))

        logger.info(
            "| Total Tasks Executed (success or not): {} ".format(execTimesAny))
        logger.info(
            "| Total Tasks In Progress at End: {}".format(
                self._tasksInProgress))
        logger.info(
            "| Total Task Busy Time (elapsed time when any task is in progress): {:.3f} seconds".format(
                self._accRunTime))
        logger.info(
            "| Average Per-Task Execution Time: {:.3f} seconds".format(self._accRunTime / execTimesAny))
        logger.info(
            "| Total Elapsed Time (from wall clock): {:.3f} seconds".format(
                self._elapsedTime))
        logger.info("| Top numbers written: {}".format(TaskExecutor.getBoundedList()))
        logger.info("| Active DB Native Connections (now): {}".format(DbConnNative.totalConnections))
        logger.info("| Longest native query time: {:.3f} seconds, started: {}".
            format(MyTDSql.longestQueryTime, 
                time.strftime("%x %X", time.localtime(MyTDSql.lqStartTime))) )
        logger.info("| Longest native query: {}".format(MyTDSql.longestQuery))
        logger.info(
            "----------------------------------------------------------------------")


class StateTransitionTask(Task):
    LARGE_NUMBER_OF_TABLES = 35
    SMALL_NUMBER_OF_TABLES = 3
    LARGE_NUMBER_OF_RECORDS = 50
    SMALL_NUMBER_OF_RECORDS = 3

    @classmethod
    def getInfo(cls):  # each sub class should supply their own information
        raise RuntimeError("Overriding method expected")

    _endState = None
    @classmethod
    def getEndState(cls):  # TODO: optimize by calling it fewer times
        raise RuntimeError("Overriding method expected")

    # @classmethod
    # def getBeginStates(cls):
    #     return cls.getInfo()[0]

    # @classmethod
    # def getEndState(cls): # returning the class name
    #     return cls.getInfo()[0]

    @classmethod
    def canBeginFrom(cls, state: AnyState):
        # return state.getValue() in cls.getBeginStates()
        raise RuntimeError("must be overriden")

    @classmethod
    def getRegTableName(cls, i):
        return "reg_table_{}".format(i)

    def execute(self, wt: WorkerThread):
        super().execute(wt)


class TaskCreateDb(StateTransitionTask):
    @classmethod
    def getEndState(cls):
        return StateDbOnly()

    @classmethod
    def canBeginFrom(cls, state: AnyState):
        return state.canCreateDb()

    # Actually creating the database(es)
    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        # was: self.execWtSql(wt, "create database db")
        repStr = ""
        if gConfig.max_replicas != 1:
            numReplica = Dice.throw(gConfig.max_replicas) + 1 # 1,2 ... N
            repStr = "replica {}".format(numReplica)
        self.execWtSql(wt, "create database {} {}"
            .format(self._db.getName(), repStr) )

class TaskDropDb(StateTransitionTask):
    @classmethod
    def getEndState(cls):
        return StateEmpty()

    @classmethod
    def canBeginFrom(cls, state: AnyState):
        return state.canDropDb()

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        self.execWtSql(wt, "drop database {}".format(self._db.getName()))
        logger.debug("[OPS] database dropped at {}".format(time.time()))

class TaskCreateSuperTable(StateTransitionTask):
    @classmethod
    def getEndState(cls):
        return StateSuperTableOnly()

    @classmethod
    def canBeginFrom(cls, state: AnyState):
        return state.canCreateFixedSuperTable()

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        if not self._db.exists(wt.getDbConn()):
            logger.debug("Skipping task, no DB yet")
            return

        sTable = self._db.getFixedSuperTable() # type: TdSuperTable
        # wt.execSql("use db")    # should always be in place
        sTable.create(wt.getDbConn(), self._db.getName(), 
            {'ts':'timestamp', 'speed':'int'}, {'b':'binary(200)', 'f':'float'})
        # self.execWtSql(wt,"create table db.{} (ts timestamp, speed int) tags (b binary(200), f float) ".format(tblName))
        # No need to create the regular tables, INSERT will do that
        # automatically


class TdSuperTable:
    def __init__(self, stName):
        self._stName = stName

    def getName(self):
        return self._stName

    # TODO: odd semantic, create() method is usually static?
    def create(self, dbc, dbName, cols: dict, tags: dict):
        '''Creating a super table'''
        sql = "CREATE TABLE {}.{} ({}) TAGS ({})".format(
            dbName,
            self._stName,
            ",".join(['%s %s'%(k,v) for (k,v) in cols.items()]),
            ",".join(['%s %s'%(k,v) for (k,v) in tags.items()])
            )
        dbc.execute(sql)        

    def getRegTables(self, dbc: DbConn, dbName: str):
        try:
            dbc.query("select TBNAME from {}.{}".format(dbName, self._stName))  # TODO: analyze result set later            
        except taos.error.ProgrammingError as err:                    
            errno2 = Helper.convertErrno(err.errno) 
            logger.debug("[=] Failed to get tables from super table: errno=0x{:X}, msg: {}".format(errno2, err))
            raise

        qr = dbc.getQueryResult()
        return [v[0] for v in qr] # list transformation, ref: https://stackoverflow.com/questions/643823/python-list-transformation

    def hasRegTables(self, dbc: DbConn, dbName: str):
        return dbc.query("SELECT * FROM {}.{}".format(dbName, self._stName)) > 0

    def ensureTable(self, dbc: DbConn, dbName: str, regTableName: str):
        sql = "select tbname from {}.{} where tbname in ('{}')".format(dbName, self._stName, regTableName)
        if dbc.query(sql) >= 1 : # reg table exists already
            return
        sql = "CREATE TABLE {}.{} USING {}.{} tags ({})".format(
            dbName, regTableName, dbName, self._stName, self._getTagStrForSql(dbc, dbName)
        )
        dbc.execute(sql)

    def _getTagStrForSql(self, dbc, dbName: str) :
        tags = self._getTags(dbc, dbName)
        tagStrs = []
        for tagName in tags: 
            tagType = tags[tagName]
            if tagType == 'BINARY':
                tagStrs.append("'Beijing-Shanghai-LosAngeles'")
            elif tagType == 'FLOAT':
                tagStrs.append('9.9')
            elif tagType == 'INT':
                tagStrs.append('88')
            else:
                raise RuntimeError("Unexpected tag type: {}".format(tagType))
        return ", ".join(tagStrs)

    def _getTags(self, dbc, dbName) -> dict:
        dbc.query("DESCRIBE {}.{}".format(dbName, self._stName))
        stCols = dbc.getQueryResult()
        # print(stCols)
        ret = {row[0]:row[1] for row in stCols if row[3]=='TAG'} # name:type
        # print("Tags retrieved: {}".format(ret))
        return ret

    def addTag(self, dbc, dbName, tagName, tagType):
        if tagName in self._getTags(dbc, dbName): # already 
            return
        # sTable.addTag("extraTag", "int")
        sql = "alter table {}.{} add tag {} {}".format(dbName, self._stName, tagName, tagType)
        dbc.execute(sql)

    def dropTag(self, dbc, dbName, tagName):
        if not tagName in self._getTags(dbc, dbName): # don't have this tag
            return
        sql = "alter table {}.{} drop tag {}".format(dbName, self._stName, tagName)
        dbc.execute(sql)

    def changeTag(self, dbc, dbName, oldTag, newTag):
        tags = self._getTags(dbc, dbName)
        if not oldTag in tags: # don't have this tag
            return
        if newTag in tags: # already have this tag
            return
        sql = "alter table {}.{} change tag {} {}".format(dbName, self._stName, oldTag, newTag)
        dbc.execute(sql)

class TaskReadData(StateTransitionTask):
    @classmethod
    def getEndState(cls):
        return None  # meaning doesn't affect state

    @classmethod
    def canBeginFrom(cls, state: AnyState):
        return state.canReadData()

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        sTable = self._db.getFixedSuperTable()

        # 1 in 5 chance, simulate a broken connection. 
        if random.randrange(5) == 0:  # TODO: break connection in all situations
            wt.getDbConn().close()
            wt.getDbConn().open()
            print("_r", end="", flush=True)
        
        dbc = wt.getDbConn()
        dbName = self._db.getName()
        for rTbName in sTable.getRegTables(dbc, dbName):  # regular tables
            aggExpr = Dice.choice([
                '*',
                'count(*)',
                'avg(speed)',
                # 'twa(speed)', # TODO: this one REQUIRES a where statement, not reasonable
                'sum(speed)', 
                'stddev(speed)', 
                # SELECTOR functions
                'min(speed)', 
                'max(speed)', 
                'first(speed)', 
                'last(speed)',
                'top(speed, 50)', # TODO: not supported?
                'bottom(speed, 50)', # TODO: not supported?
                'apercentile(speed, 10)', # TODO: TD-1316
                'last_row(speed)',
                # Transformation Functions
                # 'diff(speed)', # TODO: no supported?!
                'spread(speed)'
                ]) # TODO: add more from 'top'
            filterExpr = Dice.choice([ # TODO: add various kind of WHERE conditions
                None
            ])
            try:
                # Run the query against the regular table first
                dbc.execute("select {} from {}.{}".format(aggExpr, dbName, rTbName))
                # Then run it against the super table
                if aggExpr not in ['stddev(speed)']: #TODO: STDDEV not valid for super tables?!
                    dbc.execute("select {} from {}.{}".format(aggExpr, dbName, sTable.getName()))
            except taos.error.ProgrammingError as err:                    
                errno2 = Helper.convertErrno(err.errno)
                logger.debug("[=] Read Failure: errno=0x{:X}, msg: {}, SQL: {}".format(errno2, err, dbc.getLastSql()))
                raise

class TaskDropSuperTable(StateTransitionTask):
    @classmethod
    def getEndState(cls):
        return StateDbOnly()

    @classmethod
    def canBeginFrom(cls, state: AnyState):
        return state.canDropFixedSuperTable()

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        # 1/2 chance, we'll drop the regular tables one by one, in a randomized sequence
        if Dice.throw(2) == 0:
            # print("_7_", end="", flush=True)
            tblSeq = list(range(
                2 + (self.LARGE_NUMBER_OF_TABLES if gConfig.larger_data else self.SMALL_NUMBER_OF_TABLES)))
            random.shuffle(tblSeq)
            tickOutput = False  # if we have spitted out a "d" character for "drop regular table"
            isSuccess = True
            for i in tblSeq:
                regTableName = self.getRegTableName(i)  # "db.reg_table_{}".format(i)
                try:
                    self.execWtSql(wt, "drop table {}.{}".
                        format(self._db.getName(), regTableName))  # nRows always 0, like MySQL
                except taos.error.ProgrammingError as err:
                    # correcting for strange error number scheme                    
                    errno2 = Helper.convertErrno(err.errno)
                    if (errno2 in [0x362]):  # mnode invalid table name
                        isSuccess = False
                        logger.debug("[DB] Acceptable error when dropping a table")
                    continue  # try to delete next regular table

                if (not tickOutput):
                    tickOutput = True  # Print only one time
                    if isSuccess:
                        print("d", end="", flush=True)
                    else:
                        print("f", end="", flush=True)

        # Drop the super table itself
        tblName = self._db.getFixedSuperTableName()
        self.execWtSql(wt, "drop table {}.{}".format(self._db.getName(), tblName))


class TaskAlterTags(StateTransitionTask):
    @classmethod
    def getEndState(cls):
        return None  # meaning doesn't affect state

    @classmethod
    def canBeginFrom(cls, state: AnyState):
        return state.canDropFixedSuperTable()  # if we can drop it, we can alter tags

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        # tblName = self._dbManager.getFixedSuperTableName()
        dbc = wt.getDbConn()
        sTable = self._db.getFixedSuperTable()
        dbName = self._db.getName()
        dice = Dice.throw(4)
        if dice == 0:
            sTable.addTag(dbc, dbName, "extraTag", "int")
            # sql = "alter table db.{} add tag extraTag int".format(tblName)
        elif dice == 1:
            sTable.dropTag(dbc, dbName, "extraTag")
            # sql = "alter table db.{} drop tag extraTag".format(tblName)
        elif dice == 2:
            sTable.dropTag(dbc, dbName, "newTag")
            # sql = "alter table db.{} drop tag newTag".format(tblName)
        else:  # dice == 3
            sTable.changeTag(dbc, dbName, "extraTag", "newTag")
            # sql = "alter table db.{} change tag extraTag newTag".format(tblName)

class TaskRestartService(StateTransitionTask):
    _isRunning = False
    _classLock = threading.Lock()

    @classmethod
    def getEndState(cls):
        return None  # meaning doesn't affect state

    @classmethod
    def canBeginFrom(cls, state: AnyState):
        if gConfig.auto_start_service:
            return state.canDropFixedSuperTable()  # Basicallly when we have the super table
        return False # don't run this otherwise

    CHANCE_TO_RESTART_SERVICE = 200
    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        if not gConfig.auto_start_service: # only execute when we are in -a mode
            print("_a", end="", flush=True)
            return

        with self._classLock:
            if self._isRunning:
                print("Skipping restart task, another running already")
                return
            self._isRunning = True

        if Dice.throw(self.CHANCE_TO_RESTART_SERVICE) == 0: # 1 in N chance
            dbc = wt.getDbConn()
            dbc.execute("show databases") # simple delay, align timing with other workers
            gSvcMgr.restart()

        self._isRunning = False

class TaskAddData(StateTransitionTask):
    # Track which table is being actively worked on
    activeTable: Set[int] = set()

    # We use these two files to record operations to DB, useful for power-off
    # tests
    fAddLogReady = None
    fAddLogDone = None

    @classmethod
    def prepToRecordOps(cls):
        if gConfig.record_ops:
            if (cls.fAddLogReady is None):
                logger.info(
                    "Recording in a file operations to be performed...")
                cls.fAddLogReady = open("add_log_ready.txt", "w")
            if (cls.fAddLogDone is None):
                logger.info("Recording in a file operations completed...")
                cls.fAddLogDone = open("add_log_done.txt", "w")

    @classmethod
    def getEndState(cls):
        return StateHasData()

    @classmethod
    def canBeginFrom(cls, state: AnyState):
        return state.canAddData()

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        # ds = self._dbManager # Quite DANGEROUS here, may result in multi-thread client access
        db = self._db
        dbc = wt.getDbConn()
        tblSeq = list(range(
                self.LARGE_NUMBER_OF_TABLES if gConfig.larger_data else self.SMALL_NUMBER_OF_TABLES))
        random.shuffle(tblSeq)
        for i in tblSeq:
            if (i in self.activeTable):  # wow already active
                print("x", end="", flush=True) # concurrent insertion
            else:
                self.activeTable.add(i)  # marking it active
            
            sTable = db.getFixedSuperTable()
            regTableName = self.getRegTableName(i)  # "db.reg_table_{}".format(i)
            sTable.ensureTable(wt.getDbConn(), db.getName(), regTableName)  # Ensure the table exists           
           
            for j in range(self.LARGE_NUMBER_OF_RECORDS if gConfig.larger_data else self.SMALL_NUMBER_OF_RECORDS):  # number of records per table
                nextInt = db.getNextInt()
                nextTick = db.getNextTick()
                if gConfig.record_ops:
                    self.prepToRecordOps()
                    self.fAddLogReady.write("Ready to write {} to {}\n".format(nextInt, regTableName))
                    self.fAddLogReady.flush()
                    os.fsync(self.fAddLogReady)
                sql = "insert into {}.{} values ('{}', {});".format( # removed: tags ('{}', {})
                    db.getName(),
                    regTableName,
                    # ds.getFixedSuperTableName(),
                    # ds.getNextBinary(), ds.getNextFloat(),
                    nextTick, nextInt)
                dbc.execute(sql)
                # Successfully wrote the data into the DB, let's record it
                # somehow
                te.recordDataMark(nextInt)
                if gConfig.record_ops:
                    self.fAddLogDone.write(
                        "Wrote {} to {}\n".format(
                            nextInt, regTableName))
                    self.fAddLogDone.flush()
                    os.fsync(self.fAddLogDone)

                # Now read it back and verify, we might encounter an error if table is dropped
                if gConfig.verify_data: # only if command line asks for it
                    try:
                        readBack = dbc.queryScalar("SELECT speed from {}.{} WHERE ts= '{}'".
                            format(db.getName(), regTableName, nextTick))
                        if readBack != nextInt :
                            raise taos.error.ProgrammingError(
                                "Failed to read back same data, wrote: {}, read: {}"
                                .format(nextInt, readBack), 0x999)
                    except taos.error.ProgrammingError as err:
                        errno = Helper.convertErrno(err.errno)
                        if errno in [0x991, 0x992]  : # not a single result
                            raise taos.error.ProgrammingError(
                                "Failed to read back same data for tick: {}, wrote: {}, read: {}"
                                .format(nextTick, nextInt, "Empty Result" if errno==0x991 else "Multiple Result"),
                                errno)
                        # Re-throw no matter what
                        raise
                

            self.activeTable.discard(i)  # not raising an error, unlike remove


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
        return "[{}]{}".format(threading.get_ident() % 10000, msg), kwargs
        # return '[%s] %s' % (self.extra['connid'], msg), kwargs


class SvcManager:
    def __init__(self):
        print("Starting TDengine Service Manager")
        # signal.signal(signal.SIGTERM, self.sigIntHandler) # Moved to MainExec
        # signal.signal(signal.SIGINT, self.sigIntHandler)
        # signal.signal(signal.SIGUSR1, self.sigUsrHandler)  # different handler!

        self.inSigHandler = False
        # self._status = MainExec.STATUS_RUNNING # set inside
        # _startTaosService()
        self.svcMgrThread = None # type: ServiceManagerThread
        self._lock = threading.Lock()
        self._isRestarting = False

    def _doMenu(self):
        choice = ""
        while True:
            print("\nInterrupting Service Program, Choose an Action: ")
            print("1: Resume")
            print("2: Terminate")
            print("3: Restart")
            # Remember to update the if range below
            # print("Enter Choice: ", end="", flush=True)
            while choice == "":
                choice = input("Enter Choice: ")
                if choice != "":
                    break  # done with reading repeated input
            if choice in ["1", "2", "3"]:
                break  # we are done with whole method
            print("Invalid choice, please try again.")
            choice = ""  # reset
        return choice

    def sigUsrHandler(self, signalNumber, frame):
        print("Interrupting main thread execution upon SIGUSR1")
        if self.inSigHandler:  # already
            print("Ignoring repeated SIG...")
            return  # do nothing if it's already not running
        self.inSigHandler = True

        choice = self._doMenu()
        if choice == "1":
            # TODO: can the sub-process be blocked due to us not reading from
            # queue?
            self.sigHandlerResume()
        elif choice == "2":
            self.stopTaosService()
        elif choice == "3": # Restart
            self.restart()
        else:
            raise RuntimeError("Invalid menu choice: {}".format(choice))

        self.inSigHandler = False

    def sigIntHandler(self, signalNumber, frame):
        print("SvcManager: INT Signal Handler starting...")
        if self.inSigHandler:
            print("Ignoring repeated SIG_INT...")
            return
        self.inSigHandler = True

        self.stopTaosService()
        print("SvcManager: INT Signal Handler returning...")
        self.inSigHandler = False

    def sigHandlerResume(self):
        print("Resuming TDengine service manager thread (main thread)...\n\n")

    def _checkServiceManagerThread(self):
        if self.svcMgrThread:  # valid svc mgr thread
            if self.svcMgrThread.isStopped():  # done?
                self.svcMgrThread.procIpcBatch()  # one last time. TODO: appropriate?
                self.svcMgrThread = None  # no more

    def _procIpcAll(self):
        while self.isRunning() or self.isRestarting() :  # for as long as the svc mgr thread is still here
            if self.isRunning():
                self.svcMgrThread.procIpcBatch()  # regular processing,
                self._checkServiceManagerThread()
            elif self.isRetarting():
                print("Service restarting...")
            time.sleep(0.5)  # pause, before next round
        print(
            "Service Manager Thread (with subprocess) has ended, main thread now exiting...")

    def startTaosService(self):
        with self._lock:
            if self.svcMgrThread:
                raise RuntimeError("Cannot start TAOS service when one may already be running")

            # Find if there's already a taosd service, and then kill it
            for proc in psutil.process_iter():
                if proc.name() == 'taosd':
                    print("Killing an existing TAOSD process in 2 seconds... press CTRL-C to interrupe")
                    time.sleep(2.0)
                    proc.kill()
                # print("Process: {}".format(proc.name()))

            
            self.svcMgrThread = ServiceManagerThread()  # create the object
            print("Attempting to start TAOS service started, printing out output...")
            self.svcMgrThread.start()            
            self.svcMgrThread.procIpcBatch(trimToTarget=10, forceOutput=True)  # for printing 10 lines             
            print("TAOS service started")

    def stopTaosService(self, outputLines=20):
        with self._lock:
            if not self.isRunning():
                logger.warning("Cannot stop TAOS service, not running")
                return

            print("Terminating Service Manager Thread (SMT) execution...")
            self.svcMgrThread.stop()
            if self.svcMgrThread.isStopped():
                self.svcMgrThread.procIpcBatch(outputLines)  # one last time
                self.svcMgrThread = None
                print("End of TDengine Service Output")
                print("----- TDengine Service (managed by SMT) is now terminated -----\n")
            else:
                print("WARNING: SMT did not terminate as expected")

    def run(self):
        self.startTaosService()
        self._procIpcAll()  # pump/process all the messages, may encounter SIG + restart
        if self.isRunning():  # if sig handler hasn't destroyed it by now
            self.stopTaosService()  # should have started already

    def restart(self):
        if self._isRestarting:
            logger.warning("Cannot restart service when it's already restarting")
            return

        self._isRestarting = True
        if self.isRunning():
            self.stopTaosService()
        else:
            logger.warning("Service not running when restart requested")

        self.startTaosService()
        self._isRestarting = False

    def isRunning(self):
        return self.svcMgrThread != None

    def isRestarting(self):
        return self._isRestarting

class ServiceManagerThread:
    MAX_QUEUE_SIZE = 10000

    def __init__(self):
        self._tdeSubProcess = None # type: TdeSubProcess
        self._thread = None
        self._status = None

    def getStatus(self):
        return self._status

    def isRunning(self):
        # return self._thread and self._thread.is_alive()
        return self._status == MainExec.STATUS_RUNNING

    def isStopping(self):
        return self._status == MainExec.STATUS_STOPPING

    def isStopped(self):
        return self._status == MainExec.STATUS_STOPPED

    # Start the thread (with sub process), and wait for the sub service
    # to become fully operational
    def start(self):
        if self._thread:
            raise RuntimeError("Unexpected _thread")
        if self._tdeSubProcess:
            raise RuntimeError("TDengine sub process already created/running")

        self._status = MainExec.STATUS_STARTING

        self._tdeSubProcess = TdeSubProcess()
        self._tdeSubProcess.start()

        self._ipcQueue = Queue()
        self._thread = threading.Thread( # First thread captures server OUTPUT
            target=self.svcOutputReader,
            args=(self._tdeSubProcess.getStdOut(), self._ipcQueue))
        self._thread.daemon = True  # thread dies with the program
        self._thread.start()

        self._thread2 = threading.Thread( # 2nd thread captures server ERRORs
            target=self.svcErrorReader,
            args=(self._tdeSubProcess.getStdErr(), self._ipcQueue))
        self._thread2.daemon = True  # thread dies with the program
        self._thread2.start()

        # wait for service to start
        for i in range(0, 100):
            time.sleep(1.0)
            # self.procIpcBatch() # don't pump message during start up
            print("_zz_", end="", flush=True)
            if self._status == MainExec.STATUS_RUNNING:
                logger.info("[] TDengine service READY to process requests")
                return  # now we've started
        # TODO: handle this better?
        self.procIpcBatch(100, True) # display output before cronking out, trim to last 20 msgs, force output
        raise RuntimeError("TDengine service did not start successfully")

    def stop(self):
        # can be called from both main thread or signal handler
        print("Terminating TDengine service running as the sub process...")
        if self.isStopped():
            print("Service already stopped")
            return
        if self.isStopping():
            print("Service is already being stopped")
            return
        # Linux will send Control-C generated SIGINT to the TDengine process
        # already, ref:
        # https://unix.stackexchange.com/questions/176235/fork-and-how-signals-are-delivered-to-processes
        if not self._tdeSubProcess:
            raise RuntimeError("sub process object missing")

        self._status = MainExec.STATUS_STOPPING
        retCode = self._tdeSubProcess.stop()
        print("Attempted to stop sub process, got return code: {}".format(retCode))
        if (retCode==-11): # SGV
            logger.error("[[--ERROR--]]: TDengine service SEGV fault (check core file!)")

        if self._tdeSubProcess.isRunning():  # still running
            print("FAILED to stop sub process, it is still running... pid = {}".format(
                    self._tdeSubProcess.getPid()))
        else:
            self._tdeSubProcess = None  # not running any more
            self.join()  # stop the thread, change the status, etc.

    def join(self):
        # TODO: sanity check
        if not self.isStopping():
            raise RuntimeError(
                "Unexpected status when ending svc mgr thread: {}".format(
                    self._status))

        if self._thread:
            self._thread.join()
            self._thread = None
            self._status = MainExec.STATUS_STOPPED
            # STD ERR thread
            self._thread2.join()
            self._thread2 = None
        else:
            print("Joining empty thread, doing nothing")

    def _trimQueue(self, targetSize):
        if targetSize <= 0:
            return  # do nothing
        q = self._ipcQueue
        if (q.qsize() <= targetSize):  # no need to trim
            return

        logger.debug("Triming IPC queue to target size: {}".format(targetSize))
        itemsToTrim = q.qsize() - targetSize
        for i in range(0, itemsToTrim):
            try:
                q.get_nowait()
            except Empty:
                break  # break out of for loop, no more trimming

    TD_READY_MSG = "TDengine is initialized successfully"

    def procIpcBatch(self, trimToTarget=0, forceOutput=False):
        self._trimQueue(trimToTarget)  # trim if necessary
        # Process all the output generated by the underlying sub process,
        # managed by IO thread
        print("<", end="", flush=True)
        while True:
            try:
                line = self._ipcQueue.get_nowait()  # getting output at fast speed
                self._printProgress("_o")
            except Empty:
                # time.sleep(2.3) # wait only if there's no output
                # no more output
                print(".>", end="", flush=True)
                return  # we are done with THIS BATCH
            else:  # got line, printing out
                if forceOutput:
                    logger.info(line)
                else:
                    logger.debug(line)
        print(">", end="", flush=True)

    _ProgressBars = ["--", "//", "||", "\\\\"]

    def _printProgress(self, msg):  # TODO: assuming 2 chars
        print(msg, end="", flush=True)
        pBar = self._ProgressBars[Dice.throw(4)]
        print(pBar, end="", flush=True)
        print('\b\b\b\b', end="", flush=True)

    def svcOutputReader(self, out: IO, queue):
        # Important Reference: https://stackoverflow.com/questions/375427/non-blocking-read-on-a-subprocess-pipe-in-python
        # print("This is the svcOutput Reader...")
        # for line in out :
        for line in iter(out.readline, b''):
            # print("Finished reading a line: {}".format(line))
            # print("Adding item to queue...")
            try:
                line = line.decode("utf-8").rstrip()
            except UnicodeError:
                print("\nNon-UTF8 server output: {}\n".format(line))

            # This might block, and then causing "out" buffer to block
            queue.put(line)
            self._printProgress("_i")

            if self._status == MainExec.STATUS_STARTING:  # we are starting, let's see if we have started
                if line.find(self.TD_READY_MSG) != -1:  # found
                    logger.info("Waiting for the service to become FULLY READY")
                    time.sleep(1.0) # wait for the server to truly start. TODO: remove this
                    logger.info("Service is now FULLY READY")   
                    self._status = MainExec.STATUS_RUNNING                 

            # Trim the queue if necessary: TODO: try this 1 out of 10 times
            self._trimQueue(self.MAX_QUEUE_SIZE * 9 // 10)  # trim to 90% size

            if self.isStopping():  # TODO: use thread status instead
                # WAITING for stopping sub process to finish its outptu
                print("_w", end="", flush=True)

            # queue.put(line)
        # meaning sub process must have died
        print("\nNo more output from IO thread managing TDengine service")
        out.close()

    def svcErrorReader(self, err: IO, queue):
        for line in iter(err.readline, b''):
            print("\nTDengine Service (taosd) ERROR (from stderr): {}".format(line))


class TdeSubProcess:
    def __init__(self):
        self.subProcess = None

    def getStdOut(self):
        return self.subProcess.stdout

    def getStdErr(self):
        return self.subProcess.stderr

    def isRunning(self):
        return self.subProcess is not None

    def getPid(self):
        return self.subProcess.pid

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))
        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("communit")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        return buildPath

    def start(self):
        ON_POSIX = 'posix' in sys.builtin_module_names

        taosdPath = self.getBuildPath() + "/build/bin/taosd"
        cfgPath = self.getBuildPath() + "/test/cfg"

        # Delete the log files
        logPath = self.getBuildPath() + "/test/log"
        # ref: https://stackoverflow.com/questions/1995373/deleting-all-files-in-a-directory-with-python/1995397
        # filelist = [ f for f in os.listdir(logPath) ] # if f.endswith(".bak") ]
        # for f in filelist:
        #     filePath = os.path.join(logPath, f)
        #     print("Removing log file: {}".format(filePath))
        #     os.remove(filePath)        
        if os.path.exists(logPath):
            logPathSaved = logPath + "_" + time.strftime('%Y-%m-%d-%H-%M-%S')
            logger.info("Saving old log files to: {}".format(logPathSaved))
            os.rename(logPath, logPathSaved)
        # os.mkdir(logPath) # recreate, no need actually, TDengine will auto-create with proper perms
            
        svcCmd = [taosdPath, '-c', cfgPath]
        # svcCmdSingle = "{} -c {}".format(taosdPath, cfgPath)
        # svcCmd = ['vmstat', '1']
        if self.subProcess:  # already there
            raise RuntimeError("Corrupt process state")

        # print("Starting service: {}".format(svcCmd))
        self.subProcess = subprocess.Popen(
            svcCmd, shell=False,
            # svcCmdSingle, shell=True, # capture core dump?
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            # bufsize=1, # not supported in binary mode
            close_fds=ON_POSIX
            )  # had text=True, which interferred with reading EOF

    def stop(self):
        if not self.subProcess:
            print("Sub process already stopped")
            return -1

        retCode = self.subProcess.poll() # contains real sub process return code
        if retCode:  # valid return code, process ended
            self.subProcess = None
        else:  # process still alive, let's interrupt it
            print(
                "Sub process is running, sending SIG_INT and waiting for it to terminate...")
            # sub process should end, then IPC queue should end, causing IO
            # thread to end
            self.subProcess.send_signal(signal.SIGINT)
            try:
                self.subProcess.wait(10)
                retCode = self.subProcess.returncode
            except subprocess.TimeoutExpired as err:
                print("Time out waiting for TDengine service process to exit")
                retCode = -3
            else:
                print("TDengine service process terminated successfully from SIG_INT")
                retCode = -4
                self.subProcess = None
        return retCode

class ThreadStacks: # stack info for all threads
    def __init__(self):
        self._allStacks = {}
        allFrames = sys._current_frames()
        for th in threading.enumerate():                        
            stack = traceback.extract_stack(allFrames[th.ident])     
            self._allStacks[th.native_id] = stack

    def print(self, filteredEndName = None, filterInternal = False):
        for thNid, stack in self._allStacks.items(): # for each thread            
            lastFrame = stack[-1]
            if filteredEndName: # we need to filter out stacks that match this name                
                if lastFrame.name == filteredEndName : # end did not match
                    continue
            if filterInternal:
                if lastFrame.name in ['wait', 'invoke_excepthook', 
                    '_wait', # The Barrier exception
                    'svcOutputReader', # the svcMgr thread
                    '__init__']: # the thread that extracted the stack
                    continue # ignore
            # Now print
            print("\n<----- Thread Info for ID: {}".format(thNid))
            for frame in stack:
                # print(frame)
                print("File {filename}, line {lineno}, in {name}".format(
                    filename=frame.filename, lineno=frame.lineno, name=frame.name))
                print("    {}".format(frame.line))
            print("-----> End of Thread Info\n")

class ClientManager:
    def __init__(self):
        print("Starting service manager")
        # signal.signal(signal.SIGTERM, self.sigIntHandler)
        # signal.signal(signal.SIGINT, self.sigIntHandler)

        self._status = MainExec.STATUS_RUNNING
        self.tc = None

        self.inSigHandler = False

    def sigIntHandler(self, signalNumber, frame):
        if self._status != MainExec.STATUS_RUNNING:
            print("Repeated SIGINT received, forced exit...")
            # return  # do nothing if it's already not running
            sys.exit(-1)
        self._status = MainExec.STATUS_STOPPING  # immediately set our status

        print("ClientManager: Terminating program...")
        self.tc.requestToStop()

    def _doMenu(self):
        choice = ""
        while True:
            print("\nInterrupting Client Program, Choose an Action: ")
            print("1: Resume")
            print("2: Terminate")
            print("3: Show Threads")
            # Remember to update the if range below
            # print("Enter Choice: ", end="", flush=True)
            while choice == "":
                choice = input("Enter Choice: ")
                if choice != "":
                    break  # done with reading repeated input
            if choice in ["1", "2", "3"]:
                break  # we are done with whole method
            print("Invalid choice, please try again.")
            choice = ""  # reset
        return choice

    def sigUsrHandler(self, signalNumber, frame):
        print("Interrupting main thread execution upon SIGUSR1")
        if self.inSigHandler:  # already
            print("Ignoring repeated SIG_USR1...")
            return  # do nothing if it's already not running
        self.inSigHandler = True

        choice = self._doMenu()
        if choice == "1":
            print("Resuming execution...")
            time.sleep(1.0)
        elif choice == "2":
            print("Not implemented yet")
            time.sleep(1.0)
        elif choice == "3":
            ts = ThreadStacks()
            ts.print()
        else:
            raise RuntimeError("Invalid menu choice: {}".format(choice))

        self.inSigHandler = False

    # TODO: need to revise how we verify data durability
    # def _printLastNumbers(self):  # to verify data durability
    #     dbManager = DbManager()
    #     dbc = dbManager.getDbConn()
    #     if dbc.query("show databases") <= 1:  # no database (we have a default called "log")
    #         return
    #     dbc.execute("use db")
    #     if dbc.query("show tables") == 0:  # no tables
    #         return

    #     sTbName = dbManager.getFixedSuperTableName()

    #     # get all regular tables
    #     # TODO: analyze result set later
    #     dbc.query("select TBNAME from db.{}".format(sTbName))
    #     rTables = dbc.getQueryResult()

    #     bList = TaskExecutor.BoundedList()
    #     for rTbName in rTables:  # regular tables
    #         dbc.query("select speed from db.{}".format(rTbName[0]))
    #         numbers = dbc.getQueryResult()
    #         for row in numbers:
    #             # print("<{}>".format(n), end="", flush=True)
    #             bList.add(row[0])

    #     print("Top numbers in DB right now: {}".format(bList))
    #     print("TDengine client execution is about to start in 2 seconds...")
    #     time.sleep(2.0)
    #     dbManager = None  # release?

    def run(self, svcMgr):    
        # self._printLastNumbers()
        global gConfig

        dbManager = DbManager()  # Regular function
        thPool = ThreadPool(gConfig.num_threads, gConfig.max_steps)
        self.tc = ThreadCoordinator(thPool, dbManager)
        
        self.tc.run()
        # print("exec stats: {}".format(self.tc.getExecStats()))
        # print("TC failed = {}".format(self.tc.isFailed()))
        if svcMgr: # gConfig.auto_start_service:
            svcMgr.stopTaosService()
            svcMgr = None
        # Print exec status, etc., AFTER showing messages from the server
        self.conclude()
        # print("TC failed (2) = {}".format(self.tc.isFailed()))
        # Linux return code: ref https://shapeshed.com/unix-exit-codes/
        ret = 1 if self.tc.isFailed() else 0
        self.tc.cleanup()

        # Release global variables
        gConfig = None
        gSvcMgr = None
        logger = None

        # Release variables here
        self.tc = None
        thPool = None
        dbManager = None

        gc.collect() # force garbage collection
        # h = hpy()
        # print("\n----- Final Python Heap -----\n")        
        # print(h.heap())

        return ret

    def conclude(self):
        # self.tc.getDbManager().cleanUp() # clean up first, so we can show ZERO db connections
        self.tc.printStats()

        
        

class MainExec:
    STATUS_STARTING = 1
    STATUS_RUNNING = 2
    STATUS_STOPPING = 3
    STATUS_STOPPED = 4

    def __init__(self):        
        self._clientMgr = None
        self._svcMgr = None

        signal.signal(signal.SIGTERM, self.sigIntHandler)
        signal.signal(signal.SIGINT,  self.sigIntHandler)
        signal.signal(signal.SIGUSR1, self.sigUsrHandler)  # different handler!

    def sigUsrHandler(self, signalNumber, frame):
        if self._clientMgr:
            self._clientMgr.sigUsrHandler(signalNumber, frame)
        elif self._svcMgr: # Only if no client mgr, we are running alone
            self._svcMgr.sigUsrHandler(signalNumber, frame)
        
    def sigIntHandler(self, signalNumber, frame):
        if self._svcMgr:
            self._svcMgr.sigIntHandler(signalNumber, frame)
        if self._clientMgr:
            self._clientMgr.sigIntHandler(signalNumber, frame)

    def runClient(self):
        global gSvcMgr
        if gConfig.auto_start_service:
            self._svcMgr = SvcManager()
            gSvcMgr = self._svcMgr # hack alert
            self._svcMgr.startTaosService() # we start, don't run
        
        self._clientMgr = ClientManager()
        ret = None
        try: 
            ret = self._clientMgr.run(self._svcMgr) # stop TAOS service inside
        except requests.exceptions.ConnectionError as err:
            logger.warning("Failed to open REST connection to DB: {}".format(err.getMessage()))
            # don't raise
        return ret

    def runService(self):
        global gSvcMgr
        self._svcMgr = SvcManager()
        gSvcMgr = self._svcMgr # save it in a global variable TODO: hack alert

        self._svcMgr.run() # run to some end state
        self._svcMgr = None 
        gSvcMgr = None        

    def runTemp(self):  # for debugging purposes
        # # Hack to exercise reading from disk, imcreasing coverage. TODO: fix
        # dbc = dbState.getDbConn()
        # sTbName = dbState.getFixedSuperTableName()
        # dbc.execute("create database if not exists db")
        # if not dbState.getState().equals(StateEmpty()):
        #     dbc.execute("use db")

        # rTables = None
        # try: # the super table may not exist
        #     sql = "select TBNAME from db.{}".format(sTbName)
        #     logger.info("Finding out tables in super table: {}".format(sql))
        #     dbc.query(sql) # TODO: analyze result set later
        #     logger.info("Fetching result")
        #     rTables = dbc.getQueryResult()
        #     logger.info("Result: {}".format(rTables))
        # except taos.error.ProgrammingError as err:
        #     logger.info("Initial Super table OPS error: {}".format(err))

        # # sys.exit()
        # if ( not rTables == None):
        #     # print("rTables[0] = {}, type = {}".format(rTables[0], type(rTables[0])))
        #     try:
        #         for rTbName in rTables : # regular tables
        #             ds = dbState
        #             logger.info("Inserting into table: {}".format(rTbName[0]))
        #             sql = "insert into db.{} values ('{}', {});".format(
        #                 rTbName[0],
        #                 ds.getNextTick(), ds.getNextInt())
        #             dbc.execute(sql)
        #         for rTbName in rTables : # regular tables
        #             dbc.query("select * from db.{}".format(rTbName[0])) # TODO: check success failure
        #         logger.info("Initial READING operation is successful")
        #     except taos.error.ProgrammingError as err:
        #         logger.info("Initial WRITE/READ error: {}".format(err))

        # Sandbox testing code
        # dbc = dbState.getDbConn()
        # while True:
        #     rows = dbc.query("show databases")
        #     print("Rows: {}, time={}".format(rows, time.time()))
        return


def main():
    # Super cool Python argument library:
    # https://docs.python.org/3/library/argparse.html
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent('''\
            TDengine Auto Crash Generator (PLEASE NOTICE the Prerequisites Below)
            ---------------------------------------------------------------------
            1. You build TDengine in the top level ./build directory, as described in offical docs
            2. You run the server there before this script: ./build/bin/taosd -c test/cfg

            '''))

    # parser.add_argument('-a', '--auto-start-service', action='store_true',                        
    #                     help='Automatically start/stop the TDengine service (default: false)')
    # parser.add_argument('-c', '--connector-type', action='store', default='native', type=str,
    #                     help='Connector type to use: native, rest, or mixed (default: 10)')
    # parser.add_argument('-d', '--debug', action='store_true',                        
    #                     help='Turn on DEBUG mode for more logging (default: false)')
    # parser.add_argument('-e', '--run-tdengine', action='store_true',                        
    #                     help='Run TDengine service in foreground (default: false)')
    # parser.add_argument('-l', '--larger-data', action='store_true',                        
    #                     help='Write larger amount of data during write operations (default: false)')
    # parser.add_argument('-p', '--per-thread-db-connection', action='store_true',                        
    #                     help='Use a single shared db connection (default: false)')
    # parser.add_argument('-r', '--record-ops', action='store_true',                        
    #                     help='Use a pair of always-fsynced fils to record operations performing + performed, for power-off tests (default: false)')                    
    # parser.add_argument('-s', '--max-steps', action='store', default=1000, type=int,
    #                     help='Maximum number of steps to run (default: 100)')
    # parser.add_argument('-t', '--num-threads', action='store', default=5, type=int,
    #                     help='Number of threads to run (default: 10)')
    # parser.add_argument('-x', '--continue-on-exception', action='store_true',                        
    #                     help='Continue execution after encountering unexpected/disallowed errors/exceptions (default: false)')                        

    parser.add_argument(
        '-a',
        '--auto-start-service',
        action='store_true',
        help='Automatically start/stop the TDengine service (default: false)')
    parser.add_argument(
        '-b',
        '--max-dbs',
        action='store',
        default=0,
        type=int,
        help='Maximum number of DBs to keep, set to disable dropping DB. (default: 0)')
    parser.add_argument(
        '-c',
        '--connector-type',
        action='store',
        default='native',
        type=str,
        help='Connector type to use: native, rest, or mixed (default: 10)')
    parser.add_argument(
        '-d',
        '--debug',
        action='store_true',
        help='Turn on DEBUG mode for more logging (default: false)')
    parser.add_argument(
        '-e',
        '--run-tdengine',
        action='store_true',
        help='Run TDengine service in foreground (default: false)')
    parser.add_argument(
        '-i',
        '--max-replicas',
        action='store',
        default=1,
        type=int,
        help='Maximum number of replicas to use, when testing against clusters. (default: 1)')
    parser.add_argument(
        '-l',
        '--larger-data',
        action='store_true',
        help='Write larger amount of data during write operations (default: false)')
    parser.add_argument(
        '-p',
        '--per-thread-db-connection',
        action='store_true',
        help='Use a single shared db connection (default: false)')
    parser.add_argument(
        '-r',
        '--record-ops',
        action='store_true',
        help='Use a pair of always-fsynced fils to record operations performing + performed, for power-off tests (default: false)')
    parser.add_argument(
        '-s',
        '--max-steps',
        action='store',
        default=1000,
        type=int,
        help='Maximum number of steps to run (default: 100)')
    parser.add_argument(
        '-t',
        '--num-threads',
        action='store',
        default=5,
        type=int,
        help='Number of threads to run (default: 10)')
    parser.add_argument(
        '-v',
        '--verify-data',
        action='store_true',
        help='Verify data written in a number of places by reading back (default: false)')
    parser.add_argument(
        '-x',
        '--continue-on-exception',
        action='store_true',
        help='Continue execution after encountering unexpected/disallowed errors/exceptions (default: false)')

    global gConfig
    gConfig = parser.parse_args()

    # Logging Stuff
    global logger
    _logger = logging.getLogger('CrashGen')  # real logger
    _logger.addFilter(LoggingFilter())
    ch = logging.StreamHandler()
    _logger.addHandler(ch)

    # Logging adapter, to be used as a logger
    logger = MyLoggingAdapter(_logger, [])

    if (gConfig.debug):
        logger.setLevel(logging.DEBUG)  # default seems to be INFO
    else:
        logger.setLevel(logging.INFO)

    Dice.seed(0)  # initial seeding of dice

    # Run server or client
    mExec = MainExec()
    if gConfig.run_tdengine:  # run server
        mExec.runService()
    else:
        return mExec.runClient()


if __name__ == "__main__":
    exitCode = main()
    # print("Exiting with code: {}".format(exitCode))
    sys.exit(exitCode)
