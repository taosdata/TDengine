# -----!/usr/bin/python3.7
###################################################################
#           Copyright (c) 2016-2021 by TAOS Technologies, Inc.
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

from typing import Any, Set, Tuple
from typing import Dict
from typing import List
from typing import Optional # Type hinting, ref: https://stackoverflow.com/questions/19202633/python-3-type-hinting-for-none

import textwrap
import time
import datetime
import random
import threading
import argparse

import sys
import os
import io
import signal
import traceback
import requests
# from guppy import hpy
import gc
import taos


from .shared.types import TdColumns, TdTags

# from crash_gen import ServiceManager, TdeInstance, TdeSubProcess
# from crash_gen import ServiceManager, Config, DbConn, DbConnNative, Dice, DbManager, Status, Logging, Helper, \
#     CrashGenError, Progress, MyTDSql, \
#     TdeInstance

from .service_manager import ServiceManager, TdeInstance

from .shared.config import Config
from .shared.db import DbConn, DbManager, DbConnNative, MyTDSql
from .shared.misc import Dice, Logging, Helper, Status, CrashGenError, Progress
from .shared.types import TdDataType

# Config.init()

# Require Python 3
if sys.version_info[0] < 3:
    raise Exception("Must be using Python 3")

# Global variables, tried to keep a small number.

# Command-line/Environment Configurations, will set a bit later
# ConfigNameSpace = argparse.Namespace
# gConfig:    argparse.Namespace 
gSvcMgr:    Optional[ServiceManager] # TODO: refactor this hack, use dep injection
# logger:     logging.Logger
gContainer: Container

# def runThread(wt: WorkerThread):
#     wt.run()


class WorkerThread:
    def __init__(self, pool: ThreadPool, tid, tc: ThreadCoordinator):
        """
            Note: this runs in the main thread context
        """                 
        # self._curStep = -1
        self._pool = pool
        self._tid = tid
        self._tc = tc  # type: ThreadCoordinator
        # self.threadIdent = threading.get_ident()
        # self._thread = threading.Thread(target=runThread, args=(self,))
        self._thread = threading.Thread(target=self.run)
        self._stepGate = threading.Event()

        # Let us have a DB connection of our own
        if (Config.getConfig().per_thread_db_connection):  # type: ignore
            # print("connector_type = {}".format(gConfig.connector_type))
            tInst = gContainer.defTdeInstance
            if Config.getConfig().connector_type == 'native':                
                self._dbConn = DbConn.createNative(tInst.getDbTarget()) 
            elif Config.getConfig().connector_type == 'rest':
                self._dbConn = DbConn.createRest(tInst.getDbTarget()) 
            elif Config.getConfig().connector_type == 'mixed':
                if Dice.throw(2) == 0: # 1/2 chance
                    self._dbConn = DbConn.createNative(tInst.getDbTarget()) 
                else:
                    self._dbConn = DbConn.createRest(tInst.getDbTarget()) 
            else:
                raise RuntimeError("Unexpected connector type: {}".format(Config.getConfig().connector_type))

        # self._dbInUse = False  # if "use db" was executed already

    def logDebug(self, msg):
        Logging.debug("    TRD[{}] {}".format(self._tid, msg))

    def logInfo(self, msg):
        Logging.info("    TRD[{}] {}".format(self._tid, msg))

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
        Logging.info("Starting to run thread: {}".format(self._tid))

        if (Config.getConfig().per_thread_db_connection):  # type: ignore
            Logging.debug("Worker thread openning database connection")
            self._dbConn.open()

        self._doTaskLoop()

        # clean up
        if (Config.getConfig().per_thread_db_connection):  # type: ignore
            if self._dbConn.isOpen: #sometimes it is not open
                self._dbConn.close()
            else:
                Logging.warning("Cleaning up worker thread, dbConn already closed")

    def _doTaskLoop(self):
        # while self._curStep < self._pool.maxSteps:
        # tc = ThreadCoordinator(None)
        while True:
            tc = self._tc  # Thread Coordinator, the overall master
            try:
                tc.crossStepBarrier()  # shared barrier first, INCLUDING the last one
            except threading.BrokenBarrierError as err: # main thread timed out
                print("_bto", end="")
                Logging.debug("[TRD] Worker thread exiting due to main thread barrier time-out")
                break

            Logging.debug("[TRD] Worker thread [{}] exited barrier...".format(self._tid))
            self.crossStepGate()   # then per-thread gate, after being tapped
            Logging.debug("[TRD] Worker thread [{}] exited step gate...".format(self._tid))
            if not self._tc.isRunning():
                print("_wts", end="")
                Logging.debug("[TRD] Thread Coordinator not running any more, worker thread now stopping...")
                break

            
            # Before we fetch the task and run it, let's ensure we properly "use" the database (not needed any more)
            try:
                if (Config.getConfig().per_thread_db_connection):  # most likely TRUE
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
            Logging.debug( "[TRD] Worker thread [{}] about to fetch task".format(self._tid))
            task = tc.fetchTask()

            # Execute such a task
            Logging.debug("[TRD] Worker thread [{}] about to execute task: {}".format(
                    self._tid, task.__class__.__name__))
            task.execute(self)
            tc.saveExecutedTask(task)
            Logging.debug("[TRD] Worker thread [{}] finished executing task".format(self._tid))

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
        Logging.debug(
            "[TRD] Worker thread {} about to cross the step gate".format(
                self._tid))
        self._stepGate.wait()
        self._stepGate.clear()

        # self._curStep += 1  # off to a new step...

    def tapStepGate(self):  # give it a tap, release the thread waiting there
        # self.verifyThreadAlive()
        self.verifyThreadMain()  # only allowed for main thread

        if self._thread.is_alive():
            Logging.debug("[TRD] Tapping worker thread {}".format(self._tid))
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
        if (Config.getConfig().per_thread_db_connection):
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
    WORKER_THREAD_TIMEOUT = 120  # Normal: 120

    def __init__(self, pool: ThreadPool, dbManager: DbManager):
        self._curStep = -1  # first step is 0
        self._pool = pool
        # self._wd = wd
        self._te = None  # prepare for every new step
        self._dbManager = dbManager # type: Optional[DbManager] # may be freed
        self._executedTasks: List[Task] = []  # in a given step
        self._lock = threading.RLock()  # sync access for a few things

        self._stepBarrier = threading.Barrier(
            self._pool.numThreads + 1)  # one barrier for all threads
        self._execStats = ExecutionStats()
        self._runStatus = Status.STATUS_RUNNING
        self._initDbs()
        self._stepStartTime = None  # Track how long it takes to execute each step

    def getTaskExecutor(self):
        if self._te is None:
            raise CrashGenError("Unexpected empty TE")
        return self._te

    def getDbManager(self) -> DbManager:
        if self._dbManager is None:
            raise ChildProcessError("Unexpected empty _dbManager")
        return self._dbManager

    def crossStepBarrier(self, timeout=None):
        self._stepBarrier.wait(timeout) 

    def requestToStop(self):
        self._runStatus = Status.STATUS_STOPPING
        self._execStats.registerFailure("User Interruption")

    def _runShouldEnd(self, transitionFailed, hasAbortedTask, workerTimeout):
        maxSteps = Config.getConfig().max_steps  # type: ignore
        if self._curStep >= (maxSteps - 1): # maxStep==10, last curStep should be 9
            return True
        if self._runStatus != Status.STATUS_RUNNING:
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
        Logging.debug(
            "--\r\n\n--> Step {} starts with main thread waking up".format(self._curStep))

        # A new TE for the new step
        self._te = None # set to empty first, to signal worker thread to stop
        if not transitionFailed:  # only if not failed
            self._te = TaskExecutor(self._curStep)

        Logging.debug("[TRD] Main thread waking up at step {}, tapping worker threads".format(
                self._curStep))  # Now not all threads had time to go to sleep
        # Worker threads will wake up at this point, and each execute it's own task
        self.tapAllThreads() # release all worker thread from their "gates"

    def _syncAtBarrier(self):
         # Now main thread (that's us) is ready to enter a step
        # let other threads go past the pool barrier, but wait at the
        # thread gate
        Logging.debug("[TRD] Main thread about to cross the barrier")
        self.crossStepBarrier(timeout=self.WORKER_THREAD_TIMEOUT)
        self._stepBarrier.reset()  # Other worker threads should now be at the "gate"
        Logging.debug("[TRD] Main thread finished crossing the barrier")

    def _doTransition(self):
        transitionFailed = False
        try:
            for x in self._dbs:
                db = x # type: Database
                sm = db.getStateMachine()
                Logging.debug("[STT] starting transitions for DB: {}".format(db.getName()))
                # at end of step, transiton the DB state
                tasksForDb = db.filterTasks(self._executedTasks)
                sm.transition(tasksForDb, self.getDbManager().getDbConn())
                Logging.debug("[STT] transition ended for DB: {}".format(db.getName()))

            # Due to limitation (or maybe not) of the TD Python library,
            # we cannot share connections across threads
            # Here we are in main thread, we cannot operate the connections created in workers
            # Moving below to task loop
            # if sm.hasDatabase():
            #     for t in self._pool.threadList:
            #         Logging.debug("[DB] use db for all worker threads")
            #         t.useDb()
                    # t.execSql("use db") # main thread executing "use
                    # db" on behalf of every worker thread

        except taos.error.ProgrammingError as err:
            if (err.msg == 'network unavailable'):  # broken DB connection
                Logging.info("DB connection broken, execution failed")
                traceback.print_stack()
                transitionFailed = True
                self._te = None  # Not running any more
                self._execStats.registerFailure("Broken DB Connection")
                # continue # don't do that, need to tap all threads at
                # end, and maybe signal them to stop
            if isinstance(err, CrashGenError): # our own transition failure
                Logging.info("State transition error")
                # TODO: saw an error here once, let's print out stack info for err?
                traceback.print_stack()
                transitionFailed = True
                self._te = None  # Not running any more
                self._execStats.registerFailure("State transition error: {}".format(err))
            else:
                raise
        # return transitionFailed # Why did we have this??!!

        self.resetExecutedTasks()  # clear the tasks after we are done
        # Get ready for next step
        Logging.debug("<-- Step {} finished, trasition failed = {}".format(self._curStep, transitionFailed))
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
            if not Config.getConfig().debug: # print this only if we are not in debug mode    
                Progress.emit(Progress.STEP_BOUNDARY)            
                # print(".", end="", flush=True)
            # if (self._curStep % 2) == 0: # print memory usage once every 10 steps
            #     memUsage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            #     print("[m:{}]".format(memUsage), end="", flush=True) # print memory usage
            # if (self._curStep % 10) == 3: 
            #     h = hpy()
            #     print("\n")        
            #     print(h.heap())
            
                        
            try:
                self._syncAtBarrier() # For now just cross the barrier
                Progress.emit(Progress.END_THREAD_STEP)
                if self._stepStartTime :
                    stepExecTime = time.time() - self._stepStartTime
                    Progress.emitStr('{:.3f}s/{}'.format(stepExecTime, DbConnNative.totalRequests))
                    DbConnNative.resetTotalRequests() # reset to zero
            except threading.BrokenBarrierError as err:
                self._execStats.registerFailure("Aborted due to worker thread timeout")
                Logging.error("\n")
                Logging.error("Main loop aborted, caused by worker thread(s) time-out of {} seconds".format(
                    ThreadCoordinator.WORKER_THREAD_TIMEOUT))
                Logging.error("TAOS related threads blocked at (stack frames top-to-bottom):")
                ts = ThreadStacks()
                ts.print(filterInternal=True)
                workerTimeout = True

                # Enable below for deadlock debugging, using gdb to attach to process
                # while True:
                #     Logging.error("Deadlock detected")
                #     time.sleep(60.0)

                break

            # At this point, all threads should be pass the overall "barrier" and before the per-thread "gate"
            # We use this period to do house keeping work, when all worker
            # threads are QUIET.
            hasAbortedTask = self._hasAbortedTask() # from previous step
            if hasAbortedTask: 
                Logging.info("Aborted task encountered, exiting test program")
                self._execStats.registerFailure("Aborted Task Encountered")
                break # do transition only if tasks are error free

            # Ending previous step
            try:
                transitionFailed = self._doTransition() # To start, we end step -1 first
            except taos.error.ProgrammingError as err:
                transitionFailed = True
                errno2 = Helper.convertErrno(err.errno)  # correct error scheme
                errMsg = "Transition failed: errno=0x{:X}, msg: {}".format(errno2, err)
                Logging.info(errMsg)
                traceback.print_exc()
                self._execStats.registerFailure(errMsg)

            # Then we move on to the next step
            Progress.emit(Progress.BEGIN_THREAD_STEP)
            self._stepStartTime = time.time()
            self._releaseAllWorkerThreads(transitionFailed)                    

        if hasAbortedTask or transitionFailed : # abnormal ending, workers waiting at "gate"
            Logging.debug("Abnormal ending of main thraed")
        elif workerTimeout:
            Logging.debug("Abnormal ending of main thread, due to worker timeout")
        else: # regular ending, workers waiting at "barrier"
            Logging.debug("Regular ending, main thread waiting for all worker threads to stop...")
            self._syncAtBarrier()

        self._te = None  # No more executor, time to end
        Logging.debug("Main thread tapping all threads one last time...")
        self.tapAllThreads()  # Let the threads run one last time

        Logging.debug("\r\n\n--> Main thread ready to finish up...")
        Logging.debug("Main thread joining all threads")
        self._pool.joinAll()  # Get all threads to finish
        Logging.info(". . . All worker threads finished") # No CR/LF before
        self._execStats.endExec()

    def cleanup(self): # free resources
        self._pool.cleanup()

        self._pool = None
        self._te = None  
        self._dbManager = None
        self._executedTasks = []
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
        Logging.debug(
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
        if Config.getConfig().max_dbs == 0:
            self._dbs.append(Database(0, dbc))
        else:            
            baseDbNumber = int(datetime.datetime.now().timestamp( # Don't use Dice/random, as they are deterministic
                )*333) % 888 if Config.getConfig().dynamic_db_table_names else 0
            for i in range(Config.getConfig().max_dbs):
                self._dbs.append(Database(baseDbNumber + i, dbc))

    def pickDatabase(self):
        idxDb = 0
        if Config.getConfig().max_dbs != 0 :
            idxDb = Dice.throw(Config.getConfig().max_dbs) # 0 to N-1
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
        taskType = db.getStateMachine().pickTaskType() # dynamic name of class
        return taskType(self._execStats, db)  # create a task from it

    def resetExecutedTasks(self):
        self._executedTasks = []  # should be under single thread

    def saveExecutedTask(self, task):
        with self._lock:
            self._executedTasks.append(task)

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
            Logging.debug("Joining thread...")
            workerThread._thread.join()

    def cleanup(self):
        self.threadList = [] # maybe clean up each?

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
            # Logging.debug("LQ allocating item {}".format(i))
            if (i in self.inUse):
                raise RuntimeError(
                    "Cannot re-use same index in queue: {}".format(i))
            self.inUse.add(i)

    def release(self, i):
        with self._lock:
            # Logging.debug("LQ releasing item {}".format(i))
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
    def getInfo(self) -> List[Any]:
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
        if Config.getConfig().max_dbs > 0 or Config.getConfig().use_shadow_db : 
            return False
        return self._info[self.CAN_DROP_DB]

    def canCreateFixedSuperTable(self):
        return self._info[self.CAN_CREATE_FIXED_SUPER_TABLE]

    def canDropFixedSuperTable(self):
        if Config.getConfig().use_shadow_db: # duplicate writes to shaddow DB, in which case let's disable dropping s-table
            return False
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
                    raise CrashGenError(
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
            raise CrashGenError("Unexpected zero success for task type: {}, from tasks: {}"
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
                    raise CrashGenError(
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
        try:
            self._curState = self._findCurrentState(dbc)  # starting state
        except taos.error.ProgrammingError as err:            
            Logging.error("Failed to initialized state machine, cannot find current state: {}".format(err))
            traceback.print_stack()
            raise # re-throw

    # TODO: seems no lnoger used, remove?
    def getCurrentState(self):
        return self._curState

    def hasDatabase(self):
        return self._curState.canDropDb()  # ha, can drop DB means it has one

    # May be slow, use cautionsly...
    def getTaskTypes(self):  # those that can run (directly/indirectly) from the current state
        def typesToStrings(types) -> List:
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
        Logging.debug(
            "[OPS] Tasks found for state {}: {}".format(
                self._curState,
                typesToStrings(taskTypes)))
        return taskTypes

    def _findCurrentState(self, dbc: DbConn):
        ts = time.time()  # we use this to debug how fast/slow it is to do the various queries to find the current DB state
        dbName =self._db.getName()
        if not dbc.existsDatabase(dbName): # dbc.hasDatabases():  # no database?!
            Logging.debug( "[STT] empty database found, between {} and {}".format(ts, time.time()))
            return StateEmpty()
        # did not do this when openning connection, and this is NOT the worker
        # thread, which does this on their own
        dbc.use(dbName)
        if not dbc.hasTables():  # no tables
            Logging.debug("[STT] DB_ONLY found, between {} and {}".format(ts, time.time()))
            return StateDbOnly()

        # For sure we have tables, which means we must have the super table. # TODO: are we sure?
        sTable = self._db.getFixedSuperTable()
        if sTable.hasRegTables(dbc):  # no regular tables
            Logging.debug("[STT] SUPER_TABLE_ONLY found, between {} and {}".format(ts, time.time()))
            return StateSuperTableOnly()
        else:  # has actual tables
            Logging.debug("[STT] HAS_DATA found, between {} and {}".format(ts, time.time()))
            return StateHasData()

    # We transition the system to a new state by examining the current state itself
    def transition(self, tasks, dbc: DbConn):
        global gSvcMgr
        
        if (len(tasks) == 0):  # before 1st step, or otherwise empty
            Logging.debug("[STT] Starting State: {}".format(self._curState))
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
        Logging.debug("[STT] New DB state determined: {}".format(newState))
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
        # Logging.debug(" (weighted random:{}/{}) ".format(i, len(taskTypes)))
        return taskTypes[i]

    # ref:
    # https://eli.thegreenplace.net/2010/01/22/weighted-random-generation-in-python/
    def _weighted_choice_sub(self, weights) -> int:
        # TODO: use our dice to ensure it being determinstic?
        rnd = random.random() * sum(weights)
        for i, w in enumerate(weights):
            rnd -= w
            if rnd < 0:
                return i
        raise CrashGenError("Unexpected no choice")

class Database:
    ''' We use this to represent an actual TDengine database inside a service instance,
        possibly in a cluster environment.

        For now we use it to manage state transitions in that database

        TODO: consider moving, but keep in mind it contains "StateMachine"
    '''
    _clsLock = threading.Lock() # class wide lock
    _lastInt = 101  # next one is initial integer
    _lastTick = None # Optional[datetime]
    _lastLaggingTick = None # Optional[datetime] # lagging tick, for out-of-sequence (oos) data insertions

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

    def getFixedSuperTable(self) -> TdSuperTable:
        return TdSuperTable(self.getFixedSuperTableName(), self.getName())

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
        Logging.debug("Setting up TICKS to start from: {}".format(t4))
        return t4

    @classmethod
    def getNextTick(cls):       
        '''
            Fetch a timestamp tick, with some random factor, may not be unique.
        ''' 
        with cls._clsLock:  # prevent duplicate tick
            if cls._lastLaggingTick is None or cls._lastTick is None : # not initialized
                # 10k at 1/20 chance, should be enough to avoid overlaps
                tick = cls.setupLastTick()
                cls._lastTick = tick
                cls._lastLaggingTick = tick + datetime.timedelta(0, -60*2)  # lagging behind 2 minutes, should catch up fast
                # if : # should be quite a bit into the future

            if Config.isSet('mix_oos_data') and Dice.throw(20) == 0:  # if asked to do so, and 1 in 20 chance, return lagging tick
                cls._lastLaggingTick += datetime.timedelta(0, 1) # pick the next sequence from the lagging tick sequence
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

    ALL_COLORS = ['red', 'white', 'blue', 'green', 'purple']

    def getNextColor(self):
        return random.choice(self.ALL_COLORS)


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
    #     Logging.info("    T[{}.x]: ".format(self._curStep) + msg)

    # def logDebug(self, msg):
    #     Logging.debug("    T[{}.x]: ".format(self._curStep) + msg)


class Task():
    ''' A generic "Task" to be executed. For now we decide that there is no
        need to embed a DB connection here, we use whatever the Worker Thread has
        instead. But a task is always associated with a DB
    '''
    taskSn = 100
    _lock = threading.Lock()
    _tableLocks: Dict[str, threading.Lock] = {}

    @classmethod
    def allocTaskNum(cls):
        Task.taskSn += 1  # IMPORTANT: cannot use cls.taskSn, since each sub class will have a copy
        # Logging.debug("Allocating taskSN: {}".format(Task.taskSn))
        return Task.taskSn

    def __init__(self, execStats: ExecutionStats, db: Database):
        self._workerThread = None
        self._err: Optional[Exception] = None
        self._aborted = False
        self._curStep = None
        self._numRows = None  # Number of rows affected

        # Assign an incremental task serial number
        self._taskNum = self.allocTaskNum()
        # Logging.debug("Creating new task {}...".format(self._taskNum))

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

    def _isServiceStable(self):
        if not gSvcMgr:
            return True  # we don't run service, so let's assume it's stable
        return gSvcMgr.isStable() # otherwise let's examine the service

    def _isErrAcceptable(self, errno, msg):
        if errno in [
                0x05,  # TSDB_CODE_RPC_NOT_READY
                0x0B,  # Unable to establish connection, more details in TD-1648
                # 0x200, # invalid SQL， TODO: re-examine with TD-934
                0x20F, # query terminated, possibly due to vnoding being dropped, see TD-1776
                0x213, # "Disconnected from service", result of "kill connection ???"
                0x217, # "db not selected", client side defined error code
                # 0x218, # "Table does not exist" client side defined error code
                0x360, # Table already exists
                0x362, 
                # 0x369, # tag already exists
                0x36A, 0x36B, 0x36D,
                0x381, 
                0x380, # "db not selected"
                0x383,
                0x386,  # DB is being dropped?!
                0x503,
                0x510,  # vnode not in ready state
                0x14,   # db not ready, errno changed
                0x600,  # Invalid table ID, why?
                0x218,  # Table does not exist
                1000  # REST catch-all error
            ]: 
            return True # These are the ALWAYS-ACCEPTABLE ones
        # This case handled below already.
        # elif (errno in [ 0x0B ]) and Settings.getConfig().auto_start_service:
        #     return True # We may get "network unavilable" when restarting service
        elif Config.getConfig().ignore_errors: # something is specified on command line
            moreErrnos = [int(v, 0) for v in Config.getConfig().ignore_errors.split(',')]
            if errno in moreErrnos:
                return True
        elif errno == 0x200 : # invalid SQL, we need to div in a bit more
            if msg.find("invalid column name") != -1:
                return True 
            elif msg.find("tags number not matched") != -1: # mismatched tags after modification
                return True
            elif msg.find("duplicated column names") != -1: # also alter table tag issues
                return True
        elif not self._isServiceStable(): # We are managing service, and ...
            Logging.info("Ignoring error when service starting/stopping: errno = {}, msg = {}".format(errno, msg))
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
            if (Config.getConfig().continue_on_exception):  # user choose to continue
                self.logDebug("[=] Continue after TAOS exception: errno=0x{:X}, msg: {}, SQL: {}".format(
                        errno2, err, wt.getDbConn().getLastSql()))
                self._err = err
            elif self._isErrAcceptable(errno2, err.__str__()):
                self.logDebug("[=] Acceptable Taos library exception: errno=0x{:X}, msg: {}, SQL: {}".format(
                        errno2, err, wt.getDbConn().getLastSql()))
                # print("_", end="", flush=True)
                Progress.emit(Progress.ACCEPTABLE_ERROR)
                self._err = err
            else: # not an acceptable error
                shortTid = threading.get_ident() % 10000
                errMsg = "[=] Unexpected Taos library exception ({}): errno=0x{:X}, thread={}, msg: {}, SQL: {}".format(
                    self.__class__.__name__,
                    errno2, 
                    shortTid,
                    err, wt.getDbConn().getLastSql())
                self.logDebug(errMsg)
                if Config.getConfig().debug:
                    # raise # so that we see full stack
                    traceback.print_exc()
                print(
                    "\n\n----------------------------\nProgram ABORTED Due to Unexpected TAOS Error: \n\n{}\n".format(errMsg) +
                    "----------------------------\n")
                # sys.exit(-1)
                self._err = err
                self._aborted = True
        except Exception as e:
            Logging.info("Non-TAOS exception encountered with: {}".format(self.__class__.__name__))
            self._err = e
            self._aborted = True
            traceback.print_exc()
        except BaseException as e2:
            self.logInfo("Python base exception encountered")
            # self._err = e2 # Exception/BaseException incompatible!
            self._aborted = True
            traceback.print_exc()
        # except BaseException: # TODO: what is this again??!!
        #     raise RuntimeError("Punt")
            # self.logDebug(
            #     "[=] Unexpected exception, SQL: {}".format(
            #         wt.getDbConn().getLastSql()))
            # raise
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

    def lockTable(self, ftName): # full table name
        # print(" <<" + ftName + '_', end="", flush=True)
        with Task._lock: # SHORT lock! so we only protect lock creation
            if not ftName in Task._tableLocks: # Create new lock and add to list, if needed
                Task._tableLocks[ftName] = threading.Lock()
        
        # No lock protection, anybody can do this any time
        lock = Task._tableLocks[ftName]
        # Logging.info("Acquiring lock: {}, {}".format(ftName, lock))
        lock.acquire()
        # Logging.info("Acquiring lock successful: {}".format(lock))

    def unlockTable(self, ftName):
        # print('_' + ftName + ">> ", end="", flush=True)
        with Task._lock: 
            if not ftName in self._tableLocks:
                raise RuntimeError("Corrupt state, no such lock")
            lock = Task._tableLocks[ftName]
            if not lock.locked():
                raise RuntimeError("Corrupte state, already unlocked")

            # Important note, we want to protect unlocking under the task level
            # locking, because we don't want the lock to be deleted (maybe in the futur)
            # while we unlock it
            # Logging.info("Releasing lock: {}".format(lock))
            lock.release()
            # Logging.info("Releasing lock successful: {}".format(lock))


class ExecutionStats:
    def __init__(self):
        # total/success times for a task
        self._execTimes: Dict[str, List[int]] = {}
        self._tasksInProgress = 0
        self._lock = threading.Lock()
        self._firstTaskStartTime = 0.0
        self._execStartTime = 0.0
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
                self._firstTaskStartTime = 0.0

    def registerFailure(self, reason):
        self._failed = True
        self._failureReason = reason

    def printStats(self):
        Logging.info(
            "----------------------------------------------------------------------")
        Logging.info(
            "| Crash_Gen test {}, with the following stats:". format(
                "FAILED (reason: {})".format(
                    self._failureReason) if self._failed else "SUCCEEDED"))
        Logging.info("| Task Execution Times (success/total):")
        execTimesAny = 0.0
        for k, n in self._execTimes.items():
            execTimesAny += n[0]
            errStr = None
            if k in self._errors:
                errors = self._errors[k]
                # print("errors = {}".format(errors))
                errStrs = ["0x{:X}:{}".format(eno, n) for (eno, n) in errors.items()]
                # print("error strings = {}".format(errStrs))
                errStr = ", ".join(errStrs) 
            Logging.info("|    {0:<24}: {1}/{2} (Errors: {3})".format(k, n[1], n[0], errStr))

        Logging.info(
            "| Total Tasks Executed (success or not): {} ".format(execTimesAny))
        Logging.info(
            "| Total Tasks In Progress at End: {}".format(
                self._tasksInProgress))
        Logging.info(
            "| Total Task Busy Time (elapsed time when any task is in progress): {:.3f} seconds".format(
                self._accRunTime))
        Logging.info(
            "| Average Per-Task Execution Time: {:.3f} seconds".format(self._accRunTime / execTimesAny))
        Logging.info(
            "| Total Elapsed Time (from wall clock): {:.3f} seconds".format(
                self._elapsedTime))
        Logging.info("| Top numbers written: {}".format(TaskExecutor.getBoundedList()))
        Logging.info("| Active DB Native Connections (now): {}".format(DbConnNative.totalConnections))
        Logging.info("| Longest native query time: {:.3f} seconds, started: {}".
            format(MyTDSql.longestQueryTime, 
                time.strftime("%x %X", time.localtime(MyTDSql.lqStartTime))) )
        Logging.info("| Longest native query: {}".format(MyTDSql.longestQuery))
        Logging.info(
            "----------------------------------------------------------------------")


class StateTransitionTask(Task):
    LARGE_NUMBER_OF_TABLES = 35
    SMALL_NUMBER_OF_TABLES = 3
    LARGE_NUMBER_OF_RECORDS = 50
    SMALL_NUMBER_OF_RECORDS = 3

    _baseTableNumber = None

    _endState = None # TODO: no longter used?

    @classmethod
    def getInfo(cls):  # each sub class should supply their own information
        raise RuntimeError("Overriding method expected")
    
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
        if ( StateTransitionTask._baseTableNumber is None): # Set it one time
            StateTransitionTask._baseTableNumber = Dice.throw(
                999) if Config.getConfig().dynamic_db_table_names else 0
        return "reg_table_{}".format(StateTransitionTask._baseTableNumber + i)

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
        if Config.getConfig().num_replicas != 1:
            # numReplica = Dice.throw(Settings.getConfig().max_replicas) + 1 # 1,2 ... N
            numReplica = Config.getConfig().num_replicas # fixed, always
            repStr = "replica {}".format(numReplica)
        updatePostfix = "update 1" if Config.getConfig().verify_data else "" # allow update only when "verify data" is active
        dbName = self._db.getName()
        self.execWtSql(wt, "create database {} {} {} ".format(dbName, repStr, updatePostfix ) )
        if dbName == "db_0" and Config.getConfig().use_shadow_db:
            self.execWtSql(wt, "create database {} {} {} ".format("db_s", repStr, updatePostfix ) )

class TaskDropDb(StateTransitionTask):
    @classmethod
    def getEndState(cls):
        return StateEmpty()

    @classmethod
    def canBeginFrom(cls, state: AnyState):
        return state.canDropDb()

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        self.execWtSql(wt, "drop database {}".format(self._db.getName()))
        Logging.debug("[OPS] database dropped at {}".format(time.time()))

class TaskCreateSuperTable(StateTransitionTask):
    @classmethod
    def getEndState(cls):
        return StateSuperTableOnly()

    @classmethod
    def canBeginFrom(cls, state: AnyState):
        return state.canCreateFixedSuperTable()

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        if not self._db.exists(wt.getDbConn()):
            Logging.debug("Skipping task, no DB yet")
            return

        sTable = self._db.getFixedSuperTable() # type: TdSuperTable
        # wt.execSql("use db")    # should always be in place

        sTable.create(wt.getDbConn(),
                      {'ts': TdDataType.TIMESTAMP, 'speed': TdDataType.INT, 'color': TdDataType.BINARY16}, {
                          'b': TdDataType.BINARY200, 'f': TdDataType.FLOAT},
                      dropIfExists=True
                      )
        # self.execWtSql(wt,"create table db.{} (ts timestamp, speed int) tags (b binary(200), f float) ".format(tblName))
        # No need to create the regular tables, INSERT will do that
        # automatically


class TdSuperTable:
    def __init__(self, stName, dbName):
        self._stName = stName
        self._dbName = dbName

    def getName(self):
        return self._stName

    def drop(self, dbc, skipCheck = False):
        dbName = self._dbName
        if self.exists(dbc) : # if myself exists
            fullTableName = dbName + '.' + self._stName                
            dbc.execute("DROP TABLE {}".format(fullTableName))
        else:
            if not skipCheck:
                raise CrashGenError("Cannot drop non-existant super table: {}".format(self._stName))

    def exists(self, dbc):
        dbc.execute("USE " + self._dbName)
        return dbc.existsSuperTable(self._stName)

    # TODO: odd semantic, create() method is usually static?
    def create(self, dbc, cols: TdColumns, tags: TdTags, dropIfExists = False):
        '''Creating a super table'''

        dbName = self._dbName
        dbc.execute("USE " + dbName)
        fullTableName = dbName + '.' + self._stName       
        if dbc.existsSuperTable(self._stName):
            if dropIfExists: 
                dbc.execute("DROP TABLE {}".format(fullTableName))
            else: # error
                raise CrashGenError("Cannot create super table, already exists: {}".format(self._stName))

        # Now let's create
        sql = "CREATE TABLE {} ({})".format(
            fullTableName,
            ",".join(['%s %s'%(k,v.value) for (k,v) in cols.items()]))
        if tags :
            sql += " TAGS ({})".format(
                ",".join(['%s %s'%(k,v.value) for (k,v) in tags.items()])
            )            
        else:
            sql += " TAGS (dummy int) "
        dbc.execute(sql)        

    def getRegTables(self, dbc: DbConn):
        dbName = self._dbName
        try:
            dbc.query("select TBNAME from {}.{}".format(dbName, self._stName))  # TODO: analyze result set later            
        except taos.error.ProgrammingError as err:                    
            errno2 = Helper.convertErrno(err.errno) 
            Logging.debug("[=] Failed to get tables from super table: errno=0x{:X}, msg: {}".format(errno2, err))
            raise

        qr = dbc.getQueryResult()
        return [v[0] for v in qr] # list transformation, ref: https://stackoverflow.com/questions/643823/python-list-transformation

    def hasRegTables(self, dbc: DbConn):
        return dbc.query("SELECT * FROM {}.{}".format(self._dbName, self._stName)) > 0

    def ensureRegTable(self, task: Optional[Task], dbc: DbConn, regTableName: str):
        '''
        Make sure a regular table exists for this super table, creating it if necessary.
        If there is an associated "Task" that wants to do this, "lock" this table so that
        others don't access it while we create it.
        '''
        dbName = self._dbName
        sql = "select tbname from {}.{} where tbname in ('{}')".format(dbName, self._stName, regTableName)
        if dbc.query(sql) >= 1 : # reg table exists already
            return

        # acquire a lock first, so as to be able to *verify*. More details in TD-1471
        fullTableName = dbName + '.' + regTableName      
        if task is not None:  # Somethime thie operation is requested on behalf of a "task"
            # Logging.info("Locking table for creation: {}".format(fullTableName))
            task.lockTable(fullTableName) # in which case we'll lock this table to ensure serialized access
            # Logging.info("Table locked for creation".format(fullTableName))
        Progress.emit(Progress.CREATE_TABLE_ATTEMPT) # ATTEMPT to create a new table
        # print("(" + fullTableName[-3:] + ")", end="", flush=True)  
        try:
            sql = "CREATE TABLE {} USING {}.{} tags ({})".format(
                fullTableName, dbName, self._stName, self._getTagStrForSql(dbc)
            )
            # Logging.info("Creating regular with SQL: {}".format(sql))            
            dbc.execute(sql)
            # Logging.info("Regular table created: {}".format(sql))
        finally:
            if task is not None:
                # Logging.info("Unlocking table after creation: {}".format(fullTableName))
                task.unlockTable(fullTableName) # no matter what
                # Logging.info("Table unlocked after creation: {}".format(fullTableName))

    def _getTagStrForSql(self, dbc) :
        tags = self._getTags(dbc)
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

    def _getTags(self, dbc) -> dict:
        dbc.query("DESCRIBE {}.{}".format(self._dbName, self._stName))
        stCols = dbc.getQueryResult()
        # print(stCols)
        ret = {row[0]:row[1] for row in stCols if row[3]=='TAG'} # name:type
        # print("Tags retrieved: {}".format(ret))
        return ret

    def addTag(self, dbc, tagName, tagType):
        if tagName in self._getTags(dbc): # already 
            return
        # sTable.addTag("extraTag", "int")
        sql = "alter table {}.{} add tag {} {}".format(
            self._dbName, self._stName, tagName, tagType)
        dbc.execute(sql)

    def dropTag(self, dbc, tagName):
        if not tagName in self._getTags(dbc): # don't have this tag
            return
        sql = "alter table {}.{} drop tag {}".format(self._dbName, self._stName, tagName)
        dbc.execute(sql)

    def changeTag(self, dbc, oldTag, newTag):
        tags = self._getTags(dbc)
        if not oldTag in tags: # don't have this tag
            return
        if newTag in tags: # already have this tag
            return
        sql = "alter table {}.{} change tag {} {}".format(self._dbName, self._stName, oldTag, newTag)
        dbc.execute(sql)

    def generateQueries(self, dbc: DbConn) -> List[SqlQuery]:
        ''' Generate queries to test/exercise this super table '''
        ret = [] # type: List[SqlQuery]

        for rTbName in self.getRegTables(dbc):  # regular tables
            
            filterExpr = Dice.choice([ # TODO: add various kind of WHERE conditions
                None
            ])

            # Run the query against the regular table first
            doAggr = (Dice.throw(2) == 0) # 1 in 2 chance
            if not doAggr: # don't do aggregate query, just simple one
                ret.append(SqlQuery( # reg table
                    "select {} from {}.{}".format('*', self._dbName, rTbName)))
                ret.append(SqlQuery( # super table
                    "select {} from {}.{}".format('*', self._dbName, self.getName())))
            else: # Aggregate query
                aggExpr = Dice.choice([                
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
                    # 'last_row(speed)', # TODO: commented out per TD-3231, we should re-create
                    # Transformation Functions
                    # 'diff(speed)', # TODO: no supported?!
                    'spread(speed)'
                    ]) # TODO: add more from 'top'

            
                # if aggExpr not in ['stddev(speed)']: # STDDEV not valid for super tables?! (Done in TD-1049)
                sql = "select {} from {}.{}".format(aggExpr, self._dbName, self.getName())
                if Dice.throw(3) == 0: # 1 in X chance
                    sql = sql + ' GROUP BY color'
                    Progress.emit(Progress.QUERY_GROUP_BY)
                    # Logging.info("Executing GROUP-BY query: " + sql)
                ret.append(SqlQuery(sql))

        return ret        

class TaskReadData(StateTransitionTask):
    @classmethod
    def getEndState(cls):
        return None  # meaning doesn't affect state

    @classmethod
    def canBeginFrom(cls, state: AnyState):
        return state.canReadData()

    # def _canRestartService(self):
    #     if not gSvcMgr:
    #         return True # always
    #     return gSvcMgr.isActive() # only if it's running TODO: race condition here

    def _reconnectIfNeeded(self, wt):
        # 1 in 20 chance, simulate a broken connection, only if service stable (not restarting)
        if random.randrange(20)==0: # and self._canRestartService():  # TODO: break connection in all situations
            # Logging.info("Attempting to reconnect to server") # TODO: change to DEBUG
            Progress.emit(Progress.SERVICE_RECONNECT_START) 
            try:
                wt.getDbConn().close()
                wt.getDbConn().open()
            except ConnectionError as err: # may fail
                if not gSvcMgr:
                    Logging.error("Failed to reconnect in client-only mode")
                    raise # Not OK if we are running in client-only mode
                if gSvcMgr.isRunning(): # may have race conditon, but low prob, due to 
                    Logging.error("Failed to reconnect when managed server is running")
                    raise # Not OK if we are running normally

                Progress.emit(Progress.SERVICE_RECONNECT_FAILURE) 
                # Logging.info("Ignoring DB reconnect error")

            # print("_r", end="", flush=True)
            Progress.emit(Progress.SERVICE_RECONNECT_SUCCESS) 
            # The above might have taken a lot of time, service might be running
            # by now, causing error below to be incorrectly handled due to timing issue
            return # TODO: fix server restart status race condtion


    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        self._reconnectIfNeeded(wt)

        dbc = wt.getDbConn()
        sTable = self._db.getFixedSuperTable()
        
        for q in sTable.generateQueries(dbc):  # regular tables            
            try:
                sql = q.getSql()
                # if 'GROUP BY' in sql:
                #     Logging.info("Executing GROUP-BY query: " + sql)
                dbc.execute(sql)
            except taos.error.ProgrammingError as err:                    
                errno2 = Helper.convertErrno(err.errno)
                Logging.debug("[=] Read Failure: errno=0x{:X}, msg: {}, SQL: {}".format(errno2, err, dbc.getLastSql()))
                raise

class SqlQuery:
    @classmethod
    def buildRandom(cls, db: Database):
        '''Build a random query against a certain database'''
        
        dbName = db.getName()

    def __init__(self, sql:str = None):
        self._sql = sql

    def getSql(self):
        return self._sql
    
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
                2 + (self.LARGE_NUMBER_OF_TABLES if Config.getConfig().larger_data else self.SMALL_NUMBER_OF_TABLES)))
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
                        Logging.debug("[DB] Acceptable error when dropping a table")
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
        dice = Dice.throw(4)
        if dice == 0:
            sTable.addTag(dbc, "extraTag", "int")
            # sql = "alter table db.{} add tag extraTag int".format(tblName)
        elif dice == 1:
            sTable.dropTag(dbc, "extraTag")
            # sql = "alter table db.{} drop tag extraTag".format(tblName)
        elif dice == 2:
            sTable.dropTag(dbc, "newTag")
            # sql = "alter table db.{} drop tag newTag".format(tblName)
        else:  # dice == 3
            sTable.changeTag(dbc, "extraTag", "newTag")
            # sql = "alter table db.{} change tag extraTag newTag".format(tblName)

class TaskRestartService(StateTransitionTask):
    _isRunning = False
    _classLock = threading.Lock()

    @classmethod
    def getEndState(cls):
        return None  # meaning doesn't affect state

    @classmethod
    def canBeginFrom(cls, state: AnyState):
        if Config.getConfig().auto_start_service:
            return state.canDropFixedSuperTable()  # Basicallly when we have the super table
        return False # don't run this otherwise

    CHANCE_TO_RESTART_SERVICE = 200
    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        if not Config.getConfig().auto_start_service: # only execute when we are in -a mode
            print("_a", end="", flush=True)
            return

        with self._classLock:
            if self._isRunning:
                Logging.info("Skipping restart task, another running already")
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

    # We use these two files to record operations to DB, useful for power-off tests
    fAddLogReady = None # type: Optional[io.TextIOWrapper]
    fAddLogDone  = None # type: Optional[io.TextIOWrapper]

    @classmethod
    def prepToRecordOps(cls):
        if Config.getConfig().record_ops:
            if (cls.fAddLogReady is None):
                Logging.info(
                    "Recording in a file operations to be performed...")
                cls.fAddLogReady = open("add_log_ready.txt", "w")
            if (cls.fAddLogDone is None):
                Logging.info("Recording in a file operations completed...")
                cls.fAddLogDone = open("add_log_done.txt", "w")

    @classmethod
    def getEndState(cls):
        return StateHasData()

    @classmethod
    def canBeginFrom(cls, state: AnyState):
        return state.canAddData()

    def _lockTableIfNeeded(self, fullTableName, extraMsg = ''):
        if Config.getConfig().verify_data:
            # Logging.info("Locking table: {}".format(fullTableName))
            self.lockTable(fullTableName) 
            # Logging.info("Table locked {}: {}".format(extraMsg, fullTableName))
                # print("_w" + str(nextInt % 100), end="", flush=True) # Trace what was written
        else:
            # Logging.info("Skipping locking table")
            pass

    def _unlockTableIfNeeded(self, fullTableName):
        if Config.getConfig().verify_data:
            # Logging.info("Unlocking table: {}".format(fullTableName))
            self.unlockTable(fullTableName) 
            # Logging.info("Table unlocked: {}".format(fullTableName))
        else:
            pass
            # Logging.info("Skipping unlocking table")

    def _addDataInBatch(self, db, dbc, regTableName, te: TaskExecutor): 
        numRecords = self.LARGE_NUMBER_OF_RECORDS if Config.getConfig().larger_data else self.SMALL_NUMBER_OF_RECORDS        
        
        fullTableName = db.getName() + '.' + regTableName
        self._lockTableIfNeeded(fullTableName, 'batch')

        sql = "INSERT INTO {} VALUES ".format(fullTableName)
        for j in range(numRecords):  # number of records per table
            nextInt = db.getNextInt()
            nextTick = db.getNextTick()
            nextColor = db.getNextColor()
            sql += "('{}', {}, '{}');".format(nextTick, nextInt, nextColor)

        # Logging.info("Adding data in batch: {}".format(sql))
        try:
            dbc.execute(sql)
        finally:
            # Logging.info("Data added in batch: {}".format(sql))
            self._unlockTableIfNeeded(fullTableName)

        

    def _addData(self, db: Database, dbc, regTableName, te: TaskExecutor): # implied: NOT in batches
        numRecords = self.LARGE_NUMBER_OF_RECORDS if Config.getConfig().larger_data else self.SMALL_NUMBER_OF_RECORDS        

        for j in range(numRecords):  # number of records per table
            intToWrite = db.getNextInt()
            nextTick = db.getNextTick()
            nextColor = db.getNextColor()
            if Config.getConfig().record_ops:
                self.prepToRecordOps()
                if self.fAddLogReady is None:
                    raise CrashGenError("Unexpected empty fAddLogReady")
                self.fAddLogReady.write("Ready to write {} to {}\n".format(intToWrite, regTableName))
                self.fAddLogReady.flush()
                os.fsync(self.fAddLogReady.fileno())
                
            # TODO: too ugly trying to lock the table reliably, refactor...
            fullTableName = db.getName() + '.' + regTableName
            self._lockTableIfNeeded(fullTableName) # so that we are verify read-back. TODO: deal with exceptions before unlock
            
            try:
                sql = "INSERT INTO {} VALUES ('{}', {}, '{}');".format( # removed: tags ('{}', {})
                    fullTableName,
                    # ds.getFixedSuperTableName(),
                    # ds.getNextBinary(), ds.getNextFloat(),
                    nextTick, intToWrite, nextColor)
                # Logging.info("Adding data: {}".format(sql))
                dbc.execute(sql)
                # Logging.info("Data added: {}".format(sql))
                intWrote = intToWrite

                # Quick hack, attach an update statement here. TODO: create an "update" task
                if (not Config.getConfig().use_shadow_db) and Dice.throw(5) == 0: # 1 in N chance, plus not using shaddow DB
                    intToUpdate = db.getNextInt() # Updated， but should not succeed
                    nextColor = db.getNextColor()
                    sql = "INSERt INTO {} VALUES ('{}', {}, '{}');".format( # "INSERt" means "update" here
                    fullTableName,
                    nextTick, intToUpdate, nextColor)
                    # sql = "UPDATE {} set speed={}, color='{}' WHERE ts='{}'".format(
                    #     fullTableName, db.getNextInt(), db.getNextColor(), nextTick)
                    dbc.execute(sql)
                    intWrote = intToUpdate # We updated, seems TDengine non-cluster accepts this.

            except: # Any exception at all
                self._unlockTableIfNeeded(fullTableName) 
                raise

            # Now read it back and verify, we might encounter an error if table is dropped
            if Config.getConfig().verify_data: # only if command line asks for it
                try:
                    readBack = dbc.queryScalar("SELECT speed from {}.{} WHERE ts='{}'".
                        format(db.getName(), regTableName, nextTick))
                    if readBack != intWrote :
                        raise taos.error.ProgrammingError(
                            "Failed to read back same data, wrote: {}, read: {}"
                            .format(intWrote, readBack), 0x999)
                except taos.error.ProgrammingError as err:
                    errno = Helper.convertErrno(err.errno)
                    if errno == CrashGenError.INVALID_EMPTY_RESULT: # empty result
                        raise taos.error.ProgrammingError(
                            "Failed to read back same data for tick: {}, wrote: {}, read: EMPTY"
                            .format(nextTick, intWrote),
                            errno)
                    elif errno == CrashGenError.INVALID_MULTIPLE_RESULT : # multiple results
                        raise taos.error.ProgrammingError(
                            "Failed to read back same data for tick: {}, wrote: {}, read: MULTIPLE RESULTS"
                            .format(nextTick, intWrote),
                            errno)
                    elif errno in [0x218, 0x362]: # table doesn't exist
                        # do nothing
                        pass
                    else:
                        # Re-throw otherwise
                        raise
                finally:
                    self._unlockTableIfNeeded(fullTableName) # Quite ugly, refactor lock/unlock
            # Done with read-back verification, unlock the table now
            else:
                self._unlockTableIfNeeded(fullTableName) 

            # Successfully wrote the data into the DB, let's record it somehow
            te.recordDataMark(intWrote)

            if Config.getConfig().record_ops:
                if self.fAddLogDone is None:
                    raise CrashGenError("Unexpected empty fAddLogDone")
                self.fAddLogDone.write("Wrote {} to {}\n".format(intWrote, regTableName))
                self.fAddLogDone.flush()
                os.fsync(self.fAddLogDone.fileno())

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        # ds = self._dbManager # Quite DANGEROUS here, may result in multi-thread client access
        db = self._db
        dbc = wt.getDbConn()
        numTables  = self.LARGE_NUMBER_OF_TABLES  if Config.getConfig().larger_data else self.SMALL_NUMBER_OF_TABLES
        numRecords = self.LARGE_NUMBER_OF_RECORDS if Config.getConfig().larger_data else self.SMALL_NUMBER_OF_RECORDS
        tblSeq = list(range(numTables ))
        random.shuffle(tblSeq) # now we have random sequence
        for i in tblSeq:
            if (i in self.activeTable):  # wow already active
                # print("x", end="", flush=True) # concurrent insertion
                Progress.emit(Progress.CONCURRENT_INSERTION)
            else:
                self.activeTable.add(i)  # marking it active
            
            dbName = db.getName()
            sTable = db.getFixedSuperTable()
            regTableName = self.getRegTableName(i)  # "db.reg_table_{}".format(i)            
            fullTableName = dbName + '.' + regTableName
            # self._lockTable(fullTableName) # "create table" below. Stop it if the table is "locked"
            sTable.ensureRegTable(self, wt.getDbConn(), regTableName)  # Ensure the table exists           
            # self._unlockTable(fullTableName)
           
            if Dice.throw(1) == 0: # 1 in 2 chance
                self._addData(db, dbc, regTableName, te)
            else:
                self._addDataInBatch(db, dbc, regTableName, te)

            self.activeTable.discard(i)  # not raising an error, unlike remove


class ThreadStacks: # stack info for all threads
    def __init__(self):
        self._allStacks = {}
        allFrames = sys._current_frames() # All current stack frames
        for th in threading.enumerate():  # For each thread
            if th.ident is None:
                continue          
            stack = traceback.extract_stack(allFrames[th.ident]) # Get stack for a thread
            shortTid = th.ident % 10000
            self._allStacks[shortTid] = stack # Was using th.native_id

    def print(self, filteredEndName = None, filterInternal = False):
        for tIdent, stack in self._allStacks.items(): # for each thread, stack frames top to bottom
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
            print("\n<----- Thread Info for LWP/ID: {} (most recent call last) <-----".format(tIdent))
            stackFrame = 0
            for frame in stack: # was using: reversed(stack)
                # print(frame)
                print("[{sf}] File {filename}, line {lineno}, in {name}".format(
                    sf=stackFrame, filename=frame.filename, lineno=frame.lineno, name=frame.name))
                print("    {}".format(frame.line))
                stackFrame += 1
            print("-----> End of Thread Info ----->\n")

class ClientManager:
    def __init__(self):
        Logging.info("Starting service manager")
        # signal.signal(signal.SIGTERM, self.sigIntHandler)
        # signal.signal(signal.SIGINT, self.sigIntHandler)

        self._status = Status.STATUS_RUNNING
        self.tc = None

        self.inSigHandler = False

    def sigIntHandler(self, signalNumber, frame):
        if self._status != Status.STATUS_RUNNING:
            print("Repeated SIGINT received, forced exit...")
            # return  # do nothing if it's already not running
            sys.exit(-1)
        self._status = Status.STATUS_STOPPING  # immediately set our status

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
        # global gConfig

        # Prepare Tde Instance
        global gContainer
        tInst = gContainer.defTdeInstance = TdeInstance() # "subdir to hold the instance"

        cfg = Config.getConfig()
        dbManager = DbManager(cfg.connector_type, tInst.getDbTarget())  # Regular function
        thPool = ThreadPool(cfg.num_threads, cfg.max_steps)
        self.tc = ThreadCoordinator(thPool, dbManager)
        
        Logging.info("Starting client instance: {}".format(tInst))
        self.tc.run()
        # print("exec stats: {}".format(self.tc.getExecStats()))
        # print("TC failed = {}".format(self.tc.isFailed()))
        if svcMgr: # gConfig.auto_start_service:
            svcMgr.stopTaosServices()
            svcMgr = None
        

        # Release global variables
        # gConfig = None
        Config.clearConfig()
        gSvcMgr = None
        logger = None
        
        thPool = None
        dbManager.cleanUp() # destructor wouldn't run in time
        dbManager = None

        # Print exec status, etc., AFTER showing messages from the server
        self.conclude()
        # print("TC failed (2) = {}".format(self.tc.isFailed()))
        # Linux return code: ref https://shapeshed.com/unix-exit-codes/
        ret = 1 if self.tc.isFailed() else 0
        self.tc.cleanup()
        # Release variables here
        self.tc = None

        gc.collect() # force garbage collection
        # h = hpy()
        # print("\n----- Final Python Heap -----\n")        
        # print(h.heap())

        return ret

    def conclude(self):
        # self.tc.getDbManager().cleanUp() # clean up first, so we can show ZERO db connections
        self.tc.printStats()

class MainExec:
    def __init__(self):        
        self._clientMgr = None
        self._svcMgr = None # type: Optional[ServiceManager]

        signal.signal(signal.SIGTERM, self.sigIntHandler)
        signal.signal(signal.SIGINT,  self.sigIntHandler)
        signal.signal(signal.SIGUSR1, self.sigUsrHandler)  # different handler!

    def sigUsrHandler(self, signalNumber, frame):
        if self._clientMgr:
            self._clientMgr.sigUsrHandler(signalNumber, frame)
        elif self._svcMgr: # Only if no client mgr, we are running alone
            self._svcMgr.sigUsrHandler(signalNumber, frame)
        
    def sigIntHandler(self, signalNumber, frame):
        if  self._svcMgr:
            self._svcMgr.sigIntHandler(signalNumber, frame)
        if  self._clientMgr:
            self._clientMgr.sigIntHandler(signalNumber, frame)

    def runClient(self):
        global gSvcMgr
        if Config.getConfig().auto_start_service:
            gSvcMgr = self._svcMgr = ServiceManager(1) # hack alert
            gSvcMgr.startTaosServices() # we start, don't run
        
        self._clientMgr = ClientManager()
        ret = None
        try: 
            ret = self._clientMgr.run(self._svcMgr) # stop TAOS service inside
        except requests.exceptions.ConnectionError as err:
            Logging.warning("Failed to open REST connection to DB: {}".format(err))
            # don't raise
        return ret

    def runService(self):
        global gSvcMgr
        gSvcMgr = self._svcMgr = ServiceManager(Config.getConfig().num_dnodes) # save it in a global variable TODO: hack alert

        gSvcMgr.run() # run to some end state
        gSvcMgr = self._svcMgr = None 

    def _buildCmdLineParser(self):
        parser = argparse.ArgumentParser(
            formatter_class=argparse.RawDescriptionHelpFormatter,
            description=textwrap.dedent('''\
                TDengine Auto Crash Generator (PLEASE NOTICE the Prerequisites Below)
                ---------------------------------------------------------------------
                1. You build TDengine in the top level ./build directory, as described in offical docs
                2. You run the server there before this script: ./build/bin/taosd -c test/cfg

                '''))                      

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
            help='Number of DBs to use, set to disable dropping DB. (default: 0)')
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
            '-g',
            '--ignore-errors',
            action='store',
            default=None,
            type=str,
            help='Ignore error codes, comma separated, 0x supported (default: None)')
        parser.add_argument(
            '-i',
            '--num-replicas',
            action='store',
            default=1,
            type=int,
            help='Number (fixed) of replicas to use, when testing against clusters. (default: 1)')
        parser.add_argument(
            '-k',
            '--track-memory-leaks',
            action='store_true',
            help='Use Valgrind tool to track memory leaks (default: false)')
        parser.add_argument(
            '-l',
            '--larger-data',
            action='store_true',
            help='Write larger amount of data during write operations (default: false)')
        parser.add_argument(
            '-m',
            '--mix-oos-data',
            action='store_false',
            help='Mix out-of-sequence data into the test data stream (default: true)')
        parser.add_argument(
            '-n',
            '--dynamic-db-table-names',
            action='store_true',
            help='Use non-fixed names for dbs/tables, for -b, useful for multi-instance executions (default: false)')        
        parser.add_argument(
            '-o',
            '--num-dnodes',
            action='store',
            default=1,
            type=int,
            help='Number of Dnodes to initialize, used with -e option. (default: 1)')
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
            '-w',
            '--use-shadow-db',
            action='store_true',
            help='Use a shaddow database to verify data integrity (default: false)')
        parser.add_argument(
            '-x',
            '--continue-on-exception',
            action='store_true',
            help='Continue execution after encountering unexpected/disallowed errors/exceptions (default: false)')

        return parser


    def init(self): # TODO: refactor
        global gContainer
        gContainer = Container() # micky-mouse DI

        global gSvcMgr # TODO: refactor away
        gSvcMgr = None

        parser = self._buildCmdLineParser()
        Config.init(parser)

        # Sanity check for arguments
        if Config.getConfig().use_shadow_db and Config.getConfig().max_dbs>1 :
            raise CrashGenError("Cannot combine use-shadow-db with max-dbs of more than 1")

        Logging.clsInit(Config.getConfig().debug)

        Dice.seed(0)  # initial seeding of dice

    def run(self):
        if Config.getConfig().run_tdengine:  # run server
            try:
                self.runService()
                return 0 # success
            except ConnectionError as err:
                Logging.error("Failed to make DB connection, please check DB instance manually")
            return -1 # failure
        else:
            return self.runClient()


class Container():
    _propertyList = {'defTdeInstance'}

    def __init__(self):
        self._cargo = {} # No cargo at the beginning

    def _verifyValidProperty(self, name):
        if not name in self._propertyList:
            raise CrashGenError("Invalid container property: {}".format(name))

    # Called for an attribute, when other mechanisms fail (compare to  __getattribute__)
    def __getattr__(self, name):
        self._verifyValidProperty(name)
        return self._cargo[name] # just a simple lookup

    def __setattr__(self, name, value):
        if name == '_cargo' : # reserved vars
            super().__setattr__(name, value)
            return
        self._verifyValidProperty(name)
        self._cargo[name] = value

