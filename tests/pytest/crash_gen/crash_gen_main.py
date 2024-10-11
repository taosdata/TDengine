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
from typing import \
    Optional  # Type hinting, ref: https://stackoverflow.com/questions/19202633/python-3-type-hinting-for-none

import textwrap
import time
import datetime
import random
import threading
import argparse
from decimal import Decimal, getcontext
import re
import sys
import os
import io
import signal
import traceback
import requests
# from guppy import hpy
import gc
import taos
import taosws
import taosrest
from taos.tmq import *

from .shared.types import TdColumns, TdTags

# from crash_gen import ServiceManager, TdeInstance, TdeSubProcess
# from crash_gen import ServiceManager, Config, DbConn, DbConnNative, Dice, DbManager, Status, Logging, Helper, \
#     CrashGenError, Progress, MyTDSql, \
#     TdeInstance

from .service_manager import ServiceManager, TdeInstance

from .shared.config import Config
from .shared.db import DbConn, DbManager, DbConnNative, MyTDSql
from .shared.misc import Dice, Logging, Helper, Status, CrashGenError, Progress
from .shared.types import TdDataType, DataBoundary, FunctionMap
from .shared.common import TDCom
from util.types import TDSmlProtocolType, TDSmlTimestampType

# Config.init()

# Require Python 3
if sys.version_info[0] < 3:
    raise Exception("Must be using Python 3")

# Global variables, tried to keep a small number.

# Command-line/Environment Configurations, will set a bit later
# ConfigNameSpace = argparse.Namespace
# gConfig:    argparse.Namespace 
gSvcMgr: Optional[ServiceManager]  # TODO: refactor this hack, use dep injection
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
        self.dbName = ""

        # Let us have a DB connection of our own
        if (Config.getConfig().per_thread_db_connection):  # type: ignore
            # print("connector_type = {}".format(gConfig.connector_type))
            tInst = gContainer.defTdeInstance
            if Config.getConfig().connector_type == 'native':
                self._dbConn = DbConn.createNative(tInst.getDbTarget())
            elif Config.getConfig().connector_type == 'rest':
                self._dbConn = DbConn.createRest(tInst.getDbTarget())
            elif Config.getConfig().connector_type == 'ws':
                self._dbConn = DbConn.createWs(tInst.getDbTarget(), self.dbName)
            elif Config.getConfig().connector_type == 'mixed':
                if Dice.throw(3) == 0:  # 1/3 chance
                    self._dbConn = DbConn.createNative(tInst.getDbTarget())
                if Dice.throw(3) == 1:  # 1/3 chance
                    self._dbConn = DbConn.createRest(tInst.getDbTarget())
                else:
                    self._dbConn = DbConn.createWs(tInst.getDbTarget(), self.dbName)
            else:
                raise RuntimeError("Unexpected connector type: {}".format(Config.getConfig().connector_type))

        # self._dbInUse = False  # if "use db" was executed already

    def logDebug(self, msg):
        Logging.debug("    TRD[{}] {}".format(self._tid, msg))

    def logInfo(self, msg):
        Logging.info("    TRD[{}] {}".format(self._tid, msg))

    def logError(self, msg):
        Logging.cfgPath = self._dbConn._dbTarget.cfgPath
        Logging.error("    TRD[{}] {}".format(self._tid, msg))

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
            if self._dbConn.isOpen:  # sometimes it is not open
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
            except threading.BrokenBarrierError as err:  # main thread timed out
                print("_bto", end="")
                Logging.debug("[TRD] Worker thread exiting due to main thread barrier time-out")
                break

            Logging.debug("[TRD] Worker thread [{}] exited barrier...".format(self._tid))
            self.crossStepGate()  # then per-thread gate, after being tapped
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
                if errno in [0x383, 0x386, 0x00B,
                             0x014]:  # invalid database, dropping, Unable to establish connection, Database not ready
                    # ignore
                    dummy = 0
                else:
                    print("\nCaught programming error. errno=0x{:X}, msg={} ".format(errno, err.msg))
                    self.logError("func _doTaskLoop error")
                    raise

            # Fetch a task from the Thread Coordinator
            Logging.debug("[TRD] Worker thread [{}] about to fetch task".format(self._tid))
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
            print("_tad", end="")  # Thread already dead

    def execSql(self, sql):  # TODO: expose DbConn directly
        return self.getDbConn().execute(sql)

    def querySql(self, sql):  # TODO: expose DbConn directly
        return self.getDbConn().query(sql)

    def getQueryResult(self):
        return self.getDbConn().getQueryResult()

    def getDbConn(self) -> DbConn:
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
    WORKER_THREAD_TIMEOUT = 300  # Normal: 120

    def __init__(self, pool: ThreadPool, dbManager: DbManager):
        self._curStep = -1  # first step is 0
        self._pool = pool
        # self._wd = wd
        self._te = None  # prepare for every new step
        self._dbManager = dbManager  # type: Optional[DbManager] # may be freed
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
        if self._curStep >= (maxSteps - 1):  # maxStep==10, last curStep should be 9
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

    def _hasAbortedTask(self):  # from execution of previous step
        # print("------- self._executedTasks: {}".format(self._executedTasks))
        for task in self._executedTasks:
            if task.isAborted():
                print("------- Task aborted: {}".format(task))
                # hasAbortedTask = True
                return True
        return False

    def _releaseAllWorkerThreads(self, transitionFailed):
        self._curStep += 1  # we are about to get into next step. TODO: race condition here!
        # Now not all threads had time to go to sleep
        Logging.debug(
            "--\r\n\n--> Step {} starts with main thread waking up".format(self._curStep))

        # A new TE for the new step
        self._te = None  # set to empty first, to signal worker thread to stop
        if not transitionFailed:  # only if not failed
            self._te = TaskExecutor(self._curStep)

        Logging.debug("[TRD] Main thread waking up at step {}, tapping worker threads".format(
            self._curStep))  # Now not all threads had time to go to sleep
        # Worker threads will wake up at this point, and each execute it's own task
        self.tapAllThreads()  # release all worker thread from their "gates"

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
                db = x  # type: Database
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
            if isinstance(err, CrashGenError):  # our own transition failure
                Logging.info("State transition error")
                # TODO: saw an error here once, let's print out stack info for err?
                traceback.print_stack()  # Stack frame to here.
                Logging.info("Caused by:")
                traceback.print_exception(
                    *sys.exc_info())  # Ref: https://www.geeksforgeeks.org/how-to-print-exception-stack-trace-in-python/
                transitionFailed = True
                self._te = None  # Not running any more
                self._execStats.registerFailure("State transition error: {}".format(err))
            else:
                Logging.error("func _doTransition error")
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
            if not Config.getConfig().debug:  # print this only if we are not in debug mode
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
                self._syncAtBarrier()  # For now just cross the barrier
                Progress.emit(Progress.END_THREAD_STEP)
                if self._stepStartTime:
                    stepExecTime = time.time() - self._stepStartTime
                    Progress.emitStr('{:.3f}s/{}'.format(stepExecTime, DbConnNative.totalRequests))
                    DbConnNative.resetTotalRequests()  # reset to zero
            except threading.BrokenBarrierError as err:
                self._execStats.registerFailure("Aborted due to worker thread timeout")
                Logging.error("\n")

                Logging.error("Main loop aborted, task {} caused by worker thread(s) time-out of {} seconds".format(
                    ThreadCoordinator.WORKER_THREAD_TIMEOUT, self._executedTasks))
                Logging.error("TAOS related threads blocked at (stack frames top-to-bottom):")
                ts = ThreadStacks()
                ts.record_current_time(time.time())  # record thread exit time at current moment
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
            hasAbortedTask = self._hasAbortedTask()  # from previous step
            if hasAbortedTask:
                Logging.info("Aborted task encountered, exiting test program")
                self._execStats.registerFailure("Aborted Task Encountered")
                break  # do transition only if tasks are error free

            # Ending previous step
            try:
                transitionFailed = self._doTransition()  # To start, we end step -1 first
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

        if hasAbortedTask or transitionFailed:  # abnormal ending, workers waiting at "gate"
            Logging.debug("Abnormal ending of main thraed")
        elif workerTimeout:
            Logging.debug("Abnormal ending of main thread, due to worker timeout")
        else:  # regular ending, workers waiting at "barrier"
            Logging.debug("Regular ending, main thread waiting for all worker threads to stop...")
            self._syncAtBarrier()

        self._te = None  # No more executor, time to end
        Logging.debug("Main thread tapping all threads one last time...")
        self.tapAllThreads()  # Let the threads run one last time
        # TODO: looks like we are not capturing the failures for the last step yet (i.e. calling registerFailure if neccessary)

        Logging.debug("\r\n\n--> Main thread ready to finish up...")
        Logging.debug("Main thread joining all threads")
        self._pool.joinAll()  # Get all threads to finish
        Logging.info(". . . All worker threads finished")  # No CR/LF before
        self._execStats.endExec()

    def cleanup(self):  # free resources
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
        self._dbs = []  # type: List[Database]
        dbc = self.getDbManager().getDbConn()
        if Config.getConfig().max_dbs == 0:
            self._dbs.append(Database(0, dbc))
        else:
            baseDbNumber = int(datetime.datetime.now().timestamp(  # Don't use Dice/random, as they are deterministic
            ) * 333) % 888 if Config.getConfig().dynamic_db_table_names else 0
            for i in range(Config.getConfig().max_dbs):
                self._dbs.append(Database(baseDbNumber + i, dbc))

    def pickDatabase(self):
        idxDb = 0
        if Config.getConfig().max_dbs != 0:
            idxDb = Dice.throw(Config.getConfig().max_dbs)  # 0 to N-1
        db = self._dbs[idxDb]  # type: Database
        return db

    def fetchTask(self) -> Task:
        ''' The thread coordinator (that's us) is responsible for fetching a task
            to be executed next.
        '''
        if (not self.isRunning()):  # no task
            raise RuntimeError("Cannot fetch task when not running")

        # pick a task type for current state
        db = self.pickDatabase()
        if Dice.throw(2) == 1:
            taskType = db.getStateMachine().pickTaskType()  # dynamic name of class
        else:
            taskType = db.getStateMachine().balance_pickTaskType()  # and an method can get  balance task types
            pass
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
        self.threadList = []  # maybe clean up each?


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
    CAN_CREATE_STREAM = 3  # super table must exists
    CAN_CREATE_TOPIC = 3  # super table must exists
    CAN_CREATE_CONSUMERS = 3
    CAN_CREATE_TSMA = 3
    CAN_CREATE_VIEW = 3  # super table must exists
    CAN_CREATE_INDEX = 3  # super table must exists
    CAN_DROP_FIXED_SUPER_TABLE = 4
    CAN_DROP_TOPIC = 4
    CAN_DROP_STREAM = 4
    CAN_PAUSE_STREAM = 4
    CAN_RESUME_STREAM = 4
    CAN_DROP_TSMA = 4
    CAN_DROP_VIEW = 4
    CAN_DROP_INDEX = 4
    CAN_ADD_DATA = 5
    CAN_READ_DATA = 6
    CAN_DELETE_DATA = 6


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
        if Config.getConfig().max_dbs > 0 or Config.getConfig().use_shadow_db:
            return False
        return self._info[self.CAN_DROP_DB]

    def canCreateFixedSuperTable(self):
        return self._info[self.CAN_CREATE_FIXED_SUPER_TABLE]

    def canDropFixedSuperTable(self):
        if Config.getConfig().use_shadow_db:  # duplicate writes to shaddow DB, in which case let's disable dropping s-table
            return False
        return self._info[self.CAN_DROP_FIXED_SUPER_TABLE]

    def canCreateTopic(self):
        return self._info[self.CAN_CREATE_TOPIC]

    def canDropTopic(self):
        return self._info[self.CAN_DROP_TOPIC]

    def canCreateConsumers(self):
        return self._info[self.CAN_CREATE_CONSUMERS]

    def canCreateTsma(self):
        return self._info[self.CAN_CREATE_TSMA]

    def canDropTsma(self):
        return self._info[self.CAN_DROP_TSMA]

    def canCreateStream(self):
        return self._info[self.CAN_CREATE_STREAM]

    def canDropStream(self):
        return self._info[self.CAN_DROP_STREAM]

    def canPauseStream(self):
        return self._info[self.CAN_PAUSE_STREAM]

    def canResumeStream(self):
        return self._info[self.CAN_RESUME_STREAM]

    def canCreateView(self):
        return self._info[self.CAN_CREATE_VIEW]

    def canDropView(self):
        return self._info[self.CAN_DROP_VIEW]

    def canCreateIndex(self):
        return self._info[self.CAN_CREATE_INDEX]

    def canDropIndex(self):
        return self._info[self.CAN_DROP_INDEX]


    def canAddData(self):
        return self._info[self.CAN_ADD_DATA]

    def canReadData(self):
        return self._info[self.CAN_READ_DATA]

    def canDeleteData(self):
        return self._info[self.CAN_DELETE_DATA]

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
                        "Unexpected more than 1 success at state: {}, with task: {}, in task set: {}".format(
                            self.__class__.__name__,
                            cls.__name__,  # verified just now that isinstance(task, cls)
                            [c.__class__.__name__ for c in tasks]
                        ))

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
            raise CrashGenError("Unexpected zero success at state: {}, with task: {}, in task set: {}".format(
                self.__class__.__name__,
                cls.__name__,  # verified just now that isinstance(task, cls)
                [c.__class__.__name__ for c in tasks]
            ))

    def assertNoTask(self, tasks, cls):
        for task in tasks:
            if isinstance(task, cls):
                raise CrashGenError(
                    "This task: {}, is not expected to be present, given the success/failure of others".format(
                        cls.__name__))

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
            # self.assertAtMostOneSuccess(tasks, TaskDropSuperTable)
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
            if not (self.hasTask(tasks, TaskCreateSuperTable)
            ):  # if we didn't create the table
                # we should not have a task that drops it
                self.assertNoTask(tasks, TaskDropSuperTable)
            # self.assertIfExistThenSuccess(tasks, ReadFixedDataTask)


class StateMechine:
    def __init__(self, db: Database):
        self._db = db
        # transitition target probabilities, indexed with value of STATE_EMPTY, STATE_DB_ONLY, etc.
        self._stateWeights = [1, 2, 10, 40]

    def init(self, dbc: DbConn):  # late initailization, don't save the dbConn
        try:
            self._curState = self._findCurrentState(dbc)  # starting state
        except taos.error.ProgrammingError as err:
            Logging.error("Failed to initialized state machine, cannot find current state: {}".format(err))
            traceback.print_stack()
            raise  # re-throw

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
        dbName = self._db.getName()
        if not dbc.existsDatabase(dbName):  # dbc.hasDatabases():  # no database?!
            Logging.debug("[STT] empty database found, between {} and {}".format(ts, time.time()))
            return StateEmpty()
        # did not do this when openning connection, and this is NOT the worker
        # thread, which does this on their own
        dbc.use(dbName)

        if not dbc.hasTables(dbName):  # no tables

            Logging.debug("[STT] DB_ONLY found, between {} and {}".format(ts, time.time()))
            return StateDbOnly()

        # For sure we have tables, which means we must have the super table. # TODO: are we sure?

        sTable = self._db.getFixedSuperTable()

        if sTable.hasRegTables(dbc):  # no regular tables
            # print("debug=====*\n"*100)
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
        dbc.execute("select * from information_schema.ins_dnodes")

        # Generic Checks, first based on the start state
        if not Config.getConfig().ignore_errors:  # verify state, only if we are asked not to ignore certain errors.
            if self._curState.canCreateDb():
                self._curState.assertIfExistThenSuccess(tasks, TaskCreateDb)
                # self.assertAtMostOneSuccess(tasks, CreateDbTask) # not really, in
                # case of multiple creation and drops

            if self._curState.canDropDb():
                if gSvcMgr == None:  # only if we are running as client-only
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
        if not Config.getConfig().ignore_errors:  # verify state, only if we are asked not to ignore certain errors.
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

    def balance_pickTaskType(self):
        # all the task types we can choose from at curent state
        BasicTypes = self.getTaskTypes()
        weightsTypes = BasicTypes.copy()

        # this matrixs can balance  the Frequency of TaskTypes
        balance_TaskType_matrixs = {'TaskDropDb': 5, 'TaskDropTopics': 20, 'TaskDropStreams': 1, 'TaskDropTsmas': 1, 'TaskDropViews': 1, 'TaskDropIndexes': 1,
                                    'TaskDropStreamTables': 10, 'TaskPauseStreams': 1, 'TaskResumeStreams': 1,
                                    'TaskReadData': 50, 'TaskDropSuperTable': 5, 'TaskAlterTags': 3, 'TaskAddData': 10,
                                    'TaskDeleteData': 10, 'TaskCreateDb': 10, 'TaskCreateStream': 10, 'TaskCreateTsma': 10, 'TaskCreateView': 10, 'TaskCreateIndex': 10,
                                    'TaskCreateTopic': 3,
                                    'TaskCreateConsumers': 10,
                                    'TaskCreateSuperTable': 10}  # TaskType : balance_matrixs of task

        for task, weights in balance_TaskType_matrixs.items():

            for basicType in BasicTypes:
                if basicType.__name__ == task:
                    for _ in range(weights):
                        weightsTypes.append(basicType)

        task = random.sample(weightsTypes, 1)
        return task[0]

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
    _clsLock = threading.Lock()  # class wide lock
    _lastInt = 101  # next one is initial integer
    _lastTick = None  # Optional[datetime]
    _lastLaggingTick = None  # Optional[datetime] # lagging tick, for out-of-sequence (oos) data insertions

    def __init__(self, dbNum: int, dbc: DbConn):  # TODO: remove dbc
        self._dbNum = dbNum  # we assign a number to databases, for our testing purpose
        self._stateMachine = StateMechine(self)
        self._stateMachine.init(dbc)

        self._lock = threading.RLock()

    def getStateMachine(self) -> StateMechine:
        return self._stateMachine

    def getDbNum(self):
        return self._dbNum

    def getName(self):
        return "db_{}".format(self._dbNum)

    def filterTasks(self, inTasks: List[Task]):  # Pick out those belonging to us
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
        # start time will be auto generated , start at 10 years ago  local time 
        local_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-16]
        local_epoch_time = [int(i) for i in local_time.split("-")]
        # local_epoch_time will be such as : [2022, 7, 18]

        t1 = datetime.datetime(local_epoch_time[0] - 5, local_epoch_time[1], local_epoch_time[2])
        t2 = datetime.datetime.now()
        # maybe a very large number, takes 69 years to exceed Python int range
        elSec = int(t2.timestamp() - t1.timestamp())
        elSec2 = (elSec % (8 * 12 * 30 * 24 * 60 * 60 / 500)) * \
                 500  # a number representing seconds within 10 years
        # print("elSec = {}".format(elSec))

        t3 = datetime.datetime(local_epoch_time[0] - 10, local_epoch_time[1],
                               local_epoch_time[2])  # default "keep" is 10 years
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
            if cls._lastLaggingTick is None or cls._lastTick is None:  # not initialized
                # 10k at 1/20 chance, should be enough to avoid overlaps
                tick = cls.setupLastTick()
                cls._lastTick = tick
                cls._lastLaggingTick = tick + datetime.timedelta(0,
                                                                 -60 * 2)  # lagging behind 2 minutes, should catch up fast
                # if : # should be quite a bit into the future

            if Config.isSet('mix_oos_data') and Dice.throw(
                    20) == 0:  # if asked to do so, and 1 in 20 chance, return lagging tick
                cls._lastLaggingTick += datetime.timedelta(0,
                                                           1)  # pick the next sequence from the lagging tick sequence
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
        self._db = db  # A task is always associated/for a specific DB

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

    def logError(self, msg):
        self._workerThread.logError(
            "Step[{}.{}] {}".format(
                self._curStep, self._taskNum, msg))

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        raise RuntimeError(
            "To be implemeted by child classes, class name: {}".format(
                self.__class__.__name__))

    def _isServiceStable(self):
        if not gSvcMgr:
            return True  # we don't run service, so let's assume it's stable
        return gSvcMgr.isStable()  # otherwise let's examine the service

    def _isErrAcceptable(self, errno, msg):
        if errno in [
            # TDengine 2.x Error Codes:
            0x05,  # TSDB_CODE_RPC_NOT_READY
            0x0B,  # Unable to establish connection, more details in TD-1648
            # 0x200, # invalid SQL， TODO: re-examine with TD-934
            0x20F,  # query terminated, possibly due to vnoding being dropped, see TD-1776
            0x213,  # "Disconnected from service", result of "kill connection ???"
            0x217,  # "db not selected", client side defined error code
            # 0x218, # "Table does not exist" client side defined error code
            0x360,  # Table already exists
            0x362,
            # 0x369, # tag already exists
            0x36A, 0x36B, 0x36D,
            0x381,
            0x380,  # "db not selected"
            0x383,
            0x386,  # DB is being dropped?!
            0x503,
            0x510,  # vnode not in ready state
            0x14,  # db not ready, errno changed
            0x600,  # Invalid table ID, why?
            0x218,  # Table does not exist

            # TDengine 3.0 Error Codes:
            0x0333,  # Object is creating # TODO: this really is NOT an acceptable error
            0x0369,  # Tag already exists
            0x0388,  # Database not exist
            0x03A0,  # STable already exists
            0x03A1,  # STable [does] not exist
            0x03AA,  # Tag already exists
            0x0603,  # Table already exists
            0x2603,  # Table does not exist, replaced by 2662 below
            0x260d,  # Tags number not matched
            0x2662,  # Table does not exist #TODO: what about 2603 above?
            0x2600,  # database not specified, SQL: show stables , database droped , and show tables
            0x032C,  # Object is creating
            0x032D,  # Object is dropping
            0x03D3,  # Conflict transaction not completed
            0x0707,  # Query not ready , it always occur at replica 3
            0x707,  # Query not ready
            0x396,  # Database in creating status
            0x386,  # Database in droping status
            0x03E1,  # failed on tmq_subscribe ,topic not exist
            0x03ed,  # Topic must be dropped first, SQL: drop database db_0
            0x0203,  # Invalid value
            0x03f0,  # Stream already exist , topic already exists
            0x260d,  # Tags number not matched added for stmt, #TODO add lock when alter schema
            1000  # REST catch-all error
        ]:
            return True  # These are the ALWAYS-ACCEPTABLE ones
        # This case handled below already.
        # elif (errno in [ 0x0B ]) and Settings.getConfig().auto_start_service:
        #     return True # We may get "network unavilable" when restarting service
        elif Config.getConfig().ignore_errors:  # something is specified on command line
            moreErrnos = [int(v, 0) for v in Config.getConfig().ignore_errors.split(',')]
            if errno in moreErrnos:
                return True
        elif errno == 0x200:  # invalid SQL, we need to div in a bit more
            if msg.find("invalid column name") != -1:
                return True
            elif msg.find("tags number not matched") != -1:  # mismatched tags after modification
                return True
            elif msg.find("duplicated column names") != -1:  # also alter table tag issues
                return True
        elif not self._isServiceStable():  # We are managing service, and ...
            Logging.info("Ignoring error when service starting/stopping: errno = {}, msg = {}".format(errno, msg))
            return True

        return False  # Not an acceptable error

    def execute(self, wt: WorkerThread):
        wt.verifyThreadSelf()
        self._workerThread = wt  # type: ignore

        te = wt.getTaskExecutor()
        self._curStep = te.getCurStep()
        self.logDebug(
            "[-] executing task {}...".format(self.__class__.__name__))

        self._err = None  # TODO: type hint mess up?
        self._execStats.beginTaskType(self.__class__.__name__)  # mark beginning
        errno2 = None

        # Now pick a database, and stick with it for the duration of the task execution
        dbName = self._db.getName()
        wt.dbName = dbName
        try:
            self._executeInternal(te, wt)  # TODO: no return value?
        except (taos.error.ProgrammingError, taos.error.StatementError, taosrest.errors.Error, taosrest.errors.ConnectError) as err:
            errno2 = Helper.convertErrno(err.errno)
            if (Config.getConfig().continue_on_exception):  # user choose to continue
                self.logDebug("[=] Continue after TAOS exception: errno=0x{:X}, msg: {}, SQL: {}".format(
                    errno2, err, wt.getDbConn().getLastSql()))
                self.logError("[=] Continue after TAOS exception: errno=0x{:X}, msg: {}, SQL: {}".format(
                    errno2, err, wt.getDbConn().getLastSql()))
                self._err = err
            elif self._isErrAcceptable(errno2, err.__str__()):
                self.logDebug("[=] Acceptable Taos library exception: errno=0x{:X}, msg: {}, SQL: {}".format(
                    errno2, err, wt.getDbConn().getLastSql()))
                self.logError("[=] Acceptable Taos library exception: errno=0x{:X}, msg: {}, SQL: {}".format(
                    errno2, err, wt.getDbConn().getLastSql()))
                # print("_", end="", flush=True)
                Progress.emit(Progress.ACCEPTABLE_ERROR)
                self._err = err
            else:  # not an acceptable error
                shortTid = threading.get_ident() % 10000
                errMsg = "[=] Unexpected Taos library exception ({}): errno=0x{:X}, thread={}, msg: {}, SQL: {}".format(
                    self.__class__.__name__,
                    errno2,
                    shortTid,
                    err, wt.getDbConn().getLastSql())
                self.logDebug(errMsg)
                self.logError(f'-------------------\nProgram ABORTED Due to Unexpected TAOS Error:{errMsg}\n-------------------')
                if Config.getConfig().debug:
                    # raise # so that we see full stack
                    traceback.print_exc()
                print(
                    "\n\n----------------------------\nProgram ABORTED Due to Unexpected TAOS Error: \n\n{}\n".format(
                        errMsg) +
                    "----------------------------\n")
                # sys.exit(-1)
                self._err = err
                self._aborted = True
        except (taosws.Error, taosws.OperationalError) as err:
            errmsg = err.args[0].split()[0]
            errno2 = int(errmsg[1:-1], 16)
            if (Config.getConfig().continue_on_exception):  # user choose to continue
                self.logDebug("[=] Continue after TAOS exception: errno=0x{:X}, msg: {}, SQL: {}".format(
                    errno2, err, wt.getDbConn().getLastSql()))
                self.logError("[=] Continue after TAOS exception: errno=0x{:X}, msg: {}, SQL: {}".format(
                    errno2, err, wt.getDbConn().getLastSql()))
                self._err = err
            elif self._isErrAcceptable(errno2, err.__str__()):
                self.logDebug("[=] Acceptable Taos library exception: errno=0x{:X}, msg: {}, SQL: {}".format(
                    errno2, err, wt.getDbConn().getLastSql()))
                self.logError("[=] Acceptable Taos library exception: errno=0x{:X}, msg: {}, SQL: {}".format(
                    errno2, err, wt.getDbConn().getLastSql()))
                # print("_", end="", flush=True)
                Progress.emit(Progress.ACCEPTABLE_ERROR)
                self._err = err
            else:  # not an acceptable error
                shortTid = threading.get_ident() % 10000
                errMsg = "[=] Unexpected Taos library exception ({}): errno=0x{:X}, thread={}, msg: {}, SQL: {}".format(
                    self.__class__.__name__,
                    errno2,
                    shortTid,
                    err, wt.getDbConn().getLastSql())
                self.logDebug(errMsg)
                self.logError(f'-------------------\nProgram ABORTED Due to Unexpected TAOS Error:{errMsg}\n-------------------')
                if Config.getConfig().debug:
                    # raise # so that we see full stack
                    traceback.print_exc()
                print(
                    "\n\n----------------------------\nProgram ABORTED Due to Unexpected TAOS Error: \n\n{}\n".format(
                        errMsg) +
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

    def lockTable(self, ftName):  # full table name
        # print(" <<" + ftName + '_', end="", flush=True)
        with Task._lock:  # SHORT lock! so we only protect lock creation
            if not ftName in Task._tableLocks:  # Create new lock and add to list, if needed
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
            errors[eno] = errors[eno] + 1 if eno in errors else 1

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
            "| Crash_Gen test {}, with the following stats:".format(
                "FAILED (reason: {})".format(
                    self._failureReason) if self._failed else "SUCCEEDED"))
        Logging.info("| Task Execution Times (success/total):")
        execTimesAny = 0.0
        print("----self._execTimes:", self._execTimes)
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
                            time.strftime("%x %X", time.localtime(MyTDSql.lqStartTime))))
        Logging.info("| Longest native query: {}".format(MyTDSql.longestQuery))
        Logging.info(
            "----------------------------------------------------------------------")


class StateTransitionTask(Task):
    LARGE_NUMBER_OF_TABLES = 35
    SMALL_NUMBER_OF_TABLES = 3
    LARGE_NUMBER_OF_RECORDS = 50
    SMALL_NUMBER_OF_RECORDS = 3

    _baseTableNumber = None

    _endState = None  # TODO: no longter used?

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
        if (StateTransitionTask._baseTableNumber is None):  # Set it one time
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

    def convert_to_hours(self, value, unit):
        """
        Convert the given time value and unit into hours.
        """
        if unit == 'm':
            return value / 60  # Convert minutes to hours
        elif unit == 'd':
            return value * 24  # Convert days to hours

    def convert_from_hours(self, hours, unit):
        """
        Convert hours to the specified unit.
        """
        if unit == 'm':
            return int(hours * 60)  # Convert hours to minutes
        elif unit == 'h':
            return int(hours)       # Keep hours unchanged
        elif unit == 'd':
            return max(1, int(hours / 24))  # Convert hours to days, minimum 1 day

    def random_duration_and_keeps(self):
        # Randomly select the unit and value for duration
        units = ['m', 'h', 'd']
        duration_unit = random.choice(['m', 'd'])  # Only choose minutes or days
        if duration_unit == 'm':
            duration = random.randint(60, 1440)  # From 1 hour to 1 day in minutes
        elif duration_unit == 'd':
            duration = random.randint(1, 3)      # From 1 to 3 days

        # Convert duration to hours for calculation purposes
        duration_hours = self.convert_to_hours(duration, duration_unit)

        # Minimum of 10 years in hours for the initial keep
        min_years = 10
        min_keep_hours = min_years * 365 * 24 + 3 * duration_hours  # Adding 3 times the duration_hours

        # Ensure keep0, keep1, keep2 are increasing and start from at least the calculated minimum
        keep0_hours = random.uniform(min_keep_hours, min_keep_hours + 100)
        keep1_hours = random.uniform(keep0_hours, keep0_hours + 100)
        keep2_hours = random.uniform(keep1_hours, keep1_hours + 100)

        # Randomly choose units for keeps and convert times
        keep0_unit = random.choice(units)
        keep1_unit = random.choice(units)
        keep2_unit = random.choice(units)

        keep0 = f"{self.convert_from_hours(keep0_hours, keep0_unit)}{keep0_unit}"
        keep1 = f"{self.convert_from_hours(keep1_hours, keep1_unit)}{keep1_unit}"
        keep2 = f"{self.convert_from_hours(keep2_hours, keep2_unit)}{keep2_unit}"

        duration_with_unit = f"{duration}{duration_unit}"
        keep_val = random.choice([keep0, keep1, keep2, f'{keep0},{keep1},{keep2}'])
        return duration_with_unit, keep_val


    def get_min_maxrows(self):
        """
        Selects a random number of rows within the specified boundaries.

        Returns:
            tuple: A tuple containing the minimum and maximum number of rows selected.
        Raises:
            ValueError: If the maximum number of rows is less than the minimum number of rows.
        """
        minrows = random.randint(*DataBoundary.MINROWS_BOUNDARY.value)
        maxrows_min = max(minrows + 1, DataBoundary.MINROWS_BOUNDARY.value[0])
        if DataBoundary.MINROWS_BOUNDARY.value[1] < maxrows_min:
            raise ValueError("maxrows < minrows")
        maxrows = random.randint(maxrows_min, DataBoundary.MINROWS_BOUNDARY.value[1])
        if Dice.throw(2) == 0:
            return minrows, maxrows
        else:
            return 100, 1000

    # Actually creating the database(es)
    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        # was: self.execWtSql(wt, "create database db")
        repStr = ""
        if Config.getConfig().num_replicas != 1:
            # numReplica = Dice.throw(Settings.getConfig().max_replicas) + 1 # 1,2 ... N
            numReplica = Config.getConfig().num_replicas  # fixed, always
            repStr = "replica {}".format(numReplica)
        updatePostfix = "" if Config.getConfig().verify_data else ""  # allow update only when "verify data" is active , 3.0 version default is update 1
        vg_nums = random.randint(1, 8)
        cache_model = Dice.choice(['none', 'last_row', 'last_value', 'both'])
        buffer = random.randint(3, 128)
        walRetentionPeriod = random.randint(1, 10000)
        dbName = self._db.getName()
        duration_with_unit, keep_val = self.random_duration_and_keeps()
        minrows, maxrows = self.get_min_maxrows()
        stt_trigger = Dice.choice(DataBoundary.STT_TRIGGER_BOUNDARY.value)
        precision = Dice.choice(DataBoundary.PRECISION_BOUNDARY.value)
        comp = Dice.choice(DataBoundary.COMP_BOUNDARY.value)
        cachesize = Dice.choice(DataBoundary.CACHESIZE_BOUNDARY.value)
        self.execWtSql(wt, 'create database {} {} {} vgroups {} cachemodel "{}" cachesize {} buffer {} wal_retention_period {} duration {} keep {} minrows {} maxrows {} stt_trigger {} precision "{}" comp {}'.format(dbName, repStr,
                                                                                                    updatePostfix,
                                                                                                    vg_nums,
                                                                                                    cache_model,
                                                                                                    cachesize,
                                                                                                    buffer,
                                                                                                    walRetentionPeriod,
                                                                                                    duration_with_unit,
                                                                                                    keep_val,
                                                                                                    minrows,
                                                                                                    maxrows,
                                                                                                    stt_trigger,
                                                                                                    precision,
                                                                                                    comp))
        if dbName == "db_0" and Config.getConfig().use_shadow_db:
            self.execWtSql(wt, "create database {} {} {} ".format("db_s", repStr, updatePostfix))

class TaskDropDb(StateTransitionTask):
    @classmethod
    def getEndState(cls):
        return StateEmpty()

    @classmethod
    def canBeginFrom(cls, state: AnyState):
        return state.canDropDb()

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):

        try:
            self.queryWtSql(wt, "drop database {}".format(
                self._db.getName()))  # drop database maybe failed ,because topic exists
        except taos.error.ProgrammingError as err:
            errno = Helper.convertErrno(err.errno)
            if errno in [0x0203]:  # drop maybe failed
                pass

        Logging.debug("[OPS] database dropped at {}".format(time.time()))


class TaskCreateStream(StateTransitionTask):
    maxSelectItems = 5
    @classmethod
    def getEndState(cls):
        return StateHasData()

    @classmethod
    def canBeginFrom(cls, state: AnyState):
        return state.canCreateStream()

    def getFillHistoryValue(self, rand=None):
        useTag = random.choice([True, False]) if rand is None else True
        fillHistoryVal = f'FILL_HISTORY 1' if useTag else random.choice(["FILL_HISTORY 0", ""])
        return fillHistoryVal

    def getExpiredValue(self, rand=None, countWindow=False):
        if countWindow:
            return random.choice(["IGNORE EXPIRED 1", ""])
        useTag = random.choice([True, False]) if rand is None else True
        expiredVal = f'IGNORE EXPIRED 0' if useTag else random.choice(["IGNORE EXPIRED 1", ""])
        return expiredVal

    def getUpdateValue(self, rand=None):
        useTag = random.choice([True, False]) if rand is None else True
        updateVal = f'IGNORE UPDATE 0' if useTag else random.choice(["IGNORE UPDATE 1", ""])
        return updateVal

    def getTriggerValue(self, forceTrigger=None):
        if forceTrigger is not None:
            return f"TRIGGER {forceTrigger}"
        maxDelayTime = random.choice(DataBoundary.MAX_DELAY_UNIT.value)
        return random.choice(["TRIGGER AT_ONCE", "TRIGGER WINDOW_CLOSE", f"TRIGGER MAX_DELAY {maxDelayTime}", ""])

    def getDeleteMarkValue(self):
        deleteMarkTime = random.choice(DataBoundary.DELETE_MARK_UNIT.value)
        return random.choice([f"DELETE_MARK {deleteMarkTime}", ""])

    def getWatermarkValue(self, rand=None):
        useTag = random.choice([True, False]) if rand is None else True
        timeVal = random.choice(DataBoundary.WATERMARK_UNIT.value)
        watermarkTime = f"WATERMARK {timeVal}" if useTag else random.choice([f"WATERMARK {timeVal}", ""])
        return watermarkTime

    def getSubtableValue(self, partitionList):
        subTablePre = "pre"
        partitionList = ['tbname']
        for colname in partitionList:
            subtable = f'CONCAT("{subTablePre}", {colname})'
        return random.choice([f'SUBTABLE({subtable})', ""])

    def remove_duplicates(self, selectPartStr):
        parts = selectPartStr.split(',')
        seen_now = seen_today = seen_timezone = False
        result = []
        for part in parts:
            part_stripped = part.strip()
            if part_stripped == "NOW()":
                if not seen_now:
                    seen_now = True
                    result.append(part)
            elif part_stripped == "TODAY()":
                if not seen_today:
                    seen_today = True
                    result.append(part)
            elif part_stripped == "TIMEZONE()":
                if not seen_timezone:
                    seen_timezone = True
                    result.append(part)
            else:
                result.append(part)
        return ', '.join(result)

    # def formatSelectPartStr(self, selectPart):
    #     parts = selectPart.split(',')
    #     parts = [f"`{part}`" for part in parts]
    #     return ','.join(parts)

    def generateRandomSubQuery(self, st, colDict, dbname, tbname, subtable=False, doAggr=0):
        selectPartList = []
        groupKeyList = []
        colTypes = [member.name for member in FunctionMap]
        tsCol = "ts"
        for column_name, column_type in colDict.items():
            if column_type == "TIMESTAMP":
                tsCol = column_name
            for fm in FunctionMap:
                if column_type in fm.value['types']:
                    selectStrs, groupKey = st.selectFuncsFromType(fm.value, column_name, column_type, doAggr, subquery=True)
                    if len(selectStrs) > 0:
                        selectPartList.append(selectStrs)
                    if len(groupKey) > 0:
                        groupKeyList.append(f'`{groupKey}`')

        if doAggr == 2:
            selectPartList = [random.choice(selectPartList)] if len(selectPartList) > 0 else ["count(*)"]
        if doAggr == 1:
            selectPartList = [tsCol] + selectPartList
        selectPartStr = ', '.join(selectPartList)
        selectPartStr = self.remove_duplicates(selectPartStr)
        if len(groupKeyList) > 0:
            groupKeyStr = ",".join(groupKeyList)
            partitionVal = st.getPartitionValue(groupKeyStr)
            if subtable:
                partitionVal = f'{partitionVal},tbname' if len(partitionVal) > 0 else "partition by tbname"
            return f"SELECT {selectPartStr} FROM {dbname}.{tbname} {partitionVal} {st.getOrderByValue(groupKeyStr)} {st.getSlimitValue()};"
        else:
            groupKeyStr = "tbname"
            partitionVal = "partition by tbname" if subtable else ""
        randomSelectPart = random.choice(selectPartList) if len(selectPartList) > 0 else groupKeyStr
        randomSelectPartStr = st.formatSelectPartStr(randomSelectPart)
        windowStr = st.getWindowStr(st.getRandomWindow(), colDict, stream=True)
        if ("COUNT_WINDOW" in windowStr or "STATE_WINDOW" in windowStr or "EVENT_WINDOW" in windowStr) and "partition" not in partitionVal:
            windowStr = f"partition by tbname {windowStr}"
        return f"SELECT {selectPartStr} FROM {dbname}.{tbname} {st.getTimeRangeFilter(tbname, tsCol, doAggr=doAggr)} {partitionVal} {windowStr} {st.getFillValue()} {st.getOrderByValue(randomSelectPartStr)};"

    def genCreateStreamSql(self, st, colDict, tbname):
        dbname = self._db.getName()
        doAggr = random.choice([0, 1, 2])
        forceTrigger = "AT_ONCE" if doAggr == 1 else None
        streamName = f'{dbname}_{tbname}_stm'
        target = f'stm_{dbname}_{tbname}_target'
        # TODO
        existStbFields = ""
        customTags = ""
        subtable = self.getSubtableValue(colDict.keys())
        subQuery = self.generateRandomSubQuery(st, colDict, dbname, tbname, subtable, doAggr)
        expiredValue = self.getExpiredValue(countWindow=True) if "COUNT_WINDOW" in subQuery else self.getExpiredValue()
        streamOps = f'{self.getTriggerValue(forceTrigger)} {self.getWatermarkValue()} {self.getFillHistoryValue()} {expiredValue} {self.getUpdateValue()}'
        if ("COUNT_WINDOW" in subQuery or "STATE_WINDOW" in subQuery) and "WATERMARK" not in streamOps:
            streamOps = f'{self.getTriggerValue(forceTrigger)} {self.getWatermarkValue(True)} {self.getFillHistoryValue()} {expiredValue} {self.getUpdateValue()}'
        return f"CREATE STREAM IF NOT EXISTS {streamName} {streamOps} into {dbname}.{target} {existStbFields} {customTags} {subtable} AS {subQuery};"

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        try:
            dbc = wt.getDbConn()
            if not self._db.exists(dbc):
                Logging.debug("Skipping task, no DB yet")
                return
            sTable = self._db.getFixedSuperTable()  # type: TdSuperTable
            if not sTable.ensureSuperTable(self, wt.getDbConn()):  # Ensure the super table exists
                return
            tags = sTable._getTags(dbc)
            # print("stream tags------", tags)
            cols = sTable._getCols(dbc)
            # print("stream cols------", cols)
            # if not tags or not cols:
            #     return "no table exists"
            tagCols = {**tags, **cols}
            # print("stream tagCols------", tagCols)
            selectCnt = random.randint(1, len(tagCols))
            # print("stream selectCnt------", selectCnt)
            selectKeys = random.sample(list(tagCols.keys()), selectCnt)
            # print("stream selectKeys------", selectKeys)
            selectItems = {key: tagCols[key] for key in selectKeys[:self.maxSelectItems]}
            # print("stream selectItems------", selectItems)


            # wt.execSql("use db")    # should always be in place
            stbname = sTable.getName()
            streamSql = self.genCreateStreamSql(sTable, selectItems, stbname)
            # print("streamSql------", streamSql)
            self.execWtSql(wt, streamSql)
            Logging.debug("[OPS] stream is creating at {}".format(time.time()))
        except taos.error.ProgrammingError as err:
            errno2 = Helper.convertErrno(err.errno)
            self.logError(f"func TaskCreateStream error: {errno2}-{err}")
            raise


    # def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
    #     dbname = self._db.getName()

    #     sub_stream_name = dbname + '_sub_stream'
    #     sub_stream_tb_name = 'stream_tb_sub'
    #     super_stream_name = dbname + '_super_stream'
    #     super_stream_tb_name = 'stream_tb_super'
    #     if not self._db.exists(wt.getDbConn()):
    #         Logging.debug("Skipping task, no DB yet")
    #         return

    #     sTable = self._db.getFixedSuperTable()  # type: TdSuperTable
    #     # wt.execSql("use db")    # should always be in place
    #     stbname = sTable.getName()
    #     sub_tables = sTable.getRegTables(wt.getDbConn())
    #     aggExpr = Dice.choice([
    #         'count(*)', 'avg(speed)', 'sum(speed)', 'stddev(speed)', 'min(speed)', 'max(speed)', 'first(speed)',
    #         'last(speed)',
    #         'apercentile(speed, 10)', 'last_row(*)', 'twa(speed)'])

    #     stream_sql = ''  # set default value

    #     if sub_tables:
    #         sub_tbname = sub_tables[0]
    #         # create stream with query above sub_table
    #         stream_sql = 'create stream {} into {}.{} as select  {}, avg(speed) FROM {}.{}  PARTITION BY tbname INTERVAL(5s) SLIDING(3s)  '. \
    #             format(sub_stream_name, dbname, sub_stream_tb_name, aggExpr, dbname, sub_tbname)
    #     else:
    #         stream_sql = 'create stream {} into {}.{} as select  {}, avg(speed) FROM {}.{}  PARTITION BY tbname INTERVAL(5s) SLIDING(3s)  '. \
    #             format(super_stream_name, dbname, super_stream_tb_name, aggExpr, dbname, stbname)
    #     self.execWtSql(wt, stream_sql)
    #     Logging.debug("[OPS] stream is creating at {}".format(time.time()))


class TaskCreateTopic(StateTransitionTask):

    @classmethod
    def getEndState(cls):
        return StateHasData()

    @classmethod
    def canBeginFrom(cls, state: AnyState):
        return state.canCreateTopic()

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        dbname = self._db.getName()

        sub_topic_name = dbname + '_sub_topic'
        super_topic_name = dbname + '_super_topic'
        stable_topic = dbname + '_stable_topic'
        db_topic = 'database_' + dbname + '_topics'
        if not self._db.exists(wt.getDbConn()):
            Logging.debug("Skipping task, no DB yet")
            return

        sTable = self._db.getFixedSuperTable()  # type: TdSuperTable
        # wt.execSql("use db")    # should always be in place
        # create topic if not exists topic_ctb_column as select ts, c1, c2, c3 from stb1;

        stbname = sTable.getName()
        if not sTable.ensureSuperTable(self, wt.getDbConn()):  # Ensure the super table exists
            return
        sub_tables = sTable.getRegTables(wt.getDbConn())

        scalarExpr = Dice.choice(
            ['*', 'speed', 'color', 'abs(speed)', 'acos(speed)', 'asin(speed)', 'atan(speed)', 'ceil(speed)',
             'cos(speed)', 'cos(speed)',
             'floor(speed)', 'log(speed,2)', 'pow(speed,2)', 'round(speed)', 'sin(speed)', 'sqrt(speed)',
             'char_length(color)', 'concat(color,color)',
             'concat_ws(" ", color,color," ")', 'length(color)', 'lower(color)', 'ltrim(color)', 'substr(color , 2)',
             'upper(color)', 'cast(speed as double)',
             'cast(ts as bigint)'])
        topic_sql = ''  # set default value
        if Dice.throw(3) == 0:  # create topic : source data from sub query
            if sub_tables:  # if not empty
                sub_tbname = sub_tables[0]
                # create topic : source data from sub query of sub stable
                topic_sql = 'create topic {}  as select {} FROM {}.{}  ;  '.format(sub_topic_name, scalarExpr, dbname,
                                                                                   sub_tbname)

            else:  # create topic : source data from sub query of stable
                topic_sql = 'create topic {}  as select  {} FROM {}.{}  '.format(super_topic_name, scalarExpr, dbname,
                                                                                 stbname)
        elif Dice.throw(3) == 1:  # create topic :  source data from super table
            topic_sql = 'create topic {}  AS STABLE {}.{}  '.format(stable_topic, dbname, stbname)

        elif Dice.throw(3) == 2:  # create topic :  source data from whole database
            topic_sql = 'create topic {}  AS DATABASE {}  '.format(db_topic, dbname)
        else:
            pass

        # exec create topics
        self.execWtSql(wt, "use {}".format(dbname))
        self.execWtSql(wt, topic_sql)
        Logging.debug("[OPS] db topic is creating at {}".format(time.time()))


class TaskCreateTsma(StateTransitionTask):
    maxSelectItems = 5
    @classmethod
    def getEndState(cls):
        return StateHasData()

    @classmethod
    def canBeginFrom(cls, state: AnyState):
        return state.canCreateTsma()

    def generateRandomFuncs(self, st, colDict):
        selectPartList = []
        doAggr = random.choice([0, 1]) # tsma must use aggFuncs or selectFuncs
        for column_name, column_type in colDict.items():
            for fm in FunctionMap:
                if column_type in fm.value['types']:
                    selectStrs, _ = st.selectFuncsFromType(fm.value, column_name, column_type, doAggr, tsma=True)
                    if len(selectStrs) > 0:
                        selectPartList.append(selectStrs)
        return ', '.join(selectPartList)

    def doubleWindow(self, windowStr):
        match = re.search(r"INTERVAL\((\d+)(\w+)\)", windowStr)
        if match:
            num = int(match.group(1))
            unit = match.group(2)
            new_num = num * 2
            return f"INTERVAL({new_num}{unit})"

    def genCreateTsmaSql(self, st, colDict, tbname):
        dbname = self._db.getName()
        tsmaName = f'{dbname}_{tbname}_tsma'
        recursiveTsmaName = f'{dbname}_{tbname}_rcs_tsma'
        funcs = self.generateRandomFuncs(st, colDict)
        windowStr = st.getWindowStr("INTERVAL", colDict, tsma=True)
        createTsmaSql = f"CREATE TSMA  IF NOT EXISTS {tsmaName} ON {dbname}.{tbname} FUNCTION({funcs}) {windowStr};"
        createRecursiveTsmaSql = f"CREATE RECURSIVE TSMA IF NOT EXISTS {recursiveTsmaName} ON {dbname}.{tsmaName} {self.doubleWindow(windowStr)};"
        return [createTsmaSql, createRecursiveTsmaSql]

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        try:
            dbname = self._db.getName()
            dbc = wt.getDbConn()

            if not self._db.exists(dbc):
                Logging.debug("Skipping task, no DB yet")
                return
            sTable = self._db.getFixedSuperTable()  # type: TdSuperTable
            cols = sTable._getCols(dbc)
            selectCnt = random.randint(1, len(cols))
            selectKeys = random.sample(list(cols.keys()), selectCnt)
            selectItems = {key: cols[key] for key in selectKeys[:self.maxSelectItems]}
            stbname = sTable.getName()
            if not sTable.ensureSuperTable(self, wt.getDbConn()):  # Ensure the super table exists
                return
            createTsmaSqls = self.genCreateTsmaSql(sTable, selectItems, stbname)
            self.execWtSql(wt, "use {}".format(dbname))
            for createTsmaSql in createTsmaSqls:
                self.execWtSql(wt, createTsmaSql)
            Logging.debug("[OPS] db tsma is creating at {}".format(time.time()))
        except taos.error.ProgrammingError as err:
            errno2 = Helper.convertErrno(err.errno)
            self.logError(f"func TaskCreateTsma error: {errno2}-{err}")
            raise

class TaskCreateView(StateTransitionTask):
    maxSelectItems = 5
    @classmethod
    def getEndState(cls):
        return StateHasData()

    @classmethod
    def canBeginFrom(cls, state: AnyState):
        return state.canCreateView()

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        try:
            dbc = wt.getDbConn()
            dbname = self._db.getName()
            if not self._db.exists(dbc):
                Logging.debug("Skipping task, no DB yet")
                return
            sTable = self._db.getFixedSuperTable()  # type: TdSuperTable
            if not sTable.ensureSuperTable(self, wt.getDbConn()):  # Ensure the super table exists
                return
            tags = sTable._getTags(dbc)
            cols = sTable._getCols(dbc)
            tagCols = {**tags, **cols}
            selectCnt = random.randint(1, len(tagCols))
            selectKeys = random.sample(list(tagCols.keys()), selectCnt)
            selectItems = {key: tagCols[key] for key in selectKeys[:self.maxSelectItems]}
            stbname = sTable.getName()
            subQuerySql = sTable.generateRandomSql(selectItems, stbname)
            nextInt = self._db.getNextInt()
            print("nextTick------", nextInt)
            viewSql = f'CREATE VIEW {dbname}_{stbname}_view_{nextInt} AS {subQuerySql};'
            print("viewSql------", viewSql)
            self.execWtSql(wt, viewSql)
            Logging.debug("[OPS] view is creating at {}".format(time.time()))
        except taos.error.ProgrammingError as err:
            errno2 = Helper.convertErrno(err.errno)
            self.logError(f"func TaskCreateView error: {errno2}-{err}")
            raise

class TaskCreateIndex(StateTransitionTask):
    @classmethod
    def getEndState(cls):
        return StateHasData()

    @classmethod
    def canBeginFrom(cls, state: AnyState):
        return state.canCreateIndex()

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        try:
            dbc = wt.getDbConn()
            dbname = self._db.getName()
            if not self._db.exists(dbc):
                Logging.debug("Skipping task, no DB yet")
                return
            sTable = self._db.getFixedSuperTable()  # type: TdSuperTable
            if not sTable.ensureSuperTable(self, wt.getDbConn()):  # Ensure the super table exists
                return
            tags = sTable._getTags(dbc)
            tagNames = list(tags.keys())
            tagName = random.choice(tagNames[1:])
            stbname = sTable.getName()
            nextInt = self._db.getNextInt()
            print("nextTick------", nextInt)
            indexSql = f'CREATE INDEX IF NOT EXISTS {dbname}_{stbname}_idx_{nextInt} ON {dbname}.{stbname} ({tagName});'
            print("indexSql------", indexSql)
            self.execWtSql(wt, indexSql)
            Logging.debug("[OPS] index is creating at {}".format(time.time()))
        except taos.error.ProgrammingError as err:
            errno2 = Helper.convertErrno(err.errno)
            self.logError(f"func TaskCreateIndex error: {errno2}-{err}")
            raise

class TaskDropTopics(StateTransitionTask):

    @classmethod
    def getEndState(cls):
        return StateHasData()

    @classmethod
    def canBeginFrom(cls, state: AnyState):
        return state.canDropTopic()

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        dbname = self._db.getName()

        if not self._db.exists(wt.getDbConn()):
            Logging.debug("Skipping task, no DB yet")
            return

        sTable = self._db.getFixedSuperTable()  # type: TdSuperTable
        # wt.execSql("use db")    # should always be in place
        tblName = sTable.getName()
        if sTable.hasTopics(wt.getDbConn()):
            sTable.dropTopics(wt.getDbConn(), dbname, None)  # drop topics of database
            sTable.dropTopics(wt.getDbConn(), dbname, tblName)  # drop topics of stable


class TaskDropStreams(StateTransitionTask):

    @classmethod
    def getEndState(cls):
        return StateHasData()

    @classmethod
    def canBeginFrom(cls, state: AnyState):
        return state.canDropStream()

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        # dbname = self._db.getName()

        if not self._db.exists(wt.getDbConn()):
            Logging.debug("Skipping task, no DB yet")
            return

        sTable = self._db.getFixedSuperTable()  # type: TdSuperTable
        # wt.execSql("use db")    # should always be in place
        # tblName = sTable.getName()
        if sTable.hasStreams(wt.getDbConn()):
            sTable.dropStreams(wt.getDbConn())  # drop stream of database


class TaskPauseStreams(StateTransitionTask):

    @classmethod
    def getEndState(cls):
        return StateHasData()

    @classmethod
    def canBeginFrom(cls, state: AnyState):
        return state.canPauseStream()

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        # dbname = self._db.getName()

        if not self._db.exists(wt.getDbConn()):
            Logging.debug("Skipping task, no DB yet")
            return

        sTable = self._db.getFixedSuperTable()  # type: TdSuperTable
        # wt.execSql("use db")    # should always be in place
        # tblName = sTable.getName()
        # TODO pause stream maybe failed because checkpoint trasactions
        if sTable.hasStreams(wt.getDbConn()):
            sTable.pauseStreams(wt.getDbConn())  # pause stream

class TaskResumeStreams(StateTransitionTask):

    @classmethod
    def getEndState(cls):
        return StateHasData()

    @classmethod
    def canBeginFrom(cls, state: AnyState):
        return state.canResumeStream()

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        # dbname = self._db.getName()

        if not self._db.exists(wt.getDbConn()):
            Logging.debug("Skipping task, no DB yet")
            return

        sTable = self._db.getFixedSuperTable()  # type: TdSuperTable
        # wt.execSql("use db")    # should always be in place
        # tblName = sTable.getName()
        # TODO stream may at ready state but resume can be success, so we just execute it and ignore conflict
        if sTable.hasStreams(wt.getDbConn()):
            sTable.resumeStreams(wt.getDbConn())  # pause stream

class TaskDropStreamTables(StateTransitionTask):

    @classmethod
    def getEndState(cls):
        return StateHasData()

    @classmethod
    def canBeginFrom(cls, state: AnyState):
        return state.canDropStream()

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        # dbname = self._db.getName()

        if not self._db.exists(wt.getDbConn()):
            Logging.debug("Skipping task, no DB yet")
            return

        sTable = self._db.getFixedSuperTable()  # type: TdSuperTable
        wt.execSql("use db")  # should always be in place
        # tblName = sTable.getName()
        if sTable.hasStreamTables(wt.getDbConn()):
            sTable.dropStreamTables(wt.getDbConn())  # drop stream tables


class TaskCreateConsumers(StateTransitionTask):

    @classmethod
    def getEndState(cls):
        return StateHasData()

    @classmethod
    def canBeginFrom(cls, state: AnyState):
        return state.canCreateConsumers()

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):

        if Config.getConfig().connector_type == 'native':
            sTable = self._db.getFixedSuperTable()  # type: TdSuperTable
            # wt.execSql("use db")    # should always be in place
            if sTable.hasTopics(wt.getDbConn()):
                sTable.createConsumer(wt.getDbConn(), random.randint(1, 10))
                pass
        elif Config.getConfig().connector_type == 'rest':
            print(" Restful not support tmq consumers")
            return
        else:
            # TODO WS supported
            print("TMQ in Websocket TODO supported")
            return

class TaskDropTsmas(StateTransitionTask):

    @classmethod
    def getEndState(cls):
        return StateHasData()

    @classmethod
    def canBeginFrom(cls, state: AnyState):
        return state.canDropTsma()

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        # dbname = self._db.getName()

        if not self._db.exists(wt.getDbConn()):
            Logging.debug("Skipping task, no DB yet")
            return

        sTable = self._db.getFixedSuperTable()  # type: TdSuperTable
        # wt.execSql("use db")    # should always be in place
        # tblName = sTable.getName()
        if sTable.hasTsmas(wt.getDbConn()):
            sTable.dropStreams(wt.getDbConn())  # drop stream of database

class TaskDropViews(StateTransitionTask):

    @classmethod
    def getEndState(cls):
        return StateHasData()

    @classmethod
    def canBeginFrom(cls, state: AnyState):
        return state.canDropView()

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        dbname = self._db.getName()

        if not self._db.exists(wt.getDbConn()):
            Logging.debug("Skipping task, no DB yet")
            return

        sTable = self._db.getFixedSuperTable()  # type: TdSuperTable
        if sTable.hasViews(wt.getDbConn(), dbname):
            sTable.dropViews(wt.getDbConn(), dbname)  # drop views of database


class TaskDropIndexes(StateTransitionTask):

    @classmethod
    def getEndState(cls):
        return StateHasData()

    @classmethod
    def canBeginFrom(cls, state: AnyState):
        return state.canDropIndex()

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        dbname = self._db.getName()

        if not self._db.exists(wt.getDbConn()):
            Logging.debug("Skipping task, no DB yet")
            return

        sTable = self._db.getFixedSuperTable()  # type: TdSuperTable
        if sTable.hasIndexes(wt.getDbConn(), dbname):
            sTable.dropIndexes(wt.getDbConn(), dbname)  # drop views of database


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

        sTable = self._db.getFixedSuperTable()  # type: TdSuperTable
        # wt.execSql("use db")    # should always be in place
        # backup 20240606 by jayden
        # sTable.create(wt.getDbConn(),
        #               {'ts': TdDataType.TIMESTAMP, 'speed': TdDataType.INT, 'color': TdDataType.BINARY16}, {
        #                   'b': TdDataType.BINARY200, 'f': TdDataType.FLOAT},
        #               dropIfExists=True
        #               )
        cols = {
                    'ts'                        : TdDataType.TIMESTAMP,
                    'speed'                     : TdDataType.INT,
                    'color'                     : TdDataType.BINARY16,
                    'tinyint_type_col_name'     : TdDataType.TINYINT,
                    'smallint_type_col_name'    : TdDataType.SMALLINT,
                    'bigint_type_col_name'      : TdDataType.BIGINT,
                    'utinyint_type_col_name'    : TdDataType.TINYINT,
                    'usmallint_type_col_name'   : TdDataType.SMALLINT,
                    'uint_type_col_name'        : TdDataType.INT,
                    'ubigint_type_col_name'     : TdDataType.BIGINT,
                    'float_type_col_name'       : TdDataType.FLOAT,
                    'double_type_col_name'      : TdDataType.DOUBLE,
                    'bool_type_col_name'        : TdDataType.BOOL,
                    'nchar_type_col_name'       : TdDataType.NCHAR16,
                    'varchar_type_col_name'     : TdDataType.VARCHAR16,
                    'varbinary_type_col_name'   : TdDataType.VARBINARY16,
                    'geometry_type_col_name'    : TdDataType.GEOMETRY32,
                }
        tags = {
                    'b'                         : TdDataType.BINARY200,
                    'f'                         : TdDataType.FLOAT,
                    'tinyint_type_tag_name'     : TdDataType.TINYINT,
                    'smallint_type_tag_name'    : TdDataType.SMALLINT,
                    'int_type_tag_name'         : TdDataType.INT,
                    'bigint_type_tag_name'      : TdDataType.BIGINT,
                    'utinyint_type_tag_name'    : TdDataType.TINYINT,
                    'usmallint_type_tag_name'   : TdDataType.USMALLINT,
                    'uint_type_tag_name'        : TdDataType.UINT,
                    'ubigint_type_tag_name'     : TdDataType.BIGINT,
                    'double_type_tag_name'      : TdDataType.DOUBLE,
                    'bool_type_tag_name'        : TdDataType.BOOL,
                    'nchar_type_tag_name'       : TdDataType.NCHAR16,
                    'varchar_type_tag_name'     : TdDataType.VARCHAR16,
                    'varbinary_type_tag_name'   : TdDataType.VARBINARY16,
                    'geometry_type_tag_name'    : TdDataType.GEOMETRY32,
                }
        sTable.create(wt.getDbConn(),
                      cols,
                      tags,
                      dropIfExists=True
                      )
        # self.execWtSql(wt,"create table db.{} (ts timestamp, speed int) tags (b binary(200), f float) ".format(tblName))
        # No need to create the regular tables, INSERT will do that
        # automatically


class TdSuperTable:
    def __init__(self, stName, dbName):
        self._stName = stName
        self._dbName = dbName
        self._consumerLists = {}
        self._ConsumerInsts = []

    def getName(self):
        return self._stName

    def drop(self, dbc, skipCheck=False):
        dbName = self._dbName
        if self.exists(dbc):  # if myself exists
            fullTableName = dbName + '.' + self._stName
            dbc.execute("DROP TABLE {}".format(fullTableName))
        else:
            if not skipCheck:
                raise CrashGenError("Cannot drop non-existant super table: {}".format(self._stName))

    def exists(self, dbc):
        dbc.execute("USE " + self._dbName)
        return dbc.existsSuperTable(self._dbName, self._stName)

    # TODO: odd semantic, create() method is usually static?
    def create(self, dbc, cols: TdColumns, tags: TdTags, dropIfExists=False):
        '''Creating a super table'''

        dbName = self._dbName
        dbc.execute("USE " + dbName)
        fullTableName = dbName + '.' + self._stName

        if dbc.existsSuperTable(self._dbName, self._stName):
            if dropIfExists:
                dbc.execute("DROP TABLE {}".format(fullTableName))

            else:  # error
                raise CrashGenError("Cannot create super table, already exists: {}".format(self._stName))

        # Now let's create
        sql = "CREATE TABLE {} ({})".format(
            fullTableName,
            ",".join(['%s %s' % (k, v.value) for (k, v) in cols.items()]))
        if tags:
            sql += " TAGS ({})".format(
                ",".join(['%s %s' % (k, v.value) for (k, v) in tags.items()])
            )
        else:
            sql += " TAGS (dummy int) "
        dbc.execute(sql)

    def createConsumer(self, dbc, Consumer_nums):

        def generateConsumer(current_topic_list):
            consumer = Consumer({"group.id": "tg2", "td.connect.user": "root", "td.connect.pass": "taosdata"})
            topic_list = []
            for topic in current_topic_list:
                topic_list.append(topic)

            try:
                consumer.subscribe(topic_list)

                # consumer with random work life
                time_start = time.time()
                while 1:
                    res = consumer.poll(1)
                    consumer.commit(res)
                    if time.time() - time_start > random.randint(5, 50):
                        break
                consumer.unsubscribe()
                consumer.close()
            except TmqError as err: # topic deleted by other threads
                pass
            return

        # mulit Consumer
        current_topic_list = self.getTopicLists(dbc)
        for i in range(Consumer_nums):
            consumer_inst = threading.Thread(target=generateConsumer, args=(current_topic_list,))
            self._ConsumerInsts.append(consumer_inst)

        for ConsumerInst in self._ConsumerInsts:
            ConsumerInst.start()
        for ConsumerInst in self._ConsumerInsts:
            ConsumerInst.join()

    def getTopicLists(self, dbc: DbConn):
        dbc.query("show topics ")
        topics = dbc.getQueryResult()
        topicLists = [v[0] for v in topics]
        return topicLists

    def getRegTables(self, dbc: DbConn):
        dbName = self._dbName
        try:
            dbc.query("select distinct TBNAME from {}.{}".format(dbName,
                                                                 self._stName))  # TODO: analyze result set later
        except taos.error.ProgrammingError as err:
            errno2 = Helper.convertErrno(err.errno)
            Logging.debug("[=] Failed to get tables from super table: errno=0x{:X}, msg: {}".format(errno2, err))
            raise

        qr = dbc.getQueryResult()
        return [v[0] for v in
                qr]  # list transformation, ref: https://stackoverflow.com/questions/643823/python-list-transformation

    def hasRegTables(self, dbc: DbConn):

        if dbc.existsSuperTable(self._dbName, self._stName):

            return dbc.query("SELECT * FROM {}.{}".format(self._dbName, self._stName)) > 0
        else:
            return False

    def hasStreamTables(self, dbc: DbConn):

        return dbc.query("show {}.stables like 'stream_tb%'".format(self._dbName)) > 0

    def hasStreams(self, dbc: DbConn):
        return dbc.query("show streams") > 0

    def hasTsmas(self, dbc: DbConn):
        return dbc.query("show tsmas") > 0

    def hasViews(self, dbc: DbConn, dbname):
        return dbc.query(f"show {dbname}.views") > 0

    def hasIndexes(self, dbc: DbConn, dbname):
        return dbc.query(f'select * from information_schema.ins_indexes where db_name = "{dbname}";') > 1
        # return dbc.query(f'select * from information_schema.ins_indexes where db_name = "{dbname}" and index_name like "%idx";') > 1

    def hasTopics(self, dbc: DbConn):

        return dbc.query("show topics") > 0

    def dropTopics(self, dbc: DbConn, dbname=None, stb_name=None):
        dbc.query("show topics ")
        topics = dbc.getQueryResult()

        if dbname != None and stb_name == None:

            for topic in topics:
                if dbname in topic[0] and topic[0].startswith("database"):
                    try:
                        dbc.execute('drop topic {}'.format(topic[0]))
                        Logging.debug("[OPS] topic  {} is droping at {}".format(topic, time.time()))
                    except taos.error.ProgrammingError as err:
                        errno = Helper.convertErrno(err.errno)
                        if errno in [0x03EB]:  # Topic subscribed cannot be dropped
                            pass
                            # for subsript in subscriptions:

                        else:
                            pass

                        pass
            return True
        elif dbname != None and stb_name != None:
            for topic in topics:
                if topic[0].startswith(self._dbName) and topic[0].endswith('topic'):
                    dbc.execute('drop topic {}'.format(topic[0]))
                    Logging.debug("[OPS] topic  {} is droping at {}".format(topic, time.time()))
            return True
        else:
            return True
            pass

    def dropStreams(self, dbc: DbConn):
        dbc.query("show streams ")
        Streams = dbc.getQueryResult()
        for Stream in Streams:
            if Stream[0].startswith(self._dbName):
                dbc.execute('drop stream {}'.format(Stream[0]))

        return not dbc.query("show streams ") > 0

    def dropTsmas(self, dbc: DbConn):
        dbc.query("show tsmas ")
        tsmas = dbc.getQueryResult()
        for tsma in tsmas:
            if tsma[0].startswith(self._dbName):
                dbc.execute('drop tsma {}'.format(tsma[0]))

        return not dbc.query("show tsmas ") > 0

    def dropViews(self, dbc: DbConn, dbname):
        dbc.query(f"show {dbname}.views;")
        views = dbc.getQueryResult()
        for view in views:
            if view[0].startswith(self._dbName):
                print('drop view {}.{}'.format(dbname, view[0]))
                dbc.execute('drop view {}.{}'.format(dbname, view[0]))

        return not dbc.query(f"show {dbname}.views;") > 0

    def dropIndexes(self, dbc: DbConn, dbname):
        dbc.query(f'select * from information_schema.ins_indexes where db_name = "{dbname}";')
        indexes = dbc.getQueryResult()
        for idx in indexes:
            if idx[0].startswith(self._dbName):
                print('drop index {}.{}'.format(dbname, idx[0]))
                dbc.execute('drop index {}.{}'.format(dbname, idx[0]))
        # TODO confirm
        return not dbc.query(f'select * from information_schema.ins_indexes where db_name = "{dbname}";') > 1

    def dropStreamTables(self, dbc: DbConn):
        dbc.query("show {}.stables like 'stream_tb%'".format(self._dbName))

        StreamTables = dbc.getQueryResult()

        for StreamTable in StreamTables:
            if self.dropStreams(dbc):
                dbc.execute('drop table {}.{}'.format(self._dbName, StreamTable[0]))

        return not dbc.query("show {}.stables like 'stream_tb%'".format(self._dbName))

    def pauseStreams(self, dbc: DbConn):
        dbc.query("show streams ")
        Streams = dbc.getQueryResult()
        for Stream in Streams:
            if Stream[0].startswith(self._dbName):
                dbc.execute('pause stream {}'.format(Stream[0]))
        return True

    def resumeStreams(self, dbc: DbConn):
        dbc.query("show streams ")
        Streams = dbc.getQueryResult()
        for Stream in Streams:
            if Stream[0].startswith(self._dbName):
                resumeSql = random.choice([f'resume stream {Stream[0]}', f'resume stream ignore untreated {Stream[0]}'])
                dbc.execute(resumeSql)
        return True

    def ensureSuperTable(self, task: Optional[Task], dbc: DbConn):
        '''
        Make sure a super table exists for this database.
        If there is an associated "Task" that wants to do this, "lock" this table so that
        others don't access it while we create it.
        '''
        dbName = self._dbName
        # if not self._checkStableExists(dbc, self._stName):
        #     return
        sql = f'select `stable_name` from information_schema.ins_stables where db_name = "{dbName}" and stable_name = "{self._stName}";'
        return dbc.query(sql) > 0

    def ensureRegTable(self, task: Optional[Task], dbc: DbConn, regTableName: str):
        '''
        Make sure a regular table exists for this super table, creating it if necessary.
        If there is an associated "Task" that wants to do this, "lock" this table so that
        others don't access it while we create it.
        '''
        dbName = self._dbName
        # if not self._checkStableExists(dbc, self._stName):
        #     return
        sql = "select tbname from {}.{} where tbname in ('{}')".format(dbName, self._stName, regTableName)
        if dbc.query(sql) >= 1:  # reg table exists already
            return
        # acquire a lock first, so as to be able to *verify*. More details in TD-1471
        fullTableName = dbName + '.' + regTableName
        if task is not None:  # Somethime thie operation is requested on behalf of a "task"
            # Logging.info("Locking table for creation: {}".format(fullTableName))
            task.lockTable(fullTableName)  # in which case we'll lock this table to ensure serialized access
            # Logging.info("Table locked for creation".format(fullTableName))
        Progress.emit(Progress.CREATE_TABLE_ATTEMPT)  # ATTEMPT to create a new table
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
                task.unlockTable(fullTableName)  # no matter what
                # Logging.info("Table unlocked after creation: {}".format(fullTableName))

    def formatSelectPartStr(self, selectPart):
        level = 0
        parts = []
        current_part = []

        for char in selectPart:
            if char == '(':
                level += 1
            elif char == ')':
                level -= 1

            if char == ',' and level == 0:
                parts.append(''.join(current_part))
                current_part = []
            else:
                current_part.append(char)

        if current_part:
            parts.append(''.join(current_part))

        parts = [f"`{part}`" for part in parts]
        return ','.join(parts)

    def _getTagStrForSql(self, dbc):
        tags = self._getTags(dbc)
        tagStrs = []
        record_str_idx_lst = list()
        start_idx = 0
        for tagName in tags:
            tagType = tags[tagName]
            if tagType == 'BINARY':
                tagStrs.append("'Beijing-Shanghai-LosAngeles'")
                record_str_idx_lst.append(start_idx)
            elif tagType == 'VARCHAR' or tagType == 'VARBINARY' or tagType == 'NCHAR':
                tagStrs.append(TDCom.get_long_name(16))
                record_str_idx_lst.append(start_idx)
            elif tagType == 'GEOMETRY':
                tagStrs.append(random.choice(DataBoundary.GEOMETRY_BOUNDARY.value))
                record_str_idx_lst.append(start_idx)
            elif tagType == 'TINYINT':
                tagStrs.append(random.randint(DataBoundary.TINYINT_BOUNDARY.value[0], DataBoundary.TINYINT_BOUNDARY.value[1]))
            elif tagType == 'SMALLINT':
                tagStrs.append(random.randint(DataBoundary.SMALLINT_BOUNDARY.value[0], DataBoundary.SMALLINT_BOUNDARY.value[1]))
            elif tagType == 'INT':
                tagStrs.append(random.randint(DataBoundary.INT_BOUNDARY.value[0], DataBoundary.INT_BOUNDARY.value[1]))
            elif tagType == 'BIGINT':
                tagStrs.append(random.randint(DataBoundary.BIGINT_BOUNDARY.value[0], DataBoundary.BIGINT_BOUNDARY.value[1]))
            elif tagType == 'TINYINT UNSIGNED':
                tagStrs.append(random.randint(DataBoundary.UTINYINT_BOUNDARY.value[0], DataBoundary.UTINYINT_BOUNDARY.value[1]))
            elif tagType == 'SMALLINT UNSIGNED':
                tagStrs.append(random.randint(DataBoundary.USMALLINT_BOUNDARY.value[0], DataBoundary.USMALLINT_BOUNDARY.value[1]))
            elif tagType == 'INT UNSIGNED':
                tagStrs.append(random.randint(DataBoundary.UINT_BOUNDARY.value[0], DataBoundary.UINT_BOUNDARY.value[1]))
            elif tagType == 'BIGINT UNSIGNED':
                tagStrs.append(random.randint(DataBoundary.UBIGINT_BOUNDARY.value[0], DataBoundary.UBIGINT_BOUNDARY.value[1]))
            elif tagType == 'FLOAT':
                tagStrs.append(random.uniform(DataBoundary.FLOAT_BOUNDARY.value[0], DataBoundary.FLOAT_BOUNDARY.value[1]))
            elif tagType == 'DOUBLE':
                getcontext().prec = 50
                low = Decimal(DataBoundary.DOUBLE_BOUNDARY.value[0])
                high = Decimal(DataBoundary.DOUBLE_BOUNDARY.value[1])
                random_decimal = low + (high - low) * Decimal(random.random())
                tagStrs.append(float(random_decimal))
            elif tagType == 'BOOL':
                tagStrs.append(random.choice(DataBoundary.BOOL_BOUNDARY.value))
            else:
                raise RuntimeError("Unexpected tag type: {}".format(tagType))
            start_idx += 1
        tagStrs_to_string_list = list(map(lambda x:str(x), tagStrs))
        trans_tagStrs_to_string_list = list(map(lambda x: f'"{x[1]}"' if x[0] in record_str_idx_lst else x[1], enumerate(tagStrs_to_string_list)))
        return ", ".join(trans_tagStrs_to_string_list)

    def _checkStableExists(self, dbc, table):
        """
        Check if a table exists in the stable list of a database.

        Args:
            dbc (DatabaseConnection): The database connection object.
            table (str): The name of the table to check.

        Returns:
            bool: True if the table exists in the stable list, False otherwise.
        """
        dbc.query("show {}.stables".format(self._dbName))
        stCols = dbc.getQueryResult()
        tables = [row[0] for row in stCols]
        return table in tables

    def _getTags(self, dbc):
        """
        Retrieves the tags from the specified database table.

        Args:
            dbc: The database connection object.

        Returns:
            A dictionary containing the tags retrieved from the table, where the keys are the tag names and the values are the tag types.
        """
        dbc.query("DESCRIBE {}.{}".format(self._dbName, self._stName))
        stCols = dbc.getQueryResult()
        ret = {row[0]: row[1] for row in stCols if row[3] == 'TAG'}  # name:type
        return ret

    def _getCols(self, dbc):
        """
        Retrieve the columns of a table from the database.

        Args:
            dbc: The database connection object.

        Returns:
            A dictionary containing the column names as keys and their corresponding types as values.
        """
        # if self._checkStableExists(dbc, self._stName):
        dbc.query("DESCRIBE {}.{}".format(self._dbName, self._stName))
        stCols = dbc.getQueryResult()
        ret = {row[0]: row[1] for row in stCols if row[3] != 'TAG'}  # name:type
        return ret
        # else:
        #     return False

    def addTag(self, dbc, tagName, tagType):
        if tagName in self._getTags(dbc):  # already
            return
        # sTable.addTag("extraTag", "int")
        sql = "alter table {}.{} add tag {} {}".format(
            self._dbName, self._stName, tagName, tagType)
        dbc.execute(sql)

    def dropTag(self, dbc, tagName):
        if not tagName in self._getTags(dbc):  # don't have this tag
            return
        sql = "alter table {}.{} drop tag {}".format(self._dbName, self._stName, tagName)
        dbc.execute(sql)

    def changeTag(self, dbc, oldTag, newTag):
        tags = self._getTags(dbc)
        if not oldTag in tags:  # don't have this tag
            return
        if newTag in tags:  # already have this tag
            return
        sql = "alter table {}.{} change tag {} {}".format(self._dbName, self._stName, oldTag, newTag)
        dbc.execute(sql)

    # def generateQueries(self, dbc: DbConn) -> List[SqlQuery]:
    #     ''' Generate queries to test/exercise this super table '''
    #     ret = []  # type: List[SqlQuery]

    #     for rTbName in self.getRegTables(dbc):  # regular tables

    #         filterExpr = Dice.choice([  # TODO: add various kind of WHERE conditions
    #             None
    #         ])

    #         # Run the query against the regular table first
    #         doAggr = (Dice.throw(2) == 0)  # 1 in 2 chance
    #         if not doAggr:  # don't do aggregate query, just simple one
    #             commonExpr = Dice.choice([
    #                 '*',
    #                 'abs(speed)',
    #                 'acos(speed)',
    #                 'asin(speed)',
    #                 'atan(speed)',
    #                 'ceil(speed)',
    #                 'cos(speed)',
    #                 'cos(speed)',
    #                 'floor(speed)',
    #                 'log(speed,2)',
    #                 'pow(speed,2)',
    #                 'round(speed)',
    #                 'sin(speed)',
    #                 'sqrt(speed)',
    #                 'char_length(color)',
    #                 'concat(color,color)',
    #                 'concat_ws(" ", color,color," ")',
    #                 'length(color)',
    #                 'lower(color)',
    #                 'ltrim(color)',
    #                 'substr(color , 2)',
    #                 'upper(color)',
    #                 'cast(speed as double)',
    #                 'cast(ts as bigint)',
    #                 # 'TO_ISO8601(color)',
    #                 # 'TO_UNIXTIMESTAMP(ts)',
    #                 'now()',
    #                 'timediff(ts,now)',
    #                 'timezone()',
    #                 'TIMETRUNCATE(ts,1s)',
    #                 'TIMEZONE()',
    #                 'TODAY()',
    #                 'distinct(color)'
    #             ]
    #             )
    #             ret.append(SqlQuery(  # reg table
    #                 "select {} from {}.{}".format(commonExpr, self._dbName, rTbName)))
    #             ret.append(SqlQuery(  # super table
    #                 "select {} from {}.{}".format(commonExpr, self._dbName, self.getName())))
    #         else:  # Aggregate query
    #             aggExpr = Dice.choice([
    #                 'count(*)',
    #                 'avg(speed)',
    #                 # 'twa(speed)', # TODO: this one REQUIRES a where statement, not reasonable
    #                 'sum(speed)',
    #                 'stddev(speed)',
    #                 # SELECTOR functions
    #                 'min(speed)',
    #                 'max(speed)',
    #                 'first(speed)',
    #                 'last(speed)',
    #                 'top(speed, 50)',  # TODO: not supported?
    #                 'bottom(speed, 50)',  # TODO: not supported?
    #                 'apercentile(speed, 10)',  # TODO: TD-1316
    #                 'last_row(*)',  # TODO: commented out per TD-3231, we should re-create
    #                 # Transformation Functions
    #                 # 'diff(speed)', # TODO: no supported?!
    #                 'spread(speed)',
    #                 'elapsed(ts)',
    #                 'mode(speed)',
    #                 'bottom(speed,1)',
    #                 'top(speed,1)',
    #                 'tail(speed,1)',
    #                 'unique(color)',
    #                 'csum(speed)',
    #                 'DERIVATIVE(speed,1s,1)',
    #                 'diff(speed,1)',
    #                 'irate(speed)',
    #                 'mavg(speed,3)',
    #                 'sample(speed,5)',
    #                 'STATECOUNT(speed,"LT",1)',
    #                 'STATEDURATION(speed,"LT",1)',
    #                 'twa(speed)'

    #             ])  # TODO: add more from 'top'

    #             # if aggExpr not in ['stddev(speed)']: # STDDEV not valid for super tables?! (Done in TD-1049)
    #             sql = "select {} from {}.{}".format(aggExpr, self._dbName, self.getName())
    #             if Dice.throw(3) == 0:  # 1 in X chance
    #                 partion_expr = Dice.choice(['color', 'tbname'])
    #                 sql = sql + ' partition BY ' + partion_expr + ' order by ' + partion_expr
    #                 Progress.emit(Progress.QUERY_GROUP_BY)
    #                 # Logging.info("Executing GROUP-BY query: " + sql)
    #             ret.append(SqlQuery(sql))

    #     return ret

    def formatTimediff(self, expr1):
        """
        Formats the timedifference between two expressions.

        Args:
            expr1 (str): The first expression.

        Returns:
            str: The formatted timedifference expression.

        """
        # 1b(纳秒), 1u(微秒)，1a(毫秒)，1s(秒)，1m(分)，1h(小时)，1d(天), 1w(周)
        time_unit = random.choice(DataBoundary.TIME_UNIT.value)
        expr2 = f'{expr1}+1{time_unit}'
        return f"TIMEDIFF({expr1}, {expr2})"

    def formatDiff(self, expr1):
        """
        Formats the difference between the given expression and a randomly chosen ignore value.

        Args:
            expr1 (str): The expression to calculate the difference for.

        Returns:
            str: The formatted difference string.

        """
        ignore_val = random.choice(DataBoundary.DIFF_IGNORE_BOUNDARY.value)
        if ignore_val == 0:
            return f"DIFF({expr1})"
        else:
            return f"DIFF({expr1}, {ignore_val})"

    def formatTimeTruncate(self, expr):
        """
        Formats the given expression by truncating the time to a specified unit.

        Args:
            expr (str): The expression to be formatted.

        Returns:
            str: The formatted expression using the TIMETRUNCATE function.
        """
        time_unit = random.choice(DataBoundary.TIME_UNIT.value[2:])
        use_current_timezone = random.choice(DataBoundary.TIMEZONE_BOUNDARY.value)
        return f'TIMETRUNCATE({expr}, 1{time_unit}, {use_current_timezone})'

    def formatHistogram(self, expr):
        """
        Formats the histogram expression.

        Args:
            expr (str): The expression to be formatted.

        Returns:
            str: The formatted histogram expression.

        """
        user_input1 = [f'HISTOGRAM({expr}, "user_input", "[1, 3, 5, 7]", {random.choice([0, 1])})']
        linear_bin  = [f'HISTOGRAM({expr}, "linear_bin", \'{{"start": 0.0, "width": 5.0, "count": 5, "infinity": true}}\', {random.choice(DataBoundary.HISTOGRAM_BOUNDARY.value)})']
        user_input2 = [f'HISTOGRAM({expr}, "user_input", \'{{"start":1.0, "factor": 2.0, "count": 5, "infinity": true}}\', {random.choice(DataBoundary.HISTOGRAM_BOUNDARY.value)})']
        funcList = user_input1 + linear_bin + user_input2
        return random.choice(funcList)

    def formatPercentile(self, expr):
        """
        Formats the given expression with random percentiles.

        Args:
            expr (str): The expression to be formatted.

        Returns:
            str: The formatted expression with random percentiles.
        """
        rcnt = random.randint(DataBoundary.PERCENTILE_BOUNDARY.value[0], DataBoundary.PERCENTILE_BOUNDARY.value[1])
        p = random.sample(range(DataBoundary.PERCENTILE_BOUNDARY.value[0], DataBoundary.PERCENTILE_BOUNDARY.value[1]*10), rcnt)
        return f'PERCENTILE({expr},{",".join(map(str, p))})'

    def formatStatecount(self, expr):
        """
        Formats the state count expression.

        Args:
            expr (str): The expression to be formatted.

        Returns:
            str: The formatted state count expression.

        """
        val = random.randint(DataBoundary.PERCENTILE_BOUNDARY.value[0], DataBoundary.PERCENTILE_BOUNDARY.value[1])
        oper = random.choice(DataBoundary.STATECOUNT_UNIT.value)
        return f'STATECOUNT({expr}, "{oper}", {val})'

    def formatStateduration(self, expr):
        """
        Formats the stateduration expression with random values.

        Args:
            expr (str): The expression to be formatted.

        Returns:
            str: The formatted stateduration expression.

        """
        val = random.randint(DataBoundary.PERCENTILE_BOUNDARY.value[0], DataBoundary.PERCENTILE_BOUNDARY.value[1])
        oper = random.choice(DataBoundary.STATECOUNT_UNIT.value)
        unit = random.choice(DataBoundary.TIME_UNIT.value)
        return f'STATEDURATION({expr}, "{oper}", {val}, 1{unit})'

    def formatConcat(self, expr, *args):
        """
        Formats and concatenates the given expression and arguments.

        Args:
            expr (str): The expression to be formatted and concatenated.
            *args: Variable number of arguments to be concatenated.

        Returns:
            str: The formatted and concatenated string.
        """
        base = f'CONCAT("pre_", cast({expr} as nchar({DataBoundary.CONCAT_BOUNDARY.value[1]})))'
        argsVals = list()
        for i in range(len(args[:DataBoundary.CONCAT_BOUNDARY.value[1]-1])):
            argsVals.append(f'cast({args[i]} as nchar({DataBoundary.CONCAT_BOUNDARY.value[1]}))')
        if len(argsVals) == 0:
            return base
        else:
            return f'CONCAT({base}, {", ".join(argsVals)})'

    def formatConcatWs(self, expr, *args):
        """
        Formats the given expression and arguments into a concatenated string using CONCAT_WS function.

        Args:
            expr (str): The expression to be formatted.
            *args (str): Variable number of arguments to be concatenated.

        Returns:
            str: The formatted concatenated string.

        Example:
            >>> formatConcatWs("column1", "column2", "column3")
            'CONCAT_WS(",", CONCAT_WS("_", "pre_", cast(column1 as nchar(10))), cast(column2 as nchar(10)), cast(column3 as nchar(10)))'
        """
        separator_expr = random.choice([",", ":", ";", "_", "-"])
        base = f'CONCAT_WS("{separator_expr}", "pre_", cast({expr} as nchar({DataBoundary.CONCAT_BOUNDARY.value[1]})))'
        argsVals = list()
        for i in range(len(args[:DataBoundary.CONCAT_BOUNDARY.value[1]-1])):
            argsVals.append(f'cast({args[i]} as nchar({DataBoundary.CONCAT_BOUNDARY.value[1]}))')
        if len(argsVals) == 0:
            return base
        else:
            return f'CONCAT_WS("{separator_expr}", {base}, {", ".join(argsVals)})'

    def formatSubstr(self, expr):
        """
        Formats the given expression as a SUBSTR function with random position and length.

        Args:
            expr (str): The expression to be formatted.

        Returns:
            str: The formatted SUBSTR function.

        """
        pos = random.choice(DataBoundary.SUBSTR_BOUNDARY.value)
        length = random.choice(DataBoundary.SUBSTR_BOUNDARY.value)
        return f'SUBSTR({expr}, {pos}, {length})'

    def formatCast(self, expr, castTypeList):
        """
        Formats the given expression with a random cast type from the provided list.

        Args:
            expr (str): The expression to be casted.
            castTypeList (list): A list of cast types to choose from.

        Returns:
            str: The formatted expression with a random cast type.

        """
        return f'CAST({expr} AS {random.choice(castTypeList)})'

    def formatFunc(self, func, colname, castTypeList="nchar", *args, **kwarg):
        """
        Format the function based on the given parameters.

        Args:
            func (str): The function name.
            colname (str): The column name.
            castTypeList (str, optional): The cast type list. Defaults to "nchar".
            *args: Variable length argument list.
            **kwarg: Arbitrary keyword arguments.

        Returns:
            str: The formatted function string.
        """
        if func in ['ABS', 'ACOS', 'ASIN', 'ATAN', 'CEIL', 'COS', 'FLOOR', 'LOG', 'ROUND', 'SIN', 'SQRT', 'TAN'] + \
                    ['AVG', 'COUNT', 'SPREAD', 'STDDEV', 'SUM', 'HYPERLOGLOG'] + \
                    ['FIRST', 'LAST', 'LAST_ROW', 'MAX', 'MIN', 'MODE', 'UNIQUE'] + \
                    ['CSUM', 'IRATE', 'TWA'] + \
                    ['CHAR_LENGTH', 'LENGTH', 'LOWER', 'LTRIM', 'RTRIM', 'UPPER', 'TO_JSON']:
            return f"{func}({colname})"
        elif func in ['NOW', 'TODAY', 'TIMEZONE', 'DATABASES', 'CLIENT_VERSION', 'SERVER_VERSION', 'SERVER_STATUS', 'CURRENT_USER']:
            return f"{func}()"
        elif func in ['TIMEDIFF']:
            return self.formatTimediff(colname)
        elif func in ['TIMEDIFF']:
            return self.formatDiff(colname)
        elif func in ['TIMETRUNCATE']:
            return self.formatTimeTruncate(colname)
        elif func in ['APERCENTILE']:
            return f'{func}({colname}, {random.randint(DataBoundary.PERCENTILE_BOUNDARY.value[0], DataBoundary.PERCENTILE_BOUNDARY.value[1])}, "{random.choice(["default", "t-digest"])}")'
        elif func in ['LEASTSQUARES']:
            return f"{func}({colname}, {random.randint(1, DataBoundary.LEASTSQUARES_BOUNDARY.value[1])}, {random.randint(1, DataBoundary.LEASTSQUARES_BOUNDARY.value[1])})"
        elif func in ['HISTOGRAM']:
            return self.formatHistogram(colname)
        elif func in ['PERCENTILE']:
            return self.formatPercentile(colname)
        elif func in ['ELAPSED']:
            return f"{func}({colname}, 1{random.choice(DataBoundary.TIME_UNIT.value)})"
        elif func in ['POW']:
            return f"{func}({colname}, {random.choice(DataBoundary.SAMPLE_BOUNDARY.value)})"
        elif func in ['INTERP', 'DIFF', 'TO_UNIXTIMESTAMP']:
            return f"{func}({colname}, {random.choice(DataBoundary.IGNORE_NEGATIVE_BOUNDARY.value)})"
        elif func in ['SAMPLE']:
            return f"{func}({colname}, {random.choice(DataBoundary.SAMPLE_BOUNDARY.value)})"
        elif func in ['TAIL']:
            return f"{func}({colname}, {random.choice(DataBoundary.TAIL_BOUNDARY.value)}, {random.choice(DataBoundary.TAIL_OFFSET_BOUNDARY.value)})"
        elif func in ['TOP', 'BOTTOM']:
            return f"{func}({colname}, {random.choice(DataBoundary.TOP_BOUNDARY.value)})"
        elif func in ['DERIVATIVE']:
            return f"{func}({colname}, 1{random.choice(DataBoundary.TIME_UNIT.value[3:])}, {random.choice(DataBoundary.IGNORE_NEGATIVE_BOUNDARY.value)})"
        elif func in ['MAVG']:
            return f"{func}({colname}, {random.choice(DataBoundary.MAVG_BOUNDARY.value)})"
        elif func in ['STATECOUNT']:
            return self.formatStatecount(colname)
        elif func in ['STATEDURATION']:
            return self.formatStateduration(colname)
        elif func in ['CONCAT']:
            return self.formatConcat(colname, *args)
        elif func in ['CONCAT_WS']:
            return self.formatConcatWs(colname, *args)
        elif func in ['SUBSTR']:
            return self.formatSubstr(colname, *args)
        elif func in ['CAST']:
            return self.formatCast(colname, castTypeList)
        elif func in ['TO_ISO8601']:
            timezone = random.choice(["+00:00", "-00:00", "+08:00", "-08:00", "+12:00", "-12:00"])
            return f'{func}({colname}, "{timezone}")'
        elif func in ['TO_CHAR', 'TO_TIMESTAMP']:
            return f'{func}({colname}, "{random.choice(DataBoundary.TO_CHAR_UNIT.value)}")'
        else:
            pass

    def getShowSql(self, dbname, tbname, ctbname):
        """
        Returns a SQL query for showing information about a database, table, or tag.

        Args:
            dbname (str): The name of the database.
            tbname (str): The name of the table.
            ctbname (str): The name of the child table.

        Returns:
            str: The SQL query for showing the requested information.
        """
        showSql = random.choice(DataBoundary.SHOW_UNIT.value)
        if "DISTRIBUTED" in showSql:
            return f'{showSql} {tbname};'
        elif "SHOW CREATE DATABASE" in showSql:
            return f'{showSql} {dbname};'
        elif "SHOW CREATE STABLE" in showSql:
            return f'{showSql} {tbname};'
        elif "SHOW CREATE TABLE" in showSql:
            return f'{showSql} {ctbname};'
        elif "SHOW TAGS" in showSql:
            return f'{showSql} {ctbname};'
        else:
            return f'{showSql};'

    def getSystableSql(self):
        '''
        sysdb = random.choice(DataBoundary.SYSTABLE_UNIT.value
        self.tdSql.query(f'show {sysdb}.tables')
        systableList = list(map(lambda x:x[0], self.tdSql.query_data))
        systable = random.choice(systableList)
        if systable == "ins_tables":
            return f'select * from {systable} limit({DataBoundary.LIMIT_BOUNDARY.value})'
        else:
            return f'select * from {systable}'
        '''
        return "select * from information_schema.ins_stables;"

    def getSlimitValue(self, rand=None):
        """
        Returns a slimit value.

        Parameters:
        - rand (bool): If True, a random slimit value will be generated. If False or None, an empty string will be returned.

        Returns:
        - str: The generated slimit value.

        """
        useTag = random.choice([True, False]) if rand is None else True
        if useTag:
            slimitValList = [f'SLIMIT {random.randint(1, DataBoundary.LIMIT_BOUNDARY.value)}', f'SLIMIT {random.randint(1, DataBoundary.LIMIT_BOUNDARY.value)}, {random.randint(1, DataBoundary.LIMIT_BOUNDARY.value)}']
            slimitVal = random.choice(slimitValList)
        else:
            slimitVal = ""
        return slimitVal

    def setGroupTag(self, fm, funcList):
        """
        Check if there are common elements between `funcList` and `selectFuncs` in `fm`.

        Args:
            fm (dict): The dictionary containing the 'selectFuncs' key.
            funcList (list): The list of functions to compare with 'selectFuncs'.

        Returns:
            bool: True if there are common elements, False otherwise.
        """
        selectFuncs = fm['selectFuncs']
        s1 = set(funcList)
        s2 = set(selectFuncs)
        common_elements = s1 & s2
        if len(common_elements) > 1:
            return True
        else:
            return False

    def selectFuncsFromType(self, fm, colname, column_type, doAggr, subquery=False, tsma=False):
        """
        Selects functions based on the given parameters.

        Args:
            fm (dict): A dictionary containing function categories as keys and lists of functions as values.
            colname (str): The name of the column.
            column_type (str): The type of the column.
            doAggr (int): An integer representing the type of aggregation to perform.

        Returns:
            tuple: A tuple containing the selected functions as a comma-separated string and the group key.

        """
        if doAggr == 0:
            categoryList = ['aggFuncs']
        elif doAggr == 1:
            categoryList = ['mathFuncs', 'strFuncs', 'timeFuncs', 'selectFuncs', 'castFuncs'] if not tsma else ['selectFuncs']
        elif doAggr == 2:
            categoryList = ['VariableFuncs']
        elif doAggr == 3:
            categoryList = ['specialFuncs']
        else:
            return
        funcList = list()
        # print("----categoryList", categoryList)
        # print("----fm", fm)

        for category in categoryList:
            funcList += fm[category]
        # print("----funcList", funcList)
        if subquery:
            funcList = [func for func in funcList if func not in fm['streamUnsupported']]
        if tsma:
            funcList = [func for func in funcList if func not in fm['tsmaUnsupported']]
        selectItems = random.sample(funcList, random.randint(1, len(funcList))) if len(funcList) > 0 else list()
        funcStrList = list()
        for func in selectItems:
            # print("----func", func)
            funcStr = self.formatFunc(func, colname, fm["castTypes"])
            # print("----funcStr", funcStr)
            funcStrList.append(funcStr)
        # print("-------funcStrList:", funcStrList)
        # print("-------funcStr:", ",".join(funcStrList))
        # print("----selectItems", selectItems)

        if "INT" in column_type:
            groupKey = colname if self.setGroupTag(fm, funcList) else ""
        else:
            groupKey = colname if self.setGroupTag(fm, funcList) else ""
        if doAggr == 2:
            if len(funcStrList) > 0:
                return ",".join([random.choice(funcStrList)]), groupKey
            else:
                return "", groupKey
        return ",".join(funcStrList), groupKey

    def getFuncCategory(self, doAggr):
        """
        Returns the category of the function based on the value of doAggr.

        Parameters:
        - doAggr (int): Determines the category of the function.
                        0 for aggregate functions, 1 for normal functions, and any other value for special functions.

        Returns:
        - str: The category of the function. Possible values are "aggFuncs", "nFuncs", or "spFuncs".
        """
        if doAggr == 0:
            return "aggFuncs"
        elif doAggr == 1:
            return "nFuncs"
        else:
            return "spFuncs"

    def getRandomTimeUnitStr(self, stream=False, tsma=False):
        """
        Generates a random time unit string.

        Returns:
            str: A string representing a random time unit.
        """
        if stream:
            return f'{random.randint(*DataBoundary.SAMPLE_BOUNDARY.value)}{random.choice(DataBoundary.TIME_UNIT.value[3:])}'
        else:
            if tsma:
                return f'{random.randint(*DataBoundary.SAMPLE_BOUNDARY.value)}{random.choice(DataBoundary.TIME_UNIT.value[4:6])}'
            else:
                return f'{random.randint(*DataBoundary.SAMPLE_BOUNDARY.value)}{random.choice(DataBoundary.TIME_UNIT.value)}'

    def sepTimeStr(self, timeStr):
        match = re.match(r"(\d+)(\D+)", timeStr)
        return int(match.group(1)), match.group(2)

    def getOffsetFromInterval(self, interval):
        _, unit = self.sepTimeStr(interval)
        idx = DataBoundary.TIME_UNIT.value.index(unit)
        unit = random.choice(DataBoundary.TIME_UNIT.value[0:idx]) if idx > 0 else random.choice(DataBoundary.TIME_UNIT.value[0:idx+1])
        return f'{random.randint(*DataBoundary.SAMPLE_BOUNDARY.value)}{unit}'


    def getRandomWindow(self):
        """
        Returns a random window from the available window units.

        Returns:
            str: A random window unit.
        """
        return random.choice(DataBoundary.WINDOW_UNIT.value)

    def getOffsetValue(self, rand=None):
        """
        Returns the offset value for a SQL query.

        Args:
            rand (bool, optional): If True, a random offset value will be used. If False, a time unit string will be used. Defaults to None.

        Returns:
            str: The offset value for the SQL query.
        """
        useTag = random.choice([True, False]) if rand is None else True
        offsetVal = f',{self.getRandomTimeUnitStr()}' if useTag else ""
        return offsetVal

    def getSlidingValue(self, rand=None):
        """
        Get the sliding value for the SQL query.

        Parameters:
        - rand (bool, optional): If True, use a random value for the sliding value. If False, use a predefined value. Defaults to None.

        Returns:
        - slidingVal (str): The sliding value for the SQL query.
        """
        useTag = random.choice([True, False]) if rand is None else True
        slidingVal = f'SLIDING({self.getRandomTimeUnitStr()})' if useTag else ""
        return slidingVal

    def getOrderByValue(self, col, rand=None):
        """
        Returns the ORDER BY clause for a given column.

        Args:
            col (str): The name of the column to order by.
            rand (bool, optional): If True, a random order type will be used. Defaults to None.

        Returns:
            str: The ORDER BY clause for the given column.
        """
        useTag = random.choice([True, False]) if rand is None else True
        orderType = random.choice(["ASC", "DESC", ""]) if useTag else ""
        orderVal = f'ORDER BY {col} {orderType}' if useTag else ""
        return orderVal

    def getFillValue(self, rand=None):
        """
        Returns a fill value for SQL queries.

        Parameters:
        - rand (bool, optional): If True, a random fill value will be used. If False, an empty string will be returned. Defaults to None.

        Returns:`
        - str: The fill value for SQL queries.
        """
        useTag = random.choice([True, False]) if rand is None else True
        fillVal = f'FILL({random.choice(DataBoundary.FILL_UNIT.value)})' if useTag else ""
        return fillVal

    def getTimeRange(self, tbname, tsCol):
        # self.tdSql.query(f"SELECT first({tsCol}), last({tsCol}) FROM {tbname} tail({tsCol}, 100, 50);")
        # res = self.tdSql.query_data
        res = [('2024-08-26 14:00:00.000',), ('2024-08-26 18:00:00.000',)]
        return [res[0][0], res[1][0]]

    def getTimeRangeFilter(self, tbname, tsCol, rand=None, doAggr=0):
        """
        Returns a time range filter based on the given table name and timestamp column.

        Parameters:
        - tbname (str): The name of the table.
        - tsCol (str): The name of the timestamp column.
        - rand (bool, optional): If True, a random time range filter will be generated. 
                                    If False, an empty string will be returned. 
                                    If None, a random choice will be made between generating a time range filter or returning an empty string. 
                                    Default is None.

        Returns:
        - timeRangeFilter (str): The generated time range filter.
        """
        useTag = random.choice([True, False]) if rand is None else True
        if useTag:
            start_time, end_time = self.getTimeRange(tbname, tsCol)
            timeRangeFilter = random.choice([f'WHERE {tsCol} BETWEEN "{start_time}" AND "{end_time}"', f'where {tsCol} > "{start_time}" AND {tsCol} < "{end_time}"'])
            if doAggr != 0:
                timeRangeFilter = random.choice([timeRangeFilter, "", "", "", ""])
        else:
            timeRangeFilter = ""
        return timeRangeFilter

    def getPartitionValue(self, col, rand=None):
        """
        Returns the partition value based on the given column.

        Args:
            col (str): The column to partition by.
            rand (bool, optional): If True, a random partition value will be used. Defaults to None.

        Returns:
            str: The partition value.

        """
        useTag = random.choice([True, False]) if rand is None else True
        partitionVal = f'PARTITION BY {col}' if useTag else ""
        return partitionVal

    def getPartitionObj(self, rand=None):
        """
        Returns a partition object based on a random choice.

        Args:
            rand (bool, optional): If True, always uses a random choice. If False, does not use a random choice. Defaults to None.

        Returns:
            str: The partition object. If `useTag` is True, returns a string in the format 'PARTITION BY {random_choice}'. If `useTag` is False, returns an empty string.
        """
        useTag = random.choice([True, False]) if rand is None else True
        partitionObj = f'PARTITION BY {random.choice(DataBoundary.ALL_TYPE_UNIT.value)}' if useTag else ""
        return partitionObj

    def getEventWindowCondition(self, colDict):
        """
        Generates a random event window condition based on the column dictionary provided.

        Args:
            colDict (dict): A dictionary containing column names as keys and column types as values.

        Returns:
            str: A randomly generated event window condition.

        """
        conditionList = list()
        lteList = ["<", "<="]
        gteList = [">", ">="]
        enqList = ["=", "!=", "<>"]
        nullList = ["is null", "is not null"]
        inList = ["in", "not in"]
        betweenList = ["between", "not between"]
        likeList = ["like", "not like"]
        matchList = ["match", "nmatch"]
        tinyintRangeList = DataBoundary.TINYINT_BOUNDARY.value
        intHalfBf = random.randint(tinyintRangeList[0], round((tinyintRangeList[1]+tinyintRangeList[0])/2))
        intHalfAf = random.randint(round((tinyintRangeList[1]+tinyintRangeList[0])/2), tinyintRangeList[1])
        for columnName, columnType in colDict.items():
            if columnType in ['TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'TINYINT UNSIGNED', 'SMALLINT UNSIGNED', 'INT UNSIGNED', 'BIGINT UNSIGNED', ]:
                startTriggerCondition = f'{columnName} {inList[0]} {tuple(random.randint(0, 100) for _ in range(10))} or {columnName} {betweenList[0]} {intHalfBf} and {intHalfAf}'
                endTriggerCondition = f'{columnName} {inList[1]} {tuple(random.randint(0, 100) for _ in range(10))} and {columnName} {betweenList[1]} {intHalfBf} and {intHalfAf}'
            elif columnType in ['FLOAT', 'DOUBLE']:
                startTriggerCondition = f'{columnName} {random.choice(lteList)} {intHalfBf}'
                endTriggerCondition = f'{columnName} {random.choice(gteList)} {intHalfAf}'
            elif columnType in ['BINARY', 'VARCHAR', 'NCHAR']:
                startTriggerCondition = f'{columnName} {likeList[0]} "%a%" or {columnName} {matchList[1]} ".*a.*"'
                endTriggerCondition = f'{columnName} {likeList[1]} "_a_" and {columnName} {matchList[0]} ".*a.*"'
            else:
                startTriggerCondition = f'{columnName} {random.choice(enqList)} {intHalfBf} or {columnName} {nullList[0]}'
                endTriggerCondition = f'{columnName} {nullList[1]} and {columnName} {random.choice(enqList[1:])} {intHalfAf}'
            conditionList.append(f'EVENT_WINDOW start with {startTriggerCondition} end with {endTriggerCondition}')
        return random.choice(conditionList)

    def getWindowStr(self, window, colDict, tsCol="ts", stateUnit="1", countUnit="2", stream=False, tsma=False):
        """
        Returns a string representation of the window based on the given parameters.

        Parameters:
        - window (str): The type of window. Possible values are "INTERVAL", "SESSION", "STATE_WINDOW", "COUNT_WINDOW", and "EVENT_WINDOW".
        - colDict (dict): A dictionary containing column names and their corresponding values.
        - tsCol (str): The name of the timestamp column. Default is "ts".
        - stateUnit (str): The unit for the state window. Default is "1".
        - countUnit (str): The unit for the count window. Default is "2".

        Returns:
        - str: A string representation of the window.

        """
        if window == "INTERVAL":
            interval = self.getRandomTimeUnitStr(stream=stream, tsma=tsma)
            offset = f',{self.getOffsetFromInterval(interval)}' if not tsma else ""
            return f"{window}({interval}{offset})"
        elif window == "SESSION":
            return f"{window}({tsCol}, {self.getRandomTimeUnitStr()})"
        elif window == "STATE_WINDOW":
            return f"{window}({stateUnit})"
        elif window == "COUNT_WINDOW":
            return f"{window}({countUnit})"
        elif window == "EVENT_WINDOW":
            return self.getEventWindowCondition(colDict)
        else:
            return ""

    def generateRandomSql(self, colDict, tbname):
        """
        Generates a random SQL query based on the given column dictionary and table name.

        Args:
            colDict (dict): A dictionary containing column names as keys and column types as values.
            tbname (str): The name of the table.

        Returns:
            str: The generated SQL query.

        Raises:
            None
        """
        selectPartList = []
        groupKeyList = []
        colTypes = [member.name for member in FunctionMap]
        # print("-----colDict", colDict)
        # print("-----colTypes", colTypes)
        doAggr = random.choice([0, 1, 2, 3, 4, 5])
        if doAggr == 4:
            return self.getShowSql("test", "stb", "ctb1")
        if doAggr == 5:
            return self.getSystableSql()
        tsCol = "ts"
        for column_name, column_type in colDict.items():
            if column_type == "TIMESTAMP":
                tsCol = column_name
            for fm in FunctionMap:
                if column_type in fm.value['types']:
                    selectStrs, groupKey = self.selectFuncsFromType(fm.value, column_name, column_type, doAggr)
                    # print("-----selectStrs", selectStrs)
                    if len(selectStrs) > 0:
                        selectPartList.append(selectStrs)
                    # print("-----selectPartList", selectPartList)
                    if len(groupKey) > 0:
                        groupKeyList.append(f'`{groupKey}`')
        if doAggr == 2:
            selectPartList = [random.choice(selectPartList)] if len(selectPartList) > 0 else ["count(*)"]
        if len(groupKeyList) > 0:
            groupKeyStr = ",".join(groupKeyList)
            return f"SELECT {', '.join(selectPartList)} FROM {self._dbName}.{tbname} GROUP BY {groupKeyStr} {self.getOrderByValue(groupKeyStr)} {self.getSlimitValue()};"
        else:
            groupKeyStr = "tbname"
        partitionGroupPart = random.choice(selectPartList) if len(selectPartList) > 0 else groupKeyStr
        partitionGroupPartStr = self.formatSelectPartStr(partitionGroupPart)
        return f"SELECT {', '.join(selectPartList)} FROM {self._dbName}.{tbname} {self.getTimeRangeFilter(tbname, tsCol)} {self.getPartitionValue(groupKeyStr)} {self.getWindowStr(self.getRandomWindow(), colDict)} {self.getSlidingValue()} {self.getFillValue()} {self.getOrderByValue(partitionGroupPartStr)} {self.getSlimitValue()};"

    def remove_duplicates(self, selectPartStr):
        parts = selectPartStr.split(',')
        seen_now = seen_today = seen_timezone = False
        result = []
        for part in parts:
            part_stripped = part.strip()
            if part_stripped == "NOW()":
                if not seen_now:
                    seen_now = True
                    result.append(part)
            elif part_stripped == "TODAY()":
                if not seen_today:
                    seen_today = True
                    result.append(part)
            elif part_stripped == "TIMEZONE()":
                if not seen_timezone:
                    seen_timezone = True
                    result.append(part)
            else:
                result.append(part)
        return ', '.join(result)

    # def generateRandomSubQuery(self, colDict, tbname, subtable=False, doAggr=0):
    #     selectPartList = []
    #     groupKeyList = []
    #     # colTypes = [member.name for member in FunctionMap]
    #     doAggr = random.choice([0, 1, 2, 3])
    #     tsCol = "ts"
    #     for column_name, column_type in colDict.items():
    #         if column_type == "TIMESTAMP":
    #             tsCol = column_name
    #         for fm in FunctionMap:
    #             if column_type in fm.value['types']:
    #                 selectStrs, groupKey = self.selectFuncsFromType(fm.value, column_name, column_type, doAggr)
    #                 if len(selectStrs) > 0:
    #                     selectPartList.append(selectStrs)
    #                 if len(groupKey) > 0:
    #                     groupKeyList.append(groupKey)

    #     if doAggr == 2:
    #         selectPartList = [random.choice(selectPartList)] if len(selectPartList) > 0 else ["count(*)"]
    #     if doAggr == 1:
    #         selectPartList = [tsCol] + selectPartList
    #     selectPartStr = ', '.join(selectPartList)
    #     selectPartStr = self.remove_duplicates(selectPartStr)
    #     if len(groupKeyList) > 0:
    #         groupKeyStr = ",".join(groupKeyList)
    #         partitionVal = self.getPartitionValue(groupKeyStr)
    #         if subtable:
    #             partitionVal = f'{partitionVal},tbname' if len(partitionVal) > 0 else "partition by tbname"
    #         return f"SELECT {selectPartStr} FROM {self._dbName}.{tbname} {partitionVal} GROUP BY {groupKeyStr} {self.getSlimitValue()};"
    #     else:
    #         groupKeyStr = "tbname"
    #         partitionVal = "partition by tbname" if subtable else ""
    #     windowStr = self.getWindowStr(self.getRandomWindow(), colDict, stream=True)
    #     if ("COUNT_WINDOW" in windowStr or "STATE_WINDOW" in windowStr or "EVENT_WINDOW" in windowStr) and "partition" not in partitionVal:
    #         windowStr = f"partition by tbname {windowStr}"
    #     # randomSelectPart = f'`{random.choice(selectPartList)}`' if len(selectPartList) > 0 else groupKeyStr
    #     return f"SELECT {selectPartStr} FROM {self._dbName}.{tbname} {self.getTimeRangeFilter(tbname, tsCol, doAggr=doAggr)} {partitionVal} {windowStr};"


    def generateQueries_n(self, dbc: DbConn, selectItems) -> List[SqlQuery]:
        '''
        Generate queries to test/exercise this super table

        Args:
            dbc (DbConn): The database connection object.
            selectItems: The select items for the SQL query.

        Returns:
            List[SqlQuery]: A list of generated SQL queries.
        '''
        ret = []  # type: List[SqlQuery]

        for rTbName in self.getRegTables(dbc):  # regular tables
            filterExpr = Dice.choice([  # TODO: add various kind of WHERE conditions
                None
            ])
            sql = self.generateRandomSql(selectItems, rTbName)
            ret.append(SqlQuery(sql))

        return ret

class TaskReadData(StateTransitionTask):
    maxSelectItems = 5
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
        if random.randrange(20) == 0:  # and self._canRestartService():  # TODO: break connection in all situations
            # Logging.info("Attempting to reconnect to server") # TODO: change to DEBUG
            Progress.emit(Progress.SERVICE_RECONNECT_START)
            try:
                wt.getDbConn().close()
                wt.getDbConn().open()
            except ConnectionError as err:  # may fail
                if not gSvcMgr:
                    Logging.error("Failed to reconnect in client-only mode")
                    raise  # Not OK if we are running in client-only mode
                if gSvcMgr.isRunning():  # may have race conditon, but low prob, due to
                    Logging.error("Failed to reconnect when managed server is running")
                    raise  # Not OK if we are running normally

                Progress.emit(Progress.SERVICE_RECONNECT_FAILURE)
                # Logging.info("Ignoring DB reconnect error")

            # print("_r", end="", flush=True)
            Progress.emit(Progress.SERVICE_RECONNECT_SUCCESS)
            # The above might have taken a lot of time, service might be running
            # by now, causing error below to be incorrectly handled due to timing issue
            return  # TODO: fix server restart status race condtion

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        self._reconnectIfNeeded(wt)

        dbc = wt.getDbConn()
        sTable = self._db.getFixedSuperTable()
        tags = sTable._getTags(dbc)
        cols = sTable._getCols(dbc)
        # if not tags or not cols:
        #     return "no table exists"
        tagCols = {**tags, **cols}
        selectCnt = random.randint(1, len(tagCols))
        selectKeys = random.sample(list(tagCols.keys()), selectCnt)
        selectItems = {key: tagCols[key] for key in selectKeys[:self.maxSelectItems]}

        for q in sTable.generateQueries_n(dbc, selectItems):  # regular tables
            try:
                sql = q.getSql()
                # if 'GROUP BY' in sql:
                #     Logging.info("Executing GROUP-BY query: " + sql)
                dbc.execute(sql)
            except taos.error.ProgrammingError as err:
                errno2 = Helper.convertErrno(err.errno)
                Logging.debug("[=] Read Failure: errno=0x{:X}, msg: {}, SQL: {}".format(errno2, err, dbc.getLastSql()))
                self.logError(f"func TaskReadData error: {sql}")
                raise

class SqlQuery:
    @classmethod
    def buildRandom(cls, db: Database):
        '''Build a random query against a certain database'''

        dbName = db.getName()

    def __init__(self, sql: str = None):
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

        # # Drop the super table itself
        # tblName = self._db.getFixedSuperTableName()
        # self.execWtSql(wt, "drop table {}.{}".format(self._db.getName(), tblName))


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
        return False  # don't run this otherwise

    CHANCE_TO_RESTART_SERVICE = 200

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        if not Config.getConfig().auto_start_service:  # only execute when we are in -a mode
            print("_a", end="", flush=True)
            return

        with self._classLock:
            if self._isRunning:
                Logging.info("Skipping restart task, another running already")
                return
            self._isRunning = True

        if Dice.throw(self.CHANCE_TO_RESTART_SERVICE) == 0:  # 1 in N chance
            dbc = wt.getDbConn()
            dbc.execute(
                "select * from information_schema.ins_databases")  # simple delay, align timing with other workers
            gSvcMgr.restart()

        self._isRunning = False


class TaskAddData(StateTransitionTask):
    # Track which table is being actively worked on
    activeTable: Set[int] = set()

    # We use these two files to record operations to DB, useful for power-off tests
    fAddLogReady = None  # type: Optional[io.TextIOWrapper]
    fAddLogDone = None  # type: Optional[io.TextIOWrapper]
    smlSupportTypeList = ['TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'TINYINT UNSIGNED', 'SMALLINT UNSIGNED', 'INT UNSIGNED', 'BIGINT UNSIGNED', 'FLOAT', "DOUBLE", "VARCHAR", "NCHAR", "BOOL"]

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

    # def __init__(self, db: Database, dbc, regTableName):
    #     self.cols = self._getCols(db, dbc, regTableName)

    def _lockTableIfNeeded(self, fullTableName, extraMsg=''):
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
            self._unlockTableIfNeeded(fullTableName)

    def _addDataInBatch_n(self, db, dbc, regTableName, te: TaskExecutor):
        """
        Adds data in batch to the specified table.

        Args:
            db (Database): The database object.
            dbc (DatabaseConnection): The database connection object.
            regTableName (str): The name of the regular table.
            te (TaskExecutor): The task executor object.

        Returns:
            None
        """
        numRecords = self.LARGE_NUMBER_OF_RECORDS if Config.getConfig().larger_data else self.SMALL_NUMBER_OF_RECORDS

        fullTableName = db.getName() + '.' + regTableName
        self._lockTableIfNeeded(fullTableName, 'batch')
        sql = "INSERT INTO {} VALUES ".format(fullTableName)
        for j in range(numRecords):  # number of records per table
            colStrs = self._getTagColStrForSql(db, dbc)[0]
            # nextInt = db.getNextInt()
            # nextTick = db.getNextTick()
            # nextColor = db.getNextColor()
            sql += "({})".format(colStrs)

        # Logging.info("Adding data in batch: {}".format(sql))
        try:
            dbc.execute(sql)
        except taos.error.ProgrammingError as err:
            errno2 = Helper.convertErrno(err.errno)
            self.logError(f"func _addDataInBatch_n error: {errno2}-{sql}")
            raise
        finally:
            self._unlockTableIfNeeded(fullTableName)

    def _checkStableExists(self, dbc, table):
        """
        Check if a table exists in the stable list of a database.

        Args:
            dbc (DatabaseConnection): The database connection object.
            table (str): The name of the table to check.

        Returns:
            bool: True if the table exists in the stable list, False otherwise.
        """
        dbc.query("show {}.stables".format(self._dbName))
        stCols = dbc.getQueryResult()
        tables = [row[0] for row in stCols]
        return table in tables

    def _getCols(self, db: Database, dbc, regTableName):
        """
        Get the columns of a table in the database.

        Args:
            db (Database): The database object.
            dbc: The database connection object.
            regTableName (str): The name of the table.

        Returns:
            dict: A dictionary containing the columns of the table, where the keys are the column names and the values are the column types.
        """
        # if self._checkStableExists(dbc, regTableName):
        dbc.query("DESCRIBE {}.{}".format(db.getName(), regTableName))
        stCols = dbc.getQueryResult()
        ret = {row[0]: row[1] for row in stCols if row[3] != 'TAG'}  # name:type
        return ret
        # else:
        #     return False

    def getMeta(self, db: Database, dbc: DbConn, customStable=None):
        """
        Retrieves metadata information from the specified database and table.

        Args:
            db (Database): The database object.
            dbc (DbConn): The database connection object.
            customStable (str, optional): The name of the table. Defaults to None.

        Returns:
            tuple: A tuple containing two dictionaries. The first dictionary contains column names as keys and their corresponding data types as values. The second dictionary contains tag names as keys and their corresponding data types as values.
        """
        stableName = self._getStableName(db, customStable)
        dbc.query("DESCRIBE {}.{}".format(db.getName(), stableName))
        sts = dbc.getQueryResult()
        cols = {row[0]: row[1] for row in sts if row[3] != 'TAG'}
        tags = {row[0]: row[1] for row in sts if row[3] == 'TAG'}
        return cols, tags

    def _getVals(self, db: Database, dbc: DbConn, tagNameList, customStable=None):
        """
        Retrieves random values from the specified database and table.

        Args:
            db (Database): The database object.
            dbc (DbConn): The database connection object.
            tagNameList (list): A list of tag names to select from.
            customStable (str, optional): The name of the stable table. Defaults to None.

        Returns:
            object: A randomly selected row from the specified table.
        """
        stableName = self._getStableName(db, customStable)
        selectedTagCols = ",".join(tagNameList)
        # if self._checkStableExists(dbc, stableName):
        dbc.query(f'select {selectedTagCols} from {db.getName()}.{stableName}')
        sts = dbc.getQueryResult()
        return random.choice(sts)
        # else:
        #     return False

    def _getStableName(self, db: Database, customStable=None):
        """
        Get the stable name from the database.

        Args:
            db (Database): The database object.
            customStable (str, optional): Custom stable name. Defaults to None.

        Returns:
            str: The stable name.
        """
        sTable = db.getFixedSuperTable().getName() if customStable is None else customStable
        return sTable

    def _getRandomCols(self, db: Database, dbc: DbConn):
        """
        Randomly selects a specified number of columns from a database table.

        Args:
            db (Database): The database object.
            dbc (DbConn): The database connection object.

        Returns:
            tuple: A tuple containing two elements:
                - A string representing the randomly selected columns separated by commas.
                - A dictionary representing the selected columns and their corresponding data types.
        Raises:
            None

        """
        cols = self.getMeta(db, dbc)[0]
        # if not cols:
        #     return "No table exists"
        n = random.randint(1, len(cols))
        timestampKey = next((key for key, value in cols.items() if value == 'TIMESTAMP'), None)
        if timestampKey:
            otherKeys = [key for key in cols if key != timestampKey]
            selected_keys = random.sample(otherKeys, n-1)
            random_keys = [timestampKey] + selected_keys
            selectedTagCols = {timestampKey: cols[timestampKey]}
            selectedTagCols.update({key: cols[key] for key in selected_keys})
            return ",".join(random_keys), selectedTagCols
        else:
            return "No TIMESTAMP type key found in the dictionary."

    def _getRandomTags(self, db: Database, dbc: DbConn):
        """
        Get random tags from the database.

        Args:
            db (Database): The database object.
            dbc (DbConn): The database connection object.

        Returns:
            tuple: A tuple containing a string of random tags separated by commas, and a dictionary of the selected tags.
        """
        tags = self.getMeta(db, dbc)[1]
        # if not tags:
        #     return "No table exists"
        n = random.randint(1, len(tags))
        random_keys = random.sample(list(tags.keys()), n)
        return ",".join(random_keys), {key: tags[key] for key in random_keys}

    def _getTagColStrForSql(self, db: Database, dbc, customTagCols=None, default="cols"):
        """
        Get the tag column string for SQL query.

        Args:
            db (Database): The database object.
            dbc: The database connection object.
            customTagCols (dict, optional): Custom tag columns. Defaults to None.
            default (str, optional): Default value for meta index. Defaults to "cols".

        Returns:
            tuple: A tuple containing the tag column string and the tag column values.
        """
        meta_idx = 0 if default == "cols" else 1
        tagCols = self.getMeta(db, dbc)[meta_idx] if not customTagCols else customTagCols
        # if not tagCols:
        #     return "No table exists"
        # self.cols = self._getCols(db, dbc, regTableName)
        # print("-----tagCols:",tagCols)
        tagColStrs = []
        record_str_idx_lst = list()
        start_idx = 0
        for tagColName in tagCols:
            tagColType = tagCols[tagColName]
            if tagColType == 'TIMESTAMP':
                nextTick = db.getNextTick()
                tagColStrs.append(nextTick)
                record_str_idx_lst.append(start_idx)
            elif tagColType == 'BINARY':
                tagColStrs.append("'Beijing-Shanghai-LosAngeles'")
                record_str_idx_lst.append(start_idx)
            elif tagColType == 'VARCHAR' or tagColType == 'VARBINARY' or tagColType == 'NCHAR':
                tagColStrs.append(TDCom.get_long_name(16))
                record_str_idx_lst.append(start_idx)
            elif tagColType == 'GEOMETRY':
                tagColStrs.append(random.choice(DataBoundary.GEOMETRY_BOUNDARY.value))
                record_str_idx_lst.append(start_idx)
            elif tagColType == 'TINYINT':
                tagColStrs.append(random.randint(DataBoundary.TINYINT_BOUNDARY.value[0], DataBoundary.TINYINT_BOUNDARY.value[1]))
            elif tagColType == 'SMALLINT':
                tagColStrs.append(random.randint(DataBoundary.SMALLINT_BOUNDARY.value[0], DataBoundary.SMALLINT_BOUNDARY.value[1]))
            elif tagColType == 'INT':
                tagColStrs.append(random.randint(DataBoundary.INT_BOUNDARY.value[0], DataBoundary.INT_BOUNDARY.value[1]))
            elif tagColType == 'BIGINT':
                tagColStrs.append(random.randint(DataBoundary.BIGINT_BOUNDARY.value[0], DataBoundary.BIGINT_BOUNDARY.value[1]))
            elif tagColType == 'TINYINT UNSIGNED':
                tagColStrs.append(random.randint(DataBoundary.UTINYINT_BOUNDARY.value[0], DataBoundary.UTINYINT_BOUNDARY.value[1]))
            elif tagColType == 'SMALLINT UNSIGNED':
                tagColStrs.append(random.randint(DataBoundary.USMALLINT_BOUNDARY.value[0], DataBoundary.USMALLINT_BOUNDARY.value[1]))
            elif tagColType == 'INT UNSIGNED':
                tagColStrs.append(random.randint(DataBoundary.UINT_BOUNDARY.value[0], DataBoundary.UINT_BOUNDARY.value[1]))
            elif tagColType == 'BIGINT UNSIGNED':
                tagColStrs.append(random.randint(DataBoundary.UBIGINT_BOUNDARY.value[0], DataBoundary.UBIGINT_BOUNDARY.value[1]))
            elif tagColType == 'FLOAT':
                tagColStrs.append(random.uniform(DataBoundary.FLOAT_BOUNDARY.value[0], DataBoundary.FLOAT_BOUNDARY.value[1]))
            elif tagColType == 'DOUBLE':
                getcontext().prec = 50
                low = Decimal(DataBoundary.DOUBLE_BOUNDARY.value[0])
                high = Decimal(DataBoundary.DOUBLE_BOUNDARY.value[1])
                random_decimal = low + (high - low) * Decimal(random.random())
                tagColStrs.append(float(random_decimal))
            elif tagColType == 'BOOL':
                tagColStrs.append(random.choice(DataBoundary.BOOL_BOUNDARY.value))
            else:
                raise RuntimeError("Unexpected col type: {}".format(tagColType))
            start_idx += 1
        tagColStrs_to_string_list = list(map(lambda x:str(x), tagColStrs))
        trans_tagColStrs_to_string_list = list(map(lambda x: f'"{x[1]}"' if x[0] in record_str_idx_lst else x[1], enumerate(tagColStrs_to_string_list)))
        return ", ".join(trans_tagColStrs_to_string_list), tagColStrs

    def _addData(self, db: Database, dbc, regTableName, te: TaskExecutor):  # implied: NOT in batches
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
            self._lockTableIfNeeded(
                fullTableName)  # so that we are verify read-back. TODO: deal with exceptions before unlock

            try:
                sql = "INSERT INTO {} VALUES ('{}', {}, '{}');".format(  # removed: tags ('{}', {})
                    fullTableName,
                    # ds.getFixedSuperTableName(),
                    # ds.getNextBinary(), ds.getNextFloat(),
                    nextTick, intToWrite, nextColor)
                # Logging.info("Adding data: {}".format(sql))
                dbc.execute(sql)
                intWrote = intToWrite

                # Quick hack, attach an update statement here. TODO: create an "update" task
                if (not Config.getConfig().use_shadow_db) and Dice.throw(
                        5) == 0:  # 1 in N chance, plus not using shaddow DB
                    intToUpdate = db.getNextInt()  # Updated， but should not succeed
                    nextColor = db.getNextColor()
                    sql = "INSERt INTO {} VALUES ('{}', {}, '{}');".format(  # "INSERt" means "update" here
                        fullTableName,
                        nextTick, intToUpdate, nextColor)
                    # sql = "UPDATE {} set speed={}, color='{}' WHERE ts='{}'".format(
                    #     fullTableName, db.getNextInt(), db.getNextColor(), nextTick)
                    dbc.execute(sql)
                    intWrote = intToUpdate  # We updated, seems TDengine non-cluster accepts this.

            except:  # Any exception at all
                self._unlockTableIfNeeded(fullTableName)
                raise CrashGenError("func _addData error")

            # Now read it back and verify, we might encounter an error if table is dropped
            if Config.getConfig().verify_data:  # only if command line asks for it
                try:
                    readBack = dbc.queryScalar("SELECT speed from {}.{} WHERE ts='{}'".
                                               format(db.getName(), regTableName, nextTick))
                    if readBack != intWrote:
                        raise taos.error.ProgrammingError(
                            "Failed to read back same data, wrote: {}, read: {}"
                            .format(intWrote, readBack), 0x999)
                except taos.error.ProgrammingError as err:
                    errno = Helper.convertErrno(err.errno)
                    if errno == CrashGenError.INVALID_EMPTY_RESULT:  # empty result
                        raise taos.error.ProgrammingError(
                            "Failed to read back same data for tick: {}, wrote: {}, read: EMPTY"
                            .format(nextTick, intWrote),
                            errno)
                    elif errno == CrashGenError.INVALID_MULTIPLE_RESULT:  # multiple results
                        raise taos.error.ProgrammingError(
                            "Failed to read back same data for tick: {}, wrote: {}, read: MULTIPLE RESULTS"
                            .format(nextTick, intWrote),
                            errno)
                    elif errno in [0x218, 0x362]:  # table doesn't exist
                        # do nothing
                        pass
                    else:
                        # Re-throw otherwise
                        raise CrashGenError("func _addData error")
                finally:
                    self._unlockTableIfNeeded(fullTableName)  # Quite ugly, refactor lock/unlock
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

    def _addData_n(self, db: Database, dbc, regTableName, te: TaskExecutor):  # implied: NOT in batches
        """
        Adds data to the specified table in the database.

        Args:
            db (Database): The database object.
            dbc: The database connection object.
            regTableName (str): The name of the table to add data to.
            te (TaskExecutor): The task executor object.

        Raises:
            CrashGenError: If an unexpected error occurs during the data addition process.

        Returns:
            None
        """
        numRecords = self.LARGE_NUMBER_OF_RECORDS if Config.getConfig().larger_data else self.SMALL_NUMBER_OF_RECORDS
        for j in range(numRecords):  # number of records per table
            colStrs = self._getTagColStrForSql(db, dbc)[0]
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
            self._lockTableIfNeeded(
                fullTableName)  # so that we are verify read-back. TODO: deal with exceptions before unlock

            try:
                # sql = "INSERT INTO {} VALUES ('{}', {}, '{}');".format(  # removed: tags ('{}', {})
                #     fullTableName,
                #     # ds.getFixedSuperTableName(),
                #     # ds.getNextBinary(), ds.getNextFloat(),
                #     nextTick, intToWrite, nextColor)
                sql = "INSERT INTO {} VALUES ({});".format(  # removed: tags ('{}', {})
                    fullTableName,
                    # ds.getFixedSuperTableName(),
                    # ds.getNextBinary(), ds.getNextFloat(),
                    colStrs)
                dbc.execute(sql)
                intWrote = intToWrite

                # Quick hack, attach an update statement here. TODO: create an "update" task
                if (not Config.getConfig().use_shadow_db) and Dice.throw(
                        5) == 0:  # 1 in N chance, plus not using shaddow DB
                    intToUpdate = db.getNextInt()  # Updated， but should not succeed
                    nextColor = db.getNextColor()
                    sql = "INSERt INTO {} VALUES ({});".format(  # "INSERt" means "update" here
                        fullTableName,
                        colStrs)
                    # sql = "UPDATE {} set speed={}, color='{}' WHERE ts='{}'".format(
                    #     fullTableName, db.getNextInt(), db.getNextColor(), nextTick)
                    dbc.execute(sql)
                    intWrote = intToUpdate  # We updated, seems TDengine non-cluster accepts this.

            except Exception as e:  # Any exception at all
                self.logError(f"func _addData_n error: {sql}-{e}")
                self._unlockTableIfNeeded(fullTableName)
                raise

            # Now read it back and verify, we might encounter an error if table is dropped
            if Config.getConfig().verify_data:  # only if command line asks for it
                try:
                    readBack = dbc.queryScalar("SELECT speed from {}.{} WHERE ts='{}'".
                                               format(db.getName(), regTableName, nextTick))
                    if readBack != intWrote:
                        raise taos.error.ProgrammingError(
                            "Failed to read back same data, wrote: {}, read: {}"
                            .format(intWrote, readBack), 0x999)
                except taos.error.ProgrammingError as err:
                    errno = Helper.convertErrno(err.errno)
                    if errno == CrashGenError.INVALID_EMPTY_RESULT:  # empty result
                        raise taos.error.ProgrammingError(
                            "Failed to read back same data for tick: {}, wrote: {}, read: EMPTY"
                            .format(nextTick, intWrote),
                            errno)
                    elif errno == CrashGenError.INVALID_MULTIPLE_RESULT:  # multiple results
                        raise taos.error.ProgrammingError(
                            "Failed to read back same data for tick: {}, wrote: {}, read: MULTIPLE RESULTS"
                            .format(nextTick, intWrote),
                            errno)
                    elif errno in [0x218, 0x362]:  # table doesn't exist
                        # do nothing
                        pass
                    else:
                        # Re-throw otherwise
                        raise CrashGenError("func _addData error")
                finally:
                    self._unlockTableIfNeeded(fullTableName)  # Quite ugly, refactor lock/unlock
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

    def _addDataByAutoCreateTable_n(self, db: Database, dbc):
        """
        Adds data to the database by automatically creating tables.

        Args:
            db (Database): The database object.
            dbc: The database connection object.

        Raises:
            CrashGenError: If an error occurs while adding data.

        Returns:
            None
        """
        numRecords = self.LARGE_NUMBER_OF_RECORDS if Config.getConfig().larger_data else self.SMALL_NUMBER_OF_RECORDS
        for j in range(numRecords):  # number of records per table
            colNames, colValues = self._getRandomCols(db, dbc)
            tagNames, tagValues = self._getRandomTags(db, dbc)
            colStrs = self._getTagColStrForSql(db, dbc, colValues)[0]
            tagStrs = self._getTagColStrForSql(db, dbc, tagValues)[0]
            regTableName = self.getRegTableName(j)  # "db.reg_table_{}".format(i
            fullStableName = db.getName() + '.' + self._getStableName(db)
            fullRegTableName = db.getName() + '.' + regTableName
            self._lockTableIfNeeded(
                fullRegTableName)  # so that we are verify read-back. TODO: deal with exceptions before unlock

            try:
                sql = "INSERT INTO {} using {}({}) TAGS({}) ({}) VALUES ({});".format(  # removed: tags ('{}', {})
                    fullRegTableName,
                    fullStableName,
                    tagNames,
                    tagStrs,
                    colNames,
                    colStrs)
                dbc.execute(sql)
            except Exception as e:  # Any exception at all
                self.logError(f"func _addDataByAutoCreateTable_n error: {sql}-{e}")
                self._unlockTableIfNeeded(fullRegTableName)
                raise

    def _addDataByMultiTable_n(self, db: Database, dbc):
        """
        Adds data to multiple tables in the database.

        Args:
            db (Database): The database object.
            dbc: The database connection object.

        Returns:
            None

        Raises:
            CrashGenError: If an exception occurs while adding data.
        """
        if dbc.query("show {}.tables".format(db.getName())) == 0:  # no tables
            return
        #self.tdSql.execute(f'insert into {dbname}.tb3 using {dbname}.stb3 tags (31, 32) values (now, 31, 32) {dbname}.tb4 using {dbname}.stb4 tags (41, 42) values (now, 41, 42)')
        # insert into tb1 values (now, 11, 12) tb2 values (now, 21, 22);
        res = dbc.getQueryResult()
        regTables = list(map(lambda x:x[0], res[0:self.SMALL_NUMBER_OF_RECORDS]))
        sql = "INSERT INTO"
        for i in range(len(regTables)):
            colStrs = self._getTagColStrForSql(db, dbc)[0]
            regTableName = regTables[i]  # "db.reg_table_{}".format(i
            fullRegTableName = db.getName() + '.' + regTableName
            self._lockTableIfNeeded(
                fullRegTableName)  # so that we are verify read-back. TODO: deal with exceptions before unlock
            # TODO multi stb insert
            sql += " {} VALUES ({})".format(
                fullRegTableName,
                colStrs)
        try:
            # Logging.info("Adding data: {}".format(sql))
            dbc.execute(sql)
        except Exception as e:  # Any exception at all
            self.logError(f"func _addDataByMultiTable_n error: {sql}-{e}")
            self._unlockTableIfNeeded(fullRegTableName)
            raise

    def _getStmtBindLines(self, db: Database, dbc):
        """
        Retrieves statement and bind lines from the database.

        Args:
            db (Database): The database object.
            dbc: The database connection object.

        Returns:
            tuple: A tuple containing the lines, cols, and tags.
                - lines: A list of tuples representing the statement and bind lines.
                - cols: The columns of the database table.
                - tags: The tags of the database table.
        """
        if dbc.query("show {}.tables".format(db.getName())) == 0:  # no tables
            return
        res = dbc.getQueryResult()
        lines = list()
        regTables = list(map(lambda x:f'{x[0]}_stmt', res[0:self.SMALL_NUMBER_OF_RECORDS]))
        cols = self.getMeta(db, dbc)[0]
        tags = self.getMeta(db, dbc)[1]
        colStrs = self._getTagColStrForSql(db, dbc, cols)[1]
        tagStrs = self._getTagColStrForSql(db, dbc, tags)[1]
        for regTable in regTables:
            combine_list = [regTable] + colStrs + tagStrs
            lines.append(tuple(combine_list))
        return lines, cols, tags

    def _transTs(self, ts, precision):
        """
        Convert a timestamp to the specified precision.

        Args:
            ts (datetime.datetime): The timestamp to convert.
            precision (str): The desired precision of the converted timestamp. Valid values are "ms", "us", and "ns".

        Returns:
            int: The converted timestamp.

        """
        if precision == "ms":
            return int(ts.timestamp() * 1000)
        elif precision == "us":
            return int(ts.timestamp() * 1000000)
        elif precision == "ns":
            return int(ts.timestamp() * 1000000000)

    def bind_row_by_row(self, stmt: taos.TaosStmt, lines, tag_dict, col_dict, precision):
        """
        Binds rows of data to a TaosStmt object row by row.

        Args:
            stmt (taos.TaosStmt): The TaosStmt object to bind the data to.
            lines (list): The list of rows containing the data to bind.
            tag_dict (dict): A dictionary mapping tag names to their corresponding types.
            col_dict (dict): A dictionary mapping column names to their corresponding types.

        Returns:
            taos.TaosStmt: The TaosStmt object with the data bound.

        """
        try:
            tb_name = None
            for row in lines:
                if tb_name != row[0]:
                    tb_name = row[0]
                    # Dynamic tag binding based on tag_types list
                    tags = taos.new_bind_params(len(tag_dict))  # Dynamically set the count of tags
                    for i, tag_name in enumerate(tag_dict):
                        bind_type = tag_dict[tag_name].lower()
                        if "unsigned" in tag_dict[tag_name].lower():
                            bind_type = bind_type.replace(" unsigned", "_unsigned")
                        if bind_type == "varbinary" or bind_type == "geometry":
                            getattr(tags[i], bind_type)((row[len(col_dict) + i + 1]).encode('utf-8'))  # Dynamically call the appropriate binding method
                        else:
                            getattr(tags[i], bind_type)(row[len(col_dict) + i + 1])  # Dynamically call the appropriate binding method
                    stmt.set_tbname_tags(tb_name, tags)

                # Dynamic value binding based on value_types list
                values = taos.new_bind_params(len(col_dict))  # Dynamically set the count of columns
                for j, col_name in enumerate(col_dict):
                    bind_type = col_dict[col_name].lower()
                    if "unsigned" in col_dict[col_name].lower():
                        bind_type = bind_type.replace("unsigned", "_unsigned")
                    if j == 0:
                        getattr(values[j], col_dict[col_name].lower())(self._transTs(row[1 + j], precision))  # Dynamically call the appropriate binding method
                    else:
                        if bind_type == "varbinary" or bind_type == "geometry":
                            getattr(values[j], bind_type)(row[1 + j].encode('utf-8'))
                        else:
                            getattr(values[j], bind_type)(row[1 + j])  # Dynamically call the appropriate binding method
                stmt.bind_param(values)
            return stmt
        except Exception as e:  # Any exception at all
            self.logError(f"func bind_row_by_row error: {e}")
            raise

    def _addDataBySTMT(self, db: Database, dbc):
        """
        Add data to the database using prepared statements.

        Args:
            db (Database): The database object.
            dbc: The dbc object.

        Returns:
            None
        """
        lines, col_dict, tag_dict = self._getStmtBindLines(db, dbc)
        # TODO replace dbc
        conn = taos.connect(database=db.getName())
        dbc.query(f'select `precision` from information_schema.ins_databases where name = "{db.getName()}"')
        res = dbc.getQueryResult()
        precision = res[0][0]
        stbname = db.getFixedSuperTable().getName()
        fullTableName = f'{db.getName()}.{stbname}'
        self._lockTableIfNeeded(fullTableName, 'batch')
        try:
            # Dynamically create the SQL statement based on the number of tags and values
            tag_placeholders = ', '.join(['?' for _ in tag_dict])
            value_placeholders = ', '.join(['?' for _ in col_dict])
            sql = f"INSERT INTO ? USING {fullTableName} TAGS({tag_placeholders}) VALUES({value_placeholders})"
            stmt = conn.statement(sql)
            self.bind_row_by_row(stmt, lines, tag_dict, col_dict, precision)
            stmt.execute()
            stmt.close()
        except Exception as e:  # Any exception at all
            self.logError(f"func _addDataBySTMT error: {e}")
            raise
        finally:
            self._unlockTableIfNeeded(fullTableName)
            conn.close()

    def _format_sml(self, data, rowType="tag"):
        """
        Formats the given data into a string representation of SML (Simple Markup Language).

        Args:
            data (dict): A dictionary containing the data to be formatted.

        Returns:
            str: The formatted SML string.

        """
        namePrefix = "t" if rowType == "tag" else "c"
        result = []
        type_mapping = {
            "TINYINT": "i8",
            "SMALLINT": "i16",
            "INT": "i32",
            "BIGINT": "i64",
            "TINYINT UNSIGNED": "u8",
            "SMALLINT UNSIGNED": "u16",
            "INT UNSIGNED": "u32",
            "BIGINT UNSIGNED": "u64",
            "VARCHAR": '""',
            "BINARY": '""',
            "NCHAR": 'L""',
            "BOOL": ""
        }

        for key, value in data.items():
            if key in type_mapping:
                if key in ["BINARY", "VARCHAR", "NCHAR"]:
                    formatted_value = f'{value}{type_mapping[key]}'
                    if key == "NCHAR":
                        formatted_value = f'L"{value}"'
                    else:
                        formatted_value = f'"{value}"'
                else:
                    formatted_value = f'{value}{type_mapping[key]}'
                result.append(f"{namePrefix}{len(result)}={formatted_value}")
        return ','.join(result)

    def _transSmlVal(self, ):
        pass

    def _transSmlTsType(self, precision):
        """
        Translates the precision string to the corresponding TDSmlTimestampType value.

        Args:
            precision (str): The precision string ("ms", "us", or "ns").

        Returns:
            int: The corresponding TDSmlTimestampType value.

        """
        if precision == "ms":
            return TDSmlTimestampType.MILLI_SECOND.value
        elif precision == "us":
            return TDSmlTimestampType.MICRO_SECOND.value
        elif precision == "ns":
            return TDSmlTimestampType.NANO_SECOND.value

    def _addDataByInfluxdbLine(self, db: Database, dbc):
        """
        Add data to InfluxDB using InfluxDB line protocol.

        Args:
            db (Database): The InfluxDB database.
            dbc: The InfluxDB client.

        Raises:
            CrashGenError: If there is an error adding data to InfluxDB.

        Returns:
            None
        """
        dbc.query('show {}.stables like "influxdb_%"'.format(db.getName()))
        res = dbc.getQueryResult()
        newCreateStable = f'influxdb_{TDCom.get_long_name(8)}'
        stbList = list(map(lambda x: x[0], res)) + [newCreateStable] if len(res) > 0 else [newCreateStable]
        stbname = random.choice(stbList)
        # for stbname in stbList:
        dataDictList = list()
        tagDataDictList = list()
        dbc.query(f'select `precision` from information_schema.ins_databases where name = "{db.getName()}"')
        res = dbc.getQueryResult()
        precision = res[0][0]
        ts = self._transTs(db.getNextTick(), precision)
        if stbname == newCreateStable:
            for i in range(2):
                sample_cnt = random.randint(1, len(self.smlSupportTypeList))
                typeList = random.sample(self.smlSupportTypeList, sample_cnt)
                elm = "tag" if i == 0 else "col"
                nameTypeDict = {f'{typeList[i]}_{elm}': typeList[i] for i in range(sample_cnt)}
                valList = self._getTagColStrForSql(db, dbc, nameTypeDict)[1]
                dataDict = dict(zip(typeList, valList))
                dataDictList.append(dataDict)
            tagStrs = self._format_sml(dataDictList[0], rowType="tag")
            colStrs = self._format_sml(dataDictList[1], rowType="col")
        else:
            cols, tags = self.getMeta(db, dbc, customStable=stbname)
            # if not tags or not cols:
            #     return "No table exists"
            tagNameList = list(tags.keys())
            tagTypeList = list(tags.values())
            randomTagValList = self._getTagColStrForSql(db, dbc, tags)[1]
            randomTagDataDict = dict(zip(tagTypeList, randomTagValList))
            tagDataDictList.append(randomTagDataDict)

            existCtbTagValList = self._getVals(db, dbc, tagNameList, stbname)
            existCtbDataDict = dict(zip(tagTypeList, existCtbTagValList))
            tagDataDictList.append(existCtbDataDict)
            tagStrs = self._format_sml(random.choice(tagDataDictList), rowType="tag")

            colTypeList = list(cols.values())
            randomColValList = self._getTagColStrForSql(db, dbc, cols)[1]
            randomcolDataDict = dict(zip(colTypeList, randomColValList))
            colStrs = self._format_sml(randomcolDataDict, rowType="col")

            # # TODO add/reduce tag and col
            # if Dice.throw(4) == 0:
            #     # add tag
            #     pass
            # elif Dice.throw(4) == 1:
            #     # reduce tag
            #     pass
            # elif Dice.throw(4) == 2:
            #     # add col
            #     pass
            # elif Dice.throw(4) == 3:
            #     # reduce col
            #     pass
        try:
            lines = f'{stbname},{tagStrs} {colStrs} {ts}'
            ts_type = self._transSmlTsType(precision)
            dbc.influxdbLineInsert(line=[lines], ts_type=ts_type, dbname=db.getName())
        except Exception as e:  # Any exception at all
            self.logError(f"func _addDataByInfluxdbLine error: {lines}-{e}")
            raise

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        # ds = self._dbManager # Quite DANGEROUS here, may result in multi-thread client access
        db = self._db
        dbc = wt.getDbConn()
        numTables = self.LARGE_NUMBER_OF_TABLES if Config.getConfig().larger_data else self.SMALL_NUMBER_OF_TABLES
        numRecords = self.LARGE_NUMBER_OF_RECORDS if Config.getConfig().larger_data else self.SMALL_NUMBER_OF_RECORDS
        tblSeq = list(range(numTables))
        random.shuffle(tblSeq)  # now we have random sequence
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
            if not sTable.ensureSuperTable(self, wt.getDbConn()): # Ensure the super table exists
                return
            sTable.ensureRegTable(self, wt.getDbConn(), regTableName)  # Ensure the table exists
            # self._unlockTable(fullTableName)

            if Dice.throw(6) == 0:  # 1 in 2 chance
                self._addData_n(db, dbc, regTableName, te)
            elif Dice.throw(6) == 1:
                self._addDataInBatch_n(db, dbc, regTableName, te)
            elif Dice.throw(6) == 2:
                self._addDataByAutoCreateTable_n(db, dbc)
            elif Dice.throw(6) == 3:
                if Config.getConfig().connector_type == 'native':
                    self._addDataByMultiTable_n(db, dbc)
                elif Config.getConfig().connector_type == 'rest':
                    print("stmt is not supported by restapi")
                    return
                elif Config.getConfig().connector_type == 'ws':
                    print("stmt by websocket todo supported")
                    return

            elif Dice.throw(6) == 4:
                self._addDataBySTMT(db, dbc)
            elif Dice.throw(6) == 5:
                self._addDataByInfluxdbLine(db, dbc)
            else:
                self.activeTable.discard(i)  # not raising an error, unlike remove


class TaskDeleteData(StateTransitionTask):
    # Track which table is being actively worked on
    activeTable: Set[int] = set()

    # We use these two files to record operations to DB, useful for power-off tests
    fAddLogReady = None  # type: Optional[io.TextIOWrapper]
    fAddLogDone = None  # type: Optional[io.TextIOWrapper]

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
        return state.canDeleteData()

    def _lockTableIfNeeded(self, fullTableName, extraMsg=''):
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

    def _deleteData(self, db: Database, dbc, regTableName, te: TaskExecutor):  # implied: NOT in batches
        numRecords = self.LARGE_NUMBER_OF_RECORDS if Config.getConfig().larger_data else self.SMALL_NUMBER_OF_RECORDS
        del_Records = int(numRecords / 5)
        if Dice.throw(2) == 0:
            for j in range(del_Records):  # number of records per table
                intToWrite = db.getNextInt()
                nextTick = db.getNextTick()
                # nextColor = db.getNextColor()
                if Config.getConfig().record_ops:
                    self.prepToRecordOps()
                    if self.fAddLogReady is None:
                        raise CrashGenError("Unexpected empty fAddLogReady")
                    self.fAddLogReady.write("Ready to delete {} to {}\n".format(intToWrite, regTableName))
                    self.fAddLogReady.flush()
                    os.fsync(self.fAddLogReady.fileno())

                # TODO: too ugly trying to lock the table reliably, refactor...
                fullTableName = db.getName() + '.' + regTableName
                self._lockTableIfNeeded(
                    fullTableName)  # so that we are verify read-back. TODO: deal with exceptions before unlock

                try:
                    sql = "delete from {} where ts = '{}' ;".format(  # removed: tags ('{}', {})
                        fullTableName,
                        # ds.getFixedSuperTableName(),
                        # ds.getNextBinary(), ds.getNextFloat(),
                        nextTick)

                    # print(sql)
                    # Logging.info("Adding data: {}".format(sql))
                    dbc.execute(sql)
                    intWrote = intToWrite

                    # Quick hack, attach an update statement here. TODO: create an "update" task
                    if (not Config.getConfig().use_shadow_db) and Dice.throw(
                            5) == 0:  # 1 in N chance, plus not using shaddow DB
                        intToUpdate = db.getNextInt()  # Updated， but should not succeed
                        # nextColor = db.getNextColor()
                        sql = "delete from {} where ts = '{}' ;".format(  # "INSERt" means "update" here
                            fullTableName,
                            nextTick)
                        # sql = "UPDATE {} set speed={}, color='{}' WHERE ts='{}'".format(
                        #     fullTableName, db.getNextInt(), db.getNextColor(), nextTick)
                        dbc.execute(sql)
                        intWrote = intToUpdate  # We updated, seems TDengine non-cluster accepts this.

                except Exception as e:  # Any exception at all
                    self._unlockTableIfNeeded(fullTableName)
                    self.logError(f"func _deleteData error: {sql}-{e}")
                    raise

                # Now read it back and verify, we might encounter an error if table is dropped
                if Config.getConfig().verify_data:  # only if command line asks for it
                    try:
                        dbc.query("SELECT * from {}.{} WHERE ts='{}'".
                                  format(db.getName(), regTableName, nextTick))
                        result = dbc.getQueryResult()
                        if len(result) == 0:
                            # means data has been delete
                            print("D1", end="")  # DF means delete failed
                        else:
                            print("DF", end="")  # DF means delete failed
                    except taos.error.ProgrammingError as err:
                        errno = Helper.convertErrno(err.errno)
                        # if errno == CrashGenError.INVALID_EMPTY_RESULT: # empty result
                        #     print("D1",end="")   # D1 means delete data success and only 1 record

                        if errno in [0x218, 0x362, 0x2662]:  # table doesn't exist
                            # do nothing
                            pass
                        else:
                            # Re-throw otherwise
                            self.logError(f"func _deleteData error: {sql}")
                            raise
                    finally:
                        self._unlockTableIfNeeded(fullTableName)  # Quite ugly, refactor lock/unlock
                # Done with read-back verification, unlock the table now
                # Successfully wrote the data into the DB, let's record it somehow
                te.recordDataMark(intWrote)
            else:

                # delete all datas and verify datas ,expected table is empty
                if Config.getConfig().record_ops:
                    self.prepToRecordOps()
                    if self.fAddLogReady is None:
                        raise CrashGenError("Unexpected empty fAddLogReady")
                    self.fAddLogReady.write("Ready to delete {} to {}\n".format(intToWrite, regTableName))
                    self.fAddLogReady.flush()
                    os.fsync(self.fAddLogReady.fileno())

                # TODO: too ugly trying to lock the table reliably, refactor...
                fullTableName = db.getName() + '.' + regTableName
                self._lockTableIfNeeded(
                    fullTableName)  # so that we are verify read-back. TODO: deal with exceptions before unlock

                try:
                    sql = "delete from {} ;".format(  # removed: tags ('{}', {})
                        fullTableName)
                    # Logging.info("Adding data: {}".format(sql))
                    dbc.execute(sql)

                    # Quick hack, attach an update statement here. TODO: create an "update" task
                    if (not Config.getConfig().use_shadow_db) and Dice.throw(
                            5) == 0:  # 1 in N chance, plus not using shaddow DB
                        sql = "delete from {}  ;".format(  # "INSERt" means "update" here
                            fullTableName)
                        dbc.execute(sql)

                except:  # Any exception at all
                    self._unlockTableIfNeeded(fullTableName)
                    self.logError(f"func _deleteData error: {sql}")
                    raise

                # Now read it back and verify, we might encounter an error if table is dropped
                if Config.getConfig().verify_data:  # only if command line asks for it
                    try:
                        dbc.query("SELECT * from {}.{} WHERE ts='{}'".
                                  format(db.getName(), regTableName, nextTick))
                        result = dbc.getQueryResult()
                        if len(result) == 0:
                            # means data has been delete
                            print("DA", end="")
                        else:
                            print("DF", end="")  # DF means delete failed
                    except taos.error.ProgrammingError as err:
                        errno = Helper.convertErrno(err.errno)
                        # if errno == CrashGenError.INVALID_EMPTY_RESULT: # empty result
                        #     print("Da",end="")  # Da means delete data success and for all datas

                        if errno in [0x218, 0x362, 0x2662]:  # table doesn't exist
                            # do nothing
                            pass
                        else:
                            # Re-throw otherwise
                            self.logError(f"func _deleteData error: {sql}")
                            raise
                    finally:
                        self._unlockTableIfNeeded(fullTableName)  # Quite ugly, refactor lock/unlock
                # Done with read-back verification, unlock the table now

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
        numTables = self.LARGE_NUMBER_OF_TABLES if Config.getConfig().larger_data else self.SMALL_NUMBER_OF_TABLES
        numRecords = self.LARGE_NUMBER_OF_RECORDS if Config.getConfig().larger_data else self.SMALL_NUMBER_OF_RECORDS
        tblSeq = list(range(numTables))
        random.shuffle(tblSeq)  # now we have random sequence
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

            self._deleteData(db, dbc, regTableName, te)

            self.activeTable.discard(i)  # not raising an error, unlike remove


class ThreadStacks:  # stack info for all threads
    def __init__(self):
        self._allStacks = {}
        allFrames = sys._current_frames()  # All current stack frames, keyed with "ident"
        for th in threading.enumerate():  # For each thread
            stack = traceback.extract_stack(allFrames[th.ident])  # type: ignore # Get stack for a thread
            shortTid = th.native_id % 10000  # type: ignore
            self._allStacks[shortTid] = stack  # Was using th.native_id

    def record_current_time(self, current_time):
        self.current_time = current_time

    def print(self, filteredEndName=None, filterInternal=False):
        for shortTid, stack in self._allStacks.items():  # for each thread, stack frames top to bottom
            lastFrame = stack[-1]
            if filteredEndName:  # we need to filter out stacks that match this name
                if lastFrame.name == filteredEndName:  # end did not match
                    continue
            if filterInternal:
                if lastFrame.name in ['wait', 'invoke_excepthook',
                                      '_wait',  # The Barrier exception
                                      'svcOutputReader',  # the svcMgr thread
                                      '__init__']:  # the thread that extracted the stack
                    continue  # ignore
            # Now print
            print("\n<----- Thread Info for LWP/ID: {} (most recent call last) <-----".format(shortTid))

            lastSqlForThread = DbConn.fetchSqlForThread(shortTid)
            last_sql_commit_time = DbConn.get_save_sql_time(shortTid)
            # time_cost = DbConn.get_time_cost()
            print("Last SQL statement attempted from thread {}  ({:.4f} sec ago) is: {}".format(shortTid,
                                                                                                self.current_time - last_sql_commit_time,
                                                                                                lastSqlForThread))
            stackFrame = 0
            for frame in stack:  # was using: reversed(stack)
                # print(frame)
                print("[{sf}] File {filename}, line {lineno}, in {name}".format(
                    sf=stackFrame, filename=frame.filename, lineno=frame.lineno, name=frame.name))
                print("    {}".format(frame.line))
                stackFrame += 1
            print("-----> End of Thread Info ----->\n")
            if self.current_time - last_sql_commit_time > 100:  # dead lock occured
                print("maybe dead locked of thread {} ".format(shortTid))


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
    #     if dbc.query("select * from information_schema.ins_databases") <= 1:  # no database (we have a default called "log")
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
        tInst = gContainer.defTdeInstance = TdeInstance()  # "subdir to hold the instance"

        cfg = Config.getConfig()
        dbManager = DbManager(cfg.connector_type, tInst.getDbTarget())  # Regular function
        thPool = ThreadPool(cfg.num_threads, cfg.max_steps)
        self.tc = ThreadCoordinator(thPool, dbManager)

        Logging.info("Starting client instance: {}".format(tInst))
        self.tc.run()
        # print("exec stats: {}".format(self.tc.getExecStats()))
        # print("TC failed = {}".format(self.tc.isFailed()))
        if svcMgr:  # gConfig.auto_start_service:
            svcMgr.stopTaosServices()
            svcMgr = None

        # Release global variables
        # gConfig = None
        Config.clearConfig()
        gSvcMgr = None
        logger = None

        thPool = None
        dbManager.cleanUp()  # destructor wouldn't run in time
        dbManager = None

        # Print exec status, etc., AFTER showing messages from the server
        self.conclude()
        # print("TC failed (2) = {}".format(self.tc.isFailed()))
        # Linux return code: ref https://shapeshed.com/unix-exit-codes/
        ret = 1 if self.tc.isFailed() else 0
        self.tc.cleanup()
        # Release variables here
        self.tc = None

        gc.collect()  # force garbage collection
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
        self._svcMgr = None  # type: Optional[ServiceManager]

        signal.signal(signal.SIGTERM, self.sigIntHandler)
        signal.signal(signal.SIGINT, self.sigIntHandler)
        signal.signal(signal.SIGUSR1, self.sigUsrHandler)  # different handler!

    def sigUsrHandler(self, signalNumber, frame):
        if self._clientMgr:
            self._clientMgr.sigUsrHandler(signalNumber, frame)
        elif self._svcMgr:  # Only if no client mgr, we are running alone
            self._svcMgr.sigUsrHandler(signalNumber, frame)

    def sigIntHandler(self, signalNumber, frame):
        if self._svcMgr:
            self._svcMgr.sigIntHandler(signalNumber, frame)
        if self._clientMgr:
            self._clientMgr.sigIntHandler(signalNumber, frame)

    def runClient(self):
        global gSvcMgr
        if Config.getConfig().auto_start_service:
            gSvcMgr = self._svcMgr = ServiceManager(1)  # hack alert
            gSvcMgr.startTaosServices()  # we start, don't run

        self._clientMgr = ClientManager()
        ret = None
        try:
            ret = self._clientMgr.run(self._svcMgr)  # stop TAOS service inside
        except requests.exceptions.ConnectionError as err:
            Logging.warning("Failed to open REST connection to DB: {}".format(err))
            # don't raise
        return ret

    def runService(self):
        global gSvcMgr
        gSvcMgr = self._svcMgr = ServiceManager(
            Config.getConfig().num_dnodes)  # save it in a global variable TODO: hack alert

        gSvcMgr.run()  # run to some end state
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
            help='Connector type to use: native, rest, ws, or mixed (default: 10)')
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
            help='Ignore error codes, comma separated, 0x supported, also suppresses certain transition state checks. (default: None)')
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

    def init(self):  # TODO: refactor
        global gContainer
        gContainer = Container()  # micky-mouse DI

        global gSvcMgr  # TODO: refactor away
        gSvcMgr = None

        parser = self._buildCmdLineParser()
        Config.init(parser)

        # Sanity check for arguments
        if Config.getConfig().use_shadow_db and Config.getConfig().max_dbs > 1:
            raise CrashGenError("Cannot combine use-shadow-db with max-dbs of more than 1")

        Logging.clsInit(Config.getConfig().debug)

        Dice.seed(0)  # initial seeding of dice

    def run(self):
        if Config.getConfig().run_tdengine:  # run server
            try:
                self.runService()
                return 0  # success
            except ConnectionError as err:
                Logging.error("Failed to make DB connection, please check DB instance manually")
            return -1  # failure
        else:
            return self.runClient()


class Container():
    _propertyList = {'defTdeInstance'}

    def __init__(self):
        self._cargo = {}  # No cargo at the beginning

    def _verifyValidProperty(self, name):
        if not name in self._propertyList:
            raise CrashGenError("Invalid container property: {}".format(name))

    # Called for an attribute, when other mechanisms fail (compare to  __getattribute__)
    def __getattr__(self, name):
        self._verifyValidProperty(name)
        return self._cargo[name]  # just a simple lookup

    def __setattr__(self, name, value):
        if name == '_cargo':  # reserved vars
            super().__setattr__(name, value)
            return
        self._verifyValidProperty(name)
        self._cargo[name] = value
