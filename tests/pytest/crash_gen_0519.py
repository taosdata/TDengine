#!/usr/bin/python3.7
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
from __future__ import annotations  # For type hinting before definition, ref: https://stackoverflow.com/questions/33533148/how-do-i-specify-that-the-return-type-of-a-method-is-the-same-as-the-class-itsel    

import sys
# Require Python 3
if sys.version_info[0] < 3:
    raise Exception("Must be using Python 3")

import getopt
import argparse
import copy

import threading
import random
import logging
import datetime
import textwrap

from typing import List

from util.log import *
from util.dnodes import *
from util.cases import *
from util.sql import *

import crash_gen
import taos

# Global variables, tried to keep a small number. 
gConfig = None # Command-line/Environment Configurations, will set a bit later
logger = None

def runThread(wt: WorkerThread):    
    wt.run()

class WorkerThread:
    def __init__(self, pool: ThreadPool, tid, 
            tc: ThreadCoordinator,
            # te: TaskExecutor,
            ): # note: main thread context!
        # self._curStep = -1 
        self._pool = pool
        self._tid = tid        
        self._tc = tc
        # self.threadIdent = threading.get_ident()
        self._thread = threading.Thread(target=runThread, args=(self,))
        self._stepGate = threading.Event()

        # Let us have a DB connection of our own
        if ( gConfig.per_thread_db_connection ): # type: ignore
            self._dbConn = DbConn()   

    def logDebug(self, msg):
        logger.info("    t[{}] {}".format(self._tid, msg))

    def logInfo(self, msg):
        logger.info("    t[{}] {}".format(self._tid, msg))

   
    def getTaskExecutor(self):
        return self._tc.getTaskExecutor()     

    def start(self):
        self._thread.start()  # AFTER the thread is recorded

    def run(self): 
        # initialization after thread starts, in the thread context
        # self.isSleeping = False
        logger.info("Starting to run thread: {}".format(self._tid))

        if ( gConfig.per_thread_db_connection ): # type: ignore
            self._dbConn.open()

        self._doTaskLoop()       
        
        # clean up
        if ( gConfig.per_thread_db_connection ): # type: ignore 
            self._dbConn.close()

    def _doTaskLoop(self) :
        # while self._curStep < self._pool.maxSteps:
        # tc = ThreadCoordinator(None)
        while True:  
            tc = self._tc # Thread Coordinator, the overall master            
            tc.crossStepBarrier()  # shared barrier first, INCLUDING the last one
            logger.debug("Thread task loop exited barrier...")
            self.crossStepGate()   # then per-thread gate, after being tapped
            logger.debug("Thread task loop exited step gate...")
            if not self._tc.isRunning():
                break

            task = tc.fetchTask()
            task.execute(self)
            tc.saveExecutedTask(task)
  
    def verifyThreadSelf(self): # ensure we are called by this own thread
        if ( threading.get_ident() != self._thread.ident ): 
            raise RuntimeError("Unexpectly called from other threads")

    def verifyThreadMain(self): # ensure we are called by the main thread
        if ( threading.get_ident() != threading.main_thread().ident ): 
            raise RuntimeError("Unexpectly called from other threads")

    def verifyThreadAlive(self):
        if ( not self._thread.is_alive() ):
            raise RuntimeError("Unexpected dead thread")

    # A gate is different from a barrier in that a thread needs to be "tapped"
    def crossStepGate(self):
        self.verifyThreadAlive()
        self.verifyThreadSelf() # only allowed by ourselves
        
        # Wait again at the "gate", waiting to be "tapped"
        # logger.debug("Worker thread {} about to cross the step gate".format(self._tid))
        self._stepGate.wait() 
        self._stepGate.clear()
        
        # self._curStep += 1  # off to a new step...

    def tapStepGate(self): # give it a tap, release the thread waiting there
        self.verifyThreadAlive()
        self.verifyThreadMain() # only allowed for main thread
 
        logger.debug("Tapping worker thread {}".format(self._tid))
        self._stepGate.set() # wake up!        
        time.sleep(0) # let the released thread run a bit

    def execSql(self, sql): # not "execute", since we are out side the DB context
        if ( gConfig.per_thread_db_connection ):
            return self._dbConn.execute(sql)            
        else:
            return self._tc.getDbState().getDbConn().execute(sql)

    def querySql(self, sql): # not "execute", since we are out side the DB context
        if ( gConfig.per_thread_db_connection ):
            return self._dbConn.query(sql)            
        else:
            return self._tc.getDbState().getDbConn().query(sql)
                  
class ThreadCoordinator:
    def __init__(self, pool, wd: WorkDispatcher, dbState):
        self._curStep = -1 # first step is 0
        self._pool = pool
        self._wd = wd
        self._te = None # prepare for every new step
        self._dbState = dbState
        self._executedTasks: List[Task] = [] # in a given step
        self._lock = threading.RLock() # sync access for a few things

        self._stepBarrier = threading.Barrier(self._pool.numThreads + 1) # one barrier for all threads

    def getTaskExecutor(self):
        return self._te

    def getDbState(self) -> DbState :
        return self._dbState

    def crossStepBarrier(self):
        self._stepBarrier.wait()

    def run(self):              
        self._pool.createAndStartThreads(self)

        # Coordinate all threads step by step
        self._curStep = -1 # not started yet
        maxSteps = gConfig.max_steps # type: ignore
        while(self._curStep < maxSteps):
            print(".", end="", flush=True)
            logger.debug("Main thread going to sleep")

            # Now ready to enter a step
            self.crossStepBarrier() # let other threads go past the pool barrier, but wait at the thread gate
            self._stepBarrier.reset() # Other worker threads should now be at the "gate"            

            # At this point, all threads should be pass the overall "barrier" and before the per-thread "gate"
            self._dbState.transition(self._executedTasks) # at end of step, transiton the DB state
            self.resetExecutedTasks() # clear the tasks after we are done

            # Get ready for next step
            logger.info("<-- Step {} finished".format(self._curStep))
            self._curStep += 1 # we are about to get into next step. TODO: race condition here!                
            logger.debug("\r\n--> Step {} starts with main thread waking up".format(self._curStep)) # Now not all threads had time to go to sleep

            # A new TE for the new step
            self._te = TaskExecutor(self._curStep)

            logger.debug("Main thread waking up at step {}, tapping worker threads".format(self._curStep)) # Now not all threads had time to go to sleep            
            self.tapAllThreads()

        logger.debug("Main thread ready to finish up...")
        self.crossStepBarrier() # Cross it one last time, after all threads finish
        self._stepBarrier.reset()
        logger.debug("Main thread in exclusive zone...")
        self._te = None # No more executor, time to end
        logger.debug("Main thread tapping all threads one last time...")
        self.tapAllThreads() # Let the threads run one last time
        logger.debug("Main thread joining all threads")
        self._pool.joinAll() # Get all threads to finish

        logger.info("All threads finished")
        print("\r\nFinished")

    def tapAllThreads(self): # in a deterministic manner
        wakeSeq = []
        for i in range(self._pool.numThreads): # generate a random sequence
            if Dice.throw(2) == 1 :
                wakeSeq.append(i)
            else:
                wakeSeq.insert(0, i)
        logger.info("Waking up threads: {}".format(str(wakeSeq)))
        # TODO: set dice seed to a deterministic value
        for i in wakeSeq:
            self._pool.threadList[i].tapStepGate() # TODO: maybe a bit too deep?!
            time.sleep(0) # yield

    def isRunning(self):
        return self._te != None

    def fetchTask(self) -> Task :
        if ( not self.isRunning() ): # no task
            raise RuntimeError("Cannot fetch task when not running")
        # return self._wd.pickTask()
        # Alternatively, let's ask the DbState for the appropriate task
        dbState = self.getDbState()
        tasks = dbState.getTasksAtState()
        i = Dice.throw(len(tasks))
        # return copy.copy(tasks[i]) # Needs a fresh copy, to save execution results, etc.
        return tasks[i].clone()

    def resetExecutedTasks(self):
        self._executedTasks = [] # should be under single thread

    def saveExecutedTask(self, task):
        with self._lock:
            self._executedTasks.append(task)

# We define a class to run a number of threads in locking steps.
class ThreadPool:
    def __init__(self, dbState, numThreads, maxSteps, funcSequencer):
        self.numThreads = numThreads
        self.maxSteps = maxSteps
        self.funcSequencer = funcSequencer
        # Internal class variables
        self.dispatcher = WorkDispatcher(dbState)
        self.curStep = 0
        self.threadList = []
        # self.stepGate = threading.Condition() # Gate to hold/sync all threads
        # self.numWaitingThreads = 0    
        
    # starting to run all the threads, in locking steps
    def createAndStartThreads(self, tc: ThreadCoordinator):
        for tid in range(0, self.numThreads): # Create the threads
            workerThread = WorkerThread(self, tid, tc)            
            self.threadList.append(workerThread)
            workerThread.start() # start, but should block immediately before step 0

    def joinAll(self):
        for workerThread in self.threadList:
            logger.debug("Joining thread...")
            workerThread._thread.join()

# A queue of continguous POSITIVE integers
class LinearQueue():
    def __init__(self):
        self.firstIndex = 1  # 1st ever element
        self.lastIndex = 0
        self._lock = threading.RLock() # our functions may call each other
        self.inUse = set() # the indexes that are in use right now

    def toText(self):
        return "[{}..{}], in use: {}".format(self.firstIndex, self.lastIndex, self.inUse)

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
            if ( self.isEmpty() ): 
                # raise RuntimeError("Cannot pop an empty queue") 
                return False # TODO: None?
            
            index = self.firstIndex
            if ( index in self.inUse ):
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
            if ( i in self.inUse ):
                raise RuntimeError("Cannot re-use same index in queue: {}".format(i))
            self.inUse.add(i)

    def release(self, i):
        with self._lock:
            # logger.debug("LQ releasing item {}".format(i))
            self.inUse.remove(i) # KeyError possible, TODO: why?

    def size(self):
        return self.lastIndex + 1 - self.firstIndex

    def pickAndAllocate(self):
        if ( self.isEmpty() ):
            return None
        with self._lock:
            cnt = 0 # counting the interations
            while True:
                cnt += 1
                if ( cnt > self.size()*10 ): # 10x iteration already
                    # raise RuntimeError("Failed to allocate LinearQueue element")
                    return None
                ret = Dice.throwRange(self.firstIndex, self.lastIndex+1)
                if ( not ret in self.inUse ):
                    self.allocate(ret)
                    return ret

class DbConn:
    def __init__(self):
        self._conn = None 
        self._cursor = None
        self.isOpen = False
        
    def open(self): # Open connection
        if ( self.isOpen ):
            raise RuntimeError("Cannot re-open an existing DB connection")

        cfgPath = "../../build/test/cfg" 
        self._conn = taos.connect(host="127.0.0.1", config=cfgPath) # TODO: make configurable
        self._cursor = self._conn.cursor()

        # Get the connection/cursor ready
        self._cursor.execute('reset query cache')
        # self._cursor.execute('use db')

        # Open connection
        self._tdSql = TDSql()
        self._tdSql.init(self._cursor)
        self.isOpen = True

    def resetDb(self): # reset the whole database, etc.
        if ( not self.isOpen ):
            raise RuntimeError("Cannot reset database until connection is open")
        # self._tdSql.prepare() # Recreate database, etc.

        self._cursor.execute('drop database if exists db')
        logger.debug("Resetting DB, dropped database")
        # self._cursor.execute('create database db')
        # self._cursor.execute('use db')

        # tdSql.execute('show databases')

    def close(self):
        if ( not self.isOpen ):
            raise RuntimeError("Cannot clean up database until connection is open")
        self._tdSql.close()
        self.isOpen = False

    def execute(self, sql): 
        if ( not self.isOpen ):
            raise RuntimeError("Cannot execute database commands until connection is open")
        return self._tdSql.execute(sql)

    def query(self, sql) -> int :  # return number of rows retrieved
        if ( not self.isOpen ):
            raise RuntimeError("Cannot query database until connection is open")
        return self._tdSql.query(sql)


# State of the database as we believe it to be
class DbState():
    STATE_INVALID    = -1
    STATE_EMPTY      = 1  # nothing there, no even a DB
    STATE_DB_ONLY    = 2  # we have a DB, but nothing else
    STATE_TABLE_ONLY = 3  # we have a table, but totally empty
    STATE_HAS_DATA   = 4  # we have some data in the table

    def __init__(self):
        self.tableNumQueue = LinearQueue()
        self._lastTick = datetime.datetime(2019, 1, 1) # initial date time tick
        self._lastInt  = 0 # next one is initial integer 
        self._lock = threading.RLock()
        self._state = self.STATE_INVALID
        
        # self.openDbServerConnection()
        self._dbConn = DbConn()
        try:
            self._dbConn.open() # may throw taos.error.ProgrammingError: disconnected
        except taos.error.ProgrammingError as err:
            # print("Error type: {}, msg: {}, value: {}".format(type(err), err.msg, err))
            if ( err.msg == 'disconnected' ): # cannot open DB connection
                print("Cannot establish DB connection, please re-run script without parameter, and follow the instructions.")
                sys.exit()
            else:
                raise            
        except:
            print("[=]Unexpected exception")
            raise        
        self._dbConn.resetDb() # drop and recreate DB
        self._state = self.STATE_EMPTY # initial state, the result of above

    def getDbConn(self):
        return self._dbConn

    def pickAndAllocateTable(self): # pick any table, and "use" it
        return self.tableNumQueue.pickAndAllocate()

    def addTable(self):
        with self._lock:
            tIndex = self.tableNumQueue.push()
        return tIndex

    def getFixedTableName(self):
        return "fixed_table"

    def releaseTable(self, i): # return the table back, so others can use it
        self.tableNumQueue.release(i)

    def getNextTick(self):
        with self._lock: # prevent duplicate tick
            self._lastTick += datetime.timedelta(0, 1) # add one second to it
            return self._lastTick

    def getNextInt(self):
        with self._lock:
            self._lastInt += 1
            return self._lastInt
    
    def getTableNameToDelete(self):
        tblNum = self.tableNumQueue.pop() # TODO: race condition!
        if ( not tblNum ): # maybe false
            return False
        
        return "table_{}".format(tblNum)

    def execSql(self, sql): # using the main DB connection
        return self._dbConn.execute(sql)

    def cleanUp(self):
        self._dbConn.close()      

    def getTasksAtState(self):
        tasks = []
        tasks.append(ReadFixedDataTask(self)) # always
        if ( self._state == self.STATE_EMPTY ):
            tasks.append(CreateDbTask(self))
            tasks.append(CreateFixedTableTask(self))
        elif ( self._state == self.STATE_DB_ONLY ):
            tasks.append(DropDbTask(self))
            tasks.append(CreateFixedTableTask(self))
            tasks.append(AddFixedDataTask(self))
        elif ( self._state == self.STATE_TABLE_ONLY ):
            tasks.append(DropFixedTableTask(self))
            tasks.append(AddFixedDataTask(self))
        elif ( self._state == self.STATE_HAS_DATA ) : # same as above. TODO: adjust
            tasks.append(DropFixedTableTask(self))
            tasks.append(AddFixedDataTask(self))
        else:
            raise RuntimeError("Unexpected DbState state: {}".format(self._state))
        return tasks

    def transition(self, tasks):
        if ( len(tasks) == 0 ): # before 1st step, or otherwise empty
            return # do nothing
        if ( self._state == self.STATE_EMPTY ):
            # self.assertNoSuccess(tasks, ReadFixedDataTask) # some read may be successful, since we might be creating a table
            if ( self.hasSuccess(tasks, CreateDbTask) ):
                self.assertAtMostOneSuccess(tasks, CreateDbTask) # param is class
                self._state = self.STATE_DB_ONLY
                if ( self.hasSuccess(tasks, CreateFixedTableTask )):
                    self._state = self.STATE_TABLE_ONLY
                # else: # no successful table creation, not much we can say, as it is step 2
            else: # did not create db
                self.assertNoTask(tasks, CreateDbTask) # because we did not have such task
                # self.assertNoSuccess(tasks, CreateDbTask) # not necessary, since we just verified no such task
                self.assertNoSuccess(tasks, CreateFixedTableTask)
                
        elif ( self._state == self.STATE_DB_ONLY ): 
            self.assertAtMostOneSuccess(tasks, DropDbTask)
            self.assertIfExistThenSuccess(tasks, DropDbTask)
            self.assertAtMostOneSuccess(tasks, CreateFixedTableTask)
            # Nothing to be said about adding data task
            if ( self.hasSuccess(tasks, DropDbTask) ): # dropped the DB
                # self.assertHasTask(tasks, DropDbTask) # implied by hasSuccess
                self.assertAtMostOneSuccess(tasks, DropDbTask)
                self._state = self.STATE_EMPTY
            elif ( self.hasSuccess(tasks, CreateFixedTableTask) ): # did not drop db, create table success
                # self.assertHasTask(tasks, CreateFixedTableTask) # tried to create table
                self.assertAtMostOneSuccess(tasks, CreateFixedTableTask) # at most 1 attempt is successful
                self.assertNoTask(tasks, DropDbTask) # should have have tried
                if ( not self.hasSuccess(tasks, AddFixedDataTask) ): # just created table, no data yet
                    # can't say there's add-data attempts, since they may all fail
                    self._state = self.STATE_TABLE_ONLY
                else:                    
                    self._state = self.STATE_HAS_DATA
            else: # no success in dropping db tasks, no success in create fixed table, not acceptable
                raise RuntimeError("Unexpected no-success scenario")

        elif ( self._state == self.STATE_TABLE_ONLY ):            
            if ( self.hasSuccess(tasks, DropFixedTableTask) ):
                self.assertAtMostOneSuccess(tasks, DropFixedTableTask)
                self._state = self.STATE_DB_ONLY
            elif ( self.hasSuccess(tasks, AddFixedDataTask) ): # no success dropping the table
                self.assertNoTask(tasks, DropFixedTableTask)
                self._state = self.STATE_HAS_DATA
            else: # did not drop table, did not insert data, that is impossible
                raise RuntimeError("Unexpected no-success scenarios")

        elif ( self._state == self.STATE_HAS_DATA ): # Same as above, TODO: adjust
            if ( self.hasSuccess(tasks, DropFixedTableTask) ):
                self.assertAtMostOneSuccess(tasks, DropFixedTableTask)
                self._state = self.STATE_DB_ONLY
            elif ( self.hasSuccess(tasks, AddFixedDataTask) ): # no success dropping the table
                self.assertNoTask(tasks, DropFixedTableTask)
                self._state = self.STATE_HAS_DATA
            else: # did not drop table, did not insert data, that is impossible
                raise RuntimeError("Unexpected no-success scenarios")

        else:
            raise RuntimeError("Unexpected DbState state: {}".format(self._state))
        logger.debug("New DB state is: {}".format(self._state))

    def assertAtMostOneSuccess(self, tasks, cls):
        sCnt = 0
        for task in tasks :
            if not isinstance(task, cls):
                continue
            if task.isSuccess():
                task.logDebug("Task success found")
                sCnt += 1
                if ( sCnt >= 2 ):
                    raise RuntimeError("Unexpected more than 1 success with task: {}".format(cls))

    def assertIfExistThenSuccess(self, tasks, cls):
        sCnt = 0
        exists = False
        for task in tasks :
            if not isinstance(task, cls):
                continue
            exists = True # we have a valid instance
            if task.isSuccess():
                sCnt += 1
        if ( exists and sCnt <= 0 ):
            raise RuntimeError("Unexpected zero success for task: {}".format(cls))

    def assertNoTask(self, tasks, cls):
        for task in tasks :
            if isinstance(task, cls):
                raise RuntimeError("Unexpected task: {}".format(cls))

    def assertNoSuccess(self, tasks, cls):
        for task in tasks :
            if isinstance(task, cls):
                if task.isSuccess():
                    raise RuntimeError("Unexpected successful task: {}".format(cls))

    def hasSuccess(self, tasks, cls):
        for task in tasks :
            if not isinstance(task, cls):
                continue
            if task.isSuccess():
                return True
        return False

class TaskExecutor():
    def __init__(self, curStep):
        self._curStep = curStep

    def getCurStep(self):
        return self._curStep

    def execute(self, task: Task, wt: WorkerThread): # execute a task on a thread
        task.execute(wt)

    # def logInfo(self, msg):
    #     logger.info("    T[{}.x]: ".format(self._curStep) + msg)

    # def logDebug(self, msg):
    #     logger.debug("    T[{}.x]: ".format(self._curStep) + msg)

class Task():
    taskSn = 100

    @classmethod
    def allocTaskNum(cls):
        cls.taskSn += 1
        return cls.taskSn

    def __init__(self, dbState: DbState):
        self._dbState = dbState
        self._workerThread = None 
        self._err = None
        self._curStep = None
        self._numRows = None # Number of rows affected

        # Assign an incremental task serial number        
        self._taskNum = self.allocTaskNum()

    def isSuccess(self):
        return self._err == None

    def clone(self):
        newTask = self.__class__(self._dbState)
        return newTask

    def logDebug(self, msg):
        self._workerThread.logDebug("s[{}.{}] {}".format(self._curStep, self._taskNum, msg))

    def logInfo(self, msg):
        self._workerThread.logInfo("s[{}.{}] {}".format(self._curStep, self._taskNum, msg))

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        raise RuntimeError("To be implemeted by child classes, class name: {}".format(self.__class__.__name__))

    def execute(self, wt: WorkerThread):
        wt.verifyThreadSelf()
        self._workerThread = wt # type: ignore

        te = wt.getTaskExecutor()
        self._curStep = te.getCurStep()
        self.logDebug("[-] executing task {}...".format(self.__class__.__name__))

        self._err = None
        try:
            self._executeInternal(te, wt) # TODO: no return value?
        except taos.error.ProgrammingError as err:
            self.logDebug("[=]Taos Execution exception: {0}".format(err))
            self._err = err
        except:
            self.logDebug("[=]Unexpected exception")
            raise
        
        self.logDebug("[X] task execution completed, {}, status: {}".format(self.__class__.__name__, "Success" if self.isSuccess() else "Failure"))

    def execSql(self, sql):
        return self._dbState.execute(sql)

class CreateDbTask(Task):
    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        wt.execSql("create database db")

class DropDbTask(Task):
    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        wt.execSql("drop database db")

class CreateTableTask(Task):
    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        tIndex = self._dbState.addTable()
        self.logDebug("Creating a table {} ...".format(tIndex))
        wt.execSql("create table db.table_{} (ts timestamp, speed int)".format(tIndex))
        self.logDebug("Table {} created.".format(tIndex))
        self._dbState.releaseTable(tIndex)

class CreateFixedTableTask(Task):
    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        tblName = self._dbState.getFixedTableName()        
        wt.execSql("create table db.{} (ts timestamp, speed int)".format(tblName))

class ReadFixedDataTask(Task):
    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        tblName = self._dbState.getFixedTableName()        
        self._numRows = wt.querySql("select * from db.{}".format(tblName)) # save the result for later
        # tdSql.query(" cars where tbname in ('carzero', 'carone')")

class DropTableTask(Task):
    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        tableName = self._dbState.getTableNameToDelete()
        if ( not tableName ): # May be "False"
            self.logInfo("Cannot generate a table to delete, skipping...")
            return
        self.logInfo("Dropping a table db.{} ...".format(tableName))
        wt.execSql("drop table db.{}".format(tableName))
        
class DropFixedTableTask(Task):
    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        tblName = self._dbState.getFixedTableName()        
        wt.execSql("drop table db.{}".format(tblName))

class AddDataTask(Task):
    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        ds = self._dbState
        self.logInfo("Adding some data... numQueue={}".format(ds.tableNumQueue.toText()))
        tIndex = ds.pickAndAllocateTable()
        if ( tIndex == None ):
            self.logInfo("No table found to add data, skipping...")
            return
        sql = "insert into db.table_{} values ('{}', {});".format(tIndex, ds.getNextTick(), ds.getNextInt())
        self.logDebug("Executing SQL: {}".format(sql))
        wt.execSql(sql) 
        ds.releaseTable(tIndex)
        self.logDebug("Finished adding data")

class AddFixedDataTask(Task):
    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        ds = self._dbState
        sql = "insert into db.table_{} values ('{}', {});".format(ds.getFixedTableName(), ds.getNextTick(), ds.getNextInt())
        wt.execSql(sql) 

# Deterministic random number generator
class Dice():
    seeded = False # static, uninitialized

    @classmethod
    def seed(cls, s): # static
        if (cls.seeded):
            raise RuntimeError("Cannot seed the random generator more than once")
        cls.verifyRNG()
        random.seed(s)
        cls.seeded = True  # TODO: protect against multi-threading

    @classmethod
    def verifyRNG(cls): # Verify that the RNG is determinstic
        random.seed(0)
        x1 = random.randrange(0, 1000)
        x2 = random.randrange(0, 1000)
        x3 = random.randrange(0, 1000)
        if ( x1 != 864 or x2!=394 or x3!=776 ):
            raise RuntimeError("System RNG is not deterministic")

    @classmethod
    def throw(cls, stop): # get 0 to stop-1
        return cls.throwRange(0, stop)

    @classmethod
    def throwRange(cls, start, stop): # up to stop-1
        if ( not cls.seeded ):
            raise RuntimeError("Cannot throw dice before seeding it")
        return random.randrange(start, stop)


# Anyone needing to carry out work should simply come here
class WorkDispatcher():
    def __init__(self, dbState):
        # self.totalNumMethods = 2
        self.tasks = [
            CreateTableTask(dbState),
            DropTableTask(dbState),
            AddDataTask(dbState),
        ]

    def throwDice(self):
        max = len(self.tasks) - 1 
        dRes = random.randint(0, max)
        # logger.debug("Threw the dice in range [{},{}], and got: {}".format(0,max,dRes))
        return dRes

    def pickTask(self):
        dice = self.throwDice()
        return self.tasks[dice]

    def doWork(self, workerThread):
        task = self.pickTask()
        task.execute(workerThread)

def main():
    # Super cool Python argument library: https://docs.python.org/3/library/argparse.html
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent('''\
            TDengine Auto Crash Generator (PLEASE NOTICE the Prerequisites Below)
            ---------------------------------------------------------------------
            1. You build TDengine in the top level ./build directory, as described in offical docs
            2. You run the server there before this script: ./build/bin/taosd -c test/cfg

            '''))
    parser.add_argument('-p', '--per-thread-db-connection', action='store_true',                        
                        help='Use a single shared db connection (default: false)')
    parser.add_argument('-d', '--debug', action='store_true',                        
                        help='Turn on DEBUG mode for more logging (default: false)')
    parser.add_argument('-s', '--max-steps', action='store', default=100, type=int,
                        help='Maximum number of steps to run (default: 100)')
    parser.add_argument('-t', '--num-threads', action='store', default=10, type=int,
                        help='Number of threads to run (default: 10)')

    global gConfig
    gConfig = parser.parse_args()
    if len(sys.argv) == 1:
        parser.print_help()
        sys.exit()

    global logger
    logger = logging.getLogger('myApp')
    if ( gConfig.debug ):
        logger.setLevel(logging.DEBUG) # default seems to be INFO        
    ch = logging.StreamHandler()
    logger.addHandler(ch)

    dbState = DbState()
    Dice.seed(0) # initial seeding of dice
    tc = ThreadCoordinator(
        ThreadPool(dbState, gConfig.num_threads, gConfig.max_steps, 0), 
        WorkDispatcher(dbState),
        dbState
        )
    tc.run()
    dbState.cleanUp()
    logger.info("Finished running thread pool")

if __name__ == "__main__":
    main()
