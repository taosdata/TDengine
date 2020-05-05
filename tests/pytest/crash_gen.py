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

import threading
import random
import logging
import datetime

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
    def __init__(self, pool: SteppingThreadPool, tid, dbState, 
            tc: ThreadCoordinator,
            # te: TaskExecutor,
            ): # note: main thread context!
        # self._curStep = -1 
        self._pool = pool
        self._tid = tid
        self._dbState = dbState
        self._tc = tc
        # self.threadIdent = threading.get_ident()
        self._thread = threading.Thread(target=runThread, args=(self,))
        self._stepGate = threading.Event()

        # Let us have a DB connection of our own
        if ( gConfig.per_thread_db_connection ): # type: ignore
            self._dbConn = DbConn()   

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
            self._tc.crossStepBarrier()  # shared barrier first, INCLUDING the last one
            logger.debug("Thread task loop exited barrier...")
            self.crossStepGate()   # then per-thread gate, after being tapped
            logger.debug("Thread task loop exited step gate...")
            if not self._tc.isRunning():
                break

            task = self._tc.fetchTask()
            task.execute(self)
  
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

    def execSql(self, sql):
        if ( gConfig.per_thread_db_connection ):
            return self._dbConn.execSql(sql)            
        else:
            return self._dbState.getDbConn().execSql(sql)
                  
class ThreadCoordinator:
    def __init__(self, pool, wd: WorkDispatcher):
        self._curStep = -1 # first step is 0
        self._pool = pool
        self._wd = wd
        self._te = None # prepare for every new step

        self._stepBarrier = threading.Barrier(self._pool.numThreads + 1) # one barrier for all threads

    def getTaskExecutor(self):
        return self._te

    def crossStepBarrier(self):
        self._stepBarrier.wait()

    def run(self, dbState):              
        self._pool.createAndStartThreads(dbState, self)

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
            logger.info("<-- Step {} finished".format(self._curStep))
            self._curStep += 1 # we are about to get into next step. TODO: race condition here!                
            logger.debug("\r\n--> Step {} starts with main thread waking up".format(self._curStep)) # Now not all threads had time to go to sleep

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
        return self._wd.pickTask()

# We define a class to run a number of threads in locking steps.
class SteppingThreadPool:
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
    def createAndStartThreads(self, dbState, tc: ThreadCoordinator):
        for tid in range(0, self.numThreads): # Create the threads
            workerThread = WorkerThread(self, tid, dbState, tc)            
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

            # if ( index in self.inUse ):
            #     self.inUse.remove(index) # TODO: what about discard?

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
        self._cursor.execute('create database db')
        # self._cursor.execute('use db')

        # tdSql.execute('show databases')

    def close(self):
        if ( not self.isOpen ):
            raise RuntimeError("Cannot clean up database until connection is open")
        self._tdSql.close()
        self.isOpen = False

    def execSql(self, sql): 
        if ( not self.isOpen ):
            raise RuntimeError("Cannot query database until connection is open")
        return self._tdSql.execute(sql)

# State of the database as we believe it to be
class DbState():
    def __init__(self):
        self.tableNumQueue = LinearQueue()
        self._lastTick = datetime.datetime(2019, 1, 1) # initial date time tick
        self._lastInt  = 0 # next one is initial integer 
        self._lock = threading.RLock()

        # self.openDbServerConnection()
        self._dbConn = DbConn()
        self._dbConn.open() 
        self._dbConn.resetDb() # drop and recreate DB

    def getDbConn(self):
        return self._dbConn

    def pickAndAllocateTable(self): # pick any table, and "use" it
        return self.tableNumQueue.pickAndAllocate()

    def addTable(self):
        with self._lock:
            tIndex = self.tableNumQueue.push()
        return tIndex

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
        return self._dbConn.execSql(sql)

    def cleanUp(self):
        self._dbConn.close()      

class TaskExecutor():
    def __init__(self, curStep):
        self._curStep = curStep

    def execute(self, task, wt: WorkerThread): # execute a task on a thread
        task.execute(self, wt)

    def logInfo(self, msg):
        logger.info("    T[{}.x]: ".format(self._curStep) + msg)

    def logDebug(self, msg):
        logger.debug("    T[{}.x]: ".format(self._curStep) + msg)


class Task():
    def __init__(self, dbState):
        self.dbState = dbState

    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        raise RuntimeError("To be implemeted by child classes")

    def execute(self, wt: WorkerThread):
        wt.verifyThreadSelf()

        te = wt.getTaskExecutor()
        self._executeInternal(te, wt) # TODO: no return value?
        te.logDebug("[X] task execution completed")

    def execSql(self, sql):
        return self.dbState.execute(sql)

class CreateTableTask(Task):
    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        tIndex = self.dbState.addTable()
        te.logDebug("Creating a table {} ...".format(tIndex))
        wt.execSql("create table db.table_{} (ts timestamp, speed int)".format(tIndex))
        te.logDebug("Table {} created.".format(tIndex))
        self.dbState.releaseTable(tIndex)

class DropTableTask(Task):
    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        tableName = self.dbState.getTableNameToDelete()
        if ( not tableName ): # May be "False"
            te.logInfo("Cannot generate a table to delete, skipping...")
            return
        te.logInfo("Dropping a table db.{} ...".format(tableName))
        wt.execSql("drop table db.{}".format(tableName))

class AddDataTask(Task):
    def _executeInternal(self, te: TaskExecutor, wt: WorkerThread):
        ds = self.dbState
        te.logInfo("Adding some data... numQueue={}".format(ds.tableNumQueue.toText()))
        tIndex = ds.pickAndAllocateTable()
        if ( tIndex == None ):
            te.logInfo("No table found to add data, skipping...")
            return
        sql = "insert into db.table_{} values ('{}', {});".format(tIndex, ds.getNextTick(), ds.getNextInt())
        te.logDebug("Executing SQL: {}".format(sql))
        wt.execSql(sql) 
        ds.releaseTable(tIndex)
        te.logDebug("Finished adding data")

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
    def throw(cls, max): # get 0 to max-1
        return cls.throwRange(0, max)

    @classmethod
    def throwRange(cls, min, max): # up to max-1
        if ( not cls.seeded ):
            raise RuntimeError("Cannot throw dice before seeding it")
        return random.randrange(min, max)


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
    parser = argparse.ArgumentParser(description='TDengine Auto Crash Generator')
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

    global logger
    logger = logging.getLogger('myApp')
    if ( gConfig.debug ):
        logger.setLevel(logging.DEBUG) # default seems to be INFO        
    ch = logging.StreamHandler()
    logger.addHandler(ch)

    dbState = DbState()
    Dice.seed(0) # initial seeding of dice
    tc = ThreadCoordinator(
        SteppingThreadPool(dbState, gConfig.num_threads, gConfig.max_steps, 0), 
        WorkDispatcher(dbState) 
        )
    tc.run(dbState)
    dbState.cleanUp()
    logger.info("Finished running thread pool")

if __name__ == "__main__":
    main()
