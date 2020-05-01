#!/usr/bin/python3
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
import sys
import getopt

import threading
import random
import logging
import datetime

from util.log import *
from util.dnodes import *
from util.cases import *
from util.sql import *

import taos

# Constants
LOGGING_LEVEL = logging.DEBUG

def runThread(workerThread):
    logger.info("Running Thread: {}".format(workerThread.tid))
    workerThread.run()

# Used by one process to block till another is ready
# class Baton:
#     def __init__(self):
#         self._lock = threading.Lock() # control access to object
#         self._baton = threading.Condition() # let thread block
#         self._hasGiver = False
#         self._hasTaker = False

#     def give(self):
#         with self._lock:
#             if ( self._hasGiver ): # already?
#                 raise RuntimeError("Cannot double-give a baton")
#             self._hasGiver = True

#         self._settle() # may block, OUTSIDE self lock

#     def take(self):
#         with self._lock:
#             if ( self._hasTaker):
#                 raise RuntimeError("Cannot double-take a baton")
#             self._hasTaker = True

#         self._settle()

#     def _settle(self):



class WorkerThread:
    def __init__(self, pool, tid, dbState): # note: main thread context!
        self.curStep = -1 
        self.pool = pool
        self.tid = tid
        self.dbState = dbState
        # self.threadIdent = threading.get_ident()
        self.thread = threading.Thread(target=runThread, args=(self,))
        self.stepGate = threading.Event()

        # Let us have a DB connection of our own
        self._dbConn = DbConn()
        

    def start(self):
        self.thread.start()  # AFTER the thread is recorded

    def run(self):
        # initialization after thread starts, in the thread context
        # self.isSleeping = False
        self._dbConn.open()

        while self.curStep < self.pool.maxSteps:
            # stepNo = self.pool.waitForStep() # Step to run
            self.crossStepGate()  # self.curStep will get incremented
            self.doWork()

        # clean up
        self._dbConn.close()

    def verifyThreadSelf(self): # ensure we are called by this own thread
        if ( threading.get_ident() != self.thread.ident ): 
            raise RuntimeError("Unexpectly called from other threads")

    def verifyThreadMain(self): # ensure we are called by the main thread
        if ( threading.get_ident() != threading.main_thread().ident ): 
            raise RuntimeError("Unexpectly called from other threads")

    def verifyThreadAlive(self):
        if ( not self.thread.is_alive() ):
            raise RuntimeError("Unexpected dead thread")

    # def verifyIsSleeping(self, isSleeping):
    #     if ( isSleeping != self.isSleeping ):
    #         raise RuntimeError("Unexpected thread sleep status")

    # A gate is different from a barrier in that a thread needs to be "tapped"
    def crossStepGate(self):
        self.verifyThreadAlive()
        self.verifyThreadSelf() # only allowed by ourselves
        # self.verifyIsSleeping(False) # has to be awake

        logger.debug("Worker thread {} about to cross pool barrier".format(self.tid))
        # self.isSleeping = True # TODO: maybe too early?
        self.pool.crossPoolBarrier() # wait for all other threads
        
        # Wait again at the "gate", waiting to be "tapped"
        logger.debug("Worker thread {} about to cross the step gate".format(self.tid))
        # self.stepGate.acquire() # acquire lock immediately     
        self.stepGate.wait() 
        self.stepGate.clear()
        # self.stepGate.release() # release
        
        logger.debug("Worker thread {} woke up".format(self.tid))
        # Someone will wake us up here
        self.curStep += 1  # off to a new step...

    def tapStepGate(self): # give it a tap, release the thread waiting there
        self.verifyThreadAlive()
        self.verifyThreadMain() # only allowed for main thread
        # self.verifyIsSleeping(True) # has to be sleeping

        logger.debug("Tapping worker thread {}".format(self.tid))
        # self.stepGate.acquire()
        # logger.debug("Tapping worker thread {}, lock acquired".format(self.tid))
        self.stepGate.set() # wake up!
        # logger.debug("Tapping worker thread {}, notified!".format(self.tid))
        # self.isSleeping = False # No race condition for sure
        # self.stepGate.release() # this finishes before .wait() can return
        # logger.debug("Tapping worker thread {}, lock released".format(self.tid))
        time.sleep(0) # let the released thread run a bit, IMPORTANT, do it after release

    def doWork(self):
        logger.info("  Step {}, thread {}: ".format(self.curStep, self.tid))
        self.pool.dispatcher.doWork(self)

    def execSql(self, sql):
        return self.dbState.execSql(sql)
        

# We define a class to run a number of threads in locking steps.
class SteppingThreadPool:
    def __init__(self, dbState, numThreads, maxSteps, funcSequencer):
        self.numThreads = numThreads
        self.maxSteps = maxSteps
        self.funcSequencer = funcSequencer
        # Internal class variables
        self.dispatcher = WorkDispatcher(self, dbState)
        self.curStep = 0
        self.threadList = []
        # self.stepGate = threading.Condition() # Gate to hold/sync all threads
        # self.numWaitingThreads = 0
        
        # Thread coordination
        self.barrier = threading.Barrier(numThreads + 1) # plus main thread
        # self.lock = threading.Lock() # for critical section execution
        # self.mainGate = threading.Condition()

    # starting to run all the threads, in locking steps
    def run(self):                
        for tid in range(0, self.numThreads): # Create the threads
            workerThread = WorkerThread(self, tid, dbState)            
            self.threadList.append(workerThread)
            workerThread.start() # start, but should block immediately before step 0

        # Coordinate all threads step by step
        self.curStep = -1 # not started yet
        while(self.curStep < self.maxSteps):
            logger.debug("Main thread going to sleep")
            self.crossPoolBarrier()
            self.barrier.reset() # Other worker threads should now be at the "gate"            

            logger.debug("Main thread waking up, tapping worker threads".format(self.curStep)) # Now not all threads had time to go to sleep            
            self.tapAllThreads()

        # The threads will run through many steps
        for workerThread in self.threadList:
            workerThread.thread.join() # slight hack, accessing members

        logger.info("All threads finished")

    def crossPoolBarrier(self):
        if ( self.barrier.n_waiting == self.numThreads ):  # everyone else is waiting, inc main thread
            logger.info("<-- Step {} finished".format(self.curStep))
            self.curStep += 1 # we are about to get into next step. TODO: race condition here!    
            logger.debug(" ") # line break
            logger.debug("--> Step {} starts with main thread waking up".format(self.curStep)) # Now not all threads had time to go to sleep

        self.barrier.wait()

    def tapAllThreads(self): # in a deterministic manner
        wakeSeq = []
        for i in range(self.numThreads): # generate a random sequence
            if Dice.throw(2) == 1 :
                wakeSeq.append(i)
            else:
                wakeSeq.insert(0, i)
        logger.info("Waking up threads: {}".format(str(wakeSeq)))
        # TODO: set dice seed to a deterministic value
        for i in wakeSeq:
            self.threadList[i].tapStepGate()
            time.sleep(0) # yield

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
                raise RuntimeError("Cannot pop an empty queue") 
            index = self.firstIndex
            if ( index in self.inUse ):
                self.inUse.remove(index) # TODO: what about discard?

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
            if ( i in self.inUse ):
                raise RuntimeError("Cannot re-use same index in queue: {}".format(i))
            self.inUse.add(i)

    def release(self, i):
        with self._lock:
            self.inUse.remove(i) # KeyError possible

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
        self.isOpen = False
        
    def open(self): # Open connection
        if ( self.isOpen ):
            raise RuntimeError("Cannot re-open an existing DB connection")

        cfgPath = "../../build/test/cfg" 
        conn = taos.connect(host="127.0.0.1", config=cfgPath) # TODO: make configurable

        self._tdSql = TDSql()
        self._tdSql.init(conn.cursor())
        self.isOpen = True

    def resetDb(self): # reset the whole database, etc.
        if ( not self.isOpen ):
            raise RuntimeError("Cannot reset database until connection is open")
        self._tdSql.prepare() # Recreate database, etc.
        # tdSql.execute('show databases')

    def close(self):
        if ( not self.isOpen ):
            raise RuntimeError("Cannot clean up database until connection is open")
        self._tdSql.close()
        self.isOpen = False

    def execSql(self, sql):
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
        if self.tableNumQueue.isEmpty():
            return False
        tblNum = self.tableNumQueue.pop() # TODO: race condition!
        return "table_{}".format(tblNum)

    def execSql(self, sql): # using the main DB connection
        return self._dbConn.execSql(sql)

    def cleanUp(self):
        self._dbConn.close()      

class Task():
    def __init__(self, dbState):
        self.dbState = dbState

    def execute(self, workerThread):
        raise RuntimeError("Must be overriden by child class")

    def execSql(self, sql):
        return self.dbState.execute(sql)

class CreateTableTask(Task):
    def execute(self, wt):
        tIndex = dbState.addTable()
        logger.debug("    Creating a table {} ...".format(tIndex))
        wt.execSql("create table table_{} (ts timestamp, speed int)".format(tIndex))
        logger.debug("    Table {} created.".format(tIndex))
        dbState.releaseTable(tIndex)

class DropTableTask(Task):
    def execute(self, wt):
        tableName = dbState.getTableNameToDelete()
        if ( not tableName ): # May be "False"
            logger.info("    Cannot generate a table to delete, skipping...")
            return
        logger.info("    Dropping a table {} ...".format(tableName))
        wt.execSql("drop table {}".format(tableName))

class AddDataTask(Task):
    def execute(self, wt):
        ds = self.dbState
        logger.info("    Adding some data... numQueue={}".format(ds.tableNumQueue.toText()))
        tIndex = ds.pickAndAllocateTable()
        if ( tIndex == None ):
            logger.info("    No table found to add data, skipping...")
            return
        sql = "insert into table_{} values ('{}', {});".format(tIndex, ds.getNextTick(), ds.getNextInt())
        logger.debug("    Executing SQL: {}".format(sql))
        wt.execSql(sql) 
        ds.releaseTable(tIndex)
        logger.debug("    Finished adding data")

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
    def __init__(self, pool, dbState):
        self.pool = pool
        # self.totalNumMethods = 2
        self.tasks = [
            CreateTableTask(dbState),
            # DropTableTask(dbState),
            AddDataTask(dbState),
        ]

    def throwDice(self):
        max = len(self.tasks) - 1 
        dRes = random.randint(0, max)
        # logger.debug("Threw the dice in range [{},{}], and got: {}".format(0,max,dRes))
        return dRes

    def doWork(self, workerThread):
        dice = self.throwDice()
        task = self.tasks[dice]
        task.execute(workerThread)

if __name__ == "__main__":
    logger = logging.getLogger('myApp')
    logger.setLevel(LOGGING_LEVEL)
    ch = logging.StreamHandler()
    logger.addHandler(ch)

    Dice.seed(0) # initial seeding of dice
    dbState = DbState()
    threadPool = SteppingThreadPool(dbState, 3, 5, 0) 
    threadPool.run()
    logger.info("Finished running thread pool")
    dbState.cleanUp()
    
