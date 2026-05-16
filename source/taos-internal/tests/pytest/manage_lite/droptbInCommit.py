###################################################################
 #       Copyright (c) 2016 by TAOS Technologies, Inc.
 #             All rights reserved.
 #
 #  This file is proprietary and confidential to TAOS Technologies.
 #  No part of this file may be reproduced, stored, transmitted, 
 #  disclosed or used in any form or by any means other than as 
 #  expressly provided by the written permission from Jianhui Tao
 #
###################################################################

# -*- coding: utf-8 -*-  

# insert combined with import, no cross tables between threads
import sys
import time
import datetime
import threading
import taos
from random import randint
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *

class TDTestCase:
  def init(self, conn):
    tdLog.debug("start to execute %s" % __file__)
    tdSql.init(conn.cursor())
  
  def insertImp(self):
    conn = taos.connect(host='192.168.0.1', config=tdDnodes.getSimCfgPath())
    cursor = conn.cursor()
    cursor.execute('use db')
    threadName = threading.current_thread().name
    print('Thread %s begin to inser data ==================' %threadName)
    threadIndex = int(threadName)
    ninserted = 0
    startTime =  self.startTime
    for tid in range(threadIndex+1, self.ntables+1, self.nthreads):
      for bid in range(self.nbatchs):
        sqlcmd = ['insert into tb%d using tb tags (%d) values' %(tid,tid)]
        ninserted = bid*self.nrowsPerBatch
        for rid in range(self.nrowsPerBatch):
          sqlcmd.append('(%ld, %d)' %(startTime+rid+ninserted, rid))
        try:
          affrows = cursor.execute(" ".join(sqlcmd))
          if (affrows != self.nrowsPerBatch):
            err = 'affected rows %d != expected %d' %(affrows, self.nrowsPerBatch)
            print("\033[1;31m%s %s\033[0m\nfailed insert sqlcmd: %d:%s" \
                        % (datetime.datetime.now(), err, tid, sqlcmd[1]))
            self.successFlag = False
            sys.exit(1)
        except:
          self.successFlag = False
          print('Thread %s Failed sql: %.80s' %(threadName, " ".join(sqlcmd)))
          sys.exit(1)
    print('Thread %d finished inserting' %threadIndex)
    conn.close()
    self.queryFlag = False

  def dropImp(self):
    time.sleep(5)
    threadName = threading.current_thread().name
    print('%s begin to drop tables ==================' %threadName)
    conn = taos.connect(host='192.168.0.1', config=tdDnodes.getSimCfgPath())
    cursor = conn.cursor()
    cursor.execute('use db')
    while(self.queryFlag):
      for tid in range(self.ntables):
        cursor.execute('drop table if exists tb%d' %tid)
        time.sleep(0.1)
    conn.close() 

  def run(self):
    self.ntables = 200
    self.nrowsPerBatch = 200+randint(0,100)
    self.nbatchs = 1000
    self.nthreads = 5 
    self.successFlag = True

    tdDnodes.stop(1)
    tdDnodes.deploy(1)
    tdDnodes.start(1)
    
    #only represent insert/import thread
    print('working threads = %d' %self.nthreads)
    print('total table numbers = %d' %self.ntables)
    print('insert/import rounds = %d' %self.nbatchs)
    print('in every round insert %d records' %self.nrowsPerBatch)
    self.startTime = 1520000010000L
    self.queryFlag = True

    tdSql.execute('reset query cache')
    tdSql.execute('drop database db')
    tdSql.execute('create database db ')
    tdLog.sleep(5)
    tdSql.execute('use db')

    tdLog.info("================= step1")
    tdLog.info("create %d table" %self.ntables)
    tdSql.execute('create table tb (ts timestamp, i int) tags (id int)')
    for tid in range(1, self.ntables+1):
      tdSql.execute('create table tb%d using tb tags (%d)' %(tid,tid))
    tdLog.sleep(10)

    tdLog.info("================= step2")
    tdLog.info("%d threads begin to insert data into all %d tables" %(self.nthreads, self.ntables))
    threads = []
    for tid in range (self.nthreads) :
      threadName = "%d" % (tid)
      thread = threading.Thread(target=self.insertImp, name=threadName)
      thread.start()
      threads.append(thread)
    thread = threading.Thread(target=self.dropImp, name="drop table in inserting")
    thread.start()
    threads.append(thread)
    
    for tid in range (self.nthreads+1) :
      threads[tid].join()

    if(~self.successFlag):
      tdSql.close()
      tdLog.exit('This test failed!')

  def stop(self):
    tdSql.close()
    tdLog.success("%s successfully executed" % __file__)
  
tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
