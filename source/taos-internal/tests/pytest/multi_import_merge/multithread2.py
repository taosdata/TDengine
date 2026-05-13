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
from random import shuffle
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
    threadIndex = int(threading.current_thread().name)
    ninserted = 0
    startTime =  self.startTime
    for tid in range(threadIndex+1, self.ntables+1, self.nthreads):
      for bid in range(self.nbatchs):
        sqlcmd = ['import into tb%d values' %tid]
        ninserted = bid*self.nrowsPerBatch
        for rid in range(self.nrowsPerBatch):
          sqlcmd.append('(%ld, %d)' %(startTime+rid+ninserted, rid))
        affrows = cursor.execute(" ".join(sqlcmd))
        if (affrows != self.nrowsPerBatch):
          err = 'affected rows %d != expected %d' %(affrows, self.nrowsPerBatch)
          print("\033[1;31m%s %s\033[0m\nfailed insert sqlcmd: %d:%s" \
                      % (datetime.datetime.now(), err, tid, sqlcmd[1]))
          self.successFlag = False
          sys.exit(1)
    print('Thread %d finished inserting' %threadIndex)
    conn.close()

  def importImp(self):
    conn = taos.connect(host='192.168.0.1', config=tdDnodes.getSimCfgPath())
    cursor = conn.cursor()
    cursor.execute('use db')
    threadIndex = int(threading.current_thread().name)
    ninserted = 0
    startTime =  self.startTime
    for tid in range(threadIndex+1, self.ntables+1, self.nthreads):
      for bid in range(self.nbatchs):
        sqlcmd = ['import into tb%d values' %tid]
        ninserted = bid*self.nrowsPerBatch
        rids = range(self.nrowsPerBatch)
        shuffle(rids)
        for rid in rids:
          sqlcmd.append('(%ld, %d)' %(startTime+rid+ninserted, rid))
        affrows = cursor.execute(" ".join(sqlcmd))
        if (affrows != self.nrowsPerBatch):
          err = 'affected rows %d != expected %d' %(affrows, self.nrowsPerBatch)
          print("\033[1;31m%s %s\033[0m\nfailed import sqlcmd: %d:%s" \
                      % (datetime.datetime.now(), err, tid, sqlcmd[1]))
          self.successFlag = False
          sys.exit(1) 
    print('Thread %d finished importing' %threadIndex)
    self.queryFlag = False
    conn.close()
  
  def selectImp(self):
    conn = taos.connect(host='192.168.0.1', config=tdDnodes.getSimCfgPath())
    cursor = conn.cursor()
    cursor.execute('use db')
    count = 0
    while(True):
      cursor.execute('select * from tb')
      for line in cursor:
        continue
      print("query %d finished" %count)
      if (count == 200):break
      count += 1
    conn.close()

  def selectlastImp(self):
    conn = taos.connect(host='192.168.0.1', config=tdDnodes.getSimCfgPath())
    cursor = conn.cursor()
    cursor.execute('use db')
    while(self.queryFlag):
      cursor.execute('select last(*) from tb ')
      print('select last(*) from tb ===================')
      print(cursor.fetchall())
      time.sleep(5)
    conn.close()  

  def selectcountImp(self):
    conn = taos.connect(host='192.168.0.1', config=tdDnodes.getSimCfgPath())
    cursor = conn.cursor()
    cursor.execute('use db')
    while(self.queryFlag):
      cursor.execute('select count(*) from tb')
      print('select count(*) from tb ==================')
      print(cursor.fetchall())
      time.sleep(5)
    conn.close() 

  def run(self):
    self.ntables = 200
    self.nrowsPerBatch = 200+randint(0,100)
    self.nbatchs = 10
    self.nthreads = 5 
    self.successFlag = True

    tdDnodes.stop(1)
    tdDnodes.deploy(1)
    tdDnodes.start(1)
    
    #only represent insert/import thread
    print('working threads = %d' %self.nthreads)
    print('total table numbers = %d' %self.ntables)
    print('insert/import rounds = %d' %self.nbatchs)
    print('in every round insert/import %d records' %self.nrowsPerBatch)
    self.startTime = 1520000010000L
    self.queryFlag = True

    tdSql.execute('reset query cache')
    tdSql.execute('drop database if exists db')
    tdSql.execute('create database db tables 50')
    tdLog.sleep(5)
    tdSql.execute('use db')

    tdLog.info("================= step1")
    tdLog.info("create %d table" %self.ntables)
    tdSql.execute('create table tb (ts timestamp, i int) tags (id int)')
    for tid in range(1, self.ntables+1):
      tdSql.execute('create table tb%d using tb tags (%d)' %(tid,tid))
    tdLog.sleep(10)

    tdLog.info("================= step2")
    tdLog.info("%d threads begin to import/insert data into all %d tables" %(self.nthreads, self.ntables))
    threads = []
    for tid in range (0,1) :
      threadName = "%d" % (tid)
      thread = threading.Thread(target=self.insertImp, name=threadName)
      thread.start()
      threads.append(thread)
    for tid in range (1,self.nthreads) :
      threadName = "%d" % (tid)
      thread = threading.Thread(target=self.importImp, name=threadName)
      thread.start()
      threads.append(thread)
    thread = threading.Thread(target=self.selectlastImp, name="last select query")
    thread.start()
    threads.append(thread)
    thread = threading.Thread(target=self.selectcountImp, name="count select query")
    thread.start()
    threads.append(thread)
    
    for tid in range (self.nthreads+2) :
      threads[tid].join()

    if(not self.successFlag):
      tdSql.close()
      tdLog.exit('This test failed!')

  def stop(self):
    tdSql.close()
    tdLog.success("%s successfully executed" % __file__)
  
tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
