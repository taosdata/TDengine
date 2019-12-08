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

# multiple threads process the same table 
import sys
import time
import datetime
import threading
import taos
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *

class TDTestCase:
  def init(self, conn):
    tdLog.debug("start to execute %s" % __file__)
    tdSql.init(conn.cursor())
  
  def importImp(self):
    conn = taos.connect(host='192.168.0.1', config=tdDnodes.getSimCfgPath())
    cursor = conn.cursor()
    cursor.execute('use db')
    threadIndex = int(threading.current_thread().name)
    ninserted = 0
    startTime = 1520000010000L + threadIndex
    for rid in range(self.nrows):
      sqlcmd = ['import into']
      for tid in range(1, self.ntables+1):
        sqlcmd.append('tb%d values(%ld, %d)' %(tid, startTime+rid*self.nthreads,  rid))
        ninserted += 1
        if (ninserted == 1000):
          affrows = cursor.execute(" ".join(sqlcmd))
          if (affrows == ninserted):
            ninserted = 0
            sqlcmd = ['import into']
          else:
            err = 'affected rows %d != expected %d' %(affrows, ninserted)
            print("\033[1;31m%s %s\033[0m\nfailed sqlcmd: %s" \
                         % (datetime.datetime.now(), err, sqlcmd[1]))
            self.successFlag = False
            sys.exit(1) 
            ninserted = 0
            sqlcmd = ['import into']
      if (ninserted > 0):
        affrows = cursor.execute(" ".join(sqlcmd))
        if (affrows == ninserted):
          ninserted = 0
        else:
          err = 'affected rows %d != expected %d' %(affrows, ninserted)
          print("\033[1;31m%s %s\033[0m\nfailed sqlcmd: %s" \
                         % (datetime.datetime.now(), err, sqlcmd[1]))
          self.successFlag = False
          sys.exit(1) 
          ninserted = 0
          sqlcmd = ['import into']
    self.queryFlag = False
    conn.close()
  
  def selectImp(self):
    conn = taos.connect(host='192.168.0.1', config=tdDnodes.getSimCfgPath())
    cursor = conn.cursor()
    cursor.execute('use db')
    count = 0
    while(self.queryFlag):
      cursor.execute('select * from tb')
      for line in cursor:
        continue
      print("query %d finished" %count)
      count += 1
    conn.close()

  def run(self):
    self.ntables = 2000
    self.nrows = 200
    self.nthreads = 5
    self.queryFlag = True
    self.successFlag = True

    tdDnodes.stop(1)
    tdDnodes.deploy(1)
    tdDnodes.start(1)

    tdLog.info("total importing thread number = %d" %self.nthreads)
    tdLog.info("total table number = %d" %self.ntables)
    tdLog.info("total records in each table = %ld" %self.nthreads*self.nrows)

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
    tdLog.info("%d threads begin to import data into all %d tables" %(self.nthreads, self.ntables))
    threads = []
    for tid in range (self.nthreads) :
      threadName = "%d" % (tid)
      thread = threading.Thread(target=self.importImp, name=threadName)
      thread.start()
      threads.append(thread)
    thread = threading.Thread(target=self.selectImp, name="select query")
    thread.start()
    threads.append(thread)
    
    for tid in range (self.nthreads) :
      threads[tid].join()
    threads[tid+1].join()

    if(~self.successFlag):
      tdSql.close()
      tdLog.exit('This test failed!')

  def stop(self):
    tdSql.close()
    tdLog.success("%s successfully executed" % __file__)
  
tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
