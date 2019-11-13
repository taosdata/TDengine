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
            print "\033[1;31m%s %s\033[0m" % (datetime.datetime.now(), err)
            ##sys.exit(1) 
            ninserted = 0
            sqlcmd = ['import into']
      if (ninserted > 0):
        affrows = cursor.execute(" ".join(sqlcmd))
        if (affrows == ninserted):
          ninserted = 0
        else:
          err = 'affected rows %d != expected %d' %(affrows, ninserted)
          print "\033[1;31m%s %s\033[0m" % (datetime.datetime.now(), err)
          ##sys.exit(1) 
          ninserted = 0
          sqlcmd = ['import into']
  
  def run(self):
    self.ntables = 2000
    self.nrows = 1000
    self.nthreads = 5

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
    
    for tid in range (self.nthreads) :
      threads[tid].join()

  def stop(self):
    tdSql.close()
    tdLog.success("%s successfully executed" % __file__)
  
tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
