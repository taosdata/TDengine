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
import taos
import datetime
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *
import threading

class TDTestCase:
  def init(self, conn):
    tdLog.debug("start to execute %s" % __file__)
    tdSql.init(conn.cursor())

  def testquery(self, newconn):
    print('Thread %s begin to query' %threading.current_thread().name)
    for i in range(20):
      c1 = newconn.cursor()
      c1.execute('select last(*) from db.stb')
      c1.fetchall()
      c1.execute('select * from db.stb')
      c1.fetchall()
      c1.execute('select max(speed) from db.stb where id>4')
      c1.fetchall()
      c1.close()
    return
    
  def run(self):
    self.ntables = 10
    self.rowsPerTable = 100
    self.startTime = 1520000010000L

    tdDnodes.stop(1)
    tdDnodes.deploy(1)
    tdDnodes.start(1)

    tdSql.execute('reset query cache')
    tdSql.execute('drop database db')
    tdSql.execute('create database db tables 4')
    tdLog.sleep(5)
    tdSql.execute('use db')

    tdLog.info("================= step1")
    tdLog.info("create %d tables" %self.ntables)
    tdSql.execute('create table stb (ts timestamp, speed int) tags(id int)')
    for tid in range(1, self.ntables+1):
      tdSql.execute('create table tb%d using stb tags(%d)' %(tid, tid))

    tdLog.info("================= step2")
    tdLog.info("insert %d data" %self.rowsPerTable)
    startTime = self.startTime
    for tid in range(1, self.ntables+1):
      sqlcmd = ['insert into tb%d values' %tid]
      for rid in range(1,self.rowsPerTable+1):
        sqlcmd.append('(%ld, %d)' %(startTime+rid, rid))
      tdSql.execute(" ".join(sqlcmd))
  
    tdLog.info("================= step3")
    tdSql.query('select count(*) from stb')
    tdSql.checkData(0, 0, self.ntables*self.rowsPerTable)

    tdLog.info("================= step4")
    tdLog.info("repeatly open and close cursor using the same conn")
    newconn = taos.connect(host='192.168.0.1', config=tdDnodes.getSimCfgPath())
    threads = []
    for tid in range (3) :
      threadName = "%d" % (tid)
      thread = threading.Thread(target=self.testquery, args=(newconn,), name=threadName)
      thread.start()
      threads.append(thread)
    for tid in range (3) :
      threads[tid].join()
    newconn.close()

  def stop(self):
    tdSql.close()
    tdLog.success("%s successfully executed" % __file__)
  
tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
