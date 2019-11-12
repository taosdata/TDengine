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
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *

class TDTestCase:
  def init(self, conn):
    tdLog.debug("start to execute %s" % __file__)
    tdSql.init(conn.cursor())
    
  def run(self):
    self.ntables = 10
    self.rowsPerTable = 10
    self.startTime = 1520000010000L

    tdLog.info("prepare database:db") 
    tdSql.execute('reset query cache')
    tdSql.execute('drop database db')
    tdSql.execute('create database db tables 5')
    tdLog.sleep(5)
    tdSql.execute('use db')

    tdLog.info("================= step1")
    tdLog.info("create %d table" %self.ntables)
    for tid in range(1,self.ntables+1):
      tdSql.execute('create table tb%d (ts timestamp, i int)' %tid)
    tdLog.sleep(5)

    tdLog.info("================= step2")
    tdLog.info("import %d data into each %d tables" %(self.rowsPerTable, self.ntables))
    startTime = self.startTime
    for rid in range(self.ntables*2, 0, -2):
      sqlcmd = ['import into']
      for tid in range(1,self.ntables+1):
        sqlcmd.append('tb%d values(%ld, %d)' %(tid, startTime+rid, rid))
      tdSql.execute(" ".join(sqlcmd))
    tdSql.query('select * from tb1')
    tdSql.checkRows(self.rowsPerTable)

    tdLog.info("================= step3")
    tdLog.info("drop the table 1")
    tdSql.execute("drop table tb%d" %1)

    tdLog.info("================= step4")
    tdLog.info("import 5 data later with partly overlap into each left %d tables" %(9))
    startTime = self.startTime + 16
    for rid in range(1,6):
      sqlcmd = ['import into']
      for tid in range(2,self.ntables+1):
        sqlcmd.append('tb%d values(%ld, %d)' %(tid, startTime+rid, rid))
      tdSql.execute(" ".join(sqlcmd))
    self.rowsPerTable += 3
    tdSql.query('select * from tb2')
    tdSql.checkRows(self.rowsPerTable)

    tdLog.info("================= step5")
    tdLog.info("create the table 1")
    tdSql.execute("create table tb%d (ts timestamp, i int)" %1)

    tdLog.info("================= step6")
    tdLog.info("import 7 data before with partly overlap into each %d tables" %(self.ntables))
    startTime = self.startTime +15
    for rid in range(1,8):
      sqlcmd = ['import into']
      for tid in range(1,self.ntables+1):
        sqlcmd.append('tb%d values(%ld, %d)' %(tid, startTime+rid, rid))
      tdSql.execute(" ".join(sqlcmd))
    self.rowsPerTable += 1
    tdSql.query('select * from tb2')
    tdSql.checkRows(self.rowsPerTable)

    tdLog.info("================= step7")
    tdLog.info("drop the table 5")
    tdSql.execute("drop table tb%d" %5)

    tdLog.info("================= step8")
    tdLog.info("import 9 data before with overlap into each left %d tables" %(9))
    startTime = self.startTime +14
    for rid in range(1,10):
      sqlcmd = ['import into']
      for tid in range(1,4):
        sqlcmd.append('tb%d values(%ld, %d)' %(tid, startTime+rid, rid))
      tdSql.execute(" ".join(sqlcmd))
    for rid in range(1,10):
      sqlcmd = ['import into']
      for tid in range(6,self.ntables+1):
        sqlcmd.append('tb%d values(%ld, %d)' %(tid, startTime+rid, rid))
      tdSql.execute(" ".join(sqlcmd))
    self.rowsPerTable += 2
    tdSql.query('select * from tb2')
    tdSql.checkRows(self.rowsPerTable)

    tdLog.info("================= step9")
    tdLog.info("create the table 5")
    tdSql.execute("create table tb%d (ts timestamp, i int)" %5)

    tdLog.info("================= step10")
    tdLog.info("import 11 data before with overlap into each %d tables" %(self.ntables))
    startTime = self.startTime +13
    for rid in range(1,12):
      sqlcmd = ['import into']
      for tid in range(1,self.ntables+1):
        sqlcmd.append('tb%d values(%ld, %d)' %(tid, startTime+rid, rid))
      tdSql.execute(" ".join(sqlcmd))
    self.rowsPerTable += 1
    tdSql.query('select * from tb2')
    tdSql.checkRows(self.rowsPerTable)

    tdLog.info("================= step11")
    tdLog.info("drop the table 10")
    tdSql.execute("drop table tb%d" %10)

    tdLog.info("================= step12")
    tdLog.info("import 14 data before with overlap into each left %d tables" %(9))
    startTime = self.startTime +12
    for rid in range(1,15):
      sqlcmd = ['import into']
      for tid in range(1,self.ntables):
        sqlcmd.append('tb%d values(%ld, %d)' %(tid, startTime+rid, rid))
      tdSql.execute(" ".join(sqlcmd))
    self.rowsPerTable += 3
    tdSql.query('select * from tb2')
    tdSql.checkRows(self.rowsPerTable)

    tdLog.info("================= step13")
    tdLog.info("create the table 10")
    tdSql.execute("create table tb%d (ts timestamp, i int)" %10)

    tdLog.info("================= step14")
    tdLog.info("import 17 data before with overlap into each %d tables" %(self.ntables))
    startTime = self.startTime +11
    for rid in range(1,18):
      sqlcmd = ['import into']
      for tid in range(1,self.ntables+1):
        sqlcmd.append('tb%d values(%ld, %d)' %(tid, startTime+rid, rid))
      tdSql.execute(" ".join(sqlcmd))
    self.rowsPerTable += 2
    tdSql.query('select * from tb2')
    tdSql.checkRows(self.rowsPerTable)

  def stop(self):
    tdSql.close()
    tdLog.success("%s successfully executed" % __file__)
  
tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
