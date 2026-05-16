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
  def init(self):
    tdLog.debug("start to execute %s" % __file__)
    tdLog.info("prepare cluster") 
    tdDnodes.stopAll()
    tdDnodes.deploy(1)
    tdDnodes.start(1)
    
    self.conn = taos.connect(config=tdDnodes.getSimCfgPath())
    tdSql.init(self.conn.cursor())
    tdSql.execute('reset query cache')
    tdSql.execute('create dnode 192.168.0.2')
    tdDnodes.deploy(2)
    tdDnodes.start(2)
    tdSql.execute('create dnode 192.168.0.3')
    tdDnodes.deploy(3)
    tdDnodes.start(3)
    
  def run(self):
    self.replica = 3
    self.startTime = 1520000010000L
    self.ntables = 10
    self.rowsPerTable = 10

    tdLog.info("================= step1")
    tdLog.info("insert into each %d tables %d records" %(self.ntables, self.rowsPerTable))
    tdSql.execute('create database db replica %d' %self.replica)
    tdSql.execute('use db')
    for tid in range(1,self.ntables):
      tdSql.execute('create table tb%d(ts timestamp, i int)' %tid)
    tdLog.sleep(10)
    startTime = self.startTime
    for tid in range(1,self.ntables+1):
      sqlcmd = ['insert into tb%d values' %(tid)]
      for rid in range(1,self.rowsPerTable+1):
        sqlcmd.append('(%ld, %d)' %(startTime+rid, rid))
      tdSql.execute(" " .join(sqlcm))
    startTime += 10
    tdSql.query('select * from tb1')
    tdSql.checkRows(self.rowsPerTable)

    tdLog.info("================= step2")
    tdLog.info("drop the middle table")
    tdSql.execute('drop table tb%d' %5)
    tdLog.sleep(10)

    tdLog.info("================= step3")
    tdLog.info("insert 10 data to the left tables")
    for rid in range(1,11):
      for tid in range(1,4):
        tdSql.execute('insert into tb%d values(%ld, %d)' %(tid, startTime+rid, rid))
      for tid in range(6,11):
        tdSql.execute('insert into tb%d values(%ld, %d)' %(tid, startTime+rid, rid)) 
    startTime += 10
    self.rowsPerTable += 10
    tdSql.query('select * from tb1')
    tdSql.checkRows(self.rowsPerTable)

    tdLog.info("================= step4")
    tdLog.info("create the middle table")
    tdSql.execute('create table tb%d(ts timestamp, i int)' %5)
    tdLog.sleep(10)

    tdLog.info("================= step5")
    tdLog.info("insert into each %d tables %d records" %(self.ntables, 10))
    for tid in range(1,self.ntables+1):
      sqlcmd = ['insert into tb%d values' %(tid)]
      for rid in range(1,11):
        sqlcmd.append('(%ld, %d)' %(startTime+rid, rid))
      tdSql.execute(" " .join(sqlcm))
    startTime += 10
    tdSql.query('select * from tb5')
    tdSql.checkRows(10)
    
    tdLog.info("================= step6")
    tdLog.info("drop the first table")
    tdSql.execute('drop table tb%d' %1)
    tdLog.sleep(10)

    tdLog.info("================= step7")
    tdLog.info("insert 10 data to the left tables")
    for tid in range(2,self.ntables+1):
      sqlcmd = ['insert into tb%d values' %(tid)]
      for rid in range(1,11):
        sqlcmd.append('(%ld, %d)' %(startTime+rid, rid))
      tdSql.execute(" " .join(sqlcm))
    startTime += 10
    tdSql.query('select * from tb5')
    tdSql.checkRows(20)

    tdLog.info("================= step8")
    tdLog.info("create the first table")
    tdSql.execute('create table tb%d(ts timestamp, i int)' %1)
    tdLog.sleep(10)

    tdLog.info("================= step9")
    tdLog.info("insert into each %d tables %d records" %(self.ntables, 10))
    for tid in range(1,self.ntables+1):
      sqlcmd = ['insert into tb%d values' %(tid)]
      for rid in range(1,11):
        sqlcmd.append('(%ld, %d)' %(startTime+rid, rid))
      tdSql.execute(" " .join(sqlcm))
    startTime += 10
    tdSql.query('select * from tb1')
    tdSql.checkRows(10)

    tdLog.info("================= step10")
    tdLog.info("drop the last table")
    tdSql.execute('drop table tb%d' %10)
    tdLog.sleep(10)

    tdLog.info("================= step11")
    tdLog.info("insert 10 data to the left tables")
    for tid in range(1,self.ntables):
      sqlcmd = ['insert into tb%d values' %(tid)]
      for rid in range(1,11):
        sqlcmd.append('(%ld, %d)' %(startTime+rid, rid))
      tdSql.execute(" " .join(sqlcm))
    startTime += 10
    tdSql.checkRows(40)

    tdLog.info("================= step12")
    tdLog.info("create the last table")
    tdSql.execute('create table tb%d(ts timestamp, i int)' %10)
    tdLog.sleep(10)

    tdLog.info("================= step13")
    tdLog.info("insert into each %d tables %d records" %(self.ntables, 10))
    for tid in range(1,self.ntables+1):
      sqlcmd = ['insert into tb%d values' %(tid)]
      for rid in range(1,11):
        sqlcmd.append('(%ld, %d)' %(startTime+rid, rid))
      tdSql.execute(" " .join(sqlcm))
    startTime += 10
    tdSql.query('select * from tb10')
    tdSql.checkRows(10)

    tdLog.info("================= step14")
    tdLog.info('drop database db')
    tdSql.execute('drop database db')

    
  def stop(self):
    tdSql.close()
    self.conn.close()
    tdLog.success("%s successfully executed" % __file__)
  
tdCases.addCluster(__file__, TDTestCase())
