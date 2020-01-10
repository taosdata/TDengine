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

import os
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
    tdDnodes.cfg(1, "numOfMPeers", "1")
    tdDnodes.cfg(1, "tables", "4")
    tdDnodes.cfg(1, "monitor", "1")
    tdDnodes.start(1)
    
    self.conn = taos.connect(config=tdDnodes.getSimCfgPath())
    tdSql.init(self.conn.cursor())
    tdSql.execute('reset query cache')

  def run(self):
    self.ntables = 20
    self.startTime = 1520000010000L

    tdLog.info("================= step1")
    tdLog.info("insert 10 records into %d tables in single dnode" %self.ntables)
    tdSql.execute('drop database if exists db')
    tdSql.execute('create database db')
    tdSql.execute('use db')
    for tid in range(1,self.ntables+1):
      tdSql.execute('create table tb%d(ts timestamp, i int)' %tid)
    tdLog.sleep(5)
    for tid in range(1,self.ntables+1):
      startTime = self.startTime
      for rid in range(1,11):
        tdSql.execute('insert into tb%d values(%ld, %d)' %(tid, startTime, rid))
        startTime += 1
    self.startTime += 10
    tdSql.query('select * from tb1')
    tdSql.checkRows(10)
    tdLog.sleep(5)

    tdLog.info("================= step2")
    tdLog.info("dnode 2 join the cluster")
    tdSql.execute('create dnode 192.168.0.2')
    tdDnodes.deploy(2)
    tdDnodes.cfg(2, "numOfMPeers", "1")
    tdDnodes.cfg(2, "tables", "4")
    tdDnodes.cfg(2, "monitor", "0")
    tdDnodes.start(2)
    queryRows = tdSql.query('show dnodes')
    for i in range(queryRows):
      tdLog.info("%s:%s" %(tdSql.getData(i,0), tdSql.getData(i,5)))
    while(True):
      tdLog.sleep(20)
      stopFlag = True
      queryRows = tdSql.query('show dnodes')
      for i in range(queryRows):
        if (tdSql.getData(i,5) != "balanced"): stopFlag = False
      if (stopFlag): break

    tdLog.info("================= step3")
    tdLog.info("alter database replica to 2")
    tdSql.execute('alter database db replica 2')
    queryRows = tdSql.query('show dnodes')
    for i in range(queryRows):
      tdLog.info("%s:%s" %(tdSql.getData(i,0), tdSql.getData(i,5)))
    while(True):
      tdLog.sleep(20)
      stopFlag = True
      queryRows = tdSql.query('show dnodes')
      for i in range(queryRows):
        if (tdSql.getData(i,5) != "balanced"): stopFlag = False
      if (stopFlag): break

    tdLog.info("================= step4")
    tdLog.info("insert 10 records into %d tables" %self.ntables)
    for tid in range(1,self.ntables+1):
      startTime = self.startTime
      for rid in range(1,11):
        tdSql.execute('insert into tb%d values(%ld, %d)' %(tid, startTime, rid))
        startTime += 1
    self.startTime += 10
    tdSql.query('select * from tb1')
    tdSql.checkRows(20)
    tdLog.sleep(5)

    tdLog.info("================= step5")
    tdLog.info("check database replica")
    queryRows = tdSql.query('show databases')
    for i in range(queryRows):
      if (tdSql.getData(i,0) == 'db'):
        tdSql.checkData(i, 4, 2)

  def stop(self):
    tdSql.close()
    self.conn.close()
    tdLog.success("%s successfully executed" % __file__)

tdCases.addCluster(__file__, TDTestCase())

