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
import threading
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
    tdDnodes.cfg(1,"numOfMPeers", "1")
    tdDnodes.cfg(1,"tables", "4")
    tdDnodes.start(1)
    
    self.conn = taos.connect(config=tdDnodes.getSimCfgPath())
    tdSql.init(self.conn.cursor())
    tdSql.execute('reset query cache')
    tdSql.execute('create dnode 192.168.0.2')
    tdDnodes.deploy(2)
    tdDnodes.cfg(2,"numOfMPeers", "1")
    tdDnodes.cfg(2,"tables", "4")
    tdDnodes.start(2)
    tdSql.execute('create dnode 192.168.0.3')
    tdDnodes.deploy(3)
    tdDnodes.cfg(3,"numOfMPeers", "1")
    tdDnodes.cfg(3,"tables", "4")
    tdDnodes.start(3)
    tdLog.sleep(5)

  def run(self):
    self.ntables = 20
    self.rowsPerTable = 10
    self.startTime = 1520000010000L
    self.replica = 3

    tdLog.info("================= step1")
    tdLog.info("insert into %d records into each %d tables" %(self.rowsPerTable, self.ntables))
    tdSql.execute('create database db replica %d' %self.replica)
    tdSql.execute('use db')
    for tid in range(1,self.ntables+1):
      tdSql.execute('create table tb%d(ts timestamp, i int)' %tid)
    tdLog.sleep(5)
    for tid in range(1,self.ntables+1):
      startTime = self.startTime
      sqlcmd = ["insert into"]
      for rid in range(1, self.rowsPerTable+1):
        sqlcmd.append("tb%d values(%ld, %d)" %(tid, startTime, rid))
        startTime += 1
      tdSql.execute(" ".join(sqlcmd))
    self.startTime += self.rowsPerTable
    tdLog.sleep(5)

    tdLog.info("================= step2")
    tdSql.query('select * from tb%d' %1)
    tdSql.checkRows(self.rowsPerTable)

    tdLog.info("================= step3")
    tdDnodes.forcestop(2)
    tdDnodes.forcestop(3)
    tdLog.sleep(10)

    tdLog.info("================= step4")
    tdSql.error('select * from tb%d' %1)

    tdLog.info("================= step5")
    tdSql.error('insert into tb%d values(%ld, %d)' %(1, self.startTime, 1))

    tdLog.info("================= step6")
    tdDnodes.start(2)
    tdLog.sleep(10)

    tdLog.info("================= step7")
    tdSql.error('select * from tb%d' %1)

    tdLog.info("================= step8")
    tdSql.error('insert into tb%d values(%ld, %d)' %(1, self.startTime, 1))

    tdLog.info("================= step9")
    tdDnodes.start(3)
    tdLog.sleep(10)

    tdLog.info("================= step10")
    tdSql.execute('insert into tb%d values(%ld, %d)' %(1, self.startTime, 1))
    tdLog.sleep(10)
    sqlcmd = 'select count(*) from tb%d' %1
    tdSql.query(sqlcmd)
    tdSql.checkData(0, 0, self.rowsPerTable+1)
      
  def stop(self):
    tdSql.close()
    self.conn.close()
    tdLog.success("%s successfully executed" % __file__)
  
tdCases.addCluster(__file__, TDTestCase())
