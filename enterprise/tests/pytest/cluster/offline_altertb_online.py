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
    tdDnodes.cfg(1,"numOfMPeers", "1")
    tdDnodes.start(1)
    
    self.conn = taos.connect(config=tdDnodes.getSimCfgPath())
    tdSql.init(self.conn.cursor())
    tdSql.execute('reset query cache')
    tdSql.execute('create dnode 192.168.0.2')
    tdDnodes.deploy(2)
    tdDnodes.cfg(2,"numOfMPeers", "1")
    tdDnodes.start(2)
    tdSql.execute('create dnode 192.168.0.3')
    tdDnodes.deploy(3)
    tdDnodes.cfg(3,"numOfMPeers", "1")
    tdDnodes.start(3)
    tdLog.sleep(10)
    
  def run(self):
    self.ntables = 10
    self.rowsPerTable = 10
    self.replica = 3
    self.startTime = 1520000010000L

    tdSql.execute('create database db replica %d' %self.replica)
    tdSql.execute('use db')
    for tid in range(1,self.ntables+1):
      tdSql.execute('create table tb%d(ts timestamp, i int)' %tid)
    tdLog.sleep(10)

    tdLog.info("================= step1")
    tdLog.info("inert into %d records into each %d tables" %(self.rowsPerTable, self.ntables))
    for tid in range(1,self.ntables+1):
      startTime = self.startTime
      sqlcmd = ['insert into tb%d values' %tid]
      for rid in range(1,self.rowsPerTable+1):
        sqlcmd.append('(%ld, %d)' %(startTime+rid, rid))
      tdSql.execute(" ".join(sqlcmd))
    tdSql.query('select * from tb1')
    tdSql.checkRows(10)

    tdLog.info("================= step2")
    tdDnodes.forcestop(3)
    tdLog.sleep(2)

    tdLog.info("================= step3")
    tdLog.info("alter table tb%d" %1)
    tdSql.execute('alter table tb%d add column f float' %1)
    startTime = self.startTime
    for rid in range(1,11):
      tdSql.execute('insert into tb%d values(%ld, %d, %f)' %(1, startTime+rid+self.rowsPerTable, rid, 1.2*rid))

    tdLog.info("================= step4")
    tdDnodes.start(3)
    tdLog.sleep(5)

    tdLog.info("================= step5")
    tdSql.query('select * from tb%d' %1)
    tdSql.checkRows(20)

    
  def stop(self):
    tdSql.close()
    self.conn.close()
    tdLog.success("%s successfully executed" % __file__)
  
tdCases.addCluster(__file__, TDTestCase())
