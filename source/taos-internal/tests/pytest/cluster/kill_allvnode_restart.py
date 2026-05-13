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
    tdDnodes.cfg(1,"tables", "5")
    tdDnodes.start(1)
    
    self.conn = taos.connect(host='192.168.0.1', config=tdDnodes.getSimCfgPath())
    tdSql.init(self.conn.cursor())
    tdSql.execute('reset query cache')
    tdSql.execute('create dnode 192.168.0.2')
    tdDnodes.deploy(2)
    tdDnodes.cfg(2,"numOfMPeers", "1")
    tdDnodes.cfg(2,"tables", "5")
    tdDnodes.start(2)
    tdSql.execute('create dnode 192.168.0.3')
    tdDnodes.deploy(3)
    tdDnodes.cfg(3,"numOfMPeers", "1")
    tdDnodes.cfg(3,"tables", "5")
    tdDnodes.start(3)
    tdLog.sleep(5)

  def run(self):
    self.ntables = 20
    self.startTime = 1520000010000L
    self.rowsPerTable = 1
    self.replica = 2

    tdLog.info("================= step1")
    tdLog.info("insert %d records into each %d tables" % (self.rowsPerTable, self.ntables))
    tdSql.execute('create database db replica %d ' % (self.replica))
    tdSql.execute('use db')
    tdSql.execute('create table tb (ts timestamp, i int) tags (id int)')
    for tid in range(1,self.ntables+1):
      tdSql.execute('create table tb%d using tb tags (%d)' %(tid, tid))
    tdLog.sleep(10)
    for tid in range(1,self.ntables+1):
      startTime = self.startTime
      sqlcmd = ['insert into tb%d values' % (tid)]
      for rid in range(1,self.rowsPerTable+1):
        sqlcmd.append("(%ld, %d)" % (startTime, rid))
        startTime += 1
      tdSql.execute(" ".join(sqlcmd))
    self.startTime += self.rowsPerTable
    tdSql.query('select count(*) from tb')
    tdSql.checkData(0, 0, self.ntables*self.rowsPerTable)

    tdLog.info("================= step2")
    tdLog.info("kill all dnodes where exist vgroupId %d" %(self.ntables/5))
    tdSql.query('show vgroups')
    tmp = tdSql.getData(0,3)
    vgVnode1 = int(tmp[-1])
    tmp = tdSql.getData(0,7)
    vgVnode2 = int(tmp[-1])
    tdDnodes.forcestop(vgVnode1)
    tdDnodes.forcestop(vgVnode2)

    tdLog.info("================= step3")
    tdSql.error("select * from tb")

    tdLog.info("================= step4")
    tdDnodes.start(vgVnode1)
    tdSql.error("select * from tb")

    tdLog.info("================= step5")
    tdDnodes.start(vgVnode2)
    tdSql.query('select count(*) from tb')
    tdSql.checkData(0, 0, self.ntables*self.rowsPerTable)

  def stop(self):
    tdSql.close()
    self.conn.close()
    tdLog.success("%s successfully executed" % __file__)
  
tdCases.addCluster(__file__, TDTestCase())