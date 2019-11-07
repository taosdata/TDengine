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
    tdDnodes.cfg(1, "tables", "5")
    tdDnodes.cfg(1, "numOfTotalVnodes", "2")
    tdDnodes.start(1)
    
    self.conn = taos.connect(config=tdDnodes.getSimCfgPath())
    tdSql.init(self.conn.cursor())
    tdSql.execute('reset query cache')
    tdSql.execute('create dnode 192.168.0.2')
    tdDnodes.deploy(2)
    tdDnodes.cfg(2, "numOfMPeers", "1")
    tdDnodes.cfg(2, "tables", "5")
    tdDnodes.cfg(2, "numOfTotalVnodes", "2")
    tdDnodes.start(2)
    tdSql.execute('create dnode 192.168.0.3')
    tdDnodes.deploy(3)
    tdDnodes.cfg(3, "numOfMPeers", "1")
    tdDnodes.cfg(3, "tables", "5")
    tdDnodes.cfg(3, "numOfTotalVnodes", "2")
    tdDnodes.start(3)
    
  def run(self):
    self.ntables = 10
    self.startTime = 1520000010000L

    tdLog.info("================= step1")
    tdSql.execute('create database db replica 3')
    tdSql.execute('use db')
    for tid in range(1,self.ntables+1):
      tdSql.execute('create table tb%d(ts timestamp, i int)' %tid)
    tdLog.sleep(10)
    for tid in range(1,self.ntables+1):
      startTime = self.startTime
      for rid in range(1,11):
        tdSql.execute('insert into tb%d values(%ld, %d)' %(tid, startTime, rid))
        startTime += 1
    tdSql.query('select * from tb1')
    tdSql.checkRows(10)
    tdLog.sleep(3)

    tdLog.info("================= step2")
    tdSql.error('create table tb%d(ts timestamp, i int)' %(self.ntables+1))
    tdLog.sleep(10)
    
  def stop(self):
    tdSql.close()
    self.conn.close()
    tdLog.success("%s successfully executed" % __file__)
  
tdCases.addCluster(__file__, TDTestCase())
