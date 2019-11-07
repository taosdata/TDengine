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
    tdDnodes.cfg(1,"numOfMPeers", "1")
    tdDnodes.cfg(1,"commitTime", "30")
    tdDnodes.start(1)
    
    self.conn = taos.connect(config=tdDnodes.getSimCfgPath())
    tdSql.init(self.conn.cursor())
    tdSql.execute('reset query cache')
    tdSql.execute('create dnode 192.168.0.2')
    tdDnodes.deploy(2)
    tdDnodes.cfg(2,"numOfMPeers", "1")
    tdDnodes.cfg(2,"commitTime", "30")
    tdDnodes.start(2)
    tdSql.execute('create dnode 192.168.0.3')
    tdDnodes.deploy(3)
    tdDnodes.cfg(3,"numOfMPeers", "1")
    tdDnodes.cfg(3,"commitTime", "30")
    tdDnodes.start(3)
    
  def run(self):
    self.ntables = 100
    self.startTime = 1520000010000L

    tdSql.execute('create database db replica 2 ctime 30')
    tdSql.execute('use db')
    for tid in range(1,self.ntables+1):
      tdSql.execute('create table tb%d(ts timestamp, i int)' %tid)
    tdLog.sleep(3)

    tdLog.info("================= step1")
    for tid in range(1,self.ntables+1):
      startTime = self.startTime
      for rid in range(1,11):
        tdSql.execute('insert into tb%d values(%ld, %d)' %(tid, startTime, rid))
        startTime += 1
    tdSql.query('select * from tb1')
    tdSql.checkRows(10)
    tdLog.sleep(30)

    tdLog.info("================= step2")
    dnodesDir = tdDnodes.getDnodesRootDir()
    dataDir = dnodesDir + '/dnode1/data/data'
    dnode1SizeOld = 0
    for vnode in os.listdir(dataDir):
      vnodeDir = os.path.join(dataDir, vnode)
      for file in os.listdir(vnodeDir):
        dnode1SizeOld += os.path.getsize(os.path.join(vnodeDir, file))

    tdLog.info("================= step3")
    tdSql.execute('drop dnode 192.168.0.3')
    tdLog.sleep(30)

    tdLog.info("================= step4")
    tdSql.query('select * from tb1')
    tdSql.checkRows(10)

    tdLog.info("================= step5")
    dataDir = dnodesDir + '/dnode1/data/data'
    dnode1SizeNew = 0
    for vnode in os.listdir(dataDir):
      vnodeDir = os.path.join(dataDir, vnode)
      for file in os.listdir(vnodeDir):
        dnode1SizeNew += os.path.getsize(os.path.join(vnodeDir, file))
    if (dnode1SizeNew == dnode1SizeOld):
      tdLog.exit("Balancing Failure! File size before: %f == now: %f" %(dnode1SizeOld, dnode1SizeNew))
    else:
      tdLog.info("Balancing Success")
    
  def stop(self):
    tdSql.close()
    self.conn.close()
    tdLog.success("%s successfully executed" % __file__)
  
tdCases.addCluster(__file__, TDTestCase())
