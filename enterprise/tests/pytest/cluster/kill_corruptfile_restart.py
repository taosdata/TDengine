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
    tdDnodes.cfg(1,"commitTime", "30")
    tdDnodes.start(1)
    
    self.conn = taos.connect(host='192.168.0.1', config=tdDnodes.getSimCfgPath())
    tdSql.init(self.conn.cursor())
    tdSql.execute('reset query cache')
    tdSql.execute('create dnode 192.168.0.2')
    tdDnodes.deploy(2)
    tdDnodes.cfg(2,"numOfMPeers", "1")
    tdDnodes.cfg(2,"tables", "5")
    tdDnodes.cfg(2,"commitTime", "30")
    tdDnodes.start(2)
    tdSql.execute('create dnode 192.168.0.3')
    tdDnodes.deploy(3)
    tdDnodes.cfg(3,"numOfMPeers", "1")
    tdDnodes.cfg(3,"tables", "5")
    tdDnodes.cfg(3,"commitTime", "30")
    tdDnodes.start(3)
    tdLog.sleep(5)

  def run(self):
    self.ntables = 20
    self.rowsPerTable = 10
    self.startTime = 1520000010000L
    self.replica = 3
    self.ctime   = 30

    tdLog.info("================= step1")
    tdLog.info("insert into %d records into each %d tables" %(self.rowsPerTable, self.ntables))
    tdSql.execute('create database db replica %d ctime %d' %(self.replica, self.ctime))
    tdLog.sleep(5)
    tdSql.execute('use db')
    tdSql.execute('create table tb (ts timestamp, i int) tags(id int)')
    for tid in range(1,self.ntables+1):
      tdSql.execute('create table tb%d using tb tags(%d)' %(tid,tid))
    tdLog.sleep(5)
    for tid in range(1,self.ntables+1):
      startTime = self.startTime
      sqlcmd = ["insert into tb%d values" %(tid)]
      for rid in range(1, self.rowsPerTable+1):
        sqlcmd.append("(%ld, %d)" %(startTime+rid, rid))
      tdSql.execute(" ".join(sqlcmd))
    self.startTime += self.rowsPerTable
    tdLog.sleep(40)

    tdLog.info("================= step2")
    tdSql.query('select count(*) from tb')
    tdSql.checkData(0, 0, self.ntables*self.rowsPerTable)

    tdLog.info("================= step3")
    tdDnodes.forcestop(3)
    tdLog.sleep(5)

    tdLog.info("================= step4")
    tdLog.info("find files in dnode3")
    dnodesDir  = tdDnodes.getDnodesRootDir()
    dataDir    = dnodesDir + '/dnode3/data/data'
    vnodes     = os.listdir(dataDir)
    vnodeDir   = os.path.join(dataDir, vnodes[0])
    vnodefiles = os.listdir(vnodeDir)

    tdLog.info("================= step5")
    for i in range(len(vnodefiles)):
      fileToDel = os.path.join(vnodeDir, vnodefiles[0])
      if (filelToDel.find('data') >0): break
    cmd = 'rm -rf %s' % (fileToDel)
    if os.system(cmd) != 0 :
      tdLog.exit(cmd)
    tdLog.debug("%s" % (cmd))

    tdLog.info("================= step6")
    tdDnodes.start(3)
    tdSql.query('select count(*) from tb')
    tdSql.checkData(0, 0, self.ntables*self.rowsPerTable)
    tdLog.sleep(40)

    tdLog.info("================= step7")
    fileToRes = fileToDel
    fileToRes0 = fileToDel[:-1] + '0'
    fileToRes1 = fileToDel[:-1] + '1'
    if ((os.path.exists(fileToRes)) or (os.path.exists(fileToRes0)) or (os.path.exists(fileToRes1))):
      tdLog.debug("%s has been restored" % (fileToDel))
    else:
      tdLog.exit("%s has not been restored" % (fileToDel))

    tdLog.info("================= step8")
    tdLog.info("insert 1 data again")
    sqlcmd = ['insert into']
    for tid in range(1, self.ntables+1):
      sqlcmd.append('tb%d values(%ld, %d)' %(tid,self.startTime+11, 11))
    tdSql.execute(" ".join(sqlcmd))
    tdSql.query('select count(*) from tb')
    tdSql.checkData(0, 0, self.ntables*(self.rowsPerTable+1))
    
  def stop(self):
    tdSql.close()
    self.conn.close()
    tdLog.success("%s successfully executed" % __file__)
  
tdCases.addCluster(__file__, TDTestCase())
