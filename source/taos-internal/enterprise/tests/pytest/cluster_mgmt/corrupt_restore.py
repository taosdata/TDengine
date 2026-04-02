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
    tdDnodes.cfg(1,"numOfMPeers", "2")
    tdDnodes.start(1)
    
    self.conn = taos.connect(config=tdDnodes.getSimCfgPath())
    tdSql.init(self.conn.cursor())
    tdSql.execute('reset query cache')
    tdSql.execute('create dnode 192.168.0.2')
    tdDnodes.deploy(2)
    tdDnodes.cfg(2,"numOfMPeers", "2")
    tdDnodes.start(2)
    tdLog.sleep(5)

  def run(self):
    self.replica = 2
    self.startTime = 1520000010000L
    
    tdLog.info("================= step1")
    tdLog.info("create 1 table and insert 1 record")
    tdSql.execute('create database db replica %d' %self.replica)
    tdLog.sleep(5)
    tdSql.execute('use db')
    tdSql.execute('create table tb1 (ts timestamp, i int, f float)')
    tdLog.sleep(5)
    tdSql.execute('insert into tb1 values(%ld, %d, %f)' %(self.startTime+1, 1, 1.1))

    tdLog.info("================= step2")
    tdDnodes.stop(1)
    tdDnodes.stop(2)
    tdDnodes.start(1)
    tdDnodes.start(2)
    tdLog.sleep(5)

    tdLog.info("================= step3")
    tdLog.info("delete data file in dnode 2")
    dnode2Dir = tdDnodes.getDnodesRootDir() + '/dnode2/data'
    cmd = 'rm -rf %s/data' %dnode2Dir
    if os.system(cmd) != 0 :
      tdLog.exit(cmd)
    tdLog.debug("%s" % (cmd))

    tdLog.info("================= step4")
    tdSql.error('select * from tb1')

    tdLog.info("================= step5")
    tdSql.query('select * from tb1')
    tdSql.checkData(0, 1, 1)
    if (~os.path.isdir('%s/data' %dnode2Dir)):
      tdLog.exit('data file on dnode 2 not restored')

    tdLog.info("================= step6")
    tdLog.info("delete data file in dnode 2 again")
    dnode2Dir = tdDnodes.getDnodesRootDir() + '/dnode2/data'
    cmd = 'rm -rf %s/data' %dnode2Dir
    if os.system(cmd) != 0 :
      tdLog.exit(cmd)
    tdLog.debug("%s" % (cmd))

    tdLog.info("================= step7")
    tdLog.info('insert 1 record again')
    tdSql.execute('insert into tb1 values (%ld, %d, %f)' %(self.startTime+2, 2, 2.2))
    tdSql.query('select * from tb1')
    tdSql.checkData(1, 1, 2)

  def stop(self):
    tdSql.close()
    self.conn.close()
    tdLog.success("%s successfully executed" % __file__)
  
tdCases.addCluster(__file__, TDTestCase())