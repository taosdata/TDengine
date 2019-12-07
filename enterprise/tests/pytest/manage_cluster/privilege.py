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
    tdDnodes.cfg(1,"numOfMPeers", "3")
    tdDnodes.start(1)
    
    self.conn = taos.connect(config=tdDnodes.getSimCfgPath())
    tdSql.init(self.conn.cursor())
    tdSql.execute('reset query cache')
    tdSql.execute('create dnode 192.168.0.2')
    tdDnodes.deploy(2)
    tdDnodes.cfg(2,"numOfMPeers", "3")
    tdDnodes.start(2)
    tdSql.execute('create dnode 192.168.0.3')
    tdDnodes.deploy(3)
    tdDnodes.cfg(3,"numOfMPeers", "3")
    tdDnodes.start(3)
    tdLog.sleep(5)
    
  def run(self):
    self.ntables = 1
    self.startTime = 1520000010000L

    tdSql.execute('reset query cache')
    tdSql.execute('drop database db')
    tdSql.execute('create database db')
    tdSql.execute('use db')

    tdLog.info("================= step1")
    tdSql.query('show users')
    tdSql.checkRows(3)

    tdLog.info("================= step2")
    tdLog.info("create user user1 with read privilege")
    tdSql.execute('create user user1 pass \'123\'')
    tdSql.execute('alter user user1 privilege \'read\'')
    
    tdLog.info("================= step3")
    tdLog.info("login in using user1")
    tdSql.close()
    newconn = taos.connect(host='192.168.0.1', user='user1', \
                      password='123', config=tdDnodes.getSimCfgPath())
    tdSql.init(newconn.cursor())
    tdSql.query('show users')
    tdSql.checkRows(4)

    tdLog.info("================= step4")
    tdSql.error('create database db_user1')

    tdLog.info("================= step5")
    tdLog.info("alter user1 privilege to super by root")
    tdSql.close()
    newconn.close()
    newconn = taos.connect(host='192.168.0.1', user='root', \
                      password='taosdata', config=tdDnodes.getSimCfgPath())
    tdSql.init(newconn.cursor())
    tdSql.execute('alter user user1 privilege \'super\'')
    tdSql.close()
    newconn.close()

    tdLog.info("================= step6")
    tdLog.info("login in using user1 and create user2")
    newconn = taos.connect(host='192.168.0.1', user='user1', \
                      password='123', config=tdDnodes.getSimCfgPath())
    tdSql.init(newconn.cursor())
    tdSql.execute('create user user2 pass \'234\'')
    tdSql.query('show users')
    tdSql.checkRows(5)
    newconn.close()

  def stop(self):
    tdSql.close()
    self.conn.close()
    tdLog.success("%s successfully executed" % __file__)
  
tdCases.addCluster(__file__, TDTestCase())
