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
  def init(self, conn):
    tdLog.debug("start to execute %s" % __file__)
    tdSql.init(conn.cursor())
    
  def run(self):
    self.ntables = 1
    self.startTime = 1520000010000L

    tdDnodes.stop(1)
    tdDnodes.deploy(1)
    tdDnodes.start(1)

    tdSql.execute('reset query cache')
    tdSql.execute('drop database db')
    tdSql.execute('create database db')
    tdSql.execute('use db')

    tdLog.info("================= step1")
    tdSql.query('show users')
    tdSql.checkRows(3)

    tdLog.info("================= step2")
    tdLog.info("create an account accta ")
    tdSql.execute('create account accta pass \'123\'')
    
    tdLog.info("================= step3")
    tdLog.info("login in using accta")
    tdSql.close()
    newconn = taos.connect(host='192.168.0.1', user='accta', \
                      password='123', config=tdDnodes.getSimCfgPath())
    tdSql.init(newconn.cursor())
    tdLog.info('show users')
    resRows = tdSql.query('show users')
    for i in range (resRows):
      print('user: %s: %s' %(tdSql.getData(i,0), tdSql.getData(i,1)))

    tdLog.info("================= step4")
    tdLog.info("create a user accta2 and alter its privilege to super")
    tdSql.execute('create user accta2 pass \'123\'')
    tdSql.execute("alter user accta2 privilege super")
    tdLog.info('show users')
    resRows = tdSql.query('show users')
    for i in range (resRows):
      print('user: %s: %s' %(tdSql.getData(i,0), tdSql.getData(i,1)))

    tdLog.info("================= step5")
    tdLog.info("login in using accta2 and alter accta privilege to read")
    tdSql.close()
    newconn.close()
    newconn = taos.connect(host='192.168.0.1', user='accta', \
                      password='taosdata', config=tdDnodes.getSimCfgPath())
    tdSql.init(newconn.cursor())
    tdSql.execute('alter user accta privilege read')
    tdSql.close()
    newconn.close()

    tdLog.info("================= step6")
    tdLog.info("login in using accta and try to create database")
    newconn = taos.connect(host='192.168.0.1', user='accta', \
                      password='123', config=tdDnodes.getSimCfgPath())
    tdSql.init(newconn.cursor())
    tdLog.info('show users')
    resRows = tdSql.query('show users')
    for i in range (resRows):
      print('user: %s: %s' %(tdSql.getData(i,0), tdSql.getData(i,1)))
    tdSql.error('create database test')
    newconn.close()

  def stop(self):
    tdSql.close()
    tdLog.success("%s successfully executed" % __file__)
  
tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
