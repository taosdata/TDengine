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
import datetime
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *

class TDTestCase:
  def init(self, conn):
    tdLog.debug("start to execute %s" % __file__)
    tdSql.init(conn.cursor())
    
  def run(self):
    self.ntables = 10
    self.rowsPerTable = 100
    self.startTime = 1520000010000L

    tdDnodes.stop(1)
    tdDnodes.deploy(1)
    tdDnodes.start(1)

    tdSql.execute('reset query cache')
    tdSql.execute('drop database db')
    tdSql.execute('create database db tables 4')
    tdLog.sleep(5)
    tdSql.execute('use db')

    tdLog.info("================= step1")
    tdLog.info("create %d tables" %self.ntables)
    tdSql.execute('create table stb (ts timestamp, speed int) tags(id int)')
    for tid in range(1, self.ntables+1):
      tdSql.execute('create table tb%d using stb tags(%d)' %(tid, tid))

    tdLog.info("================= step2")
    tdLog.info("insert %d data" %self.rowsPerTable)
    startTime = self.startTime
    for tid in range(1, self.ntables+1):
      sqlcmd = ['insert into tb%d values' %tid]
      for rid in range(1,self.rowsPerTable+1):
        sqlcmd.append('(%ld, %d)' %(startTime+rid, rid))
      tdSql.execute(" ".join(sqlcmd))
  
    tdLog.info("================= step3")
    tdSql.query('select count(*) from stb')
    tdSql.checkData(0, 0, self.ntables*self.rowsPerTable)

    tdLog.info("================= step4")
    tdDnodes.stop(1)

    tdLog.info("================= step5")
    startTime = datetime.datetime.now()
    tdSql.error('select count(*) from stb')
    endTime =  datetime.datetime.now()
    deltaTime = (endTime - startTime).seconds
    print('It costs %dseconds to return failed connection' %deltaTime)

  def stop(self):
    tdSql.close()
    tdLog.success("%s successfully executed" % __file__)
  
tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
