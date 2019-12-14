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
    self.ntables = 10
    self.startTime = 1520000010000L

    tdDnodes.stop(1)
    tdDnodes.deploy(1)
    tdDnodes.start(1)

    tdSql.execute('reset query cache')
    tdSql.execute('drop database if exists db')
    tdSql.execute('create database db cache 512 tables 5')
    tdSql.execute('use db')

    tdLog.info("================= step1")
    tdLog.info("create %d table" %self.ntables)
    for tid in range(1, self.ntables+1):
      tdSql.execute('create table tb%d (ts timestamp, i int)' %tid)
    tdLog.info("one block can import 38 records")

    tdLog.info("================= step2")
    tdLog.info("import 38 data in to each %d tables" %self.ntables)
    for rid in range(1,39):
      startTime = self.startTime
      sqlcmd = ['import into']
      for tid in range(1,self.ntables+1):
        sqlcmd.append('tb%d values(%ld+%dd, %d)' %(tid, startTime+rid, rid, rid))
      tdSql.execute(" ".join(sqlcmd))
    
    tdLog.info("================= step3")
    tdSql.query('select * from tb1')
    tdSql.checkRows(38)

    tdLog.info("================= step4")
    tdLog.info("import 3 data before with overlap")
    for tid in range(1,self.ntables+1):
      startTime = self.startTime 
      sqlcmd = ['import into tb%d values' %(tid)]
      for rid in range(0,3):
        sqlcmd.append('(%ld+%dd, %d)' %(startTime+rid, rid, rid))
      tdSql.execute(" ".join(sqlcmd))
    tdSql.checkAffectedRows(1)

  def stop(self):
    tdSql.close()
    tdLog.success("%s successfully executed" % __file__)
  
tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
