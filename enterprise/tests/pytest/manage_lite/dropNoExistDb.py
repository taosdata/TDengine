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
    tdSql.execute('create database db2')
    tdLog.info('show databases')
    resRows = tdSql.query('show databases')
    for i in range(resRows):
      print('database: %s: %s' %(tdSql.getData(i,0), tdSql.getData(i,1)))
    
    tdLog.info("================= step2")
    tdLog.info("drop database db repeately")
    tdSql.execute('drop database db')
    tdLog.info('show databases')
    resRows = tdSql.query('show databases')
    for i in range(resRows):
      print('database: %s: %s' %(tdSql.getData(i,0), tdSql.getData(i,1)))
    tdSql.error('drop database db')
    

  def stop(self):
    tdSql.close()
    tdLog.success("%s successfully executed" % __file__)
  
tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
