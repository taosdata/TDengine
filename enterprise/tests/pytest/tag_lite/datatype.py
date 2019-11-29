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
    self.rowsPerTable = 10
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
    tdLog.info("create 1 super table")
    tdSql.execute('create table stb (ts timestamp, i int) \
          tags (tin int, tfl float, tbg bigint, tdo double, tbi binary(10), tbl bool)')

    tdLog.info("================= step2")
    tdLog.info("create %d tables" %self.ntables)
    for tid in range(1, self.ntables+1):
      tdSql.execute('create table tb%d using stb tags(%d,%f,%ld,%f,\'%s\',%d)' \
                     %(tid, tid, 1.2*tid, self.startTime+tid, 1.22*tid, 't'+str(tid), tid%2))
    tdLog.sleep(5)

    tdLog.info("================= step3")    
    tdLog.info("insert %d data in to each %d tables" %(self.rowsPerTable,self.ntables))  
    for rid in range(1,self.rowsPerTable+1):
      sqlcmd = ['insert into']
      for tid in range(1,self.ntables+1):
        sqlcmd.append('tb%d values(%ld,%d)' %(tid, self.startTime+rid, rid))
      tdSql.execute(" ".join(sqlcmd))
    tdSql.query('select count(*) from stb')
    tdSql.checkData(0,0, self.rowsPerTable*self.ntables)

    tdLog.info("================= step4")
    tdLog.info("drop one tag")
    tdSql.execute('alter table stb drop tag tbi')
    tdLog.info("insert %d data in to each %d tables" %(2,self.ntables))  
    self.rowsPerTable += 2
    for rid in range(self.rowsPerTable+1,self.rowsPerTable+3):
      sqlcmd = ['insert into']
      for tid in range(1,self.ntables+1):
        sqlcmd.append('tb%d values(%ld,%d)' %(tid, self.startTime+rid, rid))
      tdSql.execute(" ".join(sqlcmd))
    tdSql.query('select count(*) from stb')
    tdSql.checkData(0,0, self.rowsPerTable*self.ntables)
    tdSql.query('describe tb1')
    tdSq.checkRows(2+5)

    tdLog.info("================= step5")  
    tdLog.info("add one tag")   

    
  def stop(self):
    tdSql.close()
    tdLog.success("%s successfully executed" % __file__)
  
tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
