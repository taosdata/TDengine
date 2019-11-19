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
    tdDnodes.cfg(1, "numOfMPeers", "3")
    tdDnodes.cfg(1, "tables", "5")
    tdDnodes.start(1)
    
    self.conn = taos.connect(host='192.168.0.1', config=tdDnodes.getSimCfgPath())
    tdSql.init(self.conn.cursor())
    tdSql.execute('reset query cache')
    tdSql.execute('create dnode 192.168.0.2')
    tdDnodes.deploy(2)
    tdDnodes.cfg(2, "numOfMPeers", "3")
    tdDnodes.cfg(2, "tables", "5")
    tdDnodes.start(2)
    tdSql.execute('create dnode 192.168.0.3')
    tdDnodes.deploy(3)
    tdDnodes.cfg(3, "numOfMPeers", "3")
    tdDnodes.cfg(3, "tables", "5")
    tdDnodes.start(3)
    tdLog.sleep(5)

  def run(self):
    self.ntables = 20
    self.startTime = 1520000010000L
    self.rowsPerTable = 10
    self.replica = 3

    tdLog.info("================= step1")
    tdLog.info("insert %d records into %d tables" % (self.rowsPerTable, self.ntables))
    tdSql.execute('create database db replica %d' % self.replica)
    tdLog.sleep(5)
    tdSql.execute('use db')
    tdSql.execute('create table tb(ts timestamp, i int) tags(id int)')
    for tid in range(1,self.ntables+1):
      tdSql.execute('create table tb%d using tb tags(%d)' %(tid, tid))
    tdLog.sleep(5)
    for tid in range(1,self.ntables+1):
      startTime = self.startTime
      sqlcmd = ['insert into tb%d values' % (tid)]
      for rid in range(1,self.rowsPerTable+1):
        sqlcmd.append("(%ld, %d)" % (startTime+rid, rid))
      tdSql.execute(" ".join(sqlcmd))
    self.startTime += self.rowsPerTable
    tdSql.query('select count(*) from tb')
    tdSql.checkData(0, 0, self.ntables*self.rowsPerTable)
    tdLog.sleep(5)

    tdLog.info("================= step2")
    newUser = 'user1'
    newPass = 'taos1'
    tdLog.info("create new user %s" % (newUser))
    tdSql.execute('create user %s PASS \'%s\'' % (newUser, newPass))
    tdLog.sleep(5)
    nusers = tdSql.query('show users')
    successFlag = False
    for i in range(nusers):
      if (tdSql.getData(i,0) == newUser): 
        successFlag = True
        break
    if (successFlag):
      tdLog.info("new user %s is created successfully" % (newUser))
    else:
      tdLog.exit("Failed to create user %s" % (newUser))

    tdLog.info("================= step3")
    tdLog.info("drop new user %s" % (newUser))
    tdSql.execute('drop user %s' % (newUser))
    nusers = tdSql.query('show users')
    successFlag = True
    for i in range(nusers):
      if (tdSql.getData(i,0) == newUser): 
        successFlag = False
        break
    if (successFlag):
      tdLog.info("user %s is dropped successfully" % (newUser))
    else:
      tdLog.exit("Failed to drop user %s" % (newUser))

    tdLog.info("================= step4")
    self.ntables += 1
    tdLog.info("create table tb%d and insert %d data" % (self.ntables, self.rowsPerTable))
    tdSql.execute('create table tb%d using tb tags(%d)' % (self.ntables, self.ntables))
    tdLog.sleep(5)
    startTime = self.startTime
    sqlcmd = ['insert into tb%d values' % (self.ntables)]
    for rid in range(1,self.rowsPerTable+1):
      sqlcmd.append("(%ld, %d)" % (startTime+rid, rid))
    tdSql.execute(" ".join(sqlcmd))
    self.startTime += self.rowsPerTable
    tdSql.execute("reset query cache")
    tdSql.query("select count(*) from tb")
    tdSql.checkData(0, 0, self.ntables*self.rowsPerTable)

    tdLog.info("================= step5")
    tdLog.info("drop table tb%d" % (self.ntables))
    tdSql.execute('drop table tb%d' % (self.ntables))
    self.ntables -= 1
    tdSql.query("select count(*) from tb")
    tdSql.checkData(0, 0, self.ntables*self.rowsPerTable)

    tdLog.info("================= step6")
    tdLog.info("alter super table tb and insert %d data again" %self.rowsPerTable)
    tdSql.execute('alter table tb add column f float')
    startTime = self.startTime
    for tid in range(1, self.ntables+1):
      sqlcmd = ['insert into tb%d values' % (self.ntables)]
      for rid in range(1,self.rowsPerTable+1):
        sqlcmd.append("(%ld, %d, %f)" % (startTime+rid, rid, rid*1.2))
      tdSql.execute(" ".join(sqlcmd))
    self.startTime += self.rowsPerTable
    tdSql.query("select last(*) from tb%d" % (self.ntables))
    res = tdSql.getData(0, 2)
    if (abs((res-self.rowsPerTable*1.2)) < 0.1):
      tdLog.info("alter table tb%d successfully" % (self.ntables))
    else:
      tdLog.exit("failed to alter table tb%d" % (self.ntables))
    tdLog.sleep(10)

    tdLog.info("================= step7")
    newPass = 'taosdata1'
    tdLog.info("alter password to %s" % (newPass))
    tdSql.execute('alter user root PASS \'%s\'' % (newPass))
    tdSql.close()
    tdLog.sleep(5)
    conn = taos.connect(host='192.168.0.1', user='root', password='%s'%(newPass))
    tdSql.init(conn.cursor())
    tdSql.execute("use db")
    tdSql.query("select count(tbname) from tb")
    tdSql.checkData(0, 0, self.ntables)

    tdLog.info("================= step8")
    tdLog.info("drop database db")
    tdSql.execute('drop database db')
    tdLog.sleep(10)
    tdSql.query("show databases")
    tdSql.checkRows(0)


  def stop(self):
    tdSql.close()
    self.conn.close()
    tdLog.success("%s successfully executed" % __file__)

tdCases.addCluster(__file__, TDTestCase())


