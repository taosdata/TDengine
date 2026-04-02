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
    tdDnodes.cfg(1,"numOfMPeers", "3")
    tdDnodes.cfg(1,"commitTime", "30")
    tdDnodes.cfg(1,"tables", "5")
    tdDnodes.start(1)
    
    self.conn = taos.connect(host='192.168.0.1', config=tdDnodes.getSimCfgPath())
    tdSql.init(self.conn.cursor())
    tdSql.execute('reset query cache')
    tdSql.execute('create dnode 192.168.0.2')
    tdDnodes.deploy(2)
    tdDnodes.cfg(2,"numOfMPeers", "3")
    tdDnodes.cfg(2,"commitTime", "30")
    tdDnodes.cfg(2,"tables", "5")
    tdDnodes.start(2)
    tdSql.execute('create dnode 192.168.0.3')
    tdDnodes.deploy(3)
    tdDnodes.cfg(3,"numOfMPeers", "3")
    tdDnodes.cfg(3,"commitTime", "30")
    tdDnodes.cfg(3,"tables", "5")
    tdDnodes.start(3)
    tdLog.sleep(5)

  def sync(self):
    tdDnodes.start(3)

  def alterTable(self):
    conn = taos.connect(host='192.168.0.1', config=tdDnodes.getSimCfgPath())
    cursor = conn.cursor()
    cursor.execute('use db')
    cursor.execute('alter table tb add column f float')
    startTime = self.startTime
    for rid in range(1,2):
      sqlcmd = ['insert into']
      for tid in range(1, self.ntables+1):
        sqlcmd.append('tb%d values(%ld, %d, %f)'%(tid, startTime+rid, rid, 1.2*rid))
      cursor.execute(" ".join(sqlcmd))

  def run(self):
    self.ntables = 100
    self.startTime = 1520000010000L
    self.rowsPerTable = 100
    self.replica = 2
    self.ctime = 30

    tdLog.info("================= step1")
    tdLog.info("insert %d records into %d tables" % (self.rowsPerTable, self.ntables))
    tdSql.execute('drop database if exists db')
    tdSql.execute('create database db replica %d ctime %d' % (self.replica, self.ctime))
    tdLog.sleep(5)
    tdSql.execute('use db')
    tdSql.execute('create table tb(ts timestamp, i int) tags(id int)')
    for tid in range(1,self.ntables+1):
      tdSql.execute('create table tb%d using tb tags(%d)' %(tid,tid))
    tdLog.sleep(5)
    for tid in range(1,2):
      startTime = self.startTime
      sqlcmd = ['insert into tb%d values' % (tid)]
      for rid in range(1,self.rowsPerTable+1):
        sqlcmd.append("(%ld, %d)" % (startTime+rid, rid))
      tdSql.execute(" ".join(sqlcmd))
    tdDnodes.forcestop(3)
    for tid in range(2,self.ntables+1):
      startTime = self.startTime
      sqlcmd = ['insert into tb%d values' % (tid)]
      for rid in range(1,self.rowsPerTable+1):
        sqlcmd.append("(%ld, %d)" % (startTime+rid, rid))
      tdSql.execute(" ".join(sqlcmd))
    self.startTime += self.rowsPerTable
    tdSql.query('select count(*) from tb')
    tdSql.checkData(0, 0, self.rowsPerTable*self.ntables)
    tdLog.sleep(35)

    tdLog.info("================= step2")
    tdLog.info("alter meter meta while syncing")
    threads = []
    thread = threading.Thread(target=self.sync, name="db is syncing") 
    thread.start()
    threads.append(thread)
    thread = threading.Thread(target=self.alterTable, name="alter some tables in syncing") 
    thread.start()
    threads.append(thread)  
    for t in range (2):
      threads[t].join()
    tdLog.sleep(10)

    tdLog.info("================= step3")
    tdSql.close()
    tdSql.init(self.conn.cursor())
    tdSql.execute('use db')
    tdSql.execute('reset query cache')
    tdSql.query('select count(*) from tb')
    tdSql.checkData(0, 0, (self.rowsPerTable+1)*self.ntables)
    
  def stop(self):
    tdSql.close()
    self.conn.close()
    tdLog.success("%s successfully executed" % __file__)
  
tdCases.addCluster(__file__, TDTestCase())
