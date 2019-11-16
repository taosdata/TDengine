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
    tdDnodes.start(1)
    
    self.conn = taos.connect(host='192.168.0.1', config=tdDnodes.getSimCfgPath())
    tdSql.init(self.conn.cursor())
    tdSql.execute('reset query cache')
    tdSql.execute('create dnode 192.168.0.2')
    tdDnodes.deploy(2)
    tdDnodes.cfg(2,"numOfMPeers", "1")
    tdDnodes.cfg(2,"tables", "5")
    tdDnodes.start(2)
    tdSql.execute('create dnode 192.168.0.3')
    tdDnodes.deploy(3)
    tdDnodes.cfg(3,"numOfMPeers", "1")
    tdDnodes.cfg(3,"tables", "5")
    tdDnodes.start(3)
    tdLog.sleep(5)

  def run(self):
    self.ntables = 20
    self.startTime = 1520000010000L
    self.rowsPerTable = 1
    self.replica = 1

    tdLog.info("================= step1")
    tdLog.info("insert %d records into %d tables replica %d" % (self.rowsPerTable, self.ntables, self.replica))
    tdSql.execute('create database db replica %d ' % (self.replica))
    tdLog.sleep(5)
    tdSql.execute('use db')
    tdSql.execute('create table stb (ts timestamp, i int) tags (id int)')
    for tid in range(1,self.ntables+1):
      tdSql.execute('create table tb%d using stb tags (%d)' %(tid, tid))
    tdLog.sleep(10)
    for tid in range(1,self.ntables+1):
      startTime = self.startTime
      sqlcmd = ['insert into tb%d values' % (tid)]
      for rid in range(1,self.rowsPerTable+1):
        sqlcmd.append("(%ld, %d)" % (startTime+rid, rid))
      tdSql.execute(" ".join(sqlcmd))
    self.startTime += self.rowsPerTable
    conn = taos.connect(host='192.168.0.1')
    cur = conn.cursor()
    cur.execute('reset query cache')
    t0 = datetime.datetime.now()
    cur.execute('select * from db.stb')
    data = cur.fetchall()
    t1 = datetime.datetime.now()
    tt1 = (t1 - t0).seconds*1000
    print("replica %d query spents %fms " % (self.replica, tt1))
    tdSql.execute('drop database db')
    cur.close()
    conn.close()

    tdLog.info("================= step2")
    self.replica = 3
    tdLog.info("insert %d records into %d tables replica %d" % (self.rowsPerTable, self.ntables, self.replica))
    tdSql.execute('create database db replica %d ' % (self.replica))
    tdLog.sleep(5)
    tdSql.execute('use db')
    tdSql.execute('create table stb (ts timestamp, i int) tags (id int)')
    for tid in range(1,self.ntables+1):
      tdSql.execute('create table tb%d using stb tags (%d)' %(tid, tid))
    tdLog.sleep(10)
    for tid in range(1,self.ntables+1):
      startTime = self.startTime
      sqlcmd = ['insert into tb%d values' % (tid)]
      for rid in range(1,self.rowsPerTable+1):
        sqlcmd.append("(%ld, %d)" % (startTime+rid, rid))
      tdSql.execute(" ".join(sqlcmd))
    self.startTime += self.rowsPerTable
    conn = taos.connect(host='192.168.0.1')
    cur = conn.cursor()
    cur.execute('reset query cache')
    t0 = datetime.datetime.now()
    cur.execute('select * from db.stb')
    data = cur.fetchall()
    t1 = datetime.datetime.now()
    tt2 = (t1 - t0).seconds*1000
    print("replica %d query spents %fms" % (self.replica, tt2))
    cur.close()
    conn.close()
    if ( tt2/(tt1+0.1) > 10):
      tdLog.exit("query too slow with multiple replica")
    
  def stop(self):
    tdSql.close()
    self.conn.close()
    tdLog.success("%s successfully executed" % __file__)
  
tdCases.addCluster(__file__, TDTestCase())