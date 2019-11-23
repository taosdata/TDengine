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
    self.replica = 3
    self.ntables = 20
    self.rowsPerTable = 40*38*7/10
    self.startTime = 1520000010000L
    
    tdLog.info("================= step1")
    tdLog.info("create database db replica %d tables 10 cache 512" %self.replica)
    tdSql.execute('create database db replica %d tables 10 cache 512' %self.replica)
    tdLog.sleep(5)
    tdSql.execute('use db')
    tdSql.execute('create table tb (ts timestamp, i int) tags(id int)')
    for tid in range(1,self.ntables+1):
      tdSql.execute('create table tb%d using tb tags(%d)' %(tid,tid))
    tdLog.sleep(5)

    tdLog.info("================= step2")
    tdLog.info("insert %d records into each %d tables" %(self.rowsPerTable, self.ntables))
    for tid in range(1,self.ntables+1):
      startTime = self.startTime
      sqlcmd = ['insert into tb%d values' %(tid)]
      ninserted = 0
      for rid in range(1,self.rowsPerTable+1):
        sqlcmd.append('(%ld, %d)' %(startTime+rid, rid))
        ninserted += 1
        if (ninserted == 300):
          tdSql.execute(" ".join(sqlcmd))
          ninserted = 0
          sqlcmd = ['insert into tb%d values' %(tid)]
      if (ninserted > 0):
        tdSql.execute(" ".join(sqlcmd))
    tdSql.query('select count(*) from tb')
    tdSql.checkData(0, 0, self.rowsPerTable*self.ntables)

    tdLog.info("================= step3")
    dnodesDir  = tdDnodes.getDnodesRootDir()
    dataDir    = dnodesDir + '/dnode1/data/data'
    vnodes = os.listdir(dataDir)
    if (len(vnodes) > 0):
      tdLog.info("data is committed")
    else:
      tdLog.exit("ERROR: data has not been committed")

    tdLog.info("================= step4")
    tdLog.info("insert 10 records again into each %d tables" %(self.ntables))
    ninserted = 0
    sqlcmd = ['insert into']
    for tid in range(1,self.ntables+1):
      startTime = self.startTime
      for rid in range(1,11):
        sqlcmd.append('tb%d values(%ld, %d)' %(tid,startTime+self.rowsPerTable+rid, rid))
        ninserted += 1
        if (ninserted == 300):
          tdSql.execute(" ".join(sqlcmd))
          inserted = 0
          sqlcmd = ['insert into']
    if (ninserted > 0):
      tdSql.execute(" ".join(sqlcmd))
    self.rowsPerTable += 10
    tdSql.execute('reset query cache')
    tdSql.query('select count(*) from tb')
    tdSql.checkData(0, 0, self.rowsPerTable*self.ntables)

    tdLog.info("================= step5")
    self.replica = 2
    tdLog.info('alter database db replica %d' %self.replica)
    tdSql.execute('alter database db replica %d' %self.replica)
    tdSql.query('show dnodes')
    for i in range(3):
      tdLog.info("%s:%s" %(tdSql.getData(i,0), tdSql.getData(i,5)))
    retry = 0
    while(True):
      tdLog.sleep(5)
      retry += 1
      tdSql.query('show dnodes')
      stateRes1 = (tdSql.getData(0,5)=="balanced")
      stateRes2 = (tdSql.getData(1,5)=="balanced")
      stateRes3 = (tdSql.getData(2,5)=="balanced")
      if (stateRes1 and stateRes2 and stateRes3): break
      if (retry == 3): tdLog.exit('failed to balance')

    tdLog.info("================= step6")
    tdLog.info("insert 10 records again into each %d tables" %(self.ntables))
    ninserted = 0
    sqlcmd = ['insert into']
    for tid in range(1,self.ntables+1):
      startTime = self.startTime
      for rid in range(1,11):
        sqlcmd.append('tb%d values(%ld, %d)' %(tid,startTime+self.rowsPerTable+rid, rid))
        ninserted += 1
        if (ninserted == 300):
          tdSql.execute(" ".join(sqlcmd))
          inserted = 0
          sqlcmd = ['insert into']
    if (ninserted > 0):
      tdSql.execute(" ".join(sqlcmd))
    self.rowsPerTable += 10
    tdSql.execute('reset query cache')
    tdSql.query('select count(*) from tb')
    tdSql.checkData(0, 0, self.rowsPerTable*self.ntables)

    tdLog.info("================= step7")
    queryRows = tdSql.query('show vgroups')
    for i in range(queryRows):
      print('vnode status: %s:%s:%s|%s:%s:%s' \
             %(tdSql.getData(i,3),tdSql.getData(i,4),tdSql.getData(i,5),\
               tdSql.getData(i,7),tdSql.getData(i,8),tdSql.getData(i,9)))
    
    tdLog.info("================= step8")
    self.replica = 1
    tdLog.info('alter database db replica %d' %self.replica)
    tdSql.execute('alter database db replica %d' %self.replica)
    tdSql.query('show dnodes')
    for i in range(3):
      tdLog.info("%s:%s" %(tdSql.getData(i,0), tdSql.getData(i,5)))
    retry = 0
    while(True):
      tdLog.sleep(5)
      retry += 1
      tdSql.query('show dnodes')
      stateRes1 = (tdSql.getData(0,5)=="balanced")
      stateRes2 = (tdSql.getData(1,5)=="balanced")
      stateRes3 = (tdSql.getData(2,5)=="balanced")
      if (stateRes1 and stateRes2 and stateRes3): break
      if (retry == 3): tdLog.exit('failed to balance')

    tdLog.info("================= step9")
    tdLog.info("insert 10 records again into each %d tables" %(self.ntables))
    ninserted = 0
    sqlcmd = ['insert into']
    for tid in range(1,self.ntables+1):
      startTime = self.startTime
      for rid in range(1,11):
        sqlcmd.append('tb%d values(%ld, %d)' %(tid,startTime+self.rowsPerTable+rid, rid))
        ninserted += 1
        if (ninserted == 300):
          tdSql.execute(" ".join(sqlcmd))
          inserted = 0
          sqlcmd = ['insert into']
    if (ninserted > 0):
      tdSql.execute(" ".join(sqlcmd))
    self.rowsPerTable += 10
    tdSql.execute('reset query cache')
    tdSql.query('select count(*) from tb')
    tdSql.checkData(0, 0, self.rowsPerTable*self.ntables)

    tdLog.info("================= step10")
    queryRows = tdSql.query('show vgroups')
    for i in range(queryRows):
      print('vnode status: %s:%s:%s' \
             %(tdSql.getData(i,3),tdSql.getData(i,4),tdSql.getData(i,5)))
    
    tdLog.info("================= step11")
    self.replica = 2
    tdLog.info('alter database db replica %d' %self.replica)
    tdSql.execute('alter database db replica %d' %self.replica)
    tdSql.query('show dnodes')
    for i in range(3):
      tdLog.info("%s:%s" %(tdSql.getData(i,0), tdSql.getData(i,5)))
    retry = 0
    while(True):
      tdLog.sleep(5)
      retry += 1
      tdSql.query('show dnodes')
      stateRes1 = (tdSql.getData(0,5)=="balanced")
      stateRes2 = (tdSql.getData(1,5)=="balanced")
      stateRes3 = (tdSql.getData(2,5)=="balanced")
      if (stateRes1 and stateRes2 and stateRes3): break
      if (retry == 3): tdLog.exit('failed to balance')

    tdLog.info("================= step12")
    tdLog.info("insert 10 records again into each %d tables" %(self.ntables))
    ninserted = 0
    sqlcmd = ['insert into']
    for tid in range(1,self.ntables+1):
      startTime = self.startTime
      for rid in range(1,11):
        sqlcmd.append('tb%d values(%ld, %d)' %(tid,startTime+self.rowsPerTable+rid, rid))
        ninserted += 1
        if (ninserted == 300):
          tdSql.execute(" ".join(sqlcmd))
          inserted = 0
          sqlcmd = ['insert into']
    if (ninserted > 0):
      tdSql.execute(" ".join(sqlcmd))
    self.rowsPerTable += 10
    tdSql.execute('reset query cache')
    tdSql.query('select count(*) from tb')
    tdSql.checkData(0, 0, self.rowsPerTable*self.ntables)

    tdLog.info("================= step13")
    queryRows = tdSql.query('show vgroups')
    for i in range(queryRows):
      print('vnode status: %s:%s:%s|%s:%s:%s' \
             %(tdSql.getData(i,3),tdSql.getData(i,4),tdSql.getData(i,5),\
               tdSql.getData(i,7),tdSql.getData(i,8),tdSql.getData(i,9)))

    tdLog.info("================= step14")
    self.replica = 3
    tdLog.info('alter database db replica %d' %self.replica)
    tdSql.execute('alter database db replica %d' %self.replica)
    tdSql.query('show dnodes')
    for i in range(3):
      tdLog.info("%s:%s" %(tdSql.getData(i,0), tdSql.getData(i,5)))
    retry = 0
    while(True):
      tdLog.sleep(5)
      retry += 1
      tdSql.query('show dnodes')
      stateRes1 = (tdSql.getData(0,5)=="balanced")
      stateRes2 = (tdSql.getData(1,5)=="balanced")
      stateRes3 = (tdSql.getData(2,5)=="balanced")
      if (stateRes1 and stateRes2 and stateRes3): break
      if (retry == 3): tdLog.exit('failed to balance')

    tdLog.info("================= step15")
    tdLog.info("insert 10 records again into each %d tables" %(self.ntables))
    ninserted = 0
    sqlcmd = ['insert into']
    for tid in range(1,self.ntables+1):
      startTime = self.startTime
      for rid in range(1,11):
        sqlcmd.append('tb%d values(%ld, %d)' %(tid,startTime+self.rowsPerTable+rid, rid))
        ninserted += 1
        if (ninserted == 300):
          tdSql.execute(" ".join(sqlcmd))
          inserted = 0
          sqlcmd = ['insert into']
    if (ninserted > 0):
      tdSql.execute(" ".join(sqlcmd))
    self.rowsPerTable += 10
    tdSql.execute('reset query cache')
    tdSql.query('select count(*) from tb')
    tdSql.checkData(0, 0, self.rowsPerTable*self.ntables)

    tdLog.info("================= step16")
    queryRows = tdSql.query('show vgroups')
    for i in range(queryRows):
      print('vnode status: %s:%s:%s|%s:%s:%s|%s:%s:%s' \
             %(tdSql.getData(i,3),tdSql.getData(i,4),tdSql.getData(i,5),\
               tdSql.getData(i,7),tdSql.getData(i,8),tdSql.getData(i,9),\
               tdSql.getData(i,11),tdSql.getData(i,12),tdSql.getData(i,13)))

    
  def stop(self):
    tdSql.close()
    self.conn.close()
    tdLog.success("%s successfully executed" % __file__)
  
tdCases.addCluster(__file__, TDTestCase())
