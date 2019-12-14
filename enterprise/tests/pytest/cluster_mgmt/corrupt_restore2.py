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
    tdDnodes.cfg(1,"numOfMPeers", "2")
    tdDnodes.start(1)
    
    self.conn = taos.connect(config=tdDnodes.getSimCfgPath())
    tdSql.init(self.conn.cursor())
    tdSql.execute('reset query cache')
    tdSql.execute('create dnode 192.168.0.2')
    tdDnodes.deploy(2)
    tdDnodes.cfg(2,"numOfMPeers", "2")
    tdDnodes.start(2)
    tdLog.sleep(10)

  def run(self):
    self.replica = 2
    self.startTime = 1520000010000L
    
    tdLog.info("================= step1")
    tdLog.info("create 1 table and insert 1 record")
    tdSql.execute('create database db replica %d' %self.replica)
    tdLog.sleep(5)
    tdSql.execute('use db')
    tdSql.execute('create table tb1 (ts timestamp, i int, f float)')
    tdSql.execute('insert into tb1 values(%ld, %d, %f)' %(self.startTime+1, 1, 1.1))

    tdLog.info("================= step2")
    tdDnodes.stop(1)
    tdDnodes.stop(2)
    tdDnodes.start(1)
    tdDnodes.start(2)
    tdLog.sleep(5)

    tdLog.info("================= step3")
    tdLog.info("alter data file in dnode 2")
    dnode2DataDir = tdDnodes.getDnodesRootDir() + '/dnode2/data/data/vnode0'
    files = os.listdir(dnode2DataDir)
    fileToMod = "%s/%s" %(dnode2DataDir, files[0])
    oldFileSize = os.path.getsize(fileToMod)
    cmd = "echo \"hello world\" > %s" %(dnode2DataDir, fileToMod)
    if os.system(cmd) != 0 :
      tdLog.exit(cmd)

    tdLog.info("================= step4")
    tdSql.execute('insert into tb1 values(%ld, %d, %f)' %(self.startTime+2, 2, 2.2))
    tdSql.execute('import into tb1 values(%ld, %d, %f)' %(self.startTime, 0, 0))
    tdSql.query('select * from tb1 order by ts desc')
    tdSql.checkRows(3)

    tdLog.info("================= step5")
    tdDnodes.stop(1)
    tdDnodes.stop(2)
    tdDnodes.start(1)
    tdDnodes.start(2)
    tdLog.sleep(5)

    tdLog.info("================= step6")
    tdLog.info("check dnode2 data file not restore")
    fd = open(fileToMod,'r')
    line = fd.readline()
    fd.close()
    if (line.find("hello") < 0) :
      tdLog.exit("dnode2 data file has been restored")

    tdLog.info("================= step7")
    tdLog.info("alter data file name in dnode 1")
    dnode1DataDir = tdDnodes.getDnodesRootDir() + '/dnode1/data/data/vnode0'
    files = os.listdir(dnode1DataDir)
    fileToMod2 = "%s/%s" %(dnode1DataDir, files[0])
    cmd = 'mv %s hello.world' %fileToMod2
    if (os.system(cmd) != 0):
      tdLog.exit(cmd)

    tdLog.info("================= step8")
    tdSql.error('select * from tb1')

    tdLog.info("================= step9")
    tdLog.info("restore data file name in dnode 1")
    cmd = 'mv hello.world %s' %fileToMod2
    if (os.system(cmd) != 0):
      tdLog.exit(cmd)

    tdLog.info("================= step10") 
    tdSql.query('select last(*) from tb1')
    tdSql.chechData(0, 1, 2)

    tdLog.info("================= step11")
    tdLog.info("check dnode2 data file restored")
    fd = open(fileToMod,'r')
    line = fd.readline()
    fd.close()
    if (line.find("hello") >= 0) :
      tdLog.exit("dnode2 data file has not been restored")

    tdLog.info("================= step12")
    tdLog.info("delete all files in dnode 2")
    cmd = 'rm -rf %/*' %dnode2DataDir
    if os.system(cmd) != 0 :
      tdLog.exit(cmd)
    tdLog.debug("%s" % (cmd))

    tdLog.info("================= step13")
    tdLog.info("import 1 data 11 days ago")
    tdSql.execute('import into tb1 values(%ld-11d, %d, %f)' %(self.startTime, -11, -11.11))

    tdLog.info("================= step14")
    tdDnodes.stop(1)
    tdDnodes.stop(2)
    tdDnodes.start(1)
    tdDnodes.start(2)
    tdLog.sleep(5)

    tdLog.info("================= step15")
    tdSql.query('select last(*) from tb1')
    tdSql.checkData(0, 1, -11)
    if (len(os.listdir(dnode2DataDir)) <= 0):
      tdLog.exit('data file on dnode 2 not restored!')


  def stop(self):
    tdSql.close()
    self.conn.close()
    tdLog.success("%s successfully executed" % __file__)
  
tdCases.addCluster(__file__, TDTestCase())