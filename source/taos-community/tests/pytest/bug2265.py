###################################################################
#           Copyright (c) 2016 by TAOS Technologies, Inc.
#                     All rights reserved.
#
#  This file is proprietary and confidential to TAOS Technologies.
#  No part of this file may be reproduced, stored, transmitted,
#  disclosed or used in any form or by any means other than as
#  expressly provided by the written permission from Jianhui Tao
#
###################################################################

# -*- coding: utf-8 -*-

import sys
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *
import taos
if __name__ == "__main__":

    logSql = True
    deployPath = ""
    testCluster = False
    valgrind = 0

    print("start to execute %s" % __file__)
    tdDnodes.init(deployPath)
    tdDnodes.setTestCluster(testCluster)
    tdDnodes.setValgrind(valgrind)

    tdDnodes.stopAll()
    tdDnodes.addSimExtraCfg("maxSQLLength", "1048576")
    tdDnodes.deploy(1)
    tdDnodes.start(1)
    host = '127.0.0.1'

    tdLog.info("Procedures for tdengine deployed in %s" % (host))

    tdCases.logSql(logSql)
    print('1')
    conn = taos.connect(
        host,
        config=tdDnodes.getSimCfgPath())

    tdSql.init(conn.cursor(), True)
        
    print("==========step1")
    print("create table ")
    tdSql.execute("create database db")
    tdSql.execute("use db")
    tdSql.execute("create table t1 (ts timestamp, c1 int,c2 int ,c3 int)")

    print("==========step2")
    print("insert maxSQLLength data ")
    data = 'insert into t1 values'
    ts = 1604298064000
    i = 0
    while ((len(data)<(1024*1024)) & (i < 32767 - 1) ):
        data += '(%s,%d,%d,%d)'%(ts+i,i%1000,i%1000,i%1000)
        i+=1
    tdSql.execute(data)
    
    print("==========step4")
    print("insert data batch larger than 32767 ")
    i = 0
    while ((len(data)<(1024*1024)) & (i < 32767) ):
        data += '(%s,%d,%d,%d)'%(ts+i,i%1000,i%1000,i%1000)
        i+=1
    tdSql.error(data)

    print("==========step4")
    print("insert data larger than maxSQLLength ")
    tdSql.execute("create table t2 (ts timestamp, c1 binary(50))")
    data = 'insert into t2 values'
    i = 0
    while ((len(data)<(1024*1024)) & (i < 32767 - 1 ) ):
        data += '(%s,%s)'%(ts+i,'a'*50)
        i+=1
    tdSql.error(data)      
    tdSql.close()
    tdLog.success("%s successfully executed" % __file__)
        
   

