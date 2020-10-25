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
from clusterSetup import *
from util.sql import tdSql
from util.log import tdLog
import random

class ClusterTestcase:
    
    ## test case 28, 29, 30, 31 ##
    def run(self):
        
        nodes = Nodes()
        ctest = ClusterTest(nodes.node1.hostName)
        ctest.connectDB()        
        ctest.createSTable(3)
        ctest.run()
        tdSql.init(ctest.conn.cursor(), False)
        
        tdSql.execute("use %s" % ctest.dbName) 
        
        nodes.node2.stopTaosd()                        
        for i in range(100):
            tdSql.execute("drop table t%d" % i)
        
        nodes.node2.startTaosd()
        tdSql.query("show tables")
        tdSql.checkRows(9900)
        
        nodes.node2.stopTaosd()
        for i in range(10):
            tdSql.execute("create table a%d using meters tags(2)" % i)

        nodes.node2.startTaosd()
        tdSql.query("show tables")
        tdSql.checkRows(9910)

        nodes.node2.stopTaosd()        
        tdSql.execute("alter table meters add col col6 int")
        nodes.node2.startTaosd()        
        
        nodes.node2.stopTaosd()        
        tdSql.execute("drop database %s" % ctest.dbName)
        
        nodes.node2.startTaosd()
        tdSql.query("show databases")
        tdSql.checkRows(0)
        
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

ct = ClusterTestcase()
ct.run()
