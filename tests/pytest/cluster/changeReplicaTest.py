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
    
    ## test case 7,  ##
    def run(self):
        
        nodes = Nodes()
        ctest = ClusterTest(nodes.node1.hostName)
        ctest.connectDB()        
        tdSql.init(ctest.conn.cursor(), False)

        tdSql.execute("use %s" % ctest.dbName)
        tdSql.query("show vgroups")
        for i in range(10):
            tdSql.checkData(i, 5, "master")

        tdSql.execute("alter database %s replica 2" % ctest.dbName)    
        tdLog.sleep(30)
        tdSql.query("show vgroups")
        for i in range(10):
            tdSql.checkData(i, 5, "master")
            tdSql.checkData(i, 7, "slave")
            
        tdSql.execute("alter database %s replica 3" % ctest.dbName)
        tdLog.sleep(30)
        tdSql.query("show vgroups")
        for i in range(10):
            tdSql.checkData(i, 5, "master")
            tdSql.checkData(i, 7, "slave")
            tdSql.checkData(i, 9, "slave")
        
ct = ClusterTestcase()
ct.run()