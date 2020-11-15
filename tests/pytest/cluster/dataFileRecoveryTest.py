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
    
    ## test case 20, 21, 22 ##
    def run(self):
        
        nodes = Nodes()
        ctest = ClusterTest(nodes.node1.hostName)
        ctest.connectDB()        
        ctest.createSTable(3)
        ctest.run()
        tdSql.init(ctest.conn.cursor(), False)
        
        nodes.node2.stopTaosd()
        tdSql.execute("use %s" % ctest.dbName)        
        tdSql.query("show vgroups")
        vnodeID = tdSql.getData(0, 0)
        nodes.node2.removeDataForVnode(vnodeID)
        nodes.node2.startTaosd()

        # Wait for vnode file to recover
        for i in range(10):
            tdSql.query("select count(*) from t0")
        
        tdLog.sleep(10)

        for i in range(10):
            tdSql.query("select count(*) from t0")
            tdSql.checkData(0, 0, 1000)
        
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

ct = ClusterTestcase()
ct.run()
