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
import time

class ClusterTestcase:
    
    ## test case 32 ##
    def run(self):
        
        nodes = Nodes()
        ctest = ClusterTest(nodes.node1.hostName)
        ctest.connectDB()                
        ctest.createSTable(1)
        ctest.run()
        tdSql.init(ctest.conn.cursor(), False)
        
        tdSql.execute("use %s" % ctest.dbName) 
        totalTime = 0
        for i in range(10):
            startTime = time.time()
            tdSql.query("select * from %s" % ctest.stbName)
            totalTime += time.time() - startTime
        print("replica 1: avarage query time for %d records: %f seconds" % (ctest.numberOfTables * ctest.numberOfRecords,totalTime / 10))

        tdSql.execute("alter database %s replica 3" % ctest.dbName)
        tdLog.sleep(60)
        totalTime = 0
        for i in range(10):
            startTime = time.time()
            tdSql.query("select * from %s" % ctest.stbName)
            totalTime += time.time() - startTime
        print("replica 3: avarage query time for %d records: %f seconds" % (ctest.numberOfTables * ctest.numberOfRecords,totalTime / 10))
                                
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

ct = ClusterTestcase()
ct.run()
