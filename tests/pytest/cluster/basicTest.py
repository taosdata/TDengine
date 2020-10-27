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

    ## test case 1, 33 ##
    def run(self):
        
        nodes = Nodes()        
        ctest = ClusterTest(nodes.node1.hostName)

        ctest.connectDB()
        tdSql.init(ctest.conn.cursor(), False)

        ## Test case 1 ##       
        tdLog.info("Test case 1 repeat %d times" % ctest.repeat)
        for i in range(ctest.repeat):
            tdLog.info("Start Round %d" % (i + 1))
            replica = random.randint(1,3)        
            ctest.createSTable(replica) 
            ctest.run()
            tdLog.sleep(10)      
            tdSql.query("select count(*) from %s.%s" %(ctest.dbName, ctest.stbName))
            tdSql.checkData(0, 0, ctest.numberOfRecords * ctest.numberOfTables)            
            tdLog.info("Round %d completed" % (i + 1))

        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

ct = ClusterTestcase()
ct.run()