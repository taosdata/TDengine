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
    
    ## cover test case 6, 8, 9, 11 ##
    def run(self):
        # cluster environment set up
        nodes = Nodes()
        ctest = ClusterTest(nodes.node1.hostName)
        ctest.connectDB()
        tdSql.init(ctest.conn.cursor(), False)

        nodes.addConfigs("offlineThreshold", "10")
        nodes.removeAllDataFiles()
        nodes.restartAllTaosd()
        nodes.node3.stopTaosd()

        tdLog.sleep(10)
        tdSql.query("select * from information_schema.ins_dnodes")
        tdSql.checkRows(3)
        tdSql.checkData(2, 4, "offline")

        tdLog.sleep(60)
        tdSql.checkRows(3)
        tdSql.checkData(2, 4, "dropping")        

        tdLog.sleep(300)
        tdSql.checkRows(2)                

        nodes.removeConfigs("offlineThreshold", "10")
        nodes.restartAllTaosd()
        
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

ct = ClusterTestcase()
ct.run()