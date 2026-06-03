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
    
    ## test case 7, 10 ##
    def run(self):
        # cluster environment set up
        tdLog.info("Test case 7, 10")

        nodes = Nodes()
        ctest = ClusterTest(nodes.node1.hostName)
        ctest.connectDB()
        tdSql.init(ctest.conn.cursor(), False)

        nodes.node1.stopTaosd()
        tdSql.query("select * from information_schema.ins_dnodes")
        tdSql.checkRows(3)
        tdSql.checkData(0, 4, "offline")
        tdSql.checkData(1, 4, "ready")    
        tdSql.checkData(2, 4, "ready")

        nodes.node1.startTaosd()
        tdSql.checkRows(3)
        tdSql.checkData(0, 4, "ready")
        tdSql.checkData(1, 4, "ready")    
        tdSql.checkData(2, 4, "ready")

        nodes.node2.stopTaosd()
        tdSql.query("select * from information_schema.ins_dnodes")
        tdSql.checkRows(3)
        tdSql.checkData(0, 4, "ready")
        tdSql.checkData(1, 4, "offline")    
        tdSql.checkData(2, 4, "ready")

        nodes.node2.startTaosd()
        tdSql.checkRows(3)
        tdSql.checkData(0, 4, "ready")
        tdSql.checkData(1, 4, "ready")    
        tdSql.checkData(2, 4, "ready")

        nodes.node3.stopTaosd()
        tdSql.query("select * from information_schema.ins_dnodes")
        tdSql.checkRows(3)
        tdSql.checkData(0, 4, "ready")
        tdSql.checkData(1, 4, "ready")    
        tdSql.checkData(2, 4, "offline")

        nodes.node3.startTaosd()
        tdSql.checkRows(3)
        tdSql.checkData(0, 4, "ready")
        tdSql.checkData(1, 4, "ready")    
        tdSql.checkData(2, 4, "ready")
        
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

ct = ClusterTestcase()
ct.run()