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
    
    ##Cover test case 5 ##
    def run(self):
        # cluster environment set up
        nodes = Nodes()   
        nodes.addConfigs("maxVgroupsPerDb", "10")
        nodes.addConfigs("maxTablesPerVnode", "1000")
        nodes.restartAllTaosd()

        ctest = ClusterTest(nodes.node1.hostName)
        ctest.connectDB()
        ctest.createSTable(1)
        ctest.run()

        tdSql.init(ctest.conn.cursor(), False)
        tdSql.execute("use %s" % ctest.dbName)
        tdSql.error("create table tt1 using %s tags(1)" % ctest.stbName)

        nodes.removeConfigs("maxVgroupsPerDb", "10")
        nodes.removeConfigs("maxTablesPerVnode", "1000")
        nodes.restartAllTaosd()

        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)
        
ct = ClusterTestcase()
ct.run()