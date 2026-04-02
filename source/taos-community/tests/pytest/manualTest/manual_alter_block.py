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
from util.dnodes import tdDnodes

##TODO: auto test version is currently unsupported, need to come up with 
#       an auto test version in the future
class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def getBuildPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))

        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root)-len("/build/bin")]
                    break
        return buildPath
        
    def run(self):
        tdSql.prepare()
        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        binPath = buildPath+ "/build/bin/"  

        #alter cache block to 3, then check alter
        tdSql.execute('alter database db blocks 3')
        tdSql.query('select * from information_schema.ins_databases')
        tdSql.checkData(0,9,3)

        #run taosdemo to occupy all cache, need to manually check memory consumption
        os.system("%staosdemo -f tools/taosdemoAllTest/manual_block1_comp.json" % binPath) 
        input("please check memory usage for taosd. After checking, press enter")

        #alter cache block to 8, then check alter
        tdSql.execute('alter database db blocks 8')
        tdSql.query('select * from information_schema.ins_databases')
        tdSql.checkData(0,9,8)

        #run taosdemo to occupy all cache, need to manually check memory consumption
        os.system("%staosdemo -f tools/taosdemoAllTest/manual_block2.json" % binPath) 
        input("please check memory usage for taosd. After checking, press enter")

        ##expected result the peak memory consumption should increase by around 80MB = 5 blocks of cache

        ##test results
        #2021/06/02 before:2621700K     after: 2703640K memory usage increased by 80MB = 5 block
        #           confirm with the change in block.       Baosheng Chang

    def stop(self):
        tdSql.close()
        tdLog.debug("%s alter block manual check finish" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
