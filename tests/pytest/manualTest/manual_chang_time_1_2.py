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
from datetime import datetime

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

        ## change system time to 2020/10/20
        os.system ('timedatectl set-ntp off')
        os.system ('timedatectl set-time 2020-10-20')

        #run taosdemo to insert data. one row per second from 2020/10/11 to 2020/10/20
        #11 data files should be generated
        #vnode at TDinternal/community/sim/dnode1/data/vnode
        os.system("%staosdemo -f tools/taosdemoAllTest/manual_change_time_1_1_A.json" % binPath) 
        input("please the data file. After checking, press enter")

        tdSql.query('select first(ts) from stb_0') #check the last data in the database
        tdSql.checkData(0,0,datetime(2020,10,11,0,0,0,0))

        os.system ('timedatectl set-time 2020-10-25')

        #restart taosd
        #4 oldest data file should be removed from tsdb/data
        #vnode at TDinternal/community/sim/dnode1/data/vnode
        #7 data file should be found 
        tdDnodes.stop(1)
        tdDnodes.start(1)
        tdSql.query('select first(ts) from stb_0')
        tdSql.checkData(0,0,datetime(2020,10,14,8,0,0,0)) #check the last data in the database
        input("please the data file. After checking, press enter")



    def stop(self):
        tdSql.close()
        tdLog.debug("%s alter block manual check finish" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
