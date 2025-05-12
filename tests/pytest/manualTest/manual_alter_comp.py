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

    def getRootPath(self):
        selfPath = os.path.dirname(os.path.realpath(__file__))
        print(selfPath)
        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("community")]
            print(projPath)
        else:
            projPath = selfPath[:selfPath.find("tests")]
            print("test" + projPath)

        for root, dirs, files in os.walk(projPath):
            if ('data' in dirs and 'sim' in root):
                rootPath = root

        return rootPath
        
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
        dnodePath = self.getRootPath()
        os.system(f'rm -rf {dnodePath}/data/* {dnodePath}/log/*')
        tdSql.prepare()
        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        binPath = buildPath+ "/build/bin/"  

        #comp is at 14
        #check disk usage when comp=2
        tdSql.query('select * from information_schema.ins_databases')
        tdSql.execute('alter database db blocks 3') # minimize the data in cache
        tdSql.checkData(0,14,2)
        os.system("%staosdemo -f tools/taosdemoAllTest/manual_block1_comp.json" % binPath)
        print("default location is at /home/bryan/Documents/Github/TDengine/sim/dnode1/data/vnode")
        print('comp = 2')
        input("please check disk usage for taosd. After checking, press enter")
        
        #removing all data file
        os.system(f'sudo rm -rf {dnodePath}/data/* {dnodePath}/log/*')
        #print(f'rm -rf {dnodePath}/data/* {dnodePath}/log/*') #for showing the command ran
        input("please check if the pervious data is being deleted. Then, press enter")

        #check disk usage when comp=0
        tdSql.prepare()
        tdSql.query('select * from information_schema.ins_databases')
        tdSql.checkData(0,14,2)
        tdSql.execute('alter database db comp 0')
        tdSql.query('select * from information_schema.ins_databases')
        tdSql.checkData(0,14,0)
        os.system("%staosdemo -f tools/taosdemoAllTest/manual_block1_comp.json" % binPath)
        print("default location is at /home/bryan/Documents/Github/TDengine/sim/dnode1/data")
        print('comp = 0')
        input("please check disk usage for taosd. After checking, press enter")

        #removing all data file
        os.system(f'sudo rm -rf {dnodePath}/data/* {dnodePath}/log/*')
        #print(f'rm -rf {dnodePath}/data/* {dnodePath}/log/*') #for showing the command ran
        input("please check if the pervious data is being deleted. Then, press enter")

        #check disk usage when comp=1
        tdSql.prepare()
        tdSql.query('select * from information_schema.ins_databases')
        tdSql.checkData(0,14,2)
        tdSql.execute('alter database db comp 1')
        tdSql.query('select * from information_schema.ins_databases')
        tdSql.checkData(0,14,1)
        os.system("%staosdemo -f tools/taosdemoAllTest/manual_block1_comp.json" % binPath)
        print("default location is at /home/bryan/Documents/Github/TDengine/sim/dnode1/data")
        print('comp = 1')
        input("please check disk usage for taosd. After checking, press enter")

        ##test result
        #   2021/06/02 comp=2:13M   comp=1:57M comp=0:399M. Test past
        #       each row entered is identical       Tester - Baosheng Chang

    def stop(self):
        tdSql.close()
        tdLog.debug("%s alter block manual check finish" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
