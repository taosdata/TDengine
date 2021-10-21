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

import os
import sys
sys.path.insert(0, os.getcwd())
from util.log import *
from util.sql import *
from util.dnodes import *
import taos
import threading
import subprocess
from random import choice

 
class TwoClients:
    def initConnection(self):
        self.host = "chenhaoran01"
        self.user = "root"
        self.password = "taosdata"
        self.config = "/home/chr/cfg/single/"     
        self.port =6030 
        self.rowNum = 10
        self.ts = 1537146000000  

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
        buildPath = self.getBuildPath()
        if (buildPath == ""):
            tdLog.exit("taosd not found!")
        else:
            tdLog.info("taosd found in %s" % buildPath)
        binPath = buildPath+ "/build/bin/"
        walFilePath = "/var/lib/taos/mnode_bak/wal/"
        
        # new taos client
        conn1 = taos.connect(host=self.host, user=self.user, password=self.password, config=self.config )
        print(conn1)
        cur1 = conn1.cursor()
        tdSql.init(cur1, True)

        # create backgroud db and tb
        tdSql.execute("drop database if exists db1")
        os.system("%staosdemo -f compress/insertDataDb1.json -y " % binPath)
        # create foreground db and tb 
        tdSql.execute("drop database if exists foredb")
        tdSql.execute("create database foredb")
        tdSql.execute("use foredb")
        print("123test")
        tdSql.execute("create stable if not exists stb (ts timestamp, dataInt int, dataDouble  double,dataStr nchar(200)) tags(loc nchar(50),t1 int)")
        tdSql.execute("create table tb1 using stb tags('beijing1', 10)")
        tdSql.execute("insert into tb1 values(1614218412000,8635,98.861,'qazwsxedcrfvtgbyhnujmikolp1')(1614218422000,8636,98.862,'qazwsxedcrfvtgbyhnujmikolp2')")
        tdSql.execute("create table tb2 using stb tags('beijing2', 11)")
        tdSql.execute("insert into tb2 values(1614218432000,8647,98.863,'qazwsxedcrfvtgbyhnujmikolp3')")
        tdSql.execute("insert into tb2 values(1614218442000,8648,98.864,'qazwsxedcrfvtgbyhnujmikolp4')")


        # check data correct 
        tdSql.execute("use db1")
        tdSql.query("select count(tbname) from stb0")
        tdSql.checkData(0, 0, 50000)
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 5000000)        
        tdSql.execute("use foredb")
        tdSql.query("select count (tbname) from stb")
        tdSql.checkData(0, 0, 2)
        tdSql.query("select count (*) from stb")
        tdSql.checkData(0, 0, 4)
        tdSql.query("select * from tb1 order by ts")
        tdSql.checkData(0, 3, "qazwsxedcrfvtgbyhnujmikolp1")
        tdSql.query("select * from tb2 order by ts")
        tdSql.checkData(1, 3, "qazwsxedcrfvtgbyhnujmikolp4")



        # delete useless file       
        testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf ./insert_res.txt")
        # os.system("rm -rf compress/%s.sql" % testcaseFilename )       

clients = TwoClients()
clients.initConnection()
# clients.getBuildPath()
clients.run()