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

 
class TwoClients:
    def initConnection(self):
        self.host = "node1"
        self.host2 = "chenhaoran02"
        self.user = "root"
        self.password = "taosdata"
        self.config = "/etc/taos/"     
        self.port =6030 
        self.rowNum = 10
        self.ts = 1537146000000  

    # def getBuildPath(self):
    #     selfPath = os.path.dirname(os.path.realpath(__file__))

    #     if ("community" in selfPath):
    #         projPath = selfPath[:selfPath.find("community")]
    #     else:
    #         projPath = selfPath[:selfPath.find("tests")]

    #     for root, dirs, files in os.walk(projPath):
    #         if ("taosd" in files):
    #             rootRealPath = os.path.dirname(os.path.realpath(root))
    #             if ("packaging" not in rootRealPath):
    #                 buildPath = root[:len(root)-len("/build/bin")]
    #                 break
    #     return buildPath

    def run(self):
        # buildPath = self.getBuildPath()
        # if (buildPath == ""):
        #     tdLog.exit("taosd not found!")
        # else:
        #     tdLog.info("taosd found in %s" % buildPath)
        # binPath = buildPath+ "/build/bin/"

        # query data from cluster'db
        conn1 = taos.connect(host=self.host, user=self.user, password=self.password, config=self.config )
        print(conn1)
        # conn2 = taos.connect(host=self.host2, user=self.user, password=self.password, config=self.config)
        cur1 = conn1.cursor()
        # cur2 = conn2.cursor()
        tdSql.init(cur1, True)
        # tdSql.init(cur2, True)
        # os.system("%staosdemo -f wal/insertDataDb1.json -y " % binPath)
        # tdSql.execute("drop database if exists db1") 
        # os.system("%staosdemo -f wal/insertDataDb2.json -y " % binPath)
        # tdSql.execute("drop table if exists db2.stb0") 
        # os.system("%staosdemo -f wal/insertDataDb2Newstab.json -y " % binPath)
        # tdSql.execute("alter table db2.stb0 add column col4 int")
        # tdSql.execute("alter table db2.stb0 drop column col2")
        # os.system("ps -ef |grep taosd |grep -v 'grep' |awk '{print $2}'|xargs kill -9")
        # os.system("nohup taosd  --compact-mnode-wal  -c /home/chr/TDengine/sim/dnode1/cfg/ & ")
        # sleep(5)
        # os.system("nohup /home/chr/TDengine/debug/build/bin/taosd -c /home/chr/TDengine/sim/dnode1/cfg > /dev/null 2>&1 &")
        # sleep(4)
        # tdSql.execute("use db2")
        # tdSql.query("select count (tbname) from stb0")
        # tdSql.checkData(0, 0, 1)
        # tdSql.query("select count (tbname) from stb1")
        # tdSql.checkData(0, 0, 2)
        # tdSql.query("select count(*) from stb00_0")
        # tdSql.checkData(0, 0, 1)
        # tdSql.query("select count(*) from stb0")
        # tdSql.checkData(0, 0, 1)
        # tdSql.query("select count(*) from stb1")
        # tdSql.checkData(0, 0, 20)
       
clients = TwoClients()
clients.initConnection()
# clients.getBuildPath()
clients.run()