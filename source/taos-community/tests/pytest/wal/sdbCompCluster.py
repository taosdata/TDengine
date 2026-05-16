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
        self.host = "chenhaoran02"
        self.user = "root"
        self.password = "taosdata"
        self.config = "/etc/taos/"     
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

        # new db and insert data
        os.system("rm -rf  /var/lib/taos/mnode_bak/")
        os.system("rm -rf  /var/lib/taos/mnode_temp/")
        tdSql.execute("drop database if exists db2")
        os.system("%staosdemo -f wal/insertDataDb1.json -y " % binPath)
        tdSql.execute("drop database if exists db1") 
        os.system("%staosdemo -f wal/insertDataDb2.json -y " % binPath)
        tdSql.execute("drop table if exists db2.stb0") 
        os.system("%staosdemo -f wal/insertDataDb2Newstab.json -y " % binPath)
        query_pid1 = int(subprocess.getstatusoutput('ps aux|grep taosd |grep -v "grep"|awk \'{print $2}\'')[1])
        print(query_pid1)
        tdSql.execute("use db2")
        tdSql.execute("drop table if exists stb1_0")
        tdSql.execute("drop table if exists stb1_1")
        tdSql.execute("insert into stb0_0 values(1614218412000,8637,78.861045,'R','bf3')(1614218422000,8637,98.861045,'R','bf3')")
        tdSql.execute("alter table db2.stb0 add column col4 int")
        tdSql.execute("alter table db2.stb0 drop column col2")
        tdSql.execute("alter table db2.stb0  add tag t3 int")        
        tdSql.execute("alter table db2.stb0 drop tag t1")
        tdSql.execute("create table  if not exists stb2_0 (ts timestamp, col0 int, col1 float)  ")
        tdSql.execute("insert into stb2_0 values(1614218412000,8637,78.861045)")
        tdSql.execute("alter table stb2_0 add column col2 binary(4)")
        tdSql.execute("alter table stb2_0 drop column col1")
        tdSql.execute("insert into stb2_0 values(1614218422000,8638,'R')")

        # stop taosd and compact wal file
        os.system("ps -ef |grep taosd |grep -v 'grep' |awk '{print $2}'|xargs kill -2")
        sleep(10)
        os.system("nohup taosd  --compact-mnode-wal  -c /etc/taos & ")
        sleep(10)
        os.system("nohup /usr/bin/taosd > /dev/null 2>&1 &")
        sleep(4)
        tdSql.execute("reset query cache")
        query_pid2 = int(subprocess.getstatusoutput('ps aux|grep taosd |grep -v "grep"|awk \'{print $2}\'')[1])
        print(query_pid2)
        assert os.path.exists(walFilePath) , "%s is not generated " % walFilePath    

        # new taos connecting to server
        conn2 = taos.connect(host=self.host, user=self.user, password=self.password, config=self.config )
        print(conn2)
        cur2 = conn2.cursor()
        tdSql.init(cur2, True)

        # use new wal file to start up tasod 
        tdSql.query("select * from information_schema.ins_databases")
        for i in range(tdSql.queryRows):
            if tdSql.queryResult[i][0]=="db2":
                assert tdSql.queryResult[i][4]==1 , "replica is wrong"
        tdSql.execute("use db2")
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 1)
        tdSql.query("select count (tbname) from stb1")
        tdSql.checkRows(0)
        tdSql.query("select count(*) from stb0_0")
        tdSql.checkData(0, 0, 2)
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 2)
        tdSql.query("select count(*) from stb2_0")
        tdSql.checkData(0, 0, 2)
        tdSql.query("select * from stb2_0")
        tdSql.checkData(1, 2, 'R')
        
        # delete useless file       
        testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf ./insert_res.txt")
        os.system("rm -rf wal/%s.sql" % testcaseFilename )       

clients = TwoClients()
clients.initConnection()
# clients.getBuildPath()
clients.run()