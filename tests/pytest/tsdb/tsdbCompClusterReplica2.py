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

        # new db ,new super tables , child tables, and insert  data
        tdSql.execute("drop database if exists db2")
        os.system("%staosdemo -f tsdb/insertDataDb1Replica2.json -y " % binPath)
        tdSql.execute("drop database if exists db1") 
        os.system("%staosdemo -f tsdb/insertDataDb2Replica2.json -y " % binPath)
        tdSql.execute("drop table if exists db2.stb0") 
        os.system("%staosdemo -f tsdb/insertDataDb2NewstabReplica2.json -y " % binPath)

        # new general tables and modify general tables;  
        tdSql.execute("use db2")
        tdSql.execute("drop table if exists stb1_0")
        tdSql.execute("drop table if exists stb1_1")
        tdSql.execute("insert into stb0_0 values(1614218412000,8637,78.861045,'R','bf3')(1614218422000,8637,98.861045,'R','bf3')")
        tdSql.execute("alter table db2.stb0 add column c4 int")
        tdSql.execute("alter table db2.stb0 drop column c2")
        tdSql.execute("alter table db2.stb0  add tag t3 int")        
        tdSql.execute("alter table db2.stb0 drop tag t1")
        tdSql.execute("create table  if not exists stb2_0 (ts timestamp, c0 int, c1 float)  ")
        tdSql.execute("insert into stb2_0 values(1614218412000,8637,78.861045)")
        tdSql.execute("alter table stb2_0 add column c2 binary(4)")
        tdSql.execute("alter table stb2_0 drop column c1")
        tdSql.execute("insert into stb2_0 values(1614218422000,8638,'R')")


        # create  db utest  replica 2 and modify super tables;
        dataType= [ "tinyint", "smallint",  "int", "bigint", "float", "double", "bool", " binary(20)", "nchar(20)", "tinyint unsigned", "smallint unsigned", "int unsigned",  "bigint unsigned"] 
        tdSql.execute("drop database if exists utest") 
        tdSql.execute("create database utest keep 3650  replica 2 ")
        tdSql.execute("use utest")
        tdSql.execute('''create table test(ts timestamp, col0 tinyint, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double, 
                    col7 bool, col8 binary(20), col9 nchar(20), col10 tinyint unsigned, col11 smallint unsigned, col12 int unsigned, col13 bigint unsigned) tags(loc nchar(200), tag1 int)''')
        rowNum2= 988
        for i in range(rowNum2):
            tdSql.execute("alter table test add column col%d  %s ;" %( i+14, choice(dataType))     ) 
        rowNum3= 988
        for i in range(rowNum3):
            tdSql.execute("alter table test drop column col%d   ;" %( i+14)     )    
        self.rowNum = 1
        self.rowNum2 = 100
        self.ts = 1537146000000
        for j in range(self.rowNum2):
            tdSql.execute("create table test%d using test tags('beijing%d', 10)" % (j,j) )
            for i in range(self.rowNum):
                tdSql.execute("insert into test%d values(%d, %d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)" 
                        % (j, self.ts + i*1000, i + 1, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1))
        #  delete child tables;  
        for j in range(self.rowNum2):    
            tdSql.execute("drop table if exists test%d" % (j+1))
        
        # drop dnodes and restart taosd;
        sleep(3)
        tdSql.execute(" drop dnode 'chenhaoran02:6030'; ")
        sleep(20)
        # remove data file;
        os.system("rm -rf /home/chr/data/data0/*")
        print("clear dnode chenhaoran02'data files")
        sleep(5)
        os.system("nohup /usr/bin/taosd > /dev/null 2>&1 &")
        print("start taosd")
        sleep(10)
        tdSql.execute("reset query cache ;")
        tdSql.execute("create dnode chenhaoran02 ;")

        # # 
        # os.system("ps -ef |grep taosd |grep -v 'grep' |awk '{print $2}'|xargs kill -2")
        # sleep(20)
        # os.system("nohup /usr/bin/taosd > /dev/null 2>&1 &")
        # sleep(4)
        # tdSql.execute("reset query cache")
        # query_pid2 = int(subprocess.getstatusoutput('ps aux|grep taosd |grep -v "grep"|awk \'{print $2}\'')[1])
        # print(query_pid2)

        # new taos connecting to server
        conn2 = taos.connect(host=self.host, user=self.user, password=self.password, config=self.config )
        print(conn2)
        cur2 = conn2.cursor()
        tdSql.init(cur2, True)

        # check data correct 
        tdSql.query("select * from information_schema.ins_databases")
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

        tdSql.execute("use utest")
        tdSql.query("select count (tbname) from test")
        tdSql.checkData(0, 0, 1) 

        # delete useless file
        testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf ./insert_res.txt")
        os.system("rm -rf tsdb/%s.sql" % testcaseFilename )       
   
clients = TwoClients()
clients.initConnection()
# clients.getBuildPath()
clients.run()