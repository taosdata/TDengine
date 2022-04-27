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
import datetime
from random import choice
 
class TwoClients:
    def initConnection(self):
        self.host = sys.argv[1]
        self.user = "root"
        self.password = "taosdata"
        self.config = "/etc/taos/"     
        self.port =6030 
        self.rowNum = 10
        self.ts = 1537146000000  

    def run(self):
        
        # new taos client
        conn1 = taos.connect(host=self.host, user=self.user, password=self.password, config=self.config )
        cur1 = conn1.cursor()
        print(cur1)
        tdSql.init(cur1, True)

        # insert data with  python connector , if you want to use this case ,cancel note.
        
        # check cluster status
        tdSql.query("show dnodes") 
        print(tdSql.queryRows)
        for i in range(tdSql.queryRows):
            for j in range(1,tdSql.queryRows+1):
                if (tdSql.queryResult[i][1] == "compat_container%d:6030" %j):
                    tdSql.checkData(i,4,"ready")

        tdSql.query("show mnodes") 
        tdSql.checkRows(3)
        roles = "master slave"
        for i in range(tdSql.queryRows):
            if (tdSql.queryResult[i][2] in roles ):
                ep = tdSql.queryResult[i][1]
                role = tdSql.queryResult[i][2]
                print(" the role of %s is %s " %(ep,role))
            else:
                print("cluster is not ready")

        version = sys.argv[2]
        tdSql.query("show variables") 
        for i in range(tdSql.queryRows):
            if (tdSql.queryResult[i][0] == "version" ):
                tdSql.checkData(i,1,"%s" % version) 



        # for x in range(10):
        dataType= [ "tinyint", "smallint",  "int", "bigint", "float", "double", "bool", " binary(20)", "nchar(20)", "tinyint unsigned", "smallint unsigned", "int unsigned",  "bigint unsigned"] 
        tdSql.execute("drop database if exists db1") 
        tdSql.execute("create database db1 keep 3650  replica 2 ")
        tdSql.execute("use db1")
        tdSql.execute('''create table test(ts timestamp, col0 tinyint, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double, 
                    col7 bool, col8 binary(20), col9 nchar(20), col10 tinyint unsigned, col11 smallint unsigned, col12 int unsigned, col13 bigint unsigned) tags(loc nchar(3000), tag1 int)''')
        print(datetime.datetime.now())
        rowNum1= 20
        for i in range(rowNum1):
            tdSql.execute("alter table test add column col%d  %s ;" %( i+14, choice(dataType))     ) 
        rowNum2= 20
        for i in range(rowNum2):
            tdSql.execute("alter table test drop column col%d   ;" %( i+14)     )    
        self.rowNum3 = 50
        self.rowNum4 = 100
        self.ts = 1537146000000
        for j in range(self.rowNum4):
            tdSql.execute("create table test%d using test tags('beijing%d', 10)" % (j,j) )
            for i in range(self.rowNum3):
                tdSql.execute("insert into test%d values(%d, %d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)" 
                        % (j, self.ts + i*1000, i + 1, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1))
        print(datetime.datetime.now())
        # check data correct 
        tdSql.execute("show databases")
        tdSql.execute("use db1")
        tdSql.query("select count (tbname) from test")
        tdSql.checkData(0, 0, 100) 
        tdSql.query("select count (*) from test")
        tdSql.checkData(0, 0, 5000) 


        # delete useless file
        testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf ./insert_res.txt")
        os.system("rm -rf manualTest/TD-5114/%s.sql" % testcaseFilename )       
   
clients = TwoClients()
clients.initConnection()
# clients.getBuildPath()
clients.run()