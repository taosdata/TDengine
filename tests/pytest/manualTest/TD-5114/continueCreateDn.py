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
        self.host = "chr03"
        self.user = "root"
        self.password = "taosdata"
        self.config = "/etc/taos/"     
        self.port =6030 
        self.rowNum = 10
        self.ts = 1537146000000  

    def run(self):
        
        # new taos client
        conn1 = taos.connect(host=self.host, user=self.user, password=self.password, config=self.config )
        print(conn1)
        cur1 = conn1.cursor()
        tdSql.init(cur1, True)

        tdSql.execute("drop database if exists db3")

        # insert data with c connector 
        for i in range(10):
            os.system("taosdemo -f manualTest/TD-5114/insertDataDb3Replica2.json -y ")
        # # check data correct 
        tdSql.execute("select * from information_schema.ins_databases")
        tdSql.execute("use db3")
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 20000)
        tdSql.query("select count (*) from stb0")
        tdSql.checkData(0, 0, 4000000)

        # insert data with  python connector , if you want to use this case ,cancel note.
        
        # for x in range(10):
        #     dataType= [ "tinyint", "smallint",  "int", "bigint", "float", "double", "bool", " binary(20)", "nchar(20)", "tinyint unsigned", "smallint unsigned", "int unsigned",  "bigint unsigned"] 
        #     tdSql.execute("drop database if exists db3") 
        #     tdSql.execute("create database db3 keep 3650  replica 2 ")
        #     tdSql.execute("use db3")
        #     tdSql.execute('''create table test(ts timestamp, col0 tinyint, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double, 
        #                 col7 bool, col8 binary(20), col9 nchar(20), col10 tinyint unsigned, col11 smallint unsigned, col12 int unsigned, col13 bigint unsigned) tags(loc nchar(3000), tag1 int)''')
        #     rowNum2= 988
        #     for i in range(rowNum2):
        #         tdSql.execute("alter table test add column col%d  %s ;" %( i+14, choice(dataType))     ) 
        #     rowNum3= 988
        #     for i in range(rowNum3):
        #         tdSql.execute("alter table test drop column col%d   ;" %( i+14)     )    
        #     self.rowNum = 50
        #     self.rowNum2 = 2000
        #     self.ts = 1537146000000
        #     for j in range(self.rowNum2):
        #         tdSql.execute("create table test%d using test tags('beijing%d', 10)" % (j,j) )
        #         for i in range(self.rowNum):
        #             tdSql.execute("insert into test%d values(%d, %d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)" 
        #                     % (j, self.ts + i*1000, i + 1, i + 1, i + 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1))

        # # check data correct 
        # tdSql.execute("select * from information_schema.ins_databases")
        # tdSql.execute("use db3")
        # tdSql.query("select count (tbname) from test")
        # tdSql.checkData(0, 0, 200) 
        # tdSql.query("select count (*) from test")
        # tdSql.checkData(0, 0, 200000) 


        # delete useless file
        testcaseFilename = os.path.split(__file__)[-1]
        os.system("rm -rf ./insert_res.txt")
        os.system("rm -rf manualTest/TD-5114/%s.sql" % testcaseFilename )       
   
clients = TwoClients()
clients.initConnection()
# clients.getBuildPath()
clients.run()