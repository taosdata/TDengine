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
        self.host = "fct4"
        self.user = "root"
        self.password = "taosdata"
        self.config = "/etc/taos/"       
        self.rowNum = 10
        self.ts = 1537146000000  

    def run(self):
        # query data from cluster'db
        conn = taos.connect(host=self.host, user=self.user, password=self.password, config=self.config)
        cur = conn.cursor()
        tdSql.init(cur, True)
        tdSql.execute("use db2")
        cur.execute("select count (tbname) from stb0") 
        tdSql.query("select count (tbname) from stb0")
        tdSql.checkData(0, 0, 10)
        tdSql.query("select count (tbname) from stb1")
        tdSql.checkData(0, 0, 20)
        tdSql.query("select count(*) from stb00_0")
        tdSql.checkData(0, 0, 10000)   
        tdSql.query("select count(*) from stb0")
        tdSql.checkData(0, 0, 100000) 
        tdSql.query("select count(*) from stb01_0")
        tdSql.checkData(0, 0, 20000)   
        tdSql.query("select count(*) from stb1")
        tdSql.checkData(0, 0, 400000)
        tdSql.execute("drop table if exists squerytest")
        tdSql.execute("drop table if exists querytest")
        tdSql.execute('''create stable squerytest(ts timestamp, col1 tinyint, col2 smallint, col3 int, col4 bigint, col5 float, col6 double, col7 bool, col8 binary(20), col9 nchar(20), col11 tinyint unsigned, col12 smallint unsigned, col13 int unsigned, col14 bigint unsigned) tags(loc nchar(20))''')
        tdSql.execute("create table querytest using squerytest tags('beijing')")
        tdSql.execute("insert into querytest(ts) values(%d)" % (self.ts - 1))
        for i in range(self.rowNum):
            tdSql.execute("insert into querytest values(%d, %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d', %d, %d, %d, %d)" % (self.ts + i, i + 1, 1, i + 1, i + 1, i + 0.1, i + 0.1, i % 2, i + 1, i + 1, i + 1, i + 1, i + 1, i + 1))
        for j in range(10):
            tdSql.execute("use db2")
            tdSql.query("select count(*),last(*) from querytest  group by col1")
            tdSql.checkRows(10)
            tdSql.checkData(0, 0, 1)
            tdSql.checkData(1, 2, 2)
            tdSql.checkData(1, 3, 1)
            sleep(88)
        tdSql.execute("drop table if exists squerytest")
        tdSql.execute("drop table if exists querytest")
       
clients = TwoClients()
clients.initConnection()
clients.run()