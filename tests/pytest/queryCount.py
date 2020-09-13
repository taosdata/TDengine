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
import taos
import threading
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class QueryCountMultiThread:
    def initConnection(self):        
        self.records = 10000000
        self.numOfTherads = 50
        self.ts = 1537146000000
        self.host = "127.0.0.1"
        self.user = "root"
        self.password = "taosdata"
        self.config = "/home/xp/git/TDengine/sim/dnode1/cfg"
        self.conn = taos.connect(
            self.host,
            self.user,
            self.password,
            self.config)

    def insertData(self, threadID):
        cursor = self.conn.cursor()
        print("Thread %d: starting" % threadID)
        base = 200000 * threadID
        for i in range(200):                        
            query = "insert into tb values"
            for j in range(1000):
                query += "(%d, %d, 'test')" % (self.ts + base + i * 1000 + j, base + i * 1000 + j)                        
            cursor.execute(query)            
        cursor.close()
        print("Thread %d: finishing" % threadID)

    def run(self):
        tdDnodes.init("")
        tdDnodes.setTestCluster(False)
        tdDnodes.setValgrind(False)

        tdDnodes.stopAll()
        tdDnodes.deploy(1)
        tdDnodes.start(1)
        
        cursor = self.conn.cursor()
        cursor.execute("drop database if exists db")
        cursor.execute("create database db")
        cursor.execute("use db")
        cursor.execute("create table tb (ts timestamp, id int, name nchar(30))")
        cursor.close()

        threads = []
        for i in range(50):
            thread = threading.Thread(target=self.insertData, args=(i,))
            threads.append(thread)
            thread.start()            
        
        for i in range(50):
            threads[i].join()

        cursor = self.conn.cursor()
        cursor.execute("use db")
        sql = "select count(*) from tb"
        cursor.execute(sql)
        data = cursor.fetchall()

        if(data[0][0] == 10000000):
            tdLog.info("sql:%s, row:%d col:%d data:%d == expect:%d" % (sql, 0, 0, data[0][0], 10000000))
        else:            
            tdLog.exit("queryCount.py failed: sql:%s failed, row:%d col:%d data:%d != expect:%d" % (sql, 0, 0, data[0][0], 10000000))        

        cursor.close()
        self.conn.close()

q = QueryCountMultiThread()
q.initConnection()
q.run()