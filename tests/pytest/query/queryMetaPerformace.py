
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
import time
from datetime import datetime
import numpy as np


class MyThread(threading.Thread):

    def __init__(self, func, args=()):
        super(MyThread, self).__init__()
        self.func = func
        self.args = args

    def run(self):
        self.result = self.func(*self.args)

    def get_result(self):
        try:
            return self.result   # 如果子线程不使用join方法，此处可能会报没有self.result的错误
        except Exception:
            return None


class MetadataQuery:
    def initConnection(self):
        self.tables = 100
        self.records = 10
        self.numOfTherads = 5
        self.ts = 1537146000000
        self.host = "127.0.0.1"
        self.user = "root"
        self.password = "taosdata"
        self.config = "/etc/taos"
        self.conn = taos.connect(
            self.host,
            self.user,
            self.password,
            self.config)

    def connectDB(self):
        return self.conn.cursor()

    def createStable(self):
        print("================= Create stable meters =================")
        cursor = self.connectDB()
        cursor.execute("drop database if exists test")
        cursor.execute("create database test")
        cursor.execute("use test")
        cursor.execute('''create table if not exists meters (ts timestamp, speed int) tags(
                    tgcol1 tinyint, tgcol2 smallint, tgcol3 int, tgcol4 bigint, tgcol5 float, tgcol6 double, tgcol7 bool, tgcol8 binary(20), tgcol9 nchar(20),
                    tgcol10 tinyint, tgcol11 smallint, tgcol12 int, tgcol13 bigint, tgcol14 float, tgcol15 double, tgcol16 bool, tgcol17 binary(20), tgcol18 nchar(20),
                    tgcol19 tinyint, tgcol20 smallint, tgcol21 int, tgcol22 bigint, tgcol23 float, tgcol24 double, tgcol25 bool, tgcol26 binary(20), tgcol27 nchar(20),
                    tgcol28 tinyint, tgcol29 smallint, tgcol30 int, tgcol31 bigint, tgcol32 float, tgcol33 double, tgcol34 bool, tgcol35 binary(20), tgcol36 nchar(20),
                    tgcol37 tinyint, tgcol38 smallint, tgcol39 int, tgcol40 bigint, tgcol41 float, tgcol42 double, tgcol43 bool, tgcol44 binary(20), tgcol45 nchar(20),
                    tgcol46 tinyint, tgcol47 smallint, tgcol48 int, tgcol49 bigint, tgcol50 float, tgcol51 double, tgcol52 bool, tgcol53 binary(20), tgcol54 nchar(20))''')
        cursor.close()

    def createTablesAndInsertData(self, threadID):
        cursor = self.connectDB()
        cursor.execute("use test")
        base = threadID * self.tables

        tablesPerThread = int(self.tables / self.numOfTherads)
        for i in range(tablesPerThread):
            cursor.execute(
                '''create table t%d using meters tags(
                %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d',
                %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d',
                %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d',
                %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d',
                %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d',
                %d, %d, %d, %d, %f, %f, %d, 'taosdata%d', '涛思数据%d')''' %
                (base + i + 1, (base + i) %
                 100, (base + i) %
                    10000, (base + i) %
                    1000000, (base + i) %
                    100000000, (base + i) %
                    100 * 1.1, (base + i) %
                    100 * 2.3, (base + i) %
                    2, (base + i) %
                    100, (base + i) %
                    100, (base + i) %
                    100, (base + i) %
                    10000, (base + i) %
                    1000000, (base + i) %
                    100000000, (base + i) %
                    100 * 1.1, (base + i) %
                    100 * 2.3, (base + i) %
                    2, (base + i) %
                    100, (base + i) %
                    100, (base + i) %
                    100, (base + i) %
                    10000, (base + i) %
                    1000000, (base + i) %
                    100000000, (base + i) %
                    100 * 1.1, (base + i) %
                    100 * 2.3, (base + i) %
                    2, (base + i) %
                    100, (base + i) %
                    100, (base + i) %
                    100, (base + i) %
                    10000, (base + i) %
                    1000000, (base + i) %
                    100000000, (base + i) %
                    100 * 1.1, (base + i) %
                    100 * 2.3, (base + i) %
                    2, (base + i) %
                    100, (base + i) %
                    100, (base + i) %
                    100, (base + i) %
                    10000, (base + i) %
                    1000000, (base + i) %
                    100000000, (base + i) %
                    100 * 1.1, (base + i) %
                    100 * 2.3, (base + i) %
                    2, (base + i) %
                    100, (base + i) %
                    100, (base + i) %
                    100, (base + i) %
                    10000, (base + i) %
                    1000000, (base + i) %
                    100000000, (base + i) %
                    100 * 1.1, (base + i) %
                    100 * 2.3, (base + i) %
                    2, (base + i) %
                    100, (base + i) %
                    100))
            for j in range(self.records):
                cursor.execute(
                    "insert into t%d values(%d, %d)" %
                    (base + i + 1, self.ts + j, j))
        cursor.close()

    def queryWithTagId(self, threadId, tagId, queryNum):
        print("---------thread%d start-----------" % threadId)
        query = '''select tgcol1, tgcol2, tgcol3, tgcol4, tgcol5, tgcol6, tgcol7, tgcol8, tgcol9,
                tgcol10, tgcol11, tgcol12, tgcol13, tgcol14, tgcol15, tgcol16, tgcol17, tgcol18,
                tgcol19, tgcol20, tgcol21, tgcol22, tgcol23, tgcol24, tgcol25, tgcol26, tgcol27,
                tgcol28, tgcol29, tgcol30, tgcol31, tgcol32, tgcol33, tgcol34, tgcol35, tgcol36,
                tgcol37, tgcol38, tgcol39, tgcol40, tgcol41, tgcol42, tgcol43, tgcol44, tgcol45,
                tgcol46, tgcol47, tgcol48, tgcol49, tgcol50, tgcol51, tgcol52, tgcol53, tgcol54
                from meters where tgcol{id} > {condition}'''
        latancy = []
        cursor = self.connectDB()
        cursor.execute("use test")
        for i in range(queryNum):
            startTime = time.time()
            cursor.execute(query.format(id=tagId, condition=i))
            cursor.fetchall()
            latancy.append((time.time() - startTime))
        print("---------thread%d end-----------" % threadId)
        return latancy

    def queryData(self, query):
        cursor = self.connectDB()
        cursor.execute("use test")

        print("================= query tag data =================")
        startTime = datetime.now()
        cursor.execute(query)
        cursor.fetchall()
        endTime = datetime.now()
        print(
            "Query time for the above query is %d seconds" %
            (endTime - startTime).seconds)

        cursor.close()
        # self.conn.close()


if __name__ == '__main__':

    t = MetadataQuery()
    t.initConnection()

    latancys = []
    threads = []
    tagId = 1
    queryNum = 1000
    for i in range(t.numOfTherads):
        thread = MyThread(t.queryWithTagId, args=(i, tagId, queryNum))
        threads.append(thread)
        thread.start()
    for i in range(t.numOfTherads):
        threads[i].join()
        latancys.extend(threads[i].get_result())
    print("Total query: %d" % (queryNum * t.numOfTherads))
    print(
        "statistic(s): mean= %f, P50 = %f, P75 = %f, P95 = %f, P99 = %f" %
        (sum(latancys) /
         (
            queryNum *
            t.numOfTherads),
            np.percentile(
            latancys,
            50),
            np.percentile(
            latancys,
            75),
            np.percentile(
            latancys,
            95),
            np.percentile(
            latancys,
            99)))
