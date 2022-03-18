###################################################################
 #		   Copyright (c) 2020 by TAOS Technologies, Inc.
 #				     All rights reserved.
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
import time
import random
import string
from util.log import *
from util.cases import *
from util.sql import *
from util.sub import *

class TDTestCase:
    maxTables = 10000
    maxCols = 50
    rowsPerSecond = 1000

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdLog.notice("NOTE: this case does not stop automatically, Ctrl+C to stop")
        tdSql.init(conn.cursor(), logSql)
        self.conn = conn


    def generateString(self, length):
        chars = string.ascii_uppercase + string.ascii_lowercase
        v = ""
        for i in range(length):
            v += random.choice(chars)
        return v


    def insert(self):
        id = random.randint(0, self.maxTables - 1)
        cola = self.generateString(40)
        sql = "insert into car%d values(now, '%s', %f, %d" % (id, cola, random.random()*100, random.randint(0, 2))
        for i in range(self.maxCols):
            sql += ", %d" % random.randint(0, self.maxTables)
        sql += ")"
        tdSql.execute(sql)


    def prepare(self):
        tdLog.info("prepare database: test")
        tdSql.execute('reset query cache')
        tdSql.execute('drop database if exists test')
        tdSql.execute('create database test')
        tdSql.execute('use test')

    def run(self):
        self.prepare()

        sql = "create table cars (ts timestamp, a binary(50), b float, c bool"
        for i in range(self.maxCols):
            sql += ", c%d int" % i
        sql += ") tags(id int, category binary(30), brand binary(30));"
        tdSql.execute(sql)

        for i in range(self.maxTables):
            tdSql.execute("create table car%d using cars tags(%d, 'category%d', 'brand%d')" % (i, i, i % 30, i // 30))

        time.sleep(0.1)

        total = 0
        while True:
            start = time.time()
            for i in range(self.rowsPerSecond):
                self.insert()
                total = total + 1
            d = time.time() - start
            tdLog.info("%d rows inserted in %f seconds, total %d" % (self.rowsPerSecond, d, total))
            if d < 1:
                time.sleep(1 - d)


    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
