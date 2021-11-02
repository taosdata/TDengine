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
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *
import multiprocessing as mp

class TDTestCase:
    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        self.ts = 1609430400000

    def alterTableSchema(self):
        conn1 = taos.connect(host="127.0.0.1", user="root", password="taosdata", config=tdDnodes.getSimCfgPath())
        c1 = conn1.cursor()

        c1.execute("use db")
        c1.execute("alter table st drop column c2")
        c1.execute("alter table st add column c2 double")

        tdLog.sleep(1)
        c1.execute("select * from st")
        for data in c1:
            print("Process 1: c2 = %s" % data[2])

    
    def insertData(self):
        conn2 = taos.connect(host="127.0.0.1", user="root", password="taosdata", config=tdDnodes.getSimCfgPath())
        c2 = conn2.cursor()

        tdLog.sleep(1)
        c2.execute("use db")
        c2.execute("insert into t1 values(%d, 2, 2.22)" % (self.ts + 1))

        c2.execute("select * from st")
        for data in c2:
            print("Process 2: c2 = %f" % data[2])

    def run(self):
        tdSql.prepare()
        tdSql.execute("create table st(ts timestamp, c1 int, c2 float) tags(t1 int)")
        tdSql.execute("create table t1 using st tags(1)")
        tdSql.execute("insert into t1 values(%d, 1, 1.11)" % self.ts)
        p1 = mp.Process(target=self.alterTableSchema, args=())
        p2 = mp.Process(target=self.insertData, args=())
        p1.start()
        p2.start()

        p1.join()
        p2.join()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())