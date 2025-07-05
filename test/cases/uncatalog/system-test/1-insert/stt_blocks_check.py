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

from random import randint
from util.log import *
from util.cases import *
from util.sql import *
from util.common import *
from util.sqlset import *
from util.boundary import *

class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        self.replicaVar = int(replicaVar)
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())
        
    def stt_block_check(self):
        tdSql.prepare()
        tdSql.execute('use db')
        
        tdSql.execute('create table meters (ts timestamp, c1 int, c2 float) tags(t1 int)')
        tdSql.execute("create table d0 using meters tags(1)")
        
        ts = 1704261670000

        sql = "insert into d0 values "
        for i in range(100):
            sql = sql + f"({ts + i}, 1, 0.1)"
        tdSql.execute(sql)
        tdSql.execute("flush database db")
        
        ts = 1704261670099

        sql = "insert into d0 values "
        for i in range(100):
            sql = sql + f"({ts + i}, 1, 0.1)"
        tdSql.execute(sql)
        tdSql.execute("flush database db")
        
        tdSql.execute(f"insert into d0 values({ts + 100}, 2, 1.0)")
        tdSql.execute("flush database db")
        
        time.sleep(2)
        
        tdSql.query("show table distributed db.meters")
        tdSql.query("select count(*) from db.meters")
        tdSql.checkData(0, 0, 200)

    def run(self):
        self.stt_block_check()

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())