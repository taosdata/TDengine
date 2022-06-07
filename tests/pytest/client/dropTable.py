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

    def run(self):
        tdSql.prepare()
        os.system("taos -s 'create table db.st(ts timestamp, c1 int, c2 float) tags(t1 int)' ")
        os.system("taos -s 'create table db.t1 using db.st tags(1)' ")
        os.system("taos -s 'insert into t1 values(%d, 1, 1.11)'" % self.ts)        
        os.system("taos -s 'desc db.st'")
        tdSql.query("desc db.st")
        tdSql.checkRows(4)

        os.system("taos -s 'drop table db.st'")
        tdSql.error("select * from db.st")        
        tdSql.error("desc db.st")

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())