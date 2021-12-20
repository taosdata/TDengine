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


class TDTestCase:
    
    updatecfgDict={'clientMerge':1}

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor())

        self.ts = 1606700000000

    def run(self):
        # TS-932
        tdSql.execute("create database db3 update 2")
        tdSql.execute("use db3")
        tdSql.execute("drop table if exists tb")
        tdSql.execute("create table tb (ts timestamp, c1 int, c2 int, c3 int)")
        tdSql.execute("insert into tb values(%d, 1, 2, 3)(%d, null, 4, 5)(%d, 6, null, null)" % (self.ts, self.ts, self.ts))            
        
        tdSql.query("select * from tb")
        tdSql.checkData(0, 1, 6)
        tdSql.checkData(0, 2, None)
        tdSql.checkData(0, 3, None)
        
    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)

tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())
