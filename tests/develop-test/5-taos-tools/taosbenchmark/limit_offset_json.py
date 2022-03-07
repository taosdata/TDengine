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
from util.log import *
from util.cases import *
from util.sql import *
from util.dnodes import *


class TDTestCase:
    def caseDescription(self):
        '''
        [TD-11510] taosBenchmark test cases
        '''
        return

    def init(self, conn, logSql):
        tdLog.debug("start to execute %s" % __file__)
        tdSql.init(conn.cursor(), logSql)

    def run(self):
        cmd = "taosBenchmark -f ./5-taos-tools/taosbenchmark/json/taosc_only_create_table.json"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("select count(tbname) from db.stb")
        tdSql.checkData(0, 0, 8)
        tdSql.query("select count(*) from db.stb")
        tdSql.checkRows(0)
        tdSql.query("describe db.stb")
        tdSql.checkData(9, 1, "NCHAR")
        tdSql.checkData(14, 1, "BINARY")
        tdSql.checkData(23, 1, "NCHAR")
        tdSql.checkData(28, 1, "BINARY")
        tdSql.checkData(9, 2, 64)
        tdSql.checkData(14, 2, 64)
        tdSql.checkData(23, 2, 64)
        tdSql.checkData(28, 2, 64)


        cmd = "taosBenchmark -f ./5-taos-tools/taosbenchmark/json/taosc_limit_offset.json"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("select count(tbname) from db.stb")
        tdSql.checkData(0, 0, 8)
        tdSql.query("select count(*) from db.stb")
        tdSql.checkData(0, 0, 40)
        tdSql.query("select distinct(c1) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c3) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c4) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c5) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c6) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c7) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c8) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c9) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c10) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c11) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c12) from db.stb")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c13) from db.stb")
        tdSql.checkData(0, 0, None)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())