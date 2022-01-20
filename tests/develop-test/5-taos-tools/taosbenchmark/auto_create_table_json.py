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
        cmd = "taosBenchmark -f ./5-taos-tools/taosbenchmark/json/taosc_auto_create_table.json"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("select count(tbname) from db.stb1")
        tdSql.checkData(0, 0, 8)
        tdSql.query("select count(*) from db.stb1")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select distinct(c5) from db.stb1")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c6) from db.stb1")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c7) from db.stb1")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c8) from db.stb1")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c9) from db.stb1")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c10) from db.stb1")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c11) from db.stb1")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c12) from db.stb1")
        tdSql.checkData(0, 0, None)

        tdSql.execute("reset query cache")
        tdSql.query("select count(tbname) from db.`stb1-2`")
        tdSql.checkData(0, 0, 8)
        tdSql.query("select count(*) from db.`stb1-2`")
        tdSql.checkData(0, 0, 160)
        tdSql.query("select distinct(c5) from db.`stb1-2`")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c6) from db.`stb1-2`")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c7) from db.`stb1-2`")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c8) from db.`stb1-2`")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c9) from db.`stb1-2`")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c10) from db.`stb1-2`")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c11) from db.`stb1-2`")
        tdSql.checkData(0, 0, None)
        tdSql.query("select distinct(c12) from db.`stb1-2`")
        tdSql.checkData(0, 0, None)

        cmd = "taosBenchmark -f ./5-taos-tools/taosbenchmark/json/stmt_auto_create_table.json"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("select count(tbname) from db.stb2")
        tdSql.checkData(0, 0, 8)
        tdSql.query("select count(*) from db.stb2")
        tdSql.checkData(0, 0, 160)
        tdSql.query("show databases")
        tdSql.checkData(0, 16, "us")

        tdSql.execute("reset query cache")
        tdSql.query("select count(tbname) from db.`stb2-2`")
        tdSql.checkData(0, 0, 8)
        tdSql.query("select count(*) from db.`stb2-2`")
        tdSql.checkData(0, 0, 160)

        cmd = "taosBenchmark -f ./5-taos-tools/taosbenchmark/json/rest_auto_create_table.json"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("select count(tbname) from db.stb3")
        tdSql.checkData(0, 0, 8)
        tdSql.query("select count(*) from db.stb3")
        tdSql.checkData(0, 0, 160)
        tdSql.query("show databases")
        tdSql.checkData(0, 16, "ns")

        tdSql.execute("reset query cache")
        tdSql.query("select count(tbname) from db.`stb3-2`")
        tdSql.checkData(0, 0, 8)
        tdSql.query("select count(*) from db.`stb3-2`")
        tdSql.checkData(0, 0, 160)

        cmd = "taosBenchmark -f ./5-taos-tools/taosbenchmark/json/sml_auto_create_table.json"
        tdLog.info("%s" % cmd)
        os.system("%s" % cmd)
        tdSql.execute("reset query cache")
        tdSql.query("select count(tbname) from db.stb4")
        tdSql.checkData(0, 0, 8)
        tdSql.query("select count(*) from db.stb4")
        tdSql.checkData(0, 0, 160)

        tdSql.execute("reset query cache")
        tdSql.query("select count(tbname) from db.`stb4-2`")
        tdSql.checkData(0, 0, 8)
        tdSql.query("select count(*) from db.`stb4-2`")
        tdSql.checkData(0, 0, 160)

    def stop(self):
        tdSql.close()
        tdLog.success("%s successfully executed" % __file__)


tdCases.addWindows(__file__, TDTestCase())
tdCases.addLinux(__file__, TDTestCase())